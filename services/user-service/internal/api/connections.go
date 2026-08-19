// Connections endpoints (§5.5): start an OAuth round-trip, receive the
// provider callback, list connections (never exposing tokens), and
// disconnect. Per P4, no access token, refresh token, client secret, or
// PKCE verifier ever appears in a log line or error message.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/httpx"

	"dabet/services/user-service/internal/oauth"
	"dabet/services/user-service/internal/repo"
)

// OAuthStateTTL bounds a pending authorization round-trip (§5.5).
const OAuthStateTTL = 10 * time.Minute

// NewConnectionsGauge builds connections_active{platform} (§5.9).
func NewConnectionsGauge() *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "connections_active",
		Help: "Connections with status=active, by platform.",
	}, []string{"platform"})
}

// RunConnectionsGauge refreshes gauge from the repository every interval
// until ctx ends. Platforms with no active connections are reset to zero
// so a fully-disconnected platform does not freeze at its last value.
func RunConnectionsGauge(ctx context.Context, r repo.Repository, gauge *prometheus.GaugeVec, platforms []string, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		counts, err := r.ActiveConnectionCounts(ctx)
		if err == nil {
			for _, p := range platforms {
				gauge.WithLabelValues(p).Set(float64(counts[p]))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

type connectResponse struct {
	AuthorizeURL string `json:"authorize_url"`
	State        string `json:"state"`
}

type connectRequest struct {
	// RedirectAfter optionally overrides the post-callback redirect
	// target for this round-trip; APP_REDIRECT_URL otherwise.
	RedirectAfter string `json:"redirect_after,omitempty"`
}

// connectionsRoutes registers the §5.5 endpoints; called from Routes.
func (h *Handler) connectionsRoutes(mux *http.ServeMux) {
	authed := httpx.Auth(h.Keys.Verifier)
	mux.Handle("POST /v1/connections/{platform}", authed(http.HandlerFunc(h.handleConnect)))
	mux.HandleFunc("GET /v1/connections/callback", h.handleCallback)
	mux.Handle("GET /v1/connections", authed(http.HandlerFunc(h.handleListConnections)))
	mux.Handle("DELETE /v1/connections/{id}", authed(http.HandlerFunc(h.handleDisconnect)))
}

// platformNames lists configured platforms, sorted, for error messages.
func (h *Handler) platformNames() []string {
	out := make([]string, 0, len(h.Providers))
	for p := range h.Providers {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (h *Handler) handleConnect(w http.ResponseWriter, r *http.Request) {
	platform := r.PathValue("platform")
	provider, ok := h.Providers[platform]
	if !ok {
		httpx.WriteError(w, r, httpx.CodeValidationFailed,
			fmt.Sprintf("platform must be one of: %s", strings.Join(h.platformNames(), ", ")),
			map[string]any{"field": "platform"})
		return
	}

	var req connectRequest
	if r.Body != nil && r.ContentLength != 0 && !httpx.Decode(w, r, &req) {
		return
	}

	creatorID := httpx.CreatorIDFrom(r.Context())
	creator, err := h.Repo.CreatorByID(r.Context(), creatorID)
	if errors.Is(err, repo.ErrNotFound) {
		httpx.WriteError(w, r, httpx.CodeUnauthenticated, "invalid or expired token", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "load creator", err)
		return
	}
	// A3: email verification is required before connecting a platform.
	if creator.EmailVerifiedAt == nil {
		httpx.WriteError(w, r, httpx.CodeUnprocessable,
			"email must be verified before connecting a platform", nil)
		return
	}

	// state: 32-byte random, single-use, 10-minute TTL (§5.5 CSRF defence).
	state, _, err := h.NewToken()
	if err != nil {
		h.internal(w, r, "generate oauth state", err)
		return
	}
	verifier, err := h.NewVerifier()
	if err != nil {
		h.internal(w, r, "generate pkce verifier", err)
		return
	}
	var redirectAfter *string
	if req.RedirectAfter != "" {
		redirectAfter = &req.RedirectAfter
	}
	if err := h.Repo.CreateOAuthState(r.Context(), &repo.OAuthState{
		State:         state,
		CreatorID:     creatorID,
		Platform:      platform,
		CodeVerifier:  verifier,
		RedirectAfter: redirectAfter,
		ExpiresAt:     h.Now().Add(OAuthStateTTL),
	}); err != nil {
		h.internal(w, r, "store oauth state", err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, connectResponse{
		AuthorizeURL: provider.AuthorizeURL(state, verifier),
		State:        state,
	})
}

func (h *Handler) handleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	stateParam := r.URL.Query().Get("state")
	if code == "" || stateParam == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "code and state are required", nil)
		return
	}

	// Look up the state, delete it (single-use), verify unexpired. An
	// unknown or expired state is the CSRF defence and must not be
	// skipped (§5.5).
	state, err := h.Repo.ConsumeOAuthState(r.Context(), stateParam)
	if errors.Is(err, repo.ErrNotFound) {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "unknown or expired state", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "consume oauth state", err)
		return
	}
	if !state.ExpiresAt.After(h.Now()) {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "unknown or expired state", nil)
		return
	}
	provider, ok := h.Providers[state.Platform]
	if !ok {
		// A state for a platform no longer configured (e.g. mock flag
		// turned off between start and callback).
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "unknown or expired state", nil)
		return
	}

	tok, err := h.OAuth.Exchange(r.Context(), provider, code, state.CodeVerifier)
	if err != nil {
		h.upstream(w, r, "exchange authorization code", err)
		return
	}

	// §5.5: granted scopes must cover the moderation scopes, else 422
	// naming the missing scope — never a connection that silently cannot
	// moderate. Providers that omit the scope field are taken at their
	// word (granted = requested).
	if len(tok.Scopes) > 0 {
		if missing := oauth.MissingScopes(provider.Scopes, tok.Scopes); len(missing) > 0 {
			httpx.WriteError(w, r, httpx.CodeUnprocessable,
				fmt.Sprintf("granted scopes do not include required scope %q", missing[0]),
				map[string]any{"missing_scopes": missing})
			return
		}
	}

	providerUserID, displayName, err := h.OAuth.Userinfo(r.Context(), provider, tok.AccessToken)
	if err != nil {
		h.upstream(w, r, "fetch provider user info", err)
		return
	}

	conn := &repo.Connection{
		CreatorID:      state.CreatorID,
		Platform:       state.Platform,
		ProviderUserID: providerUserID,
		DisplayName:    displayName,
		AccessToken:    tok.AccessToken,
		Scopes:         tok.Scopes,
	}
	if len(conn.Scopes) == 0 {
		conn.Scopes = provider.Scopes
	}
	if tok.RefreshToken != "" {
		rt := tok.RefreshToken
		conn.RefreshToken = &rt
	}
	if tok.ExpiresIn > 0 {
		exp := h.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
		conn.ExpiresAt = &exp
	}
	if _, err := h.Repo.UpsertConnection(r.Context(), conn); err != nil {
		if errors.Is(err, repo.ErrConnectionConflict) {
			// A4: one platform account, one active connection, globally.
			httpx.WriteError(w, r, httpx.CodeConflict,
				"this platform account is already connected to another Dabet account", nil)
			return
		}
		h.internal(w, r, "upsert connection", err)
		return
	}

	target := h.AppRedirectURL
	if state.RedirectAfter != nil && *state.RedirectAfter != "" {
		target = *state.RedirectAfter
	}
	http.Redirect(w, r, target, http.StatusFound)
}

// upstream maps a provider failure to 502 upstream_error (§4.1). The
// logged error carries provider status codes only, never tokens.
func (h *Handler) upstream(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.Logger.Warn("oauth provider call failed", "op", op, "error", err.Error(),
		"request_id", httpx.RequestIDFrom(r.Context()))
	httpx.WriteError(w, r, httpx.CodeUpstreamError, "the platform's OAuth service failed", nil)
}

// connectionItem is the §5.5 wire shape — tokens are never exposed.
type connectionItem struct {
	ID          string `json:"id"`
	Platform    string `json:"platform"`
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
	ConnectedAt string `json:"connected_at"`
}

func (h *Handler) handleListConnections(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	conns, err := h.Repo.ListConnections(r.Context(), creatorID)
	if err != nil {
		h.internal(w, r, "list connections", err)
		return
	}
	items := make([]connectionItem, 0, len(conns))
	for _, c := range conns {
		items = append(items, connectionItem{
			ID:          c.ID,
			Platform:    c.Platform,
			DisplayName: c.DisplayName,
			Status:      c.Status,
			ConnectedAt: c.ConnectedAt.UTC().Format(time.RFC3339),
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		// A malformed id cannot be anyone's resource: 404, matching the
		// absent-or-not-yours rule (§4.1).
		httpx.WriteError(w, r, httpx.CodeNotFound, "connection not found", nil)
		return
	}
	creatorID := httpx.CreatorIDFrom(r.Context())
	conn, err := h.Repo.RevokeConnection(r.Context(), id, creatorID, h.Now())
	switch {
	case errors.Is(err, repo.ErrNotFound):
		httpx.WriteError(w, r, httpx.CodeNotFound, "connection not found", nil)
		return
	case errors.Is(err, repo.ErrAlreadyRevoked):
		httpx.WriteError(w, r, httpx.CodeStateConflict, "connection already revoked", nil)
		return
	case err != nil:
		h.internal(w, r, "revoke connection", err)
		return
	}

	// Best-effort provider-side revocation (§5.5): failure is logged
	// (status only, never the token) and does not fail the disconnect.
	if provider, ok := h.Providers[conn.Platform]; ok {
		if err := h.OAuth.Revoke(r.Context(), provider, conn.AccessToken); err != nil {
			h.Logger.Warn("provider token revocation failed",
				"platform", conn.Platform, "connection_id", conn.ID, "error", err.Error(),
				"request_id", httpx.RequestIDFrom(r.Context()))
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
