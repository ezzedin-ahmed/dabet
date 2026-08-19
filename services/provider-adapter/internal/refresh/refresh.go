// Package refresh implements §5.6 lazy token refresh: on a provider 401
// the deletion processor asks Refresh for fresh credentials, which
//
//  1. takes a Postgres advisory xact lock on
//     hashtext(connection_id::text) so concurrent workers refresh once,
//  2. re-reads the row — another worker may have already refreshed,
//  3. exchanges the refresh token at the platform's (env-configured)
//     token endpoint and updates access_token / refresh_token /
//     expires_at,
//  4. returns the fresh connection so the caller retries the original
//     call once.
//
// An auth error from the token endpoint moves the connection to
// status='expired' and drops its streams (via Evict). A transport error
// changes nothing — the connection stays active and the caller's own
// backoff applies.
//
// Per P4, no token, client secret, or provider response body is ever
// logged; errors carry status codes only.
package refresh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"dabet/services/provider-adapter/internal/driver"

	"dabet/pkg/tracing"
)

// Endpoint is one platform's token-endpoint configuration
// (OAUTH_<PLATFORM>_TOKEN_URL / _CLIENT_ID / _CLIENT_SECRET — the same
// names user-service reads, per §4.4).
type Endpoint struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
}

// Row is the connection row as re-read under the advisory lock.
type Row struct {
	ID           string
	Platform     string
	Status       string // active | expired | revoked
	AccessToken  string
	RefreshToken string // "" when NULL
	ExpiresAt    *time.Time
}

// RowOps are the row operations available inside a locked transaction.
type RowOps interface {
	Get(ctx context.Context, id string) (Row, error)
	// UpdateTokens writes the refreshed credentials. refreshToken == ""
	// keeps the stored one (providers may omit it on refresh).
	UpdateTokens(ctx context.Context, id, accessToken, refreshToken string, expiresAt *time.Time) error
	// MarkExpired sets status='expired' (§5.6 auth-error path).
	MarkExpired(ctx context.Context, id string) error
}

// Store runs fn inside a transaction holding
// pg_advisory_xact_lock(hashtext(connectionID)). fn returning an error
// rolls the transaction back; nil commits it.
type Store interface {
	Locked(ctx context.Context, connectionID string, fn func(ops RowOps) error) error
}

// ErrNotFound is returned by RowOps.Get for an absent connection.
var ErrNotFound = errors.New("refresh: connection not found")

// errAuth marks a token-endpoint auth failure (invalid_grant and kin).
type errAuth struct{ status int }

func (e *errAuth) Error() string {
	return fmt.Sprintf("refresh: token endpoint rejected the refresh token (status %d)", e.status)
}

// Metric outcomes for connection_refresh_total{platform,outcome}.
const (
	OutcomeSuccess   = "success"
	OutcomeCoalesced = "coalesced" // another worker refreshed under the lock
	OutcomeAuthFail  = "auth_failed"
	OutcomeTransient = "transient_error"
	OutcomeError     = "error" // store failure, missing endpoint config
)

// Refresher implements deletion.TokenRefresher against a Store and the
// per-platform token endpoints.
type Refresher struct {
	store     Store
	endpoints map[string]Endpoint
	http      *http.Client
	// refreshTotal is connection_refresh_total{platform,outcome} (§5.9).
	refreshTotal *prometheus.CounterVec
	log          *slog.Logger
	// evict drops the connection's streams immediately after an
	// auth-error expiry (Poller.Evict); may be nil.
	evict func(connectionID string)
	// update pushes a refreshed token into the source snapshot so watch
	// loops pick it up without a poll (Poller.Update); may be nil.
	update func(c driver.Connection)
}

// New wires a Refresher. evict and update may be nil.
func New(store Store, endpoints map[string]Endpoint, refreshTotal *prometheus.CounterVec, log *slog.Logger, evict func(string), update func(driver.Connection)) *Refresher {
	return &Refresher{
		store:        store,
		endpoints:    endpoints,
		http:         tracing.HTTPClient(10 * time.Second),
		refreshTotal: refreshTotal,
		log:          log,
		evict:        evict,
		update:       update,
	}
}

// SetHTTPClient overrides the token-endpoint client (tests).
func (r *Refresher) SetHTTPClient(c *http.Client) { r.http = c }

// Refresh implements deletion.TokenRefresher per §5.6.
func (r *Refresher) Refresh(ctx context.Context, conn driver.Connection) (driver.Connection, error) {
	out, outcome, err := r.refresh(ctx, conn)
	r.refreshTotal.WithLabelValues(conn.Platform, outcome).Inc()
	switch outcome {
	case OutcomeAuthFail:
		if r.evict != nil {
			r.evict(conn.ID)
		}
	case OutcomeSuccess, OutcomeCoalesced:
		if r.update != nil {
			r.update(out)
		}
	}
	if err != nil {
		return conn, err
	}
	return out, nil
}

func (r *Refresher) refresh(ctx context.Context, conn driver.Connection) (driver.Connection, string, error) {
	ep, ok := r.endpoints[conn.Platform]
	if !ok || ep.TokenURL == "" {
		return conn, OutcomeError, fmt.Errorf("refresh: no token endpoint configured for platform %q", conn.Platform)
	}

	out := conn
	outcome := OutcomeError
	var refreshErr error

	err := r.store.Locked(ctx, conn.ID, func(ops RowOps) error {
		row, err := ops.Get(ctx, conn.ID)
		if err != nil {
			return err
		}
		if row.Status != "active" {
			outcome = OutcomeAuthFail
			refreshErr = fmt.Errorf("refresh: connection is %s", row.Status)
			return nil // commit; nothing written
		}
		// Re-read beat us to it: another worker already refreshed while
		// we waited on the lock.
		if row.AccessToken != "" && row.AccessToken != conn.AccessToken {
			out.AccessToken = row.AccessToken
			outcome = OutcomeCoalesced
			return nil
		}
		if row.RefreshToken == "" {
			// Nothing to exchange — the §5.6 auth-error path.
			if err := ops.MarkExpired(ctx, conn.ID); err != nil {
				return err
			}
			outcome = OutcomeAuthFail
			refreshErr = errors.New("refresh: connection has no refresh token")
			return nil
		}

		tok, err := r.exchange(ctx, conn.Platform, ep, row.RefreshToken)
		var authErr *errAuth
		if errors.As(err, &authErr) {
			// Auth error on refresh: expired, streams dropped (§5.6).
			if err := ops.MarkExpired(ctx, conn.ID); err != nil {
				return err
			}
			outcome = OutcomeAuthFail
			refreshErr = err
			return nil
		}
		if err != nil {
			// Transport/5xx: stay active, caller backs off (§5.6).
			outcome = OutcomeTransient
			refreshErr = err
			return err // roll back
		}

		newRefresh := tok.RefreshToken // "" keeps the stored one
		var expiresAt *time.Time
		if tok.ExpiresIn > 0 {
			t := time.Now().UTC().Add(time.Duration(tok.ExpiresIn) * time.Second)
			expiresAt = &t
		}
		if err := ops.UpdateTokens(ctx, conn.ID, tok.AccessToken, newRefresh, expiresAt); err != nil {
			return err
		}
		out.AccessToken = tok.AccessToken
		outcome = OutcomeSuccess
		return nil
	})
	if err != nil && refreshErr == nil {
		refreshErr = err
	}
	if err != nil && outcome == OutcomeError {
		// Store-level failure: treat as transient, connection stays active.
		outcome = OutcomeTransient
	}
	if refreshErr != nil {
		r.log.Warn("token refresh failed", "platform", conn.Platform,
			"connection_id", conn.ID, "outcome", outcome, "error", refreshErr.Error())
	}
	return out, outcome, refreshErr
}

// tokenResponse is the provider refresh-grant response.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// exchange performs the refresh-token grant. 400/401/403 responses are
// auth errors (*errAuth); anything else non-2xx, and transport failures,
// are transient.
func (r *Refresher) exchange(ctx context.Context, platform string, ep Endpoint, refreshToken string) (*tokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	form.Set("client_id", ep.ClientID)
	if ep.ClientSecret != "" {
		form.Set("client_secret", ep.ClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("refresh: %s token request: %w", platform, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("refresh: %s token endpoint: %w", platform, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("refresh: %s token endpoint: %w", platform, err)
	}
	switch {
	case resp.StatusCode/100 == 2:
	case resp.StatusCode == http.StatusBadRequest,
		resp.StatusCode == http.StatusUnauthorized,
		resp.StatusCode == http.StatusForbidden:
		return nil, &errAuth{status: resp.StatusCode}
	default:
		return nil, fmt.Errorf("refresh: %s token endpoint returned status %d", platform, resp.StatusCode)
	}
	var tok tokenResponse
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("refresh: %s token endpoint: undecodable response", platform)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("refresh: %s token endpoint: no access token in response", platform)
	}
	return &tok, nil
}

// EndpointsFromEnv builds the per-platform endpoint map from env, using
// the same variable names as user-service (§4.4). Platforms without a
// configured or defaulted token URL are omitted.
func EndpointsFromEnv(getenv func(name, def string) string) map[string]Endpoint {
	defaults := map[string]string{
		"youtube": "https://oauth2.googleapis.com/token",
		"twitch":  "https://id.twitch.tv/oauth2/token",
		"discord": "https://discord.com/api/oauth2/token",
		"mock":    "http://localhost:9099/oauth/token",
	}
	out := make(map[string]Endpoint, len(defaults))
	for platform, def := range defaults {
		prefix := "OAUTH_" + strings.ToUpper(platform) + "_"
		ep := Endpoint{
			TokenURL:     getenv(prefix+"TOKEN_URL", def),
			ClientID:     getenv(prefix+"CLIENT_ID", ""),
			ClientSecret: getenv(prefix+"CLIENT_SECRET", ""),
		}
		if ep.TokenURL != "" {
			out[platform] = ep
		}
	}
	return out
}
