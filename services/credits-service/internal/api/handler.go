// Package api serves the §5.7 credits endpoints — balance, ledger
// entries, Stripe top-up and webhook — plus the internal §5.8 credits-ok
// flag consumed by moderation-service.
//
// P4/security notes:
//   - Stripe secrets are held only as handler fields and are never logged.
//   - Webhook payloads are never logged at info or above; handlers log
//     event type and object ids only.
//   - Credits are granted only on the signature-verified webhook, never on
//     the client-side confirmation (§5.7).
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"dabet/pkg/credits"
	"dabet/pkg/httpx"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/metrics"
	"dabet/services/credits-service/internal/stripe"
)

// EnvCreditsPerCent converts captured cents into granted credits.
// ASSUMPTION — the spec does not price credits; default 1 credit per
// cent, environment-overridable per §4.4.
const EnvCreditsPerCent = "CREDITS_PER_CENT"

// DefaultCreditsPerCent is the default cents-to-credits conversion.
const DefaultCreditsPerCent = 1

// Notifier receives committed balance transitions (A8). Implementations
// must be fast or internally asynchronous; the handler already calls it
// off the request path.
type Notifier interface {
	BalanceChanged(ctx context.Context, creatorID string, before, after int64)
}

// Handler carries the credits API dependencies.
type Handler struct {
	Repo           ledger.Repository
	Stripe         stripe.PaymentIntents
	Metrics        *metrics.Credits
	Logger         *slog.Logger
	WebhookSecret  []byte
	CreditsPerCent int64
	Notify         Notifier // optional

	// Now is the clock (tests override it). Defaults to time.Now.
	Now func() time.Time

	idemMu    sync.Mutex
	idemStore map[string]idemResponse
}

type idemResponse struct {
	body    []byte
	expires time.Time
}

// idemTTL is the §4.1 idempotent-response retention.
const idemTTL = 24 * time.Hour

// NewHandler builds a Handler.
func NewHandler(repo ledger.Repository, pi stripe.PaymentIntents, met *metrics.Credits, webhookSecret []byte, creditsPerCent int64, logger *slog.Logger) *Handler {
	if creditsPerCent <= 0 {
		creditsPerCent = DefaultCreditsPerCent
	}
	return &Handler{
		Repo:           repo,
		Stripe:         pi,
		Metrics:        met,
		Logger:         logger,
		WebhookSecret:  webhookSecret,
		CreditsPerCent: creditsPerCent,
		Now:            time.Now,
		idemStore:      make(map[string]idemResponse),
	}
}

// Routes mounts every endpoint on mux. Balance, entries, and topup are
// JWT-authed; the Stripe webhook is authenticated by its signature and
// the credits-ok endpoint is internal (§5.8).
func (h *Handler) Routes(mux *http.ServeMux, verifier *httpx.Verifier) {
	auth := httpx.Auth(verifier)
	mux.Handle("GET /v1/credits", auth(http.HandlerFunc(h.getCredits)))
	mux.Handle("GET /v1/credits/entries", auth(http.HandlerFunc(h.getEntries)))
	mux.Handle("POST /v1/credits/topup", auth(http.HandlerFunc(h.postTopup)))
	mux.Handle("POST /v1/webhooks/stripe", http.HandlerFunc(h.postStripeWebhook))
	mux.Handle("GET "+credits.PathPrefix+"{creator_id}", credits.Handler(h.creditsOK))
}

// creditsOK backs GET /internal/v1/credits-ok/{creator_id}: true iff the
// balance is strictly positive. A missing row is balance 0, and negative
// behaves identically to zero (§5.8).
func (h *Handler) creditsOK(ctx context.Context, creatorID string) (bool, error) {
	balance, _, _, err := h.Repo.Balance(ctx, creatorID)
	if err != nil {
		return false, err
	}
	return balance > 0, nil
}

// getCredits serves GET /v1/credits -> {balance, updated_at}. A creator
// with no balance row has balance 0; updated_at is then the current time
// (ASSUMPTION — there is no ledger timestamp to report).
func (h *Handler) getCredits(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	balance, updatedAt, found, err := h.Repo.Balance(r.Context(), creatorID)
	if err != nil {
		h.internalError(w, r, "read balance", err)
		return
	}
	if !found {
		updatedAt = h.Now()
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"balance":    balance,
		"updated_at": updatedAt.UTC().Format(time.RFC3339),
	})
}

// entriesCursor is the opaque pagination cursor: entries strictly older
// than this id follow.
type entriesCursor struct {
	ID int64 `json:"id"`
}

// getEntries serves GET /v1/credits/entries: the creator's ledger, newest
// first, cursor-paginated per §4.1.
func (h *Handler) getEntries(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	limit, err := httpx.ParseLimit(r)
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, err.Error(), nil)
		return
	}
	var beforeID int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		var c entriesCursor
		if err := httpx.DecodeCursor(raw, &c); err != nil || c.ID <= 0 {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid cursor", nil)
			return
		}
		beforeID = c.ID
	}

	// Fetch one extra row to learn whether another page exists.
	rows, err := h.Repo.Entries(r.Context(), creatorID, beforeID, limit+1)
	if err != nil {
		h.internalError(w, r, "read entries", err)
		return
	}
	var nextCursor string
	if len(rows) > limit {
		rows = rows[:limit]
		nextCursor, err = httpx.EncodeCursor(entriesCursor{ID: rows[len(rows)-1].ID})
		if err != nil {
			h.internalError(w, r, "encode cursor", err)
			return
		}
	}

	items := make([]ledger.Entry, 0, len(rows))
	items = append(items, rows...)
	resp := map[string]any{"items": items}
	if nextCursor != "" {
		resp["next_cursor"] = nextCursor
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

type topupRequest struct {
	AmountCents int64 `json:"amount_cents"`
}

// postTopup serves POST /v1/credits/topup: creates a Stripe PaymentIntent
// and returns its client_secret. No credits are granted here — only the
// webhook grants (§5.7).
//
// Idempotency: the required Idempotency-Key is (1) replayed from an
// in-memory (creator_id, key) -> response store per §4.1, and (2) passed
// through to Stripe scoped by creator, so even across instance restarts a
// retried key cannot create a second PaymentIntent.
func (h *Handler) postTopup(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	idemKey := httpx.IdempotencyKey(r)
	if idemKey == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "Idempotency-Key header is required", nil)
		return
	}
	var req topupRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if req.AmountCents <= 0 {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "amount_cents must be a positive integer",
			map[string]any{"field": "amount_cents"})
		return
	}

	storeKey := creatorID + "\x00" + idemKey
	if body, ok := h.idemLookup(storeKey); ok {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}

	pi, err := h.Stripe.CreatePaymentIntent(r.Context(), req.AmountCents,
		map[string]string{"creator_id": creatorID},
		"topup:"+creatorID+":"+idemKey)
	if err != nil {
		h.Logger.Warn("stripe payment_intent create failed", "error", err.Error(),
			"request_id", httpx.RequestIDFrom(r.Context()))
		httpx.WriteError(w, r, httpx.CodeUpstreamError, "payment provider unavailable", nil)
		return
	}

	body, err := json.Marshal(map[string]string{
		"client_secret":     pi.ClientSecret,
		"payment_intent_id": pi.ID,
	})
	if err != nil {
		h.internalError(w, r, "encode topup response", err)
		return
	}
	h.idemRemember(storeKey, body)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (h *Handler) idemLookup(key string) ([]byte, bool) {
	h.idemMu.Lock()
	defer h.idemMu.Unlock()
	e, ok := h.idemStore[key]
	if !ok || h.Now().After(e.expires) {
		delete(h.idemStore, key)
		return nil, false
	}
	return e.body, true
}

func (h *Handler) idemRemember(key string, body []byte) {
	h.idemMu.Lock()
	defer h.idemMu.Unlock()
	now := h.Now()
	// Lazy prune so the store cannot grow without bound.
	for k, e := range h.idemStore {
		if now.After(e.expires) {
			delete(h.idemStore, k)
		}
	}
	h.idemStore[key] = idemResponse{body: body, expires: now.Add(idemTTL)}
}

func (h *Handler) internalError(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.Logger.Error(what+" failed", "error", err.Error(), "request_id", httpx.RequestIDFrom(r.Context()))
	httpx.WriteError(w, r, httpx.CodeInternalError, "internal error", nil)
}
