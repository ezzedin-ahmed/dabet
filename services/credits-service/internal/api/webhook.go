package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"dabet/pkg/httpx"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/stripe"
)

// maxWebhookBody bounds the webhook payload read.
const maxWebhookBody = 1 << 20

// webhookEvent is Stripe's event envelope, reduced to what §5.7 needs.
type webhookEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object json.RawMessage `json:"object"`
	} `json:"data"`
}

// paymentIntentObject is data.object for payment_intent.* events.
type paymentIntentObject struct {
	ID             string            `json:"id"`
	AmountReceived int64             `json:"amount_received"`
	Metadata       map[string]string `json:"metadata"`
}

// chargeObject is data.object for charge.refunded.
type chargeObject struct {
	ID             string            `json:"id"`
	AmountRefunded int64             `json:"amount_refunded"`
	Metadata       map[string]string `json:"metadata"`
}

// disputeObject is data.object for charge.dispute.created.
type disputeObject struct {
	ID       string            `json:"id"`
	Amount   int64             `json:"amount"`
	Charge   string            `json:"charge"`
	Metadata map[string]string `json:"metadata"`
}

// postStripeWebhook serves POST /v1/webhooks/stripe. The Stripe-Signature
// header is the authentication (§5.7): an unverifiable request is 400 and
// nothing else is inspected. Handled and deliberately ignored event types
// both return 204; only storage failures return 5xx so Stripe redelivers
// (the idempotency key makes the retry safe).
//
// ASSUMPTION — creator resolution: every PaymentIntent we create carries
// metadata.creator_id, and Stripe propagates PaymentIntent metadata onto
// its charges; refund/dispute objects are therefore resolved from their
// own metadata. An event without metadata.creator_id was not created by
// this service and is logged and acknowledged (204), not retried.
func (h *Handler) postStripeWebhook(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "unreadable body", nil)
		return
	}
	if err := stripe.VerifySignature(payload, r.Header.Get("Stripe-Signature"),
		h.WebhookSecret, h.Now(), stripe.DefaultTolerance); err != nil {
		// P4: log the failure reason only — never the payload.
		h.Logger.Warn("stripe webhook signature rejected", "error", err.Error(),
			"request_id", httpx.RequestIDFrom(r.Context()))
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid webhook signature", nil)
		return
	}

	var ev webhookEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "undecodable webhook event", nil)
		return
	}

	switch ev.Type {
	case "payment_intent.succeeded":
		h.webhookSucceeded(w, r, ev)
	case "payment_intent.payment_failed":
		var pi paymentIntentObject
		_ = json.Unmarshal(ev.Data.Object, &pi)
		h.Logger.Info("stripe payment failed", "payment_intent_id", pi.ID, "event_id", ev.ID)
		w.WriteHeader(http.StatusNoContent)
	case "charge.refunded":
		h.webhookRefunded(w, r, ev)
	case "charge.dispute.created":
		h.webhookDisputed(w, r, ev)
	default:
		// Unhandled event types are acknowledged so Stripe stops
		// redelivering them.
		w.WriteHeader(http.StatusNoContent)
	}
}

// webhookSucceeded grants credits for a captured payment. The
// payment_intent_id is the idempotency key, so redelivered webhooks are
// no-ops (§5.7).
func (h *Handler) webhookSucceeded(w http.ResponseWriter, r *http.Request, ev webhookEvent) {
	var pi paymentIntentObject
	if err := json.Unmarshal(ev.Data.Object, &pi); err != nil || pi.ID == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "undecodable payment_intent object", nil)
		return
	}
	creatorID := pi.Metadata["creator_id"]
	if creatorID == "" || pi.AmountReceived <= 0 {
		h.Logger.Warn("ignoring payment_intent.succeeded without creator metadata or amount",
			"payment_intent_id", pi.ID, "event_id", ev.ID)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	delta := pi.AmountReceived * h.CreditsPerCent
	res, err := h.Repo.Apply(r.Context(), creatorID, delta, ledger.ReasonTopup, pi.ID, map[string]any{
		"payment_intent_id": pi.ID,
		"amount_cents":      pi.AmountReceived,
	})
	if err != nil {
		h.internalError(w, r, "apply topup", err)
		return
	}
	if !res.Replayed {
		h.Metrics.TopupCents.Add(float64(pi.AmountReceived))
		h.Logger.Info("credits granted", "payment_intent_id", pi.ID)
	}
	w.WriteHeader(http.StatusNoContent)
}

// webhookRefunded books a negative adjustment for a refunded charge. The
// balance may go negative; the ledger must reflect reality (§5.7).
func (h *Handler) webhookRefunded(w http.ResponseWriter, r *http.Request, ev webhookEvent) {
	var ch chargeObject
	if err := json.Unmarshal(ev.Data.Object, &ch); err != nil || ch.ID == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "undecodable charge object", nil)
		return
	}
	creatorID := ch.Metadata["creator_id"]
	if creatorID == "" || ch.AmountRefunded <= 0 {
		h.Logger.Warn("ignoring charge.refunded without creator metadata or amount",
			"charge_id", ch.ID, "event_id", ev.ID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.applyAdjustment(w, r, creatorID, -ch.AmountRefunded*h.CreditsPerCent, "refund:"+ch.ID, map[string]any{
		"charge_id":    ch.ID,
		"amount_cents": ch.AmountRefunded,
		"kind":         "refund",
	})
}

// webhookDisputed books a negative adjustment for a newly created dispute.
func (h *Handler) webhookDisputed(w http.ResponseWriter, r *http.Request, ev webhookEvent) {
	var dp disputeObject
	if err := json.Unmarshal(ev.Data.Object, &dp); err != nil || dp.ID == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "undecodable dispute object", nil)
		return
	}
	creatorID := dp.Metadata["creator_id"]
	if creatorID == "" || dp.Amount <= 0 {
		h.Logger.Warn("ignoring charge.dispute.created without creator metadata or amount",
			"dispute_id", dp.ID, "event_id", ev.ID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.applyAdjustment(w, r, creatorID, -dp.Amount*h.CreditsPerCent, "dispute:"+dp.ID, map[string]any{
		"dispute_id":   dp.ID,
		"charge_id":    dp.Charge,
		"amount_cents": dp.Amount,
		"kind":         "dispute",
	})
}

func (h *Handler) applyAdjustment(w http.ResponseWriter, r *http.Request, creatorID string, delta int64, idemKey string, metadata map[string]any) {
	res, err := h.Repo.Apply(r.Context(), creatorID, delta, ledger.ReasonAdjustment, idemKey, metadata)
	if err != nil {
		h.internalError(w, r, "apply adjustment", err)
		return
	}
	if !res.Replayed && h.Notify != nil {
		// Best-effort (A8): never blocks the response or the ledger.
		go h.Notify.BalanceChanged(context.WithoutCancel(r.Context()), creatorID, res.Balance-delta, res.Balance)
	}
	w.WriteHeader(http.StatusNoContent)
}
