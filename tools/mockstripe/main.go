// Command mockstripe is a stand-in for the two pieces of the Stripe REST
// API that credits-service touches (docs §5.7): creating a PaymentIntent,
// and — optionally — delivering the `payment_intent.succeeded` webhook
// that actually grants the credits.
//
//	POST /v1/payment_intents          Stripe's form-encoded create call
//	POST /internal/confirm            test-only: fire the success webhook
//
// credits-service points at it with STRIPE_API_BASE. Money is only ever
// granted on the signature-verified webhook (§5.7), so the create call
// deliberately does nothing but hand back an id and a client secret.
//
// The webhook signature uses Stripe's real scheme —
// HMAC-SHA256(secret, "<unix>.<payload>") in a `t=…,v1=…` header — so
// credits-service's verifier is exercised for real, not bypassed.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type intent struct {
	ID       string            `json:"id"`
	Amount   int64             `json:"amount"`
	Currency string            `json:"currency"`
	Metadata map[string]string `json:"metadata"`
}

type server struct {
	webhookURL    string
	webhookSecret []byte
	httpc         *http.Client

	mu sync.Mutex
	// byIdempotencyKey replays the same intent for a repeated
	// Idempotency-Key, as Stripe does.
	byIdempotencyKey map[string]intent
}

func randomID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return prefix + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return prefix + hex.EncodeToString(b)
}

// handleCreateIntent implements POST /v1/payment_intents. Stripe's API is
// form-encoded with bracketed metadata keys (metadata[creator_id]).
func (s *server) handleCreateIntent(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeStripeError(w, http.StatusBadRequest, "invalid_request_error", "unparseable form body")
		return
	}
	amount, err := strconv.ParseInt(r.PostForm.Get("amount"), 10, 64)
	if err != nil || amount <= 0 {
		writeStripeError(w, http.StatusBadRequest, "invalid_request_error", "amount must be a positive integer")
		return
	}
	metadata := map[string]string{}
	for k, vs := range r.PostForm {
		if name, ok := strings.CutPrefix(k, "metadata["); ok && strings.HasSuffix(name, "]") && len(vs) > 0 {
			metadata[strings.TrimSuffix(name, "]")] = vs[0]
		}
	}

	idemKey := r.Header.Get("Idempotency-Key")
	s.mu.Lock()
	if idemKey != "" {
		if existing, ok := s.byIdempotencyKey[idemKey]; ok {
			s.mu.Unlock()
			writeIntent(w, existing)
			return
		}
	}
	pi := intent{
		ID:       randomID("pi_"),
		Amount:   amount,
		Currency: r.PostForm.Get("currency"),
		Metadata: metadata,
	}
	if idemKey != "" {
		s.byIdempotencyKey[idemKey] = pi
	}
	s.mu.Unlock()
	writeIntent(w, pi)
}

func writeIntent(w http.ResponseWriter, pi intent) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":            pi.ID,
		"object":        "payment_intent",
		"amount":        pi.Amount,
		"currency":      pi.Currency,
		"metadata":      pi.Metadata,
		"status":        "requires_payment_method",
		"client_secret": pi.ID + "_secret_" + randomID(""),
	})
}

type confirmRequest struct {
	PaymentIntentID string `json:"payment_intent_id"`
	CreatorID       string `json:"creator_id"`
	AmountCents     int64  `json:"amount_cents"`
}

// handleConfirm is the test-only stand-in for a shopper completing the
// payment in the browser: it delivers a signed payment_intent.succeeded
// webhook to credits-service, which is the only thing that grants credits.
func (s *server) handleConfirm(w http.ResponseWriter, r *http.Request) {
	if s.webhookURL == "" || len(s.webhookSecret) == 0 {
		writeStripeError(w, http.StatusPreconditionFailed, "api_error",
			"STRIPE_WEBHOOK_URL and STRIPE_WEBHOOK_SECRET are not configured")
		return
	}
	var req confirmRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeStripeError(w, http.StatusBadRequest, "invalid_request_error", "undecodable body")
		return
	}
	if req.CreatorID == "" || req.AmountCents <= 0 {
		writeStripeError(w, http.StatusBadRequest, "invalid_request_error", "creator_id and a positive amount_cents are required")
		return
	}
	if req.PaymentIntentID == "" {
		req.PaymentIntentID = randomID("pi_")
	}

	status, err := s.deliver(r.Context(), req)
	if err != nil {
		writeStripeError(w, http.StatusBadGateway, "api_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"payment_intent_id": req.PaymentIntentID,
		"webhook_status":    status,
	})
}

func (s *server) deliver(ctx context.Context, req confirmRequest) (int, error) {
	payload, err := json.Marshal(map[string]any{
		"id":   randomID("evt_"),
		"type": "payment_intent.succeeded",
		"data": map[string]any{"object": map[string]any{
			"id":              req.PaymentIntentID,
			"object":          "payment_intent",
			"amount_received": req.AmountCents,
			"metadata":        map[string]string{"creator_id": req.CreatorID},
		}},
	})
	if err != nil {
		return 0, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("Stripe-Signature", sign(payload, s.webhookSecret, time.Now()))
	resp, err := s.httpc.Do(hr)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// sign produces a Stripe-Signature header: t=<unix>,v1=<hmac hex> over
// "<unix>.<payload>". Identical to what credits-service verifies.
func sign(payload, secret []byte, t time.Time) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(payload)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeStripeError(w http.ResponseWriter, status int, typ, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"type": typ, "message": msg}})
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payment_intents", s.handleCreateIntent)
	mux.HandleFunc("POST /internal/confirm", s.handleConfirm)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":9098"
	}
	s := &server{
		webhookURL:       os.Getenv("STRIPE_WEBHOOK_URL"),
		webhookSecret:    []byte(os.Getenv("STRIPE_WEBHOOK_SECRET")),
		httpc:            &http.Client{Timeout: 10 * time.Second},
		byIdempotencyKey: make(map[string]intent),
	}
	srv := &http.Server{Addr: addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("mockstripe listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
