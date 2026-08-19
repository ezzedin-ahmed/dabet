package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, webhookURL, secret string) *httptest.Server {
	t.Helper()
	s := &server{
		webhookURL:       webhookURL,
		webhookSecret:    []byte(secret),
		httpc:            &http.Client{Timeout: 5 * time.Second},
		byIdempotencyKey: make(map[string]intent),
	}
	ts := httptest.NewServer(s.routes())
	t.Cleanup(ts.Close)
	return ts
}

func createIntent(t *testing.T, ts *httptest.Server, amount int64, creatorID, idemKey string) map[string]any {
	t.Helper()
	form := url.Values{}
	form.Set("amount", strconv.FormatInt(amount, 10))
	form.Set("currency", "usd")
	form.Set("metadata[creator_id]", creatorID)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/payment_intents", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create intent status = %d: %s", resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode intent: %v", err)
	}
	return out
}

func TestCreatePaymentIntent(t *testing.T) {
	ts := newTestServer(t, "", "")
	pi := createIntent(t, ts, 5000, "creator-1", "")

	id, _ := pi["id"].(string)
	if !strings.HasPrefix(id, "pi_") {
		t.Fatalf("id = %q, want a pi_ prefix", id)
	}
	if secret, _ := pi["client_secret"].(string); !strings.Contains(secret, "_secret_") {
		t.Fatalf("client_secret = %q, want a Stripe-shaped secret", secret)
	}
	md, _ := pi["metadata"].(map[string]any)
	if md["creator_id"] != "creator-1" {
		t.Fatalf("metadata = %v, want creator_id echoed back", md)
	}
}

func TestCreatePaymentIntentRejectsBadAmount(t *testing.T) {
	ts := newTestServer(t, "", "")
	form := url.Values{"amount": {"0"}, "currency": {"usd"}}
	resp, err := http.Post(ts.URL+"/v1/payment_intents",
		"application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIdempotencyKeyReplaysTheSameIntent(t *testing.T) {
	ts := newTestServer(t, "", "")
	first := createIntent(t, ts, 1000, "creator-1", "topup:creator-1:abc")
	second := createIntent(t, ts, 1000, "creator-1", "topup:creator-1:abc")
	if first["id"] != second["id"] {
		t.Fatalf("idempotent create returned %v then %v", first["id"], second["id"])
	}
	other := createIntent(t, ts, 1000, "creator-1", "topup:creator-1:xyz")
	if other["id"] == first["id"] {
		t.Fatal("a different Idempotency-Key returned the same intent")
	}
}

func TestConfirmDeliversASignedWebhook(t *testing.T) {
	const secret = "whsec_test"
	type captured struct {
		sig  string
		body []byte
	}
	got := make(chan captured, 1)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- captured{sig: r.Header.Get("Stripe-Signature"), body: body}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer sink.Close()

	ts := newTestServer(t, sink.URL, secret)
	body, _ := json.Marshal(confirmRequest{PaymentIntentID: "pi_fixed", CreatorID: "creator-9", AmountCents: 2500})
	resp, err := http.Post(ts.URL+"/internal/confirm", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("confirm status = %d, want 200", resp.StatusCode)
	}

	select {
	case c := <-got:
		verifyStripeSignature(t, c.body, c.sig, secret)
		var ev struct {
			Type string `json:"type"`
			Data struct {
				Object struct {
					ID             string            `json:"id"`
					AmountReceived int64             `json:"amount_received"`
					Metadata       map[string]string `json:"metadata"`
				} `json:"object"`
			} `json:"data"`
		}
		if err := json.Unmarshal(c.body, &ev); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if ev.Type != "payment_intent.succeeded" {
			t.Fatalf("event type = %q", ev.Type)
		}
		if ev.Data.Object.ID != "pi_fixed" || ev.Data.Object.AmountReceived != 2500 {
			t.Fatalf("unexpected payment intent object: %+v", ev.Data.Object)
		}
		if ev.Data.Object.Metadata["creator_id"] != "creator-9" {
			t.Fatalf("metadata = %v", ev.Data.Object.Metadata)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("webhook was never delivered")
	}
}

// verifyStripeSignature repeats credits-service's verification so the test
// proves the header is genuinely valid, not merely present.
func verifyStripeSignature(t *testing.T, payload []byte, header, secret string) {
	t.Helper()
	var ts, v1 string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			ts = v
		case "v1":
			v1 = v
		}
	}
	if ts == "" || v1 == "" {
		t.Fatalf("malformed Stripe-Signature %q", header)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(payload)
	if want := hex.EncodeToString(mac.Sum(nil)); want != v1 {
		t.Fatalf("signature mismatch: header %s, computed %s", v1, want)
	}
}

func TestConfirmWithoutWebhookConfigIsRejected(t *testing.T) {
	ts := newTestServer(t, "", "")
	body, _ := json.Marshal(confirmRequest{CreatorID: "c", AmountCents: 1})
	resp, err := http.Post(ts.URL+"/internal/confirm", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412", resp.StatusCode)
	}
}
