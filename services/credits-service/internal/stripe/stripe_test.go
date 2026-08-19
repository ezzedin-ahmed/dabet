package stripe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var (
	secret  = []byte("whsec_test")
	payload = []byte(`{"id":"evt_1","type":"payment_intent.succeeded"}`)
	now     = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
)

func TestVerifySignatureValid(t *testing.T) {
	header := Sign(payload, secret, now)
	if err := VerifySignature(payload, header, secret, now, DefaultTolerance); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	// A few minutes of skew inside the tolerance is fine, both ways.
	if err := VerifySignature(payload, header, secret, now.Add(4*time.Minute), DefaultTolerance); err != nil {
		t.Fatalf("skew within tolerance rejected: %v", err)
	}
	if err := VerifySignature(payload, header, secret, now.Add(-4*time.Minute), DefaultTolerance); err != nil {
		t.Fatalf("future timestamp within tolerance rejected: %v", err)
	}
}

func TestVerifySignatureMultipleV1(t *testing.T) {
	// Stripe sends several v1 signatures during secret rolls; any match passes.
	good := Sign(payload, secret, now)
	_, v1, _ := strings.Cut(good, ",v1=")
	header := "t=" + parseT(t, good) + ",v1=" + strings.Repeat("ab", 32) + ",v1=" + v1
	if err := VerifySignature(payload, header, secret, now, DefaultTolerance); err != nil {
		t.Fatalf("one matching v1 among several must pass: %v", err)
	}
}

func parseT(t *testing.T, header string) string {
	t.Helper()
	rest, _, _ := strings.Cut(header, ",")
	_, ts, _ := strings.Cut(rest, "=")
	return ts
}

func TestVerifySignatureTamperedPayload(t *testing.T) {
	header := Sign(payload, secret, now)
	tampered := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","amount":9999}`)
	if err := VerifySignature(tampered, header, secret, now, DefaultTolerance); err == nil {
		t.Fatal("tampered payload must be rejected")
	}
}

func TestVerifySignatureWrongSecret(t *testing.T) {
	header := Sign(payload, []byte("other"), now)
	if err := VerifySignature(payload, header, secret, now, DefaultTolerance); err == nil {
		t.Fatal("signature with the wrong secret must be rejected")
	}
}

func TestVerifySignatureExpiredTimestamp(t *testing.T) {
	header := Sign(payload, secret, now.Add(-6*time.Minute))
	if err := VerifySignature(payload, header, secret, now, DefaultTolerance); err == nil {
		t.Fatal("timestamp older than the tolerance must be rejected")
	}
}

func TestVerifySignatureMalformedHeaders(t *testing.T) {
	valid := Sign(payload, secret, now)
	_, v1, _ := strings.Cut(valid, ",v1=")
	cases := map[string]string{
		"empty":      "",
		"missing v1": "t=" + parseT(t, valid),
		"missing t":  "v1=" + v1,
		"bad t":      "t=yesterday,v1=" + v1,
		"non-hex v1": "t=" + parseT(t, valid) + ",v1=zzzz",
		"garbage":    "sig",
	}
	for name, header := range cases {
		if err := VerifySignature(payload, header, secret, now, DefaultTolerance); err == nil {
			t.Errorf("%s: header %q must be rejected", name, header)
		}
	}
}

func TestCreatePaymentIntent(t *testing.T) {
	var gotAuth, gotIdem string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/payment_intents" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotIdem = r.Header.Get("Idempotency-Key")
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.PostForm.Get("amount") != "500" || r.PostForm.Get("currency") != "usd" {
			t.Errorf("form wrong: %v", r.PostForm)
		}
		if r.PostForm.Get("metadata[creator_id]") != "c1" {
			t.Errorf("metadata missing: %v", r.PostForm)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"id": "pi_123", "client_secret": "pi_123_secret_x",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "sk_test_123")
	pi, err := c.CreatePaymentIntent(context.Background(), 500,
		map[string]string{"creator_id": "c1"}, "topup:c1:k1")
	if err != nil {
		t.Fatal(err)
	}
	if pi.ID != "pi_123" || pi.ClientSecret != "pi_123_secret_x" {
		t.Fatalf("wrong intent: %+v", pi)
	}
	if gotAuth != "Bearer sk_test_123" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotIdem != "topup:c1:k1" {
		t.Fatalf("idempotency header = %q", gotIdem)
	}
}

func TestCreatePaymentIntentErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":{"type":"card_error","message":"declined"}}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "sk_test")
	if _, err := c.CreatePaymentIntent(context.Background(), 500, nil, "k"); err == nil {
		t.Fatal("non-2xx must be an error")
	}

	srv.Close()
	if _, err := c.CreatePaymentIntent(context.Background(), 500, nil, "k"); err == nil {
		t.Fatal("transport failure must be an error")
	}
}
