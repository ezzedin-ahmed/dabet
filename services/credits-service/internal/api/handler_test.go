package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/metrics"
	"dabet/services/credits-service/internal/stripe"

	"dabet/pkg/httpx"
)

var (
	jwtSecret     = []byte("jwt-test-secret")
	webhookSecret = []byte("whsec_test")
	testNow       = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
)

type env struct {
	h   *Handler
	mem *ledger.Memory
	mux *http.ServeMux
	met *metrics.Credits
}

func newEnv(t *testing.T, pi stripe.PaymentIntents) *env {
	t.Helper()
	mem := ledger.NewMemory()
	met := metrics.New(prometheus.NewRegistry())
	h := NewHandler(mem, pi, met, webhookSecret, 1, slog.New(slog.DiscardHandler))
	h.Now = func() time.Time { return testNow }
	mux := http.NewServeMux()
	h.Routes(mux, httpx.HMACVerifier(jwtSecret))
	return &env{h: h, mem: mem, mux: mux, met: met}
}

func token(t *testing.T, creatorID string) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   creatorID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
	}).SignedString(jwtSecret)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *env) do(t *testing.T, method, path, creatorID, body string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if creatorID != "" {
		req.Header.Set("Authorization", "Bearer "+token(t, creatorID))
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Set(k, v)
		}
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
}

// --- GET /v1/credits ---

func TestGetCreditsZeroRow(t *testing.T) {
	e := newEnv(t, nil)
	rec := e.do(t, "GET", "/v1/credits", "c1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Balance   int64  `json:"balance"`
		UpdatedAt string `json:"updated_at"`
	}
	decode(t, rec, &body)
	if body.Balance != 0 || body.UpdatedAt == "" {
		t.Fatalf("zero-row shape wrong: %+v", body)
	}
}

func TestGetCreditsWithBalance(t *testing.T) {
	e := newEnv(t, nil)
	e.mem.Apply(t.Context(), "c1", 42, ledger.ReasonTopup, "t1", nil)
	rec := e.do(t, "GET", "/v1/credits", "c1", "", nil)
	var body struct {
		Balance int64 `json:"balance"`
	}
	decode(t, rec, &body)
	if body.Balance != 42 {
		t.Fatalf("balance = %d, want 42", body.Balance)
	}
}

func TestGetCreditsRequiresAuth(t *testing.T) {
	e := newEnv(t, nil)
	if rec := e.do(t, "GET", "/v1/credits", "", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// --- GET /v1/credits/entries ---

func TestEntriesPagination(t *testing.T) {
	e := newEnv(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		e.mem.Apply(t.Context(), "c1", 1, ledger.ReasonTopup, k, nil)
	}
	type resp struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
	}

	rec := e.do(t, "GET", "/v1/credits/entries?limit=2", "c1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var page1 resp
	decode(t, rec, &page1)
	if len(page1.Items) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1 wrong: %+v", page1)
	}
	if page1.Items[0]["idempotency_key"] != "e" || page1.Items[1]["idempotency_key"] != "d" {
		t.Fatalf("not newest first: %+v", page1.Items)
	}

	rec = e.do(t, "GET", "/v1/credits/entries?limit=2&cursor="+page1.NextCursor, "c1", "", nil)
	var page2 resp
	decode(t, rec, &page2)
	if len(page2.Items) != 2 || page2.Items[0]["idempotency_key"] != "c" {
		t.Fatalf("page2 wrong: %+v", page2)
	}

	rec = e.do(t, "GET", "/v1/credits/entries?limit=2&cursor="+page2.NextCursor, "c1", "", nil)
	var page3 resp
	decode(t, rec, &page3)
	if len(page3.Items) != 1 || page3.NextCursor != "" {
		t.Fatalf("page3 must be the last page: %+v", page3)
	}
}

func TestEntriesRejectsBadCursorAndLimit(t *testing.T) {
	e := newEnv(t, nil)
	if rec := e.do(t, "GET", "/v1/credits/entries?cursor=%21%21", "c1", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor: status = %d", rec.Code)
	}
	if rec := e.do(t, "GET", "/v1/credits/entries?limit=nope", "c1", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit: status = %d", rec.Code)
	}
}

// --- POST /v1/credits/topup ---

func TestTopupRequiresIdempotencyKey(t *testing.T) {
	e := newEnv(t, nil)
	rec := e.do(t, "POST", "/v1/credits/topup", "c1", `{"amount_cents":500}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	decode(t, rec, &body)
	if body.Error.Code != "validation_failed" {
		t.Fatalf("code = %q", body.Error.Code)
	}
}

func TestTopupValidatesAmount(t *testing.T) {
	e := newEnv(t, nil)
	hdr := http.Header{"Idempotency-Key": []string{"k1"}}
	for _, body := range []string{`{"amount_cents":0}`, `{"amount_cents":-5}`, `{}`, `{"amount":5}`} {
		if rec := e.do(t, "POST", "/v1/credits/topup", "c1", body, hdr); rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
}

// fakeStripeServer is an httptest Stripe returning a fixed intent and
// counting create calls.
func fakeStripeServer(t *testing.T, calls *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		_ = r.ParseForm()
		_ = json.NewEncoder(w).Encode(map[string]string{
			"id":            "pi_test_1",
			"client_secret": "pi_test_1_secret",
		})
	}))
}

func TestTopupCreatesIntentAndReplays(t *testing.T) {
	var calls int
	srv := fakeStripeServer(t, &calls)
	defer srv.Close()
	e := newEnv(t, stripe.NewClient(srv.URL, "sk_test"))

	hdr := http.Header{"Idempotency-Key": []string{"k1"}}
	rec := e.do(t, "POST", "/v1/credits/topup", "c1", `{"amount_cents":500}`, hdr)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		ClientSecret    string `json:"client_secret"`
		PaymentIntentID string `json:"payment_intent_id"`
	}
	decode(t, rec, &body)
	if body.ClientSecret != "pi_test_1_secret" || body.PaymentIntentID != "pi_test_1" {
		t.Fatalf("response wrong: %+v", body)
	}
	if calls != 1 {
		t.Fatalf("stripe calls = %d, want 1", calls)
	}

	// Same Idempotency-Key: identical response, no second Stripe call.
	rec2 := e.do(t, "POST", "/v1/credits/topup", "c1", `{"amount_cents":500}`, hdr)
	if rec2.Code != http.StatusOK || rec2.Body.String() != rec.Body.String() {
		t.Fatalf("replay mismatch: %d %s vs %s", rec2.Code, rec2.Body.String(), rec.Body.String())
	}
	if calls != 1 {
		t.Fatalf("replay must not call stripe again: calls = %d", calls)
	}

	// No credits are granted by topup itself (§5.7).
	if balance, _, _, _ := e.mem.Balance(t.Context(), "c1"); balance != 0 {
		t.Fatalf("topup granted credits before the webhook: balance = %d", balance)
	}
}

func TestTopupStripeDownIs502(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	e := newEnv(t, stripe.NewClient(srv.URL, "sk_test"))

	hdr := http.Header{"Idempotency-Key": []string{"k1"}}
	rec := e.do(t, "POST", "/v1/credits/topup", "c1", `{"amount_cents":500}`, hdr)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	decode(t, rec, &body)
	if body.Error.Code != "upstream_error" {
		t.Fatalf("code = %q, want upstream_error", body.Error.Code)
	}
}

// --- credits-ok ---

func TestCreditsOK(t *testing.T) {
	e := newEnv(t, nil)
	get := func(creator string) bool {
		rec := e.do(t, "GET", "/internal/v1/credits-ok/"+creator, "", "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body struct {
			OK bool `json:"ok"`
		}
		decode(t, rec, &body)
		return body.OK
	}

	if get("nobody") {
		t.Fatal("zero-row creator must be not-ok")
	}
	e.mem.Apply(t.Context(), "rich", 10, ledger.ReasonTopup, "t1", nil)
	if !get("rich") {
		t.Fatal("positive balance must be ok")
	}
	e.mem.Apply(t.Context(), "rich", -10, "messages_processed", "u1", nil)
	if get("rich") {
		t.Fatal("zero balance must be not-ok")
	}
	e.mem.Apply(t.Context(), "rich", -1, ledger.ReasonAdjustment, "refund:x", nil)
	if get("rich") {
		t.Fatal("negative balance must behave identically to zero")
	}
}

// --- webhook ---

func signedWebhook(t *testing.T, e *env, payload string) *httptest.ResponseRecorder {
	t.Helper()
	hdr := http.Header{"Stripe-Signature": []string{stripe.Sign([]byte(payload), webhookSecret, testNow)}}
	return e.do(t, "POST", "/v1/webhooks/stripe", "", payload, hdr)
}

func TestWebhookRejectsBadSignatures(t *testing.T) {
	e := newEnv(t, nil)
	payload := `{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","amount_received":500,"metadata":{"creator_id":"c1"}}}}`

	// Missing header.
	if rec := e.do(t, "POST", "/v1/webhooks/stripe", "", payload, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing signature: status = %d, want 400", rec.Code)
	}
	// Tampered payload.
	hdr := http.Header{"Stripe-Signature": []string{stripe.Sign([]byte(payload), webhookSecret, testNow)}}
	if rec := e.do(t, "POST", "/v1/webhooks/stripe", "", payload+" ", hdr); rec.Code != http.StatusBadRequest {
		t.Fatalf("tampered payload: status = %d, want 400", rec.Code)
	}
	// Expired timestamp.
	old := http.Header{"Stripe-Signature": []string{stripe.Sign([]byte(payload), webhookSecret, testNow.Add(-10*time.Minute))}}
	if rec := e.do(t, "POST", "/v1/webhooks/stripe", "", payload, old); rec.Code != http.StatusBadRequest {
		t.Fatalf("expired timestamp: status = %d, want 400", rec.Code)
	}
	// Nothing landed in the ledger.
	if entries, _ := e.mem.Entries(t.Context(), "c1", 0, 10); len(entries) != 0 {
		t.Fatalf("unverified webhooks must not write: %d entries", len(entries))
	}
}

func TestWebhookPaymentSucceededGrantsOnce(t *testing.T) {
	e := newEnv(t, nil)
	payload := `{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","amount_received":500,"metadata":{"creator_id":"c1"}}}}`

	if rec := signedWebhook(t, e, payload); rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	balance, _, _, _ := e.mem.Balance(t.Context(), "c1")
	if balance != 500 { // 500 cents x 1 credit/cent
		t.Fatalf("balance = %d, want 500", balance)
	}
	entries, _ := e.mem.Entries(t.Context(), "c1", 0, 10)
	if len(entries) != 1 || entries[0].IdempotencyKey != "pi_1" || entries[0].Reason != ledger.ReasonTopup {
		t.Fatalf("entry wrong: %+v", entries)
	}
	if got := testutil.ToFloat64(e.met.TopupCents); got != 500 {
		t.Fatalf("credits_topup_cents_total = %v, want 500", got)
	}

	// Redelivered webhook: 204, no double grant, metric unchanged.
	if rec := signedWebhook(t, e, payload); rec.Code != http.StatusNoContent {
		t.Fatalf("replay status = %d", rec.Code)
	}
	if balance, _, _, _ := e.mem.Balance(t.Context(), "c1"); balance != 500 {
		t.Fatalf("replayed webhook double-granted: balance = %d", balance)
	}
	if got := testutil.ToFloat64(e.met.TopupCents); got != 500 {
		t.Fatalf("replayed webhook moved the counter: %v", got)
	}
}

func TestWebhookPaymentFailedLogsOnly(t *testing.T) {
	e := newEnv(t, nil)
	payload := `{"id":"evt_2","type":"payment_intent.payment_failed","data":{"object":{"id":"pi_2","metadata":{"creator_id":"c1"}}}}`
	if rec := signedWebhook(t, e, payload); rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	if entries, _ := e.mem.Entries(t.Context(), "c1", 0, 10); len(entries) != 0 {
		t.Fatalf("payment_failed must not write entries: %+v", entries)
	}
}

func TestWebhookChargeRefunded(t *testing.T) {
	e := newEnv(t, nil)
	e.mem.Apply(t.Context(), "c1", 500, ledger.ReasonTopup, "pi_1", nil)
	payload := `{"id":"evt_3","type":"charge.refunded","data":{"object":{"id":"ch_1","amount_refunded":600,"metadata":{"creator_id":"c1"}}}}`
	if rec := signedWebhook(t, e, payload); rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	balance, _, _, _ := e.mem.Balance(t.Context(), "c1")
	if balance != -100 {
		t.Fatalf("balance = %d, want -100 (refund may go negative)", balance)
	}
	entries, _ := e.mem.Entries(t.Context(), "c1", 0, 1)
	if entries[0].IdempotencyKey != "refund:ch_1" || entries[0].Reason != ledger.ReasonAdjustment || entries[0].Delta != -600 {
		t.Fatalf("refund entry wrong: %+v", entries[0])
	}
}

func TestWebhookDisputeCreated(t *testing.T) {
	e := newEnv(t, nil)
	payload := `{"id":"evt_4","type":"charge.dispute.created","data":{"object":{"id":"dp_1","amount":250,"charge":"ch_1","metadata":{"creator_id":"c1"}}}}`
	if rec := signedWebhook(t, e, payload); rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	entries, _ := e.mem.Entries(t.Context(), "c1", 0, 1)
	if len(entries) != 1 || entries[0].IdempotencyKey != "dispute:dp_1" || entries[0].Delta != -250 {
		t.Fatalf("dispute entry wrong: %+v", entries)
	}
}

func TestWebhookIgnoredTypesAndMissingMetadata(t *testing.T) {
	e := newEnv(t, nil)
	// Unhandled type: acknowledged, no entry.
	if rec := signedWebhook(t, e, `{"id":"evt_5","type":"customer.created","data":{"object":{"id":"cus_1"}}}`); rec.Code != http.StatusNoContent {
		t.Fatalf("ignored type status = %d, want 204", rec.Code)
	}
	// Succeeded intent without creator metadata: acknowledged, no entry.
	if rec := signedWebhook(t, e, `{"id":"evt_6","type":"payment_intent.succeeded","data":{"object":{"id":"pi_9","amount_received":100,"metadata":{}}}}`); rec.Code != http.StatusNoContent {
		t.Fatalf("no-metadata status = %d, want 204", rec.Code)
	}
	if n, _ := e.mem.NegativeBalances(t.Context()); n != 0 {
		t.Fatal("nothing should have been written")
	}
}
