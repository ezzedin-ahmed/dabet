package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/user-service/internal/auth"
	"dabet/services/user-service/internal/repo"
)

const testSecret = "test-jwt-secret-not-for-production"

// tokenSeq is a deterministic opaque-token source so tests can read back
// tokens the handler issued (verification token, refresh tokens).
type tokenSeq struct {
	n    int
	raws []string
}

func (s *tokenSeq) next() (string, string, error) {
	s.n++
	raw := fmt.Sprintf("opaque-token-%d", s.n)
	s.raws = append(s.raws, raw)
	return raw, auth.HashToken(raw), nil
}

type fixture struct {
	t      *testing.T
	h      *Handler
	fake   *repo.Fake
	seq    *tokenSeq
	server *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fake := repo.NewFake()
	logins := NewLoginsCounter()
	h, err := NewHandler(fake, []byte(testSecret), slog.New(slog.DiscardHandler), logins)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	seq := &tokenSeq{}
	h.NewToken = seq.next
	mux := http.NewServeMux()
	h.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &fixture{t: t, h: h, fake: fake, seq: seq, server: server}
}

// post sends a JSON body and returns the status plus decoded body.
func (f *fixture) post(path, body string) (int, map[string]any) {
	f.t.Helper()
	resp, err := http.Post(f.server.URL+path, "application/json; charset=utf-8", strings.NewReader(body))
	if err != nil {
		f.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func (f *fixture) get(path, bearer string) (int, map[string]any) {
	f.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.server.URL+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func errorCode(t *testing.T, body map[string]any) string {
	t.Helper()
	e, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("body has no error envelope: %v", body)
	}
	code, _ := e["code"].(string)
	return code
}

const (
	goodEmail    = "creator@example.com"
	goodPassword = "a perfectly fine passphrase"
)

func registerBody(email string) string {
	return fmt.Sprintf(`{"email":%q,"fullname":"Creator","password":%q}`, email, goodPassword)
}

func (f *fixture) register(email string) string {
	f.t.Helper()
	status, body := f.post("/v1/auth/register", registerBody(email))
	if status != http.StatusCreated {
		f.t.Fatalf("register = %d, body %v; want 201", status, body)
	}
	id, _ := body["creator_id"].(string)
	if id == "" {
		f.t.Fatalf("register returned no creator_id: %v", body)
	}
	return id
}

func (f *fixture) login(email, password string) (int, map[string]any) {
	f.t.Helper()
	return f.post("/v1/auth/login", fmt.Sprintf(`{"email":%q,"password":%q}`, email, password))
}

func TestRegisterValidation(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"short password", `{"email":"a@b.com","fullname":"A","password":"elevenchars"}`, 400, "validation_failed"},
		{"common password", `{"email":"a@b.com","fullname":"A","password":"password12345"}`, 400, "validation_failed"},
		{"long fullname", fmt.Sprintf(`{"email":"a@b.com","fullname":%q,"password":%q}`, strings.Repeat("x", 33), goodPassword), 400, "validation_failed"},
		{"empty fullname", fmt.Sprintf(`{"email":"a@b.com","fullname":"","password":%q}`, goodPassword), 400, "validation_failed"},
		{"bad email", fmt.Sprintf(`{"email":"not-an-email","fullname":"A","password":%q}`, goodPassword), 400, "validation_failed"},
		{"unknown field", fmt.Sprintf(`{"email":"a@b.com","fullname":"A","password":%q,"admin":true}`, goodPassword), 400, "validation_failed"},
		{"malformed json", `{`, 400, "validation_failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := f.post("/v1/auth/register", tt.body)
			if status != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %v)", status, tt.wantStatus, body)
			}
			if code := errorCode(t, body); code != tt.wantCode {
				t.Fatalf("code = %q, want %q", code, tt.wantCode)
			}
			if e := body["error"].(map[string]any); strings.Contains(fmt.Sprint(e["message"]), "password12345") {
				t.Fatalf("error message echoes the password")
			}
		})
	}
}

func TestRegisterDuplicateEmail(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)

	status, body := f.post("/v1/auth/register", registerBody(goodEmail))
	if status != http.StatusConflict || errorCode(t, body) != "conflict" {
		t.Fatalf("duplicate register = %d %v; want 409 conflict", status, body)
	}
	// citext: case-insensitive duplicate.
	status, _ = f.post("/v1/auth/register", registerBody(strings.ToUpper(goodEmail)))
	if status != http.StatusConflict {
		t.Fatalf("case-insensitive duplicate register = %d; want 409", status)
	}
}

func TestVerifyFlow(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)
	verificationToken := f.seq.raws[0] // issued at register

	// /v1/me before verification: email_verified false.
	status, body := f.login(goodEmail, goodPassword)
	if status != http.StatusOK {
		t.Fatalf("login = %d %v", status, body)
	}
	access := body["access_token"].(string)
	status, me := f.get("/v1/me", access)
	if status != http.StatusOK || me["email_verified"] != false {
		t.Fatalf("me before verify = %d %v; want email_verified false", status, me)
	}

	status, _ = f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, verificationToken))
	if status != http.StatusNoContent {
		t.Fatalf("verify = %d; want 204", status)
	}
	status, me = f.get("/v1/me", access)
	if status != http.StatusOK || me["email_verified"] != true {
		t.Fatalf("me after verify = %d %v; want email_verified true", status, me)
	}

	// Single use: consuming again fails.
	status, body = f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, verificationToken))
	if status != http.StatusBadRequest || errorCode(t, body) != "validation_failed" {
		t.Fatalf("verify replay = %d %v; want 400 validation_failed", status, body)
	}
	// Unknown token fails.
	status, _ = f.post("/v1/auth/verify", `{"token":"no-such-token"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("verify unknown = %d; want 400", status)
	}
}

func TestVerifyExpiredToken(t *testing.T) {
	f := newFixture(t)
	f.h.VerifyTTL = -time.Minute // token is already expired when issued
	f.register(goodEmail)
	verificationToken := f.seq.raws[0]

	status, _ := f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, verificationToken))
	if status != http.StatusBadRequest {
		t.Fatalf("verify expired = %d; want 400", status)
	}
}

func TestLoginIdenticalUnauthenticated(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)

	status1, body1 := f.login("unknown@example.com", goodPassword)
	status2, body2 := f.login(goodEmail, "the wrong passphrase")
	if status1 != http.StatusUnauthorized || status2 != http.StatusUnauthorized {
		t.Fatalf("statuses = %d, %d; want 401, 401", status1, status2)
	}
	e1 := body1["error"].(map[string]any)
	e2 := body2["error"].(map[string]any)
	// Identical shape apart from the per-request id.
	if e1["code"] != e2["code"] || e1["message"] != e2["message"] {
		t.Fatalf("401 bodies differ: %v vs %v", e1, e2)
	}
	if e1["code"] != "unauthenticated" {
		t.Fatalf("code = %v; want unauthenticated", e1["code"])
	}

	if got := testutil.ToFloat64(f.h.Logins.WithLabelValues("failure")); got != 2 {
		t.Fatalf("auth_logins_total{outcome=failure} = %v; want 2", got)
	}
}

func TestLoginSuccess(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)

	status, body := f.login(goodEmail, goodPassword)
	if status != http.StatusOK {
		t.Fatalf("login = %d %v", status, body)
	}
	if body["access_token"] == "" || body["refresh_token"] == "" {
		t.Fatalf("missing tokens: %v", body)
	}
	if body["expires_in"].(float64) != 900 {
		t.Fatalf("expires_in = %v; want 900", body["expires_in"])
	}
	if got := testutil.ToFloat64(f.h.Logins.WithLabelValues("success")); got != 1 {
		t.Fatalf("auth_logins_total{outcome=success} = %v; want 1", got)
	}
}

func TestRefreshRotationAndFamilyRevocation(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)
	_, body := f.login(goodEmail, goodPassword)
	refresh1 := body["refresh_token"].(string)

	// Rotate: refresh1 -> refresh2.
	status, body := f.post("/v1/auth/refresh", fmt.Sprintf(`{"refresh_token":%q}`, refresh1))
	if status != http.StatusOK {
		t.Fatalf("refresh = %d %v; want 200", status, body)
	}
	refresh2 := body["refresh_token"].(string)
	if refresh2 == refresh1 {
		t.Fatalf("refresh token was not rotated")
	}
	if body["access_token"] == "" {
		t.Fatalf("refresh returned no access token")
	}

	// Reuse of the rotated refresh1: 401 and the whole family is revoked.
	status, body = f.post("/v1/auth/refresh", fmt.Sprintf(`{"refresh_token":%q}`, refresh1))
	if status != http.StatusUnauthorized || errorCode(t, body) != "unauthenticated" {
		t.Fatalf("reused token = %d %v; want 401 unauthenticated", status, body)
	}
	status, _ = f.post("/v1/auth/refresh", fmt.Sprintf(`{"refresh_token":%q}`, refresh2))
	if status != http.StatusUnauthorized {
		t.Fatalf("family member after reuse = %d; want 401 (family revoked)", status)
	}
}

func TestRefreshUnknownAndExpired(t *testing.T) {
	f := newFixture(t)
	f.h.RefreshTTL = -time.Minute // refresh tokens are already expired when issued
	f.register(goodEmail)
	_, body := f.login(goodEmail, goodPassword)
	refresh := body["refresh_token"].(string)

	status, _ := f.post("/v1/auth/refresh", `{"refresh_token":"no-such-token"}`)
	if status != http.StatusUnauthorized {
		t.Fatalf("unknown refresh = %d; want 401", status)
	}

	status, _ = f.post("/v1/auth/refresh", fmt.Sprintf(`{"refresh_token":%q}`, refresh))
	if status != http.StatusUnauthorized {
		t.Fatalf("expired refresh = %d; want 401", status)
	}
}

func TestLogout(t *testing.T) {
	f := newFixture(t)
	f.register(goodEmail)
	_, body := f.login(goodEmail, goodPassword)
	refresh := body["refresh_token"].(string)

	status, _ := f.post("/v1/auth/logout", fmt.Sprintf(`{"refresh_token":%q}`, refresh))
	if status != http.StatusNoContent {
		t.Fatalf("logout = %d; want 204", status)
	}
	status, _ = f.post("/v1/auth/refresh", fmt.Sprintf(`{"refresh_token":%q}`, refresh))
	if status != http.StatusUnauthorized {
		t.Fatalf("refresh after logout = %d; want 401", status)
	}
	// Idempotent: unknown token still 204.
	status, _ = f.post("/v1/auth/logout", `{"refresh_token":"no-such-token"}`)
	if status != http.StatusNoContent {
		t.Fatalf("logout unknown = %d; want 204", status)
	}
}

func TestMeAuth(t *testing.T) {
	f := newFixture(t)
	id := f.register(goodEmail)
	_, body := f.login(goodEmail, goodPassword)
	access := body["access_token"].(string)

	status, me := f.get("/v1/me", access)
	if status != http.StatusOK {
		t.Fatalf("me = %d %v", status, me)
	}
	if me["id"] != id || me["email"] != goodEmail || me["fullname"] != "Creator" || me["email_verified"] != false {
		t.Fatalf("me body = %v", me)
	}

	status, body = f.get("/v1/me", "")
	if status != http.StatusUnauthorized || errorCode(t, body) != "unauthenticated" {
		t.Fatalf("me without token = %d %v; want 401", status, body)
	}
	status, _ = f.get("/v1/me", "garbage.token.here")
	if status != http.StatusUnauthorized {
		t.Fatalf("me with garbage token = %d; want 401", status)
	}

	// Expired access token.
	expired, err := auth.IssueAccessToken([]byte(testSecret), id, time.Now().UTC().Add(-time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	status, _ = f.get("/v1/me", expired)
	if status != http.StatusUnauthorized {
		t.Fatalf("me with expired token = %d; want 401", status)
	}

	// Valid signature but nonexistent subject.
	ghost, err := auth.IssueAccessToken([]byte(testSecret), "3b4f8f5e-0000-4000-8000-000000000000", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	status, _ = f.get("/v1/me", ghost)
	if status != http.StatusUnauthorized {
		t.Fatalf("me with unknown subject = %d; want 401", status)
	}
}
