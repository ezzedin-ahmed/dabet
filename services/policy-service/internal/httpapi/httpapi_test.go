package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/httpx"

	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/store/memstore"
)

const (
	secret   = "test-secret"
	creatorA = "9d4ecafe-0000-0000-0000-00000000000a"
	creatorB = "9d4ecafe-0000-0000-0000-00000000000b"
)

func token(t *testing.T, creatorID string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   creatorID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
	})
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type env struct {
	srv  *httptest.Server
	repo *memstore.Mem
}

func newEnv(t *testing.T) *env {
	t.Helper()
	repo := memstore.New()
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	Register(mux, httpx.HMACVerifier([]byte(secret)), repo, metrics.New(prometheus.NewRegistry()), log)
	srv := httptest.NewServer(httpx.RequestID(mux))
	t.Cleanup(srv.Close)
	return &env{srv: srv, repo: repo}
}

// do sends an authenticated JSON request and decodes the response body.
func (e *env) do(t *testing.T, creatorID, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, buf)
	if err != nil {
		t.Fatal(err)
	}
	if creatorID != "" {
		req.Header.Set("Authorization", "Bearer "+token(t, creatorID))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode %s %s response (%d): %v: %s", method, path, resp.StatusCode, err, raw)
		}
	}
	return resp.StatusCode, decoded
}

func errCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func creatorPolicyBody(scopeID string) map[string]any {
	return map[string]any{
		"scope":            "creator",
		"scope_id":         scopeID,
		"restricted_words": []string{"Foo", "foo", "BAR"},
	}
}

func TestUnauthenticated(t *testing.T) {
	e := newEnv(t)
	status, body := e.do(t, "", "GET", "/v1/policies", nil)
	if status != http.StatusUnauthorized || errCode(body) != "unauthenticated" {
		t.Errorf("status=%d code=%s, want 401 unauthenticated", status, errCode(body))
	}
}

func TestCreateNormalizesAndReturns201(t *testing.T) {
	e := newEnv(t)
	status, body := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	if status != http.StatusCreated {
		t.Fatalf("status = %d, body = %v", status, body)
	}
	if body["id"] == "" || body["scope"] != "creator" || body["scope_id"] != creatorA {
		t.Errorf("unexpected body: %v", body)
	}
	words, _ := body["restricted_words"].([]any)
	if len(words) != 2 || words[0] != "foo" || words[1] != "bar" {
		t.Errorf("restricted_words not lowercased+deduplicated: %v", words)
	}
	if body["spam"] != "none" || body["restricted_content_action"] != "auto" {
		t.Errorf("defaults not applied: spam=%v action=%v", body["spam"], body["restricted_content_action"])
	}
}

func TestCreateDuplicateScopeConflicts(t *testing.T) {
	e := newEnv(t)
	if status, _ := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA)); status != 201 {
		t.Fatalf("first create: %d", status)
	}
	status, body := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	if status != http.StatusConflict || errCode(body) != "conflict" {
		t.Errorf("status=%d code=%s, want 409 conflict", status, errCode(body))
	}
}

func TestCreateScopeOwnership(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name    string
		scope   string
		scopeID string
		want    int
	}{
		{"creator scope must be own id", "creator", creatorB, 400},
		{"platform scope must derive from caller", "platform", creatorB + ":twitch", 400},
		{"platform scope needs a known platform", "platform", creatorA + ":myspace", 400},
		{"platform scope well-formed", "platform", creatorA + ":twitch", 201},
		{"content scope accepted unvalidated", "content", "ct_unowned", 201},
		{"invalid scope value", "solar-system", "x", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := e.do(t, creatorA, "POST", "/v1/policies", map[string]any{
				"scope": tc.scope, "scope_id": tc.scopeID,
			})
			if status != tc.want {
				t.Errorf("status = %d, want %d (body %v)", status, tc.want, body)
			}
			if tc.want == 400 && errCode(body) != "validation_failed" {
				t.Errorf("code = %s, want validation_failed", errCode(body))
			}
		})
	}
}

func TestCreateRejectsUnknownFields(t *testing.T) {
	e := newEnv(t)
	status, body := e.do(t, creatorA, "POST", "/v1/policies", map[string]any{
		"scope": "creator", "scope_id": creatorA, "surprise": true,
	})
	if status != http.StatusBadRequest || errCode(body) != "validation_failed" {
		t.Errorf("status=%d code=%s, want 400 validation_failed", status, errCode(body))
	}
}

func TestValidationDetailsCarryFieldAndLimit(t *testing.T) {
	e := newEnv(t)
	words := make([]string, 501)
	for i := range words {
		words[i] = "w" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+i%7))
	}
	status, body := e.do(t, creatorA, "POST", "/v1/policies", map[string]any{
		"scope": "creator", "scope_id": creatorA, "restricted_words": words,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d", status)
	}
	env, _ := body["error"].(map[string]any)
	details, _ := env["details"].(map[string]any)
	if details["field"] != "restricted_words" || details["limit"] != float64(500) {
		t.Errorf("details = %v, want field=restricted_words limit=500", details)
	}
}

func TestGetOwnershipIs404(t *testing.T) {
	e := newEnv(t)
	_, created := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	id := created["id"].(string)

	if status, _ := e.do(t, creatorA, "GET", "/v1/policies/"+id, nil); status != 200 {
		t.Errorf("owner get = %d, want 200", status)
	}
	status, body := e.do(t, creatorB, "GET", "/v1/policies/"+id, nil)
	if status != http.StatusNotFound || errCode(body) != "not_found" {
		t.Errorf("other creator get = %d %s, want 404 not_found", status, errCode(body))
	}
	if status, _ := e.do(t, creatorA, "GET", "/v1/policies/does-not-exist", nil); status != 404 {
		t.Errorf("missing get = %d, want 404", status)
	}
}

func TestListIsCreatorScopedFilterableAndPaginated(t *testing.T) {
	e := newEnv(t)
	e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	e.do(t, creatorA, "POST", "/v1/policies", map[string]any{"scope": "platform", "scope_id": creatorA + ":twitch"})
	e.do(t, creatorA, "POST", "/v1/policies", map[string]any{"scope": "content", "scope_id": "ct_1"})
	e.do(t, creatorB, "POST", "/v1/policies", creatorPolicyBody(creatorB))

	status, body := e.do(t, creatorA, "GET", "/v1/policies", nil)
	if status != 200 {
		t.Fatalf("list = %d", status)
	}
	if items, _ := body["items"].([]any); len(items) != 3 {
		t.Errorf("creator A sees %d items, want 3 (own only)", len(items))
	}

	_, body = e.do(t, creatorA, "GET", "/v1/policies?scope=platform", nil)
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("scope filter returned %d items, want 1", len(items))
	}
	if items[0].(map[string]any)["scope"] != "platform" {
		t.Errorf("filtered item scope = %v", items[0].(map[string]any)["scope"])
	}

	if status, body := e.do(t, creatorA, "GET", "/v1/policies?scope=galaxy", nil); status != 400 || errCode(body) != "validation_failed" {
		t.Errorf("invalid scope filter = %d %s, want 400", status, errCode(body))
	}

	// Cursor pagination: walk pages of 2 and collect every id exactly once.
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 5; page++ {
		path := "/v1/policies?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		_, body := e.do(t, creatorA, "GET", path, nil)
		for _, it := range body["items"].([]any) {
			id := it.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatalf("id %s returned twice across pages", id)
			}
			seen[id] = true
		}
		next, _ := body["next_cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 3 {
		t.Errorf("pagination walked %d items, want 3", len(seen))
	}

	if status, body := e.do(t, creatorA, "GET", "/v1/policies?cursor=%25bad", nil); status != 400 || errCode(body) != "validation_failed" {
		t.Errorf("invalid cursor = %d %s, want 400 validation_failed", status, errCode(body))
	}
}

func TestPutReplacesWholeDocument(t *testing.T) {
	e := newEnv(t)
	_, created := e.do(t, creatorA, "POST", "/v1/policies", map[string]any{
		"scope": "creator", "scope_id": creatorA,
		"rate_limit_messages": 5, "rate_limit_seconds": 10,
		"restricted_words": []string{"badword"},
		"spam":             "identical",
	})
	id := created["id"].(string)

	// PUT with an empty document: every field resets, nothing merges.
	status, body := e.do(t, creatorA, "PUT", "/v1/policies/"+id, map[string]any{})
	if status != 200 {
		t.Fatalf("put = %d, body %v", status, body)
	}
	if words, _ := body["restricted_words"].([]any); len(words) != 0 {
		t.Errorf("restricted_words survived the replace: %v", words)
	}
	if body["rate_limit_messages"] != nil || body["rate_limit_seconds"] != nil {
		t.Errorf("rate limit survived the replace: %v/%v", body["rate_limit_messages"], body["rate_limit_seconds"])
	}
	if body["spam"] != "none" {
		t.Errorf("spam = %v, want default none", body["spam"])
	}
	if body["scope"] != "creator" || body["scope_id"] != creatorA {
		t.Errorf("scope pair changed: %v/%v", body["scope"], body["scope_id"])
	}
}

func TestPutScopeIsImmutable(t *testing.T) {
	e := newEnv(t)
	_, created := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	id := created["id"].(string)

	for _, field := range []string{"scope", "scope_id"} {
		status, body := e.do(t, creatorA, "PUT", "/v1/policies/"+id, map[string]any{field: "content"})
		if status != http.StatusBadRequest || errCode(body) != "validation_failed" {
			t.Errorf("PUT with %s = %d %s, want 400 validation_failed", field, status, errCode(body))
		}
	}
}

func TestPutOwnershipAndValidation(t *testing.T) {
	e := newEnv(t)
	_, created := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	id := created["id"].(string)

	if status, _ := e.do(t, creatorB, "PUT", "/v1/policies/"+id, map[string]any{}); status != 404 {
		t.Errorf("other creator PUT = %d, want 404", status)
	}
	status, body := e.do(t, creatorA, "PUT", "/v1/policies/"+id, map[string]any{"rate_limit_messages": 5})
	if status != 400 {
		t.Errorf("half rate limit PUT = %d %v, want 400", status, body)
	}
}

func TestDelete(t *testing.T) {
	e := newEnv(t)
	_, created := e.do(t, creatorA, "POST", "/v1/policies", creatorPolicyBody(creatorA))
	id := created["id"].(string)

	if status, _ := e.do(t, creatorB, "DELETE", "/v1/policies/"+id, nil); status != 404 {
		t.Errorf("other creator DELETE = %d, want 404", status)
	}
	if status, _ := e.do(t, creatorA, "DELETE", "/v1/policies/"+id, nil); status != http.StatusNoContent {
		t.Errorf("DELETE = %d, want 204", status)
	}
	if status, _ := e.do(t, creatorA, "GET", "/v1/policies/"+id, nil); status != 404 {
		t.Errorf("GET after delete = %d, want 404", status)
	}
	if status, _ := e.do(t, creatorA, "DELETE", "/v1/policies/"+id, nil); status != 404 {
		t.Errorf("second DELETE = %d, want 404", status)
	}
}
