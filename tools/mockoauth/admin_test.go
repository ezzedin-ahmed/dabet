package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// connect drives a full authorization-code exchange for a pinned user id and
// returns the resulting token pair — the state a real connection is left in
// after §5.5.
func connect(t *testing.T, ts *httptest.Server, userID string) tokenResponse {
	t.Helper()
	const verifier = "admin-test-verifier-0123456789abcdef"
	q := url.Values{
		"response_type":         {"code"},
		"redirect_uri":          {"http://app.invalid/callback"},
		"scope":                 {"mock:moderate"},
		"user_id":               {userID},
		"code_challenge":        {challengeFor(verifier)},
		"code_challenge_method": {"S256"},
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(ts.URL + "/oauth/authorize?" + q.Encode())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	defer resp.Body.Close()
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatalf("parse Location: %v", err)
	}

	tokResp := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {loc.Query().Get("code")},
		"redirect_uri":  {"http://app.invalid/callback"},
		"code_verifier": {verifier},
	})
	if tokResp.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d, want 200", tokResp.StatusCode)
	}
	var tok tokenResponse
	if err := json.NewDecoder(tokResp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	return tok
}

func postJSON(t *testing.T, ts *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	enc, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode %s body: %v", path, err)
	}
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(enc))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func adminTokens(t *testing.T, ts *httptest.Server, userID string) tokenState {
	t.Helper()
	resp, err := http.Get(ts.URL + "/admin/tokens?user_id=" + url.QueryEscape(userID))
	if err != nil {
		t.Fatalf("GET /admin/tokens: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/admin/tokens status = %d, want 200", resp.StatusCode)
	}
	var out tokenState
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode token state: %v", err)
	}
	return out
}

func userinfoStatus(t *testing.T, ts *httptest.Server, token string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/oauth/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("userinfo: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestAdminTokensReportsTheLiveCredentials(t *testing.T) {
	ts := newTestServer(t)
	tok := connect(t, ts, "mu_admin1")

	state := adminTokens(t, ts, "mu_admin1")
	if len(state.AccessTokens) != 1 || state.AccessTokens[0] != tok.AccessToken {
		t.Errorf("access tokens = %v, want [%s]", state.AccessTokens, tok.AccessToken)
	}
	if len(state.RefreshTokens) != 1 || state.RefreshTokens[0] != tok.RefreshToken {
		t.Errorf("refresh tokens = %v, want [%s]", state.RefreshTokens, tok.RefreshToken)
	}

	// A different user's tokens are not this user's.
	if other := adminTokens(t, ts, "mu_admin2"); len(other.AccessTokens) != 0 {
		t.Errorf("unknown user reported tokens: %v", other)
	}
}

// scope=access is the positive branch of §5.6: the API call starts failing
// with a 401, but the refresh grant still works, so the adapter can recover.
func TestInvalidateAccessLeavesRefreshUsable(t *testing.T) {
	ts := newTestServer(t)
	tok := connect(t, ts, "mu_positive")

	if got := userinfoStatus(t, ts, tok.AccessToken); got != http.StatusOK {
		t.Fatalf("userinfo before invalidation = %d, want 200", got)
	}

	resp := postJSON(t, ts, "/admin/invalidate", invalidateRequest{UserID: "mu_positive", Scope: "access"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/admin/invalidate status = %d, want 200", resp.StatusCode)
	}
	var out invalidateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode invalidate response: %v", err)
	}
	if out.Access != 1 || out.Refresh != 0 {
		t.Fatalf("invalidate reported %+v, want access=1 refresh=0", out)
	}

	if got := userinfoStatus(t, ts, tok.AccessToken); got != http.StatusUnauthorized {
		t.Fatalf("userinfo after invalidation = %d, want 401", got)
	}

	// The refresh grant still works and hands back a *different* access token.
	refreshed := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tok.RefreshToken},
	})
	if refreshed.StatusCode != http.StatusOK {
		t.Fatalf("refresh after access invalidation = %d, want 200", refreshed.StatusCode)
	}
	var fresh tokenResponse
	if err := json.NewDecoder(refreshed.Body).Decode(&fresh); err != nil {
		t.Fatalf("decode refreshed token: %v", err)
	}
	if fresh.AccessToken == tok.AccessToken {
		t.Fatal("refresh returned the same access token; §5.6 requires a new one")
	}
	if got := userinfoStatus(t, ts, fresh.AccessToken); got != http.StatusOK {
		t.Fatalf("userinfo with the refreshed token = %d, want 200", got)
	}
}

// scope=refresh is the negative branch: the refresh grant fails with the
// auth-shaped error (400 invalid_grant) that provider-adapter classifies as
// terminal and turns into connection status `expired`.
func TestInvalidateRefreshMakesTheGrantFailWithAnAuthError(t *testing.T) {
	ts := newTestServer(t)
	tok := connect(t, ts, "mu_negative")

	resp := postJSON(t, ts, "/admin/invalidate", invalidateRequest{UserID: "mu_negative", Scope: "refresh"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/admin/invalidate status = %d, want 200", resp.StatusCode)
	}

	failed := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tok.RefreshToken},
	})
	// 400/401/403 are what refresh.exchange classifies as errAuth; anything
	// else is transient and would leave the connection `active` instead.
	if failed.StatusCode != http.StatusBadRequest {
		t.Fatalf("refresh after revocation = %d, want 400 so it classifies as an auth error",
			failed.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(failed.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body["error"] != "invalid_grant" {
		t.Errorf("error = %q, want invalid_grant", body["error"])
	}

	// The access token was deliberately left alone.
	if got := userinfoStatus(t, ts, tok.AccessToken); got != http.StatusOK {
		t.Errorf("userinfo after refresh-only revocation = %d, want 200", got)
	}
}

func TestInvalidateAllClearsBoth(t *testing.T) {
	ts := newTestServer(t)
	connect(t, ts, "mu_all")

	resp := postJSON(t, ts, "/admin/invalidate", invalidateRequest{UserID: "mu_all", Scope: "all"})
	var out invalidateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Access != 1 || out.Refresh != 1 {
		t.Fatalf("invalidate all reported %+v, want access=1 refresh=1", out)
	}
	if state := adminTokens(t, ts, "mu_all"); len(state.AccessTokens) != 0 || len(state.RefreshTokens) != 0 {
		t.Fatalf("tokens survived scope=all: %+v", state)
	}
}

// Invalidation must not reach across users — a test with two connections in
// flight has to be able to break exactly one of them.
func TestInvalidateIsScopedToOneUser(t *testing.T) {
	ts := newTestServer(t)
	victim := connect(t, ts, "mu_victim")
	bystander := connect(t, ts, "mu_bystander")

	postJSON(t, ts, "/admin/invalidate", invalidateRequest{UserID: "mu_victim", Scope: "all"})

	if got := userinfoStatus(t, ts, victim.AccessToken); got != http.StatusUnauthorized {
		t.Errorf("victim userinfo = %d, want 401", got)
	}
	if got := userinfoStatus(t, ts, bystander.AccessToken); got != http.StatusOK {
		t.Errorf("bystander userinfo = %d, want 200", got)
	}
}

func TestAdminInvalidateRejectsBadRequests(t *testing.T) {
	ts := newTestServer(t)
	for _, tc := range []struct {
		name string
		body any
	}{
		{"no user_id", invalidateRequest{Scope: "access"}},
		{"unknown scope", invalidateRequest{UserID: "mu_x", Scope: "sideways"}},
	} {
		if resp := postJSON(t, ts, "/admin/invalidate", tc.body); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", tc.name, resp.StatusCode)
		}
	}

	resp, err := http.Post(ts.URL+"/admin/invalidate", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("POST malformed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed body: status = %d, want 400", resp.StatusCode)
	}

	if r, err := http.Get(ts.URL + "/admin/tokens"); err != nil {
		t.Fatalf("GET /admin/tokens: %v", err)
	} else {
		defer r.Body.Close()
		if r.StatusCode != http.StatusBadRequest {
			t.Errorf("/admin/tokens without user_id: status = %d, want 400", r.StatusCode)
		}
	}
}

// scope defaults to access — the failure a §5.6 test reaches for most often.
func TestInvalidateDefaultsToAccessScope(t *testing.T) {
	ts := newTestServer(t)
	tok := connect(t, ts, "mu_default")

	postJSON(t, ts, "/admin/invalidate", map[string]string{"user_id": "mu_default"})

	if got := userinfoStatus(t, ts, tok.AccessToken); got != http.StatusUnauthorized {
		t.Errorf("userinfo = %d, want 401", got)
	}
	if state := adminTokens(t, ts, "mu_default"); len(state.RefreshTokens) != 1 {
		t.Errorf("refresh tokens = %v, want the pair's refresh token untouched", state.RefreshTokens)
	}
}

func TestAdminReset(t *testing.T) {
	ts := newTestServer(t)
	tok := connect(t, ts, "mu_reset")

	resp, err := http.Post(ts.URL+"/admin/reset", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /admin/reset: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("/admin/reset status = %d, want 204", resp.StatusCode)
	}
	if got := userinfoStatus(t, ts, tok.AccessToken); got != http.StatusUnauthorized {
		t.Errorf("userinfo after reset = %d, want 401", got)
	}
	if state := adminTokens(t, ts, "mu_reset"); len(state.AccessTokens) != 0 {
		t.Errorf("tokens survived a reset: %+v", state)
	}
}
