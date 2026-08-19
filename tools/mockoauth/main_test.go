package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(newServer().routes())
	t.Cleanup(ts.Close)
	return ts
}

// authorize drives GET /oauth/authorize without following the redirect and
// returns the code it handed back.
func authorize(t *testing.T, ts *httptest.Server, verifier, state string) string {
	t.Helper()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", "mock-client")
	q.Set("redirect_uri", "http://app.invalid/callback")
	q.Set("scope", "mock:moderate")
	q.Set("state", state)
	q.Set("code_challenge", challengeFor(verifier))
	q.Set("code_challenge_method", "S256")

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(ts.URL + "/oauth/authorize?" + q.Encode())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize status = %d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatalf("parse Location: %v", err)
	}
	if got := loc.Query().Get("state"); got != state {
		t.Fatalf("state = %q, want %q", got, state)
	}
	code := loc.Query().Get("code")
	if code == "" {
		t.Fatal("callback carried no code")
	}
	return code
}

func postForm(t *testing.T, ts *httptest.Server, path string, form url.Values) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+path, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestAuthorizationCodeFlow(t *testing.T) {
	ts := newTestServer(t)
	const verifier = "test-verifier-value-0123456789abcdef"
	code := authorize(t, ts, verifier, "state-123")

	resp := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {"http://app.invalid/callback"},
		"code_verifier": {verifier},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d, want 200", resp.StatusCode)
	}
	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if tok.AccessToken == "" || tok.RefreshToken == "" {
		t.Fatalf("token response missing tokens: %+v", tok)
	}
	if tok.Scope != "mock:moderate" {
		t.Fatalf("scope = %q, want the requested scope echoed", tok.Scope)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/oauth/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	ui, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("userinfo: %v", err)
	}
	defer ui.Body.Close()
	if ui.StatusCode != http.StatusOK {
		t.Fatalf("userinfo status = %d, want 200", ui.StatusCode)
	}
	var info map[string]string
	if err := json.NewDecoder(ui.Body).Decode(&info); err != nil {
		t.Fatalf("decode userinfo: %v", err)
	}
	if info["id"] == "" || info["name"] == "" {
		t.Fatalf("userinfo missing id/name: %v", info)
	}
}

func TestTokenRejectsWrongVerifier(t *testing.T) {
	ts := newTestServer(t)
	code := authorize(t, ts, "the-real-verifier", "s")

	resp := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"code_verifier": {"a-different-verifier"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a PKCE mismatch", resp.StatusCode)
	}
}

func TestCodeIsSingleUse(t *testing.T) {
	ts := newTestServer(t)
	const verifier = "verifier"
	code := authorize(t, ts, verifier, "s")
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"code_verifier": {verifier},
	}
	if resp := postForm(t, ts, "/oauth/token", form); resp.StatusCode != http.StatusOK {
		t.Fatalf("first exchange status = %d, want 200", resp.StatusCode)
	}
	if resp := postForm(t, ts, "/oauth/token", form); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("replayed exchange status = %d, want 400", resp.StatusCode)
	}
}

func TestRefreshGrantRotates(t *testing.T) {
	ts := newTestServer(t)
	const verifier = "verifier"
	code := authorize(t, ts, verifier, "s")
	resp := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"code_verifier": {verifier},
	})
	var first tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&first); err != nil {
		t.Fatalf("decode: %v", err)
	}

	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {first.RefreshToken}}
	r2 := postForm(t, ts, "/oauth/token", form)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("refresh status = %d, want 200", r2.StatusCode)
	}
	var second tokenResponse
	if err := json.NewDecoder(r2.Body).Decode(&second); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if second.AccessToken == first.AccessToken {
		t.Fatal("refresh returned the same access token")
	}
	if r3 := postForm(t, ts, "/oauth/token", form); r3.StatusCode != http.StatusBadRequest {
		t.Fatalf("reused refresh token status = %d, want 400", r3.StatusCode)
	}
}

func TestRevokeInvalidatesAccessToken(t *testing.T) {
	ts := newTestServer(t)
	const verifier = "verifier"
	code := authorize(t, ts, verifier, "s")
	resp := postForm(t, ts, "/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"code_verifier": {verifier},
	})
	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if r := postForm(t, ts, "/oauth/revoke", url.Values{"token": {tok.AccessToken}}); r.StatusCode != http.StatusOK {
		t.Fatalf("revoke status = %d, want 200", r.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/oauth/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	ui, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("userinfo: %v", err)
	}
	defer ui.Body.Close()
	if ui.StatusCode != http.StatusUnauthorized {
		t.Fatalf("userinfo after revoke = %d, want 401", ui.StatusCode)
	}
}

func TestDistinctAuthorizationsMintDistinctUsers(t *testing.T) {
	ts := newTestServer(t)
	const verifier = "verifier"
	ids := make(map[string]bool)
	for range 3 {
		code := authorize(t, ts, verifier, "s")
		resp := postForm(t, ts, "/oauth/token", url.Values{
			"grant_type":    {"authorization_code"},
			"code":          {code},
			"code_verifier": {verifier},
		})
		var tok tokenResponse
		if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
			t.Fatalf("decode: %v", err)
		}
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/oauth/userinfo", nil)
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		ui, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("userinfo: %v", err)
		}
		var info map[string]string
		_ = json.NewDecoder(ui.Body).Decode(&info)
		ui.Body.Close()
		if ids[info["id"]] {
			t.Fatalf("provider user id %q reused across authorizations", info["id"])
		}
		ids[info["id"]] = true
	}
}
