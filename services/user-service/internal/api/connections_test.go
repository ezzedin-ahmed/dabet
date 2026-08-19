package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"dabet/services/user-service/internal/auth"
	"dabet/services/user-service/internal/oauth"
)

// fakeProvider is an httptest OAuth provider: token, userinfo, and revoke
// endpoints with scriptable responses.
type fakeProvider struct {
	t      *testing.T
	server *httptest.Server

	mu            sync.Mutex
	tokenForm     url.Values     // last token-exchange form
	tokenStatus   int            // 0 => 200
	tokenResponse map[string]any // nil => default
	userinfo      map[string]any
	revoked       []string // tokens received at the revoke endpoint
}

func newFakeProvider(t *testing.T) *fakeProvider {
	fp := &fakeProvider{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("token endpoint form parse: %v", err)
		}
		fp.mu.Lock()
		fp.tokenForm = r.PostForm
		status, resp := fp.tokenStatus, fp.tokenResponse
		fp.mu.Unlock()
		if status == 0 {
			status = http.StatusOK
		}
		if resp == nil {
			resp = map[string]any{
				"access_token":  "prov-access-token",
				"refresh_token": "prov-refresh-token",
				"expires_in":    3600,
				"scope":         "moderator:manage:chat_messages user:read:chat",
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("GET /oauth/userinfo", func(w http.ResponseWriter, r *http.Request) {
		fp.mu.Lock()
		info := fp.userinfo
		fp.mu.Unlock()
		if info == nil {
			info = map[string]any{"id": "prov-user-1", "display_name": "somechannel"}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	})
	mux.HandleFunc("POST /oauth/revoke", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		fp.mu.Lock()
		fp.revoked = append(fp.revoked, r.PostForm.Get("token"))
		fp.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	fp.server = httptest.NewServer(mux)
	t.Cleanup(fp.server.Close)
	return fp
}

func (fp *fakeProvider) provider(platform string, scopes ...string) *oauth.Provider {
	if len(scopes) == 0 {
		scopes = []string{"moderator:manage:chat_messages", "user:read:chat"}
	}
	return &oauth.Provider{
		Platform:     platform,
		AuthURL:      fp.server.URL + "/oauth/authorize",
		TokenURL:     fp.server.URL + "/oauth/token",
		UserinfoURL:  fp.server.URL + "/oauth/userinfo",
		RevokeURL:    fp.server.URL + "/oauth/revoke",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		Scopes:       scopes,
		RedirectURI:  "http://localhost:8080/v1/connections/callback",
	}
}

// connFixture is fixture plus a fake provider wired for twitch.
type connFixture struct {
	*fixture
	fp *fakeProvider
}

func newConnFixture(t *testing.T) *connFixture {
	f := newFixture(t)
	fp := newFakeProvider(t)
	f.h.Providers = map[string]*oauth.Provider{"twitch": fp.provider("twitch")}
	f.h.NewVerifier = func() (string, error) { return "test-pkce-verifier", nil }
	f.h.AppRedirectURL = "http://app.example/connections"
	return &connFixture{fixture: f, fp: fp}
}

// verifiedCreator registers a creator, verifies its email, and returns
// (creatorID, bearer token).
func (f *connFixture) verifiedCreator(email string) (string, string) {
	f.t.Helper()
	id := f.register(email)
	verifyToken := f.seq.raws[len(f.seq.raws)-1]
	if status, body := f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, verifyToken)); status != 204 {
		f.t.Fatalf("verify = %d, body %v", status, body)
	}
	return id, f.bearerFor(id)
}

func (f *connFixture) bearerFor(creatorID string) string {
	f.t.Helper()
	tok, err := auth.IssueAccessToken([]byte(testSecret), creatorID, time.Now().UTC(), 15*time.Minute)
	if err != nil {
		f.t.Fatalf("issue access token: %v", err)
	}
	return tok
}

// do sends an arbitrary request without following redirects.
func (f *connFixture) do(method, path, bearer, body string) (*http.Response, map[string]any) {
	f.t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, f.server.URL+path, rd)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, path, err)
	}
	f.t.Cleanup(func() { resp.Body.Close() })
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp, decoded
}

// connect starts the flow and returns the issued state.
func (f *connFixture) connect(bearer, platform string) (authorizeURL, state string) {
	f.t.Helper()
	resp, body := f.do(http.MethodPost, "/v1/connections/"+platform, bearer, "")
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("connect = %d, body %v; want 200", resp.StatusCode, body)
	}
	authorizeURL, _ = body["authorize_url"].(string)
	state, _ = body["state"].(string)
	if authorizeURL == "" || state == "" {
		f.t.Fatalf("connect response incomplete: %v", body)
	}
	return authorizeURL, state
}

func TestConnectAuthorizeURLShape(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("conn1@example.com")

	authorizeURL, state := f.connect(bearer, "twitch")
	u, err := url.Parse(authorizeURL)
	if err != nil {
		t.Fatalf("authorize_url unparsable: %v", err)
	}
	if got := u.Scheme + "://" + u.Host + u.Path; got != f.fp.server.URL+"/oauth/authorize" {
		t.Errorf("authorize_url base = %q", got)
	}
	q := u.Query()
	want := map[string]string{
		"response_type":         "code",
		"client_id":             "test-client-id",
		"redirect_uri":          "http://localhost:8080/v1/connections/callback",
		"scope":                 "moderator:manage:chat_messages user:read:chat",
		"state":                 state,
		"code_challenge":        oauth.Challenge("test-pkce-verifier"),
		"code_challenge_method": "S256",
	}
	for k, v := range want {
		if q.Get(k) != v {
			t.Errorf("authorize_url %s = %q, want %q", k, q.Get(k), v)
		}
	}
	// state is stored with the PKCE verifier and a 10-minute TTL.
	s, err := f.fake.ConsumeOAuthState(context.Background(), state)
	if err != nil {
		t.Fatalf("state not stored: %v", err)
	}
	if s.CodeVerifier != "test-pkce-verifier" || s.Platform != "twitch" {
		t.Errorf("stored state = %+v", s)
	}
	if ttl := time.Until(s.ExpiresAt); ttl > 10*time.Minute+time.Second || ttl < 9*time.Minute {
		t.Errorf("state TTL = %v, want ~10m", ttl)
	}
}

func TestConnectRequiresVerifiedEmail(t *testing.T) {
	f := newConnFixture(t)
	id := f.register("unverified@example.com")
	resp, body := f.do(http.MethodPost, "/v1/connections/twitch", f.bearerFor(id), "")
	if resp.StatusCode != http.StatusUnprocessableEntity || errorCode(t, body) != "unprocessable" {
		t.Fatalf("connect unverified = %d %v; want 422 unprocessable", resp.StatusCode, body)
	}
}

func TestConnectUnknownPlatform(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("conn2@example.com")
	resp, body := f.do(http.MethodPost, "/v1/connections/myspace", bearer, "")
	if resp.StatusCode != http.StatusBadRequest || errorCode(t, body) != "validation_failed" {
		t.Fatalf("connect unknown platform = %d %v; want 400 validation_failed", resp.StatusCode, body)
	}
}

func TestConnectMockPlatformGated(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("conn3@example.com")

	// Not configured (OAUTH_MOCK_ENABLED unset): rejected.
	resp, body := f.do(http.MethodPost, "/v1/connections/mock", bearer, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("mock while disabled = %d %v; want 400", resp.StatusCode, body)
	}

	f.h.Providers["mock"] = f.fp.provider("mock", "mock:moderate")
	if resp, _ := f.do(http.MethodPost, "/v1/connections/mock", bearer, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("mock while enabled = %d; want 200", resp.StatusCode)
	}
}

func TestConnectRequiresAuth(t *testing.T) {
	f := newConnFixture(t)
	resp, body := f.do(http.MethodPost, "/v1/connections/twitch", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("connect unauthenticated = %d %v; want 401", resp.StatusCode, body)
	}
}

func TestCallbackFullFlow(t *testing.T) {
	f := newConnFixture(t)
	creatorID, bearer := f.verifiedCreator("flow@example.com")
	_, state := f.connect(bearer, "twitch")

	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=authcode-1&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("callback = %d %v; want 302", resp.StatusCode, body)
	}
	if loc := resp.Header.Get("Location"); loc != "http://app.example/connections" {
		t.Errorf("redirect Location = %q", loc)
	}

	// The exchange carried the code, the PKCE verifier, and the fixed
	// redirect URI.
	f.fp.mu.Lock()
	form := f.fp.tokenForm
	f.fp.mu.Unlock()
	for k, v := range map[string]string{
		"grant_type":    "authorization_code",
		"code":          "authcode-1",
		"code_verifier": "test-pkce-verifier",
		"redirect_uri":  "http://localhost:8080/v1/connections/callback",
		"client_id":     "test-client-id",
	} {
		if form.Get(k) != v {
			t.Errorf("token form %s = %q, want %q", k, form.Get(k), v)
		}
	}

	// The connection row was upserted with tokens and provider identity.
	conns, err := f.fake.ListConnections(context.Background(), creatorID)
	if err != nil || len(conns) != 1 {
		t.Fatalf("connections = %v, %v; want 1 row", conns, err)
	}
	c := conns[0]
	if c.Platform != "twitch" || c.ProviderUserID != "prov-user-1" || c.DisplayName != "somechannel" ||
		c.Status != "active" || c.AccessToken != "prov-access-token" ||
		c.RefreshToken == nil || *c.RefreshToken != "prov-refresh-token" || c.ExpiresAt == nil {
		t.Errorf("upserted connection = %+v", c)
	}
}

func TestCallbackStateIsSingleUse(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("single@example.com")
	_, state := f.connect(bearer, "twitch")

	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c1&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("first callback = %d; want 302", resp.StatusCode)
	}
	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=c1&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusBadRequest || errorCode(t, body) != "validation_failed" {
		t.Fatalf("replayed state = %d %v; want 400 validation_failed", resp.StatusCode, body)
	}
}

func TestCallbackUnknownAndExpiredState(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("expired@example.com")

	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=c&state=never-issued", "", "")
	if resp.StatusCode != http.StatusBadRequest || errorCode(t, body) != "validation_failed" {
		t.Fatalf("unknown state = %d %v; want 400 validation_failed", resp.StatusCode, body)
	}

	_, state := f.connect(bearer, "twitch")
	f.h.Now = func() time.Time { return time.Now().UTC().Add(11 * time.Minute) }
	resp, body = f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusBadRequest || errorCode(t, body) != "validation_failed" {
		t.Fatalf("expired state = %d %v; want 400 validation_failed", resp.StatusCode, body)
	}
}

func TestCallbackMissingScope(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("scopes@example.com")
	_, state := f.connect(bearer, "twitch")

	f.fp.mu.Lock()
	f.fp.tokenResponse = map[string]any{
		"access_token": "tok",
		// Twitch-style JSON array scope, missing the moderation scope.
		"scope": []string{"user:read:chat"},
	}
	f.fp.mu.Unlock()

	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusUnprocessableEntity || errorCode(t, body) != "unprocessable" {
		t.Fatalf("missing scope = %d %v; want 422 unprocessable", resp.StatusCode, body)
	}
	env := body["error"].(map[string]any)
	if msg, _ := env["message"].(string); !strings.Contains(msg, "moderator:manage:chat_messages") {
		t.Errorf("422 message does not name the missing scope: %q", msg)
	}
}

func TestCallbackDuplicateConnectionConflict(t *testing.T) {
	f := newConnFixture(t)
	_, bearerA := f.verifiedCreator("owner-a@example.com")
	_, bearerB := f.verifiedCreator("owner-b@example.com")

	_, stateA := f.connect(bearerA, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(stateA), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("creator A callback = %d; want 302", resp.StatusCode)
	}

	// Creator B connects the same platform account (same provider user).
	_, stateB := f.connect(bearerB, "twitch")
	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(stateB), "", "")
	if resp.StatusCode != http.StatusConflict || errorCode(t, body) != "conflict" {
		t.Fatalf("duplicate connection = %d %v; want 409 conflict", resp.StatusCode, body)
	}
}

func TestCallbackReconnectReactivatesRow(t *testing.T) {
	f := newConnFixture(t)
	creatorID, bearer := f.verifiedCreator("reconnect@example.com")

	_, state := f.connect(bearer, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("first connect = %d; want 302", resp.StatusCode)
	}
	conns, _ := f.fake.ListConnections(context.Background(), creatorID)
	firstID := conns[0].ID

	// Disconnect, then run the flow again for the same platform account.
	resp, _ := f.do(http.MethodDelete, "/v1/connections/"+firstID, bearer, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("disconnect = %d; want 204", resp.StatusCode)
	}
	_, state = f.connect(bearer, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c2&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("reconnect = %d; want 302", resp.StatusCode)
	}

	conns, _ = f.fake.ListConnections(context.Background(), creatorID)
	if len(conns) != 1 || conns[0].ID != firstID || conns[0].Status != "active" {
		t.Errorf("reconnect rows = %+v; want the original row reactivated", conns)
	}
}

func TestCallbackRedirectAfterOverride(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("redir@example.com")

	resp, body := f.do(http.MethodPost, "/v1/connections/twitch", bearer, `{"redirect_after":"http://app.example/done"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect = %d %v", resp.StatusCode, body)
	}
	state, _ := body["state"].(string)
	resp, _ = f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "http://app.example/done" {
		t.Fatalf("callback = %d Location %q; want 302 to redirect_after", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestCallbackProviderFailureIsUpstreamError(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("upstream@example.com")
	_, state := f.connect(bearer, "twitch")

	f.fp.mu.Lock()
	f.fp.tokenStatus = http.StatusInternalServerError
	f.fp.mu.Unlock()

	resp, body := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", "")
	if resp.StatusCode != http.StatusBadGateway || errorCode(t, body) != "upstream_error" {
		t.Fatalf("provider failure = %d %v; want 502 upstream_error", resp.StatusCode, body)
	}
}

func TestListConnectionsNeverExposesTokens(t *testing.T) {
	f := newConnFixture(t)
	_, bearer := f.verifiedCreator("list@example.com")
	_, state := f.connect(bearer, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("callback failed")
	}

	req, _ := http.NewRequest(http.MethodGet, f.server.URL+"/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list = %d: %s", resp.StatusCode, raw)
	}

	// The raw JSON must not leak token material in keys or values.
	for _, needle := range []string{"access_token", "refresh_token", "prov-access-token", "prov-refresh-token", "scopes"} {
		if strings.Contains(string(raw), needle) {
			t.Errorf("list response leaks %q: %s", needle, raw)
		}
	}

	var body struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 {
		t.Fatalf("items = %v; want 1", body.Items)
	}
	item := body.Items[0]
	for _, key := range []string{"id", "platform", "display_name", "status", "connected_at"} {
		if _, ok := item[key]; !ok {
			t.Errorf("item missing %q: %v", key, item)
		}
	}
	if len(item) != 5 {
		t.Errorf("item has extra fields: %v", item)
	}
}

func TestDisconnect(t *testing.T) {
	f := newConnFixture(t)
	creatorID, bearer := f.verifiedCreator("disc@example.com")
	_, otherBearer := f.verifiedCreator("other@example.com")
	_, state := f.connect(bearer, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("callback failed")
	}
	conns, _ := f.fake.ListConnections(context.Background(), creatorID)
	id := conns[0].ID

	// Another creator's delete probes nothing: 404.
	resp, body := f.do(http.MethodDelete, "/v1/connections/"+id, otherBearer, "")
	if resp.StatusCode != http.StatusNotFound || errorCode(t, body) != "not_found" {
		t.Fatalf("foreign delete = %d %v; want 404 not_found", resp.StatusCode, body)
	}

	// Owner's delete: 204, row kept as revoked, provider token revoked.
	resp, _ = f.do(http.MethodDelete, "/v1/connections/"+id, bearer, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("disconnect = %d; want 204", resp.StatusCode)
	}
	conns, _ = f.fake.ListConnections(context.Background(), creatorID)
	if len(conns) != 1 || conns[0].Status != "revoked" {
		t.Errorf("after disconnect rows = %+v; want the row kept, revoked", conns)
	}
	f.fp.mu.Lock()
	revoked := append([]string(nil), f.fp.revoked...)
	f.fp.mu.Unlock()
	if len(revoked) != 1 || revoked[0] != "prov-access-token" {
		t.Errorf("provider revocations = %v; want the access token revoked", revoked)
	}

	// Repeat delete: 409 state_conflict.
	resp, body = f.do(http.MethodDelete, "/v1/connections/"+id, bearer, "")
	if resp.StatusCode != http.StatusConflict || errorCode(t, body) != "state_conflict" {
		t.Fatalf("repeat disconnect = %d %v; want 409 state_conflict", resp.StatusCode, body)
	}

	// Unknown id: 404.
	resp, body = f.do(http.MethodDelete, "/v1/connections/00000000-0000-0000-0000-000000000000", bearer, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown id delete = %d %v; want 404", resp.StatusCode, body)
	}
}

func TestActiveConnectionCounts(t *testing.T) {
	f := newConnFixture(t)
	creatorID, bearer := f.verifiedCreator("gauge@example.com")
	_, state := f.connect(bearer, "twitch")
	if resp, _ := f.do(http.MethodGet, "/v1/connections/callback?code=c&state="+url.QueryEscape(state), "", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("callback failed")
	}
	counts, err := f.fake.ActiveConnectionCounts(context.Background())
	if err != nil || counts["twitch"] != 1 {
		t.Fatalf("counts = %v, %v; want twitch:1", counts, err)
	}
	conns, _ := f.fake.ListConnections(context.Background(), creatorID)
	if _, err := f.fake.RevokeConnection(context.Background(), conns[0].ID, creatorID, time.Now()); err != nil {
		t.Fatal(err)
	}
	counts, _ = f.fake.ActiveConnectionCounts(context.Background())
	if counts["twitch"] != 0 {
		t.Fatalf("counts after revoke = %v; want twitch:0", counts)
	}
}
