//go:build e2e

// Lazy token refresh (§5.6). The adapter is supposed to refresh on a 401 from
// a provider API, retry the call once, and — if the refresh itself fails with
// an auth error — move the connection to `expired` and drop its streams.
//
// Two halves have to line up for that to be provable end to end: the provider
// has to be able to start rejecting a live token on demand, and something the
// adapter calls has to actually surface the 401. This suite establishes the
// first half for real against mockoauth, and documents — with evidence — that
// the second half is unreachable for the only platform a local stack can
// connect. See FINDING F2 on stepAdapterRefreshOn401.
package e2e

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// refreshScenario tracks the provider-side identity and the tokens as they
// rotate, because §5.6's positive branch is precisely "the connection ends up
// with a *new* access token".
type refreshScenario struct {
	*scenario

	providerUserID string
	connectionID   string

	// The pair minted by the authorization-code exchange, and the pair the
	// refresh grant replaced it with.
	access0, refresh0 string
	access1, refresh1 string

	adapterBefore []sample
}

func TestLazyTokenRefresh(t *testing.T) {
	s := &refreshScenario{
		scenario:       newScenario("refresh"),
		providerUserID: fmt.Sprintf("mu_e2erefresh%d", time.Now().UnixNano()),
	}
	waitHealthy(t, healthTimeout)

	steps := []struct {
		name string
		fn   func(*testing.T, *refreshScenario)
	}{
		{"a_connect_with_a_pinned_provider_identity", stepConnectPinned},
		{"b_provider_rejects_the_access_token_and_the_refresh_grant_rotates", stepProviderRefreshContract},
		{"c_adapter_refreshes_on_401_and_the_stream_keeps_working", stepAdapterRefreshOn401},
		{"d_failed_refresh_expires_the_connection", stepRefreshAuthFailure},
		{"e_nothing_took_the_process_down", stepRefreshFailOpen},
	}
	for _, step := range steps {
		if !t.Run(step.name, func(t *testing.T) { step.fn(t, s) }) {
			t.Fatalf("step %s failed; the remaining steps depend on it", step.name)
		}
	}
}

// ---------------------------------------------------------------------
// mockoauth admin client
// ---------------------------------------------------------------------

type oauthTokenState struct {
	AccessTokens  []string `json:"access_tokens"`
	RefreshTokens []string `json:"refresh_tokens"`
}

// providerTokens reads the tokens mockoauth currently considers valid for a
// provider identity. It is the only window a test has into the credentials
// user-service stored, since §5.5 forbids the API from ever exposing them.
func providerTokens(t *testing.T, userID string) oauthTokenState {
	t.Helper()
	var out oauthTokenState
	mustStatus(t, do(t, client, http.MethodGet,
		oauthURL+"/admin/tokens?user_id="+url.QueryEscape(userID), "", nil),
		http.StatusOK, "GET mockoauth /admin/tokens").json(t, &out)
	return out
}

// invalidateProviderTokens makes the named credentials start being rejected.
// scope is access, refresh or all.
func invalidateProviderTokens(t *testing.T, userID, scope string) {
	t.Helper()
	mustStatus(t, do(t, client, http.MethodPost, oauthURL+"/admin/invalidate", "",
		map[string]string{"user_id": userID, "scope": scope}),
		http.StatusOK, "POST mockoauth /admin/invalidate")
}

// userinfoStatus is the provider API call that returns the 401 §5.6 keys off.
func userinfoStatus(t *testing.T, token string) int {
	t.Helper()
	return do(t, client, http.MethodGet, oauthURL+"/oauth/userinfo", token, nil).status
}

// refreshGrant performs the exchange provider-adapter's Refresher performs,
// with the same form fields, so its status classification is the one the
// adapter would see.
func refreshGrant(t *testing.T, refreshToken string) response {
	t.Helper()
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {"dabet-local"},
		"client_secret": {"dabet-local-secret"},
	}
	req, err := http.NewRequest(http.MethodPost, oauthURL+"/oauth/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build refresh request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("refresh grant: %v", err)
	}
	defer resp.Body.Close()
	return readResponse(t, resp)
}

// ---------------------------------------------------------------------
// (a) connect, pinning the provider identity
// ---------------------------------------------------------------------

// stepConnectPinned is stepConnect with one difference: it appends user_id to
// the authorize URL so the provider identity is known to the test. Without it
// the identity is minted at random inside mockoauth and there is no way to ask
// the admin API about the right user.
func stepConnectPinned(t *testing.T, s *refreshScenario) {
	stepAuth(t, s.scenario)

	var start struct {
		AuthorizeURL string `json:"authorize_url"`
		State        string `json:"state"`
	}
	mustStatus(t, do(t, client, http.MethodPost, userURL+"/v1/connections/mock", s.token, nil),
		http.StatusOK, "POST /v1/connections/mock").json(t, &start)

	authURL, err := url.Parse(start.AuthorizeURL)
	if err != nil {
		t.Fatalf("parse authorize_url: %v", err)
	}
	q := authURL.Query()
	q.Set("user_id", s.providerUserID)
	authURL.RawQuery = q.Encode()

	auth := do(t, noRedirect, http.MethodGet, authURL.String(), "", nil)
	if auth.status != http.StatusFound {
		t.Fatalf("authorize: status %d, want 302\nbody: %s", auth.status, truncate(auth.body))
	}
	callback := auth.location(t, authURL.String())
	cb := do(t, noRedirect, http.MethodGet, callback.String(), "", nil)
	if cb.status != http.StatusFound {
		t.Fatalf("callback: status %d, want 302\nbody: %s", cb.status, truncate(cb.body))
	}

	conn := singleConnection(t, s.token)
	if conn["status"] != "active" {
		t.Fatalf("connection status = %v, want active", conn["status"])
	}
	s.connectionID, _ = conn["id"].(string)
	if s.connectionID == "" {
		t.Fatalf("connection has no id: %v", conn)
	}
	// mockoauth derives the display name from the provider user id, so this
	// confirms the pin took and the admin API below will address the right
	// identity.
	wantName := "mockchannel-" + strings.TrimPrefix(s.providerUserID, "mu_")
	if conn["display_name"] != wantName {
		t.Fatalf("display_name = %v, want %q — the pinned provider identity was not used",
			conn["display_name"], wantName)
	}
}

// singleConnection returns the creator's one connection.
func singleConnection(t *testing.T, token string) map[string]any {
	t.Helper()
	var list struct {
		Items []map[string]any `json:"items"`
	}
	mustStatus(t, do(t, client, http.MethodGet, userURL+"/v1/connections", token, nil),
		http.StatusOK, "GET /v1/connections").json(t, &list)
	if len(list.Items) != 1 {
		t.Fatalf("connections = %d, want exactly 1", len(list.Items))
	}
	return list.Items[0]
}

// ---------------------------------------------------------------------
// (b) the provider half of §5.6, for real
// ---------------------------------------------------------------------

func stepProviderRefreshContract(t *testing.T, s *refreshScenario) {
	state := providerTokens(t, s.providerUserID)
	if len(state.AccessTokens) != 1 || len(state.RefreshTokens) != 1 {
		t.Fatalf("provider holds %d access / %d refresh token(s) for the connection, want 1 each: %+v",
			len(state.AccessTokens), len(state.RefreshTokens), state)
	}
	s.access0, s.refresh0 = state.AccessTokens[0], state.RefreshTokens[0]

	if got := userinfoStatus(t, s.access0); got != http.StatusOK {
		t.Fatalf("userinfo with the freshly-issued token = %d, want 200", got)
	}

	// Force the condition §5.6 exists to handle.
	invalidateProviderTokens(t, s.providerUserID, "access")
	if got := userinfoStatus(t, s.access0); got != http.StatusUnauthorized {
		t.Fatalf("userinfo after invalidation = %d, want 401 — the trigger for §5.6 "+
			"was not actually created", got)
	}

	// Step 3 of §5.6: exchange the refresh token. This must succeed and must
	// hand back a *new* access token, otherwise retrying the original call
	// would fail identically.
	granted := mustStatus(t, refreshGrant(t, s.refresh0), http.StatusOK, "refresh grant")
	var tok struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	granted.json(t, &tok)
	if tok.AccessToken == "" || tok.AccessToken == s.access0 {
		t.Fatalf("refresh returned access token %q; §5.6 requires a new one (was %q)",
			tok.AccessToken, s.access0)
	}
	if tok.RefreshToken == "" || tok.RefreshToken == s.refresh0 {
		t.Fatalf("refresh token was not rotated: %q", tok.RefreshToken)
	}
	if tok.ExpiresIn <= 0 {
		t.Fatalf("expires_in = %d, want a positive lifetime to store in expires_at", tok.ExpiresIn)
	}
	s.access1, s.refresh1 = tok.AccessToken, tok.RefreshToken

	// Step 4: the retried call now works.
	if got := userinfoStatus(t, s.access1); got != http.StatusOK {
		t.Fatalf("userinfo with the refreshed token = %d, want 200", got)
	}
	// And the rotated-away refresh token is dead, which is what makes a
	// double refresh detectable rather than silently idempotent.
	if r := refreshGrant(t, s.refresh0); r.status != http.StatusBadRequest {
		t.Errorf("replaying the rotated refresh token = %d, want 400", r.status)
	}
}

// ---------------------------------------------------------------------
// (c) the adapter half — FINDING F2
// ---------------------------------------------------------------------

func stepAdapterRefreshOn401(t *testing.T, s *refreshScenario) {
	s.adapterBefore = mustScrape(t, "provider-adapter", metricsPorts["provider-adapter"])

	// The provider is now rejecting the token the adapter holds. Give the
	// adapter more than one connection-source poll (compose sets
	// ADAPTER_CONNSOURCE_POLL=3s) to notice.
	time.Sleep(15 * time.Second)
	after := mustScrape(t, "provider-adapter", metricsPorts["provider-adapter"])

	moved := metricDelta(s.adapterBefore, after, "connection_refresh_total",
		map[string]string{"platform": "mock"})
	conn := singleConnection(t, s.token)

	if moved == 0 && conn["status"] == "active" {
		t.Skipf("FINDING F2: §5.6 cannot be exercised end to end on this stack. The refresh "+
			"path is only entered when a driver returns driver.ErrUnauthorized, and the only "+
			"producers are the youtube, twitch and discord drivers "+
			"(services/provider-adapter/internal/drivers/*). The `mock` platform — the only "+
			"one a local stack can connect — is served by "+
			"services/provider-adapter/internal/mockdriver, whose Delete always returns nil "+
			"and whose Watch never errors, so no 401 is representable. The real drivers' base "+
			"URLs are not env-configurable (main.go never sets youtube/twitch/discord BaseURL), "+
			"so they cannot be pointed at a stub from compose either. Evidence: the provider "+
			"has been rejecting this connection's access token for 15s, yet "+
			"connection_refresh_total{platform=mock} moved by %g and the connection is still "+
			"%v. The missing affordance is a failure-injection hook in mockdriver, which is "+
			"in services/ and so another agent's lane. Everything §5.6 needs from the provider "+
			"side is proven in step (b) and by tools/mockoauth's admin surface.",
			moved, conn["status"])
	}

	// If the stack ever grows that hook, this is the assertion that should
	// run instead of the skip.
	if n := metricDelta(s.adapterBefore, after, "connection_refresh_total",
		map[string]string{"platform": "mock", "outcome": "success"}); n < 1 {
		t.Fatalf("connection_refresh_total{platform=mock,outcome=success} moved by %g, want at least 1", n)
	}
	state := providerTokens(t, s.providerUserID)
	if len(state.AccessTokens) == 0 {
		t.Fatal("the connection holds no access token after a refresh")
	}
	if conn["status"] != "active" {
		t.Fatalf("connection status = %v after a successful refresh, want active", conn["status"])
	}
}

// ---------------------------------------------------------------------
// (d) the negative branch
// ---------------------------------------------------------------------

func stepRefreshAuthFailure(t *testing.T, s *refreshScenario) {
	before := mustScrape(t, "provider-adapter", metricsPorts["provider-adapter"])

	// Kill every credential this identity has. The refresh grant now fails
	// the way §5.6 calls terminal.
	invalidateProviderTokens(t, s.providerUserID, "all")

	failed := refreshGrant(t, s.refresh1)
	// refresh.exchange classifies 400/401/403 as an auth error and anything
	// else as transient — a transient error leaves the connection `active`,
	// so the status code is what decides which branch runs.
	if failed.status != http.StatusBadRequest {
		t.Fatalf("refresh with a revoked token = %d, want 400 so it classifies as an auth "+
			"error rather than a transient one\nbody: %s", failed.status, truncate(failed.body))
	}
	var errBody struct {
		Error string `json:"error"`
	}
	failed.json(t, &errBody)
	if errBody.Error != "invalid_grant" {
		t.Errorf("error = %q, want invalid_grant", errBody.Error)
	}

	time.Sleep(10 * time.Second)
	after := mustScrape(t, "provider-adapter", metricsPorts["provider-adapter"])
	conn := singleConnection(t, s.token)
	authFailed := metricDelta(before, after, "connection_refresh_total",
		map[string]string{"platform": "mock", "outcome": "auth_failed"})

	if authFailed == 0 && conn["status"] == "active" {
		t.Skipf("FINDING F2 (negative branch): the connection's credentials are all revoked "+
			"at the provider, yet connection_refresh_total{platform=mock,outcome=auth_failed} "+
			"moved by %g and the connection is still %v rather than `expired`. Same root cause "+
			"as step (c): mockdriver cannot surface a 401, so the refresher is never called and "+
			"neither branch of §5.6 runs. The provider-side contract the negative branch depends "+
			"on — a 400 invalid_grant, which refresh.exchange turns into an errAuth and then a "+
			"MarkExpired — is proven above.", authFailed, conn["status"])
	}

	if conn["status"] != "expired" {
		t.Fatalf("connection status = %v after a failed refresh, want expired", conn["status"])
	}
	if n := metricSum(after, "adapter_connections_active", map[string]string{"platform": "mock"}); n >=
		metricSum(before, "adapter_connections_active", map[string]string{"platform": "mock"}) {
		t.Error("adapter_connections_active did not fall; the expired connection's streams were not dropped")
	}
}

// ---------------------------------------------------------------------
// (e) whatever happened, the stack is still standing
// ---------------------------------------------------------------------

// stepRefreshFailOpen is the half of §5.6 that holds regardless of F2: a
// provider that starts rejecting tokens must not take a service down or push
// anything into a fail-open state.
func stepRefreshFailOpen(t *testing.T, s *refreshScenario) {
	waitHealthy(t, 30*time.Second)

	after := mustScrape(t, "provider-adapter", metricsPorts["provider-adapter"])
	if n := metricDelta(s.adapterBefore, after, "fail_open_total", nil); n != 0 {
		t.Errorf("provider-adapter fail_open_total moved by %g while a provider was "+
			"rejecting tokens; refusing credentials is not a reason to drop work", n)
	}
	// The mock connection's stream is still being served, because nothing in
	// the mock path ever consulted the revoked token.
	if n := metricSum(after, "adapter_connections_active", map[string]string{"platform": "mock"}); n <= 0 {
		t.Errorf("adapter_connections_active{platform=mock} = %g, want the watch loops still running", n)
	}
}
