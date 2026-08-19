package refresh

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/provider-adapter/internal/driver"
)

// fakeStore is an in-memory Store: one row, a real mutex standing in for
// the advisory lock, and commit/rollback semantics on the row copy.
type fakeStore struct {
	mu     sync.Mutex
	row    Row
	exists bool
	locks  int
}

type fakeOps struct {
	row    *Row // the transaction's working copy
	exists bool
}

func (s *fakeStore) Locked(ctx context.Context, connectionID string, fn func(ops RowOps) error) error {
	s.mu.Lock() // the advisory lock
	defer s.mu.Unlock()
	s.locks++
	work := s.row // transaction snapshot
	ops := &fakeOps{row: &work, exists: s.exists}
	if err := fn(ops); err != nil {
		return err // rollback: s.row untouched
	}
	s.row = work // commit
	return nil
}

func (o *fakeOps) Get(_ context.Context, id string) (Row, error) {
	if !o.exists || o.row.ID != id {
		return Row{}, ErrNotFound
	}
	return *o.row, nil
}

func (o *fakeOps) UpdateTokens(_ context.Context, _, accessToken, refreshToken string, expiresAt *time.Time) error {
	o.row.AccessToken = accessToken
	if refreshToken != "" {
		o.row.RefreshToken = refreshToken
	}
	o.row.ExpiresAt = expiresAt
	return nil
}

func (o *fakeOps) MarkExpired(_ context.Context, _ string) error {
	o.row.Status = "expired"
	return nil
}

// fakeTokenServer scripts the provider token endpoint.
type fakeTokenServer struct {
	server *httptest.Server
	mu     sync.Mutex
	status int
	form   url.Values
	calls  int
	resp   map[string]any
}

func newFakeTokenServer(t *testing.T) *fakeTokenServer {
	fs := &fakeTokenServer{status: http.StatusOK}
	fs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		fs.mu.Lock()
		fs.calls++
		fs.form = r.PostForm
		status, resp := fs.status, fs.resp
		fs.mu.Unlock()
		if resp == nil {
			resp = map[string]any{
				"access_token":  "new-access",
				"refresh_token": "new-refresh",
				"expires_in":    3600,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(fs.server.Close)
	return fs
}

func (fs *fakeTokenServer) callCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.calls
}

type refreshFixture struct {
	store   *fakeStore
	ts      *fakeTokenServer
	metric  *prometheus.CounterVec
	evicted []string
	updated []driver.Connection
	r       *Refresher
}

func newRefreshFixture(t *testing.T) *refreshFixture {
	f := &refreshFixture{
		store: &fakeStore{
			exists: true,
			row: Row{
				ID: "conn-1", Platform: "twitch", Status: "active",
				AccessToken: "old-access", RefreshToken: "old-refresh",
			},
		},
		ts: newFakeTokenServer(t),
		metric: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connection_refresh_total", Help: "test",
		}, []string{"platform", "outcome"}),
	}
	endpoints := map[string]Endpoint{
		"twitch": {TokenURL: f.ts.server.URL, ClientID: "cid", ClientSecret: "sec"},
	}
	f.r = New(f.store, endpoints, f.metric, slog.New(slog.DiscardHandler),
		func(id string) { f.evicted = append(f.evicted, id) },
		func(c driver.Connection) { f.updated = append(f.updated, c) },
	)
	return f
}

func (f *refreshFixture) conn() driver.Connection {
	return driver.Connection{ID: "conn-1", CreatorID: "cr-1", Platform: "twitch", AccessToken: "old-access"}
}

func (f *refreshFixture) outcomeCount(outcome string) float64 {
	return testutil.ToFloat64(f.metric.WithLabelValues("twitch", outcome))
}

func TestRefreshSuccessUpdatesRowAndReturnsFreshToken(t *testing.T) {
	f := newRefreshFixture(t)
	got, err := f.r.Refresh(context.Background(), f.conn())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got.AccessToken != "new-access" {
		t.Errorf("returned token = %q; want the refreshed one", got.AccessToken)
	}
	row := f.store.row
	if row.AccessToken != "new-access" || row.RefreshToken != "new-refresh" ||
		row.Status != "active" || row.ExpiresAt == nil {
		t.Errorf("stored row = %+v", row)
	}
	// The exchange used the refresh grant with the stored token.
	f.ts.mu.Lock()
	form := f.ts.form
	f.ts.mu.Unlock()
	if form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != "old-refresh" ||
		form.Get("client_id") != "cid" || form.Get("client_secret") != "sec" {
		t.Errorf("token form = %v", form)
	}
	if f.store.locks != 1 {
		t.Errorf("advisory lock taken %d times; want 1", f.store.locks)
	}
	if f.outcomeCount(OutcomeSuccess) != 1 {
		t.Error("success outcome not counted")
	}
	if len(f.updated) != 1 || f.updated[0].AccessToken != "new-access" {
		t.Errorf("source snapshot not updated: %v", f.updated)
	}
	if len(f.evicted) != 0 {
		t.Errorf("evicted on success: %v", f.evicted)
	}
}

func TestRefreshKeepsStoredRefreshTokenWhenOmitted(t *testing.T) {
	f := newRefreshFixture(t)
	f.ts.mu.Lock()
	f.ts.resp = map[string]any{"access_token": "new-access"} // no refresh_token
	f.ts.mu.Unlock()
	if _, err := f.r.Refresh(context.Background(), f.conn()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if f.store.row.RefreshToken != "old-refresh" {
		t.Errorf("stored refresh token = %q; want the old one kept", f.store.row.RefreshToken)
	}
}

func TestRefreshCoalescesWithConcurrentWorker(t *testing.T) {
	f := newRefreshFixture(t)
	// Another worker already refreshed: the stored token differs from the
	// one our caller saw the 401 with.
	f.store.row.AccessToken = "already-fresh"

	got, err := f.r.Refresh(context.Background(), f.conn())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got.AccessToken != "already-fresh" {
		t.Errorf("returned token = %q; want the concurrently refreshed one", got.AccessToken)
	}
	if f.ts.callCount() != 0 {
		t.Errorf("token endpoint called %d times; want 0 (re-read short-circuits)", f.ts.callCount())
	}
	if f.outcomeCount(OutcomeCoalesced) != 1 {
		t.Error("coalesced outcome not counted")
	}
}

func TestRefreshAuthErrorExpiresConnection(t *testing.T) {
	f := newRefreshFixture(t)
	f.ts.mu.Lock()
	f.ts.status = http.StatusBadRequest // invalid_grant
	f.ts.mu.Unlock()

	_, err := f.r.Refresh(context.Background(), f.conn())
	if err == nil {
		t.Fatal("Refresh succeeded on auth error")
	}
	if f.store.row.Status != "expired" {
		t.Errorf("row status = %q; want expired (§5.6)", f.store.row.Status)
	}
	if len(f.evicted) != 1 || f.evicted[0] != "conn-1" {
		t.Errorf("streams not dropped: evicted = %v", f.evicted)
	}
	if f.outcomeCount(OutcomeAuthFail) != 1 {
		t.Error("auth_failed outcome not counted")
	}
}

func TestRefreshTransportErrorKeepsConnectionActive(t *testing.T) {
	f := newRefreshFixture(t)
	f.ts.server.Close() // transport failure

	_, err := f.r.Refresh(context.Background(), f.conn())
	if err == nil {
		t.Fatal("Refresh succeeded on transport error")
	}
	if f.store.row.Status != "active" {
		t.Errorf("row status = %q; want still active (§5.6)", f.store.row.Status)
	}
	if f.store.row.AccessToken != "old-access" {
		t.Errorf("row token changed on rollback: %q", f.store.row.AccessToken)
	}
	if len(f.evicted) != 0 {
		t.Errorf("evicted on transport error: %v", f.evicted)
	}
	if f.outcomeCount(OutcomeTransient) != 1 {
		t.Error("transient_error outcome not counted")
	}
}

func TestRefreshServerErrorIsTransient(t *testing.T) {
	f := newRefreshFixture(t)
	f.ts.mu.Lock()
	f.ts.status = http.StatusInternalServerError
	f.ts.mu.Unlock()

	if _, err := f.r.Refresh(context.Background(), f.conn()); err == nil {
		t.Fatal("Refresh succeeded on 500")
	}
	if f.store.row.Status != "active" {
		t.Errorf("row status = %q; want still active", f.store.row.Status)
	}
	if f.outcomeCount(OutcomeTransient) != 1 {
		t.Error("transient_error outcome not counted")
	}
}

func TestRefreshWithoutRefreshTokenExpires(t *testing.T) {
	f := newRefreshFixture(t)
	f.store.row.RefreshToken = ""

	if _, err := f.r.Refresh(context.Background(), f.conn()); err == nil {
		t.Fatal("Refresh succeeded without a refresh token")
	}
	if f.store.row.Status != "expired" {
		t.Errorf("row status = %q; want expired", f.store.row.Status)
	}
	if f.ts.callCount() != 0 {
		t.Errorf("token endpoint called with no refresh token")
	}
}

func TestRefreshInactiveConnectionFails(t *testing.T) {
	f := newRefreshFixture(t)
	f.store.row.Status = "revoked"

	if _, err := f.r.Refresh(context.Background(), f.conn()); err == nil {
		t.Fatal("Refresh succeeded on a revoked connection")
	}
	if f.ts.callCount() != 0 {
		t.Error("token endpoint called for a revoked connection")
	}
	if f.store.row.Status != "revoked" {
		t.Errorf("row status = %q; want untouched", f.store.row.Status)
	}
}

func TestRefreshUnknownPlatformFails(t *testing.T) {
	f := newRefreshFixture(t)
	c := f.conn()
	c.Platform = "myspace"
	if _, err := f.r.Refresh(context.Background(), c); err == nil {
		t.Fatal("Refresh succeeded without endpoint config")
	}
	if f.store.locks != 0 {
		t.Error("lock taken without endpoint config")
	}
}

func TestEndpointsFromEnv(t *testing.T) {
	env := map[string]string{
		"OAUTH_TWITCH_TOKEN_URL":     "http://stub:9099/oauth/token",
		"OAUTH_TWITCH_CLIENT_ID":     "cid",
		"OAUTH_TWITCH_CLIENT_SECRET": "sec",
	}
	getenv := func(name, def string) string {
		if v, ok := env[name]; ok {
			return v
		}
		return def
	}
	eps := EndpointsFromEnv(getenv)
	if eps["twitch"].TokenURL != "http://stub:9099/oauth/token" || eps["twitch"].ClientID != "cid" {
		t.Errorf("twitch endpoint = %+v", eps["twitch"])
	}
	if eps["youtube"].TokenURL != "https://oauth2.googleapis.com/token" {
		t.Errorf("youtube default = %+v", eps["youtube"])
	}
	if _, ok := eps["mock"]; !ok {
		t.Error("mock endpoint missing")
	}
}
