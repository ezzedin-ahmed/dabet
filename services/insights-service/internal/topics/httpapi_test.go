package topics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"dabet/pkg/httpx"
)

const (
	secret   = "test-secret"
	creatorA = "9d4ecafe-0000-0000-0000-00000000000a"
	creatorB = "9d4ecafe-0000-0000-0000-00000000000b"
	topic1   = "10000000-0000-0000-0000-000000000001"
	topic2   = "10000000-0000-0000-0000-000000000002"
	theme1   = "20000000-0000-0000-0000-000000000001"
)

var now = time.Date(2026, 8, 19, 15, 30, 0, 0, time.UTC)

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

// fakeStore serves canned rows and records the queries it saw.
type fakeStore struct {
	series  []SeriesRow
	topics  map[string][]Meta // creator -> topics (parent zero)
	themes  map[string][]Meta // creator|topic -> themes
	err     error
	queries []SeriesQuery
}

func (f *fakeStore) Series(_ context.Context, q SeriesQuery) ([]SeriesRow, error) {
	f.queries = append(f.queries, q)
	if f.err != nil {
		return nil, f.err
	}
	var out []SeriesRow
	for _, r := range f.series {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeStore) Meta(_ context.Context, creatorID, parentID string) ([]Meta, error) {
	if f.err != nil {
		return nil, f.err
	}
	if parentID == ZeroUUID {
		return f.topics[creatorID], nil
	}
	return f.themes[creatorID+"|"+parentID], nil
}

func (f *fakeStore) Get(_ context.Context, creatorID, topicID string) (Meta, bool, error) {
	if f.err != nil {
		return Meta{}, false, f.err
	}
	for _, m := range f.topics[creatorID] {
		if m.ID == topicID {
			return m, true, nil
		}
	}
	return Meta{}, false, nil
}

// authWrap applies the same httpx.Auth middleware Register uses.
func authWrap(t *testing.T, h http.Handler) http.Handler {
	t.Helper()
	return httpx.Auth([]byte(secret))(h)
}

// get performs an authed GET against a mux built around the fixed clock
// (Register wires the real clock; tests need a pinned now for the window
// defaults).
func get(t *testing.T, store Store, creatorID, path string) *httptest.ResponseRecorder {
	t.Helper()
	a := NewAPI(store, slog.New(slog.NewTextHandler(io.Discard, nil)), func() time.Time { return now })
	mux := http.NewServeMux()
	mux.Handle("GET /v1/topics", http.HandlerFunc(a.List))
	mux.Handle("GET /v1/topics/{id}", http.HandlerFunc(a.Get))
	mux.Handle("GET /v1/topics/{id}/themes", http.HandlerFunc(a.Themes))
	authed := authWrap(t, mux)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token(t, creatorID))
	w := httptest.NewRecorder()
	authed.ServeHTTP(w, req)
	return w
}

func decodeItems(t *testing.T, w *httptest.ResponseRecorder) []Topic {
	t.Helper()
	var resp struct {
		Items []Topic `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad response %s: %v", w.Body.String(), err)
	}
	return resp.Items
}

func TestListDefaultsWindowAndGranularity(t *testing.T) {
	store := &fakeStore{topics: map[string][]Meta{}}
	w := get(t, store, creatorA, "/v1/topics")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	if len(store.queries) != 1 {
		t.Fatalf("expected 1 series query, got %d", len(store.queries))
	}
	q := store.queries[0]
	if q.Granularity != GranularityHour {
		t.Fatalf("default granularity = %q, want hour", q.Granularity)
	}
	if !q.To.Equal(now) || !q.From.Equal(now.Add(-24*time.Hour)) {
		t.Fatalf("default window = [%v, %v], want last 24h ending now", q.From, q.To)
	}
	if q.CreatorID != creatorA {
		t.Fatalf("query not creator-scoped: %q", q.CreatorID)
	}
}

func TestListParsesParams(t *testing.T) {
	store := &fakeStore{topics: map[string][]Meta{}}
	w := get(t, store, creatorA, "/v1/topics?content_id=ct-9&from=2026-08-01T00:00:00Z&to=2026-08-02T00:00:00Z&granularity=day")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	q := store.queries[0]
	if q.ContentID != "ct-9" || q.Granularity != GranularityDay {
		t.Fatalf("query = %+v", q)
	}
	if !q.From.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) || !q.To.Equal(time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("window = [%v, %v]", q.From, q.To)
	}
}

func TestListRejectsBadParams(t *testing.T) {
	store := &fakeStore{topics: map[string][]Meta{}}
	for _, path := range []string{
		"/v1/topics?granularity=minute",
		"/v1/topics?from=yesterday",
		"/v1/topics?from=2026-08-02T00:00:00Z&to=2026-08-01T00:00:00Z",
	} {
		if w := get(t, store, creatorA, path); w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, w.Code)
		}
	}
}

func TestListOrdersByVolumeAndLabels(t *testing.T) {
	store := &fakeStore{
		series: []SeriesRow{
			{ID: topic1, Bucket: now.Add(-2 * time.Hour), Count: 87},
			{ID: topic1, Bucket: now.Add(-time.Hour), Count: 13},
			{ID: topic2, Bucket: now.Add(-time.Hour), Count: 1000},
		},
		topics: map[string][]Meta{creatorA: {
			{ID: topic1, Label: "Ticket resale", Description: "Tickets."},
			{ID: topic2, Label: "Match thread", Description: "The match."},
		}},
	}
	w := get(t, store, creatorA, "/v1/topics")
	items := decodeItems(t, w)
	if len(items) != 2 {
		t.Fatalf("items = %+v", items)
	}
	if items[0].ID != topic2 || items[0].MessageCount != 1000 {
		t.Fatalf("not ordered by volume: %+v", items)
	}
	if items[1].ID != topic1 || items[1].MessageCount != 100 || items[1].Label != "Ticket resale" {
		t.Fatalf("second item wrong: %+v", items[1])
	}
	if len(items[1].Series) != 2 || items[1].Series[0].Count != 87 {
		t.Fatalf("series wrong: %+v", items[1].Series)
	}
}

func TestListIncludesQuietLabelledTopics(t *testing.T) {
	store := &fakeStore{
		series: []SeriesRow{{ID: topic1, Bucket: now.Add(-time.Hour), Count: 5}},
		topics: map[string][]Meta{creatorA: {
			{ID: topic1, Label: "Busy"},
			{ID: topic2, Label: "Quiet"},
		}},
	}
	items := decodeItems(t, get(t, store, creatorA, "/v1/topics"))
	if len(items) != 2 || items[1].ID != topic2 || items[1].MessageCount != 0 || len(items[1].Series) != 0 {
		t.Fatalf("quiet topic handling wrong: %+v", items)
	}
}

func TestGetTopic(t *testing.T) {
	store := &fakeStore{
		series: []SeriesRow{{ID: topic1, Bucket: now.Add(-time.Hour), Count: 42}},
		topics: map[string][]Meta{creatorA: {{ID: topic1, Label: "Ticket resale", Description: "Tickets."}}},
	}
	w := get(t, store, creatorA, "/v1/topics/"+topic1)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	var got Topic
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != topic1 || got.Label != "Ticket resale" || got.MessageCount != 42 {
		t.Fatalf("topic = %+v", got)
	}
	q := store.queries[0]
	if q.TopicID != topic1 || q.ByTheme {
		t.Fatalf("series query = %+v", q)
	}
}

func TestGetTopicOwnershipIs404(t *testing.T) {
	store := &fakeStore{
		topics: map[string][]Meta{creatorA: {{ID: topic1, Label: "Mine"}}},
	}
	// creatorB asks for creatorA's topic: indistinguishable from absent.
	w := get(t, store, creatorB, "/v1/topics/"+topic1)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", w.Code, w.Body.String())
	}
	if len(store.queries) != 0 {
		t.Fatal("must not query series for a topic the creator does not own")
	}
}

func TestThemes(t *testing.T) {
	store := &fakeStore{
		series: []SeriesRow{
			{ID: theme1, Bucket: now.Add(-time.Hour), Count: 7},
		},
		topics: map[string][]Meta{creatorA: {{ID: topic1, Label: "Topic"}}},
		themes: map[string][]Meta{creatorA + "|" + topic1: {{ID: theme1, Label: "A theme", Description: "Strand."}}},
	}
	w := get(t, store, creatorA, "/v1/topics/"+topic1+"/themes?from=2026-08-19T00:00:00Z&to=2026-08-19T15:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	items := decodeItems(t, w)
	if len(items) != 1 || items[0].ID != theme1 || items[0].Label != "A theme" || items[0].MessageCount != 7 {
		t.Fatalf("themes = %+v", items)
	}
	q := store.queries[0]
	if !q.ByTheme || q.TopicID != topic1 || q.Granularity != GranularityHour {
		t.Fatalf("series query = %+v", q)
	}
	if !q.From.Equal(time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("from = %v", q.From)
	}
}

func TestThemesForUnownedTopicIs404(t *testing.T) {
	store := &fakeStore{topics: map[string][]Meta{}}
	if w := get(t, store, creatorB, "/v1/topics/"+topic1+"/themes"); w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestStoreDownIs503(t *testing.T) {
	store := &fakeStore{err: errors.New("clickhouse down")}
	if w := get(t, store, creatorA, "/v1/topics"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestUnauthenticatedIs401(t *testing.T) {
	store := &fakeStore{topics: map[string][]Meta{}}
	a := NewAPI(store, slog.New(slog.NewTextHandler(io.Discard, nil)), func() time.Time { return now })
	mux := http.NewServeMux()
	mux.Handle("GET /v1/topics", http.HandlerFunc(a.List))
	authed := authWrap(t, mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	w := httptest.NewRecorder()
	authed.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestBucketExpr(t *testing.T) {
	cases := map[string]string{
		GranularityHour:  "bucket_hour",
		GranularityDay:   "toStartOfDay(bucket_hour)",
		GranularityMonth: "toStartOfMonth(bucket_hour)",
	}
	for g, want := range cases {
		got, err := bucketExpr(g)
		if err != nil || got != want {
			t.Fatalf("bucketExpr(%q) = %q, %v; want %q", g, got, err, want)
		}
	}
	if _, err := bucketExpr("minute"); err == nil {
		t.Fatal("bucketExpr must reject unknown granularities")
	}
}
