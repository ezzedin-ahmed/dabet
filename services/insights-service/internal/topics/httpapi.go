package topics

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"dabet/pkg/httpx"
)

// API serves the topics endpoints (§8.8) on the shared httpx conventions:
// error envelope, JWT auth, ownership-as-404.
type API struct {
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// NewAPI builds the handler set. now is injectable for tests.
func NewAPI(store Store, log *slog.Logger, now func() time.Time) *API {
	if now == nil {
		now = time.Now
	}
	return &API{store: store, log: log, now: now}
}

// Register mounts the topics routes on mux behind JWT auth. There are
// deliberately no POST routes and no sample/message endpoints (§8.8).
func Register(mux *http.ServeMux, verifier *httpx.Verifier, store Store, log *slog.Logger) {
	a := NewAPI(store, log, nil)
	auth := httpx.Auth(verifier)
	mux.Handle("GET /v1/topics", auth(http.HandlerFunc(a.List)))
	mux.Handle("GET /v1/topics/{id}", auth(http.HandlerFunc(a.Get)))
	mux.Handle("GET /v1/topics/{id}/themes", auth(http.HandlerFunc(a.Themes)))
}

type itemsResponse struct {
	Items []Topic `json:"items"`
}

// window parses from/to/granularity with the §8.8 defaults: the last 24
// hours, hourly buckets. ok=false means the error envelope was written.
func (a *API) window(w http.ResponseWriter, r *http.Request, withGranularity bool) (from, to time.Time, granularity string, ok bool) {
	q := r.URL.Query()
	to = a.now().UTC()
	if s := q.Get("to"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "to must be RFC 3339", nil)
			return
		}
		to = t.UTC()
	}
	from = to.Add(-DefaultWindow)
	if s := q.Get("from"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "from must be RFC 3339", nil)
			return
		}
		from = t.UTC()
	}
	if !from.Before(to) {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "from must be before to", nil)
		return
	}
	granularity = GranularityHour
	if withGranularity {
		if g := q.Get("granularity"); g != "" {
			if g != GranularityHour && g != GranularityDay && g != GranularityMonth {
				httpx.WriteError(w, r, httpx.CodeValidationFailed, "granularity must be hour, day, or month", nil)
				return
			}
			granularity = g
		}
	}
	return from, to, granularity, true
}

// assemble folds series rows (grouped by id, buckets ascending) into Topic
// items labelled from meta, ordered by volume over the window, largest
// first. Labelled entries with no volume in the window trail with empty
// series; counted entries missing a label (not yet labelled by
// clusters-job) still appear.
func assemble(rows []SeriesRow, meta []Meta) []Topic {
	byID := make(map[string]*Topic)
	order := make([]string, 0)
	get := func(id string) *Topic {
		t, ok := byID[id]
		if !ok {
			t = &Topic{ID: id, Series: []Bucket{}}
			byID[id] = t
			order = append(order, id)
		}
		return t
	}
	for _, m := range meta {
		t := get(m.ID)
		t.Label, t.Description = m.Label, m.Description
	}
	for _, r := range rows {
		t := get(r.ID)
		t.MessageCount += r.Count
		t.Series = append(t.Series, Bucket{Bucket: r.Bucket, Count: r.Count})
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := byID[order[i]], byID[order[j]]
		if a.MessageCount != b.MessageCount {
			return a.MessageCount > b.MessageCount
		}
		return a.ID < b.ID
	})
	out := make([]Topic, len(order))
	for i, id := range order {
		out[i] = *byID[id]
	}
	return out
}

func (a *API) storeError(w http.ResponseWriter, r *http.Request, err error) {
	// P4: the error may name tables, never message content. Log and return
	// the unavailable envelope — ClickHouse is the only failure mode here.
	a.log.Error("topics store error", "error", err.Error())
	httpx.WriteError(w, r, httpx.CodeUnavailable, "topic store unavailable", nil)
}

// List is GET /v1/topics?content_id=&from=&to=&granularity= — all topics
// for the authenticated creator, ordered by volume over the window.
func (a *API) List(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	from, to, granularity, ok := a.window(w, r, true)
	if !ok {
		return
	}
	ctx := r.Context()
	rows, err := a.store.Series(ctx, SeriesQuery{
		CreatorID:   creatorID,
		ContentID:   r.URL.Query().Get("content_id"),
		From:        from,
		To:          to,
		Granularity: granularity,
	})
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	meta, err := a.store.Meta(ctx, creatorID, ZeroUUID)
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, itemsResponse{Items: assemble(rows, meta)})
}

// Get is GET /v1/topics/{id}. Another creator's topic — or a nonexistent
// one; the two are indistinguishable by design — is 404.
func (a *API) Get(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	topicID := r.PathValue("id")
	from, to, granularity, ok := a.window(w, r, true)
	if !ok {
		return
	}
	ctx := r.Context()
	meta, found, err := a.getOwned(ctx, w, r, creatorID, topicID)
	if err != nil || !found {
		return
	}
	rows, err := a.store.Series(ctx, SeriesQuery{
		CreatorID:   creatorID,
		TopicID:     topicID,
		From:        from,
		To:          to,
		Granularity: granularity,
	})
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	items := assemble(rows, []Meta{meta})
	httpx.WriteJSON(w, http.StatusOK, items[0])
}

// Themes is GET /v1/topics/{id}/themes?from=&to= — the topic's themes with
// hourly series, ordered by volume over the window.
func (a *API) Themes(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	topicID := r.PathValue("id")
	from, to, _, ok := a.window(w, r, false)
	if !ok {
		return
	}
	ctx := r.Context()
	if _, found, err := a.getOwned(ctx, w, r, creatorID, topicID); err != nil || !found {
		return
	}
	rows, err := a.store.Series(ctx, SeriesQuery{
		CreatorID:   creatorID,
		TopicID:     topicID,
		ByTheme:     true,
		From:        from,
		To:          to,
		Granularity: GranularityHour,
	})
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	meta, err := a.store.Meta(ctx, creatorID, topicID)
	if err != nil {
		a.storeError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, itemsResponse{Items: assemble(rows, meta)})
}

// getOwned resolves a creator-scoped topic, writing 404 or 503 on the way
// out. found=false or err!=nil means a response was already written.
func (a *API) getOwned(ctx context.Context, w http.ResponseWriter, r *http.Request, creatorID, topicID string) (Meta, bool, error) {
	meta, found, err := a.store.Get(ctx, creatorID, topicID)
	if err != nil {
		a.storeError(w, r, err)
		return Meta{}, false, err
	}
	if !found {
		httpx.WriteError(w, r, httpx.CodeNotFound, "topic not found", nil)
		return Meta{}, false, nil
	}
	return meta, true, nil
}
