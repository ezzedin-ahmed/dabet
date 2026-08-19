// Package api serves the review queue of docs §7.6: GET /v1/reviews and
// POST /v1/reviews on the shared httpx conventions. There is no message
// database — a creator's queue is a position in their flagged.v1
// partition, and the only persisted state is the review_cursors row.
//
// # Cursor and window design (documented, §7.6 leaves it open)
//
// The DB cursor (review_cursors.next_offset) is the front of the queue:
// the first partition offset not yet reviewed. Reads NEVER move it.
//
// GET scans forward from the front (or, when the request carries a
// pagination cursor, from that cursor's end — read-ahead paging), collects
// up to limit events with this creator_id AND action=review, skipping
// other creators' interleaved events and auto_delete events, and returns
// them as PendingReview items. next_cursor is an opaque base64 token
// encoding {p: partition, b: window base offset, e: window end offset,
// exclusive}. e is the offset immediately after the last item returned —
// the scan window end. next_cursor is absent when the scan reached the
// high watermark (end of the queue at this moment; §4.1 notes it may
// become non-empty again later).
//
// POST re-seeks to the front and re-reads the window to correlate
// decisions with events. The request body optionally carries the GET's
// next_cursor back in a "cursor" field (an additive, documented extension
// of the §7.6 body):
//
//   - cursor present and its base b equals the current front: the window
//     is pinned to exactly [b, e) — the page the GET returned. Decisions
//     are matched inside it, deletions.v1 is produced for every matched
//     flagged:true, and the cursor advances to e. Items in the window
//     with no decision are implicitly kept (review is sequential, §7.6.1).
//   - cursor absent (the GET page ended at the high watermark and had no
//     next_cursor) or stale (b no longer the front — a replay after a
//     previous advance): the minimal window is derived instead — scan
//     forward from the front over at most httpx.MaxLimit matching events
//     (no GET page can be larger), match decisions, and advance to just
//     past the LAST matched event only. Undecided trailing events stay
//     queued. A replayed batch matches nothing (its events are behind the
//     front) and is a no-op.
//
// In both shapes the produce-and-advance runs inside one transaction on
// the cursor row (store.Advance), decisions matching nothing are reported
// in "ignored" rather than failing the batch, and a POST that matches
// nothing does not move the cursor.
//
// # New creators and lost windows
//
// A creator with no cursor row starts at the partition's earliest
// retained offset (documented choice: the whole retained backlog — up to
// 7 days — is reviewable; starting at the high watermark would silently
// discard it). If a stored cursor has fallen below the earliest retained
// offset, the skipped window is already lost (§7.6.3): the cursor snaps
// forward to the earliest offset and review_windows_lost_total counts it.
// The same recovery applies if the topic's partition count changed and
// the creator now maps to a different partition.
//
// P4: message text appears ONLY in API response bodies — never in logs,
// metrics, or error envelopes. Ownership: the JWT subject is the only
// creator whose queue is reachable; there is no cross-creator parameter
// anywhere on this surface.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"dabet/pkg/contracts"
	"dabet/pkg/httpx"

	"dabet/services/review-service/internal/metrics"
	"dabet/services/review-service/internal/partition"
	"dabet/services/review-service/internal/queue"
	"dabet/services/review-service/internal/store"
)

// Producer publishes deletions.v1; satisfied by *kafkax.Producer.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
}

// PendingReview is the §7.6 queue item.
type PendingReview struct {
	MessageID string             `json:"message_id"`
	ContentID string             `json:"content_id"`
	Text      string             `json:"text"`
	Detector  contracts.Detector `json:"detector"`
	PolicyID  string             `json:"policy_id"`
	FlaggedAt time.Time          `json:"flagged_at"`
}

// cursorToken is the decoded next_cursor: partition, window base
// (inclusive) and window end (exclusive). Opaque to clients (§4.1).
type cursorToken struct {
	P int32 `json:"p"`
	B int64 `json:"b"`
	E int64 `json:"e"`
}

// API holds the handler dependencies.
type API struct {
	reader   queue.Reader
	cursors  store.Store
	producer Producer
	mapper   *partition.Mapper
	tracker  *metrics.LagTracker
	m        *metrics.Metrics
	log      *slog.Logger
	now      func() time.Time
	idem     *idemStore
}

// New builds the API. now is injectable for tests; nil means time.Now.
func New(reader queue.Reader, cursors store.Store, producer Producer, mapper *partition.Mapper, m *metrics.Metrics, tracker *metrics.LagTracker, log *slog.Logger, now func() time.Time) *API {
	if now == nil {
		now = time.Now
	}
	return &API{
		reader:   reader,
		cursors:  cursors,
		producer: producer,
		mapper:   mapper,
		tracker:  tracker,
		m:        m,
		log:      log,
		now:      now,
		idem:     newIdemStore(now),
	}
}

// Register mounts the review routes on mux behind JWT auth.
func (a *API) Register(mux *http.ServeMux, verifier *httpx.Verifier) {
	auth := httpx.Auth(verifier)
	mux.Handle("GET /v1/reviews", auth(http.HandlerFunc(a.list)))
	mux.Handle("POST /v1/reviews", auth(http.HandlerFunc(a.post)))
}

// queuePos is the resolved per-request queue position.
type queuePos struct {
	part     int32
	earliest int64
	high     int64
	front    int64 // review_cursors.next_offset after init / snap-forward
}

// resolve maps the creator to their partition, initialises the cursor if
// absent, and applies the §7.6.3 snap-forward. It never advances a live
// cursor past unreviewed retained events.
func (a *API) resolve(ctx context.Context, creatorID string) (queuePos, error) {
	parts, err := a.reader.Partitions(ctx)
	if err != nil {
		return queuePos{}, &depErr{err}
	}
	part, err := a.mapper.Partition(contracts.FlaggedKey(creatorID), parts)
	if err != nil {
		return queuePos{}, err
	}
	earliest, high, err := a.reader.Offsets(ctx, part)
	if err != nil {
		return queuePos{}, &depErr{err}
	}
	cur, err := a.cursors.GetOrInit(ctx, creatorID, part, earliest)
	if err != nil {
		return queuePos{}, &depErr{err}
	}

	if cur.Partition != part {
		// The topic's partition count changed under the stored cursor:
		// the old position is meaningless. Reinitialise on the new
		// partition and count the lost window (§7.6.3 recovery).
		a.m.WindowsLost.WithLabelValues(creatorID).Inc()
		a.log.Warn("review cursor partition changed, window lost",
			"creator_id", creatorID, "old_partition", cur.Partition, "new_partition", part)
		if err := a.cursors.Reset(ctx, creatorID, part, earliest); err != nil {
			return queuePos{}, &depErr{err}
		}
		cur.Partition, cur.NextOffset = part, earliest
	}

	if cur.NextOffset < earliest {
		// The cursor fell behind retention: that window is silently
		// lost (§7.6.3). Snap forward and count it.
		a.m.WindowsLost.WithLabelValues(creatorID).Inc()
		a.log.Warn("review cursor behind retention, window lost",
			"creator_id", creatorID, "next_offset", cur.NextOffset, "earliest", earliest,
			"offsets_lost", earliest-cur.NextOffset)
		if _, err := a.cursors.SetNextOffset(ctx, creatorID, cur.NextOffset, earliest); err != nil {
			return queuePos{}, &depErr{err}
		}
		cur.NextOffset = earliest
	}

	return queuePos{part: part, earliest: earliest, high: high, front: cur.NextOffset}, nil
}

// depErr marks a dependency failure -> 503 unavailable.
type depErr struct{ err error }

func (d *depErr) Error() string { return d.err.Error() }

// writeResolveError renders resolve/scan errors. P4: never any message
// content; the envelope carries generic text only.
func (a *API) writeResolveError(w http.ResponseWriter, r *http.Request, err error) {
	if _, ok := err.(*depErr); ok {
		a.log.Error("review dependency failure", "error", err.Error())
		httpx.WriteError(w, r, httpx.CodeUnavailable, "review queue temporarily unavailable", nil)
		return
	}
	a.log.Error("review internal failure", "error", err.Error())
	httpx.WriteError(w, r, httpx.CodeInternalError, "internal error", nil)
}

// pending is one matched queue event kept for the POST path.
type pending struct {
	offset int64
	item   PendingReview
}

// collect scans [from, end) — end < 0 meaning unbounded — and gathers up
// to max events for creatorID with action=review. It returns the items,
// the window end (offset after the last collected event, or the scan end
// when nothing was collected), and whether the scan exhausted the
// partition (reached the high watermark without hitting max or end).
func (a *API) collect(ctx context.Context, pos queuePos, from, end int64, max int, creatorID string) (items []pending, windowEnd int64, exhausted bool, err error) {
	windowEnd = from
	stopped := false
	scanErr := a.reader.Scan(ctx, pos.part, from, func(rec queue.Record) bool {
		if end >= 0 && rec.Offset >= end {
			stopped = true
			return false
		}
		var f contracts.Flagged
		if jerr := json.Unmarshal(rec.Value, &f); jerr != nil {
			// Malformed event: skip. Log offset only — never the payload (P4).
			a.log.Warn("skipping undecodable flagged event", "partition", pos.part, "offset", rec.Offset)
			return true
		}
		if f.CreatorID != creatorID || f.Action != contracts.ActionReview {
			return true
		}
		items = append(items, pending{offset: rec.Offset, item: PendingReview{
			MessageID: f.MessageID,
			ContentID: f.ContentID,
			Text:      f.Text,
			Detector:  f.Detector,
			PolicyID:  f.PolicyID,
			FlaggedAt: f.FlaggedAt,
		}})
		if len(items) >= max {
			stopped = true
			return false
		}
		return true
	})
	if scanErr != nil {
		return nil, 0, false, &depErr{scanErr}
	}
	if n := len(items); n > 0 {
		windowEnd = items[n-1].offset + 1
	}
	return items, windowEnd, !stopped, nil
}

// listResponse is the GET body per §4.1 pagination.
type listResponse struct {
	Items      []PendingReview `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	creatorID := httpx.CreatorIDFrom(ctx)

	limit, err := httpx.ParseLimit(r)
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, err.Error(), map[string]any{"field": "limit"})
		return
	}

	pos, err := a.resolve(ctx, creatorID)
	if err != nil {
		a.writeResolveError(w, r, err)
		return
	}

	scanFrom := pos.front
	paged := false
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		var ct cursorToken
		if derr := httpx.DecodeCursor(raw, &ct); derr != nil || ct.P != pos.part || ct.E < ct.B {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid cursor", map[string]any{"field": "cursor"})
			return
		}
		paged = true
		scanFrom = ct.E
		if scanFrom < pos.earliest {
			scanFrom = pos.earliest
		}
	}

	items, windowEnd, exhausted, err := a.collect(ctx, pos, scanFrom, -1, limit, creatorID)
	if err != nil {
		a.writeResolveError(w, r, err)
		return
	}

	if !paged {
		// Front-of-queue read: observe lag and pending for the metrics
		// sampler. Exact count when the scan saw the whole partition,
		// offset-span upper bound otherwise (documented estimate).
		pendingEstimate := float64(pos.high - pos.front)
		if exhausted {
			pendingEstimate = float64(len(items))
		}
		var front time.Time
		if len(items) > 0 {
			front = items[0].item.FlaggedAt
		}
		a.tracker.Observe(creatorID, front, len(items) > 0, pendingEstimate)
	}

	resp := listResponse{Items: make([]PendingReview, 0, len(items))}
	for _, it := range items {
		resp.Items = append(resp.Items, it.item)
	}
	if !exhausted && windowEnd < pos.high {
		token, terr := httpx.EncodeCursor(cursorToken{P: pos.part, B: scanFrom, E: windowEnd})
		if terr != nil {
			a.writeResolveError(w, r, terr)
			return
		}
		resp.NextCursor = token
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// decision is one POST decision. Flagged is a pointer so an absent field
// is rejected rather than silently meaning "keep".
type decision struct {
	MessageID string `json:"message_id"`
	Flagged   *bool  `json:"flagged"`
}

// postRequest is the POST body. Cursor optionally carries back the GET's
// next_cursor to pin the window (see the package comment).
type postRequest struct {
	Decisions []decision `json:"decisions"`
	Cursor    string     `json:"cursor,omitempty"`
}

// postResponse reports the batch outcome. Ignored lists the message_ids
// of decisions outside the current window, which are not failures (§7.6).
type postResponse struct {
	Applied int      `json:"applied"`
	Deleted int      `json:"deleted"`
	Ignored []string `json:"ignored"`
}

func (a *API) post(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	creatorID := httpx.CreatorIDFrom(ctx)

	var req postRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if verr := validateDecisions(req.Decisions); verr != "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, verr, map[string]any{"field": "decisions"})
		return
	}
	var pinned *cursorToken
	if req.Cursor != "" {
		var ct cursorToken
		if derr := httpx.DecodeCursor(req.Cursor, &ct); derr != nil || ct.E < ct.B {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid cursor", map[string]any{"field": "cursor"})
			return
		}
		pinned = &ct
	}

	idemKey := httpx.IdempotencyKey(r)
	if idemKey != "" {
		if status, body, ok := a.idem.Get(creatorID, idemKey); ok {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(status)
			_, _ = w.Write(body)
			return
		}
	}

	pos, err := a.resolve(ctx, creatorID)
	if err != nil {
		a.writeResolveError(w, r, err)
		return
	}

	// Window shape: pinned to [front, e) when the client returned the
	// GET's cursor for the current front; minimal window otherwise.
	scanEnd := int64(-1)
	maxMatch := httpx.MaxLimit
	if pinned != nil && pinned.P == pos.part && pinned.B == pos.front {
		scanEnd = pinned.E
		maxMatch = int(^uint(0) >> 1) // window is bounded by scanEnd, not by count
	}

	window, _, _, err := a.collect(ctx, pos, pos.front, scanEnd, maxMatch, creatorID)
	if err != nil {
		a.writeResolveError(w, r, err)
		return
	}

	byID := make(map[string]*pending, len(window))
	for i := range window {
		p := &window[i]
		if _, dup := byID[p.item.MessageID]; !dup {
			byID[p.item.MessageID] = p
		}
	}

	type match struct {
		p       *pending
		flagged bool
	}
	var (
		matches  []match
		ignored  = []string{}
		advance  = pos.front
		deletion = 0
	)
	for _, d := range req.Decisions {
		p, ok := byID[d.MessageID]
		if !ok {
			ignored = append(ignored, d.MessageID)
			continue
		}
		matches = append(matches, match{p: p, flagged: *d.Flagged})
		if p.offset+1 > advance {
			advance = p.offset + 1
		}
		if *d.Flagged {
			deletion++
		}
	}
	if scanEnd >= 0 {
		// Pinned window: advance past the whole page — undecided items
		// in it are implicitly kept (§7.6.1, review is sequential).
		advance = scanEnd
	}

	resp := postResponse{Applied: len(matches), Deleted: deletion, Ignored: ignored}
	if len(matches) == 0 {
		// Nothing in the window: replay or stale batch. No-op — the
		// cursor must not move on unmatched decisions.
		a.finishPost(w, r, creatorID, idemKey, resp)
		return
	}

	now := a.now()
	ok, err := a.cursors.Advance(ctx, creatorID, pos.front, advance, func(ctx context.Context) error {
		for _, mt := range matches {
			if !mt.flagged {
				continue
			}
			del := contracts.Deletion{
				MessageID: mt.p.item.MessageID,
				ContentID: mt.p.item.ContentID,
				CreatorID: creatorID,
				Reason:    mt.p.item.Detector,
				IssuedAt:  now,
			}
			val, merr := json.Marshal(del)
			if merr != nil {
				return merr
			}
			if perr := a.producer.Produce(ctx, contracts.TopicDeletions, contracts.DeletionsKey(del.ContentID), val); perr != nil {
				return perr
			}
		}
		return nil
	})
	if err != nil {
		a.log.Error("review decision apply failed", "creator_id", creatorID, "error", err.Error())
		httpx.WriteError(w, r, httpx.CodeUnavailable, "could not apply review decisions", nil)
		return
	}
	if !ok {
		// The cursor moved between resolve and Advance (concurrent
		// POST): treat this batch as out of window rather than failing.
		resp = postResponse{Applied: 0, Deleted: 0, Ignored: allIDs(req.Decisions)}
		a.finishPost(w, r, creatorID, idemKey, resp)
		return
	}

	a.tracker.Invalidate(creatorID, float64(pos.high-advance))
	a.log.Info("review decisions applied",
		"creator_id", creatorID,
		"applied", resp.Applied, "deleted", resp.Deleted, "ignored", len(resp.Ignored),
		"from_offset", pos.front, "to_offset", advance)
	a.finishPost(w, r, creatorID, idemKey, resp)
}

// finishPost renders the response and stores it for idempotent replay.
func (a *API) finishPost(w http.ResponseWriter, r *http.Request, creatorID, idemKey string, resp postResponse) {
	body, err := json.Marshal(resp)
	if err != nil {
		a.writeResolveError(w, r, err)
		return
	}
	if idemKey != "" {
		a.idem.Put(creatorID, idemKey, http.StatusOK, body)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func validateDecisions(ds []decision) string {
	if len(ds) == 0 {
		return "decisions must not be empty"
	}
	if len(ds) > httpx.MaxLimit {
		return "decisions exceeds the maximum batch of 200"
	}
	seen := make(map[string]struct{}, len(ds))
	for _, d := range ds {
		if d.MessageID == "" || len(d.MessageID) > 64 {
			return "each decision requires a message_id of at most 64 characters"
		}
		if d.Flagged == nil {
			return "each decision requires an explicit flagged value"
		}
		if _, dup := seen[d.MessageID]; dup {
			return "duplicate message_id in decisions"
		}
		seen[d.MessageID] = struct{}{}
	}
	return ""
}

func allIDs(ds []decision) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.MessageID)
	}
	return out
}
