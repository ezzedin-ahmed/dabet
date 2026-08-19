package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"

	"dabet/services/review-service/internal/metrics"
	"dabet/services/review-service/internal/partition"
	"dabet/services/review-service/internal/queue/queuetest"
	"dabet/services/review-service/internal/store/memstore"
)

const testSecret = "test-secret"

var testNow = time.Date(2026, 8, 19, 15, 0, 0, 0, time.UTC)

func token(t *testing.T, creatorID string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   creatorID,
		IssuedAt:  jwt.NewNumericDate(testNow),
		ExpiresAt: jwt.NewNumericDate(testNow.Add(15 * time.Minute)),
	})
	s, err := tok.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

type producedRec struct {
	topic string
	key   string
	value []byte
}

type fakeProducer struct {
	mu   sync.Mutex
	recs []producedRec
	fail error
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.recs = append(f.recs, producedRec{topic: topic, key: string(key), value: value})
	return nil
}

func (f *fakeProducer) deletions() []contracts.Deletion {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []contracts.Deletion
	for _, r := range f.recs {
		var d contracts.Deletion
		if err := json.Unmarshal(r.value, &d); err == nil {
			out = append(out, d)
		}
	}
	return out
}

type env struct {
	log    *queuetest.FakeLog
	mem    *memstore.Mem
	prod   *fakeProducer
	mux    *http.ServeMux
	m      *metrics.Metrics
	mapper *partition.Mapper
}

func setup(t *testing.T, partitions int32) *env {
	t.Helper()
	e := &env{
		log:    queuetest.NewFakeLog(partitions),
		mem:    memstore.New(),
		prod:   &fakeProducer{},
		mux:    http.NewServeMux(),
		mapper: partition.NewMapper(contracts.TopicFlagged),
	}
	reg := prometheus.NewRegistry()
	e.m = metrics.New(reg)
	tracker := metrics.NewLagTracker(e.m, func() time.Time { return testNow })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := New(e.log, e.mem, e.prod, e.mapper, e.m, tracker, logger, func() time.Time { return testNow })
	a.Register(e.mux, []byte(testSecret))
	return e
}

func (e *env) partitionFor(t *testing.T, creatorID string) int32 {
	t.Helper()
	n, _ := e.log.Partitions(context.Background())
	p, err := e.mapper.Partition(contracts.FlaggedKey(creatorID), n)
	if err != nil {
		t.Fatalf("partition for %s: %v", creatorID, err)
	}
	return p
}

// appendFlagged appends one flagged.v1 event to the creator's partition
// and returns its offset.
func (e *env) appendFlagged(t *testing.T, creatorID, msgID string, action contracts.Action, age time.Duration) int64 {
	t.Helper()
	f := contracts.Flagged{
		MessageID: msgID,
		ContentID: "ct_" + msgID,
		AuthorID:  "sd_author",
		CreatorID: creatorID,
		Text:      "offensive text " + msgID,
		Detector:  contracts.DetectorRestrictedContent,
		Action:    action,
		PolicyID:  "pol_7a13",
		FlaggedAt: testNow.Add(-age),
	}
	v, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal flagged: %v", err)
	}
	return e.log.Append(e.partitionFor(t, creatorID), contracts.FlaggedKey(creatorID), v)
}

func (e *env) get(t *testing.T, creatorID, query string) (int, listResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/reviews"+query, nil)
	req.Header.Set("Authorization", "Bearer "+token(t, creatorID))
	rr := httptest.NewRecorder()
	e.mux.ServeHTTP(rr, req)
	var resp listResponse
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode GET response: %v", err)
		}
	}
	return rr.Code, resp
}

func (e *env) post(t *testing.T, creatorID string, body any, idemKey string) (int, postResponse, string) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal POST body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/reviews", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token(t, creatorID))
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rr := httptest.NewRecorder()
	e.mux.ServeHTTP(rr, req)
	var resp postResponse
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode POST response: %v", err)
		}
	}
	return rr.Code, resp, rr.Body.String()
}

func decisionsBody(cursor string, ds ...map[string]any) map[string]any {
	body := map[string]any{"decisions": ds}
	if cursor != "" {
		body["cursor"] = cursor
	}
	return body
}

func d(msgID string, flagged bool) map[string]any {
	return map[string]any{"message_id": msgID, "flagged": flagged}
}

const (
	creatorA = "9d4ec8a1-93b8-4c58-bd21-0c8f8a2f9e11"
	creatorB = "1b7e2ea0-52ad-4f0e-9a3e-6f6f0d5a7c42"
)

// seedInterleaved appends, on a single partition, creatorA review events
// interleaved with creatorB events and creatorA auto_delete events.
func seedInterleaved(t *testing.T, e *env) (reviewIDs []string) {
	e.appendFlagged(t, creatorA, "a1", contracts.ActionReview, time.Hour)
	e.appendFlagged(t, creatorB, "b1", contracts.ActionReview, time.Hour)
	e.appendFlagged(t, creatorA, "a2", contracts.ActionAutoDelete, 50*time.Minute)
	e.appendFlagged(t, creatorA, "a3", contracts.ActionReview, 40*time.Minute)
	e.appendFlagged(t, creatorB, "b2", contracts.ActionAutoDelete, 30*time.Minute)
	e.appendFlagged(t, creatorA, "a4", contracts.ActionReview, 20*time.Minute)
	return []string{"a1", "a3", "a4"}
}

func itemIDs(items []PendingReview) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.MessageID)
	}
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGetFiltersInterleavedAndAutoDelete(t *testing.T) {
	e := setup(t, 1)
	want := seedInterleaved(t, e)

	code, resp := e.get(t, creatorA, "")
	if code != http.StatusOK {
		t.Fatalf("GET status %d", code)
	}
	if !eqStrings(itemIDs(resp.Items), want) {
		t.Errorf("items %v, want %v", itemIDs(resp.Items), want)
	}
	if resp.NextCursor != "" {
		t.Errorf("expected no next_cursor at high watermark, got %q", resp.NextCursor)
	}
	for _, it := range resp.Items {
		if it.Text == "" || it.Detector != contracts.DetectorRestrictedContent || it.PolicyID != "pol_7a13" || it.ContentID == "" {
			t.Errorf("incomplete PendingReview: %+v", it)
		}
	}

	// creatorB sees only their own queue; no cross-creator surface.
	code, respB := e.get(t, creatorB, "")
	if code != http.StatusOK {
		t.Fatalf("GET B status %d", code)
	}
	if !eqStrings(itemIDs(respB.Items), []string{"b1"}) {
		t.Errorf("creator B items %v, want [b1]", itemIDs(respB.Items))
	}
}

func TestGetUnauthenticated(t *testing.T) {
	e := setup(t, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/reviews", nil)
	rr := httptest.NewRecorder()
	e.mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", rr.Code)
	}
}

func TestGetLimitAndPaginationDoNotMoveCursor(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	code, page1 := e.get(t, creatorA, "?limit=2")
	if code != http.StatusOK {
		t.Fatalf("GET status %d", code)
	}
	if !eqStrings(itemIDs(page1.Items), []string{"a1", "a3"}) {
		t.Errorf("page1 %v", itemIDs(page1.Items))
	}
	if page1.NextCursor == "" {
		t.Fatal("expected next_cursor on page 1")
	}

	code, page2 := e.get(t, creatorA, "?limit=2&cursor="+page1.NextCursor)
	if code != http.StatusOK {
		t.Fatalf("GET page2 status %d", code)
	}
	if !eqStrings(itemIDs(page2.Items), []string{"a4"}) {
		t.Errorf("page2 %v, want [a4]", itemIDs(page2.Items))
	}
	if page2.NextCursor != "" {
		t.Errorf("expected no next_cursor on final page")
	}

	// Reads never advance the DB cursor.
	cur, ok := e.mem.Get(creatorA)
	if !ok {
		t.Fatal("cursor row not initialised on GET")
	}
	if cur.NextOffset != 0 {
		t.Errorf("cursor moved to %d by reads", cur.NextOffset)
	}

	// Repeat GET is identical (idempotent).
	_, again := e.get(t, creatorA, "?limit=2")
	if !eqStrings(itemIDs(again.Items), []string{"a1", "a3"}) {
		t.Errorf("repeat GET differs: %v", itemIDs(again.Items))
	}
}

func TestCursorInitStartsAtEarliestRetained(t *testing.T) {
	e := setup(t, 1)
	e.appendFlagged(t, creatorA, "old1", contracts.ActionReview, 2*time.Hour) // offset 0
	e.appendFlagged(t, creatorA, "old2", contracts.ActionReview, 2*time.Hour) // offset 1
	part := e.partitionFor(t, creatorA)
	e.log.Truncate(part, 2)                                                   // retention expired both
	e.appendFlagged(t, creatorA, "new1", contracts.ActionReview, time.Minute) // offset 2

	code, resp := e.get(t, creatorA, "")
	if code != http.StatusOK {
		t.Fatalf("GET status %d", code)
	}
	if !eqStrings(itemIDs(resp.Items), []string{"new1"}) {
		t.Errorf("items %v, want [new1]", itemIDs(resp.Items))
	}
	cur, ok := e.mem.Get(creatorA)
	if !ok || cur.NextOffset != 2 {
		t.Errorf("cursor should initialise at earliest retained offset 2, got %+v (ok=%v)", cur, ok)
	}
	// Initialisation at the earliest offset is not a lost window.
	if got := testutil.ToFloat64(e.m.WindowsLost.WithLabelValues(creatorA)); got != 0 {
		t.Errorf("windows_lost = %v on plain init", got)
	}
}

func TestPostAppliesDecisionsAndAdvancesWindow(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e) // creatorA review events at offsets 0, 3, 5

	_, page := e.get(t, creatorA, "?limit=2")
	if page.NextCursor == "" {
		t.Fatal("expected next_cursor")
	}

	code, resp, _ := e.post(t, creatorA, decisionsBody(page.NextCursor, d("a1", true), d("a3", false)), "")
	if code != http.StatusOK {
		t.Fatalf("POST status %d", code)
	}
	if resp.Applied != 2 || resp.Deleted != 1 || len(resp.Ignored) != 0 {
		t.Errorf("response %+v, want applied=2 deleted=1 ignored=[]", resp)
	}

	dels := e.prod.deletions()
	if len(dels) != 1 {
		t.Fatalf("produced %d deletions, want 1", len(dels))
	}
	del := dels[0]
	if del.MessageID != "a1" || del.ContentID != "ct_a1" || del.CreatorID != creatorA {
		t.Errorf("deletion %+v", del)
	}
	if del.Reason != contracts.DetectorRestrictedContent {
		t.Errorf("deletion reason %q, want detector value restricted_content", del.Reason)
	}
	if e.prod.recs[0].topic != contracts.TopicDeletions || e.prod.recs[0].key != "ct_a1" {
		t.Errorf("deletion produced to %s key %q, want deletions.v1 keyed by content_id", e.prod.recs[0].topic, e.prod.recs[0].key)
	}

	// Cursor advanced past the window: a1 (offset 0) .. a3 (offset 3) -> 4.
	cur, _ := e.mem.Get(creatorA)
	if cur.NextOffset != 4 {
		t.Errorf("cursor at %d, want 4 (past the reviewed window)", cur.NextOffset)
	}

	// Next page starts after the window.
	_, next := e.get(t, creatorA, "")
	if !eqStrings(itemIDs(next.Items), []string{"a4"}) {
		t.Errorf("post-review queue %v, want [a4]", itemIDs(next.Items))
	}
}

func TestPostReplaySameCursorIsNoop(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	_, page := e.get(t, creatorA, "?limit=2")
	body := decisionsBody(page.NextCursor, d("a1", true), d("a3", false))

	if code, resp, _ := e.post(t, creatorA, body, ""); code != http.StatusOK || resp.Applied != 2 {
		t.Fatalf("first POST: code %d resp %+v", code, resp)
	}
	curAfter, _ := e.mem.Get(creatorA)

	// Replay without Idempotency-Key: safe no-op.
	code, resp, _ := e.post(t, creatorA, body, "")
	if code != http.StatusOK {
		t.Fatalf("replay status %d", code)
	}
	if resp.Applied != 0 || resp.Deleted != 0 || !eqStrings(resp.Ignored, []string{"a1", "a3"}) {
		t.Errorf("replay response %+v, want everything ignored", resp)
	}
	if got := len(e.prod.deletions()); got != 1 {
		t.Errorf("replay produced deletions: total %d, want 1", got)
	}
	if cur, _ := e.mem.Get(creatorA); cur.NextOffset != curAfter.NextOffset {
		t.Errorf("replay moved cursor %d -> %d", curAfter.NextOffset, cur.NextOffset)
	}
}

func TestPostIdempotencyKeyReplaysStoredResponse(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	_, page := e.get(t, creatorA, "?limit=2")
	body := decisionsBody(page.NextCursor, d("a1", true), d("a3", false))

	code1, _, raw1 := e.post(t, creatorA, body, "key-1")
	if code1 != http.StatusOK {
		t.Fatalf("first POST status %d", code1)
	}
	code2, _, raw2 := e.post(t, creatorA, body, "key-1")
	if code2 != http.StatusOK {
		t.Fatalf("replay POST status %d", code2)
	}
	if raw1 != raw2 {
		t.Errorf("idempotent replay body differs:\n%s\n%s", raw1, raw2)
	}
	if got := len(e.prod.deletions()); got != 1 {
		t.Errorf("idempotent replay produced deletions: total %d, want 1", got)
	}
}

func TestPostOutOfWindowDecisionsIgnoredNotFailed(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	_, page := e.get(t, creatorA, "?limit=2")
	code, resp, _ := e.post(t, creatorA,
		decisionsBody(page.NextCursor, d("a1", true), d("a4", true), d("ghost", false)), "")
	if code != http.StatusOK {
		t.Fatalf("POST status %d, want 200 (ignored, not failed)", code)
	}
	// a4 exists but is beyond the pinned window [0,4); ghost never existed.
	if resp.Applied != 1 || resp.Deleted != 1 {
		t.Errorf("response %+v, want applied=1 deleted=1", resp)
	}
	if !eqStrings(resp.Ignored, []string{"a4", "ghost"}) {
		t.Errorf("ignored %v, want [a4 ghost]", resp.Ignored)
	}
	// a4 stays queued.
	_, next := e.get(t, creatorA, "")
	if !eqStrings(itemIDs(next.Items), []string{"a4"}) {
		t.Errorf("queue after POST %v, want [a4]", itemIDs(next.Items))
	}
}

func TestPostWithoutCursorUsesMinimalWindow(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e) // a1@0, a3@3, a4@5

	// The GET page reached the high watermark, so there is no
	// next_cursor; the POST derives the minimal window instead.
	_, page := e.get(t, creatorA, "")
	if page.NextCursor != "" {
		t.Fatal("precondition: no next_cursor expected")
	}
	code, resp, _ := e.post(t, creatorA, decisionsBody("", d("a1", false), d("a3", true)), "")
	if code != http.StatusOK {
		t.Fatalf("POST status %d", code)
	}
	if resp.Applied != 2 || resp.Deleted != 1 {
		t.Errorf("response %+v", resp)
	}
	// Advance stops just past the last matched event (a3 at offset 3);
	// the undecided a4 stays queued.
	cur, _ := e.mem.Get(creatorA)
	if cur.NextOffset != 4 {
		t.Errorf("cursor %d, want 4", cur.NextOffset)
	}
	_, next := e.get(t, creatorA, "")
	if !eqStrings(itemIDs(next.Items), []string{"a4"}) {
		t.Errorf("remaining queue %v, want [a4]", itemIDs(next.Items))
	}
}

func TestGetSnapsForwardOnLostWindow(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	// Establish the cursor at 0, then expire the first 4 offsets.
	if code, _ := e.get(t, creatorA, ""); code != http.StatusOK {
		t.Fatal("initial GET failed")
	}
	part := e.partitionFor(t, creatorA)
	e.log.Truncate(part, 4)

	code, resp := e.get(t, creatorA, "")
	if code != http.StatusOK {
		t.Fatalf("GET status %d", code)
	}
	if !eqStrings(itemIDs(resp.Items), []string{"a4"}) {
		t.Errorf("items after truncation %v, want [a4]", itemIDs(resp.Items))
	}
	cur, _ := e.mem.Get(creatorA)
	if cur.NextOffset != 4 {
		t.Errorf("cursor %d, want snapped to earliest 4", cur.NextOffset)
	}
	if got := testutil.ToFloat64(e.m.WindowsLost.WithLabelValues(creatorA)); got != 1 {
		t.Errorf("review_windows_lost_total = %v, want 1", got)
	}
}

func TestPostValidation(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)

	cases := []struct {
		name string
		body any
	}{
		{"empty decisions", map[string]any{"decisions": []any{}}},
		{"missing flagged", map[string]any{"decisions": []any{map[string]any{"message_id": "a1"}}}},
		{"missing message_id", map[string]any{"decisions": []any{map[string]any{"flagged": true}}}},
		{"duplicate ids", map[string]any{"decisions": []any{d("a1", true), d("a1", false)}}},
		{"unknown field", map[string]any{"decisions": []any{d("a1", true)}, "bogus": 1}},
		{"bad cursor", map[string]any{"decisions": []any{d("a1", true)}, "cursor": "not-a-cursor!"}},
	}
	for _, tc := range cases {
		code, _, raw := e.post(t, creatorA, tc.body, "")
		if code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400 (%s)", tc.name, code, raw)
		}
	}
	if cur, ok := e.mem.Get(creatorA); ok && cur.NextOffset != 0 {
		t.Errorf("validation failures moved the cursor to %d", cur.NextOffset)
	}
}

func TestQueueMetricsObservedOnGet(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e) // oldest pending for A flagged 1h ago

	if code, _ := e.get(t, creatorA, ""); code != http.StatusOK {
		t.Fatal("GET failed")
	}
	if lag := testutil.ToFloat64(e.m.QueueLag.WithLabelValues(creatorA)); lag != 3600 {
		t.Errorf("review_queue_lag_seconds = %v, want 3600", lag)
	}
	// The scan reached the high watermark, so pending is exact.
	if pend := testutil.ToFloat64(e.m.PendingEstimate.WithLabelValues(creatorA)); pend != 3 {
		t.Errorf("review_pending_estimate = %v, want 3", pend)
	}
}

func TestMultiPartitionCreatorsResolveOwnPartitions(t *testing.T) {
	e := setup(t, 3)
	pa, pb := e.partitionFor(t, creatorA), e.partitionFor(t, creatorB)
	if pa == pb {
		t.Fatalf("test creators map to the same partition (%d); pick different ids", pa)
	}

	e.appendFlagged(t, creatorA, "a1", contracts.ActionReview, time.Hour)
	e.appendFlagged(t, creatorB, "b1", contracts.ActionReview, time.Hour)
	e.appendFlagged(t, creatorA, "a2", contracts.ActionReview, time.Minute)

	_, respA := e.get(t, creatorA, "")
	if !eqStrings(itemIDs(respA.Items), []string{"a1", "a2"}) {
		t.Errorf("creator A items %v", itemIDs(respA.Items))
	}
	_, respB := e.get(t, creatorB, "")
	if !eqStrings(itemIDs(respB.Items), []string{"b1"}) {
		t.Errorf("creator B items %v", itemIDs(respB.Items))
	}

	curA, _ := e.mem.Get(creatorA)
	if curA.Partition != pa {
		t.Errorf("creator A cursor row partition %d, want %d", curA.Partition, pa)
	}

	code, resp, _ := e.post(t, creatorA, decisionsBody("", d("a1", true)), "")
	if code != http.StatusOK || resp.Deleted != 1 {
		t.Fatalf("POST on partitioned queue: code %d resp %+v", code, resp)
	}
	if _, respB2 := e.get(t, creatorB, ""); !eqStrings(itemIDs(respB2.Items), []string{"b1"}) {
		t.Errorf("creator B queue disturbed: %v", itemIDs(respB2.Items))
	}
}

func TestProduceFailureDoesNotAdvanceCursor(t *testing.T) {
	e := setup(t, 1)
	seedInterleaved(t, e)
	_, page := e.get(t, creatorA, "?limit=2")
	e.prod.fail = fmt.Errorf("broker down")

	code, _, _ := e.post(t, creatorA, decisionsBody(page.NextCursor, d("a1", true)), "")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", code)
	}
	if cur, _ := e.mem.Get(creatorA); cur.NextOffset != 0 {
		t.Errorf("cursor advanced to %d despite produce failure", cur.NextOffset)
	}

	// Recovery: the same window can be reviewed again.
	e.prod.fail = nil
	code, resp, _ := e.post(t, creatorA, decisionsBody(page.NextCursor, d("a1", true)), "")
	if code != http.StatusOK || resp.Applied != 1 || resp.Deleted != 1 {
		t.Fatalf("retry after failure: code %d resp %+v", code, resp)
	}
}
