package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var t0 = time.Date(2026, 8, 19, 13, 37, 42, 0, time.UTC)

// fakeIndex resolves searches from a static table keyed by
// creator|parent. err poisons every search.
type fakeIndex struct {
	matches map[string]Match
	err     error
	calls   int
}

func key(creatorID, parentID string) string { return creatorID + "|" + parentID }

func (f *fakeIndex) Nearest(_ context.Context, creatorID, parentID string, _ []float32) (Match, bool, error) {
	f.calls++
	if f.err != nil {
		return Match{}, false, f.err
	}
	m, ok := f.matches[key(creatorID, parentID)]
	return m, ok, nil
}

type fakeSink struct {
	batches [][]CountRow
	err     error
}

func (f *fakeSink) InsertCounts(_ context.Context, rows []CountRow) error {
	if f.err != nil {
		return f.err
	}
	f.batches = append(f.batches, rows)
	return nil
}

type harness struct {
	assigner *Assigner
	counts   *CountBuffer
	sink     *fakeSink
	metrics  *Metrics
	failOpen *prometheus.CounterVec
	depUp    *prometheus.GaugeVec
}

func newHarness(t *testing.T, index CentroidIndex) *harness {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	failOpen := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fail_open_total"}, []string{"component", "reason"})
	depUp := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dependency_up"}, []string{"dependency"})
	sink := &fakeSink{}
	counts := NewCountBuffer(sink, 1000, time.Minute, time.Second, failOpen, depUp)
	return &harness{
		assigner: NewAssigner(index, counts, 0.75, m, failOpen, depUp),
		counts:   counts,
		sink:     sink,
		metrics:  m,
		failOpen: failOpen,
		depUp:    depUp,
	}
}

func rec(creator string) Record {
	return Record{CreatorID: creator, ContentID: "ct-1", EmbeddedAt: t0, Vector: []float32{1, 2, 3}}
}

func result(t *testing.T, m *Metrics, result string) float64 {
	t.Helper()
	return testutil.ToFloat64(m.AssignmentsTotal.WithLabelValues(result))
}

func TestAssignTopicAndTheme(t *testing.T) {
	idx := &fakeIndex{matches: map[string]Match{
		key("cr-1", ZeroUUID):  {TopicID: "topic-1", Score: 0.9},
		key("cr-1", "topic-1"): {TopicID: "theme-1", Score: 0.8},
	}}
	h := newHarness(t, idx)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1")})

	if got := result(t, h.metrics, ResultTheme); got != 1 {
		t.Fatalf("theme assignments = %v, want 1", got)
	}
	h.counts.Flush(context.Background())
	rows := h.sink.batches[0]
	if len(rows) != 1 {
		t.Fatalf("expected 1 count row, got %d", len(rows))
	}
	r := rows[0]
	if r.TopicID != "topic-1" || r.ThemeID != "theme-1" || r.Count != 1 {
		t.Fatalf("unexpected row: %+v", r)
	}
	if r.BucketHour != time.Date(2026, 8, 19, 13, 0, 0, 0, time.UTC) {
		t.Fatalf("bucket_hour not truncated to the hour: %v", r.BucketHour)
	}
}

func TestAssignTopicOnlyWhenThemeBelowThreshold(t *testing.T) {
	idx := &fakeIndex{matches: map[string]Match{
		key("cr-1", ZeroUUID):  {TopicID: "topic-1", Score: 0.75}, // exactly at threshold assigns
		key("cr-1", "topic-1"): {TopicID: "theme-1", Score: 0.7490},
	}}
	h := newHarness(t, idx)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1")})

	if got := result(t, h.metrics, ResultTopic); got != 1 {
		t.Fatalf("topic assignments = %v, want 1", got)
	}
	if got := result(t, h.metrics, ResultTheme); got != 0 {
		t.Fatalf("theme assignments = %v, want 0", got)
	}
	h.counts.Flush(context.Background())
	r := h.sink.batches[0][0]
	if r.ThemeID != ZeroUUID {
		t.Fatalf("topic-level assignment must carry the zero UUID theme, got %q", r.ThemeID)
	}
}

func TestAssignTopicOnlyWhenNoThemes(t *testing.T) {
	idx := &fakeIndex{matches: map[string]Match{
		key("cr-1", ZeroUUID): {TopicID: "topic-1", Score: 0.8},
	}}
	h := newHarness(t, idx)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1")})
	if got := result(t, h.metrics, ResultTopic); got != 1 {
		t.Fatalf("topic assignments = %v, want 1", got)
	}
}

func TestAssignUnassignedBelowThreshold(t *testing.T) {
	idx := &fakeIndex{matches: map[string]Match{
		key("cr-1", ZeroUUID): {TopicID: "topic-1", Score: 0.7},
	}}
	h := newHarness(t, idx)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1")})

	if got := result(t, h.metrics, ResultUnassigned); got != 1 {
		t.Fatalf("unassigned = %v, want 1", got)
	}
	h.counts.Flush(context.Background())
	if len(h.sink.batches) != 0 {
		t.Fatalf("unassigned must not write count rows, got %v", h.sink.batches)
	}
	// Only the topic-level search runs — no theme probe for an unassigned record.
	if idx.calls != 1 {
		t.Fatalf("expected 1 search, got %d", idx.calls)
	}
}

func TestAssignUnassignedForColdCreator(t *testing.T) {
	h := newHarness(t, &fakeIndex{matches: map[string]Match{}})
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-cold")})
	if got := result(t, h.metrics, ResultUnassigned); got != 1 {
		t.Fatalf("unassigned = %v, want 1", got)
	}
}

func TestAssignMilvusDownDropsBatch(t *testing.T) {
	idx := &fakeIndex{err: errors.New("milvus down")}
	h := newHarness(t, idx)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1"), rec("cr-1"), rec("cr-1")})

	if got := testutil.ToFloat64(h.failOpen.WithLabelValues("milvus", "search_failed")); got != 3 {
		t.Fatalf("fail_open milvus = %v, want 3", got)
	}
	if got := testutil.ToFloat64(h.depUp.WithLabelValues("milvus")); got != 0 {
		t.Fatalf("dependency_up milvus = %v, want 0", got)
	}
	for _, res := range []string{ResultTopic, ResultTheme, ResultUnassigned} {
		if got := result(t, h.metrics, res); got != 0 {
			t.Fatalf("assignments %s = %v, want 0", res, got)
		}
	}
}

func TestAssignBatchSetsDependencyUp(t *testing.T) {
	idx := &fakeIndex{matches: map[string]Match{}}
	h := newHarness(t, idx)
	h.depUp.WithLabelValues("milvus").Set(0)
	h.assigner.AssignBatch(context.Background(), []Record{rec("cr-1")})
	if got := testutil.ToFloat64(h.depUp.WithLabelValues("milvus")); got != 1 {
		t.Fatalf("dependency_up milvus = %v, want 1", got)
	}
}
