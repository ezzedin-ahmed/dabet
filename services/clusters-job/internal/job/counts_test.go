package job

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"
)

// --- countRowsFor: the recomputation itself ---

// countFixture is a hand-built run result: six points over two hours and
// two content ids, two topics, one theme under the first topic, and one
// point (index 4) that HDBSCAN left as noise — a member of nothing.
func countFixture() (points []Point, topics, themes []clusterOut, topicIDs, themeIDs []string) {
	base := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return base.Add(d) }
	points = []Point{
		{ContentID: "ct-A", EmbeddedAt: at(0)},                // 0 -> topic 0
		{ContentID: "ct-A", EmbeddedAt: at(30 * time.Minute)}, // 1 -> topic 0 / theme 0
		{ContentID: "ct-B", EmbeddedAt: at(5 * time.Minute)},  // 2 -> topic 0
		{ContentID: "ct-A", EmbeddedAt: at(time.Hour)},        // 3 -> topic 0 / theme 0
		{ContentID: "ct-A", EmbeddedAt: at(70 * time.Minute)}, // 4 -> noise
		{ContentID: "ct-B", EmbeddedAt: at(time.Hour)},        // 5 -> topic 1
		{ContentID: "ct-A", EmbeddedAt: at(45 * time.Minute)}, // 6 -> topic 0
	}
	topics = []clusterOut{
		{members: []int{0, 1, 2, 3, 6}, themeOf: -1},
		{members: []int{5}, themeOf: -1},
	}
	themes = []clusterOut{{members: []int{1, 3}, themeOf: 0}}
	topicIDs = []string{"topic-0", "topic-1"}
	themeIDs = []string{"theme-0"}
	return
}

func rowKey(r CountRow) string {
	return strings.Join([]string{r.BucketHour.UTC().Format(time.RFC3339),
		r.TopicID, r.ThemeID, r.ContentID}, "|")
}

func rowMap(rows []CountRow) map[string]uint64 {
	out := map[string]uint64{}
	for _, r := range rows {
		out[rowKey(r)] += r.Count
	}
	return out
}

func TestCountRowsForAggregatesAndExcludesNoise(t *testing.T) {
	points, topics, themes, topicIDs, themeIDs := countFixture()
	far := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := countRowsFor("cr-1", points, topics, themes, topicIDs, themeIDs, far)

	want := map[string]uint64{
		// Hour 10: points 0 and 6 share (ct-A, topic-0, no theme) and are
		// summed into one row; point 1 is theme-assigned; point 2 is ct-B.
		"2026-08-01T10:00:00Z|topic-0|" + ZeroUUID + "|ct-A": 2,
		"2026-08-01T10:00:00Z|topic-0|theme-0|ct-A":          1,
		"2026-08-01T10:00:00Z|topic-0|" + ZeroUUID + "|ct-B": 1,
		// Hour 11: point 3 (theme), point 5 (topic 1). Point 4 is noise.
		"2026-08-01T11:00:00Z|topic-0|theme-0|ct-A":          1,
		"2026-08-01T11:00:00Z|topic-1|" + ZeroUUID + "|ct-B": 1,
	}
	got := rowMap(rows)
	if len(got) != len(want) {
		t.Fatalf("got %d distinct rows, want %d: %+v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row %s = %d, want %d", k, got[k], v)
		}
	}
	// Six of the seven points are clustered; the noise point is counted
	// into no topic, exactly as a below-threshold live record is (§8.5).
	var total uint64
	for _, r := range rows {
		total += r.Count
		if r.CreatorID != "cr-1" {
			t.Errorf("row creator = %q, want cr-1", r.CreatorID)
		}
	}
	if total != 6 {
		t.Errorf("counted %d points, want 6 (7 points minus 1 noise)", total)
	}
}

func TestCountRowsForStopsAtCutoff(t *testing.T) {
	points, topics, themes, topicIDs, themeIDs := countFixture()
	cutoff := time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC)
	rows := countRowsFor("cr-1", points, topics, themes, topicIDs, themeIDs, cutoff)
	for _, r := range rows {
		if !r.BucketHour.Before(cutoff) {
			t.Errorf("row in bucket %s is at or after the cutoff %s", r.BucketHour, cutoff)
		}
	}
	var total uint64
	for _, r := range rows {
		total += r.Count
	}
	if total != 4 {
		t.Errorf("counted %d points before the cutoff, want 4", total)
	}
}

func TestCountRowsForIsDeterministic(t *testing.T) {
	points, topics, themes, topicIDs, themeIDs := countFixture()
	far := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	a := countRowsFor("cr-1", points, topics, themes, topicIDs, themeIDs, far)
	b := countRowsFor("cr-1", points, topics, themes, topicIDs, themeIDs, far)
	if len(a) != len(b) {
		t.Fatalf("row counts differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("batch order differs at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

// --- pipeline wiring ---

// spreadCorpus is testCorpus rewritten to span three hourly buckets and
// two content ids, so per-hour and per-content attribution is observable.
func spreadCorpus(from time.Time) []EmbeddingRecord {
	recs := testCorpus(from)
	for i := range recs {
		recs[i].EmbeddedAt = from.
			Add(time.Duration(i%3) * time.Hour).
			Add(time.Duration(i) * time.Second)
		if i%2 == 0 {
			recs[i].ContentID = "ct-A"
		} else {
			recs[i].ContentID = "ct-B"
		}
	}
	return recs
}

// clusteredPoints is how many of spreadCorpus' 40 points the two-pass
// HDBSCAN places in a topic; the rest are noise and are counted nowhere.
const clusteredPoints = 40

func liveRow(creator, content, topic, theme string, bucket time.Time, n uint64) CountRow {
	return CountRow{CreatorID: creator, ContentID: content, TopicID: topic,
		ThemeID: theme, BucketHour: bucket, Count: n}
}

func TestBackfillRewritesWindowToNewIDs(t *testing.T) {
	from, to := testWindow()
	h := newHarness(t, spreadCorpus(from))
	// Counts the live path wrote against the topic ids this run supersedes.
	h.counts.rows = []CountRow{
		liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from, 17),
		liveRow("cr-1", "ct-B", "old-topic", ZeroUUID, from.Add(time.Hour), 9),
	}

	res, err := h.runner.Run(context.Background(), onDemandDecision())
	if err != nil {
		t.Fatal(err)
	}

	// Delete before insert: a SummingMergeTree sums what it is given, so
	// the reverse order would double every count in the window.
	if got := strings.Join(h.counts.ops, ","); got != "delete,insert" {
		t.Fatalf("store ops = %q, want \"delete,insert\"", got)
	}
	if len(h.counts.deletes) != 1 {
		t.Fatalf("delete calls = %d, want 1", len(h.counts.deletes))
	}
	del := h.counts.deletes[0]
	if del.creatorID != "cr-1" {
		t.Errorf("delete creator = %q, want cr-1", del.creatorID)
	}
	if !del.from.Equal(from) || !del.to.Equal(to) {
		t.Errorf("delete range = [%s, %s), want [%s, %s)", del.from, del.to, from, to)
	}
	if res.CountsDeleted != 2 {
		t.Errorf("res.CountsDeleted = %d, want 2 superseded rows", res.CountsDeleted)
	}
	if res.CountsWritten == 0 {
		t.Fatal("no rows written")
	}

	// Nothing attributed to the superseded id survives, and every row now
	// names a topic (and theme) this run just wrote.
	topicIDs := map[string]bool{}
	themeParent := map[string]string{}
	for _, row := range h.topics.upserted {
		if row.ParentID == ZeroUUID {
			topicIDs[row.TopicID] = true
		} else {
			themeParent[row.TopicID] = row.ParentID
		}
	}
	var total uint64
	buckets := map[string]bool{}
	contents := map[string]bool{}
	for _, r := range h.counts.rows {
		if r.TopicID == "old-topic" {
			t.Fatalf("superseded topic id still counted: %+v", r)
		}
		if !topicIDs[r.TopicID] {
			t.Errorf("row names unknown topic %s", r.TopicID)
		}
		if r.ThemeID != ZeroUUID && themeParent[r.ThemeID] != r.TopicID {
			t.Errorf("theme %s is not nested under topic %s", r.ThemeID, r.TopicID)
		}
		if !r.BucketHour.Equal(BucketHour(r.BucketHour)) {
			t.Errorf("bucket %s is not hour-aligned", r.BucketHour)
		}
		total += r.Count
		buckets[r.BucketHour.Format(time.RFC3339)] = true
		contents[r.ContentID] = true
	}
	if total != clusteredPoints {
		t.Errorf("counted %d points, want %d clustered points", total, clusteredPoints)
	}
	if len(buckets) != 3 {
		t.Errorf("counts landed in %d hourly buckets, want 3", len(buckets))
	}
	if len(contents) != 2 {
		t.Errorf("counts landed on %d content ids, want 2", len(contents))
	}
	// Topic-level-only assignments carry the zero UUID, as §8.7 requires.
	zero := false
	for _, r := range h.counts.rows {
		if r.ThemeID == ZeroUUID {
			zero = true
		}
	}
	if !zero {
		t.Error("no topic-level-only row carries the zero UUID theme")
	}
}

func TestBackfillIdempotentRerun(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, spreadCorpus(from))
	h.counts.rows = []CountRow{liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from, 17)}

	if _, err := h.runner.Run(context.Background(), onDemandDecision()); err != nil {
		t.Fatal(err)
	}
	first := h.counts.merged()
	firstTotal := h.counts.total()

	// Same window, same points, same job: the second run must converge on
	// the first run's state rather than sum on top of it.
	res, err := h.runner.Run(context.Background(), onDemandDecision())
	if err != nil {
		t.Fatal(err)
	}
	second := h.counts.merged()

	if got := strings.Join(h.counts.ops, ","); got != "delete,insert,delete,insert" {
		t.Fatalf("store ops = %q", got)
	}
	if h.counts.total() != firstTotal {
		t.Fatalf("re-run total = %d, want %d (accumulated instead of converging)",
			h.counts.total(), firstTotal)
	}
	if len(second) != len(first) {
		t.Fatalf("re-run wrote %d distinct rows, want %d", len(second), len(first))
	}
	for k, v := range first {
		if second[k] != v {
			t.Errorf("row %s = %d after re-run, want %d", k, second[k], v)
		}
	}
	// The second delete removed exactly what the first run wrote.
	if res.CountsDeleted != res.CountsWritten {
		t.Errorf("re-run deleted %d rows and wrote %d; a converging rewrite deletes what it replaces",
			res.CountsDeleted, res.CountsWritten)
	}
	// And it is still one usage event per run, quantity = points processed.
	if len(h.usage.events) != 2 {
		t.Fatalf("usage events = %d, want one per run and none for the backfill", len(h.usage.events))
	}
	for _, ev := range h.usage.events {
		if ev.EventType != contracts.EventMessagesReclustered || ev.Quantity != 40 {
			t.Errorf("usage event = %+v, want messages_reclustered with quantity 40", ev)
		}
		if want := "recluster:" + onDemandDecision().JobID + ":cr-1"; ev.IdempotencyKey != want {
			t.Errorf("idempotency_key = %q, want %q", ev.IdempotencyKey, want)
		}
	}
}

func TestBackfillTouchesOnlyThisCreatorAndWindow(t *testing.T) {
	from, to := testWindow()
	h := newHarness(t, spreadCorpus(from))
	// Another creator inside the window, and this creator just outside it
	// on both ends. None of the three may move (§8.6).
	other := liveRow("cr-2", "ct-A", "other-topic", ZeroUUID, from.Add(2*time.Hour), 11)
	before := liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from.Add(-time.Hour), 5)
	after := liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, to, 7)
	h.counts.rows = []CountRow{other, before, after}

	if _, err := h.runner.Run(context.Background(), onDemandDecision()); err != nil {
		t.Fatal(err)
	}
	survivors := map[string]uint64{}
	for _, r := range h.counts.rows {
		if r.TopicID == "other-topic" || r.TopicID == "old-topic" {
			survivors[r.CreatorID+"|"+r.BucketHour.Format(time.RFC3339)] += r.Count
		}
	}
	want := map[string]uint64{
		"cr-2|" + other.BucketHour.Format(time.RFC3339):  11,
		"cr-1|" + before.BucketHour.Format(time.RFC3339): 5,
		"cr-1|" + after.BucketHour.Format(time.RFC3339):  7,
	}
	if len(survivors) != len(want) {
		t.Fatalf("out-of-scope rows = %+v, want %+v", survivors, want)
	}
	for k, v := range want {
		if survivors[k] != v {
			t.Errorf("out-of-scope row %s = %d, want %d", k, survivors[k], v)
		}
	}
}

func TestBackfillInsertFailureUnderCountsRatherThanDoubles(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, spreadCorpus(from))
	h.counts.rows = []CountRow{liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from, 17)}
	h.counts.insertErr = errors.New("clickhouse down")

	res, err := h.runner.Run(context.Background(), onDemandDecision())
	if err == nil || !strings.Contains(err.Error(), "backfill counts") {
		t.Fatalf("err = %v, want a backfill failure", err)
	}
	// Deleted but not reinserted: the window reads as zero until the next
	// run. That is the documented exposure — an under-count, never a
	// double-count.
	if h.counts.total() != 0 {
		t.Errorf("window total = %d after a failed insert, want 0 (under-count, not doubled)", h.counts.total())
	}
	if res.CountsWritten != 0 {
		t.Errorf("res.CountsWritten = %d, want 0", res.CountsWritten)
	}
	// The clustering itself still happened and is still billed once.
	if len(h.usage.events) != 1 || h.usage.events[0].Quantity != 40 {
		t.Errorf("usage events = %+v, want exactly one with quantity 40", h.usage.events)
	}
	if got := testutil.ToFloat64(h.runner.Metrics.Backfills.WithLabelValues(TriggerOnDemand, "error")); got != 1 {
		t.Errorf("backfill error metric = %v, want 1", got)
	}
}

func TestBackfillDeleteFailureLeavesRowsUntouched(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, spreadCorpus(from))
	live := liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from, 17)
	h.counts.rows = []CountRow{live}
	h.counts.deleteErr = errors.New("mutation refused")

	if _, err := h.runner.Run(context.Background(), onDemandDecision()); err == nil ||
		!strings.Contains(err.Error(), "backfill counts") {
		t.Fatalf("err = %v, want a backfill failure", err)
	}
	if got := strings.Join(h.counts.ops, ","); got != "delete" {
		t.Fatalf("store ops = %q, want just \"delete\" — nothing may be inserted on top of undeleted rows", got)
	}
	if len(h.counts.rows) != 1 || h.counts.rows[0] != live {
		t.Errorf("rows = %+v, want the live row untouched", h.counts.rows)
	}
}

func TestBackfillModeGatesTheRewrite(t *testing.T) {
	from, _ := testWindow()
	cases := []struct {
		name    string
		mode    string
		trigger string
		want    bool
	}{
		{"on_demand default rewrites a creator request", BackfillOnDemand, TriggerOnDemand, true},
		{"on_demand default rewrites a RUN_ONCE", BackfillOnDemand, TriggerManual, true},
		{"on_demand default leaves periodic runs alone", BackfillOnDemand, TriggerDoubled, false},
		{"unset behaves as on_demand", "", TriggerDoubled, false},
		{"always rewrites periodic runs too", BackfillAlways, TriggerUnassigned, true},
		{"off never rewrites", BackfillOff, TriggerOnDemand, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, spreadCorpus(from))
			h.runner.Cfg.BackfillCounts = tc.mode
			d := onDemandDecision()
			d.Trigger = tc.trigger
			if _, err := h.runner.Run(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			if got := len(h.counts.ops) > 0; got != tc.want {
				t.Errorf("backfill ran = %v (ops %v), want %v", got, h.counts.ops, tc.want)
			}
		})
	}
}

func TestBackfillNilStoreIsANoOp(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, spreadCorpus(from))
	h.runner.Counts = nil
	res, err := h.runner.Run(context.Background(), onDemandDecision())
	if err != nil {
		t.Fatal(err)
	}
	if res.CountsDeleted != 0 || res.CountsWritten != 0 {
		t.Errorf("res = %+v, want no backfill without a counts store", res)
	}
}

func TestBackfillLagBoundsTheLiveWriterRace(t *testing.T) {
	from, _ := testWindow()

	// Window entirely inside the lag: decline rather than race a writer we
	// do not control.
	h := newHarness(t, spreadCorpus(from))
	h.runner.Now = func() time.Time { return from.Add(time.Hour) }
	if _, err := h.runner.Run(context.Background(), onDemandDecision()); err != nil {
		t.Fatal(err)
	}
	if len(h.counts.ops) != 0 {
		t.Errorf("store ops = %v, want none for a window newer than the lag", h.counts.ops)
	}
	if got := testutil.ToFloat64(h.runner.Metrics.Backfills.WithLabelValues(TriggerOnDemand, "skipped")); got != 1 {
		t.Errorf("skipped metric = %v, want 1", got)
	}

	// Window partly inside the lag: rewrite the settled buckets, leave the
	// trailing ones to the live writer. spreadCorpus spans hours 0..2, the
	// cutoff lands at hour 2.
	h2 := newHarness(t, spreadCorpus(from))
	h2.runner.Now = func() time.Time { return from.Add(4 * time.Hour) }
	cutoff := from.Add(2 * time.Hour)
	tail := liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, cutoff, 3)
	h2.counts.rows = []CountRow{tail}
	if _, err := h2.runner.Run(context.Background(), onDemandDecision()); err != nil {
		t.Fatal(err)
	}
	if len(h2.counts.deletes) != 1 || !h2.counts.deletes[0].to.Equal(cutoff) {
		t.Fatalf("delete range = %+v, want it to stop at the cutoff %s", h2.counts.deletes, cutoff)
	}
	for _, r := range h2.counts.rows {
		if r.TopicID == "old-topic" {
			continue // the documented trailing inconsistency
		}
		if !r.BucketHour.Before(cutoff) {
			t.Errorf("backfilled row in bucket %s is at or past the cutoff %s", r.BucketHour, cutoff)
		}
	}
	found := false
	for _, r := range h2.counts.rows {
		if r == tail {
			found = true
		}
	}
	if !found {
		t.Error("the live writer's trailing row was deleted; it is outside the rewrite range")
	}
}

func TestBackfillMetrics(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, spreadCorpus(from))
	h.counts.rows = []CountRow{
		liveRow("cr-1", "ct-A", "old-topic", ZeroUUID, from, 4),
		liveRow("cr-1", "ct-B", "old-topic", ZeroUUID, from, 4),
	}
	res, err := h.runner.Run(context.Background(), onDemandDecision())
	if err != nil {
		t.Fatal(err)
	}
	m := h.runner.Metrics
	if got := testutil.ToFloat64(m.Backfills.WithLabelValues(TriggerOnDemand, "ok")); got != 1 {
		t.Errorf("backfill ok = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.BackfillRows.WithLabelValues(TriggerOnDemand, "deleted")); got != 2 {
		t.Errorf("rows deleted = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.BackfillRows.WithLabelValues(TriggerOnDemand, "written")); got != float64(res.CountsWritten) {
		t.Errorf("rows written = %v, want %d", got, res.CountsWritten)
	}
	if got := testutil.CollectAndCount(m.BackfillDuration, "clusters_job_counts_backfill_duration_seconds"); got != 1 {
		t.Errorf("duration series = %d, want 1", got)
	}
	// §4.5: no creator_id (or any identifier) on a clusters_job_* label.
	for _, name := range []string{
		"clusters_job_counts_backfill_total",
		"clusters_job_counts_backfill_rows_total",
		"clusters_job_counts_backfill_duration_seconds",
	} {
		families, err := h.registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if f.GetName() != name {
				continue
			}
			for _, metric := range f.GetMetric() {
				for _, l := range metric.GetLabel() {
					switch l.GetName() {
					case "creator_id", "content_id", "topic_id", "theme_id":
						t.Errorf("%s carries a forbidden label %s", name, l.GetName())
					}
				}
			}
		}
	}
}
