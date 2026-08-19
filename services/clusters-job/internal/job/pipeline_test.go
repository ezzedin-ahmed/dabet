package job

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/contracts"
	"dabet/pkg/embeddings"
)

// --- fakes ---

type fakeStore struct {
	objs []ObjectInfo
	data map[string][]byte
}

func (f *fakeStore) ListCreatorObjects(context.Context, string, time.Time, time.Time) ([]ObjectInfo, error) {
	return f.objs, nil
}
func (f *fakeStore) Get(_ context.Context, key string) ([]byte, error) { return f.data[key], nil }

type fakeCentroids struct {
	prior    []Centroid
	replaced map[string][]Centroid
}

func (f *fakeCentroids) ListByCreator(context.Context, string) ([]Centroid, error) {
	return f.prior, nil
}
func (f *fakeCentroids) ReplaceCreator(_ context.Context, creatorID string, cs []Centroid) error {
	if f.replaced == nil {
		f.replaced = map[string][]Centroid{}
	}
	f.replaced[creatorID] = cs
	return nil
}

type fakeTopics struct {
	prior       []TopicRow
	upserted    []TopicRow
	deletedKeep []string
	deleteCall  bool
}

func (f *fakeTopics) PriorTopics(context.Context, string) ([]TopicRow, error) { return f.prior, nil }
func (f *fakeTopics) UpsertTopics(_ context.Context, _ string, rows []TopicRow) error {
	f.upserted = append(f.upserted, rows...)
	return nil
}
func (f *fakeTopics) DeleteTopicsExcept(_ context.Context, _ string, keep []string) error {
	f.deleteCall = true
	f.deletedKeep = keep
	return nil
}

// fakeCounts stands in for the topic_counts SummingMergeTree: inserts
// accumulate (they are summed, never replaced — which is exactly the
// hazard the backfill has to handle) and DeleteCounts removes rows by
// (creator, bucket range) the way the lightweight DELETE does.
type fakeCounts struct {
	mu        sync.Mutex
	rows      []CountRow
	ops       []string // "delete"/"insert", in call order
	deletes   []countCall
	inserts   [][]CountRow
	deleteErr error
	insertErr error
}

type countCall struct {
	creatorID string
	from, to  time.Time
}

func (f *fakeCounts) DeleteCounts(_ context.Context, creatorID string, from, to time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "delete")
	f.deletes = append(f.deletes, countCall{creatorID, from, to})
	if f.deleteErr != nil {
		return 0, f.deleteErr
	}
	kept := make([]CountRow, 0, len(f.rows))
	var n int64
	for _, r := range f.rows {
		if r.CreatorID == creatorID && !r.BucketHour.Before(from) && r.BucketHour.Before(to) {
			n++
			continue
		}
		kept = append(kept, r)
	}
	f.rows = kept
	return n, nil
}

func (f *fakeCounts) InsertCounts(_ context.Context, rows []CountRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "insert")
	f.inserts = append(f.inserts, append([]CountRow(nil), rows...))
	if f.insertErr != nil {
		return f.insertErr
	}
	f.rows = append(f.rows, rows...)
	return nil
}

// merged collapses the stored rows the way SummingMergeTree does: sum
// count over the ordering key. This is what the §8.8 API reads.
func (f *fakeCounts) merged() map[string]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]uint64{}
	for _, r := range f.rows {
		key := strings.Join([]string{r.CreatorID, r.BucketHour.UTC().Format(time.RFC3339),
			r.TopicID, r.ThemeID, r.ContentID}, "|")
		out[key] += r.Count
	}
	return out
}

func (f *fakeCounts) total() uint64 {
	var n uint64
	for _, v := range f.merged() {
		n += v
	}
	return n
}

type fakeTexts struct {
	texts []string
	err   error
}

func (f *fakeTexts) Sample(context.Context, string, int) ([]string, error) {
	return f.texts, f.err
}

type fakeEmbed struct{ byText map[string][]float32 }

func (f *fakeEmbed) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v, ok := f.byText[t]
		if !ok {
			return nil, errors.New("unexpected text")
		}
		out[i] = append([]float32(nil), v...)
	}
	return out, nil
}

type fakeLLM struct {
	label, desc string
	err         error
	calls       [][]string
	priors      []string
}

func (f *fakeLLM) Label(_ context.Context, texts []string, prior string) (string, string, error) {
	f.calls = append(f.calls, append([]string(nil), texts...))
	f.priors = append(f.priors, prior)
	return f.label, f.desc, f.err
}

type fakeUsage struct{ events []contracts.Usage }

func (f *fakeUsage) PublishUsage(_ context.Context, u contracts.Usage) error {
	f.events = append(f.events, u)
	return nil
}

// --- test data ---

func mix(entries map[int]float32) []float32 {
	v := make([]float32, embeddings.Dimensions)
	for i, x := range entries {
		v[i] = x
	}
	return v
}

// testCorpus builds 40 points: topic one is two 10-point sub-blobs along
// +e2/-e2 around e0 (one topic at min_cluster_size 15, two themes at 5),
// topic two is a 20-point blob around e1.
func testCorpus(at time.Time) []EmbeddingRecord {
	var recs []EmbeddingRecord
	add := func(i int, base map[int]float32) {
		v := mix(base)
		v[20+i] = 0.01 * float32(1+i%3) // unique jitter dimension per point
		recs = append(recs, EmbeddingRecord{
			CreatorID: "cr-1", ContentID: "ct-1",
			EmbeddedAt: at.Add(time.Duration(i) * time.Second), Vector: v,
		})
	}
	for i := 0; i < 10; i++ {
		add(i, map[int]float32{0: 1, 2: 0.3})
	}
	for i := 10; i < 20; i++ {
		add(i, map[int]float32{0: 1, 2: -0.3})
	}
	for i := 20; i < 40; i++ {
		add(i, map[int]float32{1: 1})
	}
	return recs
}

func testWindow() (time.Time, time.Time) {
	return time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
}

// testNow is well past testWindow, so the default backfill lag never
// truncates the rewrite unless a test moves the clock deliberately.
func testNow() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) }

func testConfig() Config {
	return Config{
		MinClusterSize: 15, MinSamples: 3,
		ThemeMinClusterSize: 5, ThemeMinSamples: 3,
		MaxPoints:       200_000,
		AssignThreshold: 0.75, ReuseThreshold: 0.75,
		LabelPoints: 20, TextSampleMax: 100, EmbedBatch: 64,
		BackfillCounts: BackfillOnDemand, BackfillLag: 2 * time.Hour,
	}
}

type harness struct {
	runner    *Runner
	store     *fakeStore
	centroids *fakeCentroids
	topics    *fakeTopics
	counts    *fakeCounts
	texts     *fakeTexts
	embed     *fakeEmbed
	llm       *fakeLLM
	usage     *fakeUsage
	registry  *prometheus.Registry
}

func newHarness(t *testing.T, recs []EmbeddingRecord) *harness {
	t.Helper()
	h := &harness{
		store: &fakeStore{
			objs: []ObjectInfo{{Key: "obj1", Size: 1}},
			data: map[string][]byte{"obj1": writeParquetFixture(t, recs)},
		},
		centroids: &fakeCentroids{},
		topics:    &fakeTopics{},
		counts:    &fakeCounts{},
		texts:     &fakeTexts{},
		embed:     &fakeEmbed{byText: map[string][]float32{}},
		llm:       &fakeLLM{err: errors.New("llm down")},
		usage:     &fakeUsage{},
		registry:  prometheus.NewRegistry(),
	}
	h.runner = &Runner{
		Store: h.store, Centroids: h.centroids, Topics: h.topics, Counts: h.counts,
		Texts: h.texts, Embed: h.embed, LLM: h.llm, Usage: h.usage,
		Metrics: NewMetrics(h.registry),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Cfg:     testConfig(),
		Now:     testNow,
	}
	return h
}

// onDemandDecision is a creator-requested recluster of the test window —
// the path §8.6 says rewrites history, and the backfill's default path.
func onDemandDecision() Decision {
	from, to := testWindow()
	return Decision{CreatorID: "cr-1", Trigger: TriggerOnDemand, From: from, To: to,
		JobID: ReclusterJobID("cr-1", from, to)}
}

func scheduledDecision() Decision {
	from, to := testWindow()
	return Decision{CreatorID: "cr-1", Trigger: TriggerDoubled, From: from, To: to}
}

// --- tests ---

func TestPipelineTwoPassNesting(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, testCorpus(from))

	res, err := h.runner.Run(context.Background(), scheduledDecision())
	if err != nil {
		t.Fatal(err)
	}
	if res.PointsRead != 40 || res.PointsProcessed != 40 {
		t.Errorf("points read/processed = %d/%d, want 40/40", res.PointsRead, res.PointsProcessed)
	}
	if res.Topics != 2 || res.Themes != 2 {
		t.Fatalf("topics/themes = %d/%d, want 2/2", res.Topics, res.Themes)
	}

	cents := h.centroids.replaced["cr-1"]
	if len(cents) != 4 {
		t.Fatalf("replaced %d centroids, want 4 (2 topics + 2 themes)", len(cents))
	}
	// Identify the e0 topic; both themes must nest under it.
	var e0Topic, e1Topic string
	for _, c := range cents {
		if c.ParentID != ZeroUUID {
			continue
		}
		if c.Vector[0] > 0.9 {
			e0Topic = c.TopicID
		}
		if c.Vector[1] > 0.9 {
			e1Topic = c.TopicID
		}
	}
	if e0Topic == "" || e1Topic == "" {
		t.Fatalf("topic centroids not found in %+v", cents)
	}
	themeParents := map[string]int{}
	themeSigns := map[bool]int{}
	for _, c := range cents {
		if c.ParentID == ZeroUUID {
			continue
		}
		themeParents[c.ParentID]++
		themeSigns[c.Vector[2] > 0]++
	}
	if themeParents[e0Topic] != 2 {
		t.Errorf("themes nest under %v, want both under the e0 topic %s", themeParents, e0Topic)
	}
	if themeSigns[true] != 1 || themeSigns[false] != 1 {
		t.Errorf("theme centroids do not split +e2/-e2: %v", themeSigns)
	}

	// Centroids are unit length.
	for _, c := range cents {
		var norm float64
		for _, x := range c.Vector {
			norm += float64(x) * float64(x)
		}
		if norm < 0.999 || norm > 1.001 {
			t.Errorf("centroid %s norm² = %v, want 1", c.TopicID, norm)
		}
	}

	// ClickHouse rows mirror the centroids: version 1 (no prior), themes
	// pointing at their topic.
	if len(h.topics.upserted) != 4 {
		t.Fatalf("upserted %d rows, want 4", len(h.topics.upserted))
	}
	for _, row := range h.topics.upserted {
		if row.Version != 1 {
			t.Errorf("row %s version = %d, want 1", row.TopicID, row.Version)
		}
	}
	if !h.topics.deleteCall || len(h.topics.deletedKeep) != 4 {
		t.Errorf("stale-delete keep list = %v, want the 4 new ids", h.topics.deletedKeep)
	}

	// No text in retention, no prior: generic degradation labels.
	genericTopic := regexp.MustCompile(`^Topic [0-9]+$`)
	genericTheme := regexp.MustCompile(`^Theme [0-9]+$`)
	for _, row := range h.topics.upserted {
		if row.ParentID == ZeroUUID && !genericTopic.MatchString(row.Label) {
			t.Errorf("topic label %q, want generic", row.Label)
		}
		if row.ParentID != ZeroUUID && !genericTheme.MatchString(row.Label) {
			t.Errorf("theme label %q, want generic", row.Label)
		}
	}
}

func TestPipelineDeterministicIdentity(t *testing.T) {
	from, _ := testWindow()
	ids := func() []string {
		h := newHarness(t, testCorpus(from))
		if _, err := h.runner.Run(context.Background(), scheduledDecision()); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, row := range h.topics.upserted {
			out = append(out, row.TopicID+"|"+row.ParentID)
		}
		sort.Strings(out)
		return out
	}
	a, b := ids(), ids()
	if len(a) != 4 {
		t.Fatalf("got %d rows", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("run ids differ: %v vs %v", a, b)
		}
	}
}

func TestPipelineLLMLabelsFromMatchedText(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, testCorpus(from))
	h.texts.texts = []string{"game night", "which game", "unrelated"}
	h.embed.byText["game night"] = mix(map[int]float32{1: 1})
	h.embed.byText["which game"] = mix(map[int]float32{1: 1, 30: 0.05})
	h.embed.byText["unrelated"] = mix(map[int]float32{5: 1}) // matches nothing
	h.llm = &fakeLLM{label: "Games", desc: "Talking about games."}
	h.runner.LLM = h.llm

	if _, err := h.runner.Run(context.Background(), scheduledDecision()); err != nil {
		t.Fatal(err)
	}
	var e1Label string
	genericCount := 0
	for _, row := range h.topics.upserted {
		if row.ParentID != ZeroUUID {
			continue
		}
		if row.Label == "Games" {
			e1Label = row.Label
			if row.Description != "Talking about games." {
				t.Errorf("description = %q", row.Description)
			}
		} else {
			genericCount++
		}
	}
	if e1Label == "" {
		t.Fatalf("no topic took the LLM label; rows: %+v", h.topics.upserted)
	}
	if genericCount != 1 {
		t.Errorf("expected the other topic to stay generic, rows: %+v", h.topics.upserted)
	}
	// The LLM saw only the matched texts, never the unassigned one.
	if len(h.llm.calls) != 1 {
		t.Fatalf("llm called %d times, want 1 (only the matched topic)", len(h.llm.calls))
	}
	for _, txt := range h.llm.calls[0] {
		if txt == "unrelated" {
			t.Error("unassigned text leaked into the label prompt")
		}
	}
	if len(h.llm.calls[0]) != 2 {
		t.Errorf("llm saw %d texts, want 2", len(h.llm.calls[0]))
	}
}

func TestPipelinePriorLabelFallback(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, testCorpus(from))
	// Prior topic centroid-similar to the e1 blob, with an existing label
	// and version. The LLM is down and there is no text: §10.6 says the
	// prior label carries over and the version still bumps.
	h.centroids.prior = []Centroid{
		{TopicID: "prior-e1", ParentID: ZeroUUID, Vector: mix(map[int]float32{1: 1})},
	}
	h.topics.prior = []TopicRow{
		{TopicID: "prior-e1", ParentID: ZeroUUID, Label: "Old Label", Description: "Old desc", Version: 3},
	}

	if _, err := h.runner.Run(context.Background(), scheduledDecision()); err != nil {
		t.Fatal(err)
	}
	var reused *TopicRow
	for i, row := range h.topics.upserted {
		if row.TopicID == "prior-e1" {
			reused = &h.topics.upserted[i]
		}
		if row.Version != 4 {
			t.Errorf("row %s version = %d, want 4 (prior 3 + 1)", row.TopicID, row.Version)
		}
	}
	if reused == nil {
		t.Fatalf("prior topic id not reused; rows: %+v", h.topics.upserted)
	}
	if reused.Label != "Old Label" || reused.Description != "Old desc" {
		t.Errorf("prior label not carried: %q / %q", reused.Label, reused.Description)
	}
}

func TestPipelineLLMTimeoutFallsBackToPrior(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, testCorpus(from))
	h.centroids.prior = []Centroid{
		{TopicID: "prior-e1", ParentID: ZeroUUID, Vector: mix(map[int]float32{1: 1})},
	}
	h.topics.prior = []TopicRow{
		{TopicID: "prior-e1", ParentID: ZeroUUID, Label: "Old Label", Version: 1},
	}
	// Text matches the e1 topic but the LLM errors (e.g. timeout): the
	// prior label wins over the fresh sample.
	h.texts.texts = []string{"game night"}
	h.embed.byText["game night"] = mix(map[int]float32{1: 1})
	h.llm = &fakeLLM{err: context.DeadlineExceeded}
	h.runner.LLM = h.llm

	if _, err := h.runner.Run(context.Background(), scheduledDecision()); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range h.topics.upserted {
		if row.TopicID == "prior-e1" && row.Label == "Old Label" {
			found = true
		}
	}
	if !found {
		t.Errorf("timeout did not fall back to prior label; rows: %+v", h.topics.upserted)
	}
	if len(h.llm.calls) == 0 {
		t.Error("llm was never consulted")
	}
}

func TestPipelineUsageEvents(t *testing.T) {
	from, to := testWindow()
	// Scheduled trigger.
	h := newHarness(t, testCorpus(from))
	if _, err := h.runner.Run(context.Background(), scheduledDecision()); err != nil {
		t.Fatal(err)
	}
	if len(h.usage.events) != 1 {
		t.Fatalf("scheduled run emitted %d events, want 1", len(h.usage.events))
	}
	ev := h.usage.events[0]
	if ev.EventType != contracts.EventMessagesReclustered {
		t.Errorf("event_type = %s", ev.EventType)
	}
	if ev.Quantity != 40 {
		t.Errorf("quantity = %d, want 40 points processed", ev.Quantity)
	}
	if ev.CreatorID != "cr-1" || !ev.WindowStart.Equal(from) || !ev.WindowEnd.Equal(to) {
		t.Errorf("event window/creator wrong: %+v", ev)
	}
	if want := "job:2026-08-01T00:00:00Z/2026-08-02T00:00:00Z:cr-1"; ev.IdempotencyKey != want {
		t.Errorf("idempotency_key = %q, want %q", ev.IdempotencyKey, want)
	}

	// On-demand trigger keys by job id.
	h2 := newHarness(t, testCorpus(from))
	d := Decision{CreatorID: "cr-1", Trigger: TriggerOnDemand, From: from, To: to,
		JobID: ReclusterJobID("cr-1", from, to)}
	if _, err := h2.runner.Run(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if len(h2.usage.events) != 1 {
		t.Fatalf("on-demand run emitted %d events, want 1", len(h2.usage.events))
	}
	if want := "recluster:" + d.JobID + ":cr-1"; h2.usage.events[0].IdempotencyKey != want {
		t.Errorf("idempotency_key = %q, want %q", h2.usage.events[0].IdempotencyKey, want)
	}
}

func TestPipelineCapAndNoClusterPath(t *testing.T) {
	from, _ := testWindow()
	h := newHarness(t, testCorpus(from))
	h.runner.Cfg.MaxPoints = 10 // below min_cluster_size total structure

	res, err := h.runner.Run(context.Background(), scheduledDecision())
	if err != nil {
		t.Fatal(err)
	}
	if res.PointsRead != 40 || res.PointsProcessed != 10 {
		t.Errorf("read/processed = %d/%d, want 40/10", res.PointsRead, res.PointsProcessed)
	}
	// 10 points < coarse min_cluster_size 15: no clusters; existing
	// topics and centroids must be left untouched.
	if res.Topics != 0 || len(h.centroids.replaced) != 0 || len(h.topics.upserted) != 0 || h.topics.deleteCall {
		t.Errorf("thin run must not rewrite state: %+v", res)
	}
	// The compute still happened and is still billed.
	if len(h.usage.events) != 1 || h.usage.events[0].Quantity != 10 {
		t.Errorf("usage events = %+v, want one with quantity 10", h.usage.events)
	}
}

func TestPipelineWindowFiltersRecords(t *testing.T) {
	from, to := testWindow()
	recs := testCorpus(from)
	// One record before the window, one at To (exclusive): both dropped.
	recs = append(recs,
		EmbeddingRecord{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: from.Add(-time.Hour), Vector: mix(map[int]float32{3: 1})},
		EmbeddingRecord{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: to, Vector: mix(map[int]float32{3: 1})},
	)
	h := newHarness(t, recs)
	res, err := h.runner.Run(context.Background(), scheduledDecision())
	if err != nil {
		t.Fatal(err)
	}
	if res.PointsRead != 40 {
		t.Errorf("points read = %d, want 40 (out-of-window records dropped)", res.PointsRead)
	}
}

func TestPipelineEmptyWindow(t *testing.T) {
	h := newHarness(t, nil)
	h.store.objs = nil
	res, err := h.runner.Run(context.Background(), scheduledDecision())
	if err != nil {
		t.Fatal(err)
	}
	if res.PointsRead != 0 || len(h.usage.events) != 0 || len(h.centroids.replaced) != 0 {
		t.Errorf("empty window must be a no-op: %+v, usage %+v", res, h.usage.events)
	}
}
