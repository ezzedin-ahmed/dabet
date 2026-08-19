// Package tail consumes flagged.v1 during a run and measures the
// latency SLI from the events themselves.
//
// moderation-service already exports moderation_e2e_latency_seconds,
// and that histogram is the contractual SLI of §4.6 — but it has eleven
// buckets and the N1 target of 1.5 s falls between the 1 s and 2.5 s
// bounds, so a p95 near the target is decided by interpolation. Reading
// flagged_at - ingested_at off the topic gives the same quantity at
// microsecond resolution, as a cross-check. Both are reported; a
// disagreement between them is itself a finding.
//
// The tailer joins its own consumer group, starting at the END of the
// topic, so it never competes with review-service or insights-service
// for delivery and never replays a previous run's verdicts.
package tail

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/test/load/internal/gen"
	"dabet/test/load/internal/hist"
)

// Tailer reads flagged.v1 and records the SLI.
type Tailer struct {
	cl  *kgo.Client
	lat *hist.Recorder

	// arrival is flagged_at measured against the harness's own clock at
	// the moment the verdict was read, which bounds the whole loop
	// (produce -> moderate -> publish -> consume) rather than the
	// service-internal segment the SLI covers.
	arrival *hist.Recorder

	mu       sync.Mutex
	byDetect map[string]int64
	byAction map[string]int64
	// byContent is only populated when the scenario asks for it (the
	// sampler-coverage run). content_id must never become a metric
	// label (§4.5 cardinality rule) — this is a harness-local tally of
	// the harness's own synthetic ids, not an exported series.
	byContent  map[string]int64
	perContent bool

	count atomic.Int64
	// prefix restricts accounting to this run's messages; the harness
	// mints message_ids with a per-run prefix so a stale verdict from a
	// previous run cannot contaminate the numbers.
	prefix string

	first atomic.Int64 // unix nanos of the first verdict seen
	last  atomic.Int64
}

// New joins a fresh group at the end of flagged.v1.
func New(brokers []string, group, prefix string) (*Tailer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(contracts.TopicFlagged),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Tailer{
		cl:        cl,
		lat:       hist.New(),
		arrival:   hist.New(),
		byDetect:  map[string]int64{},
		byAction:  map[string]int64{},
		byContent: map[string]int64{},
		prefix:    prefix,
	}, nil
}

// TrackPerContent turns on the per-content verdict tally the sampler
// scenario needs to reconstruct §7.5's coverage table empirically.
func (t *Tailer) TrackPerContent() { t.perContent = true }

// Run consumes until ctx ends.
func (t *Tailer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := t.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		now := time.Now()
		fetches.EachRecord(func(rec *kgo.Record) {
			var f contracts.Flagged
			if err := json.Unmarshal(rec.Value, &f); err != nil {
				return
			}
			if t.prefix != "" && !strings.HasPrefix(f.MessageID, t.prefix) {
				return
			}
			t.count.Add(1)
			// flagged.v1 carries flagged_at but not ingested_at, so the
			// intended send time comes back out of the harness-minted
			// message_id (see gen.MintMessageID).
			if sent, ok := gen.DecodeIntendedSend(f.MessageID); ok {
				t.lat.Record(f.FlaggedAt.Sub(sent))
				t.arrival.Record(now.Sub(sent))
			}
			t.stamp(now)
			t.mu.Lock()
			t.byDetect[string(f.Detector)]++
			t.byAction[string(f.Action)]++
			if t.perContent {
				t.byContent[f.ContentID]++
			}
			t.mu.Unlock()
		})
	}
}

func (t *Tailer) stamp(now time.Time) {
	ns := now.UnixNano()
	t.first.CompareAndSwap(0, ns)
	t.last.Store(ns)
}

// Close leaves the group.
func (t *Tailer) Close() { t.cl.Close() }

// Result is the tailer's contribution to the run report.
type Result struct {
	Verdicts         int64            `json:"verdicts"`
	SLILatency       hist.Summary     `json:"sli_latency_from_topic"`
	ArrivalLatency   hist.Summary     `json:"arrival_latency_from_topic"`
	FractionUnder1s5 float64          `json:"fraction_under_1s5"`
	ByDetector       map[string]int64 `json:"by_detector"`
	ByAction         map[string]int64 `json:"by_action"`
	ByContent        map[string]int64 `json:"by_content,omitempty"`
	VerdictRate      float64          `json:"verdict_rate_per_s"`
}

// Result snapshots what the tailer saw.
func (t *Tailer) Result() Result {
	t.mu.Lock()
	det := make(map[string]int64, len(t.byDetect))
	for k, v := range t.byDetect {
		det[k] = v
	}
	act := make(map[string]int64, len(t.byAction))
	for k, v := range t.byAction {
		act[k] = v
	}
	var byContent map[string]int64
	if t.perContent {
		byContent = make(map[string]int64, len(t.byContent))
		for k, v := range t.byContent {
			byContent[k] = v
		}
	}
	t.mu.Unlock()

	r := Result{
		Verdicts:         t.count.Load(),
		SLILatency:       t.lat.Summarize(),
		ArrivalLatency:   t.arrival.Summarize(),
		FractionUnder1s5: t.lat.FractionAtMost(1500 * time.Millisecond),
		ByDetector:       det,
		ByAction:         act,
		ByContent:        byContent,
	}
	if f, l := t.first.Load(), t.last.Load(); f > 0 && l > f {
		r.VerdictRate = float64(r.Verdicts) / (time.Duration(l - f).Seconds())
	}
	return r
}
