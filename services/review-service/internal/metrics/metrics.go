// Package metrics registers the review-service metrics of docs §7.11 —
// review_queue_lag_seconds and review_pending_estimate — plus
// review_windows_lost_total, a documented addition counting §7.6.3
// silently-lost review windows (cursor fell behind retention, or the
// topic's partition count changed under a stored cursor).
//
// Deviation from the §7.11 table (documented): both gauges carry a
// creator_id label. §7.6.3 requires lag to be exposed "per creator", which
// the label-less table row cannot do. Cardinality is bounded by the
// tracker's idle expiry: creators inactive longer than maxIdle have their
// label series deleted.
//
// Sampling (documented design): a full lag computation needs a Kafka scan
// to find the front pending message, which is too expensive to do
// per-creator on a timer. Instead the handler observes the front pending
// message's flagged_at on every GET (lazy, exact at that moment), and a
// cheap background sampler re-ages that observation for recently-active
// creators between GETs: lag(t) = t - front_flagged_at, an O(1) update
// with no Kafka reads. The gauge is therefore exact on read paths and a
// monotonically-aging estimate between them. P4: only timestamps, counts,
// and creator ids are tracked — never message text.
package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Area C review metrics.
type Metrics struct {
	QueueLag        *prometheus.GaugeVec   // creator_id
	PendingEstimate *prometheus.GaugeVec   // creator_id
	WindowsLost     *prometheus.CounterVec // creator_id
}

// New registers the review metrics on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		QueueLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "review_queue_lag_seconds",
			Help: "Age of the oldest unreviewed message in the creator's queue (now - flagged_at at the cursor). Exact on each GET, re-aged between GETs for recently-active creators.",
		}, []string{"creator_id"}),
		PendingEstimate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "review_pending_estimate",
			Help: "Estimated pending reviews for the creator: exact when the last scan reached the high watermark, otherwise the partition offset span (upper bound; other creators' events interleave).",
		}, []string{"creator_id"}),
		WindowsLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "review_windows_lost_total",
			Help: "Review windows lost per docs §7.6.3: the cursor fell behind the earliest retained offset (or the partition mapping changed) and was snapped forward.",
		}, []string{"creator_id"}),
	}
	reg.MustRegister(m.QueueLag, m.PendingEstimate, m.WindowsLost)
	return m
}

type lagEntry struct {
	front      time.Time // flagged_at of the message at the cursor
	hasPending bool
	lastActive time.Time
}

// LagTracker keeps the per-creator lag observations behind the sampler.
type LagTracker struct {
	m   *Metrics
	now func() time.Time

	mu      sync.Mutex
	entries map[string]lagEntry
}

// NewLagTracker builds a tracker over m. now is injectable for tests.
func NewLagTracker(m *Metrics, now func() time.Time) *LagTracker {
	if now == nil {
		now = time.Now
	}
	return &LagTracker{m: m, now: now, entries: make(map[string]lagEntry)}
}

// Observe records the queue front seen by a GET: the flagged_at of the
// first pending message (ignored when hasPending is false) and the
// pending estimate. It sets both gauges immediately.
func (t *LagTracker) Observe(creatorID string, front time.Time, hasPending bool, pending float64) {
	now := t.now()
	t.mu.Lock()
	t.entries[creatorID] = lagEntry{front: front, hasPending: hasPending, lastActive: now}
	t.mu.Unlock()

	lag := 0.0
	if hasPending {
		lag = now.Sub(front).Seconds()
	}
	t.m.QueueLag.WithLabelValues(creatorID).Set(lag)
	t.m.PendingEstimate.WithLabelValues(creatorID).Set(pending)
}

// Invalidate drops the tracked front after a POST advanced the cursor:
// the old front is reviewed, and the next GET re-observes. The lag gauge
// is zeroed rather than left aging on stale data; pending is updated.
func (t *LagTracker) Invalidate(creatorID string, pending float64) {
	now := t.now()
	t.mu.Lock()
	t.entries[creatorID] = lagEntry{hasPending: false, lastActive: now}
	t.mu.Unlock()
	t.m.QueueLag.WithLabelValues(creatorID).Set(0)
	t.m.PendingEstimate.WithLabelValues(creatorID).Set(pending)
}

// Sample re-ages the lag gauge for every tracked creator and expires
// entries (and their label series) idle longer than maxIdle. O(active
// creators), no I/O.
func (t *LagTracker) Sample(maxIdle time.Duration) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, e := range t.entries {
		if now.Sub(e.lastActive) > maxIdle {
			delete(t.entries, id)
			t.m.QueueLag.DeleteLabelValues(id)
			t.m.PendingEstimate.DeleteLabelValues(id)
			continue
		}
		if e.hasPending {
			t.m.QueueLag.WithLabelValues(id).Set(now.Sub(e.front).Seconds())
		}
	}
}

// Run samples on interval until ctx ends.
func (t *LagTracker) Run(ctx context.Context, interval, maxIdle time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.Sample(maxIdle)
		}
	}
}
