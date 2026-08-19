package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestLagTrackerObserveSampleAndExpire(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	m := New(prometheus.NewRegistry())
	tr := NewLagTracker(m, func() time.Time { return now })

	front := now.Add(-90 * time.Second)
	tr.Observe("c1", front, true, 7)

	if lag := testutil.ToFloat64(m.QueueLag.WithLabelValues("c1")); lag != 90 {
		t.Errorf("lag after Observe = %v, want 90", lag)
	}
	if p := testutil.ToFloat64(m.PendingEstimate.WithLabelValues("c1")); p != 7 {
		t.Errorf("pending = %v, want 7", p)
	}

	// The sampler re-ages the observation without any I/O.
	now = now.Add(60 * time.Second)
	tr.Sample(time.Hour)
	if lag := testutil.ToFloat64(m.QueueLag.WithLabelValues("c1")); lag != 150 {
		t.Errorf("lag after Sample = %v, want 150", lag)
	}

	// An empty queue observation pins lag to zero and stays there.
	tr.Observe("c2", time.Time{}, false, 0)
	tr.Sample(time.Hour)
	if lag := testutil.ToFloat64(m.QueueLag.WithLabelValues("c2")); lag != 0 {
		t.Errorf("empty-queue lag = %v, want 0", lag)
	}

	// A POST invalidation zeroes lag until the next GET re-observes.
	tr.Invalidate("c1", 2)
	if lag := testutil.ToFloat64(m.QueueLag.WithLabelValues("c1")); lag != 0 {
		t.Errorf("lag after Invalidate = %v, want 0", lag)
	}
	if p := testutil.ToFloat64(m.PendingEstimate.WithLabelValues("c1")); p != 2 {
		t.Errorf("pending after Invalidate = %v, want 2", p)
	}

	// Idle creators fall out of the tracker and the gauge series.
	now = now.Add(2 * time.Hour)
	tr.Sample(time.Hour)
	if n := testutil.CollectAndCount(m.QueueLag); n != 0 {
		t.Errorf("idle creators not expired: %d series remain", n)
	}
}
