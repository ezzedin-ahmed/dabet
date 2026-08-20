package hist

import (
	"sync"
	"testing"
	"time"
)

// Quantiles must round UP to the containing bucket's edge: the harness
// must never flatter the system it is measuring.
func TestQuantilesNeverUnderReport(t *testing.T) {
	r := New()
	for i := 1; i <= 1000; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}
	for _, c := range []struct{ q, want float64 }{
		{0.50, 500}, {0.95, 950}, {0.99, 990},
	} {
		got := float64(r.Quantile(c.q)) / float64(time.Millisecond)
		if got < c.want {
			t.Errorf("q%.2f = %.2f ms, must be >= the true %.0f ms", c.q, got, c.want)
		}
		// 32 linear sub-buckets per octave bounds the over-report at
		// about 3.2%.
		if got > c.want*1.04 {
			t.Errorf("q%.2f = %.2f ms, more than 4%% above the true %.0f ms", c.q, got, c.want)
		}
	}
	if r.Count() != 1000 {
		t.Errorf("count = %d", r.Count())
	}
	if got := r.Min(); got != time.Millisecond {
		t.Errorf("min = %v", got)
	}
	if got := r.Max(); got != time.Second {
		t.Errorf("max = %v", got)
	}
	if got := float64(r.Mean()) / float64(time.Millisecond); got < 500 || got > 501 {
		t.Errorf("mean = %v ms", got)
	}
}

// The N1 check reduces to a fraction-under-target, taken from bucket
// counts with no interpolation.
func TestFractionAtMost(t *testing.T) {
	r := New()
	for range 900 {
		r.Record(100 * time.Millisecond)
	}
	for range 100 {
		r.Record(5 * time.Second)
	}
	if got := r.FractionAtMost(1500 * time.Millisecond); got < 0.89 || got > 0.91 {
		t.Errorf("fraction under 1.5s = %v, want ~0.9", got)
	}
	if got := r.FractionAtMost(time.Microsecond); got != 0 {
		t.Errorf("fraction under 1us = %v, want 0", got)
	}
	if got := r.FractionAtMost(time.Hour); got != 1 {
		t.Errorf("fraction under an hour = %v, want 1", got)
	}
}

func TestEmpty(t *testing.T) {
	r := New()
	if r.Count() != 0 || r.Mean() != 0 || r.Min() != 0 || r.Quantile(0.95) != 0 {
		t.Error("an empty recorder must report zeros, not garbage")
	}
	s := r.Summarize()
	if s.Count != 0 {
		t.Errorf("summary of an empty recorder = %+v", s)
	}
}

// Sub-microsecond and negative durations (a clock step, or a send that
// beat its own schedule) must not corrupt the histogram.
func TestDegenerateValues(t *testing.T) {
	r := New()
	r.Record(0)
	r.Record(-5 * time.Millisecond)
	r.Record(500 * time.Nanosecond)
	if r.Count() != 3 {
		t.Fatalf("count = %d, want 3", r.Count())
	}
	if got := r.Quantile(0.99); got > time.Millisecond {
		t.Errorf("q99 of sub-microsecond values = %v", got)
	}
}

// The recorder is written from every send goroutine at once.
func TestConcurrentRecord(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 5000 {
				r.Record(time.Duration(w*1000+i) * time.Microsecond)
			}
		}(w)
	}
	wg.Wait()
	if got := r.Count(); got != 40_000 {
		t.Fatalf("count = %d, want 40000", got)
	}
}

// Monotonicity across the whole range: a bigger observation must never
// land in an earlier bucket.
func TestBucketMonotonic(t *testing.T) {
	prev := -1
	for micros := int64(0); micros < 1<<22; micros = micros*3/2 + 1 {
		b := bucketOf(micros)
		if b < prev {
			t.Fatalf("bucketOf(%d) = %d went backwards from %d", micros, b, prev)
		}
		if up := bucketUpper(b); up < float64(micros) {
			t.Fatalf("bucketUpper(%d) = %v is below its own value %d", b, up, micros)
		}
		prev = b
	}
}
