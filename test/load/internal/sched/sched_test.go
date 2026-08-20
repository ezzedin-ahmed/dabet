package sched

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

// A steady profile must place message i at exactly i/rate. This is the
// ideal clock the whole coordinated-omission story rests on: it is
// computed from the profile alone and never from when a send actually
// happened.
func TestSteadyIsExact(t *testing.T) {
	s, err := Compile(Steady(1000, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Total(); got != 10_000 {
		t.Fatalf("Total() = %d, want 10000", got)
	}
	for _, i := range []int64{0, 1, 500, 5000, 9999} {
		want := time.Duration(float64(i) / 1000 * float64(time.Second))
		if got := s.At(i); absDur(got-want) > time.Microsecond {
			t.Errorf("At(%d) = %v, want %v", i, got, want)
		}
	}
}

// A ramp must deliver the area under its own rate curve, and its
// inverse must be the analytic one — not a stepwise approximation that
// would smear the knee the ramp scenario exists to find.
func TestRampIntegratesItsOwnCurve(t *testing.T) {
	s, err := Compile(Ramp(0, 2000, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Area of the triangle: 0.5 * 2000 * 10 = 10 000.
	if got := s.Total(); got != 10_000 {
		t.Fatalf("Total() = %d, want 10000", got)
	}
	// n(t) = 100 t^2, so t(n) = sqrt(n/100).
	for _, i := range []int64{0, 100, 2500, 9999} {
		want := time.Duration(math.Sqrt(float64(i)/100) * float64(time.Second))
		if got := s.At(i); absDur(got-want) > 2*time.Millisecond {
			t.Errorf("At(%d) = %v, want %v", i, got, want)
		}
	}
	// Halfway through a linear ramp, only a quarter of the messages
	// have been scheduled: a naive uniform schedule would say half.
	if got := s.At(5000).Seconds(); math.Abs(got-math.Sqrt(50)) > 0.02 {
		t.Errorf("At(5000) = %.3fs, want %.3fs", got, math.Sqrt(50))
	}
}

// A staircase must hold each plateau at a constant rate.
func TestStepsPlateaus(t *testing.T) {
	s, err := Compile(Steps(1000, 3000, 3, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// 10s at 1000 + 10s at 2000 + 10s at 3000 = 60 000.
	if got := s.Total(); got != 60_000 {
		t.Fatalf("Total() = %d, want 60000", got)
	}
	for _, c := range []struct {
		off  time.Duration
		want float64
	}{{time.Second, 1000}, {15 * time.Second, 2000}, {25 * time.Second, 3000}} {
		if got := s.RateAt(c.off); math.Abs(got-c.want) > 1 {
			t.Errorf("RateAt(%v) = %v, want %v", c.off, got, c.want)
		}
	}
	// The plateau boundaries fall where the cumulative counts say.
	if got := s.At(10_000); absDur(got-10*time.Second) > time.Millisecond {
		t.Errorf("first plateau ends at %v, want 10s", got)
	}
	if got := s.At(30_000); absDur(got-20*time.Second) > time.Millisecond {
		t.Errorf("second plateau ends at %v, want 20s", got)
	}
}

func TestSpikeShape(t *testing.T) {
	p := Spike(100, 5000, 2*time.Second, time.Second, 2*time.Second)
	s, err := Compile(p)
	if err != nil {
		t.Fatal(err)
	}
	// 200 + 5000 + 200.
	if got := s.Total(); got != 5400 {
		t.Fatalf("Total() = %d, want 5400", got)
	}
	if got := s.RateAt(2500 * time.Millisecond); got != 5000 {
		t.Errorf("rate during the spike = %v, want 5000", got)
	}
}

func TestProfileValidation(t *testing.T) {
	if _, err := Compile(Profile{}); err == nil {
		t.Error("empty profile accepted")
	}
	if _, err := Compile(Profile{Segments: []Segment{{Duration: 0, From: 1, To: 1}}}); err == nil {
		t.Error("zero-duration segment accepted")
	}
	if _, err := Compile(Profile{Segments: []Segment{{Duration: time.Second, From: -1}}}); err == nil {
		t.Error("negative rate accepted")
	}
}

// The driver must emit every scheduled message exactly once, and the
// intended times it hands out must be the schedule's — not the wall
// clock at which the emit happened. A generator that re-derived the
// intended time from time.Now() would silently omit exactly the slow
// samples that matter.
func TestDriverEmitsIdealClockTimes(t *testing.T) {
	s, err := Compile(Steady(2000, 500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	seen := map[int64]time.Time{}
	d := &Driver{Schedule: s, Workers: 4, Granularity: 500 * time.Microsecond}
	start := time.Now()
	st := d.Run(context.Background(), func(idx int64, intended time.Time, _ time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		if _, dup := seen[idx]; dup {
			t.Errorf("index %d emitted twice", idx)
		}
		seen[idx] = intended
	})
	if st.Sent != s.Total() {
		t.Fatalf("sent %d of %d", st.Sent, s.Total())
	}
	if int64(len(seen)) != s.Total() {
		t.Fatalf("distinct indices %d, want %d", len(seen), s.Total())
	}
	for _, i := range []int64{0, 1, 999} {
		want := st.Start.Add(s.At(i))
		if !seen[i].Equal(want) {
			t.Errorf("intended time for %d = %v, want %v", i, seen[i], want)
		}
	}
	// Sanity: a 500 ms profile should not take wildly longer.
	if el := time.Since(start); el > 3*time.Second {
		t.Errorf("driving a 500ms profile took %v", el)
	}
}

// The lag the driver reports must be measured against the intended
// time. A slow emit must push lag onto the FOLLOWING messages rather
// than sliding the schedule — sliding is exactly coordinated omission.
func TestDriverLagIsAgainstIntendedTime(t *testing.T) {
	s, err := Compile(Steady(1000, 200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var lags []time.Duration
	var intended []time.Time
	stall := 60 * time.Millisecond

	d := &Driver{Schedule: s, Workers: 1, Granularity: 200 * time.Microsecond}
	n := 0
	st := d.Run(context.Background(), func(_ int64, at time.Time, lag time.Duration) {
		mu.Lock()
		n++
		if n == 5 {
			time.Sleep(stall)
		}
		lags = append(lags, lag)
		intended = append(intended, at)
		mu.Unlock()
	})
	if st.Sent != s.Total() {
		t.Fatalf("sent %d of %d", st.Sent, s.Total())
	}
	// The intended times are untouched by the stall: still 1 ms apart.
	for i := 1; i < len(intended); i++ {
		if gap := intended[i].Sub(intended[i-1]); absDur(gap-time.Millisecond) > 10*time.Microsecond {
			t.Fatalf("intended gap %d = %v, want 1ms — the schedule slid with the stall", i, gap)
		}
	}
	// And the stall is visible as lag on the messages after it.
	peak := time.Duration(0)
	for _, l := range lags {
		if l > peak {
			peak = l
		}
	}
	if peak < stall/2 {
		t.Fatalf("peak observed lag %v; a %v stall should have been recorded", peak, stall)
	}
}

// Rate accuracy: over a window long enough to swamp scheduling
// granularity, the achieved rate must be within a few percent of the
// requested one.
func TestDriverRateAccuracy(t *testing.T) {
	const rate = 5000
	s, err := Compile(Steady(rate, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{Schedule: s, Workers: 4, Granularity: 200 * time.Microsecond}
	start := time.Now()
	st := d.Run(context.Background(), func(int64, time.Time, time.Duration) {})
	elapsed := time.Since(start)
	got := float64(st.Sent) / elapsed.Seconds()
	if math.Abs(got-rate)/rate > 0.10 {
		t.Errorf("achieved %.0f msg/s over %v, want ~%d (within 10%%)", got, elapsed, rate)
	}
}

// A cancelled run must stop promptly and account for what it did not
// send, rather than reporting the full schedule as delivered.
func TestDriverCancellation(t *testing.T) {
	s, err := Compile(Steady(1000, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	d := &Driver{Schedule: s, Workers: 2, Granularity: 500 * time.Microsecond}
	st := d.Run(ctx, func(int64, time.Time, time.Duration) {})
	if st.Sent >= s.Total() {
		t.Fatalf("cancelled run sent %d of %d", st.Sent, s.Total())
	}
	if st.Skipped != s.Total()-st.Sent {
		t.Errorf("Skipped = %d, want %d", st.Skipped, s.Total()-st.Sent)
	}
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
