package retry

import (
	"context"
	"testing"
	"time"
)

func TestNextDoublesAndCaps(t *testing.T) {
	// Jitter pinned to 0.5 makes the step exactly the exponential value:
	// d/2 + 0.5*d == d.
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second, Jitter: func() float64 { return 0.5 }}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := b.Next(); got != w*time.Millisecond {
			t.Errorf("step %d = %s, want %s", i, got, w*time.Millisecond)
		}
	}
	b.Reset()
	if got := b.Next(); got != 100*time.Millisecond {
		t.Errorf("after Reset = %s, want the first step back", got)
	}
}

func TestJitterSpreadsReconnects(t *testing.T) {
	// The whole point of jitter: thousands of connections on one instance
	// must not reconnect in lockstep after a provider blip.
	lo := Backoff{Base: time.Second, Max: time.Minute, Jitter: func() float64 { return 0 }}
	hi := Backoff{Base: time.Second, Max: time.Minute, Jitter: func() float64 { return 0.999 }}
	if got := lo.Next(); got != 500*time.Millisecond {
		t.Errorf("floor = %s, want half the step", got)
	}
	if got := hi.Next(); got <= time.Second || got > 1500*time.Millisecond {
		t.Errorf("ceiling = %s, want just under 1.5x the step", got)
	}
}

func TestZeroValuesFallBackToDefaults(t *testing.T) {
	var b Backoff
	b.Jitter = func() float64 { return 0.5 }
	if got := b.Next(); got != 500*time.Millisecond {
		t.Errorf("zero-value first step = %s, want the 500ms default", got)
	}
}

func TestWaitReturnsOnCancellation(t *testing.T) {
	b := Backoff{Base: time.Hour, Max: time.Hour, Jitter: func() float64 { return 0.5 }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Wait(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Wait should report the cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait ignored cancellation")
	}
}

func TestSleepZeroChecksCancellationWithoutBlocking(t *testing.T) {
	if err := Sleep(context.Background(), 0); err != nil {
		t.Errorf("Sleep(0) = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, 0); err == nil {
		t.Error("Sleep(0) on a cancelled context should report it")
	}
}
