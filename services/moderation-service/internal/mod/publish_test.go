package mod

import (
	"context"
	"errors"
	"testing"
	"time"
)

type flakyProducer struct {
	failures int // fail the first N calls
	calls    int
	produced []producedRecord
}

func (f *flakyProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.calls++
	if f.calls <= f.failures {
		return errors.New("broker unavailable")
	}
	f.produced = append(f.produced, producedRecord{Topic: topic, Key: string(key), Value: value})
	return nil
}

func TestPublisherRetriesThenSucceeds(t *testing.T) {
	prod := &flakyProducer{failures: 3}
	dropped := 0
	p := NewPublisher(prod, time.Second, func() { dropped++ })
	p.baseDelay = time.Nanosecond
	slept := 0
	p.sleep = func(time.Duration) { slept++ }

	if !p.Publish(context.Background(), "flagged.v1", []byte("k"), []byte("v")) {
		t.Fatal("publish should succeed after retries")
	}
	if prod.calls != 4 || slept != 3 || dropped != 0 {
		t.Fatalf("calls=%d slept=%d dropped=%d, want 4/3/0", prod.calls, slept, dropped)
	}
}

func TestPublisherDropsAfterBudgetAndCountsFailOpen(t *testing.T) {
	prod := &flakyProducer{failures: 1 << 30}
	dropped := 0
	clock := newFakeClock(t0)
	p := NewPublisher(prod, 30*time.Second, func() { dropped++ })
	p.baseDelay = 100 * time.Millisecond
	p.now = clock.Now
	p.sleep = func(d time.Duration) { clock.Advance(d) } // fake time: no real sleeping

	if p.Publish(context.Background(), "flagged.v1", []byte("k"), []byte("v")) {
		t.Fatal("publish must report the drop")
	}
	if dropped != 1 {
		t.Fatalf("fail-open callback fired %d times, want 1", dropped)
	}
	// The retry loop must have stayed within the 30 s budget.
	if elapsed := clock.Now().Sub(t0); elapsed > 30*time.Second {
		t.Fatalf("retried for %v, budget is 30s", elapsed)
	}
	if prod.calls < 2 {
		t.Fatalf("expected multiple attempts, got %d", prod.calls)
	}
}

func TestPublisherStopsOnCancelledContext(t *testing.T) {
	prod := &flakyProducer{failures: 1 << 30}
	dropped := 0
	p := NewPublisher(prod, time.Hour, func() { dropped++ })
	p.baseDelay = time.Nanosecond
	p.sleep = func(time.Duration) {}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.Publish(ctx, "t", nil, nil) {
		t.Fatal("cancelled context must drop")
	}
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}
