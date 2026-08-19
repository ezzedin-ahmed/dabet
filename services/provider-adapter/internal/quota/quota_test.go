package quota

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock lets the budget be advanced without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestPaceSpreadsTheAllowanceAcrossStreams(t *testing.T) {
	// The documented model: 10 000 units/day at 5 units per
	// liveChatMessages.list call is 2 000 polls/day, one every 43.2 s.
	b := New(10000)
	if got := b.Pace(5, 1); got != 43200*time.Millisecond {
		t.Errorf("Pace(5,1) = %s, want 43.2s", got)
	}
	// Ten concurrent chats share the same allowance, so each waits ten
	// times as long.
	if got := b.Pace(5, 10); got != 432*time.Second {
		t.Errorf("Pace(5,10) = %s, want 432s", got)
	}
	if got := b.Pace(5, 0); got != 43200*time.Millisecond {
		t.Errorf("Pace with no streams should behave as one: %s", got)
	}
	if got := Unlimited().Pace(5, 100); got != 0 {
		t.Errorf("unlimited Pace = %s, want 0", got)
	}
}

func TestReserveSpendsAndRefills(t *testing.T) {
	c := &fakeClock{t: time.Unix(0, 0)}
	b := New(100)
	b.Now = c.now

	ctx := context.Background()
	// Draw the whole allowance down without blocking.
	for i := 0; i < 10; i++ {
		if err := b.Reserve(ctx, 10); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	if got := b.Remaining(); got > 0.001 {
		t.Fatalf("remaining = %v, want ~0", got)
	}
	// A tenth of a day refills a tenth of the allowance.
	c.advance(Day / 10)
	if got := b.Remaining(); got < 9.9 || got > 10.1 {
		t.Errorf("remaining after 2.4h = %v, want ~10", got)
	}
	// Refill is capped at the daily allowance, not accumulated forever.
	c.advance(5 * Day)
	if got := b.Remaining(); got != 100 {
		t.Errorf("remaining after 5 days = %v, want 100", got)
	}
}

func TestReserveHonoursCancellation(t *testing.T) {
	c := &fakeClock{t: time.Unix(0, 0)}
	b := New(100)
	b.Now = c.now
	if err := b.Reserve(context.Background(), 100); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// The clock never advances, so the next reservation can never be
	// satisfied; cancellation must still return promptly.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Reserve(ctx, 50) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Reserve returned nil after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reserve did not return on cancellation")
	}
}

func TestReserveIsFairSoCheapCallersCannotStarveExpensiveOnes(t *testing.T) {
	b := New(100)
	if err := b.Reserve(context.Background(), 100); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// Refill is 100 units/day, far slower than the cheap caller's appetite.
	// Without arrival-order fairness the 1-unit caller would take every
	// unit as it accrued and the 5-unit caller would wait forever.
	b.Now = func() time.Time { return time.Now().Add(Day) } // instantly full

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	big := make(chan error, 1)
	go func() { big <- b.Reserve(ctx, 50) }()
	for i := 0; i < 20; i++ {
		_ = b.Reserve(ctx, 1)
	}
	select {
	case err := <-big:
		if err != nil {
			t.Errorf("expensive reservation starved: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expensive reservation never completed")
	}
}

func TestDrainEmptiesTheBucket(t *testing.T) {
	b := New(10000)
	b.Drain()
	if got := b.Remaining(); got > 0.001 {
		t.Errorf("remaining after Drain = %v, want ~0", got)
	}
	// Draining an unlimited budget is a no-op, not a lockup.
	u := Unlimited()
	u.Drain()
	if err := u.Reserve(context.Background(), 500); err != nil {
		t.Errorf("unlimited reserve after drain: %v", err)
	}
}

func TestReserveDoesNotDeadlockOnAnImpossibleCost(t *testing.T) {
	b := New(10)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Reserve(ctx, 1000); err != nil {
		t.Errorf("a cost above the whole allowance should proceed, not block: %v", err)
	}
}
