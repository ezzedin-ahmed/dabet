package mod

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"dabet/pkg/contracts"
)

func newTestUsage(t *testing.T) (*UsageAggregator, *fakeProducer, *fakeClock) {
	t.Helper()
	prod := &fakeProducer{}
	clock := newFakeClock(time.Date(2026, 8, 19, 14, 2, 30, 0, time.UTC))
	pub := NewPublisher(prod, 10*time.Millisecond, func() {})
	pub.baseDelay = time.Nanosecond
	pub.sleep = func(time.Duration) {}
	return NewUsageAggregator("mod-7", pub, clock.Now), prod, clock
}

func decodeUsage(t *testing.T, recs []producedRecord) map[string]contracts.Usage {
	t.Helper()
	out := make(map[string]contracts.Usage)
	for _, r := range recs {
		var u contracts.Usage
		if err := json.Unmarshal(r.Value, &u); err != nil {
			t.Fatal(err)
		}
		out[u.CreatorID] = u
	}
	return out
}

func TestUsageAggregationAndDeterministicKeys(t *testing.T) {
	u, prod, clock := newTestUsage(t)
	ctx := context.Background()

	u.Inc("creator-a")
	u.Inc("creator-a")
	u.Inc("creator-a")
	u.Inc("creator-b")

	// Still inside minute 14:02 — nothing due.
	u.FlushDue(ctx)
	if len(prod.records) != 0 {
		t.Fatal("open window must not flush")
	}

	clock.Advance(time.Minute) // now 14:03:30
	u.FlushDue(ctx)
	got := decodeUsage(t, prod.byTopic(contracts.TopicUsage))
	if len(got) != 2 {
		t.Fatalf("flushed %d events, want 2", len(got))
	}
	a := got["creator-a"]
	if a.Quantity != 3 || a.EventType != contracts.EventMessagesProcessed {
		t.Fatalf("creator-a event = %+v", a)
	}
	wantStart := time.Date(2026, 8, 19, 14, 2, 0, 0, time.UTC)
	if !a.WindowStart.Equal(wantStart) || !a.WindowEnd.Equal(wantStart.Add(time.Minute)) {
		t.Fatalf("window = [%v, %v]", a.WindowStart, a.WindowEnd)
	}
	if a.IdempotencyKey != "mod:mod-7:2026-08-19T14:02:creator-a" {
		t.Fatalf("idempotency key = %q", a.IdempotencyKey)
	}
	if got["creator-b"].Quantity != 1 {
		t.Fatalf("creator-b quantity = %d, want 1", got["creator-b"].Quantity)
	}

	// Flushing again emits nothing: windows are consumed.
	u.FlushDue(ctx)
	if len(prod.byTopic(contracts.TopicUsage)) != 2 {
		t.Fatal("windows must flush exactly once")
	}
}

func TestUsageMinuteRollover(t *testing.T) {
	u, prod, clock := newTestUsage(t)
	ctx := context.Background()

	u.Inc("c") // minute 14:02
	clock.Advance(time.Minute)
	u.Inc("c") // minute 14:03

	u.FlushDue(ctx) // only 14:02 is closed
	got := decodeUsage(t, prod.byTopic(contracts.TopicUsage))
	if len(got) != 1 || got["c"].Quantity != 1 {
		t.Fatalf("rollover flush = %+v, want one event of quantity 1", got)
	}
	if got["c"].IdempotencyKey != "mod:mod-7:2026-08-19T14:02:c" {
		t.Fatalf("key = %q", got["c"].IdempotencyKey)
	}

	// Shutdown flushes the still-open 14:03 window (§7.10).
	u.FlushAll(ctx)
	all := prod.byTopic(contracts.TopicUsage)
	if len(all) != 2 {
		t.Fatalf("after FlushAll got %d events, want 2", len(all))
	}
	var last contracts.Usage
	if err := json.Unmarshal(all[1].Value, &last); err != nil {
		t.Fatal(err)
	}
	if last.IdempotencyKey != "mod:mod-7:2026-08-19T14:03:c" {
		t.Fatalf("open-window key = %q", last.IdempotencyKey)
	}
}

func TestUsageKeyDeterminism(t *testing.T) {
	ws := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	k1 := UsageIdempotencyKey("host-1", ws, "9d4e")
	k2 := UsageIdempotencyKey("host-1", ws, "9d4e")
	if k1 != k2 || k1 != "mod:host-1:2026-08-19T14:00:9d4e" {
		t.Fatalf("keys %q / %q not deterministic", k1, k2)
	}
	if UsageIdempotencyKey("host-2", ws, "9d4e") == k1 {
		t.Fatal("different instances must produce different keys")
	}
	// Non-UTC input renders the same UTC minute.
	loc := time.FixedZone("plus2", 2*3600)
	if UsageIdempotencyKey("host-1", ws.In(loc), "9d4e") != k1 {
		t.Fatal("key must be timezone-stable")
	}
}
