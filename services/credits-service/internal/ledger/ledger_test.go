package ledger

import (
	"context"
	"testing"
)

func TestApplyIdempotentReplay(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	res, err := m.Apply(ctx, "c1", 100, ReasonTopup, "pi_1", nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.Replayed || res.Balance != 100 {
		t.Fatalf("first apply: got %+v, want applied balance 100", res)
	}

	// Same key again: one entry, balance moved once, replayed.
	res, err = m.Apply(ctx, "c1", 100, ReasonTopup, "pi_1", nil)
	if err != nil {
		t.Fatalf("replay apply: %v", err)
	}
	if !res.Replayed {
		t.Fatalf("replay not detected: %+v", res)
	}
	if res.Balance != 100 {
		t.Fatalf("replay balance = %d, want 100 (moved once)", res.Balance)
	}
	entries, err := m.Entries(ctx, "c1", 0, 10)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	balance, _, found, err := m.Balance(ctx, "c1")
	if err != nil || !found || balance != 100 {
		t.Fatalf("balance = %d found=%v err=%v, want 100 true nil", balance, found, err)
	}
}

func TestApplyReplayForDifferentCreatorKeepsKeyGlobal(t *testing.T) {
	// The unique index is on idempotency_key alone (§5.3): a second
	// creator reusing the key replays and gets their own balance.
	m := NewMemory()
	ctx := context.Background()
	if _, err := m.Apply(ctx, "c1", 100, ReasonTopup, "k", nil); err != nil {
		t.Fatal(err)
	}
	res, err := m.Apply(ctx, "c2", 50, ReasonTopup, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Replayed || res.Balance != 0 {
		t.Fatalf("got %+v, want replayed with c2's existing balance 0", res)
	}
}

func TestNegativeBalanceAllowed(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, err := m.Apply(ctx, "c1", 10, ReasonTopup, "t1", nil); err != nil {
		t.Fatal(err)
	}
	res, err := m.Apply(ctx, "c1", -25, ReasonAdjustment, "refund:ch_1", nil)
	if err != nil {
		t.Fatalf("negative apply must be allowed: %v", err)
	}
	if res.Balance != -15 {
		t.Fatalf("balance = %d, want -15", res.Balance)
	}
	n, err := m.NegativeBalances(ctx)
	if err != nil || n != 1 {
		t.Fatalf("NegativeBalances = %d, %v; want 1, nil", n, err)
	}
}

func TestBalanceZeroRow(t *testing.T) {
	m := NewMemory()
	balance, _, found, err := m.Balance(context.Background(), "nobody")
	if err != nil {
		t.Fatal(err)
	}
	if found || balance != 0 {
		t.Fatalf("got balance=%d found=%v, want 0 false", balance, found)
	}
}

func TestEntriesNewestFirstAndCursor(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		if _, err := m.Apply(ctx, "c1", 1, ReasonTopup, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Apply(ctx, "other", 1, ReasonTopup, "x", nil); err != nil {
		t.Fatal(err)
	}

	page, err := m.Entries(ctx, "c1", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || page[0].IdempotencyKey != "d" || page[2].IdempotencyKey != "b" {
		t.Fatalf("first page wrong: %+v", page)
	}
	rest, err := m.Entries(ctx, "c1", page[2].ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].IdempotencyKey != "a" {
		t.Fatalf("second page wrong: %+v", rest)
	}
}

func TestLastTopup(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, found, err := m.LastTopup(ctx, "c1"); err != nil || found {
		t.Fatalf("LastTopup on empty ledger: found=%v err=%v", found, err)
	}
	_, _ = m.Apply(ctx, "c1", 100, ReasonTopup, "t1", nil)
	_, _ = m.Apply(ctx, "c1", -5, "messages_processed", "u1", nil)
	_, _ = m.Apply(ctx, "c1", 200, ReasonTopup, "t2", nil)
	delta, found, err := m.LastTopup(ctx, "c1")
	if err != nil || !found || delta != 200 {
		t.Fatalf("LastTopup = %d found=%v err=%v, want 200 true nil", delta, found, err)
	}
}
