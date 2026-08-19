package repo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/migrate"
)

// newTestRepo connects to POSTGRES_DSN and applies migrations. The whole
// file is skipped when the env var is unset, so the unit suite needs no
// database; the in-memory fake (internal/ledger) carries the same
// semantics and covers the handlers.
func newTestRepo(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping real-database ledger test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := migrate.Run(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return NewPostgres(pool)
}

// uid returns a UUID-shaped creator id unique to this test run.
func uid(t *testing.T) string {
	t.Helper()
	n := time.Now().UnixNano()
	return fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x",
		uint32(n), uint16(n>>32)&0xffff, uint16(n>>8)&0xfff, uint16(n>>20)&0xfff, uint64(n)&0xffffffffffff)
}

func TestPostgresApplyReplayAndReads(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	creator := uid(t)
	key := "test:" + creator

	res, err := r.Apply(ctx, creator, 100, ledger.ReasonTopup, key, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.Replayed || res.Balance != 100 {
		t.Fatalf("first apply: %+v", res)
	}

	res, err = r.Apply(ctx, creator, 100, ledger.ReasonTopup, key, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !res.Replayed || res.Balance != 100 {
		t.Fatalf("replay must return existing balance: %+v", res)
	}

	// Negative balances are allowed.
	res, err = r.Apply(ctx, creator, -150, ledger.ReasonAdjustment, "refund:"+creator, nil)
	if err != nil {
		t.Fatalf("negative apply: %v", err)
	}
	if res.Balance != -50 {
		t.Fatalf("balance = %d, want -50", res.Balance)
	}

	balance, updatedAt, found, err := r.Balance(ctx, creator)
	if err != nil || !found || balance != -50 || updatedAt.IsZero() {
		t.Fatalf("balance read: %d %v %v %v", balance, updatedAt, found, err)
	}
	if n, err := r.NegativeBalances(ctx); err != nil || n < 1 {
		t.Fatalf("NegativeBalances = %d, %v", n, err)
	}

	entries, err := r.Entries(ctx, creator, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].IdempotencyKey != "refund:"+creator || entries[1].Metadata["k"] != "v" {
		t.Fatalf("entries wrong: %+v", entries)
	}
	// Keyset pagination: strictly older than the newest entry.
	older, err := r.Entries(ctx, creator, entries[0].ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(older) != 1 || older[0].ID != entries[1].ID {
		t.Fatalf("pagination wrong: %+v", older)
	}

	if delta, found, err := r.LastTopup(ctx, creator); err != nil || !found || delta != 100 {
		t.Fatalf("LastTopup = %d %v %v", delta, found, err)
	}
	if _, found, err := r.LastTopup(ctx, uid(t)); err != nil || found {
		t.Fatalf("LastTopup for unknown creator: found=%v err=%v", found, err)
	}

	// Zero-row balance read.
	if balance, _, found, err := r.Balance(ctx, uid(t)); err != nil || found || balance != 0 {
		t.Fatalf("zero-row balance: %d %v %v", balance, found, err)
	}
}
