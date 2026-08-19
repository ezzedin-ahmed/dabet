// Package repo is the Postgres implementation of ledger.Repository over
// the billing schema of docs §5.3.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/services/credits-service/internal/ledger"
)

// Postgres implements ledger.Repository on a pgx pool.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

// Apply runs the §5.3 transaction verbatim:
//
//	BEGIN;
//	  INSERT INTO credit_entries (creator_id, delta, reason, idempotency_key)
//	  VALUES ($1, $2, $3, $4)
//	  ON CONFLICT (idempotency_key) DO NOTHING
//	  RETURNING id;
//	  -- if no row returned, this is a replay: COMMIT and return the existing balance
//
//	  INSERT INTO creator_balances (creator_id, balance) VALUES ($1, $2)
//	  ON CONFLICT (creator_id) DO UPDATE
//	    SET balance = creator_balances.balance + EXCLUDED.balance, updated_at = now();
//	COMMIT;
//
// (plus the metadata column, which §5.3's table has and its INSERT elides).
func (p *Postgres) Apply(ctx context.Context, creatorID string, delta int64, reason, idempotencyKey string, metadata map[string]any) (ledger.ApplyResult, error) {
	if metadata == nil {
		metadata = map[string]any{}
	}
	meta, err := json.Marshal(metadata)
	if err != nil {
		return ledger.ApplyResult{}, fmt.Errorf("marshal metadata: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ledger.ApplyResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var id int64
	err = tx.QueryRow(ctx, `
		INSERT INTO billing.credit_entries (creator_id, delta, reason, idempotency_key, metadata)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`,
		creatorID, delta, reason, idempotencyKey, meta).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Replay: nothing inserted, nothing adjusted. Commit and return
		// the existing balance.
		var balance int64
		err = tx.QueryRow(ctx,
			`SELECT balance FROM billing.creator_balances WHERE creator_id = $1`,
			creatorID).Scan(&balance)
		if errors.Is(err, pgx.ErrNoRows) {
			balance, err = 0, nil
		}
		if err != nil {
			return ledger.ApplyResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ledger.ApplyResult{}, err
		}
		return ledger.ApplyResult{Replayed: true, Balance: balance}, nil
	}
	if err != nil {
		return ledger.ApplyResult{}, err
	}

	var balance int64
	err = tx.QueryRow(ctx, `
		INSERT INTO billing.creator_balances (creator_id, balance) VALUES ($1, $2)
		ON CONFLICT (creator_id) DO UPDATE
		  SET balance = creator_balances.balance + EXCLUDED.balance, updated_at = now()
		RETURNING balance`,
		creatorID, delta).Scan(&balance)
	if err != nil {
		return ledger.ApplyResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ledger.ApplyResult{}, err
	}
	return ledger.ApplyResult{Replayed: false, Balance: balance}, nil
}

// Balance implements ledger.Repository.
func (p *Postgres) Balance(ctx context.Context, creatorID string) (int64, time.Time, bool, error) {
	var (
		balance   int64
		updatedAt time.Time
	)
	err := p.pool.QueryRow(ctx,
		`SELECT balance, updated_at FROM billing.creator_balances WHERE creator_id = $1`,
		creatorID).Scan(&balance, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, false, nil
	}
	if err != nil {
		return 0, time.Time{}, false, err
	}
	return balance, updatedAt, true, nil
}

// Entries implements ledger.Repository: newest first by descending id.
func (p *Postgres) Entries(ctx context.Context, creatorID string, beforeID int64, limit int) ([]ledger.Entry, error) {
	q := `
		SELECT id, delta, reason, idempotency_key, metadata, created_at
		FROM billing.credit_entries
		WHERE creator_id = $1`
	args := []any{creatorID}
	if beforeID != 0 {
		q += ` AND id < $2`
		args = append(args, beforeID)
	}
	q += fmt.Sprintf(` ORDER BY id DESC LIMIT %d`, limit)

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ledger.Entry
	for rows.Next() {
		var (
			e    ledger.Entry
			meta []byte
		)
		if err := rows.Scan(&e.ID, &e.Delta, &e.Reason, &e.IdempotencyKey, &meta, &e.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(meta, &e.Metadata); err != nil {
			return nil, fmt.Errorf("entry %d metadata: %w", e.ID, err)
		}
		e.CreatorID = creatorID
		out = append(out, e)
	}
	return out, rows.Err()
}

// LastTopup implements ledger.Repository.
func (p *Postgres) LastTopup(ctx context.Context, creatorID string) (int64, bool, error) {
	var delta int64
	err := p.pool.QueryRow(ctx, `
		SELECT delta FROM billing.credit_entries
		WHERE creator_id = $1 AND reason = $2
		ORDER BY id DESC LIMIT 1`,
		creatorID, ledger.ReasonTopup).Scan(&delta)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return delta, true, nil
}

// NegativeBalances implements ledger.Repository.
func (p *Postgres) NegativeBalances(ctx context.Context) (int64, error) {
	var n int64
	err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM billing.creator_balances WHERE balance < 0`).Scan(&n)
	return n, err
}
