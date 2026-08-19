package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the pgx-backed Store. "partition" is quoted in every
// statement to stay clear of the SQL keyword.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps a pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

// GetOrInit implements Store.
func (p *Postgres) GetOrInit(ctx context.Context, creatorID string, partition int32, offset int64) (Cursor, error) {
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO review.review_cursors (creator_id, "partition", next_offset)
		VALUES ($1, $2, $3)
		ON CONFLICT (creator_id) DO NOTHING`,
		creatorID, partition, offset); err != nil {
		return Cursor{}, fmt.Errorf("init review cursor: %w", err)
	}
	var c Cursor
	err := p.pool.QueryRow(ctx, `
		SELECT creator_id, "partition", next_offset, updated_at
		FROM review.review_cursors WHERE creator_id = $1`,
		creatorID).Scan(&c.CreatorID, &c.Partition, &c.NextOffset, &c.UpdatedAt)
	if err != nil {
		return Cursor{}, fmt.Errorf("get review cursor: %w", err)
	}
	return c, nil
}

// SetNextOffset implements Store.
func (p *Postgres) SetNextOffset(ctx context.Context, creatorID string, from, to int64) (bool, error) {
	tag, err := p.pool.Exec(ctx, `
		UPDATE review.review_cursors
		SET next_offset = $3, updated_at = now()
		WHERE creator_id = $1 AND next_offset = $2`,
		creatorID, from, to)
	if err != nil {
		return false, fmt.Errorf("set review cursor: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Reset implements Store.
func (p *Postgres) Reset(ctx context.Context, creatorID string, partition int32, offset int64) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO review.review_cursors (creator_id, "partition", next_offset)
		VALUES ($1, $2, $3)
		ON CONFLICT (creator_id) DO UPDATE
		SET "partition" = EXCLUDED."partition",
		    next_offset = EXCLUDED.next_offset,
		    updated_at  = now()`,
		creatorID, partition, offset)
	if err != nil {
		return fmt.Errorf("reset review cursor: %w", err)
	}
	return nil
}

// Advance implements Store.
func (p *Postgres) Advance(ctx context.Context, creatorID string, from, to int64, mid func(ctx context.Context) error) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("advance review cursor: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var current int64
	err = tx.QueryRow(ctx, `
		SELECT next_offset FROM review.review_cursors
		WHERE creator_id = $1 FOR UPDATE`,
		creatorID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("advance review cursor: %w", err)
	}
	if current != from {
		return false, nil
	}
	if mid != nil {
		if err := mid(ctx); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE review.review_cursors
		SET next_offset = $2, updated_at = now()
		WHERE creator_id = $1`,
		creatorID, to); err != nil {
		return false, fmt.Errorf("advance review cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("advance review cursor: %w", err)
	}
	return true, nil
}
