// Package connpg is the adapter's read/refresh access to Area A's
// identity.connections table: it implements connsource.Lister (the
// Postgres-backed connection source) and refresh.Store (the §5.6
// advisory-locked refresh transaction). The adapter never writes rows
// beyond token refresh and the §5.6 expired transition.
package connpg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/refresh"
)

// Store wraps a pgx pool.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// ActiveConnections implements connsource.Lister: every active
// connection, with the token and native account ref drivers need.
func (s *Store) ActiveConnections(ctx context.Context) ([]driver.Connection, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, creator_id, platform::text, provider_user_id, access_token
		FROM identity.connections
		WHERE status = 'active'`)
	if err != nil {
		return nil, fmt.Errorf("connpg: list active connections: %w", err)
	}
	defer rows.Close()

	var out []driver.Connection
	for rows.Next() {
		var c driver.Connection
		if err := rows.Scan(&c.ID, &c.CreatorID, &c.Platform, &c.NativeUserID, &c.AccessToken); err != nil {
			return nil, fmt.Errorf("connpg: scan connection: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("connpg: list active connections: %w", err)
	}
	return out, nil
}

// Locked implements refresh.Store: fn runs inside a transaction holding
// pg_advisory_xact_lock(hashtext(connectionID)) (§5.6 step 1), so
// concurrent workers on the same connection refresh once, not N times.
func (s *Store) Locked(ctx context.Context, connectionID string, fn func(ops refresh.RowOps) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("connpg: begin refresh tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1::text))`, connectionID); err != nil {
		return fmt.Errorf("connpg: advisory lock: %w", err)
	}
	if err := fn(txOps{tx: tx}); err != nil {
		return err // deferred rollback
	}
	return tx.Commit(ctx)
}

// txOps implements refresh.RowOps over one transaction.
type txOps struct {
	tx pgx.Tx
}

func (o txOps) Get(ctx context.Context, id string) (refresh.Row, error) {
	var r refresh.Row
	err := o.tx.QueryRow(ctx, `
		SELECT id, platform::text, status::text, access_token, COALESCE(refresh_token, ''), expires_at
		FROM identity.connections
		WHERE id = $1`, id,
	).Scan(&r.ID, &r.Platform, &r.Status, &r.AccessToken, &r.RefreshToken, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return refresh.Row{}, refresh.ErrNotFound
	}
	if err != nil {
		return refresh.Row{}, fmt.Errorf("connpg: read connection: %w", err)
	}
	return r, nil
}

func (o txOps) UpdateTokens(ctx context.Context, id, accessToken, refreshToken string, expiresAt *time.Time) error {
	_, err := o.tx.Exec(ctx, `
		UPDATE identity.connections
		SET access_token = $2,
		    refresh_token = COALESCE(NULLIF($3, ''), refresh_token),
		    expires_at = $4,
		    updated_at = now()
		WHERE id = $1`, id, accessToken, refreshToken, expiresAt)
	if err != nil {
		return fmt.Errorf("connpg: update tokens: %w", err)
	}
	return nil
}

func (o txOps) MarkExpired(ctx context.Context, id string) error {
	_, err := o.tx.Exec(ctx, `
		UPDATE identity.connections
		SET status = 'expired', updated_at = now()
		WHERE id = $1 AND status = 'active'`, id)
	if err != nil {
		return fmt.Errorf("connpg: mark expired: %w", err)
	}
	return nil
}
