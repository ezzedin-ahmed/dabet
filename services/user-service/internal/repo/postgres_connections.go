package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (p *Postgres) CreateOAuthState(ctx context.Context, s *OAuthState) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO identity.oauth_states (state, creator_id, platform, code_verifier, redirect_after, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		s.State, s.CreatorID, s.Platform, s.CodeVerifier, s.RedirectAfter, s.ExpiresAt)
	if err != nil {
		return fmt.Errorf("create oauth state: %w", err)
	}
	return nil
}

func (p *Postgres) ConsumeOAuthState(ctx context.Context, state string) (*OAuthState, error) {
	var s OAuthState
	err := p.pool.QueryRow(ctx, `
		DELETE FROM identity.oauth_states
		WHERE state = $1
		RETURNING state, creator_id, platform, code_verifier, redirect_after, created_at, expires_at`,
		state,
	).Scan(&s.State, &s.CreatorID, &s.Platform, &s.CodeVerifier, &s.RedirectAfter, &s.CreatedAt, &s.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("consume oauth state: %w", err)
	}
	return &s, nil
}

const connectionColumns = `id, creator_id, platform, provider_user_id, display_name,
	access_token, refresh_token, expires_at, scopes, status, connected_at, updated_at`

func scanConnection(row pgx.Row) (*Connection, error) {
	var c Connection
	err := row.Scan(&c.ID, &c.CreatorID, &c.Platform, &c.ProviderUserID, &c.DisplayName,
		&c.AccessToken, &c.RefreshToken, &c.ExpiresAt, &c.Scopes, &c.Status, &c.ConnectedAt, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (p *Postgres) UpsertConnection(ctx context.Context, c *Connection) (string, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("upsert connection: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// Reconnect path: the creator already linked this platform account
	// once (any status) — refresh it in place rather than growing a new
	// row per reconnect.
	var id string
	err = tx.QueryRow(ctx, `
		UPDATE identity.connections
		SET display_name = $4, access_token = $5, refresh_token = $6,
		    expires_at = $7, scopes = $8, status = 'active', updated_at = now()
		WHERE id = (
			SELECT id FROM identity.connections
			WHERE creator_id = $1 AND platform = $2 AND provider_user_id = $3
			ORDER BY (status = 'active') DESC, connected_at DESC
			LIMIT 1
		)
		RETURNING id`,
		c.CreatorID, c.Platform, c.ProviderUserID, c.DisplayName,
		c.AccessToken, c.RefreshToken, c.ExpiresAt, c.Scopes).Scan(&id)
	if err == nil {
		return id, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", connErr("upsert connection", err)
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO identity.connections
			(creator_id, platform, provider_user_id, display_name, access_token, refresh_token, expires_at, scopes)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`,
		c.CreatorID, c.Platform, c.ProviderUserID, c.DisplayName,
		c.AccessToken, c.RefreshToken, c.ExpiresAt, c.Scopes).Scan(&id)
	if err != nil {
		return "", connErr("insert connection", err)
	}
	return id, tx.Commit(ctx)
}

// connErr surfaces a violation of the connections_active_uniq partial
// unique index (A4) as ErrConnectionConflict.
func connErr(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return ErrConnectionConflict
	}
	return fmt.Errorf("%s: %w", op, err)
}

func (p *Postgres) ListConnections(ctx context.Context, creatorID string) ([]Connection, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT `+connectionColumns+`
		FROM identity.connections
		WHERE creator_id = $1
		ORDER BY connected_at, id`, creatorID)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()

	var out []Connection
	for rows.Next() {
		c, err := scanConnection(rows)
		if err != nil {
			return nil, fmt.Errorf("list connections: %w", err)
		}
		out = append(out, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	return out, nil
}

func (p *Postgres) RevokeConnection(ctx context.Context, id, creatorID string, now time.Time) (*Connection, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("revoke connection: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	c, err := scanConnection(tx.QueryRow(ctx, `
		SELECT `+connectionColumns+`
		FROM identity.connections
		WHERE id = $1 AND creator_id = $2
		FOR UPDATE`, id, creatorID))
	if errors.Is(err, pgx.ErrNoRows) {
		// Absent or owned by another creator — indistinguishable by
		// design (§4.1: 404, never 403).
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("revoke connection: %w", err)
	}
	if c.Status == "revoked" {
		return nil, ErrAlreadyRevoked
	}
	if _, err := tx.Exec(ctx, `
		UPDATE identity.connections
		SET status = 'revoked', updated_at = $2
		WHERE id = $1`, id, now); err != nil {
		return nil, fmt.Errorf("revoke connection: %w", err)
	}
	return c, tx.Commit(ctx)
}

func (p *Postgres) ActiveConnectionCounts(ctx context.Context) (map[string]int, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT platform::text, count(*)
		FROM identity.connections
		WHERE status = 'active'
		GROUP BY platform`)
	if err != nil {
		return nil, fmt.Errorf("active connection counts: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int)
	for rows.Next() {
		var platform string
		var n int
		if err := rows.Scan(&platform, &n); err != nil {
			return nil, fmt.Errorf("active connection counts: %w", err)
		}
		out[platform] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("active connection counts: %w", err)
	}
	return out, nil
}
