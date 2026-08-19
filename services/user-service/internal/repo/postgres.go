package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres implements Repository over a pgx pool.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

var _ Repository = (*Postgres)(nil)

const uniqueViolation = "23505"

func (p *Postgres) CreateCreator(ctx context.Context, email, fullname, passwordHash string) (string, error) {
	var id string
	err := p.pool.QueryRow(ctx, `
		INSERT INTO identity.creators (email, fullname, password_hash)
		VALUES ($1, $2, $3)
		RETURNING id`, email, fullname, passwordHash).Scan(&id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return "", ErrDuplicateEmail
		}
		return "", fmt.Errorf("create creator: %w", err)
	}
	return id, nil
}

const creatorColumns = `id, email, fullname, password_hash, email_verified_at, created_at, updated_at`

func (p *Postgres) creatorBy(ctx context.Context, where string, arg any) (*Creator, error) {
	var c Creator
	err := p.pool.QueryRow(ctx,
		`SELECT `+creatorColumns+` FROM identity.creators WHERE `+where, arg,
	).Scan(&c.ID, &c.Email, &c.Fullname, &c.PasswordHash, &c.EmailVerifiedAt, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select creator: %w", err)
	}
	return &c, nil
}

func (p *Postgres) CreatorByEmail(ctx context.Context, email string) (*Creator, error) {
	return p.creatorBy(ctx, `email = $1`, email)
}

func (p *Postgres) CreatorByID(ctx context.Context, id string) (*Creator, error) {
	return p.creatorBy(ctx, `id = $1`, id)
}

func (p *Postgres) CreateEmailVerification(ctx context.Context, creatorID, tokenHash string, expiresAt time.Time) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO identity.email_verifications (creator_id, token_hash, expires_at)
		VALUES ($1, $2, $3)`, creatorID, tokenHash, expiresAt)
	if err != nil {
		return fmt.Errorf("create email verification: %w", err)
	}
	return nil
}

func (p *Postgres) ConsumeEmailVerification(ctx context.Context, tokenHash string, now time.Time) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("consume email verification: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var creatorID string
	err = tx.QueryRow(ctx, `
		UPDATE identity.email_verifications
		SET consumed_at = $2
		WHERE token_hash = $1 AND consumed_at IS NULL AND expires_at > $2
		RETURNING creator_id`, tokenHash, now).Scan(&creatorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("consume email verification: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE identity.creators
		SET email_verified_at = COALESCE(email_verified_at, $2), updated_at = $2
		WHERE id = $1`, creatorID, now); err != nil {
		return fmt.Errorf("mark email verified: %w", err)
	}
	return tx.Commit(ctx)
}

func insertRefreshToken(ctx context.Context, q interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}, t *RefreshToken) error {
	_, err := q.Exec(ctx, `
		INSERT INTO identity.refresh_tokens (id, creator_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5)`,
		t.ID, t.CreatorID, t.FamilyID, t.TokenHash, t.ExpiresAt)
	if err != nil {
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

func (p *Postgres) InsertRefreshToken(ctx context.Context, t *RefreshToken) error {
	return insertRefreshToken(ctx, p.pool, t)
}

func (p *Postgres) RefreshTokenByHash(ctx context.Context, tokenHash string) (*RefreshToken, error) {
	var t RefreshToken
	err := p.pool.QueryRow(ctx, `
		SELECT id, creator_id, family_id, token_hash, expires_at, revoked_at, created_at
		FROM identity.refresh_tokens WHERE token_hash = $1`, tokenHash,
	).Scan(&t.ID, &t.CreatorID, &t.FamilyID, &t.TokenHash, &t.ExpiresAt, &t.RevokedAt, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select refresh token: %w", err)
	}
	return &t, nil
}

func (p *Postgres) RotateRefreshToken(ctx context.Context, oldID string, next *RefreshToken, now time.Time) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("rotate refresh token: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	tag, err := tx.Exec(ctx, `
		UPDATE identity.refresh_tokens SET revoked_at = $2
		WHERE id = $1 AND revoked_at IS NULL`, oldID, now)
	if err != nil {
		return fmt.Errorf("rotate refresh token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if err := insertRefreshToken(ctx, tx, next); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) RevokeRefreshFamily(ctx context.Context, familyID string, now time.Time) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE identity.refresh_tokens SET revoked_at = $2
		WHERE family_id = $1 AND revoked_at IS NULL`, familyID, now)
	if err != nil {
		return fmt.Errorf("revoke refresh family: %w", err)
	}
	return nil
}
