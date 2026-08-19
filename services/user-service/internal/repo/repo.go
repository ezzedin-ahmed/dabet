// Package repo defines the identity repository interface and its pgx
// and in-memory implementations. Handlers depend only on Repository.
package repo

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors the API layer maps to envelope codes.
var (
	ErrDuplicateEmail = errors.New("email already registered")
	ErrNotFound       = errors.New("not found")
)

// Creator is a row of identity.creators.
type Creator struct {
	ID              string
	Email           string
	Fullname        string
	PasswordHash    string
	EmailVerifiedAt *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// RefreshToken is a row of identity.refresh_tokens. FamilyID groups a
// rotation chain: reuse of a rotated member revokes the whole family (A2).
type RefreshToken struct {
	ID        string
	CreatorID string
	FamilyID  string
	TokenHash string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
}

// Repository is the persistence boundary of user-service.
type Repository interface {
	// CreateCreator inserts a creator and returns its id.
	// Returns ErrDuplicateEmail on a unique violation.
	CreateCreator(ctx context.Context, email, fullname, passwordHash string) (string, error)
	// CreatorByEmail returns ErrNotFound when absent.
	CreatorByEmail(ctx context.Context, email string) (*Creator, error)
	// CreatorByID returns ErrNotFound when absent.
	CreatorByID(ctx context.Context, id string) (*Creator, error)

	// CreateEmailVerification stores a hashed verification token.
	CreateEmailVerification(ctx context.Context, creatorID, tokenHash string, expiresAt time.Time) error
	// ConsumeEmailVerification atomically consumes an unconsumed,
	// unexpired verification token and marks the creator's email
	// verified. Returns ErrNotFound for unknown, consumed, or expired
	// tokens.
	ConsumeEmailVerification(ctx context.Context, tokenHash string, now time.Time) error

	// InsertRefreshToken stores a new refresh token (hashed).
	InsertRefreshToken(ctx context.Context, t *RefreshToken) error
	// RefreshTokenByHash returns ErrNotFound when absent.
	RefreshTokenByHash(ctx context.Context, tokenHash string) (*RefreshToken, error)
	// RotateRefreshToken revokes the token with id oldID — only if it is
	// still unrevoked — and inserts next in the same transaction.
	// Returns ErrNotFound if oldID is absent or already revoked, so a
	// lost rotation race is indistinguishable from token reuse.
	RotateRefreshToken(ctx context.Context, oldID string, next *RefreshToken, now time.Time) error
	// RevokeRefreshFamily revokes every unrevoked token in a family.
	RevokeRefreshFamily(ctx context.Context, familyID string, now time.Time) error
}
