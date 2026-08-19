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
	// ErrConnectionConflict: another creator holds an active connection
	// for the same (platform, provider_user_id) — the partial unique
	// index connections_active_uniq (A4). Maps to 409 conflict.
	ErrConnectionConflict = errors.New("platform account already connected by another creator")
	// ErrAlreadyRevoked: disconnect of a connection whose status is
	// already 'revoked'. Maps to 409 state_conflict.
	ErrAlreadyRevoked = errors.New("connection already revoked")
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

// Connection is a row of identity.connections (§5.2).
type Connection struct {
	ID             string
	CreatorID      string
	Platform       string
	ProviderUserID string
	DisplayName    string
	AccessToken    string
	RefreshToken   *string
	ExpiresAt      *time.Time
	Scopes         []string
	Status         string // active | expired | revoked
	ConnectedAt    time.Time
	UpdatedAt      time.Time
}

// OAuthState is a row of identity.oauth_states: one pending authorization
// round-trip, single-use, short TTL (§5.5).
type OAuthState struct {
	State         string
	CreatorID     string
	Platform      string
	CodeVerifier  string
	RedirectAfter *string
	CreatedAt     time.Time
	ExpiresAt     time.Time
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

	// CreateOAuthState stores a pending authorization state row.
	CreateOAuthState(ctx context.Context, s *OAuthState) error
	// ConsumeOAuthState deletes the state row and returns it — single
	// use, the CSRF defence of §5.5. ErrNotFound for unknown states.
	// Expiry is checked by the caller against the returned row.
	ConsumeOAuthState(ctx context.Context, state string) (*OAuthState, error)

	// UpsertConnection re-activates the creator's existing row for
	// (platform, provider_user_id) with fresh tokens, or inserts a new
	// one. Returns the connection id, or ErrConnectionConflict when
	// another creator holds the active connection (A4).
	UpsertConnection(ctx context.Context, c *Connection) (string, error)
	// ListConnections returns the creator's connections, all statuses,
	// oldest first.
	ListConnections(ctx context.Context, creatorID string) ([]Connection, error)
	// RevokeConnection sets status='revoked' and returns the row as it
	// was before revocation (for best-effort provider-side revocation).
	// ErrNotFound when absent or owned by another creator (§4.1);
	// ErrAlreadyRevoked when status is already 'revoked'.
	RevokeConnection(ctx context.Context, id, creatorID string, now time.Time) (*Connection, error)
	// ActiveConnectionCounts returns active-connection counts by
	// platform, for the connections_active gauge (§5.9).
	ActiveConnectionCounts(ctx context.Context) (map[string]int, error)
}
