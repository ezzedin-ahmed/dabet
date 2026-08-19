package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"
)

// AccessTokenTTL is the access-JWT lifetime per A2 (15 minutes),
// RefreshTokenTTL the refresh-token lifetime (30 days).
const (
	AccessTokenTTL  = 15 * time.Minute
	RefreshTokenTTL = 30 * 24 * time.Hour
)

// NewOpaqueToken returns a 32-byte random opaque token (base64url,
// handed to the client, never stored) and its SHA-256 hex digest (the
// only form that is stored). Used for refresh tokens and email
// verification tokens.
func NewOpaqueToken() (raw, hash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generate token: %w", err)
	}
	raw = base64.RawURLEncoding.EncodeToString(b)
	return raw, HashToken(raw), nil
}

// HashToken returns the SHA-256 hex digest of an opaque token.
func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// IssueAccessToken signs an HS256 access JWT with claims sub/iat/exp/jti
// (A2), compatible with the pkg/httpx Auth middleware. It is the HS256
// shorthand for Signer.Issue; production goes through the Keyring so
// RS256 is available (see signer.go).
func IssueAccessToken(secret []byte, creatorID string, now time.Time, ttl time.Duration) (string, error) {
	return HMACSigner(secret).Issue(creatorID, now, ttl)
}
