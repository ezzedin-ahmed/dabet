package oauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

// NewVerifier returns a fresh PKCE code verifier: 32 random bytes,
// base64url without padding (RFC 7636 §4.1).
func NewVerifier() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate pkce verifier: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Challenge returns the S256 code challenge for a verifier (RFC 7636 §4.2).
func Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// AuthorizeURL builds the provider authorization URL for one round-trip:
// code flow, PKCE S256, the provider's configured scopes, and the given
// state.
func (p *Provider) AuthorizeURL(state, verifier string) string {
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", p.ClientID)
	q.Set("redirect_uri", p.RedirectURI)
	q.Set("scope", strings.Join(p.Scopes, " "))
	q.Set("state", state)
	q.Set("code_challenge", Challenge(verifier))
	q.Set("code_challenge_method", "S256")
	sep := "?"
	if strings.Contains(p.AuthURL, "?") {
		sep = "&"
	}
	return p.AuthURL + sep + q.Encode()
}
