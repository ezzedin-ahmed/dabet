package auth

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"dabet/pkg/httpx"
)

// Issuer configuration (docs §5.4, "HS256 in local / RS256 in target").
// The verifying half — JWT_ALG, JWT_SECRET, JWT_PUBLIC_KEY[_FILE] — lives
// in pkg/httpx and is shared by every service; only user-service holds a
// signing key.
//
//	JWT_ALG               HS256 (default) or RS256
//	JWT_SECRET            HS256 shared secret            (required for HS256)
//	JWT_PRIVATE_KEY       RS256 private key, PEM inline  (RS256: this or the file)
//	JWT_PRIVATE_KEY_FILE  RS256 private key, PEM path
//	JWT_KID               `kid` header override; defaults to the key's
//	                      SHA-256 thumbprint, which is stable, derivable by
//	                      a verifier from the public key alone, and reveals
//	                      nothing secret
//
// Local and e2e set only JWT_SECRET, so they keep the existing HS256 path
// byte for byte.
const (
	EnvJWTAlg            = httpx.EnvJWTAlg
	EnvJWTSecret         = httpx.EnvJWTSecret
	EnvJWTPrivateKey     = "JWT_PRIVATE_KEY"
	EnvJWTPrivateKeyFile = "JWT_PRIVATE_KEY_FILE"
	EnvJWTKid            = "JWT_KID"
)

// Signer mints access JWTs under exactly one algorithm.
type Signer struct {
	alg  string
	hmac []byte
	rsa  *rsa.PrivateKey
	kid  string
}

// HMACSigner returns an HS256 signer over the shared secret.
func HMACSigner(secret []byte) *Signer { return &Signer{alg: httpx.AlgHS256, hmac: secret} }

// RSASigner returns an RS256 signer. kid is stamped into the token header
// so a verifier with more than one public key can pick the right one;
// empty means "derive the thumbprint".
func RSASigner(key *rsa.PrivateKey, kid string) (*Signer, error) {
	if key == nil {
		return nil, errors.New("jwt private key: nil")
	}
	if kid == "" {
		var err error
		if kid, err = Thumbprint(&key.PublicKey); err != nil {
			return nil, err
		}
	}
	return &Signer{alg: httpx.AlgRS256, rsa: key, kid: kid}, nil
}

// Alg reports the algorithm this signer uses.
func (s *Signer) Alg() string { return s.alg }

// Kid reports the key id stamped on RS256 tokens, or "" under HS256.
func (s *Signer) Kid() string { return s.kid }

// Thumbprint is the base64url SHA-256 of the PKIX DER encoding of pub —
// the conventional stable key id, computable by anyone holding the public
// key.
func Thumbprint(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("jwt kid: %w", err)
	}
	sum := sha256.Sum256(der)
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

// Issue signs an access JWT with claims sub/iat/exp/jti (A2). Under
// RS256 the header additionally carries kid.
func (s *Signer) Issue(creatorID string, now time.Time, ttl time.Duration) (string, error) {
	if s == nil {
		return "", errors.New("jwt signer: not configured")
	}
	claims := jwt.RegisteredClaims{
		Subject:   creatorID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		ID:        uuid.NewString(),
	}
	switch s.alg {
	case httpx.AlgHS256:
		return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.hmac)
	case httpx.AlgRS256:
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = s.kid
		return tok.SignedString(s.rsa)
	default:
		return "", fmt.Errorf("jwt signer: unsupported algorithm %q", s.alg)
	}
}

// Keyring is the pair user-service needs: the signer that mints access
// tokens and the verifier that checks them on its own authenticated
// endpoints. They are built together so a deployment cannot end up
// issuing RS256 while verifying HS256.
type Keyring struct {
	Signer   *Signer
	Verifier *httpx.Verifier
}

// HMACKeyring is the HS256 pair over one shared secret. Used by tests and
// by the default local configuration.
func HMACKeyring(secret []byte) *Keyring {
	return &Keyring{Signer: HMACSigner(secret), Verifier: httpx.HMACVerifier(secret)}
}

// KeyringFromEnv builds the pair from getenv (pass os.Getenv). Under
// RS256 the verifier is derived from the private key's own public half,
// so user-service needs no JWT_PUBLIC_KEY of its own — other services
// still do.
func KeyringFromEnv(getenv func(string) string) (*Keyring, error) {
	alg := strings.ToUpper(strings.TrimSpace(getenv(EnvJWTAlg)))
	if alg == "" {
		alg = httpx.AlgHS256
	}
	switch alg {
	case httpx.AlgHS256:
		secret := getenv(EnvJWTSecret)
		if secret == "" {
			return nil, fmt.Errorf("required environment variable %s is not set", EnvJWTSecret)
		}
		return HMACKeyring([]byte(secret)), nil
	case httpx.AlgRS256:
		pemBytes, err := readPrivatePEM(getenv)
		if err != nil {
			return nil, err
		}
		key, err := ParseRSAPrivateKey(pemBytes)
		if err != nil {
			return nil, err
		}
		signer, err := RSASigner(key, strings.TrimSpace(getenv(EnvJWTKid)))
		if err != nil {
			return nil, err
		}
		return &Keyring{Signer: signer, Verifier: httpx.RSAVerifier(&key.PublicKey)}, nil
	default:
		return nil, fmt.Errorf("environment variable %s: unsupported algorithm %q (want %s or %s)",
			EnvJWTAlg, alg, httpx.AlgHS256, httpx.AlgRS256)
	}
}

func readPrivatePEM(getenv func(string) string) ([]byte, error) {
	if inline := strings.TrimSpace(getenv(EnvJWTPrivateKey)); inline != "" {
		return []byte(inline), nil
	}
	path := strings.TrimSpace(getenv(EnvJWTPrivateKeyFile))
	if path == "" {
		return nil, fmt.Errorf("%s=RS256 requires %s or %s", EnvJWTAlg, EnvJWTPrivateKey, EnvJWTPrivateKeyFile)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvJWTPrivateKeyFile, err)
	}
	return b, nil
}

// ParseRSAPrivateKey parses a PEM-encoded RSA private key in either
// PKCS#1 ("RSA PRIVATE KEY") or PKCS#8 ("PRIVATE KEY") form. Encrypted
// PEM is deliberately not supported: the passphrase would have to live in
// the environment next to it, which buys nothing.
func ParseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("jwt private key: not PEM-encoded")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("jwt private key: %w", err)
		}
		return key, nil
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("jwt private key: %w", err)
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("jwt private key: %T is not RSA", parsed)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("jwt private key: unexpected PEM block %q", block.Type)
	}
}
