package httpx

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Access-token verification config (docs §4.1 auth, §5.4 "HS256 in local /
// RS256 in target").
//
//	JWT_ALG              HS256 (default) or RS256
//	JWT_SECRET           HS256 shared secret          (required for HS256)
//	JWT_PUBLIC_KEY       RS256 public key, PEM inline (RS256: this or the file)
//	JWT_PUBLIC_KEY_FILE  RS256 public key, PEM path
//
// Local and e2e set neither JWT_ALG nor any public key, so they keep the
// existing HS256 path unchanged.
const (
	EnvJWTAlg           = "JWT_ALG"
	EnvJWTSecret        = "JWT_SECRET"
	EnvJWTPublicKey     = "JWT_PUBLIC_KEY"
	EnvJWTPublicKeyFile = "JWT_PUBLIC_KEY_FILE"
)

// Supported signing algorithms.
const (
	AlgHS256 = "HS256"
	AlgRS256 = "RS256"
)

// Verifier validates access tokens under exactly one algorithm.
//
// # Algorithm confusion
//
// A verifier is pinned to a single alg and rejects a token whose header
// says anything else, before any signature check. This is not belt and
// braces, it is the whole point: the classic attack against a mixed
// HS256/RS256 deployment is to take the RS256 *public* key — which is
// public — feed it to an HMAC signer as the shared secret, and present the
// result as an HS256 token. A verifier that picks its algorithm from the
// token's own header would happily verify it and mint an arbitrary
// subject. jwt.WithValidMethods pins the accepted alg, and the keyfunc
// additionally refuses to hand an RSA key to an HMAC method (and vice
// versa) so neither layer alone is load-bearing.
type Verifier struct {
	alg    string
	secret []byte
	pub    *rsa.PublicKey
}

// HMACVerifier returns an HS256 verifier over the shared secret.
func HMACVerifier(secret []byte) *Verifier {
	return &Verifier{alg: AlgHS256, secret: secret}
}

// RSAVerifier returns an RS256 verifier over the issuer's public key.
func RSAVerifier(pub *rsa.PublicKey) *Verifier {
	return &Verifier{alg: AlgRS256, pub: pub}
}

// Alg reports the algorithm this verifier accepts.
func (v *Verifier) Alg() string { return v.alg }

// VerifierFromEnv builds a Verifier from getenv (pass os.Getenv). The
// default is HS256 with JWT_SECRET, so a deployment that sets nothing new
// behaves exactly as before.
func VerifierFromEnv(getenv func(string) string) (*Verifier, error) {
	alg := strings.ToUpper(strings.TrimSpace(getenv(EnvJWTAlg)))
	if alg == "" {
		alg = AlgHS256
	}
	switch alg {
	case AlgHS256:
		secret := getenv(EnvJWTSecret)
		if secret == "" {
			return nil, fmt.Errorf("required environment variable %s is not set", EnvJWTSecret)
		}
		return HMACVerifier([]byte(secret)), nil
	case AlgRS256:
		pemBytes, err := readPEM(getenv, EnvJWTPublicKey, EnvJWTPublicKeyFile)
		if err != nil {
			return nil, err
		}
		pub, err := ParseRSAPublicKey(pemBytes)
		if err != nil {
			return nil, err
		}
		return RSAVerifier(pub), nil
	default:
		return nil, fmt.Errorf("environment variable %s: unsupported algorithm %q (want %s or %s)",
			EnvJWTAlg, alg, AlgHS256, AlgRS256)
	}
}

// readPEM returns PEM bytes from the inline variable or the file
// variable, erroring when neither is usable.
func readPEM(getenv func(string) string, inlineVar, fileVar string) ([]byte, error) {
	if inline := strings.TrimSpace(getenv(inlineVar)); inline != "" {
		return []byte(inline), nil
	}
	path := strings.TrimSpace(getenv(fileVar))
	if path == "" {
		return nil, fmt.Errorf("%s requires %s or %s", EnvJWTAlg, inlineVar, fileVar)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fileVar, err)
	}
	return b, nil
}

// ErrInvalidToken is the single failure the middleware surfaces; callers
// must not distinguish "bad signature" from "expired" to a client.
var ErrInvalidToken = errors.New("invalid token")

// ParseRSAPublicKey parses a PEM-encoded RSA public key, accepting both
// "PUBLIC KEY" (PKIX, what openssl and every JWKS tool emits) and "RSA
// PUBLIC KEY" (PKCS#1). A certificate is also accepted, since deployments
// often have one to hand.
func ParseRSAPublicKey(pemBytes []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("jwt public key: not PEM-encoded")
	}
	switch block.Type {
	case "PUBLIC KEY":
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("jwt public key: %w", err)
		}
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("jwt public key: %T is not RSA", key)
		}
		return pub, nil
	case "RSA PUBLIC KEY":
		pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("jwt public key: %w", err)
		}
		return pub, nil
	case "CERTIFICATE":
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("jwt public key: %w", err)
		}
		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("jwt public key: certificate key %T is not RSA", cert.PublicKey)
		}
		return pub, nil
	default:
		return nil, fmt.Errorf("jwt public key: unexpected PEM block %q", block.Type)
	}
}

// Parse validates raw and returns its registered claims. Every failure
// mode — wrong alg, bad signature, expired, missing subject — collapses
// to ErrInvalidToken, because telling a client which one it was is an
// oracle.
func (v *Verifier) Parse(raw string) (*jwt.RegisteredClaims, error) {
	if v == nil {
		return nil, ErrInvalidToken
	}
	claims := &jwt.RegisteredClaims{}
	tok, err := jwt.ParseWithClaims(raw, claims, v.keyFunc,
		jwt.WithValidMethods([]string{v.alg}),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !tok.Valid || claims.Subject == "" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// keyFunc hands back the key only when the token's method matches the
// configured algorithm's *family*. Without this check a caller who forgot
// jwt.WithValidMethods would get the alg-confusion bug back.
func (v *Verifier) keyFunc(tok *jwt.Token) (any, error) {
	switch v.alg {
	case AlgHS256:
		if _, ok := tok.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		if len(v.secret) == 0 {
			return nil, ErrInvalidToken
		}
		return v.secret, nil
	case AlgRS256:
		if _, ok := tok.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, ErrInvalidToken
		}
		if v.pub == nil {
			return nil, ErrInvalidToken
		}
		return v.pub, nil
	default:
		return nil, ErrInvalidToken
	}
}

// Auth validates a Bearer JWT (claims sub/iat/exp/jti per docs §5.4) with
// v and stores the subject as the creator_id in the request context.
// Failures render the unauthenticated envelope.
func Auth(v *Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || raw == "" {
				WriteError(w, r, CodeUnauthenticated, "missing bearer token", nil)
				return
			}
			claims, err := v.Parse(raw)
			if err != nil {
				WriteError(w, r, CodeUnauthenticated, "invalid or expired token", nil)
				return
			}
			ctx := context.WithValue(r.Context(), ctxCreatorID, claims.Subject)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// CreatorIDFrom returns the authenticated creator_id stored by Auth, or "".
func CreatorIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxCreatorID).(string)
	return id
}
