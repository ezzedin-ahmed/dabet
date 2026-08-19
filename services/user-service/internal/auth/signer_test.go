package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"dabet/pkg/httpx"
)

var testKey = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

func envMap(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func pkcs1PEM(t *testing.T, k *rsa.PrivateKey) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
}

func pkcs8PEM(t *testing.T, k *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// header decodes a token's JOSE header without verifying it.
func header(t *testing.T, raw string) map[string]any {
	t.Helper()
	tok, _, err := jwt.NewParser().ParseUnverified(raw, &jwt.RegisteredClaims{})
	if err != nil {
		t.Fatal(err)
	}
	return tok.Header
}

func TestHMACKeyringIsTheDefault(t *testing.T) {
	kr, err := KeyringFromEnv(envMap(map[string]string{EnvJWTSecret: "shhh"}))
	if err != nil {
		t.Fatalf("KeyringFromEnv: %v", err)
	}
	if kr.Signer.Alg() != httpx.AlgHS256 {
		t.Fatalf("alg = %q, want HS256", kr.Signer.Alg())
	}
	if kr.Signer.Kid() != "" {
		t.Errorf("HS256 tokens should carry no kid, got %q", kr.Signer.Kid())
	}
	raw, err := kr.Signer.Issue("creator-1", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if h := header(t, raw); h["alg"] != "HS256" {
		t.Errorf("header alg = %v", h["alg"])
	}
	claims, err := kr.Verifier.Parse(raw)
	if err != nil {
		t.Fatalf("the keyring's own verifier rejected its own token: %v", err)
	}
	if claims.Subject != "creator-1" {
		t.Errorf("sub = %q", claims.Subject)
	}
}

func TestHS256RequiresSecret(t *testing.T) {
	if _, err := KeyringFromEnv(envMap(nil)); err == nil {
		t.Error("want an error when JWT_SECRET is unset")
	}
}

func TestRS256KeyringRoundTrip(t *testing.T) {
	for name, keyPEM := range map[string][]byte{
		"PKCS#1": pkcs1PEM(t, testKey),
		"PKCS#8": pkcs8PEM(t, testKey),
	} {
		t.Run(name, func(t *testing.T) {
			kr, err := KeyringFromEnv(envMap(map[string]string{
				EnvJWTAlg:        "RS256",
				EnvJWTPrivateKey: string(keyPEM),
			}))
			if err != nil {
				t.Fatalf("KeyringFromEnv: %v", err)
			}
			if kr.Signer.Alg() != httpx.AlgRS256 {
				t.Fatalf("alg = %q", kr.Signer.Alg())
			}
			raw, err := kr.Signer.Issue("creator-rs", time.Now().UTC(), time.Minute)
			if err != nil {
				t.Fatalf("Issue: %v", err)
			}
			h := header(t, raw)
			if h["alg"] != "RS256" {
				t.Errorf("header alg = %v", h["alg"])
			}
			kid, _ := h["kid"].(string)
			if kid == "" {
				t.Error("RS256 tokens must carry a kid")
			}
			// The default kid is the public key's thumbprint, so a
			// verifier holding only the public half can derive it.
			want, err := Thumbprint(&testKey.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			if kid != want {
				t.Errorf("kid = %q, want the thumbprint %q", kid, want)
			}

			// The independently-configured pkg/httpx verifier — the one
			// every other service builds from JWT_PUBLIC_KEY — accepts it.
			pubDER, err := x509.MarshalPKIXPublicKey(&testKey.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
			v, err := httpx.VerifierFromEnv(envMap(map[string]string{
				httpx.EnvJWTAlg:       "RS256",
				httpx.EnvJWTPublicKey: string(pubPEM),
			}))
			if err != nil {
				t.Fatalf("VerifierFromEnv: %v", err)
			}
			if _, err := v.Parse(raw); err != nil {
				t.Errorf("a peer service rejected a user-service RS256 token: %v", err)
			}
		})
	}
}

func TestRS256ExplicitKid(t *testing.T) {
	kr, err := KeyringFromEnv(envMap(map[string]string{
		EnvJWTAlg:        "RS256",
		EnvJWTPrivateKey: string(pkcs1PEM(t, testKey)),
		EnvJWTKid:        "2026-08-rotation",
	}))
	if err != nil {
		t.Fatalf("KeyringFromEnv: %v", err)
	}
	raw, err := kr.Signer.Issue("creator-rs", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if kid := header(t, raw)["kid"]; kid != "2026-08-rotation" {
		t.Errorf("kid = %v", kid)
	}
}

func TestRS256FromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jwt.key")
	if err := os.WriteFile(path, pkcs8PEM(t, testKey), 0o600); err != nil {
		t.Fatal(err)
	}
	kr, err := KeyringFromEnv(envMap(map[string]string{
		EnvJWTAlg:            "rs256",
		EnvJWTPrivateKeyFile: path,
	}))
	if err != nil {
		t.Fatalf("KeyringFromEnv: %v", err)
	}
	raw, err := kr.Signer.Issue("creator-rs", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kr.Verifier.Parse(raw); err != nil {
		t.Errorf("Parse: %v", err)
	}
}

// TestKeyringNeverIssuesWhatItCannotVerify is the property that matters:
// signer and verifier are built together, so an RS256 deployment cannot
// end up handing out tokens its own /v1/me rejects — and the HS256
// verifier must not accept the RS256 token, which is the alg-confusion
// guard seen from the issuing side.
func TestKeyringNeverIssuesWhatItCannotVerify(t *testing.T) {
	rs, err := KeyringFromEnv(envMap(map[string]string{
		EnvJWTAlg:        "RS256",
		EnvJWTPrivateKey: string(pkcs1PEM(t, testKey)),
	}))
	if err != nil {
		t.Fatal(err)
	}
	hs := HMACKeyring([]byte("shhh"))

	rsToken, err := rs.Signer.Issue("creator-rs", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	hsToken, err := hs.Signer.Issue("creator-hs", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := rs.Verifier.Parse(rsToken); err != nil {
		t.Errorf("RS256 keyring rejected its own token: %v", err)
	}
	if _, err := hs.Verifier.Parse(hsToken); err != nil {
		t.Errorf("HS256 keyring rejected its own token: %v", err)
	}
	if _, err := hs.Verifier.Parse(rsToken); err == nil {
		t.Error("ALG CONFUSION: the HS256 verifier accepted an RS256 token")
	}
	if _, err := rs.Verifier.Parse(hsToken); err == nil {
		t.Error("ALG CONFUSION: the RS256 verifier accepted an HS256 token")
	}
}

func TestKeyringBadConfig(t *testing.T) {
	tests := map[string]map[string]string{
		"unknown alg":       {EnvJWTAlg: "ES256", EnvJWTSecret: "x"},
		"RS256 without key": {EnvJWTAlg: "RS256"},
		"missing file":      {EnvJWTAlg: "RS256", EnvJWTPrivateKeyFile: "/nonexistent/jwt.key"},
		"not PEM":           {EnvJWTAlg: "RS256", EnvJWTPrivateKey: "hunter2"},
		"truncated PEM":     {EnvJWTAlg: "RS256", EnvJWTPrivateKey: "-----BEGIN RSA PRIVATE KEY-----\nZm9v\n-----END RSA PRIVATE KEY-----\n"},
		"public key given as private": {
			EnvJWTAlg: "RS256",
			EnvJWTPrivateKey: string(pem.EncodeToMemory(&pem.Block{
				Type: "PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&testKey.PublicKey),
			})),
		},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := KeyringFromEnv(envMap(env)); err == nil {
				t.Error("want a startup error")
			}
		})
	}
}

func TestNilSignerErrors(t *testing.T) {
	var s *Signer
	if _, err := s.Issue("x", time.Now(), time.Minute); err == nil {
		t.Error("a nil signer minted a token")
	}
	if _, err := RSASigner(nil, ""); err == nil {
		t.Error("RSASigner(nil) succeeded")
	}
}

// TestIssueAccessTokenStillHS256 pins the compatibility shim used by the
// existing tests and by anything that only has a shared secret.
func TestIssueAccessTokenStillHS256(t *testing.T) {
	raw, err := IssueAccessToken([]byte("shhh"), "creator-1", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if h := header(t, raw); h["alg"] != "HS256" {
		t.Errorf("header alg = %v, want HS256", h["alg"])
	}
	if _, err := httpx.HMACVerifier([]byte("shhh")).Parse(raw); err != nil {
		t.Errorf("Parse: %v", err)
	}
}
