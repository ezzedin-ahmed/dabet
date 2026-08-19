package httpx

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// One 2048-bit key for the whole file: RSA keygen is slow and the test
// does not care which key it is.
var testRSAKey = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

func publicKeyPEM(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func pkcs1PublicKeyPEM(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(pub),
	})
}

func claimsFor(sub string, expiresIn time.Duration) jwt.RegisteredClaims {
	now := time.Now()
	return jwt.RegisteredClaims{
		Subject:   sub,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(expiresIn)),
		ID:        "jti-rs",
	}
}

func signRS256(t *testing.T, key *rsa.PrivateKey, sub string, expiresIn time.Duration) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claimsFor(sub, expiresIn)).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRSAVerifierHappyPath(t *testing.T) {
	v := RSAVerifier(&testRSAKey.PublicKey)
	if v.Alg() != AlgRS256 {
		t.Fatalf("alg = %q", v.Alg())
	}
	claims, err := v.Parse(signRS256(t, testRSAKey, "creator-rs", 15*time.Minute))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if claims.Subject != "creator-rs" {
		t.Errorf("sub = %q", claims.Subject)
	}
}

func TestRSAVerifierRejectsExpiredAndSubjectless(t *testing.T) {
	v := RSAVerifier(&testRSAKey.PublicKey)
	if _, err := v.Parse(signRS256(t, testRSAKey, "creator-rs", -time.Minute)); err == nil {
		t.Error("expired RS256 token was accepted")
	}
	if _, err := v.Parse(signRS256(t, testRSAKey, "", 15*time.Minute)); err == nil {
		t.Error("subject-less RS256 token was accepted")
	}
}

func TestRSAVerifierRejectsWrongKey(t *testing.T) {
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v := RSAVerifier(&testRSAKey.PublicKey)
	if _, err := v.Parse(signRS256(t, other, "creator-rs", time.Minute)); err == nil {
		t.Error("token signed by a different key was accepted")
	}
}

// TestAlgConfusionHS256TokenAgainstRS256Verifier is the attack the §5.4
// RS256 story has to survive. The RSA public key is, by definition,
// public. An attacker takes the PEM, HMAC-signs a token they wrote
// themselves using those PEM bytes as the shared secret, and stamps
// alg:HS256 on the header. A verifier that trusts the token's own header
// to pick the algorithm hands the "secret" to HMAC verification, the
// signature checks out, and the attacker is now any creator they like.
//
// The verifier is pinned to RS256, so this must fail — in every spelling
// of "the secret" an attacker might try.
func TestAlgConfusionHS256TokenAgainstRS256Verifier(t *testing.T) {
	v := RSAVerifier(&testRSAKey.PublicKey)
	pubPEM := publicKeyPEM(t, &testRSAKey.PublicKey)
	pkcs1 := pkcs1PublicKeyPEM(t, &testRSAKey.PublicKey)
	der, err := x509.MarshalPKIXPublicKey(&testRSAKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	secrets := map[string][]byte{
		"PKIX PEM":            pubPEM,
		"PKIX PEM no newline": pubPEM[:len(pubPEM)-1],
		"PKCS#1 PEM":          pkcs1,
		"raw DER":             der,
		"modulus bytes":       testRSAKey.PublicKey.N.Bytes(),
	}
	for name, secret := range secrets {
		t.Run(name, func(t *testing.T) {
			forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256,
				claimsFor("attacker-owns-you", time.Hour)).SignedString(secret)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := v.Parse(forged); err == nil {
				t.Fatal("ALG CONFUSION: an HS256 token signed with the public key was accepted by an RS256 verifier")
			}
		})
	}
}

// TestAlgConfusionRS256TokenAgainstHS256Verifier is the other direction:
// an HS256-configured verifier must not accept a validly-signed RS256
// token just because the signature is cryptographically sound. The
// deployment said HS256; anything else is a downgrade/upgrade attack.
func TestAlgConfusionRS256TokenAgainstHS256Verifier(t *testing.T) {
	v := HMACVerifier([]byte("test-secret"))
	if _, err := v.Parse(signRS256(t, testRSAKey, "creator-rs", time.Hour)); err == nil {
		t.Fatal("ALG CONFUSION: an RS256 token was accepted by an HS256-configured verifier")
	}
}

// TestAlgNoneRejected covers the oldest JWT bug of all.
func TestAlgNoneRejected(t *testing.T) {
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone,
		claimsFor("nobody", time.Hour)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]*Verifier{
		"HS256": HMACVerifier([]byte("test-secret")),
		"RS256": RSAVerifier(&testRSAKey.PublicKey),
	} {
		if _, err := v.Parse(unsigned); err == nil {
			t.Errorf("%s verifier accepted an alg:none token", name)
		}
	}
}

// TestKeyFuncGuardsIndependently proves the two defences are independent:
// even reached directly (as it would be if a future caller forgot
// jwt.WithValidMethods), keyFunc refuses to hand a key to the wrong
// method family.
func TestKeyFuncGuardsIndependently(t *testing.T) {
	rs := RSAVerifier(&testRSAKey.PublicKey)
	if _, err := rs.keyFunc(&jwt.Token{Method: jwt.SigningMethodHS256}); err == nil {
		t.Error("RS256 verifier handed a key to an HMAC method")
	}
	hs := HMACVerifier([]byte("s"))
	if _, err := hs.keyFunc(&jwt.Token{Method: jwt.SigningMethodRS256}); err == nil {
		t.Error("HS256 verifier handed a key to an RSA method")
	}
	// And a verifier with no key at all never produces one.
	empty := &Verifier{alg: AlgRS256}
	if _, err := empty.keyFunc(&jwt.Token{Method: jwt.SigningMethodRS256}); err == nil {
		t.Error("a keyless RS256 verifier produced a key")
	}
}

func TestNilVerifierRejects(t *testing.T) {
	var v *Verifier
	if _, err := v.Parse("anything"); err == nil {
		t.Error("nil verifier accepted a token")
	}
}

func TestVerifierFromEnvDefaultsToHS256(t *testing.T) {
	v, err := VerifierFromEnv(envMap(map[string]string{EnvJWTSecret: "test-secret"}))
	if err != nil {
		t.Fatalf("VerifierFromEnv: %v", err)
	}
	if v.Alg() != AlgHS256 {
		t.Fatalf("alg = %q, want the HS256 default", v.Alg())
	}
	if _, err := v.Parse(signToken(t, []byte("test-secret"), "creator-1", time.Minute)); err != nil {
		t.Errorf("Parse: %v", err)
	}
}

func TestVerifierFromEnvHS256RequiresSecret(t *testing.T) {
	if _, err := VerifierFromEnv(envMap(nil)); err == nil {
		t.Error("want an error when JWT_SECRET is unset")
	}
}

func TestVerifierFromEnvRS256Inline(t *testing.T) {
	v, err := VerifierFromEnv(envMap(map[string]string{
		EnvJWTAlg:       "RS256",
		EnvJWTPublicKey: string(publicKeyPEM(t, &testRSAKey.PublicKey)),
	}))
	if err != nil {
		t.Fatalf("VerifierFromEnv: %v", err)
	}
	if v.Alg() != AlgRS256 {
		t.Fatalf("alg = %q", v.Alg())
	}
	if _, err := v.Parse(signRS256(t, testRSAKey, "creator-rs", time.Minute)); err != nil {
		t.Errorf("Parse: %v", err)
	}
}

func TestVerifierFromEnvRS256File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jwt.pub")
	if err := os.WriteFile(path, publicKeyPEM(t, &testRSAKey.PublicKey), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := VerifierFromEnv(envMap(map[string]string{
		EnvJWTAlg:           "rs256", // case-insensitive
		EnvJWTPublicKeyFile: path,
	}))
	if err != nil {
		t.Fatalf("VerifierFromEnv: %v", err)
	}
	if _, err := v.Parse(signRS256(t, testRSAKey, "creator-rs", time.Minute)); err != nil {
		t.Errorf("Parse: %v", err)
	}
	// Inline wins over the file when both are set.
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v, err = VerifierFromEnv(envMap(map[string]string{
		EnvJWTAlg:           "RS256",
		EnvJWTPublicKey:     string(publicKeyPEM(t, &other.PublicKey)),
		EnvJWTPublicKeyFile: path,
	}))
	if err != nil {
		t.Fatalf("VerifierFromEnv: %v", err)
	}
	if _, err := v.Parse(signRS256(t, other, "creator-rs", time.Minute)); err != nil {
		t.Errorf("inline key should win: %v", err)
	}
}

func TestVerifierFromEnvBadConfig(t *testing.T) {
	tests := map[string]map[string]string{
		"unknown alg":       {EnvJWTAlg: "ES256"},
		"RS256 without key": {EnvJWTAlg: "RS256"},
		"missing file":      {EnvJWTAlg: "RS256", EnvJWTPublicKeyFile: "/nonexistent/jwt.pub"},
		"not PEM":           {EnvJWTAlg: "RS256", EnvJWTPublicKey: "definitely not a pem"},
		"truncated PEM":     {EnvJWTAlg: "RS256", EnvJWTPublicKey: "-----BEGIN PUBLIC KEY-----\nZm9v\n-----END PUBLIC KEY-----\n"},
		"private key given as public": {
			EnvJWTAlg: "RS256",
			EnvJWTPublicKey: string(pem.EncodeToMemory(&pem.Block{
				Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(testRSAKey),
			})),
		},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := VerifierFromEnv(envMap(env)); err == nil {
				t.Error("want a startup error")
			}
		})
	}
}

func TestParseRSAPublicKeyAcceptsPKCS1(t *testing.T) {
	pub, err := ParseRSAPublicKey(pkcs1PublicKeyPEM(t, &testRSAKey.PublicKey))
	if err != nil {
		t.Fatalf("ParseRSAPublicKey: %v", err)
	}
	if pub.N.Cmp(testRSAKey.PublicKey.N) != 0 {
		t.Error("parsed a different key")
	}
}

func TestParseRSAPublicKeyRejectsNonRSA(t *testing.T) {
	// An EC key in a PKIX block is well-formed PEM but the wrong type.
	if _, err := ParseRSAPublicKey([]byte(ecPublicKeyPEM)); err == nil {
		t.Error("an EC public key was accepted as RSA")
	}
}

const ecPublicKeyPEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEH4E4ppRSpqxIYAtRvJDCM7TgSDl1
5Y4rlOhCZKPmCyPYCa7BE1H+VOm9Y5rEZ0d1JX6qk0OEqNlH9RcJVuNn0g==
-----END PUBLIC KEY-----
`

// TestAuthMiddlewareRS256 checks the middleware end to end under RS256.
func TestAuthMiddlewareRS256(t *testing.T) {
	h := Auth(RSAVerifier(&testRSAKey.PublicKey))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if CreatorIDFrom(r.Context()) != "creator-rs" {
			t.Errorf("creator id = %q", CreatorIDFrom(r.Context()))
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+signRS256(t, testRSAKey, "creator-rs", 15*time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	// And the confusion attack through the HTTP surface, not just Parse.
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256,
		claimsFor("creator-rs", time.Hour)).SignedString(publicKeyPEM(t, &testRSAKey.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+forged)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertUnauthenticated(t, rec)
}

func envMap(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}
