package httpx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var testSecret = []byte("test-secret")

func signToken(t *testing.T, secret []byte, sub string, expiresIn time.Duration) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   sub,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(expiresIn)),
		ID:        "jti-1",
	})
	s, err := tok.SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func authProbe() (http.Handler, *string) {
	var got string
	h := Auth(testSecret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = CreatorIDFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	return h, &got
}

func TestAuthValidToken(t *testing.T) {
	h, got := authProbe()
	req := httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+signToken(t, testSecret, "creator-9d4e", 15*time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", rec.Code, rec.Body.String())
	}
	if *got != "creator-9d4e" {
		t.Errorf("creator id in context = %q", *got)
	}
}

func assertUnauthenticated(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	var env ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != CodeUnauthenticated {
		t.Errorf("code = %q, want unauthenticated", env.Error.Code)
	}
}

func TestAuthExpiredToken(t *testing.T) {
	h, _ := authProbe()
	req := httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+signToken(t, testSecret, "creator-9d4e", -time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertUnauthenticated(t, rec)
}

func TestAuthMissingToken(t *testing.T) {
	h, _ := authProbe()
	req := httptest.NewRequest("GET", "/v1/me", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertUnauthenticated(t, rec)
}

func TestAuthWrongSecret(t *testing.T) {
	h, _ := authProbe()
	req := httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+signToken(t, []byte("other-secret"), "creator-9d4e", 15*time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertUnauthenticated(t, rec)
}

func TestAuthMissingSubject(t *testing.T) {
	h, _ := authProbe()
	req := httptest.NewRequest("GET", "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+signToken(t, testSecret, "", 15*time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertUnauthenticated(t, rec)
}
