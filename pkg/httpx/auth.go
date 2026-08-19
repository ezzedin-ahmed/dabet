package httpx

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Auth validates a Bearer JWT (HS256 in local, claims sub/iat/exp/jti per
// docs §5.4) and stores the subject as the creator_id in the request
// context. Failures render the unauthenticated envelope.
func Auth(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || raw == "" {
				WriteError(w, r, CodeUnauthenticated, "missing bearer token", nil)
				return
			}
			claims := &jwt.RegisteredClaims{}
			tok, err := jwt.ParseWithClaims(raw, claims,
				func(*jwt.Token) (any, error) { return secret, nil },
				jwt.WithValidMethods([]string{"HS256"}),
				jwt.WithExpirationRequired(),
			)
			if err != nil || !tok.Valid || claims.Subject == "" {
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
