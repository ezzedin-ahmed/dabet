// Package api implements the §5.4 auth endpoints on top of the shared
// httpx conventions (§4.1): error envelope, strict JSON decoding,
// request IDs. Per P4, no password or token ever appears in a log line,
// metric label, or error message — with the single documented dev-mode
// exception in handleRegister.
package api

import (
	"errors"
	"log/slog"
	"net/http"
	"net/mail"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/httpx"

	"dabet/services/user-service/internal/auth"
	"dabet/services/user-service/internal/repo"
)

// VerificationTokenTTL bounds how long an email verification token stays
// redeemable.
const VerificationTokenTTL = 24 * time.Hour

// Handler serves the /v1/auth endpoints and /v1/me.
type Handler struct {
	Repo      repo.Repository
	JWTSecret []byte
	Logger    *slog.Logger
	// Logins is auth_logins_total{outcome} (§5.9).
	Logins *prometheus.CounterVec

	// Now and NewToken are injection points for tests.
	Now      func() time.Time
	NewToken func() (raw, hash string, err error)

	AccessTTL  time.Duration
	RefreshTTL time.Duration
	VerifyTTL  time.Duration

	// dummyHash keeps login constant-time for unknown emails: the
	// password is verified against this hash so both 401 paths cost one
	// Argon2id computation.
	dummyHash string
}

// NewLoginsCounter builds auth_logins_total{outcome} for registration
// against a service registry.
func NewLoginsCounter() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "auth_logins_total",
		Help: "Login attempts by outcome.",
	}, []string{"outcome"})
}

// NewHandler wires a Handler with production defaults.
func NewHandler(r repo.Repository, jwtSecret []byte, logger *slog.Logger, logins *prometheus.CounterVec) (*Handler, error) {
	dummy, err := auth.HashPassword(uuid.NewString() + uuid.NewString())
	if err != nil {
		return nil, err
	}
	return &Handler{
		Repo:       r,
		JWTSecret:  jwtSecret,
		Logger:     logger,
		Logins:     logins,
		Now:        func() time.Time { return time.Now().UTC() },
		NewToken:   auth.NewOpaqueToken,
		AccessTTL:  auth.AccessTokenTTL,
		RefreshTTL: auth.RefreshTokenTTL,
		VerifyTTL:  VerificationTokenTTL,
		dummyHash:  dummy,
	}, nil
}

// Routes registers the §5.4 endpoints on mux. /v1/me is wrapped in the
// shared JWT auth middleware.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/auth/register", h.handleRegister)
	mux.HandleFunc("POST /v1/auth/verify", h.handleVerify)
	mux.HandleFunc("POST /v1/auth/login", h.handleLogin)
	mux.HandleFunc("POST /v1/auth/refresh", h.handleRefresh)
	mux.HandleFunc("POST /v1/auth/logout", h.handleLogout)
	mux.Handle("GET /v1/me", httpx.Auth(h.JWTSecret)(http.HandlerFunc(h.handleMe)))
}

func (h *Handler) internal(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.Logger.Error("internal error", "op", op, "error", err.Error(),
		"request_id", httpx.RequestIDFrom(r.Context()))
	httpx.WriteError(w, r, httpx.CodeInternalError, "internal error", nil)
}

type registerRequest struct {
	Email    string `json:"email"`
	Fullname string `json:"fullname"`
	Password string `json:"password"`
}

func (h *Handler) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if field, msg := validateRegister(req); field != "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, msg, map[string]any{"field": field})
		return
	}

	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		h.internal(w, r, "hash password", err)
		return
	}
	creatorID, err := h.Repo.CreateCreator(r.Context(), req.Email, req.Fullname, hash)
	if errors.Is(err, repo.ErrDuplicateEmail) {
		httpx.WriteError(w, r, httpx.CodeConflict, "email already registered", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "create creator", err)
		return
	}

	now := h.Now()
	raw, tokenHash, err := h.NewToken()
	if err != nil {
		h.internal(w, r, "issue verification token", err)
		return
	}
	if err := h.Repo.CreateEmailVerification(r.Context(), creatorID, tokenHash, now.Add(h.VerifyTTL)); err != nil {
		h.internal(w, r, "store verification token", err)
		return
	}
	// DEVIATION (documented): no mailer exists in v1, so the debug log is
	// the dev-mode delivery channel for the verification token. This is a
	// deliberate, narrow exception to P4's no-token-logging rule; the
	// default logger level is info, so it is silent unless debug logging
	// is explicitly enabled.
	h.Logger.Debug("email verification token issued (dev-mode delivery channel)",
		"creator_id", creatorID, "verification_token", raw)

	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"creator_id": creatorID})
}

func validateRegister(req registerRequest) (field, msg string) {
	if req.Email == "" || len(req.Email) > 254 {
		return "email", "email must be a valid address"
	}
	if a, err := mail.ParseAddress(req.Email); err != nil || a.Address != req.Email {
		return "email", "email must be a valid address"
	}
	if n := utf8.RuneCountInString(req.Fullname); n == 0 || n > 32 {
		return "fullname", "fullname must be 1-32 characters"
	}
	if err := auth.ValidatePassword(req.Password); err != nil {
		return "password", err.Error()
	}
	return "", ""
}

type verifyRequest struct {
	Token string `json:"token"`
}

func (h *Handler) handleVerify(w http.ResponseWriter, r *http.Request) {
	var req verifyRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if req.Token == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "token is required", map[string]any{"field": "token"})
		return
	}
	err := h.Repo.ConsumeEmailVerification(r.Context(), auth.HashToken(req.Token), h.Now())
	if errors.Is(err, repo.ErrNotFound) {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid or expired verification token", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "consume verification token", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// unauthenticated writes the single 401 shape shared by every login and
// refresh failure, so responses are not an account-enumeration oracle.
func (h *Handler) unauthenticated(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, r, httpx.CodeUnauthenticated, "invalid credentials", nil)
}

func (h *Handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if !httpx.Decode(w, r, &req) {
		return
	}

	creator, err := h.Repo.CreatorByEmail(r.Context(), req.Email)
	switch {
	case errors.Is(err, repo.ErrNotFound):
		// Burn the same Argon2id cost as the known-email path, then fail
		// with the identical 401 (§5.4).
		_, _ = auth.VerifyPassword(h.dummyHash, req.Password)
		h.Logins.WithLabelValues("failure").Inc()
		h.unauthenticated(w, r)
		return
	case err != nil:
		h.Logins.WithLabelValues("error").Inc()
		h.internal(w, r, "load creator", err)
		return
	}

	ok, err := auth.VerifyPassword(creator.PasswordHash, req.Password)
	if err != nil {
		h.Logins.WithLabelValues("error").Inc()
		h.internal(w, r, "verify password", err)
		return
	}
	if !ok {
		h.Logins.WithLabelValues("failure").Inc()
		h.unauthenticated(w, r)
		return
	}

	resp, err := h.issueTokens(r, creator.ID, uuid.NewString())
	if err != nil {
		h.Logins.WithLabelValues("error").Inc()
		h.internal(w, r, "issue tokens", err)
		return
	}
	h.Logins.WithLabelValues("success").Inc()
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// issueTokens mints an access JWT plus a refresh token in familyID and
// stores the refresh token hash.
func (h *Handler) issueTokens(r *http.Request, creatorID, familyID string) (*tokenResponse, error) {
	now := h.Now()
	access, err := auth.IssueAccessToken(h.JWTSecret, creatorID, now, h.AccessTTL)
	if err != nil {
		return nil, err
	}
	raw, hash, err := h.NewToken()
	if err != nil {
		return nil, err
	}
	if err := h.Repo.InsertRefreshToken(r.Context(), &repo.RefreshToken{
		ID:        uuid.NewString(),
		CreatorID: creatorID,
		FamilyID:  familyID,
		TokenHash: hash,
		ExpiresAt: now.Add(h.RefreshTTL),
	}); err != nil {
		return nil, err
	}
	return &tokenResponse{
		AccessToken:  access,
		RefreshToken: raw,
		ExpiresIn:    int64(h.AccessTTL.Seconds()),
	}, nil
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (h *Handler) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if req.RefreshToken == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "refresh_token is required", map[string]any{"field": "refresh_token"})
		return
	}

	now := h.Now()
	tok, err := h.Repo.RefreshTokenByHash(r.Context(), auth.HashToken(req.RefreshToken))
	switch {
	case errors.Is(err, repo.ErrNotFound):
		h.unauthenticated(w, r)
		return
	case err != nil:
		h.internal(w, r, "load refresh token", err)
		return
	}

	// Reuse of a rotated (or otherwise revoked) token: revoke the entire
	// family (A2) — the stolen-token alarm.
	if tok.RevokedAt != nil {
		if err := h.Repo.RevokeRefreshFamily(r.Context(), tok.FamilyID, now); err != nil {
			h.internal(w, r, "revoke token family", err)
			return
		}
		h.Logger.Warn("refresh token reuse detected; family revoked",
			"creator_id", tok.CreatorID, "family_id", tok.FamilyID,
			"request_id", httpx.RequestIDFrom(r.Context()))
		h.unauthenticated(w, r)
		return
	}
	if !tok.ExpiresAt.After(now) {
		h.unauthenticated(w, r)
		return
	}

	access, err := auth.IssueAccessToken(h.JWTSecret, tok.CreatorID, now, h.AccessTTL)
	if err != nil {
		h.internal(w, r, "issue access token", err)
		return
	}
	raw, hash, err := h.NewToken()
	if err != nil {
		h.internal(w, r, "issue refresh token", err)
		return
	}
	next := &repo.RefreshToken{
		ID:        uuid.NewString(),
		CreatorID: tok.CreatorID,
		FamilyID:  tok.FamilyID,
		TokenHash: hash,
		ExpiresAt: now.Add(h.RefreshTTL),
	}
	err = h.Repo.RotateRefreshToken(r.Context(), tok.ID, next, now)
	if errors.Is(err, repo.ErrNotFound) {
		// Lost a rotation race: someone else consumed this token between
		// our read and our rotate. Treat as reuse.
		if err := h.Repo.RevokeRefreshFamily(r.Context(), tok.FamilyID, now); err != nil {
			h.internal(w, r, "revoke token family", err)
			return
		}
		h.unauthenticated(w, r)
		return
	}
	if err != nil {
		h.internal(w, r, "rotate refresh token", err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, &tokenResponse{
		AccessToken:  access,
		RefreshToken: raw,
		ExpiresIn:    int64(h.AccessTTL.Seconds()),
	})
}

func (h *Handler) handleLogout(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if req.RefreshToken == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "refresh_token is required", map[string]any{"field": "refresh_token"})
		return
	}

	tok, err := h.Repo.RefreshTokenByHash(r.Context(), auth.HashToken(req.RefreshToken))
	switch {
	case errors.Is(err, repo.ErrNotFound):
		// Idempotent: an unknown token is already logged out, and a 204
		// avoids being a token-validity oracle.
	case err != nil:
		h.internal(w, r, "load refresh token", err)
		return
	default:
		// Revoke the whole family: logout ends the session, not just one
		// rotation link.
		if err := h.Repo.RevokeRefreshFamily(r.Context(), tok.FamilyID, h.Now()); err != nil {
			h.internal(w, r, "revoke token family", err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleMe(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	creator, err := h.Repo.CreatorByID(r.Context(), creatorID)
	if errors.Is(err, repo.ErrNotFound) {
		// The JWT subject no longer exists; the token is no longer good.
		httpx.WriteError(w, r, httpx.CodeUnauthenticated, "invalid or expired token", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "load creator", err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"id":             creator.ID,
		"email":          creator.Email,
		"fullname":       creator.Fullname,
		"email_verified": creator.EmailVerifiedAt != nil,
	})
}
