// Command mockoauth is a deterministic OAuth 2.0 authorization-code +
// PKCE provider for local end-to-end runs. It speaks exactly the four
// endpoints user-service's OAuth client and provider-adapter's token
// refresher expect (docs §5.5, §5.6), so the `mock` platform can be
// connected end to end with no third party involved:
//
//	GET  /oauth/authorize  -> 302 back to redirect_uri with ?code&state
//	POST /oauth/token      -> access/refresh token JSON (code or refresh grant)
//	GET  /oauth/userinfo   -> {"id":…,"name":…} for the bearer token
//	POST /oauth/revoke     -> 200, token forgotten
//
// PKCE S256 is verified for real: a token exchange whose code_verifier
// does not hash to the authorization's code_challenge is rejected. That
// keeps the e2e honest about the CSRF/PKCE half of §5.5 rather than
// rubber-stamping it.
//
// Every authorization mints a fresh provider user id, so repeated e2e
// runs do not collide on identity's `connections_active_uniq` partial
// unique index (§5.2).
//
// No dependencies, no timers, no persistence — state is in memory and
// dies with the process.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// tokenTTL is what the token endpoint reports as expires_in.
const tokenTTL = 3600 * time.Second

// authorization is one pending authorization code.
type authorization struct {
	challenge   string // PKCE S256 challenge, empty when the client sent none
	method      string
	scope       string
	redirectURI string
	userID      string
	userName    string
}

// grant is one issued access token.
type grant struct {
	scope    string
	userID   string
	userName string
}

type server struct {
	mu      sync.Mutex
	codes   map[string]authorization // code -> pending authorization
	access  map[string]grant         // access_token -> grant
	refresh map[string]grant         // refresh_token -> grant
}

func newServer() *server {
	return &server{
		codes:   make(map[string]authorization),
		access:  make(map[string]grant),
		refresh: make(map[string]grant),
	}
}

func randomID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand does not fail in practice; fall back to a clock value
		// rather than crashing a test fixture.
		return prefix + hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return prefix + hex.EncodeToString(b)
}

// challengeFor is the RFC 7636 S256 transformation.
func challengeFor(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// handleAuthorize consents immediately and bounces back to redirect_uri.
// A real provider would render a consent screen here; the mock's whole
// job is to be the step a test can drive with one HTTP GET.
func (s *server) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirectURI := q.Get("redirect_uri")
	if redirectURI == "" {
		http.Error(w, "redirect_uri is required", http.StatusBadRequest)
		return
	}
	if rt := q.Get("response_type"); rt != "" && rt != "code" {
		http.Error(w, "unsupported response_type", http.StatusBadRequest)
		return
	}
	// user_id lets a test pin the provider identity (e.g. to exercise the
	// already-connected 409); otherwise every run gets a fresh one.
	userID := q.Get("user_id")
	if userID == "" {
		userID = randomID("mu_")
	}

	code := randomID("code_")
	s.mu.Lock()
	s.codes[code] = authorization{
		challenge:   q.Get("code_challenge"),
		method:      q.Get("code_challenge_method"),
		scope:       q.Get("scope"),
		redirectURI: redirectURI,
		userID:      userID,
		userName:    "mockchannel-" + strings.TrimPrefix(userID, "mu_"),
	}
	s.mu.Unlock()

	target, err := url.Parse(redirectURI)
	if err != nil {
		http.Error(w, "malformed redirect_uri", http.StatusBadRequest)
		return
	}
	rq := target.Query()
	rq.Set("code", code)
	if state := q.Get("state"); state != "" {
		rq.Set("state", state)
	}
	target.RawQuery = rq.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope"`
}

func writeOAuthError(w http.ResponseWriter, status int, code, desc string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "error_description": desc})
}

// issue mints a token pair for g and records both halves.
func (s *server) issue(g grant) tokenResponse {
	at, rt := randomID("mat_"), randomID("mrt_")
	s.mu.Lock()
	s.access[at] = g
	s.refresh[rt] = g
	s.mu.Unlock()
	return tokenResponse{
		AccessToken:  at,
		RefreshToken: rt,
		TokenType:    "bearer",
		ExpiresIn:    int64(tokenTTL.Seconds()),
		Scope:        g.scope,
	}
}

func (s *server) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "unparseable form body")
		return
	}
	switch r.PostForm.Get("grant_type") {
	case "authorization_code":
		s.exchangeCode(w, r)
	case "refresh_token":
		s.exchangeRefresh(w, r)
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token")
	}
}

func (s *server) exchangeCode(w http.ResponseWriter, r *http.Request) {
	code := r.PostForm.Get("code")
	s.mu.Lock()
	auth, ok := s.codes[code]
	delete(s.codes, code) // single use, like a real provider
	s.mu.Unlock()
	if !ok {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "unknown or already-redeemed code")
		return
	}
	// PKCE is verified for real so the e2e proves the §5.5 flow rather
	// than a rubber stamp.
	if auth.challenge != "" {
		verifier := r.PostForm.Get("code_verifier")
		if verifier == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code_verifier is required")
			return
		}
		expected := verifier
		if auth.method == "" || strings.EqualFold(auth.method, "S256") {
			expected = challengeFor(verifier)
		}
		if expected != auth.challenge {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "code_verifier does not match code_challenge")
			return
		}
	}
	if ru := r.PostForm.Get("redirect_uri"); ru != "" && ru != auth.redirectURI {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "redirect_uri mismatch")
		return
	}
	writeJSON(w, s.issue(grant{scope: auth.scope, userID: auth.userID, userName: auth.userName}))
}

func (s *server) exchangeRefresh(w http.ResponseWriter, r *http.Request) {
	token := r.PostForm.Get("refresh_token")
	s.mu.Lock()
	g, ok := s.refresh[token]
	delete(s.refresh, token) // rotate on use
	s.mu.Unlock()
	if !ok {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "unknown refresh token")
		return
	}
	writeJSON(w, s.issue(g))
}

func (s *server) handleUserinfo(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer"))
	s.mu.Lock()
	g, ok := s.access[token]
	s.mu.Unlock()
	if !ok {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "unknown access token")
		return
	}
	// {id,name} is the Google-userinfo shape, which user-service's
	// tolerant parser accepts alongside the Twitch and Discord ones.
	writeJSON(w, map[string]string{"id": g.userID, "name": g.userName})
}

func (s *server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	token := r.PostForm.Get("token")
	s.mu.Lock()
	delete(s.access, token)
	delete(s.refresh, token)
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /oauth/authorize", s.handleAuthorize)
	mux.HandleFunc("POST /oauth/token", s.handleToken)
	mux.HandleFunc("GET /oauth/userinfo", s.handleUserinfo)
	mux.HandleFunc("POST /oauth/revoke", s.handleRevoke)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":9099"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           newServer().routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("mockoauth listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
