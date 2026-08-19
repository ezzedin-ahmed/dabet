// Package credits is the internal credits-ok contract of docs §5.8:
// GET /internal/v1/credits-ok/{creator_id} -> 200 {"ok":true|false}.
//
// The client caches per-creator answers for 60s and fails open (returns
// true) on any transport error — moderation never blocks on a synchronous
// credits lookup. Callers count fail-opens themselves via OnFailOpen
// (fail_open_total{reason="no_credits"} is for the ok=false path, not for
// transport failures).
package credits

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"dabet/pkg/tracing"
)

// PathPrefix is the internal endpoint path prefix; the creator_id follows.
const PathPrefix = "/internal/v1/credits-ok/"

type okBody struct {
	OK bool `json:"ok"`
}

type cacheEntry struct {
	ok      bool
	expires time.Time
}

// Client queries credits-service for the advisory credits_ok flag.
type Client struct {
	base  string
	httpc *http.Client
	ttl   time.Duration

	// OnFailOpen, when set, is called once per fail-open (transport or
	// decode error) so the caller can count it.
	OnFailOpen func(err error)

	mu    sync.Mutex
	cache map[string]cacheEntry
	now   func() time.Time
}

// Option configures a Client.
type Option func(*Client)

// WithTTL overrides the 60s cache TTL.
func WithTTL(d time.Duration) Option { return func(c *Client) { c.ttl = d } }

// WithHTTPClient overrides the underlying HTTP client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpc = h } }

// NewClient builds a client for the credits-service at baseURL.
func NewClient(baseURL string, opts ...Option) *Client {
	c := &Client{
		base:  strings.TrimSuffix(baseURL, "/"),
		httpc: tracing.HTTPClient(2 * time.Second),
		ttl:   60 * time.Second,
		cache: make(map[string]cacheEntry),
		now:   time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// OK reports whether the creator has credits. Cached per creator for the
// TTL; fails open to true on any error without caching the failure.
func (c *Client) OK(ctx context.Context, creatorID string) bool {
	now := c.now()
	c.mu.Lock()
	if e, hit := c.cache[creatorID]; hit && now.Before(e.expires) {
		c.mu.Unlock()
		return e.ok
	}
	c.mu.Unlock()

	ok, err := c.fetch(ctx, creatorID)
	if err != nil {
		if c.OnFailOpen != nil {
			c.OnFailOpen(err)
		}
		return true
	}
	c.mu.Lock()
	c.cache[creatorID] = cacheEntry{ok: ok, expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return ok
}

func (c *Client) fetch(ctx context.Context, creatorID string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.base+PathPrefix+url.PathEscape(creatorID), nil)
	if err != nil {
		return false, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("credits-ok: unexpected status %d", resp.StatusCode)
	}
	var body okBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, err
	}
	return body.OK, nil
}

// Handler serves the credits-ok contract for credits-service. check
// resolves the flag; an error renders a 500, which clients treat as
// fail-open.
func Handler(check func(ctx context.Context, creatorID string) (bool, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		creatorID := strings.TrimPrefix(r.URL.Path, PathPrefix)
		if creatorID == "" || strings.Contains(creatorID, "/") {
			http.NotFound(w, r)
			return
		}
		ok, err := check(r.Context(), creatorID)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(okBody{OK: ok})
	})
}
