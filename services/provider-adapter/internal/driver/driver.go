// Package driver defines the platform driver contract of docs §7.2. The
// whole of N3 (extensible platforms) rests on this interface: adding a
// platform is one new Driver implementation and one registry entry, and
// nothing outside provider-adapter changes.
package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Connection is one linked platform account, resolved to its Dabet creator.
// The adapter receives connections from Area A (§5.5); tokens ride along so
// drivers can authenticate without a lookup.
type Connection struct {
	// ID is the Dabet connection id.
	ID string
	// CreatorID is the owning Dabet creator UUID. The adapter stamps it on
	// every ingested message so no downstream consumer needs a lookup (§4.2).
	CreatorID string
	// Platform names the driver that owns this connection.
	Platform string
	// NativeUserID is the platform-native account id of the connected user
	// (e.g. the Twitch moderator id used on delete calls).
	NativeUserID string
	// AccessToken is the current OAuth access token (or bot token).
	AccessToken string
}

// ContentRef identifies one currently-live piece of content (a stream, a
// live chat, a channel) in platform-native terms.
type ContentRef struct {
	// NativeChannelID is the platform-native channel/chat identifier. It is
	// the input to opaque content_id minting and never leaves the adapter.
	NativeChannelID string
	// Title is a human-readable hint, for logs and debugging only.
	Title string
}

// Message is one raw chat message as received from a platform, in native
// terms. The ingest loop mints the opaque IDs and stamps ingested_at; the
// driver only reports what the platform said.
type Message struct {
	NativeChannelID string
	NativeAuthorID  string
	NativeMessageID string
	Text            string
}

// Driver is the per-platform integration (docs §7.2, quoted verbatim).
type Driver interface {
	Platform() string
	// Watch streams messages for one connection until ctx is cancelled.
	Watch(ctx context.Context, conn Connection, out chan<- Message) error
	// Delete removes one message. Returns nil if already gone.
	Delete(ctx context.Context, conn Connection, contentID, messageID string) error
	// DiscoverLive reports currently-live content for a connection.
	DiscoverLive(ctx context.Context, conn Connection) ([]ContentRef, error)
}

// Resolver maps opaque adapter IDs back to platform-native identifiers.
// Only drivers (inside this service) may use it — opaque IDs are never
// parsed outside provider-adapter (P5, docs §1.4).
type Resolver interface {
	// NativeContentID returns the platform-native channel id behind an
	// opaque content_id, if known to this instance.
	NativeContentID(contentID string) (string, bool)
	// NativeMessageID returns the platform-native message id behind an
	// opaque message_id, if recoverable.
	NativeMessageID(messageID string) (string, bool)
}

// Error classes drivers translate provider responses into. The deletion
// consumer maps these onto the §7.2 response table; any other error is
// treated as transient (5xx-class) and retried with backoff.
var (
	// ErrNotImplemented marks driver methods that need live platform APIs
	// not yet wired (stub drivers).
	ErrNotImplemented = errors.New("driver: not implemented")
	// ErrNotFound: message not found / already deleted. Treated as success —
	// the viewer or another mod got there first.
	ErrNotFound = errors.New("driver: message not found or already deleted")
	// ErrGone: stream ended / content gone. Terminal drop.
	ErrGone = errors.New("driver: content gone")
	// ErrRateLimited: provider returned 429. Backoff with jitter, retry.
	ErrRateLimited = errors.New("driver: rate limited")
	// ErrUnauthorized: provider returned 401. Refresh token (§5.6), retry once.
	ErrUnauthorized = errors.New("driver: unauthorized")
)

// FromHTTPStatus maps a provider HTTP status onto the shared error classes.
// 2xx is success; unknown non-2xx statuses become transient errors so the
// deletion consumer retries them with backoff up to its attempt cap.
func FromHTTPStatus(status int) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusNotFound:
		return ErrNotFound
	case status == http.StatusGone:
		return ErrGone
	case status == http.StatusTooManyRequests:
		return ErrRateLimited
	case status == http.StatusUnauthorized:
		return ErrUnauthorized
	default:
		return fmt.Errorf("driver: provider returned status %d", status)
	}
}

// Registry holds drivers keyed by platform name.
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{drivers: make(map[string]Driver)}
}

// Register adds a driver under its Platform() name. Registering the same
// platform twice is a programming error and panics at startup.
func (r *Registry) Register(d Driver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := d.Platform()
	if _, dup := r.drivers[p]; dup {
		panic(fmt.Sprintf("driver: platform %q registered twice", p))
	}
	r.drivers[p] = d
}

// Get returns the driver for a platform.
func (r *Registry) Get(platform string) (Driver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.drivers[platform]
	return d, ok
}

// Platforms lists registered platform names, sorted.
func (r *Registry) Platforms() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.drivers))
	for p := range r.drivers {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
