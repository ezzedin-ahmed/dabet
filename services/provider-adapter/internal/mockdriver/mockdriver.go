// Package mockdriver is the deterministic "mock" platform used for local
// end-to-end runs: messages are injected over HTTP instead of arriving
// from a real chat, and deletes are recorded in memory instead of hitting
// a provider API. No external dependencies, no timers, no randomness.
package mockdriver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dabet/services/provider-adapter/internal/driver"
)

// PlatformName is the registry key for the mock driver.
const PlatformName = "mock"

// DeletionRecord is one recorded Delete call, exposed on /mock/deletions.
type DeletionRecord struct {
	ConnectionID string    `json:"connection_id"`
	ContentID    string    `json:"content_id"`
	MessageID    string    `json:"message_id"`
	DeletedAt    time.Time `json:"deleted_at"`
}

// Driver implements driver.Driver for the mock platform.
type Driver struct {
	mu        sync.Mutex
	queues    map[string]chan driver.Message // per connection ID
	deletions []DeletionRecord
	now       func() time.Time
}

// New returns a mock driver. now stamps deletion records; pass nil for
// time.Now.
func New(now func() time.Time) *Driver {
	if now == nil {
		now = time.Now
	}
	return &Driver{queues: make(map[string]chan driver.Message), now: now}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return PlatformName }

// queue returns (creating if needed) the injection queue for a connection.
// Queues exist independently of watch loops so injections that race a
// watcher start are buffered, not lost.
func (d *Driver) queue(connectionID string) chan driver.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	q, ok := d.queues[connectionID]
	if !ok {
		q = make(chan driver.Message, 1024)
		d.queues[connectionID] = q
	}
	return q
}

// Inject feeds one message into a connection's Watch stream. Returns an
// error when the queue is full (bounded, deterministic backpressure).
func (d *Driver) Inject(connectionID string, msg driver.Message) error {
	select {
	case d.queue(connectionID) <- msg:
		return nil
	default:
		return fmt.Errorf("mockdriver: injection queue full for connection %s", connectionID)
	}
}

// Watch implements driver.Driver: it relays injected messages until ctx is
// cancelled, then returns nil (clean shutdown).
func (d *Driver) Watch(ctx context.Context, conn driver.Connection, out chan<- driver.Message) error {
	q := d.queue(conn.ID)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-q:
			select {
			case <-ctx.Done():
				return nil
			case out <- msg:
			}
		}
	}
}

// Delete implements driver.Driver: it records the call and succeeds.
func (d *Driver) Delete(_ context.Context, conn driver.Connection, contentID, messageID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deletions = append(d.deletions, DeletionRecord{
		ConnectionID: conn.ID,
		ContentID:    contentID,
		MessageID:    messageID,
		DeletedAt:    d.now().UTC(),
	})
	return nil
}

// Deletions returns a snapshot of recorded deletions.
func (d *Driver) Deletions() []DeletionRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DeletionRecord, len(d.deletions))
	copy(out, d.deletions)
	return out
}

// DiscoverLive implements driver.Driver. The mock platform is "live"
// whenever something is injected, so discovery reports nothing.
func (d *Driver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return []driver.ContentRef{}, nil
}
