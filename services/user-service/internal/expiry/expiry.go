// Package expiry implements the A6 notification: when a platform revokes
// Dabet's access, provider-adapter detects it lazily on a failed token
// refresh (§5.6), moves the connection to 'expired', and stops. The
// creator has no in-app notification system in v1, so they are told by
// email — and this is what sends it.
//
// The trigger is a sweep rather than a call from the adapter, for three
// reasons:
//
//   - The adapter marks the row expired inside its refresh transaction.
//     Anything it called afterwards would have to be undone if the
//     transaction rolled back; a row that says 'expired' is the only fact
//     that survives, so the row is what we react to.
//   - user-service owns identity.creators, so it owns the address. The
//     adapter would otherwise need a way to resolve one.
//   - It needs no new endpoint, no queue, and no change to a service this
//     work does not own — the seam is a column.
//
// expired_notified_at makes the send exactly-once: the sweep only picks
// up rows where it is NULL and stamps it after the mail is accepted.
package expiry

import (
	"context"
	"log/slog"
	"time"

	"dabet/services/user-service/internal/repo"
)

// Defaults for the sweep loop.
const (
	DefaultInterval = time.Minute
	DefaultBatch    = 50
)

// Store is the slice of the repository the sweeper needs.
type Store interface {
	PendingExpiryNotifications(ctx context.Context, limit int) ([]repo.ExpiredConnection, error)
	MarkExpiryNotified(ctx context.Context, id string, now time.Time) error
}

// Mailer sends the A6 message. internal/mail satisfies it; the send is
// asynchronous, so a nil error means "queued", not "delivered".
type Mailer interface {
	SendConnectionExpired(ctx context.Context, email, fullname, platform, displayName string) error
}

// Sweeper notifies creators whose connections the platform revoked.
type Sweeper struct {
	store    Store
	mailer   Mailer
	logger   *slog.Logger
	interval time.Duration
	batch    int
	now      func() time.Time
}

// New builds a Sweeper. A zero interval or batch takes the default.
func New(store Store, mailer Mailer, logger *slog.Logger, interval time.Duration, batch int) *Sweeper {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if batch <= 0 {
		batch = DefaultBatch
	}
	return &Sweeper{
		store: store, mailer: mailer, logger: logger,
		interval: interval, batch: batch,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// Run sweeps every interval until ctx is cancelled. Errors are logged and
// retried on the next tick: an unnotifiable creator must never wedge the
// loop, and the backlog is bounded by how many connections actually
// broke.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("connection-expiry sweep failed", "error", err.Error())
			}
		}
	}
}

// Sweep notifies one batch and returns how many messages were queued.
// A connection is stamped only once its mail has been accepted by the
// mailer, so a full queue or a shut-down mailer means the row is picked
// up again next time rather than silently skipped.
func (s *Sweeper) Sweep(ctx context.Context) (int, error) {
	pending, err := s.store.PendingExpiryNotifications(ctx, s.batch)
	if err != nil {
		return 0, err
	}
	var queued int
	for _, c := range pending {
		if c.Email == "" {
			// No address to write to: stamp it so the backlog does not
			// grow forever on a row that can never be notified.
			s.logger.Warn("expired connection has no creator address", "platform", c.Platform)
			if err := s.store.MarkExpiryNotified(ctx, c.ID, s.now()); err != nil {
				return queued, err
			}
			continue
		}
		if err := s.mailer.SendConnectionExpired(ctx, c.Email, c.Fullname, c.Platform, c.DisplayName); err != nil {
			s.logger.Warn("connection-expiry email not queued",
				"platform", c.Platform, "error", err.Error())
			continue
		}
		if err := s.store.MarkExpiryNotified(ctx, c.ID, s.now()); err != nil {
			return queued, err
		}
		queued++
	}
	return queued, nil
}
