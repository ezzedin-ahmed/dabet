package expiry

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"dabet/services/user-service/internal/repo"
)

type recorder struct {
	mu   sync.Mutex
	sent []string // "email|platform|display"
	err  error
}

func (r *recorder) SendConnectionExpired(_ context.Context, email, _, platform, display string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.sent = append(r.sent, email+"|"+platform+"|"+display)
	return nil
}

func (r *recorder) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

// fixture builds a fake repository holding one creator with one
// connection, and returns the connection id.
func fixture(t *testing.T) (*repo.Fake, string) {
	t.Helper()
	fake := repo.NewFake()
	ctx := t.Context()
	creatorID, err := fake.CreateCreator(ctx, "creator@example.test", "Ada", "hash")
	if err != nil {
		t.Fatalf("CreateCreator: %v", err)
	}
	id, err := fake.UpsertConnection(ctx, &repo.Connection{
		CreatorID: creatorID, Platform: "twitch",
		ProviderUserID: "123", DisplayName: "somechannel", AccessToken: "at",
	})
	if err != nil {
		t.Fatalf("UpsertConnection: %v", err)
	}
	return fake, id
}

func newSweeper(store Store, m Mailer) *Sweeper {
	return New(store, m, slog.New(slog.DiscardHandler), time.Millisecond, 10)
}

func TestSweepNotifiesExpiredConnectionsOnce(t *testing.T) {
	fake, id := fixture(t)
	rec := &recorder{}
	s := newSweeper(fake, rec)

	// Active connections are not A6 events.
	if n, err := s.Sweep(t.Context()); err != nil || n != 0 {
		t.Fatalf("Sweep over active connections = %d, %v", n, err)
	}

	// provider-adapter marks the connection expired (§5.6).
	fake.ExpireConnection(id)

	n, err := s.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("queued %d messages, want 1", n)
	}
	if got := rec.messages(); len(got) != 1 || got[0] != "creator@example.test|twitch|somechannel" {
		t.Fatalf("sent %v", got)
	}

	// Exactly once: the stamp keeps the next sweep quiet.
	if n, err := s.Sweep(t.Context()); err != nil || n != 0 {
		t.Fatalf("second sweep = %d, %v; want 0", n, err)
	}
	if got := rec.messages(); len(got) != 1 {
		t.Fatalf("connection notified %d times", len(got))
	}
}

// A creator-initiated disconnect (§5.5 DELETE) is not a platform
// revocation and must not generate mail.
func TestRevokedConnectionsAreNotNotified(t *testing.T) {
	fake, id := fixture(t)
	rec := &recorder{}
	if _, err := fake.RevokeConnection(t.Context(), id, creatorOf(t, fake, id), time.Now().UTC()); err != nil {
		t.Fatalf("RevokeConnection: %v", err)
	}
	if n, err := newSweeper(fake, rec).Sweep(t.Context()); err != nil || n != 0 {
		t.Fatalf("Sweep = %d, %v; want 0", n, err)
	}
	if len(rec.messages()) != 0 {
		t.Errorf("revoked connection produced mail: %v", rec.messages())
	}
}

// A mailer that cannot take the message leaves the row unstamped, so the
// notification is retried instead of lost.
func TestUnqueueableMailIsRetriedNextSweep(t *testing.T) {
	fake, id := fixture(t)
	fake.ExpireConnection(id)
	rec := &recorder{err: errors.New("queue full")}
	s := newSweeper(fake, rec)

	if n, err := s.Sweep(t.Context()); err != nil || n != 0 {
		t.Fatalf("Sweep = %d, %v; want 0 queued", n, err)
	}
	rec.mu.Lock()
	rec.err = nil
	rec.mu.Unlock()

	if n, err := s.Sweep(t.Context()); err != nil || n != 1 {
		t.Fatalf("retry sweep = %d, %v; want 1", n, err)
	}
}

// Reconnecting and expiring again is a new event, so it is notified
// again.
func TestReconnectResetsTheNotification(t *testing.T) {
	fake, id := fixture(t)
	fake.ExpireConnection(id)
	rec := &recorder{}
	s := newSweeper(fake, rec)
	if _, err := s.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, err := fake.UpsertConnection(t.Context(), &repo.Connection{
		CreatorID: creatorOf(t, fake, id), Platform: "twitch",
		ProviderUserID: "123", DisplayName: "somechannel", AccessToken: "at2",
	}); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	fake.ExpireConnection(id)

	if n, err := s.Sweep(t.Context()); err != nil || n != 1 {
		t.Fatalf("post-reconnect sweep = %d, %v; want 1", n, err)
	}
	if len(rec.messages()) != 2 {
		t.Errorf("messages = %v, want one per expiry event", rec.messages())
	}
}

func TestRunStopsWithContext(t *testing.T) {
	fake, id := fixture(t)
	fake.ExpireConnection(id)
	rec := &recorder{}
	s := newSweeper(fake, rec)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for len(rec.messages()) == 0 {
		select {
		case <-deadline:
			t.Fatal("Run never swept")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run ignored context cancellation")
	}
}

// creatorOf reads the owning creator id back out of the fake.
func creatorOf(t *testing.T, fake *repo.Fake, connectionID string) string {
	t.Helper()
	pending, err := fake.PendingExpiryNotifications(t.Context(), 100)
	if err != nil {
		t.Fatalf("PendingExpiryNotifications: %v", err)
	}
	for _, p := range pending {
		if p.ID == connectionID {
			return p.CreatorID
		}
	}
	// Not expired (or already notified): fall back to the creator the
	// fixture created.
	c, err := fake.CreatorByEmail(t.Context(), "creator@example.test")
	if err != nil {
		t.Fatalf("CreatorByEmail: %v", err)
	}
	return c.ID
}
