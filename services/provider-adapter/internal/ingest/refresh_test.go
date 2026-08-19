package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/contracts"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/opaque"
)

// scriptedDriver returns a scripted error from each Watch call, optionally
// emitting a message first. It stands in for a real driver whose stream
// dies of an auth failure mid-flight.
type scriptedDriver struct {
	platform string

	mu    sync.Mutex
	calls []driver.Connection
	// errs is returned in order; the last entry repeats.
	errs []error
	// emit, when set, is sent before the error on every call.
	emit *driver.Message
	// rawSend writes emit straight to the channel instead of going through
	// driver.Send, which is how a driver that never stamps behaves.
	rawSend bool
}

func (d *scriptedDriver) Platform() string { return d.platform }

func (d *scriptedDriver) Watch(ctx context.Context, conn driver.Connection, out chan<- driver.Message) error {
	d.mu.Lock()
	d.calls = append(d.calls, conn)
	n := len(d.calls)
	err := error(nil)
	if len(d.errs) > 0 {
		err = d.errs[min(n-1, len(d.errs)-1)]
	}
	emit := d.emit
	d.mu.Unlock()

	if emit != nil {
		if d.rawSend {
			select {
			case <-ctx.Done():
				return nil
			case out <- *emit:
			}
		} else if serr := driver.Send(ctx, out, *emit); serr != nil {
			return nil
		}
	}
	if err == nil {
		<-ctx.Done()
		return nil
	}
	return err
}

func (d *scriptedDriver) Delete(context.Context, driver.Connection, string, string) error { return nil }

func (d *scriptedDriver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, nil
}

func (d *scriptedDriver) watchCalls() []driver.Connection {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]driver.Connection(nil), d.calls...)
}

// fakeRefresher hands back a rotated token, or refuses.
type fakeRefresher struct {
	mu    sync.Mutex
	calls int
	fail  bool
}

func (r *fakeRefresher) Refresh(_ context.Context, conn driver.Connection) (driver.Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.fail {
		return conn, errors.New("refresh: connection is expired")
	}
	conn.AccessToken = fmt.Sprintf("refreshed-%d", r.calls)
	return conn, nil
}

func (r *fakeRefresher) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func newScriptedFixture(t *testing.T, drv *scriptedDriver) (*Manager, *connsource.Static, *fakeProducer) {
	t.Helper()
	reg := driver.NewRegistry()
	reg.Register(drv)
	source := connsource.NewStatic()
	prod := &fakeProducer{}
	m := metrics.New(prometheus.NewRegistry())
	mgr := NewManager(reg, source, prod, opaque.NewMinter(), m, slog.New(slog.DiscardHandler))
	mgr.WatchRetry = 5 * time.Millisecond
	return mgr, source, prod
}

func runManager(t *testing.T, mgr *Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = mgr.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("manager did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func TestAuthFailureTriggersRefreshAndRestartsWithTheNewToken(t *testing.T) {
	drv := &scriptedDriver{
		platform: "mock",
		errs: []error{
			// The stream dies of an expired token, then runs fine.
			fmt.Errorf("stream died: %w", driver.ErrUnauthorized),
			nil,
		},
	}
	mgr, source, _ := newScriptedFixture(t, drv)
	ref := &fakeRefresher{}
	mgr.Refresher = ref
	runManager(t, mgr)

	source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock", AccessToken: "stale"})
	waitFor(t, "watch restarted after refresh", func() bool { return len(drv.watchCalls()) >= 2 })

	if ref.count() != 1 {
		t.Errorf("refresh calls = %d, want exactly 1", ref.count())
	}
	calls := drv.watchCalls()
	if calls[0].AccessToken != "stale" {
		t.Errorf("first watch token = %q", calls[0].AccessToken)
	}
	// The restart must carry the refreshed credential, or the driver just
	// 401s again forever.
	if calls[1].AccessToken != "refreshed-1" {
		t.Errorf("second watch token = %q, want the refreshed one", calls[1].AccessToken)
	}
}

func TestAuthFailureStopsTheWatchWhenRefreshFails(t *testing.T) {
	drv := &scriptedDriver{
		platform: "mock",
		errs:     []error{fmt.Errorf("revoked: %w", driver.ErrUnauthorized)},
	}
	mgr, source, _ := newScriptedFixture(t, drv)
	ref := &fakeRefresher{fail: true}
	mgr.Refresher = ref
	runManager(t, mgr)

	source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock", AccessToken: "revoked"})
	waitFor(t, "refresh attempted", func() bool { return ref.count() >= 1 })

	// §5.6: refresh failed, the connection is expired and its streams are
	// dropped — so no reconnect storm against credentials that are gone.
	time.Sleep(100 * time.Millisecond)
	if got := len(drv.watchCalls()); got != 1 {
		t.Errorf("watch calls = %d, want the watch to stop after a failed refresh", got)
	}
	if got := ref.count(); got != 1 {
		t.Errorf("refresh calls = %d, want exactly 1", got)
	}
}

func TestTerminalDriverErrorStopsTheWatch(t *testing.T) {
	drv := &scriptedDriver{
		platform: "mock",
		errs:     []error{fmt.Errorf("channel deleted: %w", driver.ErrGone)},
	}
	mgr, source, _ := newScriptedFixture(t, drv)
	runManager(t, mgr)

	source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock"})
	waitFor(t, "watch attempted", func() bool { return len(drv.watchCalls()) >= 1 })

	// Retrying a deleted channel is a guaranteed-failure hot loop (P2).
	time.Sleep(100 * time.Millisecond)
	if got := len(drv.watchCalls()); got != 1 {
		t.Errorf("watch calls = %d, want the watch to stop on a terminal error", got)
	}
}

func TestIngestedAtUsesTheDriversReceiptStamp(t *testing.T) {
	received := time.Date(2026, 8, 19, 14, 2, 11, 412_000_000, time.UTC)
	drv := &scriptedDriver{
		platform: "mock",
		emit: &driver.Message{
			NativeChannelID: "chan-1",
			NativeAuthorID:  "author-1",
			NativeMessageID: "msg-1",
			Text:            "hello",
			ReceivedAt:      received,
		},
	}
	mgr, source, prod := newScriptedFixture(t, drv)
	// The loop's own clock is deliberately far away, so a driver stamp
	// being ignored would be obvious.
	mgr.Now = func() time.Time { return received.Add(time.Hour) }
	runManager(t, mgr)

	source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock"})
	waitFor(t, "message produced", func() bool { return len(prod.snapshot()) >= 1 })

	var msg contracts.Message
	if err := json.Unmarshal(prod.snapshot()[0].value, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The §4.6 clock must start where the adapter took delivery, not where
	// it got round to producing.
	if !msg.IngestedAt.Equal(received) {
		t.Errorf("ingested_at = %s, want the driver's receipt stamp %s", msg.IngestedAt, received)
	}
}

func TestIngestedAtFallsBackToTheLoopClock(t *testing.T) {
	now := time.Date(2026, 8, 19, 15, 0, 0, 0, time.UTC)
	drv := &scriptedDriver{
		platform: "mock",
		emit: &driver.Message{
			NativeChannelID: "chan-1",
			NativeAuthorID:  "author-1",
			NativeMessageID: "msg-1",
			Text:            "hello",
			// No ReceivedAt: a driver that did not stamp.
		},
		rawSend: true,
	}
	mgr, source, prod := newScriptedFixture(t, drv)
	mgr.Now = func() time.Time { return now }
	runManager(t, mgr)

	source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock"})
	waitFor(t, "message produced", func() bool { return len(prod.snapshot()) >= 1 })

	var msg contracts.Message
	if err := json.Unmarshal(prod.snapshot()[0].value, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !msg.IngestedAt.Equal(now) {
		t.Errorf("ingested_at = %s, want the loop clock %s", msg.IngestedAt, now)
	}
}
