package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/mockdriver"
	"dabet/services/provider-adapter/internal/opaque"
)

type produced struct {
	topic string
	key   []byte
	value []byte
}

type fakeProducer struct {
	mu   sync.Mutex
	recs []produced
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, produced{topic: topic, key: key, value: value})
	return nil
}

func (f *fakeProducer) snapshot() []produced {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]produced, len(f.recs))
	copy(out, f.recs)
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type fixture struct {
	manager *Manager
	mock    *mockdriver.Driver
	source  *connsource.Static
	prod    *fakeProducer
	metrics *metrics.Metrics
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	mock := mockdriver.New(nil)
	reg := driver.NewRegistry()
	reg.Register(mock)
	source := connsource.NewStatic()
	prod := &fakeProducer{}
	m := metrics.New(prometheus.NewRegistry())
	mgr := NewManager(reg, source, prod, opaque.NewMinter(), m, slog.New(slog.DiscardHandler))
	now := time.Date(2026, 8, 19, 14, 2, 11, 412_000_000, time.UTC)
	mgr.Now = func() time.Time { return now }
	return &fixture{manager: mgr, mock: mock, source: source, prod: prod, metrics: m, now: now}
}

func (f *fixture) run(t *testing.T) (cancel func(), done chan struct{}) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		defer close(done)
		_ = f.manager.Run(ctx)
	}()
	return stop, done
}

func TestInjectedMessageIsProducedWithCorrectFieldsAndKey(t *testing.T) {
	f := newFixture(t)
	cancel, done := f.run(t)
	defer func() { cancel(); <-done }()

	f.source.Add(driver.Connection{ID: "conn-1", CreatorID: "creator-9d4e", Platform: "mock"})
	waitFor(t, "watch loop start", func() bool {
		return testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")) == 1
	})
	if err := f.mock.Inject("conn-1", driver.Message{
		NativeChannelID: "channel-A",
		NativeAuthorID:  "author-B",
		NativeMessageID: "native-1",
		Text:            "hello chat",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "message produced", func() bool { return len(f.prod.snapshot()) == 1 })

	rec := f.prod.snapshot()[0]
	if rec.topic != contracts.TopicMessages {
		t.Errorf("topic = %q, want %q", rec.topic, contracts.TopicMessages)
	}
	var msg contracts.Message
	if err := json.Unmarshal(rec.value, &msg); err != nil {
		t.Fatal(err)
	}
	wantContent, _ := opaque.MintContentID("mock", "channel-A")
	wantAuthor, _ := opaque.MintAuthorID("mock", "author-B")
	wantMessage, _, _ := opaque.MintMessageID("mock", "native-1")
	if msg.ContentID != wantContent || msg.AuthorID != wantAuthor || msg.MessageID != wantMessage {
		t.Errorf("ids = (%q,%q,%q), want (%q,%q,%q)",
			msg.MessageID, msg.ContentID, msg.AuthorID, wantMessage, wantContent, wantAuthor)
	}
	if msg.CreatorID != "creator-9d4e" {
		t.Errorf("creator_id = %q, want the connection's creator", msg.CreatorID)
	}
	if msg.Text != "hello chat" {
		t.Errorf("text = %q, want %q", msg.Text, "hello chat")
	}
	if !msg.IngestedAt.Equal(f.now) {
		t.Errorf("ingested_at = %v, want %v", msg.IngestedAt, f.now)
	}
	if want := contracts.MessagesKey(wantAuthor, wantContent); !bytes.Equal(rec.key, want) {
		t.Errorf("key = %q, want MessagesKey(author, content) = %q", rec.key, want)
	}
	if got := testutil.ToFloat64(f.metrics.IngestTotal.WithLabelValues("mock")); got != 1 {
		t.Errorf("adapter_ingest_total = %v, want 1", got)
	}
}

func TestRemovedConnectionStopsItsWatchLoop(t *testing.T) {
	f := newFixture(t)
	cancel, done := f.run(t)
	defer func() { cancel(); <-done }()

	f.source.Add(driver.Connection{ID: "conn-1", CreatorID: "c", Platform: "mock"})
	waitFor(t, "gauge up", func() bool {
		return testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")) == 1
	})
	f.source.Remove("conn-1")
	waitFor(t, "gauge down", func() bool {
		return testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")) == 0
	})
}

func TestCancellationStopsAllWatchLoopsCleanly(t *testing.T) {
	f := newFixture(t)
	cancel, done := f.run(t)

	for _, id := range []string{"conn-1", "conn-2", "conn-3"} {
		f.source.Add(driver.Connection{ID: id, CreatorID: "c-" + id, Platform: "mock"})
	}
	waitFor(t, "all loops running", func() bool {
		return testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")) == 3
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop after cancellation")
	}
	if got := testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")); got != 0 {
		t.Errorf("adapter_connections_active = %v after shutdown, want 0", got)
	}
}

func TestUnknownPlatformConnectionIsSkipped(t *testing.T) {
	f := newFixture(t)
	cancel, done := f.run(t)
	defer func() { cancel(); <-done }()

	f.source.Add(driver.Connection{ID: "conn-x", CreatorID: "c", Platform: "myspace"})
	f.source.Add(driver.Connection{ID: "conn-1", CreatorID: "c", Platform: "mock"})
	waitFor(t, "mock loop running", func() bool {
		return testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("mock")) == 1
	})
	if got := testutil.ToFloat64(f.metrics.ConnectionsActive.WithLabelValues("myspace")); got != 0 {
		t.Errorf("driverless platform has %v active connections, want 0", got)
	}
}
