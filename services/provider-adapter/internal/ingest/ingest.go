// Package ingest runs one watch loop per assigned connection and turns
// platform-native chat messages into messages.v1 events (docs §7.2): the
// driver reports native messages, the loop mints the opaque IDs, resolves
// creator_id from the connection, stamps ingested_at at receipt (§4.6),
// and produces keyed by hash(author_id, content_id) (§4.2).
//
// Backpressure is structural: each connection's driver writes into a
// bounded channel that the loop drains with synchronous produces, so a
// slow broker slows the driver instead of growing memory.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"dabet/pkg/contracts"
	"dabet/pkg/tracing"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/opaque"
)

// Producer is the slice of kafkax.Producer the ingest path needs; tests
// substitute a capture fake.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
}

// Manager reconciles running watch loops against the connection source.
type Manager struct {
	registry *driver.Registry
	source   connsource.Source
	producer Producer
	minter   *opaque.Minter
	metrics  *metrics.Metrics
	log      *slog.Logger

	// Now stamps ingested_at; injectable for tests.
	Now func() time.Time
	// Buffer is the per-connection channel capacity.
	Buffer int
	// WatchRetry is how long to wait before restarting a failed Watch.
	WatchRetry time.Duration
}

// NewManager wires a Manager with defaults.
func NewManager(reg *driver.Registry, src connsource.Source, prod Producer, minter *opaque.Minter, m *metrics.Metrics, log *slog.Logger) *Manager {
	return &Manager{
		registry:   reg,
		source:     src,
		producer:   prod,
		minter:     minter,
		metrics:    m,
		log:        log,
		Now:        time.Now,
		Buffer:     256,
		WatchRetry: 2 * time.Second,
	}
}

type watcher struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Run reconciles until ctx is cancelled, then stops every watch loop and
// waits for them to drain.
func (m *Manager) Run(ctx context.Context) error {
	running := make(map[string]*watcher)
	defer func() {
		for _, w := range running {
			w.cancel()
		}
		for _, w := range running {
			<-w.done
		}
	}()

	for {
		if err := m.reconcile(ctx, running); err != nil && ctx.Err() == nil {
			m.log.Error("connection list failed", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-m.source.Changes():
		}
	}
}

func (m *Manager) reconcile(ctx context.Context, running map[string]*watcher) error {
	conns, err := m.source.List(ctx)
	if err != nil {
		return err
	}
	want := make(map[string]driver.Connection, len(conns))
	for _, c := range conns {
		want[c.ID] = c
	}
	for id, w := range running {
		if _, ok := want[id]; !ok {
			w.cancel()
			<-w.done
			delete(running, id)
		}
	}
	for id, conn := range want {
		if _, ok := running[id]; ok {
			continue
		}
		drv, ok := m.registry.Get(conn.Platform)
		if !ok {
			m.log.Error("no driver for platform", "platform", conn.Platform, "connection_id", conn.ID)
			continue
		}
		wctx, cancel := context.WithCancel(ctx)
		w := &watcher{cancel: cancel, done: make(chan struct{})}
		running[id] = w
		go func(conn driver.Connection) {
			defer close(w.done)
			m.watch(wctx, drv, conn)
		}(conn)
	}
	return nil
}

// watch runs one connection's Watch loop until ctx is cancelled,
// restarting after WatchRetry on driver failure.
func (m *Manager) watch(ctx context.Context, drv driver.Driver, conn driver.Connection) {
	m.metrics.ConnectionsActive.WithLabelValues(conn.Platform).Inc()
	defer m.metrics.ConnectionsActive.WithLabelValues(conn.Platform).Dec()

	for ctx.Err() == nil {
		out := make(chan driver.Message, m.Buffer)
		werr := make(chan error, 1)
		go func() {
			werr <- drv.Watch(ctx, conn, out)
			close(out)
		}()
		for msg := range out {
			if err := m.emit(ctx, conn, msg); err != nil && ctx.Err() == nil {
				// P4: log identifiers only, never message text.
				m.log.Error("produce failed", "connection_id", conn.ID, "platform", conn.Platform, "error", err.Error())
			}
		}
		err := <-werr
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, driver.ErrNotImplemented) {
			m.log.Warn("watch not implemented; connection idle", "platform", conn.Platform, "connection_id", conn.ID)
			return
		}
		if err == nil {
			m.log.Info("watch ended; restarting", "platform", conn.Platform, "connection_id", conn.ID)
		} else {
			m.log.Error("watch failed; restarting", "platform", conn.Platform, "connection_id", conn.ID, "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.WatchRetry):
		}
	}
}

// emit mints opaque IDs, builds the messages.v1 event, and produces it.
//
// This span is the root of a message's trace: the clock the §4.6 SLI
// measures starts here, at adapter ingress. Everything downstream —
// messages.v1, the cascade, the verdict on flagged.v1, the delete call —
// hangs off it via the record headers.
//
// P4: message_id, content_id, creator_id and the platform go on the span;
// msg.Text does not, and neither does author_id (see pkg/tracing).
func (m *Manager) emit(ctx context.Context, conn driver.Connection, msg driver.Message) error {
	ctx, span := tracing.Tracer().Start(ctx, "adapter.ingest",
		trace.WithAttributes(
			tracing.Platform(conn.Platform),
			tracing.CreatorID(conn.CreatorID),
		))
	defer span.End()

	contentID, err := m.minter.ContentID(conn.Platform, msg.NativeChannelID)
	if err != nil {
		return err
	}
	authorID, err := m.minter.AuthorID(conn.Platform, msg.NativeAuthorID)
	if err != nil {
		return err
	}
	messageID, err := m.minter.MessageID(conn.Platform, msg.NativeMessageID)
	if err != nil {
		return err
	}
	event := contracts.Message{
		MessageID:  messageID,
		ContentID:  contentID,
		AuthorID:   authorID,
		CreatorID:  conn.CreatorID,
		Text:       msg.Text,
		IngestedAt: m.Now().UTC(),
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	span.SetAttributes(tracing.MessageID(messageID), tracing.ContentID(contentID))

	key := contracts.MessagesKey(authorID, contentID)
	if err := m.producer.Produce(ctx, contracts.TopicMessages, key, value); err != nil {
		return err
	}
	m.metrics.IngestTotal.WithLabelValues(conn.Platform).Inc()
	return nil
}
