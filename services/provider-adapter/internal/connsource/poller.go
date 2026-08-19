package connsource

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"time"

	"dabet/services/provider-adapter/internal/driver"
)

// Lister enumerates the active connections this instance must watch.
// Implemented by connpg.Store over identity.connections; faked in tests.
type Lister interface {
	ActiveConnections(ctx context.Context) ([]driver.Connection, error)
}

// Poller is the Area-A-backed Source: it polls a Lister (the
// identity.connections table) on an interval, keeps a snapshot, and
// signals Changes when the snapshot differs — new connections start watch
// loops, revoked/expired ones are dropped. Polling over LISTEN/NOTIFY is
// a deliberate simplicity trade; the interval is env-tunable
// (ADAPTER_CONNSOURCE_POLL).
type Poller struct {
	lister   Lister
	interval time.Duration
	log      *slog.Logger

	mu      sync.RWMutex
	conns   map[string]driver.Connection
	loaded  bool
	changes chan struct{}
}

var _ Source = (*Poller)(nil)

// NewPoller wires a Poller; call Run to start the loop.
func NewPoller(l Lister, interval time.Duration, log *slog.Logger) *Poller {
	return &Poller{
		lister:   l,
		interval: interval,
		log:      log,
		conns:    make(map[string]driver.Connection),
		changes:  make(chan struct{}, 1),
	}
}

// Run polls until ctx ends. Poll failures keep the last good snapshot —
// a database blip must not tear down every watch loop (P2).
func (p *Poller) Run(ctx context.Context) error {
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	for {
		if err := p.Load(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("connection poll failed; keeping last snapshot", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// Load performs one poll, replacing the snapshot and signalling on change.
func (p *Poller) Load(ctx context.Context) error {
	list, err := p.lister.ActiveConnections(ctx)
	if err != nil {
		return err
	}
	next := make(map[string]driver.Connection, len(list))
	for _, c := range list {
		next[c.ID] = c
	}

	p.mu.Lock()
	changed := !p.loaded || !maps.Equal(p.conns, next)
	p.conns = next
	p.loaded = true
	p.mu.Unlock()

	if changed {
		p.signal()
	}
	return nil
}

// List implements Source. Before the first successful poll it loads
// synchronously so startup does not race the ticker.
func (p *Poller) List(ctx context.Context) ([]driver.Connection, error) {
	p.mu.RLock()
	loaded := p.loaded
	p.mu.RUnlock()
	if !loaded {
		if err := p.Load(ctx); err != nil {
			return nil, err
		}
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]driver.Connection, 0, len(p.conns))
	for _, c := range p.conns {
		out = append(out, c)
	}
	return out, nil
}

// Lookup implements Source.
func (p *Poller) Lookup(creatorID, platform string) (driver.Connection, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, c := range p.conns {
		if c.CreatorID == creatorID && c.Platform == platform {
			return c, true
		}
	}
	return driver.Connection{}, false
}

// Changes implements Source.
func (p *Poller) Changes() <-chan struct{} { return p.changes }

// Evict drops a connection from the snapshot immediately (used when a
// refresh marks it expired — §5.6: drop its streams) rather than waiting
// out the poll interval.
func (p *Poller) Evict(connectionID string) {
	p.mu.Lock()
	_, ok := p.conns[connectionID]
	if ok {
		delete(p.conns, connectionID)
	}
	p.mu.Unlock()
	if ok {
		p.signal()
	}
}

// Update replaces one connection in the snapshot (fresh access token
// after a refresh) without waiting for the next poll. No signal: the
// connection set did not change, only its credentials.
func (p *Poller) Update(c driver.Connection) {
	p.mu.Lock()
	if _, ok := p.conns[c.ID]; ok {
		p.conns[c.ID] = c
	}
	p.mu.Unlock()
}

func (p *Poller) signal() {
	select {
	case p.changes <- struct{}{}:
	default: // a signal is already pending; List is a full snapshot
	}
}
