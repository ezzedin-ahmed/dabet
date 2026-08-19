package connsource

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"dabet/services/provider-adapter/internal/driver"
)

// fakeLister scripts ActiveConnections results.
type fakeLister struct {
	mu    sync.Mutex
	conns []driver.Connection
	err   error
	calls int
}

func (f *fakeLister) set(conns []driver.Connection, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conns, f.err = conns, err
}

func (f *fakeLister) ActiveConnections(context.Context) ([]driver.Connection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]driver.Connection(nil), f.conns...), nil
}

func signalled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func conn(id, creator, platform, token string) driver.Connection {
	return driver.Connection{ID: id, CreatorID: creator, Platform: platform, AccessToken: token}
}

func TestPollerLoadAndChangeDetection(t *testing.T) {
	ctx := context.Background()
	lister := &fakeLister{}
	lister.set([]driver.Connection{conn("c1", "cr1", "twitch", "t1")}, nil)
	p := NewPoller(lister, time.Minute, slog.New(slog.DiscardHandler))

	// First load always signals: the initial assignment is a change.
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if !signalled(p.Changes()) {
		t.Error("no change signal after first load")
	}
	list, err := p.List(ctx)
	if err != nil || len(list) != 1 || list[0].ID != "c1" {
		t.Fatalf("List = %v, %v", list, err)
	}

	// Identical snapshot: no signal.
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if signalled(p.Changes()) {
		t.Error("change signal for identical snapshot")
	}

	// New connection appears: signal, and List reflects it.
	lister.set([]driver.Connection{
		conn("c1", "cr1", "twitch", "t1"),
		conn("c2", "cr2", "youtube", "t2"),
	}, nil)
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if !signalled(p.Changes()) {
		t.Error("no change signal for new connection")
	}

	// Connection disappears (revoked/expired in Area A): signal.
	lister.set([]driver.Connection{conn("c2", "cr2", "youtube", "t2")}, nil)
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if !signalled(p.Changes()) {
		t.Error("no change signal for removed connection")
	}
	if _, ok := p.Lookup("cr1", "twitch"); ok {
		t.Error("removed connection still resolvable")
	}
	if c, ok := p.Lookup("cr2", "youtube"); !ok || c.AccessToken != "t2" {
		t.Errorf("Lookup = %v, %v", c, ok)
	}
}

func TestPollerListLoadsLazily(t *testing.T) {
	lister := &fakeLister{}
	lister.set([]driver.Connection{conn("c1", "cr1", "twitch", "t1")}, nil)
	p := NewPoller(lister, time.Minute, slog.New(slog.DiscardHandler))

	list, err := p.List(context.Background())
	if err != nil || len(list) != 1 {
		t.Fatalf("List before Run = %v, %v; want the snapshot loaded on demand", list, err)
	}
}

func TestPollerKeepsSnapshotOnError(t *testing.T) {
	ctx := context.Background()
	lister := &fakeLister{}
	lister.set([]driver.Connection{conn("c1", "cr1", "twitch", "t1")}, nil)
	p := NewPoller(lister, time.Minute, slog.New(slog.DiscardHandler))
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	<-p.Changes()

	lister.set(nil, errors.New("db down"))
	if err := p.Load(ctx); err == nil {
		t.Fatal("Load did not surface the lister error")
	}
	// Last good snapshot survives a poll failure (P2).
	if c, ok := p.Lookup("cr1", "twitch"); !ok || c.ID != "c1" {
		t.Errorf("snapshot lost on poll error: %v, %v", c, ok)
	}
}

func TestPollerEvictAndUpdate(t *testing.T) {
	ctx := context.Background()
	lister := &fakeLister{}
	lister.set([]driver.Connection{
		conn("c1", "cr1", "twitch", "t1"),
		conn("c2", "cr2", "twitch", "t2"),
	}, nil)
	p := NewPoller(lister, time.Minute, slog.New(slog.DiscardHandler))
	if err := p.Load(ctx); err != nil {
		t.Fatal(err)
	}
	<-p.Changes()

	// Update swaps credentials in place, no membership signal.
	p.Update(conn("c1", "cr1", "twitch", "t1-fresh"))
	if signalled(p.Changes()) {
		t.Error("Update signalled a membership change")
	}
	if c, _ := p.Lookup("cr1", "twitch"); c.AccessToken != "t1-fresh" {
		t.Errorf("Update lost: %v", c)
	}

	// Evict drops the connection and signals so its streams stop.
	p.Evict("c1")
	if !signalled(p.Changes()) {
		t.Error("Evict did not signal")
	}
	if _, ok := p.Lookup("cr1", "twitch"); ok {
		t.Error("evicted connection still resolvable")
	}
	// Evicting an unknown id is a no-op without a spurious signal.
	p.Evict("nope")
	if signalled(p.Changes()) {
		t.Error("Evict of unknown id signalled")
	}
}

func TestMultiUnionAndPrecedence(t *testing.T) {
	ctx := context.Background()
	lister := &fakeLister{}
	lister.set([]driver.Connection{conn("db1", "cr1", "twitch", "t1")}, nil)
	poller := NewPoller(lister, time.Minute, slog.New(slog.DiscardHandler))
	static := NewStatic(conn("st1", "cr9", "mock", ""))

	m := NewMulti(poller, static)
	list, err := m.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("List = %v, %v; want union of both sources", list, err)
	}
	if c, ok := m.Lookup("cr9", "mock"); !ok || c.ID != "st1" {
		t.Errorf("Lookup static via multi = %v, %v", c, ok)
	}
	if c, ok := m.Lookup("cr1", "twitch"); !ok || c.ID != "db1" {
		t.Errorf("Lookup poller via multi = %v, %v", c, ok)
	}

	// Change signals from either child surface on the merged channel.
	fwdCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	m.Forward(fwdCtx)
	static.Add(conn("st2", "cr8", "mock", ""))
	select {
	case <-m.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("static change did not propagate through Multi")
	}
	lister.set([]driver.Connection{conn("db2", "cr2", "twitch", "t2")}, nil)
	if err := poller.Load(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("poller change did not propagate through Multi")
	}
}
