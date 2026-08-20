// Package connsource tells the adapter which connections this instance is
// responsible for watching.
//
// A13 (multi-instance sharding) lands in internal/shard, exactly where
// this comment always said it would: shard.Filter wraps any Source here
// and yields only the connections this instance's ring segment owns, so
// the ingest manager still sees a Source and never learns the difference.
// It is off by default (ADAPTER_SHARDING_ENABLED); with it off the
// sources below behave as they always have.
package connsource

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"dabet/services/provider-adapter/internal/driver"
)

// Source lists the active connections assigned to this instance and
// signals when the assignment changes.
type Source interface {
	// List returns the connections this instance must watch right now.
	List(ctx context.Context) ([]driver.Connection, error)
	// Lookup finds the active connection for a creator on a platform, used
	// by the deletion consumer to pick credentials for a delete call.
	Lookup(creatorID, platform string) (driver.Connection, bool)
	// Changes yields a signal whenever the assignment may have changed;
	// consumers re-List on each signal. The channel never closes.
	Changes() <-chan struct{}
}

// Static is an in-memory, single-instance Source: seeded from the
// environment at startup, mutable at runtime (the mock driver's injection
// endpoint registers connections through it).
type Static struct {
	mu      sync.RWMutex
	conns   map[string]driver.Connection // by connection ID
	changes chan struct{}
}

// NewStatic returns a Static seeded with conns.
func NewStatic(conns ...driver.Connection) *Static {
	s := &Static{
		conns:   make(map[string]driver.Connection, len(conns)),
		changes: make(chan struct{}, 1),
	}
	for _, c := range conns {
		s.conns[c.ID] = c
	}
	return s
}

// ParseEnv parses the ADAPTER_CONNECTIONS format: a comma-separated list
// of platform:connection_id:creator_id[:native_user_id] entries, e.g.
// "mock:conn-1:9d4e...,twitch:conn-2:9d4e...:44322889". Tokens are not
// carried in env; the connections phase loads them from Area A.
func ParseEnv(v string) ([]driver.Connection, error) {
	var out []driver.Connection
	for _, entry := range strings.Split(v, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) < 3 || len(parts) > 4 {
			return nil, fmt.Errorf("connsource: bad connection entry %q (want platform:connection_id:creator_id[:native_user_id])", entry)
		}
		c := driver.Connection{Platform: parts[0], ID: parts[1], CreatorID: parts[2]}
		if len(parts) == 4 {
			c.NativeUserID = parts[3]
		}
		if c.Platform == "" || c.ID == "" || c.CreatorID == "" {
			return nil, fmt.Errorf("connsource: empty field in connection entry %q", entry)
		}
		out = append(out, c)
	}
	return out, nil
}

// List implements Source.
func (s *Static) List(context.Context) ([]driver.Connection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]driver.Connection, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out, nil
}

// Lookup implements Source.
func (s *Static) Lookup(creatorID, platform string) (driver.Connection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.conns {
		if c.CreatorID == creatorID && c.Platform == platform {
			return c, true
		}
	}
	return driver.Connection{}, false
}

// Changes implements Source.
func (s *Static) Changes() <-chan struct{} { return s.changes }

// Add registers or replaces a connection and signals the change.
func (s *Static) Add(c driver.Connection) {
	s.mu.Lock()
	s.conns[c.ID] = c
	s.mu.Unlock()
	s.signal()
}

// Remove drops a connection and signals the change.
func (s *Static) Remove(connectionID string) {
	s.mu.Lock()
	delete(s.conns, connectionID)
	s.mu.Unlock()
	s.signal()
}

// Get returns a connection by id.
func (s *Static) Get(connectionID string) (driver.Connection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.conns[connectionID]
	return c, ok
}

func (s *Static) signal() {
	select {
	case s.changes <- struct{}{}:
	default: // a signal is already pending; List is a full snapshot
	}
}
