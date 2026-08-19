package connsource

import (
	"context"

	"dabet/services/provider-adapter/internal/driver"
)

// Multi unions several Sources: the Postgres poller for real connections
// plus the Static source the mock driver's injection endpoint registers
// into. Earlier sources win on ID and Lookup collisions.
type Multi struct {
	sources []Source
	changes chan struct{}
}

var _ Source = (*Multi)(nil)

// NewMulti wraps sources; call Forward to fan their change signals in.
func NewMulti(sources ...Source) *Multi {
	return &Multi{sources: sources, changes: make(chan struct{}, 1)}
}

// Forward relays every child's change signal onto the merged channel
// until ctx ends. Run it as a goroutine.
func (m *Multi) Forward(ctx context.Context) {
	for _, s := range m.sources {
		go func(ch <-chan struct{}) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
					select {
					case m.changes <- struct{}{}:
					default:
					}
				}
			}
		}(s.Changes())
	}
}

// List implements Source: the union of all children, first source wins
// on duplicate connection IDs.
func (m *Multi) List(ctx context.Context) ([]driver.Connection, error) {
	seen := make(map[string]bool)
	var out []driver.Connection
	for _, s := range m.sources {
		list, err := s.List(ctx)
		if err != nil {
			return nil, err
		}
		for _, c := range list {
			if !seen[c.ID] {
				seen[c.ID] = true
				out = append(out, c)
			}
		}
	}
	return out, nil
}

// Lookup implements Source: first child with a hit wins.
func (m *Multi) Lookup(creatorID, platform string) (driver.Connection, bool) {
	for _, s := range m.sources {
		if c, ok := s.Lookup(creatorID, platform); ok {
			return c, true
		}
	}
	return driver.Connection{}, false
}

// Changes implements Source.
func (m *Multi) Changes() <-chan struct{} { return m.changes }
