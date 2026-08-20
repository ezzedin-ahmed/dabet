package shard

import (
	"context"
	"slices"
	"sync"
)

// fakeCoordinator drives membership from the test rather than from a
// broker, so every sharding behaviour below is exercised with no Docker,
// no network, and no timing.
//
// It mirrors KafkaCoordinator's contract exactly where it matters: the
// member list is never cleared by Disconnect, only by an explicit
// SetMembership, because "hold the last known view" is the failure stance
// under test.
type fakeCoordinator struct {
	changes chan struct{}

	mu        sync.RWMutex
	self      string
	members   []string
	epoch     uint64
	connected bool

	// runs counts Run invocations that are still in flight, so tests can
	// assert the coordinator goroutine actually exits.
	runs sync.WaitGroup
}

var _ Coordinator = (*fakeCoordinator)(nil)

func newFakeCoordinator(self string, members ...string) *fakeCoordinator {
	c := &fakeCoordinator{changes: make(chan struct{}, 1), self: self}
	if len(members) > 0 {
		c.members = slices.Clone(members)
		slices.Sort(c.members)
		c.epoch = 1
		c.connected = true
	}
	return c
}

func (c *fakeCoordinator) Run(ctx context.Context) error {
	c.runs.Add(1)
	defer c.runs.Done()
	<-ctx.Done()
	return nil
}

func (c *fakeCoordinator) Membership() Membership {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Membership{
		Self:      c.self,
		Members:   slices.Clone(c.members),
		Epoch:     c.epoch,
		Connected: c.connected,
	}
}

func (c *fakeCoordinator) Changes() <-chan struct{} { return c.changes }

func (c *fakeCoordinator) Close() {}

// SetMembership publishes a new view and signals, as a successful sync
// would.
func (c *fakeCoordinator) SetMembership(members ...string) {
	sorted := slices.Clone(members)
	slices.Sort(sorted)
	c.mu.Lock()
	c.members = sorted
	c.epoch++
	c.connected = true
	c.mu.Unlock()
	c.signal()
}

// Disconnect marks the coordinator unreachable while keeping the view, the
// way KafkaCoordinator behaves on a broker outage. It signals so a
// consumer that re-Lists on the signal is proven to compute the *same*
// assignment rather than merely never being asked.
func (c *fakeCoordinator) Disconnect() {
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
	c.signal()
}

func (c *fakeCoordinator) signal() {
	select {
	case c.changes <- struct{}{}:
	default:
	}
}
