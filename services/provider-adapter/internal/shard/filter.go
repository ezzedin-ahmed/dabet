package shard

import (
	"context"
	"log/slog"
	"slices"
	"sync"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
)

// Filter is a connsource.Source that wraps another Source and yields only
// the connections this instance's ring segment owns.
//
// It sits *inside* the Source chain rather than beside it, which is what
// keeps A13 invisible to everything else: the ingest manager still calls
// List and waits on Changes, and its existing reconcile loop — stop what
// disappeared, start what appeared, leave the rest alone — is already
// exactly the rebalance semantics §7.2 asks for. A connection retained
// across a rebalance is never stopped and never restarted, so its socket
// survives. No code in ingest changes.
//
// Changes fires on both inputs: the wrapped Source (a connection was
// created or revoked) and the Coordinator (an instance joined or left).
// Both mean "re-List"; the manager cannot tell them apart and does not
// need to.
type Filter struct {
	inner connsource.Source
	coord Coordinator

	replicas int
	max      int
	m        *metrics.Metrics
	log      *slog.Logger

	changes chan struct{}

	mu sync.Mutex
	// ring is cached and rebuilt only when the member list actually
	// changes; List runs on every connection-set change too, and
	// rebuilding replicas×members SHA-256 points each time would be pure
	// waste.
	ring          *Ring
	ringMembers   []string
	warnedEmpty   bool
	warnedEvicted bool
	refused       map[string]bool
}

var _ connsource.Source = (*Filter)(nil)

// NewFilter wraps inner with the segment owned by coord's view of this
// instance. replicas <= 0 uses DefaultReplicas; max <= 0 disables the cap.
func NewFilter(inner connsource.Source, coord Coordinator, replicas, max int, m *metrics.Metrics, log *slog.Logger) *Filter {
	return &Filter{
		inner:    inner,
		coord:    coord,
		replicas: replicas,
		max:      max,
		m:        m,
		log:      log,
		changes:  make(chan struct{}, 1),
		ring:     NewRing(replicas, nil),
		refused:  make(map[string]bool),
	}
}

// Forward relays the wrapped Source's and the Coordinator's change
// signals onto this Filter's channel until ctx ends. Run it as a
// goroutine, like connsource.Multi.Forward.
func (f *Filter) Forward(ctx context.Context) {
	for _, ch := range []<-chan struct{}{f.inner.Changes(), f.coord.Changes()} {
		go func(ch <-chan struct{}) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
					f.signal()
				}
			}
		}(ch)
	}
}

// List implements connsource.Source: the wrapped Source's connections,
// narrowed to this instance's ring segment and truncated at the cap.
func (f *Filter) List(ctx context.Context) ([]driver.Connection, error) {
	all, err := f.inner.List(ctx)
	if err != nil {
		return nil, err
	}
	return f.Assignment(all).Owned, nil
}

// Assignment computes this instance's share of conns and updates the
// shard metrics. It is exported for tests and for callers that need to
// see the refused set; List is the Source-facing path.
func (f *Filter) Assignment(conns []driver.Connection) Assignment {
	view := f.coord.Membership()

	f.mu.Lock()
	defer f.mu.Unlock()

	// A view that excludes this instance means the fleet has declared it
	// dead and handed its segment elsewhere. Adopting it would drop every
	// stream here at once, so an instance that already has a working
	// assignment keeps serving it and waits to be re-admitted — the same
	// hold-the-last-view stance as a coordinator outage, for the same
	// reason. KafkaCoordinator refuses such a view outright; this guard is
	// here because Coordinator is an interface and an etcd implementation
	// watching a prefix could legitimately observe its own lease expire.
	if len(view.Members) > 0 && !slices.Contains(view.Members, view.Self) && f.ring.Has(view.Self) {
		if !f.warnedEvicted {
			f.warnedEvicted = true
			f.log.Warn("shard membership view excludes this instance; holding last assignment",
				"self", view.Self, "members", len(view.Members), "epoch", view.Epoch)
		}
		view.Members = f.ringMembers
	} else if len(view.Members) > 0 {
		f.warnedEvicted = false
	}

	if !slices.Equal(view.Members, f.ringMembers) {
		// A coordinator that has gone away holds its last view rather than
		// clearing it (see the package comment), so this branch is not
		// reached on a coordinator outage and the assignment does not move.
		f.ring = NewRing(f.replicas, view.Members)
		f.ringMembers = slices.Clone(view.Members)
		f.m.ShardRebalancesTotal.Inc()
		f.log.Info("shard membership changed",
			"self", view.Self, "members", len(view.Members),
			"epoch", view.Epoch, "connected", view.Connected)
	}

	a := Assign(f.ring, view.Self, conns, f.max)
	a.Epoch = view.Epoch

	f.m.ShardMembers.Set(float64(len(view.Members)))
	f.m.ShardConnectionsOwned.Set(float64(len(a.Owned)))
	f.m.ShardConnectionsRefused.Set(float64(len(a.Refused)))

	// Count each connection once per time it becomes refused, so the
	// counter's rate reads as "connections we started dropping" instead of
	// re-counting a standing overflow on every poll.
	next := make(map[string]bool, len(a.Refused))
	var fresh int
	for _, c := range a.Refused {
		next[c.ID] = true
		if !f.refused[c.ID] {
			fresh++
		}
	}
	f.refused = next
	if fresh > 0 {
		f.m.ShardRefusedTotal.Add(float64(fresh))
		// Counts only, never the connection IDs: at a 5 000 cap the
		// overflow can be arbitrarily large and this would be the loudest
		// line in the log during the incident it is reporting.
		f.log.Error("shard capacity exceeded; connections left unwatched",
			"self", view.Self, "cap", f.max, "owned", len(a.Owned),
			"refused", len(a.Refused), "newly_refused", fresh,
			"members", len(view.Members))
	}

	// Once per transition into the state, not once per poll: this is the
	// cold-start-with-an-unreachable-coordinator case and it can persist
	// for as long as Kafka is down.
	if len(view.Members) == 0 && !f.warnedEmpty {
		f.warnedEmpty = true
		f.log.Warn("no shard membership view; watching nothing until the coordinator answers",
			"self", view.Self, "connections", len(conns))
	} else if len(view.Members) > 0 {
		f.warnedEmpty = false
	}
	return a
}

// Lookup implements connsource.Source by delegating to the wrapped Source
// *unfiltered*, on purpose.
//
// Lookup is the deletion consumer's path to a connection's credentials
// (§7.2). deletions.v1 is partitioned by content_id and consumed by an
// ordinary Kafka consumer group, whose partition assignment has nothing to
// do with the shard ring — so an instance routinely receives a deletion
// for a creator whose chat another instance is watching. Narrowing Lookup
// to the local segment would turn those into "no connection found" and
// silently stop deletions for most of the fleet, which is a P2 violation
// dressed up as a sharding detail.
func (f *Filter) Lookup(creatorID, platform string) (driver.Connection, bool) {
	return f.inner.Lookup(creatorID, platform)
}

// Changes implements connsource.Source.
func (f *Filter) Changes() <-chan struct{} { return f.changes }

func (f *Filter) signal() {
	select {
	case f.changes <- struct{}{}:
	default: // a signal is already pending; List is a full snapshot
	}
}
