// Package shard implements A13 — connection sharding for the adapter
// (docs §7.2).
//
// The shape the spec asks for: "each active connection is a work item,
// hashed onto a ring of adapter instances registered in a coordinator
// (etcd or Kafka group membership). An instance watches only the
// connections in its ring segment. Instances joining or leaving trigger a
// rebalance in which only the affected segment's connections reconnect.
// Each instance holds a bounded number of connections (5 000) and
// horizontal scale is the only lever."
//
// Three pieces, deliberately separable:
//
//   - Ring (ring.go) — consistent hashing with virtual nodes, pure and
//     deterministic. Given the same membership view, every instance in the
//     fleet computes the identical connection→instance map with no
//     coordination, which is what makes the assignment safe to derive
//     locally instead of handing out.
//   - Coordinator (this file) — the live membership view. KafkaCoordinator
//     (kafka.go) is the shipped implementation; the interface is four
//     methods so an etcd lease+watch implementation is a drop-in.
//   - Filter (filter.go) — a connsource.Source that wraps another Source
//     and yields only this instance's segment. The ingest manager sees a
//     Source, exactly as it does today, and never learns sharding exists.
//
// # Why Kafka group membership and not etcd
//
// Both are named in §7.2 as acceptable. Kafka wins on every axis that
// matters here:
//
//   - It is already a hard dependency of every service (P1: areas talk
//     only over Kafka), so sharding adds no new failure domain. etcd
//     exists in this stack only as a Milvus dependency behind an opt-in
//     profile — turning it into a moderation-path dependency would put a
//     component nobody runs in production today between chat and
//     ingestion.
//   - A consumer group *is* a membership service: join/leave, session
//     timeouts for liveness, generation fencing, and a leader elected per
//     generation, all implemented by the broker. The equivalent on etcd is
//     a lease, a keepalive loop, a prefix watch, and hand-rolled
//     tie-breaking.
//   - The rebalance callback arrives on exactly the event we care about,
//     with no polling interval to tune.
//
// The one thing a raw consumer group does not give you is the *list* of
// members: Kafka tells each member its own assignment, not who else is in
// the group. Only the group leader sees the full member list, in its
// balance callback. So the balancer in kafka.go does the standard thing
// (this is how Kafka Streams and Connect distribute cluster state): each
// member advertises its stable instance ID in its JoinGroup metadata, and
// the leader echoes the sorted member list back to everyone in the sync
// assignment's user data. Every member therefore ends a generation
// holding the identical membership view, which is the ring's input.
//
// Note what the coordination topic is *not*: it carries no records and is
// never produced to. It exists solely to give the group something to
// subscribe to.
//
// # Failure stance (P2, N2, §4.7)
//
// Losing the coordinator does not stop ingestion. Membership is
// last-known-good and sticky: KafkaCoordinator never clears its member
// list once it has one, so a coordinator outage leaves every instance
// computing the same assignment it was already serving, and every live
// socket stays up. dependency_up{dependency="shard_coordinator"} goes to
// 0 and that is the alert.
//
// The cost of that stance is temporary double-ownership after a network
// partition: an instance the group has evicted keeps watching its
// connections while its replacement also picks them up, so some messages
// are ingested twice. That is the right side to err on. §4.7's table is
// unanimous that a broken dependency means Dabet does *less* moderation,
// never less chat, and duplicate ingestion is not even a degradation
// here — moderation-service's `seen:{message_id}` redelivery guard
// (§7.4) drops the second copy, because message_id is minted
// deterministically from the platform's native ID and is identical from
// both instances. Dropped ingestion has no such backstop: it is silent
// under-moderation, invisible in every metric except a gap in
// adapter_ingest_total that nothing alerts on.
//
// The one case that is *not* fail-open is cold start: an instance that
// has never obtained a membership view owns nothing. Owning everything
// instead would have N instances all opening every platform socket at
// once, which is how a fleet earns a provider-side rate-limit ban —
// failing closed for everyone, and for hours. And the trade costs
// nothing: the coordinator is Kafka, so an instance that cannot reach it
// at startup has nowhere to produce messages.v1 either. Ingestion was
// already down; sharding did not take it down.
package shard

import (
	"context"
	"slices"
	"sort"

	"dabet/services/provider-adapter/internal/driver"
)

// DefaultMaxConnections is the §7.2 per-instance bound. Horizontal scale
// is the only lever: an instance at the cap does not stretch, it refuses,
// and the fleet is expected to grow.
const DefaultMaxConnections = 5000

// Membership is one instance's view of the adapter fleet.
type Membership struct {
	// Self is this instance's stable ID (ADAPTER_INSTANCE_ID, hostname by
	// default). It is normally an element of Members.
	Self string
	// Members is the sorted, de-duplicated set of live instance IDs. It is
	// empty only before the first successful coordinator sync; after that
	// it is held at its last known value even while Connected is false.
	Members []string
	// Epoch increments on every accepted change to Members. It exists so
	// callers can tell "the same view again" from "a new view that happens
	// to have the same size", and so logs can be correlated across the
	// fleet.
	Epoch uint64
	// Connected reports whether the coordinator is currently reachable.
	// False with a non-empty Members means "serving the last known
	// assignment" — see the package comment.
	Connected bool
}

// Coordinator reports which adapter instances are alive.
//
// It is deliberately this small so the §7.2 alternative stays open: an
// etcd implementation is a lease-backed key per instance, a prefix watch
// feeding Changes, and the watch's key set feeding Membership. Nothing
// about the ring, the filter, or the ingest manager would change.
type Coordinator interface {
	// Run maintains the membership view until ctx is cancelled. It returns
	// nil on clean shutdown. Implementations must not return early on a
	// transient coordinator failure — they retry, holding the last view.
	Run(ctx context.Context) error
	// Membership returns the last known view. It never blocks and never
	// fails; an unreachable coordinator is reported through Connected.
	Membership() Membership
	// Changes signals that Membership may have changed. Consumers re-read
	// the whole view on each signal. The channel never closes.
	Changes() <-chan struct{}
	// Close releases the coordinator's resources.
	Close()
}

// Assignment is the connections one instance is responsible for, and the
// ones it had to turn away.
type Assignment struct {
	// Owned are the connections this instance must watch, sorted by ID.
	Owned []driver.Connection
	// Refused are connections this instance owns on the ring but cannot
	// watch because the per-instance cap is full, sorted by ID. Nobody
	// else picks them up: the ring gave them to this instance, so they go
	// unwatched until the fleet grows. That is a capacity alarm
	// (adapter_shard_connections_refused > 0), not a silent drop.
	Refused []driver.Connection
	// Members is the membership view the assignment was computed from.
	Members []string
	// Epoch is the membership epoch it was computed at.
	Epoch uint64
}

// Assign computes one instance's share of conns.
//
// max <= 0 means unbounded. A ring with no members yields an empty
// assignment: nothing is owned by default, which is the cold-start stance
// argued in the package comment.
//
// Refusal at the cap is deterministic and stable: the owned set is sorted
// by connection ID and the first max entries are kept. Stability matters
// more than which connections lose — an unstable rule would rotate the
// victims on every poll, so instead of 500 permanently unwatched streams
// you would get 5 000 streams flapping, each ingesting a fraction of its
// messages. Sorted-prefix means the same connections are refused every
// time until membership or the connection set actually changes, and every
// instance in the fleet would compute the same refusal for the same
// segment.
func Assign(ring *Ring, self string, conns []driver.Connection, max int) Assignment {
	a := Assignment{Members: ring.Members(), Owned: []driver.Connection{}}
	if len(ring.Members()) == 0 || self == "" {
		return a
	}
	for _, c := range conns {
		if ring.Owns(self, c.ID) {
			a.Owned = append(a.Owned, c)
		}
	}
	sort.Slice(a.Owned, func(i, j int) bool { return a.Owned[i].ID < a.Owned[j].ID })
	if max > 0 && len(a.Owned) > max {
		a.Refused = slices.Clone(a.Owned[max:])
		a.Owned = a.Owned[:max]
	}
	return a
}
