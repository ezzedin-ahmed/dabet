package shard

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
)

func testConns(n int) []driver.Connection {
	ids := connIDs(n)
	out := make([]driver.Connection, n)
	for i, id := range ids {
		out[i] = driver.Connection{
			ID:        id,
			Platform:  "mock",
			CreatorID: fmt.Sprintf("creator-%d", i),
		}
	}
	return out
}

func newFilterFixture(t *testing.T, self string, maxConns int, conns []driver.Connection, memberList ...string) (*Filter, *connsource.Static, *fakeCoordinator, *metrics.Metrics) {
	t.Helper()
	static := connsource.NewStatic(conns...)
	coord := newFakeCoordinator(self, memberList...)
	m := metrics.New(prometheus.NewRegistry())
	f := NewFilter(static, coord, DefaultReplicas, maxConns, m, slog.New(slog.DiscardHandler))
	return f, static, coord, m
}

func idsOf(conns []driver.Connection) []string {
	out := make([]string, len(conns))
	for i, c := range conns {
		out[i] = c.ID
	}
	slices.Sort(out)
	return out
}

func listIDs(t *testing.T, f *Filter) []string {
	t.Helper()
	got, err := f.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return idsOf(got)
}

// TestFilterPartitionsTheConnectionSet: across the whole fleet every
// connection is owned exactly once. A filter that dropped connections
// would under-moderate silently; one that double-assigned would duplicate
// every stream.
func TestFilterPartitionsTheConnectionSet(t *testing.T) {
	conns := testConns(3000)
	fleet := members(6)

	seen := make(map[string]string)
	for _, self := range fleet {
		f, _, _, _ := newFilterFixture(t, self, 0, conns, fleet...)
		for _, id := range listIDs(t, f) {
			if other, dup := seen[id]; dup {
				t.Fatalf("connection %q owned by both %q and %q", id, other, self)
			}
			seen[id] = self
		}
	}
	if len(seen) != len(conns) {
		t.Fatalf("the fleet covers %d of %d connections", len(seen), len(conns))
	}
}

// TestFilterCapRefusesDeterministically: at the cap the excess is refused,
// counted, and the *same* excess is refused every time. §7.2 makes
// horizontal scale the only lever, so an instance at the bound must not
// quietly overload.
func TestFilterCapRefusesDeterministically(t *testing.T) {
	conns := testConns(2000)
	const limit = 200

	// A single-member ring, so the segment is the whole connection set and
	// the overflow is exactly len(conns)-limit.
	f, _, _, m := newFilterFixture(t, "adapter-0", limit, conns, "adapter-0")
	a := f.Assignment(conns)

	if len(a.Owned) != limit {
		t.Fatalf("owned %d connections with a cap of %d", len(a.Owned), limit)
	}
	if len(a.Refused) != len(conns)-limit {
		t.Fatalf("refused %d connections, want %d", len(a.Refused), len(conns)-limit)
	}
	// Owned and refused must be disjoint and together cover the segment:
	// a connection may be watched or alarmed on, never neither and never
	// both.
	covered := append(idsOf(a.Owned), idsOf(a.Refused)...)
	slices.Sort(covered)
	if !slices.Equal(covered, idsOf(conns)) {
		t.Fatalf("owned+refused covers %d distinct connections, want the whole segment of %d",
			len(slices.Compact(covered)), len(conns))
	}

	if got := testutil.ToFloat64(m.ShardConnectionsOwned); got != limit {
		t.Errorf("adapter_shard_connections_owned = %v, want %d", got, limit)
	}
	if got := testutil.ToFloat64(m.ShardConnectionsRefused); got != float64(len(conns)-limit) {
		t.Errorf("adapter_shard_connections_refused = %v, want %d", got, len(conns)-limit)
	}
	if got := testutil.ToFloat64(m.ShardRefusedTotal); got != float64(len(conns)-limit) {
		t.Errorf("adapter_shard_refused_total = %v, want %d", got, len(conns)-limit)
	}

	// Stability: recomputing must refuse exactly the same connections, and
	// must not re-count them. Churning victims would flap every stream in
	// the overflow instead of leaving a fixed, alarmable set unwatched.
	for range 3 {
		b := f.Assignment(conns)
		if !slices.Equal(idsOf(a.Owned), idsOf(b.Owned)) {
			t.Fatal("the owned set changed between identical inputs")
		}
		if !slices.Equal(idsOf(a.Refused), idsOf(b.Refused)) {
			t.Fatal("the refused set changed between identical inputs")
		}
	}
	if got := testutil.ToFloat64(m.ShardRefusedTotal); got != float64(len(conns)-limit) {
		t.Errorf("adapter_shard_refused_total = %v after re-computation; a standing overflow must not be re-counted", got)
	}

	// §7.2: horizontal scale is the only lever. Growing the fleet to 20 —
	// a mean share of 100 against a cap of 200, twice the headroom the
	// measured 1.21x worst-case spread needs — must clear the overflow.
	f2, _, coord, m2 := newFilterFixture(t, "adapter-0", limit, conns, "adapter-0")
	coord.SetMembership(members(20)...)
	if got := f2.Assignment(conns); len(got.Refused) != 0 {
		t.Errorf("with 20 instances, 2000 connections and a cap of 200, %d were still refused", len(got.Refused))
	}
	if got := testutil.ToFloat64(m2.ShardConnectionsRefused); got != 0 {
		t.Errorf("adapter_shard_connections_refused = %v after scaling out, want 0", got)
	}
}

// TestFilterHoldsAssignmentWhenCoordinatorIsLost is the P2/N2 stance:
// losing the coordinator must not move a single connection, because every
// move is a dropped chat socket. Recovery re-syncs.
func TestFilterHoldsAssignmentWhenCoordinatorIsLost(t *testing.T) {
	conns := testConns(2000)
	fleet := members(4)
	f, _, coord, m := newFilterFixture(t, "adapter-0", 0, conns, fleet...)

	before := listIDs(t, f)
	if len(before) == 0 {
		t.Fatal("instance owns nothing to begin with")
	}
	rebalances := testutil.ToFloat64(m.ShardRebalancesTotal)

	coord.Disconnect()
	during := listIDs(t, f)
	if !slices.Equal(before, during) {
		t.Fatalf("losing the coordinator moved connections: %d owned before, %d during", len(before), len(during))
	}
	if got := testutil.ToFloat64(m.ShardRebalancesTotal); got != rebalances {
		t.Errorf("a coordinator outage counted as a rebalance (%v -> %v)", rebalances, got)
	}
	if got := testutil.ToFloat64(m.ShardConnectionsOwned); got != float64(len(before)) {
		t.Errorf("adapter_shard_connections_owned = %v while disconnected, want %d", got, len(before))
	}

	// While disconnected, connections created or revoked in the meantime
	// are still assigned by the held ring — a stale ring is a complete
	// ring, so new work is not dropped either.
	extra := driver.Connection{ID: "conn-created-during-outage", Platform: "mock", CreatorID: "creator-x"}
	withExtra := append(slices.Clone(conns), extra)
	ring := NewRing(DefaultReplicas, fleet)
	owner, _ := ring.Owner(extra.ID)
	got := f.Assignment(withExtra)
	if slices.Contains(idsOf(got.Owned), extra.ID) != (owner == "adapter-0") {
		t.Errorf("a connection created during the outage was assigned to %q but this instance's view disagrees", owner)
	}

	// Recovery: a real membership change is applied and counted.
	coord.SetMembership(members(5)...)
	after := listIDs(t, f)
	if slices.Equal(before, after) {
		t.Error("re-syncing after recovery did not change the assignment even though an instance joined")
	}
	if got := testutil.ToFloat64(m.ShardRebalancesTotal); got != rebalances+1 {
		t.Errorf("adapter_shard_rebalances_total = %v after recovery, want %v", got, rebalances+1)
	}
	// And the recovery move is itself minimal.
	for _, id := range before {
		if !slices.Contains(after, id) {
			if o, _ := NewRing(DefaultReplicas, members(5)).Owner(id); o == "adapter-4" {
				continue // taken by the joiner, as it should be
			}
			t.Fatalf("connection %q left this instance for someone other than the joiner", id)
		}
	}
}

// TestFilterOwnsNothingWithoutMembership pins the cold-start stance
// argued in the package comment: no view means no work, not all the work.
func TestFilterOwnsNothingWithoutMembership(t *testing.T) {
	conns := testConns(100)
	f, _, coord, m := newFilterFixture(t, "adapter-0", 0, conns)

	if got := listIDs(t, f); len(got) != 0 {
		t.Fatalf("owned %d connections with no membership view; cold start must own nothing", len(got))
	}
	if got := testutil.ToFloat64(m.ShardMembers); got != 0 {
		t.Errorf("adapter_shard_members = %v with no view", got)
	}

	coord.SetMembership("adapter-0")
	if got := listIDs(t, f); len(got) != len(conns) {
		t.Fatalf("a single-member view owned %d of %d connections", len(got), len(conns))
	}
}

// TestFilterHoldsAssignmentWhenEvictedFromTheView: a view that leaves
// this instance out says the fleet thinks it is dead. Adopting it would
// drop every stream here in one go, so a working assignment is held until
// the instance is re-admitted.
func TestFilterHoldsAssignmentWhenEvictedFromTheView(t *testing.T) {
	conns := testConns(1000)
	f, _, coord, _ := newFilterFixture(t, "adapter-0", 0, conns, members(3)...)

	before := listIDs(t, f)
	if len(before) == 0 {
		t.Fatal("this instance owns nothing to begin with")
	}

	coord.SetMembership("adapter-1", "adapter-2")
	if got := listIDs(t, f); !slices.Equal(got, before) {
		t.Errorf("a view excluding this instance moved connections: %d owned before, %d after", len(before), len(got))
	}

	// Re-admission is applied normally.
	coord.SetMembership(members(3)...)
	if got := listIDs(t, f); !slices.Equal(got, before) {
		t.Errorf("re-admission changed the assignment: %d owned, want %d", len(got), len(before))
	}

	// The guard is for an instance that *has* an assignment. One that
	// never had one still owns nothing: it has no streams to protect and
	// no claim on the ring.
	cold, _, _, _ := newFilterFixture(t, "adapter-0", 0, conns, "adapter-1", "adapter-2")
	if got := listIDs(t, cold); len(got) != 0 {
		t.Errorf("an instance absent from its first view owned %d connections", len(got))
	}
}

// TestFilterLookupIsUnfiltered: the deletion consumer must be able to
// resolve credentials for a connection this instance does not watch,
// because deletions.v1's partitioning is unrelated to the ring.
func TestFilterLookupIsUnfiltered(t *testing.T) {
	conns := testConns(500)
	fleet := members(4)
	f, _, _, _ := newFilterFixture(t, "adapter-0", 0, conns, fleet...)

	owned := listIDs(t, f)
	if len(owned) == len(conns) {
		t.Fatal("this instance owns everything; the test proves nothing")
	}
	for i, c := range conns {
		got, ok := f.Lookup(c.CreatorID, c.Platform)
		if !ok || got.ID != c.ID {
			t.Fatalf("Lookup(%q, mock) = %+v, %v; every connection must resolve regardless of ownership (index %d)",
				c.CreatorID, got, ok, i)
		}
	}
}

// TestFilterForwardsBothChangeSources: the ingest manager wakes on a
// connection change and on a membership change alike.
func TestFilterForwardsBothChangeSources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, static, coord, _ := newFilterFixture(t, "adapter-0", 0, testConns(10), "adapter-0")
	f.Forward(ctx)

	drain(f.Changes())
	static.Add(driver.Connection{ID: "conn-new", Platform: "mock", CreatorID: "creator-new"})
	awaitSignal(t, f.Changes(), "a connection change")

	drain(f.Changes())
	coord.SetMembership("adapter-0", "adapter-1")
	awaitSignal(t, f.Changes(), "a membership change")
}

// TestFilterPropagatesListErrors: a Postgres blip must surface as an
// error the manager logs and retries, not as an empty assignment that
// tears down every watch loop.
func TestFilterPropagatesListErrors(t *testing.T) {
	f := NewFilter(errSource{}, newFakeCoordinator("adapter-0", "adapter-0"),
		DefaultReplicas, 0, metrics.New(prometheus.NewRegistry()), slog.New(slog.DiscardHandler))
	if _, err := f.List(context.Background()); err == nil {
		t.Fatal("List swallowed the wrapped source's error")
	}
}

type errSource struct{}

func (errSource) List(context.Context) ([]driver.Connection, error) {
	return nil, fmt.Errorf("connection store unavailable")
}
func (errSource) Lookup(string, string) (driver.Connection, bool) { return driver.Connection{}, false }
func (errSource) Changes() <-chan struct{}                        { return make(chan struct{}) }

func drain(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("no change signal after %s", what)
	}
}
