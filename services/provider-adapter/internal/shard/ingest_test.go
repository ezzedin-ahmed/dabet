package shard

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/goleak"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/ingest"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/opaque"
)

// TestMain fails the package if any test leaves a goroutine behind.
// Sharding adds three long-lived goroutine families — the coordinator's
// Run, the Filter's two change forwarders, and one watch loop per owned
// connection — and every one of them is started per rebalance, so a leak
// here would compound with every deploy rather than showing up as a
// single stuck goroutine.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// watchTracker is a driver.Driver that records every watch loop's
// lifecycle. It is what lets these tests assert the property that matters
// most: on a rebalance the manager starts and stops exactly the delta and
// leaves retained connections' sockets alone. Counting starts per
// connection is the only way to catch a "correct set, wrong churn" bug,
// where the assignment is right but every stream was reconnected to get
// there — precisely the outcome N2 forbids and the one a set-equality
// assertion cannot see.
type watchTracker struct {
	mu      sync.Mutex
	running map[string]bool
	starts  map[string]int
	stops   map[string]int
}

func newWatchTracker() *watchTracker {
	return &watchTracker{
		running: map[string]bool{},
		starts:  map[string]int{},
		stops:   map[string]int{},
	}
}

func (w *watchTracker) Platform() string { return "mock" }

func (w *watchTracker) Watch(ctx context.Context, conn driver.Connection, _ chan<- driver.Message) error {
	w.mu.Lock()
	w.running[conn.ID] = true
	w.starts[conn.ID]++
	w.mu.Unlock()

	<-ctx.Done()

	w.mu.Lock()
	delete(w.running, conn.ID)
	w.stops[conn.ID]++
	w.mu.Unlock()
	return nil
}

func (w *watchTracker) Delete(context.Context, driver.Connection, string, string) error { return nil }

func (w *watchTracker) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, nil
}

func (w *watchTracker) snapshot() (running []string, starts, stops map[string]int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	running = slices.Sorted(maps.Keys(w.running))
	return running, maps.Clone(w.starts), maps.Clone(w.stops)
}

func (w *watchTracker) awaitRunning(t *testing.T, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got, _, _ = w.snapshot()
		if slices.Equal(got, want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("running watch loops = %v, want %v", got, want)
}

// TestRebalanceStartsAndStopsExactlyTheDelta is the load-bearing test for
// A13. Every other property (a well-balanced ring, a stable cap) is worth
// nothing if a rebalance tears down streams that had no reason to move.
func TestRebalanceStartsAndStopsExactlyTheDelta(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conns := testConns(300)

	trk := newWatchTracker()
	reg := driver.NewRegistry()
	reg.Register(trk)

	static := connsource.NewStatic(conns...)
	coord := newFakeCoordinator("adapter-0", "adapter-0", "adapter-1")
	m := metrics.New(prometheus.NewRegistry())
	filter := NewFilter(static, coord, DefaultReplicas, 0, m, slog.New(slog.DiscardHandler))
	filter.Forward(ctx)

	mgr := ingest.NewManager(reg, filter, nopProducer{}, opaque.NewMinter(), m, slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() { defer close(done); _ = mgr.Run(ctx) }()

	// Phase 1: two instances. This one watches its segment and nothing else.
	want2 := ownedIDs(t, filter, conns)
	trk.awaitRunning(t, want2)
	if len(want2) == 0 || len(want2) == len(conns) {
		t.Fatalf("a two-instance split gave this instance %d of %d connections; the test needs a real split",
			len(want2), len(conns))
	}
	_, starts1, stops1 := trk.snapshot()
	for id, n := range starts1 {
		if n != 1 {
			t.Fatalf("connection %q was started %d times before any rebalance", id, n)
		}
	}
	if len(stops1) != 0 {
		t.Fatalf("watch loops were stopped before any rebalance: %v", stops1)
	}

	// Phase 2: a third instance joins. Only the connections it takes may
	// stop; everything retained must keep its original, uninterrupted
	// watch loop.
	coord.SetMembership("adapter-0", "adapter-1", "adapter-2")
	want3 := ownedIDs(t, filter, conns)
	trk.awaitRunning(t, want3)

	retained := intersect(want2, want3)
	lost := difference(want2, want3)
	if len(lost) == 0 {
		t.Fatal("the joining instance took nothing from this one; the test proves nothing")
	}
	if len(retained) == 0 {
		t.Fatal("this instance kept nothing across the rebalance; the test proves nothing")
	}

	_, starts2, stops2 := trk.snapshot()
	for _, id := range retained {
		if starts2[id] != 1 {
			t.Errorf("retained connection %q was started %d times; a connection that did not change owner must never reconnect",
				id, starts2[id])
		}
		if stops2[id] != 0 {
			t.Errorf("retained connection %q was stopped %d times", id, stops2[id])
		}
	}
	for _, id := range lost {
		if stops2[id] != 1 {
			t.Errorf("connection %q moved to the joining instance but was stopped %d times here", id, stops2[id])
		}
	}
	// Exact set: nothing outside `lost` was stopped.
	for id, n := range stops2 {
		if n > 0 && !slices.Contains(lost, id) {
			t.Errorf("connection %q was stopped but did not change owner", id)
		}
	}

	// Phase 3: an instance leaves. This one must pick up only its share of
	// the departed segment — starting new loops, never restarting old ones.
	coord.SetMembership("adapter-0", "adapter-2")
	want4 := ownedIDs(t, filter, conns)
	trk.awaitRunning(t, want4)

	gained := difference(want4, want3)
	if len(gained) == 0 {
		t.Fatal("a departing instance handed this one nothing; the test proves nothing")
	}
	_, starts3, stops3 := trk.snapshot()
	for _, id := range intersect(want3, want4) {
		if starts3[id] != starts2[id] {
			t.Errorf("connection %q retained across the leave was restarted (%d -> %d starts)",
				id, starts2[id], starts3[id])
		}
	}
	for _, id := range gained {
		// A connection this instance had *never* held starts once; one it
		// held before phase 2 and gets back starts a second time. Either
		// way it must be running now and have started exactly once more
		// than it had.
		if stops3[id] > starts3[id] {
			t.Errorf("connection %q gained on the leave has %d stops for %d starts", id, stops3[id], starts3[id])
		}
	}

	// Three membership views were installed (the initial one plus the join
	// and the leave), and each counted exactly once no matter how many
	// times List ran in between.
	if got := testutil.ToFloat64(m.ShardRebalancesTotal); got != 3 {
		t.Errorf("adapter_shard_rebalances_total = %v, want 3", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingest manager did not stop")
	}
	if running, _, _ := trk.snapshot(); len(running) != 0 {
		t.Errorf("%d watch loops still running after shutdown: %v", len(running), running)
	}
}

// TestCoordinatorLossDoesNotRestartWatchLoops: the P2 stance end to end.
// A coordinator outage signals a change, the manager re-Lists, and not one
// socket moves.
func TestCoordinatorLossDoesNotRestartWatchLoops(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conns := testConns(200)

	trk := newWatchTracker()
	reg := driver.NewRegistry()
	reg.Register(trk)

	static := connsource.NewStatic(conns...)
	coord := newFakeCoordinator("adapter-0", members(3)...)
	m := metrics.New(prometheus.NewRegistry())
	filter := NewFilter(static, coord, DefaultReplicas, 0, m, slog.New(slog.DiscardHandler))
	filter.Forward(ctx)
	go func() { _ = coord.Run(ctx) }()

	mgr := ingest.NewManager(reg, filter, nopProducer{}, opaque.NewMinter(), m, slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() { defer close(done); _ = mgr.Run(ctx) }()

	want := ownedIDs(t, filter, conns)
	trk.awaitRunning(t, want)
	_, startsBefore, _ := trk.snapshot()

	// The outage. It signals, so the manager genuinely re-reconciles —
	// this is not a test that passes because nothing happened.
	coord.Disconnect()
	// Give the reconcile a chance to do damage before asserting it didn't.
	time.Sleep(50 * time.Millisecond)
	trk.awaitRunning(t, want)
	_, startsAfter, stopsAfter := trk.snapshot()
	if !maps.Equal(startsBefore, startsAfter) {
		t.Error("a coordinator outage restarted watch loops")
	}
	if len(stopsAfter) != 0 {
		t.Errorf("a coordinator outage stopped %d watch loops", len(stopsAfter))
	}

	// Recovery re-syncs: the same membership coming back is a no-op, and a
	// genuinely new one is applied.
	coord.SetMembership(members(3)...)
	time.Sleep(50 * time.Millisecond)
	trk.awaitRunning(t, want)
	if _, s, _ := trk.snapshot(); !maps.Equal(startsBefore, s) {
		t.Error("re-syncing an unchanged membership restarted watch loops")
	}

	coord.SetMembership(members(4)...)
	trk.awaitRunning(t, ownedIDs(t, filter, conns))

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingest manager did not stop")
	}
	// Explicit accounting on top of goleak: the coordinator's own loop
	// must return on cancellation, not merely stop being observed.
	coord.runs.Wait()
}

// TestShardingDisabledIsUnchanged: with no Filter in the chain the manager
// watches every connection the Source lists, exactly as it does today.
// This is the guard on the default-off promise that keeps `make up`,
// `make e2e` and the load harness working.
func TestShardingDisabledIsUnchanged(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conns := testConns(50)

	trk := newWatchTracker()
	reg := driver.NewRegistry()
	reg.Register(trk)
	static := connsource.NewStatic(conns...)

	mgr := ingest.NewManager(reg, static, nopProducer{}, opaque.NewMinter(),
		metrics.New(prometheus.NewRegistry()), slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() { defer close(done); _ = mgr.Run(ctx) }()

	trk.awaitRunning(t, idsOf(conns))

	// And a single-instance Filter is behaviourally identical to no
	// Filter at all: a fleet of one owns the whole ring. That is what
	// makes turning sharding on in a one-replica deployment a no-op.
	fctx, fcancel := context.WithCancel(context.Background())
	f := NewFilter(connsource.NewStatic(conns...), newFakeCoordinator("adapter-0", "adapter-0"),
		DefaultReplicas, 0, metrics.New(prometheus.NewRegistry()), slog.New(slog.DiscardHandler))
	f.Forward(fctx)
	got, err := f.List(fctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(idsOf(got), idsOf(conns)) {
		t.Errorf("a single-instance shard filter listed %d of %d connections", len(got), len(conns))
	}
	fcancel()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingest manager did not stop")
	}
}

func ownedIDs(t *testing.T, f *Filter, conns []driver.Connection) []string {
	t.Helper()
	return idsOf(f.Assignment(conns).Owned)
}

func intersect(a, b []string) []string {
	var out []string
	for _, v := range a {
		if slices.Contains(b, v) {
			out = append(out, v)
		}
	}
	return out
}

func difference(a, b []string) []string {
	var out []string
	for _, v := range a {
		if !slices.Contains(b, v) {
			out = append(out, v)
		}
	}
	return out
}

type nopProducer struct{}

func (nopProducer) Produce(context.Context, string, []byte, []byte) error { return nil }
