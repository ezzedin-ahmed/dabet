package kafkax

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// These tests drive the per-key concurrency path through the same fake
// groupClient the rest of the dispatcher tests use: no broker, no
// container, no network. The properties asserted here are the ones the
// feature is only worth having if it keeps — ordering per key, real
// parallelism between keys, and a commit mark that never runs past the
// contiguous completed prefix.

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

// keyedRecords builds consecutive records for one partition, taking one
// key per record.
func keyedRecords(topic string, partition int32, start int64, keys []string) []*kgo.Record {
	out := make([]*kgo.Record, len(keys))
	for i, k := range keys {
		out[i] = &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Offset:    start + int64(i),
			Key:       []byte(k),
			Value:     []byte("v"),
		}
	}
	return out
}

// keysOnDistinctWorkers returns exactly n keys, one for each worker index
// of an n-wide fan-out. It is how the parallelism tests guarantee that
// what they park on the barrier really is n different workers.
func keysOnDistinctWorkers(t *testing.T, n int) []string {
	t.Helper()
	out := make([]string, n)
	found := 0
	for i := 0; found < n && i < 100_000; i++ {
		k := fmt.Sprintf("key-%d", i)
		w := workerFor([]byte(k), n)
		if out[w] == "" {
			out[w] = k
			found++
		}
	}
	if found != n {
		t.Fatalf("could only cover %d of %d worker slots", found, n)
	}
	return out
}

// keysAvoidingWorker returns count keys, none of which route to avoid.
// Used to keep a deliberately blocked worker from starving the records a
// test needs to see complete.
func keysAvoidingWorker(t *testing.T, n, avoid, count int) []string {
	t.Helper()
	out := make([]string, 0, count)
	for i := 0; len(out) < count && i < 1_000_000; i++ {
		k := fmt.Sprintf("avoid-%d", i)
		if workerFor([]byte(k), n) != avoid {
			out = append(out, k)
		}
	}
	if len(out) != count {
		t.Fatalf("only found %d of %d keys avoiding worker %d", len(out), count, avoid)
	}
	return out
}

// keyConcurrentConfig is testConfig with the feature switched on.
func keyConcurrentConfig(n int, opts ...Option) ConsumerConfig {
	return testConfig(append([]Option{WithKeyConcurrency(n)}, opts...)...)
}

// handledPrefixOK asserts the invariant that makes at-least-once true:
// every offset below the commit mark was actually handled.
func handledPrefixOK(t *testing.T, mark int64, handled map[int64]bool) {
	t.Helper()
	for off := int64(0); off < mark; off++ {
		if !handled[off] {
			t.Fatalf("offset %d is below the commit mark %d but was never handled: a crash here loses it", off, mark)
		}
	}
}

// ---------------------------------------------------------------------
// routing
// ---------------------------------------------------------------------

// TestRoutingIsKeyStable is the whole basis of the ordering guarantee: the
// same key must always pick the same worker, for the life of the process
// and across processes, so a key is never handled by two goroutines.
func TestRoutingIsKeyStable(t *testing.T) {
	const n = 16
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("hash(%d, %d)", i, i*7))
		want := workerFor(k, n)
		for rep := 0; rep < 4; rep++ {
			if got := workerFor(k, n); got != want {
				t.Fatalf("key %q routed to %d then %d: routing is not stable", k, want, got)
			}
		}
		if want < 0 || want >= n {
			t.Fatalf("key %q routed to worker %d, outside [0,%d)", k, want, n)
		}
	}
}

// TestRoutingIsProcessIndependent pins the hash itself. maphash would pass
// TestRoutingIsKeyStable and still be wrong: it is seeded per process, so
// two members of the same group would disagree about which worker owns a
// key. These constants are FNV-1a of the given inputs and must not drift.
func TestRoutingIsProcessIndependent(t *testing.T) {
	cases := map[string]uint64{
		"":     14695981039346656037,
		"a":    12638187200555641996,
		"dab":  14595390244277163932,
		"abcd": 18165163011005162717,
	}
	for in, want := range cases {
		if got := stableHash([]byte(in)); got != want {
			t.Errorf("stableHash(%q) = %d, want %d: the routing hash must be stable across processes and releases", in, got, want)
		}
	}
}

// TestRoutingSpreadsKeys checks the fan-out is actually a fan-out: if
// every key landed on one worker the feature would be a no-op that still
// paid for its bookkeeping.
func TestRoutingSpreadsKeys(t *testing.T) {
	const (
		n    = 8
		keys = 4000
	)
	counts := make([]int, n)
	for i := 0; i < keys; i++ {
		counts[workerFor([]byte(fmt.Sprintf("hash(sd_%d, ct_%d)", i, i%97)), n)]++
	}
	for w, c := range counts {
		if c == 0 {
			t.Fatalf("worker %d received none of %d keys", w, keys)
		}
		// Very loose: this is a smoke test for a degenerate hash, not a
		// statistical claim about FNV.
		if c < keys/(n*4) {
			t.Errorf("worker %d received %d of %d keys, a badly skewed spread", w, c, keys)
		}
	}
}

// TestEmptyKeyRoutingIsDefined pins the documented behaviour for records
// with no key: all of them go to worker 0, so their relative order is
// preserved exactly as it was on the serial consumer.
func TestEmptyKeyRoutingIsDefined(t *testing.T) {
	for _, k := range [][]byte{nil, {}} {
		if got := workerFor(k, 32); got != 0 {
			t.Errorf("workerFor(%v, 32) = %d, want 0", k, got)
		}
	}
	// A fan-out of one is always worker 0 whatever the key.
	if got := workerFor([]byte("anything"), 1); got != 0 {
		t.Errorf("workerFor with n=1 = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------
// ordering
// ---------------------------------------------------------------------

// TestPerKeyOrderingUnderConcurrency is the load-bearing test. Many keys
// share one partition and run concurrently; for every key the handler must
// see strictly increasing offsets and must never be entered twice at once.
//
// A router that is not key-stable — anything from round-robin to a
// per-process hash seed — fails this: two records of one key land on
// different workers, overlap, and trip the in-flight guard or the
// monotonicity check.
func TestPerKeyOrderingUnderConcurrency(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic    = "messages.v1"
		fanOut   = 8
		nKeys    = 64
		perKey   = 12
		total    = nKeys * perKey
		nFetches = 4
	)

	type keyState struct {
		active atomic.Int32
		mu     sync.Mutex
		seen   []int64
	}
	states := make([]*keyState, nKeys)
	for i := range states {
		states[i] = &keyState{}
	}
	keyIndex := make(map[string]int, nKeys)
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("hash(sd_%d, ct_%d)", i, i)
		keyIndex[keys[i]] = i
	}

	var (
		violations atomic.Int64
		overlaps   atomic.Int64
		done       sync.WaitGroup
	)
	done.Add(total)

	handler := func(_ context.Context, rec *kgo.Record) error {
		st := states[keyIndex[string(rec.Key)]]
		if !st.active.CompareAndSwap(0, 1) {
			// Two records of the same key in flight at once. This is the
			// exact thing §7.3 forbids.
			overlaps.Add(1)
		}
		// Uneven work, so a router that merely happens to preserve order
		// on a fast path does not get away with it.
		if rec.Offset%3 == 0 {
			time.Sleep(200 * time.Microsecond)
		}
		st.mu.Lock()
		if n := len(st.seen); n > 0 && rec.Offset <= st.seen[n-1] {
			violations.Add(1)
		}
		st.seen = append(st.seen, rec.Offset)
		st.mu.Unlock()
		st.active.Store(0)
		done.Done()
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	defer d.shutdown()

	// One partition, records interleaved across all keys, split over
	// several fetches so the batch boundary is exercised too.
	perFetch := total / nFetches
	for f := 0; f < nFetches; f++ {
		batchKeys := make([]string, perFetch)
		for i := range batchKeys {
			batchKeys[i] = keys[(f*perFetch+i)%nKeys]
		}
		d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
			0: keyedRecords(topic, 0, int64(f*perFetch), batchKeys),
		}))
	}
	waitGroup(t, &done, 30*time.Second)

	if n := overlaps.Load(); n != 0 {
		t.Fatalf("%d times two records of one key were in flight together: routing is not key-stable", n)
	}
	if n := violations.Load(); n != 0 {
		t.Fatalf("%d out-of-order deliveries within a key", n)
	}
	for i, st := range states {
		st.mu.Lock()
		seen := append([]int64(nil), st.seen...)
		st.mu.Unlock()
		if len(seen) != perKey {
			t.Fatalf("key %d: handled %d records, want %d", i, len(seen), perKey)
		}
		for j := 1; j < len(seen); j++ {
			if seen[j] <= seen[j-1] {
				t.Fatalf("key %d: offset %d handled after %d", i, seen[j], seen[j-1])
			}
		}
	}
	// Everything succeeded, so the low-water mark must be the end of the
	// partition — no record may be left behind by the prefix logic.
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(topicPartition{topic, 0}) == total })
}

// TestSingleHotKeyPartitionStaysCorrect is the degenerate assignment: one
// key carries the whole partition, so exactly one worker does all the work
// and the consumer must behave precisely as the serial one did.
func TestSingleHotKeyPartitionStaysCorrect(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "messages.v1"
		fanOut = 16
		total  = 200
	)
	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		mu       sync.Mutex
		seen     []int64
		done     sync.WaitGroup
	)
	done.Add(total)

	handler := func(_ context.Context, rec *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		mu.Lock()
		seen = append(seen, rec.Offset)
		mu.Unlock()
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	keys := make([]string, total)
	for i := range keys {
		keys[i] = "hash(sd_1, ct_1)"
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitGroup(t, &done, 30*time.Second)

	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent handlers = %d for a single key, want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, off := range seen {
		if off != int64(i) {
			t.Fatalf("position %d holds offset %d: a hot key lost its order", i, off)
		}
	}
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(topicPartition{topic, 0}) == total })
}

// TestEmptyKeyRecordsStayOrdered is the other half of the empty-key
// contract, end to end through the dispatcher: unkeyed records share
// worker 0 and are therefore handled one at a time, in order.
func TestEmptyKeyRecordsStayOrdered(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "deletions.v1"
		fanOut = 8
		total  = 60
	)
	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		mu       sync.Mutex
		seen     []int64
		done     sync.WaitGroup
	)
	done.Add(total)

	handler := func(_ context.Context, rec *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		mu.Lock()
		seen = append(seen, rec.Offset)
		mu.Unlock()
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	// records() leaves Key nil, which is exactly the case under test.
	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{0: records(topic, 0, 0, total)}))
	waitGroup(t, &done, 30*time.Second)

	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent handlers = %d for unkeyed records, want 1 (all route to worker 0)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, off := range seen {
		if off != int64(i) {
			t.Fatalf("position %d holds offset %d: unkeyed records lost their order", i, off)
		}
	}
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(topicPartition{topic, 0}) == total })
}

// ---------------------------------------------------------------------
// parallelism
// ---------------------------------------------------------------------

// TestKeysRunInParallelWithinOnePartition proves the point of the whole
// change by construction rather than by timing luck: eight keys of ONE
// partition park on a barrier that only opens when all eight are inside
// their handlers at the same moment. The pre-change consumer — one
// goroutine per partition — can never open it, so a regression to serial
// processing fails here by timing out instead of passing quietly.
func TestKeysRunInParallelWithinOnePartition(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "messages.v1"
		fanOut = 8
	)
	keys := keysOnDistinctWorkers(t, fanOut)

	var (
		arrived atomic.Int64
		gate    = make(chan struct{})
		stalled atomic.Bool
		done    sync.WaitGroup
	)
	done.Add(fanOut)

	handler := func(_ context.Context, rec *kgo.Record) error {
		if arrived.Add(1) == fanOut {
			close(gate)
		}
		select {
		case <-gate:
		case <-time.After(10 * time.Second):
			// Only reachable if the keys are not actually overlapping in
			// time, i.e. the partition is still being processed serially.
			stalled.Store(true)
		}
		done.Done()
		return nil
	}

	d := newDispatcher(context.Background(), newFakeClient(), handler, "g", keyConcurrentConfig(fanOut))
	defer func() {
		// Make sure a failure cannot wedge the suite.
		select {
		case <-gate:
		default:
			close(gate)
		}
		d.shutdown()
	}()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitGroup(t, &done, 20*time.Second)

	if stalled.Load() {
		t.Fatal("keys within one partition did not run concurrently: the barrier timed out")
	}
	if got := arrived.Load(); got < fanOut {
		t.Fatalf("only %d of %d keys reached the barrier", got, fanOut)
	}
}

// TestKeyConcurrencyRespectsThePartitionCeiling checks that the two knobs
// compose: a fan-out of 16 behind an instance-wide ceiling of 3 may never
// run more than 3 handlers at once.
func TestKeyConcurrencyRespectsThePartitionCeiling(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "messages.v1"
		fanOut = 16
		limit  = 3
		total  = 120
	)
	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		done     sync.WaitGroup
	)
	done.Add(total)

	handler := func(context.Context, *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(200 * time.Microsecond)
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	d := newDispatcher(context.Background(), newFakeClient(), handler, "g",
		keyConcurrentConfig(fanOut, WithPartitionConcurrency(limit)))
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitGroup(t, &done, 30*time.Second)

	if got := maxSeen.Load(); got > limit {
		t.Fatalf("max concurrent handlers = %d, want at most the instance ceiling of %d", got, limit)
	}
	if got := maxSeen.Load(); got < 2 {
		t.Fatalf("max concurrent handlers = %d: nothing ran in parallel at all", got)
	}
}

// ---------------------------------------------------------------------
// low-water-mark commits
// ---------------------------------------------------------------------

// TestLowWaterMarkHoldsBehindASlowRecord is the commit invariant, and the
// reason this change is dangerous if it is wrong. One record stalls; every
// record after it finishes. The mark must stop at the stalled record and
// stay there, however many later records succeeded — and must jump to the
// end the moment the stalled one completes.
func TestLowWaterMarkHoldsBehindASlowRecord(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic   = "messages.v1"
		fanOut  = 8
		total   = 40
		stallAt = int64(5)
	)
	tp := topicPartition{topic, 0}

	blocker := keysOnDistinctWorkers(t, fanOut)[3]
	blockerWorker := workerFor([]byte(blocker), fanOut)
	// Everything except the stalled record must be able to finish, so no
	// other key may queue behind the blocked worker.
	others := keysAvoidingWorker(t, fanOut, blockerWorker, total-1)

	keys := make([]string, total)
	oi := 0
	for i := range keys {
		if int64(i) == stallAt {
			keys[i] = blocker
			continue
		}
		keys[i] = others[oi]
		oi++
	}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	reached := make(chan struct{})
	var once sync.Once

	var (
		mu       sync.Mutex
		handled  = make(map[int64]bool)
		restDone sync.WaitGroup
	)
	restDone.Add(total - 1)

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == stallAt {
			once.Do(func() { close(reached) })
			<-release
		}
		mu.Lock()
		handled[rec.Offset] = true
		mu.Unlock()
		if rec.Offset != stallAt {
			restDone.Done()
		}
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	defer func() {
		unblock()
		d.shutdown()
	}()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitClosed(t, reached, 10*time.Second)
	// Every other record — including all 34 above the stall — completes.
	waitGroup(t, &restDone, 20*time.Second)

	// The prefix below the stall is committable; nothing above it is.
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(tp) == stallAt })
	// Hold it there for a while: a late mark from a completed record above
	// the stall would show up as a jump.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := cl.markOf(tp); got != stallAt {
			t.Fatalf("commit mark = %d while offset %d is still in flight: records above it would be lost on a crash", got, stallAt)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The lag position must be the same low-water mark, not the count of
	// records finished.
	if pos := d.positions()[tp]; pos != stallAt {
		t.Fatalf("positions()[%v] = %d, want the low-water mark %d", tp, pos, stallAt)
	}

	mu.Lock()
	snapshot := make(map[int64]bool, len(handled))
	for k, v := range handled {
		snapshot[k] = v
	}
	mu.Unlock()
	handledPrefixOK(t, cl.markOf(tp), snapshot)

	// Once the blocker lands, the mark must jump over everything that was
	// already finished behind it — in one step, to the end.
	unblock()
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(tp) == total })
	if pos := d.positions()[tp]; pos != total {
		t.Fatalf("positions()[%v] = %d after the stall cleared, want %d", tp, pos, total)
	}
}

// TestFailedRecordHoldsTheMarkForever is the same shape with the record
// failing instead of stalling: at-least-once demands that neither the
// failed record nor anything after it is ever committed, no matter how
// many later records succeeded first.
func TestFailedRecordHoldsTheMarkForever(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "messages.v1"
		fanOut = 8
		total  = 40
		failAt = int64(7)
	)
	tp := topicPartition{topic, 0}
	boom := errors.New("handler exploded")

	blocker := keysOnDistinctWorkers(t, fanOut)[1]
	blockerWorker := workerFor([]byte(blocker), fanOut)
	others := keysAvoidingWorker(t, fanOut, blockerWorker, total-1)

	keys := make([]string, total)
	oi := 0
	for i := range keys {
		if int64(i) == failAt {
			keys[i] = blocker
			continue
		}
		keys[i] = others[oi]
		oi++
	}

	// The failing record waits until the whole prefix below it has
	// succeeded and plenty of records *above* it have too, so the failure
	// genuinely lands after out-of-order completion rather than before any
	// of it. Both counters are needed: without the second the test would
	// pass on a consumer that never ran ahead at all.
	const wantAbove = 20
	var below, above atomic.Int64
	enough := make(chan struct{})
	var once sync.Once
	arm := func() {
		if below.Load() == failAt && above.Load() >= wantAbove {
			once.Do(func() { close(enough) })
		}
	}

	var (
		mu      sync.Mutex
		handled = make(map[int64]bool)
	)

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == failAt {
			select {
			case <-enough:
			case <-time.After(20 * time.Second):
			}
			return boom
		}
		mu.Lock()
		handled[rec.Offset] = true
		mu.Unlock()
		if rec.Offset < failAt {
			below.Add(1)
		} else {
			above.Add(1)
		}
		arm()
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitClosed(t, d.stop, 30*time.Second)

	if err := d.failure(); !errors.Is(err, boom) {
		t.Fatalf("failure() = %v, want %v", err, boom)
	}
	if n := above.Load(); n < wantAbove {
		t.Fatalf("only %d records above the failure completed; the test did not reach out-of-order completion", n)
	}
	if n := below.Load(); n != failAt {
		t.Fatalf("%d of %d records below the failure completed", n, failAt)
	}
	// The mark reaches the failed record — the whole prefix below it
	// succeeded — and then stops dead, however many records above it
	// finished. Polling rather than sampling once: the last successful
	// record below the failure may still have been between its handler
	// returning and its mark landing when d.stop closed.
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(tp) == failAt })
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := cl.markOf(tp); got != failAt {
			t.Fatalf("commit mark = %d, want %d: the mark must stop at the failed record", got, failAt)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := cl.CommitMarkedOffsets(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := cl.committedOf(tp); got != failAt {
		t.Fatalf("committed %d, want %d", got, failAt)
	}
	mu.Lock()
	defer mu.Unlock()
	handledPrefixOK(t, cl.committedOf(tp), handled)
}

// TestCrashRedeliversFromTheLowWaterMark is the crash story stated as the
// operator sees it: after a partial, out-of-order completion the process
// dies, and the offset that survives must be the low-water mark. Nothing
// below it may be missing (that would be loss) and nothing above it may be
// skipped (that would be loss too) — the successful records above the mark
// are simply replayed, which §7.8's idempotent effects absorb.
func TestCrashRedeliversFromTheLowWaterMark(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic   = "messages.v1"
		fanOut  = 8
		total   = 50
		stallAt = int64(9)
	)
	tp := topicPartition{topic, 0}

	blocker := keysOnDistinctWorkers(t, fanOut)[5]
	others := keysAvoidingWorker(t, fanOut, workerFor([]byte(blocker), fanOut), total-1)
	keys := make([]string, total)
	oi := 0
	for i := range keys {
		if int64(i) == stallAt {
			keys[i] = blocker
			continue
		}
		keys[i] = others[oi]
		oi++
	}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	reached := make(chan struct{})
	var once sync.Once

	var (
		mu      sync.Mutex
		handled = make(map[int64]bool)
		rest    sync.WaitGroup
	)
	rest.Add(total - 1)

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == stallAt {
			once.Do(func() { close(reached) })
			<-release
		}
		mu.Lock()
		handled[rec.Offset] = true
		mu.Unlock()
		if rec.Offset != stallAt {
			rest.Done()
		}
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitClosed(t, reached, 10*time.Second)
	waitGroup(t, &rest, 20*time.Second)
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(tp) == stallAt })

	// "Crash": commit whatever is marked — which is exactly what Run does
	// on its way out — and stop asking questions about anything else.
	if err := cl.CommitMarkedOffsets(context.Background()); err != nil {
		t.Fatal(err)
	}
	committed := cl.committedOf(tp)
	if committed != stallAt {
		t.Fatalf("committed %d, want the low-water mark %d", committed, stallAt)
	}

	mu.Lock()
	snapshot := make(map[int64]bool, len(handled))
	for k, v := range handled {
		snapshot[k] = v
	}
	nAbove := 0
	for off := range handled {
		if off > stallAt {
			nAbove++
		}
	}
	mu.Unlock()

	// The scenario is only interesting if records above the mark really
	// did complete out of order before the crash.
	if nAbove == 0 {
		t.Fatal("no record above the stall completed; this is not the out-of-order crash case")
	}
	// Nothing below the committed offset may be unhandled: that is loss.
	handledPrefixOK(t, committed, snapshot)

	// The restarted member resumes at the mark and re-reads the tail
	// contiguously — nothing between the mark and the end is skipped.
	unblock()
	d.shutdown()

	var redelivered []int64
	var second sync.WaitGroup
	second.Add(int(total - committed))
	var rmu sync.Mutex
	d2 := newDispatcher(context.Background(), newFakeClient(),
		func(_ context.Context, rec *kgo.Record) error {
			rmu.Lock()
			redelivered = append(redelivered, rec.Offset)
			rmu.Unlock()
			second.Done()
			return nil
		}, "g", keyConcurrentConfig(fanOut))
	defer d2.shutdown()
	d2.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, committed, keys[committed:]),
	}))
	waitGroup(t, &second, 20*time.Second)

	rmu.Lock()
	defer rmu.Unlock()
	got := make(map[int64]bool, len(redelivered))
	for _, off := range redelivered {
		got[off] = true
	}
	for off := committed; off < total; off++ {
		if !got[off] {
			t.Fatalf("offset %d was neither committed nor redelivered: it is lost", off)
		}
	}
}

// ---------------------------------------------------------------------
// rebalance and shutdown
// ---------------------------------------------------------------------

// TestRevokeMidFlightAcrossKeys is the rebalance path with several keys in
// flight at once: the revoke waits for the records actually inside
// handlers, drops the queued ones, commits nothing that was not processed,
// and leaves no goroutine behind.
func TestRevokeMidFlightAcrossKeys(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic    = "messages.v1"
		fanOut   = 8
		inFlight = 4
		total    = 400
	)
	tp := topicPartition{topic, 0}

	distinct := keysOnDistinctWorkers(t, fanOut)
	keys := make([]string, total)
	for i := range keys {
		keys[i] = distinct[i%fanOut]
	}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	var (
		parked  atomic.Int64
		allIn   = make(chan struct{})
		once    sync.Once
		mu      sync.Mutex
		handled = make(map[int64]bool)
	)

	handler := func(_ context.Context, rec *kgo.Record) error {
		// The first record of each of the first `inFlight` workers parks,
		// so several keys are genuinely mid-record when the revoke lands.
		if rec.Offset < inFlight {
			if parked.Add(1) == inFlight {
				once.Do(func() { close(allIn) })
			}
			<-release
		}
		mu.Lock()
		handled[rec.Offset] = true
		mu.Unlock()
		return nil
	}

	cl := newFakeClient()
	var forgotten []topicPartition
	var forgetMu sync.Mutex
	d := newDispatcher(context.Background(), cl, handler, "g", keyConcurrentConfig(fanOut))
	d.onForget = func(tp topicPartition) {
		forgetMu.Lock()
		forgotten = append(forgotten, tp)
		forgetMu.Unlock()
	}
	defer func() {
		unblock()
		d.shutdown()
	}()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitClosed(t, allIn, 10*time.Second)

	w := d.worker(tp)
	revokeDone := make(chan struct{})
	go func() {
		defer close(revokeDone)
		d.releasePartitions(map[string][]int32{topic: {0}})
	}()
	waitClosed(t, w.quit, 10*time.Second)

	// The revoke must be waiting on the in-flight handlers, not tearing
	// them down underneath.
	select {
	case <-revokeDone:
		t.Fatal("revoke returned while handlers were still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	waitClosed(t, revokeDone, 10*time.Second)

	// Every worker of the partition is gone.
	select {
	case <-w.done:
	default:
		t.Fatal("the partition's workers were still running after the revoke returned")
	}
	d.mu.Lock()
	_, stillThere := d.workers[tp]
	d.mu.Unlock()
	if stillThere {
		t.Fatal("revoked partition still has a worker")
	}

	mark := cl.markOf(tp)
	if mark >= total {
		t.Fatalf("commit mark = %d of %d: the revoke committed records it never processed", mark, total)
	}
	mu.Lock()
	defer mu.Unlock()
	handledPrefixOK(t, mark, handled)

	forgetMu.Lock()
	got := append([]topicPartition(nil), forgotten...)
	forgetMu.Unlock()
	if len(got) != 1 || got[0] != tp {
		t.Fatalf("forgotten lag series = %v, want exactly %v", got, tp)
	}
}

// TestKeyedShutdownDrainsEveryWorker is the explicit no-leak accounting:
// after shutdown every partition's coordinator and every one of its
// sub-workers has returned, and the drain fits inside DrainTimeout.
func TestKeyedShutdownDrainsEveryWorker(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic      = "messages.v1"
		fanOut     = 8
		partitions = 4
		perPart    = 50
	)
	var done sync.WaitGroup
	done.Add(partitions * perPart)
	handler := func(context.Context, *kgo.Record) error {
		done.Done()
		return nil
	}
	d := newDispatcher(context.Background(), newFakeClient(), handler, "g",
		keyConcurrentConfig(fanOut, WithDrainTimeout(5*time.Second)))

	keys := make([]string, perPart)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	byPartition := make(map[int32][]*kgo.Record, partitions)
	for p := int32(0); p < partitions; p++ {
		byPartition[p] = keyedRecords(topic, p, 0, keys)
	}
	d.dispatch(fetchOf(topic, byPartition))
	waitGroup(t, &done, 20*time.Second)

	d.mu.Lock()
	ws := make([]*partitionWorker, 0, len(d.workers))
	for _, w := range d.workers {
		ws = append(ws, w)
	}
	d.mu.Unlock()
	if len(ws) != partitions {
		t.Fatalf("workers = %d, want %d", len(ws), partitions)
	}

	start := time.Now()
	d.shutdown()
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("shutdown took %s, longer than the drain timeout", took)
	}
	for _, w := range ws {
		select {
		case <-w.done:
		default:
			t.Fatalf("worker for %v was still running after shutdown", w.tp)
		}
	}
}

// ---------------------------------------------------------------------
// bounded queues
// ---------------------------------------------------------------------

// TestSlowKeyDoesNotGrowTheInFlightWindow is the memory bound: a key whose
// handler never returns must stop the partition dispatching new work once
// the in-flight window is full, rather than letting the uncommitted set
// grow for as long as records keep arriving. It must also not deadlock —
// the blocked record is already dispatched and needs no slot to finish.
func TestSlowKeyDoesNotGrowTheInFlightWindow(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic  = "messages.v1"
		fanOut = 4
		depth  = 3
		window = fanOut * depth
		total  = 500
	)
	tp := topicPartition{topic, 0}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	reached := make(chan struct{})
	var once sync.Once
	var handledCount atomic.Int64

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 0 {
			once.Do(func() { close(reached) })
			<-release
		}
		handledCount.Add(1)
		return nil
	}

	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	d := newDispatcher(context.Background(), newFakeClient(), handler, "g",
		keyConcurrentConfig(fanOut, WithKeyQueueDepth(depth)))
	defer func() {
		unblock()
		d.shutdown()
	}()

	go d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitClosed(t, reached, 10*time.Second)

	w := d.worker(tp)
	waitFor(t, 10*time.Second, func() bool {
		tr := w.track.Load()
		return tr != nil && tr.pending() >= 1
	})
	// Give the coordinator every chance to over-dispatch, then check it
	// did not.
	time.Sleep(300 * time.Millisecond)
	tr := w.track.Load()
	if tr == nil {
		t.Fatal("no offset tracker on a key-concurrent partition")
	}
	if got := tr.pending(); got > window {
		t.Fatalf("%d records pending with a window of %d: the in-flight set is unbounded", got, window)
	}
	if got := handledCount.Load(); got > int64(window) {
		t.Fatalf("handled %d records past the blocked head with a window of %d", got, window)
	}

	// Releasing the head must let the whole partition drain — the bound
	// throttles, it does not deadlock.
	unblock()
	waitFor(t, 20*time.Second, func() bool { return handledCount.Load() == total })
}

// ---------------------------------------------------------------------
// offsetTracker unit tests
// ---------------------------------------------------------------------

func trackerRecords(n int) []*kgo.Record {
	return records("messages.v1", 0, 0, n)
}

// TestOffsetTrackerAdvancesOnlyOnTheContiguousPrefix is the mark logic on
// its own, with no goroutines in the way.
func TestOffsetTrackerAdvancesOnlyOnTheContiguousPrefix(t *testing.T) {
	recs := trackerRecords(5)
	tr := newOffsetTracker(16)
	for _, r := range recs {
		if !tr.reserve(nil, nil) {
			t.Fatal("reserve failed with a free window")
		}
		tr.begin(r)
	}
	if got := tr.mark(); got != 0 {
		t.Fatalf("mark = %d before anything completed, want the first offset 0", got)
	}
	// Complete out of order: 4, 3, 2, 1 must all be held by 0.
	for _, off := range []int64{4, 3, 2, 1} {
		if last := tr.complete(off); last != nil {
			t.Fatalf("completing %d advanced the mark to %d while 0 is outstanding", off, last.Offset)
		}
		if got := tr.mark(); got != 0 {
			t.Fatalf("mark = %d after completing %d, want 0", got, off)
		}
	}
	last := tr.complete(0)
	if last == nil {
		t.Fatal("completing the head did not advance the mark")
	}
	if last.Offset != 4 {
		t.Fatalf("mark advanced to record %d, want 4 (the whole prefix)", last.Offset)
	}
	if got := tr.mark(); got != 5 {
		t.Fatalf("mark = %d, want 5", got)
	}
	if got := tr.pending(); got != 0 {
		t.Fatalf("%d records still pending, want 0", got)
	}
}

// TestOffsetTrackerHandlesOffsetGaps: contiguity is by dispatch position,
// not by offset arithmetic, so a gap in the log (a transaction marker, a
// compacted topic) must not wedge the mark forever.
func TestOffsetTrackerHandlesOffsetGaps(t *testing.T) {
	offsets := []int64{10, 11, 17, 18, 40}
	tr := newOffsetTracker(16)
	for _, off := range offsets {
		if !tr.reserve(nil, nil) {
			t.Fatal("reserve failed")
		}
		tr.begin(&kgo.Record{Topic: "messages.v1", Offset: off})
	}
	if got := tr.mark(); got != 10 {
		t.Fatalf("mark = %d, want the first dispatched offset 10", got)
	}
	for _, off := range offsets[1:] {
		tr.complete(off)
	}
	if got := tr.mark(); got != 10 {
		t.Fatalf("mark = %d, want 10 while the head is outstanding", got)
	}
	last := tr.complete(10)
	if last == nil || last.Offset != 40 {
		t.Fatalf("mark did not reach the end of a gapped prefix: %v", last)
	}
	if got := tr.mark(); got != 41 {
		t.Fatalf("mark = %d, want 41", got)
	}
}

// TestOffsetTrackerReserveIsBoundedAndReleases pins the backpressure: the
// window fills, reserve blocks, and completing the head frees exactly the
// slots that left the prefix.
func TestOffsetTrackerReserveIsBoundedAndReleases(t *testing.T) {
	const window = 4
	tr := newOffsetTracker(window)
	recs := trackerRecords(window)
	for _, r := range recs {
		if !tr.reserve(nil, nil) {
			t.Fatal("reserve failed inside the window")
		}
		tr.begin(r)
	}

	quit := make(chan struct{})
	blocked := make(chan bool, 1)
	go func() { blocked <- tr.reserve(quit, nil) }()
	select {
	case <-blocked:
		t.Fatal("reserve succeeded past the in-flight window")
	case <-time.After(100 * time.Millisecond):
	}
	// A teardown must be able to unblock it: no goroutine may be stuck
	// here across a revoke.
	close(quit)
	select {
	case ok := <-blocked:
		if ok {
			t.Fatal("reserve reported success after being cancelled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reserve did not observe the quit signal")
	}

	// Completing the whole prefix frees every slot.
	for i := window - 1; i >= 0; i-- {
		tr.complete(int64(i))
	}
	for i := 0; i < window; i++ {
		if !tr.reserve(nil, nil) {
			t.Fatal("reserve failed after the window drained")
		}
	}
}

// ---------------------------------------------------------------------
// backwards compatibility
// ---------------------------------------------------------------------

// TestDefaultConfigIsNotKeyConcurrent is the compatibility pin: with the
// knob unset the dispatcher runs the old serial-per-partition code, not
// the new one.
func TestDefaultConfigIsNotKeyConcurrent(t *testing.T) {
	t.Setenv(EnvConsumerKeyConcurrency, "")
	cfg, err := DefaultConsumerConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.normalise()
	if cfg.KeyConcurrency != 1 {
		t.Fatalf("default KeyConcurrency = %d, want 1: the feature must be opt-in", cfg.KeyConcurrency)
	}

	d := newDispatcher(context.Background(), newFakeClient(),
		func(context.Context, *kgo.Record) error { return nil }, "g", testConfig())
	defer d.shutdown()
	if d.keyed() {
		t.Fatal("the default configuration took the key-concurrent path")
	}
}

// TestDefaultPathIsByteIdenticalBehaviour drives the default configuration
// through the dispatcher and asserts the pre-change observable behaviour
// exactly: one goroutine per partition, one record at a time, in order,
// with a commit mark set per record and no offset tracker at all.
func TestDefaultPathIsByteIdenticalBehaviour(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic = "messages.v1"
		total = 60
	)
	tp := topicPartition{topic, 0}

	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		marks    []int64
		mu       sync.Mutex
		done     sync.WaitGroup
	)
	done.Add(total)

	cl := newFakeClient()
	handler := func(_ context.Context, rec *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		mu.Lock()
		marks = append(marks, cl.markOf(tp))
		mu.Unlock()
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	d := newDispatcher(context.Background(), cl, handler, "g", testConfig())
	defer d.shutdown()
	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: keyedRecords(topic, 0, 0, keys),
	}))
	waitGroup(t, &done, 20*time.Second)

	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent handlers on one partition = %d, want 1", got)
	}
	if w := d.worker(tp); w.track.Load() != nil {
		t.Fatal("the default path allocated an offset tracker; it must run the original code")
	}
	// The mark visible to record n is n: per-record marking, exactly as
	// before this change.
	mu.Lock()
	defer mu.Unlock()
	for i, m := range marks {
		if m != int64(i) {
			t.Fatalf("record %d saw commit mark %d, want %d: marking is no longer per record", i, m, i)
		}
	}
	waitFor(t, 10*time.Second, func() bool { return cl.markOf(tp) == total })
}

// TestKeyConcurrencyOfOneIsTheSerialPath: the knob set explicitly to 1 is
// the same code as the knob unset, not a one-wide fan-out.
func TestKeyConcurrencyOfOneIsTheSerialPath(t *testing.T) {
	d := newDispatcher(context.Background(), newFakeClient(),
		func(context.Context, *kgo.Record) error { return nil }, "g", keyConcurrentConfig(1))
	defer d.shutdown()
	if d.keyed() {
		t.Fatal("KeyConcurrency=1 took the key-concurrent path")
	}
}

// TestClientOptionsAreUnchangedByKeyConcurrency: the knob is entirely
// consumer-side. It must not alter a single franz-go option, so a
// deployment that turns it on changes its own processing and nothing about
// how it talks to the broker.
func TestClientOptionsAreUnchangedByKeyConcurrency(t *testing.T) {
	build := func(opts ...Option) []kgo.Opt {
		cfg, err := DefaultConsumerConfig()
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range opts {
			o(&cfg)
		}
		cfg.normalise()
		c := &Consumer{cfg: cfg}
		return c.clientOptions([]string{"broker:9092"}, "moderation", []string{"messages.v1"}, nil)
	}
	base := build()
	keyed := build(WithKeyConcurrency(16), WithKeyQueueDepth(8))
	if len(base) != len(keyed) {
		t.Fatalf("client options: %d without key concurrency, %d with it", len(base), len(keyed))
	}
	// kgo.Opt values are closures, so they cannot be compared directly.
	// What can be pinned is that the two knobs never reach the client
	// builder at all: turning them on must not add, drop or reorder an
	// option, and the same must hold with every other tunable moved too.
	moved := build(WithKeyConcurrency(16), WithCommitRecords(7), WithQueueDepth(9))
	if len(moved) != len(base) {
		t.Fatalf("client options: %d baseline, %d with consumer-side tunables changed", len(base), len(moved))
	}
}

// ---------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------

func TestKeyConcurrencyNormalisation(t *testing.T) {
	cases := []struct {
		name              string
		partition, key    int
		wantKey, wantPart int
	}{
		{"default", DefaultPartitionConcurrency, 1, 1, DefaultPartitionConcurrency},
		{"opt in", 64, 8, 8, 64},
		{"zero is one", 64, 0, 1, 64},
		{"negative is one", 64, -5, 1, 64},
		{"clamped", 64, MaxKeyConcurrency * 10, MaxKeyConcurrency, 64},
		// A serial instance stays serial: the explicit ceiling of one wins
		// over a fan-out that could not use it anyway.
		{"serial instance subsumes the fan-out", 1, 32, 1, 1},
		// Unlimited handlers plus a fan-out is the intended fast config.
		{"unlimited ceiling keeps the fan-out", 0, 32, 32, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ConsumerConfig{PartitionConcurrency: tc.partition, KeyConcurrency: tc.key}
			cfg.normalise()
			if cfg.KeyConcurrency != tc.wantKey {
				t.Errorf("KeyConcurrency = %d, want %d", cfg.KeyConcurrency, tc.wantKey)
			}
			if cfg.PartitionConcurrency != tc.wantPart {
				t.Errorf("PartitionConcurrency = %d, want %d", cfg.PartitionConcurrency, tc.wantPart)
			}
			if cfg.KeyQueueDepth < 1 {
				t.Errorf("KeyQueueDepth = %d, want at least 1", cfg.KeyQueueDepth)
			}
		})
	}
}
