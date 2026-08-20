package kafkax

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// The tests in this file drive the dispatcher — the ordering, commit,
// rebalance and error logic — through a fake groupClient. No broker, no
// container, no network: everything asserted here is a property of this
// package's own code, so a failure names the bug rather than the
// environment. consumer_kfake_test.go then checks the same properties end
// to end against franz-go's in-process Kafka.

// fakeClient records what the dispatcher marks and commits. Marks are what
// the dispatcher considers safe to commit; commits are snapshots of the
// marks at the moment the commit was issued.
type fakeClient struct {
	mu        sync.Mutex
	marks     map[topicPartition]int64 // next offset to commit
	committed map[topicPartition]int64 // last successfully committed
	commits   int
	err       error
	onCommit  func()
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		marks:     make(map[topicPartition]int64),
		committed: make(map[topicPartition]int64),
	}
}

func (f *fakeClient) MarkCommitRecords(rs ...*kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rs {
		tp := topicPartition{r.Topic, r.Partition}
		if next := r.Offset + 1; next > f.marks[tp] {
			f.marks[tp] = next
		}
	}
}

func (f *fakeClient) CommitMarkedOffsets(context.Context) error {
	f.mu.Lock()
	if f.err != nil {
		err := f.err
		f.mu.Unlock()
		return err
	}
	f.commits++
	for tp, next := range f.marks {
		f.committed[tp] = next
	}
	cb := f.onCommit
	f.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

func (f *fakeClient) markOf(tp topicPartition) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.marks[tp]
}

func (f *fakeClient) committedOf(tp topicPartition) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committed[tp]
}

func (f *fakeClient) commitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(opts ...Option) ConsumerConfig {
	cfg := ConsumerConfig{
		PartitionConcurrency: DefaultPartitionConcurrency,
		QueueDepth:           DefaultQueueDepth,
		MaxPollRecords:       DefaultMaxPollRecords,
		DrainTimeout:         5 * time.Second,
		CommitInterval:       time.Hour, // count-triggered unless a test says otherwise
		CommitRecords:        0,
		Logger:               discardLogger(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	cfg.normalise()
	return cfg
}

// records builds n consecutive records for one partition starting at start.
func records(topic string, partition int32, start int64, n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Offset:    start + int64(i),
			Value:     []byte(fmt.Sprintf("%d", start+int64(i))),
		}
	}
	return out
}

// fetchOf assembles a kgo.Fetches the way a poll would return one.
func fetchOf(topic string, byPartition map[int32][]*kgo.Record) kgo.Fetches {
	parts := make([]kgo.FetchPartition, 0, len(byPartition))
	ps := make([]int32, 0, len(byPartition))
	for p := range byPartition {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	for _, p := range ps {
		parts = append(parts, kgo.FetchPartition{Partition: p, Records: byPartition[p]})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: topic, Partitions: parts}}}}
}

// TestInPartitionOrderingUnderConcurrency is the load-bearing test: every
// partition's records must be handled strictly in offset order even though
// partitions run concurrently, because §7.3's no-locking guarantee rests on
// exactly that.
//
// Parallelism is proven rather than assumed: the first record of every
// partition parks on a gate that only opens once all partitions have
// arrived. A serial consumer can never open it, so a regression to
// one-goroutine-per-instance fails this test by timing out rather than by
// passing quietly.
func TestInPartitionOrderingUnderConcurrency(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic       = "messages.v1"
		nPartitions = 8
		perFetch    = 10
		nFetches    = 5
		total       = perFetch * nFetches
	)

	var (
		mu      sync.Mutex
		seen    = make(map[int32][]int64)
		gate    = make(chan struct{})
		arrived atomic.Int64
		firsts  [nPartitions]sync.Once
		stalled atomic.Bool

		inFlight atomic.Int64
		maxSeen  atomic.Int64

		done sync.WaitGroup
	)
	done.Add(nPartitions * total)

	handler := func(_ context.Context, rec *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		firsts[rec.Partition].Do(func() {
			if arrived.Add(1) == nPartitions {
				close(gate)
			}
			select {
			case <-gate:
			case <-time.After(10 * time.Second):
				// Only reachable if partitions are not actually running
				// concurrently.
				stalled.Store(true)
			}
		})
		mu.Lock()
		seen[rec.Partition] = append(seen[rec.Partition], rec.Offset)
		mu.Unlock()
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", testConfig())
	defer d.shutdown()

	for f := 0; f < nFetches; f++ {
		byPartition := make(map[int32][]*kgo.Record, nPartitions)
		for p := int32(0); p < nPartitions; p++ {
			byPartition[p] = records(topic, p, int64(f*perFetch), perFetch)
		}
		d.dispatch(fetchOf(topic, byPartition))
	}
	waitGroup(t, &done, 15*time.Second)

	if stalled.Load() {
		t.Fatal("partitions did not run concurrently: the cross-partition gate timed out")
	}
	if got := maxSeen.Load(); got < nPartitions {
		t.Fatalf("max concurrent handlers = %d, want %d: partitions are not overlapping in time", got, nPartitions)
	}

	mu.Lock()
	defer mu.Unlock()
	for p := int32(0); p < nPartitions; p++ {
		got := seen[p]
		if len(got) != total {
			t.Fatalf("partition %d: handled %d records, want %d", p, len(got), total)
		}
		for i, off := range got {
			if off != int64(i) {
				t.Fatalf("partition %d: record %d handled out of order: offset %d, want %d", p, i, off, i)
			}
		}
	}
	// Marks are set just after the handler returns, so poll rather than
	// assume the last one landed before the WaitGroup did.
	for p := int32(0); p < nPartitions; p++ {
		tp := topicPartition{topic, p}
		waitFor(t, 5*time.Second, func() bool { return cl.markOf(tp) == total })
	}
}

// TestPartitionConcurrencyIsBounded checks the configurable ceiling: with a
// limit of 2, no more than 2 handlers may run at once however many
// partitions are assigned.
func TestPartitionConcurrencyIsBounded(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic       = "messages.v1"
		nPartitions = 8
		perPart     = 20
		limit       = 2
	)
	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		done     sync.WaitGroup
	)
	done.Add(nPartitions * perPart)

	handler := func(context.Context, *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	d := newDispatcher(context.Background(), newFakeClient(), handler, "g",
		testConfig(WithPartitionConcurrency(limit)))
	defer d.shutdown()

	byPartition := make(map[int32][]*kgo.Record, nPartitions)
	for p := int32(0); p < nPartitions; p++ {
		byPartition[p] = records(topic, p, 0, perPart)
	}
	d.dispatch(fetchOf(topic, byPartition))
	waitGroup(t, &done, 15*time.Second)

	if got := maxSeen.Load(); got > limit {
		t.Fatalf("max concurrent handlers = %d, want at most %d", got, limit)
	}
}

// TestSerialConcurrencyStillOrdered pins the compatibility escape hatch:
// KAFKA_CONSUMER_PARTITION_CONCURRENCY=1 reproduces the old one-at-a-time
// behaviour for a service that ever needs it.
func TestSerialConcurrencyStillOrdered(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	var (
		inFlight atomic.Int64
		maxSeen  atomic.Int64
		done     sync.WaitGroup
	)
	done.Add(40)

	handler := func(context.Context, *kgo.Record) error {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(100 * time.Microsecond)
		inFlight.Add(-1)
		done.Done()
		return nil
	}

	d := newDispatcher(context.Background(), newFakeClient(), handler, "g",
		testConfig(WithPartitionConcurrency(1)))
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: records(topic, 0, 0, 20),
		1: records(topic, 1, 0, 20),
	}))
	waitGroup(t, &done, 15*time.Second)

	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent handlers = %d, want 1", got)
	}
}

// TestHandlerErrorDoesNotAdvanceOffset is the at-least-once invariant: the
// offset must never move past a record whose handler failed, and the run
// must surface that error to the caller.
func TestHandlerErrorDoesNotAdvanceOffset(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic    = "messages.v1"
		failAt   = int64(5)
		nRecords = 10
	)
	boom := errors.New("handler exploded")
	var handled atomic.Int64

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == failAt {
			return boom
		}
		handled.Add(1)
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", testConfig())
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{0: records(topic, 0, 0, nRecords)}))
	waitClosed(t, d.stop, 5*time.Second)

	if err := d.failure(); !errors.Is(err, boom) {
		t.Fatalf("failure() = %v, want %v", err, boom)
	}
	tp := topicPartition{topic, 0}
	if mark := cl.markOf(tp); mark != failAt {
		t.Fatalf("marked offset %d, want %d: the offset must not advance past the failed record", mark, failAt)
	}
	// A commit now — the one Run issues on the way out — must land on the
	// failed record, so it is what gets redelivered.
	if err := cl.CommitMarkedOffsets(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := cl.committedOf(tp); got != failAt {
		t.Fatalf("committed %d, want %d (record %d redelivered)", got, failAt, failAt)
	}
	if got := handled.Load(); got != failAt {
		t.Fatalf("handled %d records before the failure, want %d", got, failAt)
	}
}

// TestCommitGranularityIsPartialProgress is finding F6: a commit must be
// able to land in the middle of a fetch, so that a crash re-delivers the
// uncommitted tail and not the whole batch.
func TestCommitGranularityIsPartialProgress(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic    = "messages.v1"
		nRecords = 40
		every    = 5
		stallAt  = int64(12)
	)
	tp := topicPartition{topic, 0}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	// Unblock before shutdown whatever happens, so a failed assertion
	// reports itself instead of deadlocking the suite.
	defer unblock()

	reached := make(chan struct{})
	var once sync.Once
	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == stallAt {
			once.Do(func() { close(reached) })
			<-release
		}
		return nil
	}

	cl := newFakeClient()
	// Commit on the record trigger only, so the assertion is about
	// granularity and not about a timer firing.
	d := newDispatcher(context.Background(), cl, handler, "g",
		testConfig(WithCommitRecords(every), WithCommitInterval(time.Hour)))
	ctx, cancel := context.WithCancel(context.Background())
	d.cancelRun = cancel
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runCommitter(ctx)
	}()
	defer func() {
		cancel()
		unblock()
		d.shutdown()
	}()

	// One fetch. Under the old code nothing at all would be committed
	// until all 40 records were done.
	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{0: records(topic, 0, 0, nRecords)}))
	waitClosed(t, reached, 5*time.Second)

	// The committer runs asynchronously; give it a moment to catch up to
	// the marks made before the stall.
	waitFor(t, 5*time.Second, func() bool { return cl.committedOf(tp) >= every })

	committed := cl.committedOf(tp)
	if committed == 0 {
		t.Fatal("nothing committed mid-fetch: commit granularity is still a whole fetch")
	}
	if committed > stallAt {
		t.Fatalf("committed %d, past the record still in flight at %d", committed, stallAt)
	}
	// A crash here re-delivers only [committed, 40) — the uncommitted tail
	// — rather than the whole fetch, which is the whole of finding F6.
	if redelivered := nRecords - committed; redelivered >= nRecords {
		t.Fatalf("a crash would re-deliver %d of %d records: no partial progress was committed", redelivered, nRecords)
	}

	unblock()
	waitFor(t, 5*time.Second, func() bool { return cl.markOf(tp) == nRecords })
}

// TestRevokeStopsWorkersCleanly covers the rebalance path: a revoked
// partition stops after the record it is mid-way through, never commits
// the records it had queued but not started, leaves no goroutine behind,
// and does not disturb the partitions that were not revoked.
func TestRevokeStopsWorkersCleanly(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	revoked := topicPartition{topic, 0}
	kept := topicPartition{topic, 1}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	reached := make(chan struct{})
	var once sync.Once
	var keptDone sync.WaitGroup
	keptDone.Add(10)

	handler := func(_ context.Context, rec *kgo.Record) error {
		if rec.Partition == 0 {
			if rec.Offset == 0 {
				once.Do(func() { close(reached) })
				<-release
				return nil
			}
			t.Errorf("revoked partition handled offset %d after the revoke", rec.Offset)
			return nil
		}
		keptDone.Done()
		return nil
	}

	cl := newFakeClient()
	var forgotten []topicPartition
	var forgetMu sync.Mutex
	d := newDispatcher(context.Background(), cl, handler, "g", testConfig())
	d.onForget = func(tp topicPartition) {
		forgetMu.Lock()
		forgotten = append(forgotten, tp)
		forgetMu.Unlock()
	}
	defer d.shutdown()

	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{
		0: records(topic, 0, 0, 10),
		1: records(topic, 1, 0, 10),
	}))
	waitClosed(t, reached, 5*time.Second)

	w := d.worker(revoked)
	revokeDone := make(chan struct{})
	go func() {
		defer close(revokeDone)
		d.releasePartitions(map[string][]int32{topic: {0}})
	}()
	waitClosed(t, w.quit, 5*time.Second)

	// The revoke must be waiting for the in-flight record, not tearing it
	// down underneath the handler.
	select {
	case <-revokeDone:
		t.Fatal("revoke returned while a handler was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	waitClosed(t, revokeDone, 5*time.Second)

	if mark := cl.markOf(revoked); mark != 1 {
		t.Fatalf("revoked partition marked %d, want 1: only the completed record may be committed", mark)
	}
	d.mu.Lock()
	_, stillThere := d.workers[revoked]
	d.mu.Unlock()
	if stillThere {
		t.Fatal("revoked partition still has a worker")
	}

	forgetMu.Lock()
	got := append([]topicPartition(nil), forgotten...)
	forgetMu.Unlock()
	if len(got) != 1 || got[0] != revoked {
		t.Fatalf("forgotten lag series = %v, want exactly %v", got, revoked)
	}

	// The partition that was not revoked keeps working.
	waitGroup(t, &keptDone, 5*time.Second)
	waitFor(t, 5*time.Second, func() bool { return cl.markOf(kept) == 10 })
}

// TestRevokeDropsQueuedRecordsUncommitted checks the other half of the
// revoke contract: batches queued behind the revoke are dropped without
// being handled, so the member taking the partition over re-reads them.
func TestRevokeDropsQueuedRecordsUncommitted(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	tp := topicPartition{topic, 0}

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	reached := make(chan struct{})
	var once sync.Once
	handler := func(_ context.Context, rec *kgo.Record) error {
		once.Do(func() { close(reached) })
		<-release
		return nil
	}

	cl := newFakeClient()
	d := newDispatcher(context.Background(), cl, handler, "g", testConfig())
	defer d.shutdown()

	// First batch enters the handler; the second sits in the queue.
	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{0: records(topic, 0, 0, 1)}))
	waitClosed(t, reached, 5*time.Second)
	d.dispatch(fetchOf(topic, map[int32][]*kgo.Record{0: records(topic, 0, 1, 50)}))

	w := d.worker(tp)
	revokeDone := make(chan struct{})
	go func() {
		defer close(revokeDone)
		d.releasePartitions(map[string][]int32{topic: {0}})
	}()
	// Only release the in-flight record once the revoke has actually
	// signalled the worker, so the assertion is about the revoke and not
	// about a race with it.
	waitClosed(t, w.quit, 5*time.Second)
	unblock()
	waitClosed(t, revokeDone, 5*time.Second)

	if mark := cl.markOf(tp); mark != 1 {
		t.Fatalf("marked %d, want 1: queued-but-unhandled records must not be committed", mark)
	}
}

// TestShutdownWaitsForEveryGoroutine is the explicit no-leak accounting to
// go with goleak: after shutdown every worker has closed its done channel.
func TestShutdownWaitsForEveryGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	var done sync.WaitGroup
	done.Add(40)
	handler := func(context.Context, *kgo.Record) error {
		done.Done()
		return nil
	}
	d := newDispatcher(context.Background(), newFakeClient(), handler, "g", testConfig())

	byPartition := make(map[int32][]*kgo.Record, 4)
	for p := int32(0); p < 4; p++ {
		byPartition[p] = records(topic, p, 0, 10)
	}
	d.dispatch(fetchOf(topic, byPartition))
	waitGroup(t, &done, 5*time.Second)

	d.mu.Lock()
	ws := make([]*partitionWorker, 0, len(d.workers))
	for _, w := range d.workers {
		ws = append(ws, w)
	}
	d.mu.Unlock()
	if len(ws) != 4 {
		t.Fatalf("workers = %d, want 4 (one per assigned partition)", len(ws))
	}

	d.shutdown()
	for _, w := range ws {
		select {
		case <-w.done:
		default:
			t.Fatalf("worker for %v was still running after shutdown", w.tp)
		}
	}
	d.mu.Lock()
	left := len(d.workers)
	d.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d workers left registered after shutdown", left)
	}
}

// TestAssignPreStartsWorkers checks the assign callback path, which is what
// makes lag observable for a partition before its first record arrives.
func TestAssignPreStartsWorkers(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	d := newDispatcher(context.Background(), newFakeClient(),
		func(context.Context, *kgo.Record) error { return nil }, "g", testConfig())
	defer d.shutdown()

	d.assign(map[string][]int32{"messages.v1": {0, 1, 2}})
	d.mu.Lock()
	n := len(d.workers)
	d.mu.Unlock()
	if n != 3 {
		t.Fatalf("workers = %d, want 3", n)
	}
	// Nothing consumed yet, so no partition has a position to report.
	if pos := d.positions(); len(pos) != 0 {
		t.Fatalf("positions = %v, want none before any record is seen", pos)
	}
}

// waitGroup waits for wg with a deadline, so a broken concurrency change
// fails the test instead of hanging the suite.
func waitGroup(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitClosed(t, done, d)
}

func waitClosed[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for progress", d)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}
