package kafkax

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// These tests drive the whole Consumer — franz-go client, group protocol,
// offset commits and all — against kfake, the in-process Kafka franz-go
// ships. It was chosen over a testcontainer because the requirement is
// "no Docker and no network": kfake speaks the real wire protocol over
// loopback, in the test binary, in milliseconds, so it can run under -race
// on every commit. The dispatcher's own guarantees are pinned without any
// broker at all in dispatch_test.go; what kfake adds here is that the
// wiring around them — assignment, commit, redelivery on restart — is real.

func newFakeCluster(t *testing.T, partitions int32, topics ...string) []string {
	t.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(partitions, topics...),
	)
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// produceTo writes n records to an explicit partition, so the tests can
// reason about partitions without reimplementing the partitioner.
func produceTo(t *testing.T, addrs []string, topic string, partition int32, n int) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(fmt.Sprintf("k%d", i)),
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// produceKeyed writes one record per key to an explicit partition, so a
// test can decide exactly which per-key worker each offset lands on.
func produceKeyed(t *testing.T, addrs []string, topic string, partition int32, keys []string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(keys))
	for i, k := range keys {
		recs[i] = &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(k),
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func fetchedOffsets(t *testing.T, addrs []string, group string) kadm.OffsetResponses {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	os, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	return os
}

// orderRecorder asserts in-partition ordering as records arrive, so a
// violation is reported at the record that broke it.
type orderRecorder struct {
	mu   sync.Mutex
	seen map[int32][]int64
	bad  []string
}

func newOrderRecorder() *orderRecorder {
	return &orderRecorder{seen: make(map[int32][]int64)}
}

func (o *orderRecorder) record(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	prev := o.seen[rec.Partition]
	if n := len(prev); n > 0 && rec.Offset <= prev[n-1] {
		o.bad = append(o.bad, fmt.Sprintf("partition %d: offset %d arrived after %d",
			rec.Partition, rec.Offset, prev[n-1]))
	}
	o.seen[rec.Partition] = append(o.seen[rec.Partition], rec.Offset)
}

func (o *orderRecorder) check(t *testing.T, partitions int32, want int) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, b := range o.bad {
		t.Error(b)
	}
	for p := int32(0); p < partitions; p++ {
		got := o.seen[p]
		if len(got) != want {
			t.Errorf("partition %d: %d records, want %d", p, len(got), want)
			continue
		}
		for i, off := range got {
			if off != int64(i) {
				t.Errorf("partition %d: position %d holds offset %d", p, i, off)
				break
			}
		}
	}
}

// TestConsumerEndToEnd is the whole feature against a real protocol: every
// record consumed, every partition in order, offsets committed, and §4.5's
// lag gauge populated per topic/partition/group.
func TestConsumerEndToEnd(t *testing.T) {
	const (
		topic      = "messages.v1"
		group      = "moderation"
		partitions = 4
		perPart    = 100
		total      = partitions * perPart
	)
	addrs := newFakeCluster(t, partitions, topic)
	for p := int32(0); p < partitions; p++ {
		produceTo(t, addrs, topic, p, perPart)
	}

	order := newOrderRecorder()
	var got atomic.Int64
	done := make(chan struct{})
	var once sync.Once

	gauge := newFakeGauge()
	c, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			order.record(rec)
			if got.Add(1) == total {
				once.Do(func() { close(done) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagGauge(gauge),
		WithLagSampling(20*time.Millisecond),
		// Interval only: this run proves the periodic commit path on its
		// own, with no record trigger to reach the same offsets first.
		WithCommitInterval(MinCommitInterval),
		WithCommitRecords(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	waitClosed(t, done, 60*time.Second)
	// A lag sample must land, per partition, and read zero now that the
	// backlog is drained. This is finding F1: the family exists and has
	// values.
	waitFor(t, 30*time.Second, func() bool {
		for p := int32(0); p < partitions; p++ {
			v, ok := gauge.get(lagKey{topic, p, group})
			if !ok || v != 0 {
				return false
			}
		}
		return true
	})

	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}

	order.check(t, partitions, perPart)

	// Stopping the member drops its series rather than leaving a frozen
	// value behind for an alert to read.
	if n := gauge.size(); n != 0 {
		t.Errorf("%d lag series survived shutdown, want 0", n)
	}
	if len(gauge.forgotten()) < partitions {
		t.Errorf("forgot %d series on shutdown, want %d", len(gauge.forgotten()), partitions)
	}

	os := fetchedOffsets(t, addrs, group)
	for p := int32(0); p < partitions; p++ {
		o, ok := os.Lookup(topic, p)
		if !ok || o.Err != nil {
			t.Errorf("partition %d: no committed offset (%v)", p, o.Err)
			continue
		}
		if o.At != perPart {
			t.Errorf("partition %d: committed offset %d, want %d", p, o.At, perPart)
		}
	}
}

// TestConsumerRedeliversOnlyTheUncommittedTail is F6 and at-least-once
// together: a handler failure part way through a backlog must commit the
// progress that succeeded, must not commit the record that failed, and a
// fresh member must resume at exactly that record.
func TestConsumerRedeliversOnlyTheUncommittedTail(t *testing.T) {
	const (
		topic  = "messages.v1"
		group  = "moderation"
		total  = 40
		failAt = int64(20)
	)
	addrs := newFakeCluster(t, 1, topic)
	produceTo(t, addrs, topic, 0, total)

	boom := errors.New("handler exploded")
	first, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			if rec.Offset >= failAt {
				return boom
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithCommitInterval(20*time.Millisecond),
		WithCommitRecords(5),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := first.Run(ctx); !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want %v", err, boom)
	}
	first.Close()

	os := fetchedOffsets(t, addrs, group)
	o, ok := os.Lookup(topic, 0)
	if !ok || o.Err != nil {
		t.Fatalf("no committed offset after the failure (%v)", o.Err)
	}
	if o.At > failAt {
		t.Fatalf("committed offset %d is past the failed record %d: at-least-once is broken", o.At, failAt)
	}
	if o.At == 0 {
		t.Fatal("nothing was committed: the whole backlog would be re-processed")
	}
	committed := o.At

	// A fresh member — a restarted process — resumes at the commit, so it
	// re-delivers the tail and nothing before it.
	order := newOrderRecorder()
	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	second, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			order.record(rec)
			if seen.Add(1) == total-committed {
				once.Do(func() { close(done) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithCommitInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- second.Run(ctx2) }()
	waitClosed(t, done, 60*time.Second)
	cancel2()
	select {
	case <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("second Run did not return")
	}

	order.mu.Lock()
	got := append([]int64(nil), order.seen[0]...)
	order.mu.Unlock()
	if len(got) == 0 {
		t.Fatal("the restarted member received nothing")
	}
	if got[0] != committed {
		t.Fatalf("resumed at offset %d, want the committed offset %d", got[0], committed)
	}
	for i, off := range got {
		if off != committed+int64(i) {
			t.Fatalf("re-delivery out of order at position %d: offset %d", i, off)
		}
	}
}

// TestConsumerLagSamplingFailureDoesNotStopConsumption is the P2
// requirement end to end: the lag sampler's broker calls fail continuously
// and the records keep flowing.
func TestConsumerLagSamplingFailureDoesNotStopConsumption(t *testing.T) {
	const (
		topic = "messages.v1"
		group = "moderation"
		total = 50
	)
	addrs := newFakeCluster(t, 1, topic)
	produceTo(t, addrs, topic, 0, total)

	gauge := newFakeGauge()
	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	c, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			if seen.Add(1) == total {
				once.Do(func() { close(done) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagGauge(gauge),
		WithLagSampling(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Break the sampler's view of the broker without breaking the
	// consumer's.
	broken := &fakeLister{}
	broken.setErr(errors.New("broker unreachable"))
	c.adm = broken

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	waitClosed(t, done, 60*time.Second)
	// Wait for the sampler to have actually failed, so the assertion below
	// is about a failure that happened rather than one that might have.
	waitFor(t, 60*time.Second, func() bool {
		s := c.sampler.Load()
		return s != nil && s.failures.Load() > 0
	})

	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v; a failed lag sample must not surface as a consumer error", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return")
	}

	s := c.sampler.Load()
	if s == nil {
		t.Fatal("no lag sampler was started")
	}
	if s.samples.Load() != 0 {
		t.Fatalf("%d samples succeeded against a broken lister", s.samples.Load())
	}
	if gauge.size() != 0 {
		t.Fatalf("gauge has %d series from failed samples, want 0", gauge.size())
	}
	if got := seen.Load(); got != total {
		t.Fatalf("consumed %d of %d records while lag sampling was failing", got, total)
	}
}

// TestConsumerLagGaugeReportsBacklog checks the gauge against a real
// watermark while a backlog exists, which is the state §4.7 wants alerted
// on.
func TestConsumerLagGaugeReportsBacklog(t *testing.T) {
	const (
		topic = "messages.v1"
		group = "moderation"
		total = 200
	)
	addrs := newFakeCluster(t, 1, topic)
	produceTo(t, addrs, topic, 0, total)

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	stalled := make(chan struct{})
	var once sync.Once

	gauge := newFakeGauge()
	c, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			if rec.Offset == 10 {
				once.Do(func() { close(stalled) })
				<-release
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagGauge(gauge),
		WithLagSampling(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	waitClosed(t, stalled, 60*time.Second)
	// Wait for the gauge to settle on the read position rather than
	// asserting on whichever sample happens to land first: the handler
	// parks at offset 10, but records 0..9 may still be finishing when the
	// sampler ticks, and a sample of 195 mid-way there is correct at the
	// moment it was taken. The claim under test — lag is the high
	// watermark minus the member's true position, not its fetch position —
	// is unchanged, and a consumer that never reaches position 10 still
	// fails here.
	want := float64(total - 10)
	waitFor(t, 30*time.Second, func() bool {
		v, ok := gauge.get(lagKey{topic, 0, group})
		return ok && v == want
	})
	v, _ := gauge.get(lagKey{topic, 0, group})
	if v != want {
		t.Fatalf("lag = %v, want %v (high watermark %d minus position 10)", v, want, total)
	}

	unblock()
	cancel()
	select {
	case <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestConsumerRunIsRestartable pins the shape moderation-service and
// credits-service rely on: Run in a loop on one Consumer, restarting after
// a transient failure.
func TestConsumerRunIsRestartable(t *testing.T) {
	const (
		topic = "messages.v1"
		group = "moderation"
	)
	addrs := newFakeCluster(t, 1, topic)
	produceTo(t, addrs, topic, 0, 10)

	boom := errors.New("transient")
	var fail atomic.Bool
	fail.Store(true)
	var seen atomic.Int64
	resumed := make(chan struct{})
	var once sync.Once

	c, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			if fail.CompareAndSwap(true, false) {
				return boom
			}
			if seen.Add(1) >= 5 {
				once.Do(func() { close(resumed) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithCommitInterval(MinCommitInterval),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, boom) {
		t.Fatalf("first Run returned %v, want %v", err, boom)
	}

	// Same Consumer, second Run: it must rejoin, keep consuming and not
	// panic, leak or wedge. Records produced after the restart are the
	// unambiguous proof it is alive — the client keeps its fetch position
	// across Run calls, so the already-fetched backlog is not necessarily
	// re-read within one process.
	ctx2, cancel2 := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx2) }()
	produceTo(t, addrs, topic, 0, 20)

	select {
	case <-resumed:
	case err := <-runErr:
		t.Fatalf("second Run returned early: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("the restarted Run consumed only %d records", seen.Load())
	}
	cancel2()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("second Run returned %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("second Run did not return")
	}
}

// TestRebalanceMovesPartitionsWithoutLossOrLeak is the rebalance path end
// to end: a second member joins, partitions move, and afterwards every
// record has been handled and every committed offset is the true end of
// the log. Nothing may be committed that was not processed, and no
// goroutine may outlive the members.
func TestRebalanceMovesPartitionsWithoutLossOrLeak(t *testing.T) {
	// The fake broker is torn down by t.Cleanup, after this check runs, so
	// its own goroutines are excluded. Everything else new must be ours.
	leaks := []goleak.Option{
		goleak.IgnoreCurrent(),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).run"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*broker).listen"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*group).manage"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*clientConn).read"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*clientConn).write"),
	}

	const (
		topic      = "messages.v1"
		group      = "moderation"
		partitions = 4
		perPart    = 60
		total      = partitions * perPart
	)
	addrs := newFakeCluster(t, partitions, topic)
	for p := int32(0); p < partitions; p++ {
		produceTo(t, addrs, topic, p, perPart)
	}

	var mu sync.Mutex
	seen := make(map[topicPartition]map[int64]bool)
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, offs := range seen {
			n += len(offs)
		}
		return n
	}
	handler := func(_ context.Context, rec *kgo.Record) error {
		tp := topicPartition{rec.Topic, rec.Partition}
		mu.Lock()
		if seen[tp] == nil {
			seen[tp] = make(map[int64]bool)
		}
		seen[tp][rec.Offset] = true
		mu.Unlock()
		// Slow enough that the second member joins mid-backlog.
		time.Sleep(time.Millisecond)
		return nil
	}

	newMember := func() *Consumer {
		c, err := NewConsumer(addrs, group, []string{topic}, handler,
			WithLogger(discardLogger()),
			WithLagSampling(0),
			WithCommitInterval(MinCommitInterval),
			WithCommitRecords(10),
		)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	ctx, cancel := context.WithCancel(context.Background())
	a, b := newMember(), newMember()
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- a.Run(ctx) }()

	// Let the first member take everything, then force a rebalance.
	waitFor(t, 60*time.Second, func() bool { return count() > 0 })
	go func() { errB <- b.Run(ctx) }()

	waitFor(t, 120*time.Second, func() bool { return count() == total })
	cancel()

	for _, ch := range []chan error{errA, errB} {
		select {
		case err := <-ch:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("a member did not stop")
		}
	}
	a.Close()
	b.Close()

	mu.Lock()
	for p := int32(0); p < partitions; p++ {
		if got := len(seen[topicPartition{topic, p}]); got != perPart {
			t.Errorf("partition %d: %d distinct records handled, want %d", p, got, perPart)
		}
	}
	mu.Unlock()

	os := fetchedOffsets(t, addrs, group)
	for p := int32(0); p < partitions; p++ {
		o, ok := os.Lookup(topic, p)
		if !ok || o.Err != nil {
			t.Errorf("partition %d: no committed offset (%v)", p, o.Err)
			continue
		}
		if o.At != perPart {
			t.Errorf("partition %d: committed %d, want %d", p, o.At, perPart)
		}
	}

	goleak.VerifyNone(t, leaks...)
}

// TestConsumerKeyConcurrencyEndToEnd runs the opt-in fan-out against the
// real wire protocol: every record consumed as many times as it was
// produced, every key strictly in offset order and never in two handlers
// at once, and the committed offset the true end of each partition once
// the backlog drains.
func TestConsumerKeyConcurrencyEndToEnd(t *testing.T) {
	const (
		topic      = "messages.v1"
		group      = "moderation"
		partitions = 2
		nKeys      = 40
		perKey     = 6
		perPart    = nKeys * perKey
		total      = partitions * perPart
		fanOut     = 8
	)
	addrs := newFakeCluster(t, partitions, topic)

	keys := make([]string, perPart)
	for i := range keys {
		keys[i] = fmt.Sprintf("hash(sd_%d, ct_%d)", i%nKeys, i%nKeys)
	}
	for p := int32(0); p < partitions; p++ {
		produceKeyed(t, addrs, topic, p, keys)
	}

	type seenKey struct {
		topic     string
		partition int32
		key       string
	}
	var (
		mu     sync.Mutex
		seen   = make(map[seenKey][]int64)
		active = make(map[seenKey]bool)
		bad    []string
		got    atomic.Int64
		done   = make(chan struct{})
		once   sync.Once
	)

	c, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			k := seenKey{rec.Topic, rec.Partition, string(rec.Key)}
			mu.Lock()
			if active[k] {
				bad = append(bad, fmt.Sprintf("key %q of partition %d entered twice at once", k.key, k.partition))
			}
			active[k] = true
			prev := seen[k]
			if n := len(prev); n > 0 && rec.Offset <= prev[n-1] {
				bad = append(bad, fmt.Sprintf("key %q of partition %d: offset %d after %d",
					k.key, k.partition, rec.Offset, prev[n-1]))
			}
			seen[k] = append(seen[k], rec.Offset)
			active[k] = false
			mu.Unlock()
			if got.Add(1) == total {
				once.Do(func() { close(done) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithKeyConcurrency(fanOut),
		WithCommitInterval(MinCommitInterval),
		WithCommitRecords(25),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	waitClosed(t, done, 60*time.Second)
	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}

	mu.Lock()
	for _, b := range bad {
		t.Error(b)
	}
	if n := len(seen); n != partitions*nKeys {
		t.Errorf("saw %d distinct (partition, key) pairs, want %d", n, partitions*nKeys)
	}
	for k, offs := range seen {
		if len(offs) != perKey {
			t.Errorf("key %q of partition %d: %d records, want %d", k.key, k.partition, len(offs), perKey)
		}
	}
	mu.Unlock()

	// Everything succeeded, so the low-water mark is the end of the log:
	// the prefix logic must not have stranded a record.
	os := fetchedOffsets(t, addrs, group)
	for p := int32(0); p < partitions; p++ {
		o, ok := os.Lookup(topic, p)
		if !ok || o.Err != nil {
			t.Errorf("partition %d: no committed offset (%v)", p, o.Err)
			continue
		}
		if o.At != perPart {
			t.Errorf("partition %d: committed offset %d, want %d", p, o.At, perPart)
		}
	}
}

// TestConsumerKeyConcurrencyCommitsTheLowWaterMark is the crash story
// against a real broker. One key's record fails only after many records
// above it have already succeeded out of order; the committed offset must
// still be the failed record, and a fresh member must resume there.
// Anything higher would be silent data loss.
func TestConsumerKeyConcurrencyCommitsTheLowWaterMark(t *testing.T) {
	const (
		topic  = "messages.v1"
		group  = "moderation"
		total  = 60
		failAt = int64(12)
		fanOut = 8
	)
	addrs := newFakeCluster(t, 1, topic)

	// The failing record owns a worker of its own; nothing else routes
	// there, so every other record is free to run ahead of it.
	blocker := keysOnDistinctWorkers(t, fanOut)[2]
	others := keysAvoidingWorker(t, fanOut, workerFor([]byte(blocker), fanOut), total-1)
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
	produceKeyed(t, addrs, topic, 0, keys)

	boom := errors.New("handler exploded")
	const wantAbove = 20
	var below, above atomic.Int64
	enough := make(chan struct{})
	var once sync.Once

	first, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			if rec.Offset == failAt {
				select {
				case <-enough:
				case <-time.After(30 * time.Second):
				}
				return boom
			}
			if rec.Offset < failAt {
				below.Add(1)
			} else {
				above.Add(1)
			}
			if below.Load() == failAt && above.Load() >= wantAbove {
				once.Do(func() { close(enough) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithKeyConcurrency(fanOut),
		WithCommitInterval(MinCommitInterval),
		WithCommitRecords(5),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := first.Run(ctx); !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want %v", err, boom)
	}
	first.Close()

	if n := above.Load(); n < wantAbove {
		t.Fatalf("only %d records above the failure completed: this is not the out-of-order case", n)
	}

	os := fetchedOffsets(t, addrs, group)
	o, ok := os.Lookup(topic, 0)
	if !ok || o.Err != nil {
		t.Fatalf("no committed offset after the failure (%v)", o.Err)
	}
	if o.At != failAt {
		t.Fatalf("committed offset %d, want the low-water mark %d: %d records above it finished but must not be committed",
			o.At, failAt, above.Load())
	}
	committed := o.At

	// The restarted member resumes at the mark and re-reads the tail
	// contiguously: nothing between the mark and the end is skipped.
	var (
		mu    sync.Mutex
		seen  = make(map[int64]bool)
		got   atomic.Int64
		done  = make(chan struct{})
		once2 sync.Once
	)
	second, err := NewConsumer(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			mu.Lock()
			seen[rec.Offset] = true
			mu.Unlock()
			if got.Add(1) >= total-committed {
				once2.Do(func() { close(done) })
			}
			return nil
		},
		WithLogger(discardLogger()),
		WithLagSampling(0),
		WithKeyConcurrency(fanOut),
		WithCommitInterval(MinCommitInterval),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- second.Run(ctx2) }()
	waitClosed(t, done, 90*time.Second)
	cancel2()
	select {
	case <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("second Run did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	for off := committed; off < total; off++ {
		if !seen[off] {
			t.Fatalf("offset %d was neither committed nor redelivered: it is lost", off)
		}
	}
}
