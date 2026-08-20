package kafkax

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This file implements per-key concurrency inside a single partition.
//
// # Why it is safe
//
// docs §7.3 requires that "all state for a given (sender, content) is
// mutated by exactly one consumer, in order". The unit that sentence
// actually names is the (sender, content) pair, which is precisely the
// record key: messages.v1 is keyed by hash(author_id, content_id)
// (§4.2, contracts.MessagesKey). Per-partition serialisation was only ever
// a coarse way of achieving per-key serialisation — a partition carries
// thousands of unrelated keys, and today an unrelated sender waits behind
// every other sender that hashed to the same partition.
//
// Routing every record to worker stableHash(key) % N therefore keeps the
// guarantee verbatim: a key is always handled by the same worker of the
// same partition of the same member, one record at a time, in offset
// order. What it drops is the incidental serialisation between keys, which
// §7.3 never asked for.
//
// The Redis keyspace of §4.3 confirms it. dup:{content:author},
// rate:{content:author} and emb:{content:author} are keyed by exactly the
// record key, so they stay single-owner and single-threaded. samp:{content}
// and seen:{message_id} are not: one content has many authors and therefore
// many record keys spread over many partitions, and a message id is unique
// to one record. Both are already touched concurrently across partitions
// and across instances today, and both are already atomic rather than
// ordered (SET NX for the guard, a Lua token bucket for the sampler). This
// change adds no key to that second group, so it introduces no new
// concurrency hazard.
//
// # Empty keys
//
// A record with a nil or zero-length key has no (sender, content) identity
// to route on. All such records go to worker 0, so their order relative to
// each other is preserved exactly as before. This is a defined behaviour,
// not a fallback: a topic produced without keys behaves, per partition,
// like the serial consumer.

// FNV-1a, 64-bit. Chosen over maphash because routing must be stable for
// the life of a partition assignment and identical in every process and
// every Go release — maphash is seeded per process and is explicitly not.
// Written out rather than using hash/fnv so hashing a key allocates
// nothing on the hot path.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// stableHash is the routing hash. Deterministic, allocation-free.
func stableHash(key []byte) uint64 {
	h := fnvOffset64
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// workerFor picks the sub-worker that owns a record key. n must be >= 1.
// An empty or nil key routes to worker 0 (see the package note above).
func workerFor(key []byte, n int) int {
	if n <= 1 || len(key) == 0 {
		return 0
	}
	return int(stableHash(key) % uint64(n))
}

// trackEntry is one dispatched record awaiting completion.
type trackEntry struct {
	rec  *kgo.Record
	done bool
}

// offsetTracker is the low-water mark for one partition.
//
// With per-key concurrency the records of a partition complete out of
// order, so "the last record I finished" is no longer a safe commit point:
// finishing offset 7 while offset 4 is still in a handler and then
// committing 8 would lose records 4..6 on a crash. The tracker instead
// tracks the *contiguous completed prefix*: an offset is only ever handed
// to MarkCommitRecords once every record dispatched before it has returned
// nil. A failed or still-running record holds the mark behind it, exactly
// as a failed record holds the offset today.
//
// Contiguity is by dispatch position, not by offset arithmetic, so gaps in
// the offset sequence (transaction markers, compacted topics) cannot wedge
// the mark.
//
// The tracker also bounds memory: begin acquires one of maxTracked slots
// and the slot is only released when the record leaves the prefix. A slow
// key therefore stops the partition dispatching new work rather than
// letting the pending set grow without limit. It cannot deadlock: the
// record holding the prefix has already been dispatched and needs no slot
// to finish.
type offsetTracker struct {
	slots chan struct{}

	mu    sync.Mutex
	q     []*trackEntry
	head  int
	index map[int64]*trackEntry
	// next is the low-water mark: the offset this partition would resume
	// from. -1 until the first record is dispatched.
	next int64
}

func newOffsetTracker(maxTracked int) *offsetTracker {
	if maxTracked < 1 {
		maxTracked = 1
	}
	return &offsetTracker{
		slots: make(chan struct{}, maxTracked),
		index: make(map[int64]*trackEntry),
		next:  -1,
	}
}

// reserve takes one in-flight slot, or reports false if the partition is
// being torn down first. It must be called before begin. Blocking here is
// the backpressure that bounds a slow key's memory.
func (t *offsetTracker) reserve(quit, stop <-chan struct{}) bool {
	select {
	case t.slots <- struct{}{}:
		return true
	case <-quit:
		return false
	case <-stop:
		return false
	}
}

// begin records a dispatched record. Records must be passed in the order
// the broker returned them, which is what makes the prefix meaningful.
func (t *offsetTracker) begin(rec *kgo.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := &trackEntry{rec: rec}
	t.q = append(t.q, e)
	t.index[rec.Offset] = e
	if t.next < 0 {
		t.next = rec.Offset
	}
}

// complete marks a record's handler as having returned nil and returns the
// highest record the commit mark may now advance to, or nil if the prefix
// did not move because something dispatched earlier is still outstanding.
//
// The returned value is the caller's *original* record pointer, never a
// synthesised one: franz-go's MarkCommitRecords compares (LeaderEpoch,
// Offset+1) and ignores a mark whose epoch is lower, so a fabricated
// record with a zero epoch would be silently dropped and the group would
// stop committing.
func (t *offsetTracker) complete(offset int64) *kgo.Record {
	t.mu.Lock()
	e, ok := t.index[offset]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	e.done = true

	var last *kgo.Record
	freed := 0
	for t.head < len(t.q) && t.q[t.head].done {
		last = t.q[t.head].rec
		delete(t.index, last.Offset)
		t.q[t.head] = nil
		t.head++
		freed++
	}
	if last != nil {
		t.next = last.Offset + 1
		t.compactLocked()
	}
	t.mu.Unlock()

	// Slots are released outside the lock; each record that left the
	// pending window lets the coordinator dispatch one more.
	for i := 0; i < freed; i++ {
		<-t.slots
	}
	return last
}

// compactLocked drops the consumed head of the queue once it is more than
// half of it, so a long-lived partition does not grow a slice of nils.
func (t *offsetTracker) compactLocked() {
	if t.head == len(t.q) {
		t.q = t.q[:0]
		t.head = 0
		return
	}
	if t.head > 64 && t.head*2 > len(t.q) {
		t.q = append(t.q[:0], t.q[t.head:]...)
		t.head = 0
	}
}

// mark returns the current low-water mark: the offset this partition would
// resume from if the process died now. -1 means nothing has been
// dispatched yet.
func (t *offsetTracker) mark() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.next
}

// pending is the number of dispatched records not yet in the committed
// prefix. Test and diagnostic use only.
func (t *offsetTracker) pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.q) - t.head
}

// keyedPartition is one partition's fan-out: a coordinator (the goroutine
// running runKeyedWorker) that routes records to N sub-workers, plus the
// low-water-mark tracker they all report completion to.
//
// Ownership of a key never moves between sub-workers for the life of the
// assignment, so a key is handled by one goroutine, in offset order — the
// property §7.3 rests on.
type keyedPartition struct {
	d     *dispatcher
	w     *partitionWorker
	subs  []chan *kgo.Record
	track *offsetTracker

	// halt is closed by the coordinator on its way out, so sub-workers
	// stop after the record they are on rather than mid-record.
	halt chan struct{}
	wg   sync.WaitGroup
}

// runKeyedWorker is the KAFKA_CONSUMER_KEY_CONCURRENCY > 1 path. It never
// runs a handler itself: it routes, and it waits for its sub-workers on
// the way out so a revoke leaves nothing behind.
func (d *dispatcher) runKeyedWorker(w *partitionWorker) {
	n := d.cfg.KeyConcurrency
	k := &keyedPartition{
		d:     d,
		w:     w,
		subs:  make([]chan *kgo.Record, n),
		track: newOffsetTracker(n * d.cfg.KeyQueueDepth),
		halt:  make(chan struct{}),
	}
	w.track.Store(k.track)
	for i := range k.subs {
		k.subs[i] = make(chan *kgo.Record, d.cfg.KeyQueueDepth)
		ch := k.subs[i]
		k.wg.Add(1)
		go func() {
			defer k.wg.Done()
			k.runSub(ch)
		}()
	}
	// Sub-workers are accounted for here rather than in d.wg: the
	// coordinator is in d.wg and does not return until they have, so
	// d.wg.Wait() still covers every goroutine this partition started, and
	// w.done (closed by runWorker once this returns) still means "this
	// partition is completely quiet".
	defer func() {
		close(k.halt)
		k.wg.Wait()
	}()

	for {
		select {
		case <-w.quit:
			return
		case <-d.stop:
			return
		case <-d.ctx.Done():
			return
		case batch := <-w.recs:
			if !k.route(batch) {
				return
			}
		}
	}
}

// route hands one polled batch to the sub-workers in offset order. The
// tracker is told about a record before it is queued, so the low-water
// mark can never run ahead of something that was dispatched and not
// finished.
func (k *keyedPartition) route(batch []*kgo.Record) bool {
	for _, rec := range batch {
		select {
		case <-k.w.quit:
			return false
		case <-k.d.stop:
			return false
		case <-k.d.ctx.Done():
			return false
		default:
		}
		if !k.track.reserve(k.w.quit, k.d.stop) {
			return false
		}
		k.track.begin(rec)
		ch := k.subs[workerFor(rec.Key, len(k.subs))]
		select {
		case ch <- rec:
		case <-k.w.quit:
			return false
		case <-k.d.stop:
			return false
		case <-k.d.ctx.Done():
			return false
		}
	}
	return true
}

// runSub is one key-affine worker: every record it receives shares a
// worker index, hashed from the record key, so records for one key reach
// it in offset order and it handles them one at a time.
func (k *keyedPartition) runSub(ch <-chan *kgo.Record) {
	for {
		// Priority check first, so a queued record is never *started*
		// after a stop signal is already visible — a revoke leaves the
		// record it was mid-way through finished and everything queued
		// behind it untouched, which is what the serial worker has always
		// done. Without this, a bare select would pick between "stop" and
		// "next record" at random.
		select {
		case <-k.halt:
			return
		case <-k.w.quit:
			return
		case <-k.d.stop:
			return
		case <-k.d.ctx.Done():
			return
		default:
		}
		select {
		case <-k.halt:
			return
		case <-k.w.quit:
			return
		case <-k.d.stop:
			return
		case <-k.d.ctx.Done():
			return
		case rec := <-ch:
			if !k.handleOne(rec) {
				return
			}
		}
	}
}

// handleOne runs one record and folds the result into the low-water mark.
// Nothing is marked unless the whole contiguous prefix below the record
// has already succeeded, so at-least-once is exactly what it was: a crash
// re-delivers from the mark, and every record at or above it is replayed.
func (k *keyedPartition) handleOne(rec *kgo.Record) bool {
	if !k.d.acquire(k.w) {
		return false
	}
	err := k.d.handle(rec)
	k.d.release()
	if err != nil {
		// Do not complete: the prefix stops at this record, so neither it
		// nor anything after it on this partition is ever committed.
		k.d.fail(err)
		return false
	}
	// The mark and the reported position both come from the tracker, which
	// advances them under one lock: two sub-workers completing at once
	// would otherwise be free to store their positions in the wrong order
	// and rewind the partition's reported read position.
	if last := k.track.complete(rec.Offset); last != nil {
		k.d.cl.MarkCommitRecords(last)
	}
	if n := k.d.cfg.CommitRecords; n > 0 && k.d.sinceCommit.Add(1) >= int64(n) {
		k.d.sinceCommit.Store(0)
		k.d.signalCommit()
	}
	return true
}
