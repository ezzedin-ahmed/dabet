package kafkax

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartition identifies one assigned partition. It is the unit of
// ownership, of ordering, and of commit granularity.
type topicPartition struct {
	topic     string
	partition int32
}

// groupClient is the slice of *kgo.Client the dispatcher uses. It exists so
// the ordering, commit, rebalance and error logic below can be exercised
// without a broker, a container or a network.
type groupClient interface {
	// MarkCommitRecords marks a record as safe to commit. franz-go's
	// implementation is mutex-guarded and refuses to rewind, so partition
	// workers may call it concurrently.
	MarkCommitRecords(rs ...*kgo.Record)
	// CommitMarkedOffsets synchronously commits everything marked.
	CommitMarkedOffsets(ctx context.Context) error
}

// partitionWorker owns exactly one partition. All records for that
// partition are handled by this one goroutine, in the order the broker
// returned them — which is what makes §7.3's "all state for a
// (sender, content) pair is mutated by exactly one consumer, in order"
// hold with a concurrent consumer.
type partitionWorker struct {
	tp topicPartition
	// recs carries whole polled batches. FIFO, and the poll loop finishes
	// enqueuing every partition of poll N before it polls again, so batch
	// N always reaches the worker before batch N+1.
	recs chan []*kgo.Record
	quit chan struct{}
	done chan struct{}

	quitOnce sync.Once
	// next is the offset this worker will process next: the first offset
	// it has seen until it completes a record, then last processed + 1.
	// Read by the lag sampler; -1 means "nothing seen yet".
	next atomic.Int64
}

func (w *partitionWorker) stop() { w.quitOnce.Do(func() { close(w.quit) }) }

// dispatcher runs one Consumer.Run: a worker per assigned partition, a
// commit trigger, and the bookkeeping the rebalance callbacks need.
type dispatcher struct {
	cl      groupClient
	handler Handler
	group   string
	cfg     ConsumerConfig
	log     *slog.Logger
	ctx     context.Context

	// sem bounds concurrent handler invocations across all partitions.
	// nil means unlimited. Held for exactly one record at a time by any
	// worker, so it can never reorder a partition.
	sem chan struct{}

	mu      sync.Mutex
	workers map[topicPartition]*partitionWorker
	wg      sync.WaitGroup

	// commitNow is a coalescing signal; the committer goroutine owns the
	// commit RPC so no handler ever waits on one.
	commitNow   chan struct{}
	sinceCommit atomic.Int64

	// stop is closed when the run is finished for any reason. err holds
	// the first handler error, which Run returns.
	stopOnce sync.Once
	stop     chan struct{}
	errOnce  sync.Once
	err      atomic.Pointer[error]

	// cancelRun wakes the poll loop when the run ends for a reason the
	// poll loop cannot see, such as a handler failing on a worker.
	cancelRun context.CancelFunc

	onForget func(topicPartition)
}

func newDispatcher(ctx context.Context, cl groupClient, h Handler, group string, cfg ConsumerConfig) *dispatcher {
	d := &dispatcher{
		cl:        cl,
		handler:   h,
		group:     group,
		cfg:       cfg,
		log:       cfg.Logger,
		ctx:       ctx,
		workers:   make(map[topicPartition]*partitionWorker),
		commitNow: make(chan struct{}, 1),
		stop:      make(chan struct{}),
		onForget:  func(topicPartition) {},
	}
	if cfg.PartitionConcurrency > 0 {
		d.sem = make(chan struct{}, cfg.PartitionConcurrency)
	}
	return d
}

// fail records the first handler error and stops the run. Nothing at or
// past the failed record is ever marked, so the offset does not advance
// and the record is redelivered (at-least-once, exactly as before).
func (d *dispatcher) fail(err error) {
	d.errOnce.Do(func() { d.err.Store(&err) })
	d.halt()
}

// halt ends the run. It closes stop, which workers check between records —
// never mid-record — and wakes the poll loop. It deliberately does not
// cancel the handler context: a record already in a handler is allowed to
// finish and be committed.
func (d *dispatcher) halt() {
	d.stopOnce.Do(func() {
		close(d.stop)
		if d.cancelRun != nil {
			d.cancelRun()
		}
	})
}

func (d *dispatcher) failure() error {
	if p := d.err.Load(); p != nil {
		return *p
	}
	return nil
}

// worker returns the worker for tp, starting one if this is the first
// record seen for it. Creating lazily rather than only in the assign
// callback keeps the dispatcher correct when Run is restarted on a client
// that is already a member of the group — which is how moderation-service
// and credits-service use it.
func (d *dispatcher) worker(tp topicPartition) *partitionWorker {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.workerLocked(tp)
}

func (d *dispatcher) workerLocked(tp topicPartition) *partitionWorker {
	if w, ok := d.workers[tp]; ok {
		return w
	}
	w := &partitionWorker{
		tp:   tp,
		recs: make(chan []*kgo.Record, d.cfg.QueueDepth),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	w.next.Store(-1)
	d.workers[tp] = w
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runWorker(w)
	}()
	return w
}

// assign pre-starts workers for newly assigned partitions.
func (d *dispatcher) assign(assigned map[string][]int32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for topic, parts := range assigned {
		for _, p := range parts {
			d.workerLocked(topicPartition{topic, p})
		}
	}
}

// dispatch hands each partition of one poll to its own worker. Partitions
// are independent, so a slow one gets a goroutine rather than stalling the
// others; the wait at the end is what keeps successive polls of the *same*
// partition in order.
func (d *dispatcher) dispatch(fetches kgo.Fetches) {
	var slow sync.WaitGroup
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		w := d.worker(topicPartition{p.Topic, p.Partition})
		recs := p.Records
		select {
		case w.recs <- recs:
			return
		default:
		}
		slow.Add(1)
		go func() {
			defer slow.Done()
			select {
			case w.recs <- recs:
			case <-w.quit:
			case <-d.stop:
			case <-d.ctx.Done():
			}
		}()
	})
	slow.Wait()
}

// runWorker is the whole of the per-partition ordering guarantee: one
// goroutine, one partition, records handled one at a time in offset order,
// and the offset marked only after the handler has returned nil.
func (d *dispatcher) runWorker(w *partitionWorker) {
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		case <-d.stop:
			return
		case <-d.ctx.Done():
			return
		case batch := <-w.recs:
			if !d.handleBatch(w, batch) {
				return
			}
		}
	}
}

// handleBatch processes one polled batch in order, reporting whether the
// worker should keep going. Ownership is re-checked between records but
// never abandoned mid-record: a half-applied handler is the one thing a
// revoke must not create.
func (d *dispatcher) handleBatch(w *partitionWorker, batch []*kgo.Record) bool {
	if w.next.Load() < 0 && len(batch) > 0 {
		w.next.Store(batch[0].Offset)
	}
	for _, rec := range batch {
		select {
		case <-w.quit:
			return false
		case <-d.stop:
			return false
		case <-d.ctx.Done():
			return false
		default:
		}
		if !d.acquire(w) {
			return false
		}
		err := d.handle(rec)
		d.release()
		if err != nil {
			// Do not mark: the offset must not advance past a record
			// whose handler failed.
			d.fail(err)
			return false
		}
		d.cl.MarkCommitRecords(rec)
		w.next.Store(rec.Offset + 1)
		if n := d.cfg.CommitRecords; n > 0 && d.sinceCommit.Add(1) >= int64(n) {
			d.sinceCommit.Store(0)
			d.signalCommit()
		}
	}
	return true
}

// handle runs one record under a CONSUMER span that continues the
// producer's trace, so the handler's own work (policy gRPC, Redis, the
// LLM call, the verdict publish) hangs off the same trace as the ingest
// that created the record.
func (d *dispatcher) handle(rec *kgo.Record) error {
	ctx, span := StartConsumeSpan(d.ctx, rec, d.group)
	defer span.End()
	err := d.handler(ctx, rec)
	recordError(span, err)
	return err
}

func (d *dispatcher) acquire(w *partitionWorker) bool {
	if d.sem == nil {
		return true
	}
	select {
	case d.sem <- struct{}{}:
		return true
	case <-w.quit:
		return false
	case <-d.stop:
		return false
	case <-d.ctx.Done():
		return false
	}
}

func (d *dispatcher) release() {
	if d.sem == nil {
		return
	}
	<-d.sem
}

func (d *dispatcher) signalCommit() {
	select {
	case d.commitNow <- struct{}{}:
	default:
	}
}

// positions snapshots the next offset each owned partition will process.
// This is the member's true read position, so high watermark minus this is
// the backlog it is actually carrying.
func (d *dispatcher) positions() map[topicPartition]int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[topicPartition]int64, len(d.workers))
	for tp, w := range d.workers {
		if next := w.next.Load(); next >= 0 {
			out[tp] = next
		}
	}
	return out
}

// commit issues one synchronous commit of everything marked. Marks are set
// only after a handler returned nil, so a commit can never move an offset
// past an unprocessed or failed record whatever the timing.
func (d *dispatcher) commit(ctx context.Context) {
	if err := d.cl.CommitMarkedOffsets(ctx); err != nil {
		// A failed commit costs redelivery, not correctness (P3: handlers
		// are idempotent), so it is logged and the run continues.
		d.log.Warn("kafka commit failed", "group", d.group, "error", err.Error())
	}
}

// runCommitter services the record-count commit trigger. The interval
// trigger is franz-go's own autocommit of marked offsets, configured with
// AutoCommitInterval, so there is exactly one timer rather than two
// committing the same offsets. Issuing the RPC here rather than in a
// worker is what keeps commits off the handler's critical path.
func (d *dispatcher) runCommitter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case <-d.commitNow:
			d.commit(ctx)
		}
	}
}

// release ownership of tps: stop their workers, wait for the record each
// is mid-way through, and forget their lag series. Queued-but-unstarted
// batches are dropped unprocessed and therefore uncommitted, so the new
// owner redelivers them.
func (d *dispatcher) releasePartitions(tps map[string][]int32) {
	d.mu.Lock()
	var ws []*partitionWorker
	for topic, parts := range tps {
		for _, p := range parts {
			tp := topicPartition{topic, p}
			if w, ok := d.workers[tp]; ok {
				delete(d.workers, tp)
				ws = append(ws, w)
			}
		}
	}
	d.mu.Unlock()

	for _, w := range ws {
		w.stop()
	}
	d.waitDrained(ws)
	for _, w := range ws {
		d.onForget(w.tp)
	}
}

// waitDrained waits for stopped workers to finish their in-flight record,
// bounded so a wedged handler cannot hold a rebalance open forever. The
// goroutines stay accounted for in d.wg either way, so Run still waits for
// them before returning and nothing leaks.
func (d *dispatcher) waitDrained(ws []*partitionWorker) {
	if len(ws) == 0 {
		return
	}
	deadline := time.NewTimer(d.cfg.DrainTimeout)
	defer deadline.Stop()
	for _, w := range ws {
		select {
		case <-w.done:
		case <-deadline.C:
			d.log.Warn("kafka partition drain timed out; rebalance proceeding",
				"group", d.group, "topic", w.tp.topic, "partition", w.tp.partition,
				"timeout", d.cfg.DrainTimeout.String())
			return
		}
	}
}

// shutdown stops every worker and waits for all of this run's goroutines.
func (d *dispatcher) shutdown() {
	d.halt()
	d.mu.Lock()
	ws := make([]*partitionWorker, 0, len(d.workers))
	for tp, w := range d.workers {
		ws = append(ws, w)
		delete(d.workers, tp)
	}
	d.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
	d.wg.Wait()
	for _, w := range ws {
		d.onForget(w.tp)
	}
}
