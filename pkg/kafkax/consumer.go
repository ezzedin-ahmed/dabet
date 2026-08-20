package kafkax

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/obs"
)

// Handler processes one record. Returning an error stops the consumer
// without committing that record, so it and everything after it on its
// partition are redelivered (at-least-once); handlers must be idempotent
// (P3).
type Handler func(ctx context.Context, rec *kgo.Record) error

// Consumer is a consumer-group member that processes each assigned
// partition on its own goroutine, strictly in order, and commits a
// partition's offsets as that partition makes progress.
//
// See the package comment for the delivery and ordering guarantees.
type Consumer struct {
	cl      *kgo.Client
	adm     endOffsetLister
	group   string
	topics  []string
	handler Handler
	cfg     ConsumerConfig

	// active is the dispatcher of the current Run, looked up by the
	// rebalance callbacks (which outlive any single Run).
	active atomic.Pointer[dispatcher]
	// sampler is the current run's lag sampler, kept for introspection.
	sampler atomic.Pointer[lagSampler]

	// lagGauge is read by the rebalance callbacks and by each run's
	// sampler, and may be resolved late (see gauge), so it is guarded
	// rather than read off cfg.
	lagMu    sync.Mutex
	lagGauge LagGauge

	runMu sync.Mutex
}

// gauge returns the lag gauge, resolving the process default on first use
// if no explicit choice was made. Late resolution means a service that
// builds its consumer before obs.NewMetrics still reports §4.5's metric.
func (c *Consumer) gauge() LagGauge {
	c.lagMu.Lock()
	defer c.lagMu.Unlock()
	if c.lagGauge == nil && !c.cfg.lagGaugeSet {
		c.lagGauge = PrometheusLagGauge(obs.DefaultKafkaConsumerLag())
	}
	return c.lagGauge
}

// NewConsumer joins group on topics.
//
// The variadic options are the §4.4-style tunables described on
// ConsumerConfig; with none supplied the consumer reads its configuration
// from the environment and falls back to the documented defaults, so
// existing four-argument call sites are unchanged and get the concurrent
// consumer, finer commits, and the lag gauge for free.
func NewConsumer(brokers []string, group string, topics []string, h Handler, opts ...Option) (*Consumer, error) {
	cfg, err := DefaultConsumerConfig()
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	// Default the gauge to the process's kafka_consumer_lag_messages, so
	// §4.5's mandated metric is populated without every service having to
	// remember to wire it (finding F1). Resolved after the options so an
	// explicit WithLagGauge — including WithLagGauge(nil) — wins.
	if !cfg.lagGaugeSet {
		cfg.LagGauge = PrometheusLagGauge(obs.DefaultKafkaConsumerLag())
	}
	cfg.normalise()

	c := &Consumer{
		group:    group,
		topics:   append([]string(nil), topics...),
		handler:  h,
		cfg:      cfg,
		lagGauge: cfg.LagGauge,
	}

	kopts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// Commit only what a handler has finished: marks are set after a
		// handler returns nil and never rewind, so no autocommit tick can
		// move an offset past an unprocessed record.
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(cfg.CommitInterval),
		// Rebalances are gated on the poll loop, so a partition can never
		// be revoked while its records are being handed to a worker. This
		// is what keeps §7.3's single-owner property true during a
		// rebalance and not merely between them.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
		kgo.OnPartitionsCallbackBlocked(c.onRebalanceBlocked),
	}
	kopts = append(kopts, cfg.KgoOpts...)

	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	c.cl = cl
	c.adm = kadm.NewClient(cl)
	return c, nil
}

// Run consumes until ctx is cancelled, the client is closed, a fetch fails,
// or a handler returns an error. It is restartable: callers that restart a
// consumer after a transient failure (moderation-service, credits-service,
// insights-service all do) may call Run again on the same Consumer.
func (c *Consumer) Run(ctx context.Context) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	d := newDispatcher(ctx, c.cl, c.handler, c.group, c.cfg)
	d.cancelRun = cancelRun
	d.onForget = c.forgetLag
	c.active.Store(d)
	defer c.active.Store(nil)

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runCommitter(runCtx)
	}()

	if c.cfg.LagInterval > 0 {
		s := &lagSampler{
			lister:    c.adm,
			gauge:     c.gauge(),
			group:     c.group,
			topics:    c.topics,
			interval:  c.cfg.LagInterval,
			timeout:   c.cfg.LagTimeout,
			log:       c.cfg.Logger,
			positions: d.positions,
		}
		c.sampler.Store(s)
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			s.run(runCtx)
		}()
	}

	err := c.poll(ctx, runCtx, d)

	cancelRun()
	d.shutdown()

	// One last commit of everything that finished, on a context detached
	// from the caller's: on SIGTERM ctx is already cancelled, and dropping
	// the work we just did would replay it on the next start.
	commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.DrainTimeout)
	d.commit(commitCtx)
	cancelCommit()

	if herr := d.failure(); herr != nil {
		return herr
	}
	return err
}

// poll is the fetch loop. It never handles a record itself: its whole job
// is to hand each partition of a fetch to that partition's worker and then
// release the rebalance gate.
func (c *Consumer) poll(ctx, runCtx context.Context, d *dispatcher) error {
	for {
		fetches := c.cl.PollRecords(runCtx, c.cfg.MaxPollRecords)

		stop, err := c.classify(ctx, runCtx, d, fetches)
		if !stop {
			d.dispatch(fetches)
		}
		// Always release the gate: the poll blocked rebalancing whether or
		// not it returned anything, and an un-released gate wedges the
		// group until the session times out.
		c.cl.AllowRebalance()
		if stop {
			return err
		}
		if herr := d.failure(); herr != nil {
			return herr
		}
	}
}

// classify decides whether this poll ends the run, and with what error.
// Handler failures win over everything: they are what the caller retries on.
func (c *Consumer) classify(ctx, runCtx context.Context, d *dispatcher, fetches kgo.Fetches) (bool, error) {
	if err := d.failure(); err != nil {
		return true, err
	}
	if fetches.IsClientClosed() {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if runCtx.Err() != nil {
		// Halted by us rather than by the caller; d.failure() above already
		// reported any reason worth returning.
		return true, nil
	}
	for _, fe := range fetches.Errors() {
		return true, fmt.Errorf("kafka fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
	}
	return false, nil
}

// onAssigned starts a worker per newly assigned partition.
func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	if d := c.active.Load(); d != nil {
		d.assign(assigned)
	}
}

// onRevoked stops the revoked partitions' workers, waits for the record
// each is mid-way through, and then commits — so the member that takes the
// partitions over starts from what was actually finished here, and never
// from an offset past a record nobody completed.
func (c *Consumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	d := c.active.Load()
	if d == nil {
		c.forgetAll(revoked)
		return
	}
	d.releasePartitions(revoked)
	d.commit(ctx)
}

// onLost is the fatal-error path: the partitions are gone and a commit
// would be rejected, so the workers are stopped and nothing is committed.
// The records they did not finish are redelivered to the new owner.
func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	d := c.active.Load()
	if d == nil {
		c.forgetAll(lost)
		return
	}
	d.releasePartitions(lost)
	c.cfg.Logger.Warn("kafka partitions lost", "group", c.group, "partitions", countPartitions(lost))
}

// onRebalanceBlocked fires when a rebalance is waiting on our poll loop,
// which means a handler is slow enough to risk the session timeout. It is
// a signal, not a failure, so it is logged and nothing else.
func (c *Consumer) onRebalanceBlocked(_ context.Context, _ *kgo.Client) {
	c.cfg.Logger.Warn("kafka rebalance blocked by in-flight processing", "group", c.group)
}

func countPartitions(m map[string][]int32) int {
	n := 0
	for _, ps := range m {
		n += len(ps)
	}
	return n
}

// forgetLag drops one partition's lag series. Leaving it behind would
// freeze a stale value on an instance that no longer owns the partition,
// which is exactly the alerting failure §4.7 cannot afford.
func (c *Consumer) forgetLag(tp topicPartition) {
	if g := c.gauge(); g != nil {
		g.ForgetLag(tp.topic, tp.partition, c.group)
	}
}

func (c *Consumer) forgetAll(tps map[string][]int32) {
	for topic, parts := range tps {
		for _, p := range parts {
			c.forgetLag(topicPartition{topic, p})
		}
	}
}

// Close leaves the group and closes the client.
func (c *Consumer) Close() { c.cl.Close() }
