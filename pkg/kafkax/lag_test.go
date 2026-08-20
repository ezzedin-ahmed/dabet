package kafkax

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"dabet/pkg/obs"
)

// fakeLister stands in for kadm.Client. It can fail on demand, which is
// how the "a broker hiccup must not disturb consumption" requirement of
// P2 gets tested without a broker to break.
type fakeLister struct {
	mu    sync.Mutex
	ends  map[topicPartition]int64
	err   error
	calls int
}

func (f *fakeLister) ListEndOffsets(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make(kadm.ListedOffsets)
	for tp, off := range f.ends {
		if out[tp.topic] == nil {
			out[tp.topic] = make(map[int32]kadm.ListedOffset)
		}
		out[tp.topic][tp.partition] = kadm.ListedOffset{
			Topic: tp.topic, Partition: tp.partition, Offset: off,
		}
	}
	return out, nil
}

func (f *fakeLister) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeLister) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

type lagKey struct {
	topic     string
	partition int32
	group     string
}

type fakeGauge struct {
	mu     sync.Mutex
	values map[lagKey]float64
	gone   []lagKey
}

func newFakeGauge() *fakeGauge { return &fakeGauge{values: make(map[lagKey]float64)} }

func (g *fakeGauge) SetLag(topic string, partition int32, group string, lag float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values[lagKey{topic, partition, group}] = lag
}

func (g *fakeGauge) ForgetLag(topic string, partition int32, group string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	k := lagKey{topic, partition, group}
	delete(g.values, k)
	g.gone = append(g.gone, k)
}

func (g *fakeGauge) get(k lagKey) (float64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.values[k]
	return v, ok
}

func (g *fakeGauge) size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.values)
}

func (g *fakeGauge) forgotten() []lagKey {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]lagKey(nil), g.gone...)
}

// TestLagSamplerPublishesPerPartition is finding F1: §4.5's mandated gauge
// gets a value, per topic, partition and group, for the partitions this
// member owns and no others.
func TestLagSamplerPublishesPerPartition(t *testing.T) {
	const topic = "messages.v1"
	lister := &fakeLister{ends: map[topicPartition]int64{
		{topic, 0}: 1000,
		{topic, 1}: 500,
		{topic, 2}: 42, // owned by another member
	}}
	gauge := newFakeGauge()
	s := &lagSampler{
		lister:   lister,
		gauge:    gauge,
		group:    "moderation",
		topics:   []string{topic},
		interval: time.Hour,
		timeout:  time.Second,
		log:      discardLogger(),
		positions: func() map[topicPartition]int64 {
			return map[topicPartition]int64{
				{topic, 0}: 400,
				{topic, 1}: 500,
			}
		},
	}

	s.sample(context.Background())

	if got := gauge.size(); got != 2 {
		t.Fatalf("gauge has %d series, want 2 (only the owned partitions)", got)
	}
	if v, ok := gauge.get(lagKey{topic, 0, "moderation"}); !ok || v != 600 {
		t.Fatalf("partition 0 lag = %v (present=%v), want 600", v, ok)
	}
	// Caught up: the high watermark is the next offset we would read.
	if v, ok := gauge.get(lagKey{topic, 1, "moderation"}); !ok || v != 0 {
		t.Fatalf("partition 1 lag = %v (present=%v), want 0", v, ok)
	}
}

// TestLagSamplerClampsNegativeLag guards the transient where our position
// is ahead of a stale watermark: lag is a backlog, and a negative backlog
// would be nonsense on a dashboard.
func TestLagSamplerClampsNegativeLag(t *testing.T) {
	const topic = "messages.v1"
	gauge := newFakeGauge()
	s := &lagSampler{
		lister:   &fakeLister{ends: map[topicPartition]int64{{topic, 0}: 10}},
		gauge:    gauge,
		group:    "g",
		topics:   []string{topic},
		interval: time.Hour,
		timeout:  time.Second,
		log:      discardLogger(),
		positions: func() map[topicPartition]int64 {
			return map[topicPartition]int64{{topic, 0}: 25}
		},
	}
	s.sample(context.Background())
	if v, _ := gauge.get(lagKey{topic, 0, "g"}); v != 0 {
		t.Fatalf("lag = %v, want 0", v)
	}
}

// TestLagSamplerFailureIsCountedNotPropagated is P2 in miniature: a broker
// failure while sampling must be logged and counted, and must leave the
// gauge and the consumer alone.
func TestLagSamplerFailureIsCountedNotPropagated(t *testing.T) {
	const topic = "messages.v1"
	boom := errors.New("broker unreachable")
	lister := &fakeLister{ends: map[topicPartition]int64{{topic, 0}: 100}}
	gauge := newFakeGauge()
	s := &lagSampler{
		lister:   lister,
		gauge:    gauge,
		group:    "g",
		topics:   []string{topic},
		interval: time.Hour,
		timeout:  time.Second,
		log:      discardLogger(),
		positions: func() map[topicPartition]int64 {
			return map[topicPartition]int64{{topic, 0}: 40}
		},
	}

	s.sample(context.Background())
	if v, _ := gauge.get(lagKey{topic, 0, "g"}); v != 60 {
		t.Fatalf("lag = %v, want 60", v)
	}

	lister.setErr(boom)
	s.sample(context.Background())
	if got := s.failures.Load(); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
	// The last good value stands; nothing panicked and nothing was
	// returned that a caller could mistake for a consumption error.
	if v, _ := gauge.get(lagKey{topic, 0, "g"}); v != 60 {
		t.Fatalf("lag = %v after a failed sample, want the last good value 60", v)
	}

	lister.setErr(nil)
	s.sample(context.Background())
	if got := s.samples.Load(); got != 2 {
		t.Fatalf("successful samples = %d, want 2: sampling did not recover", got)
	}
}

// TestLagSamplerRunsOnItsInterval checks the ticker, and that stopping is
// clean.
func TestLagSamplerRunsOnItsInterval(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	lister := &fakeLister{ends: map[topicPartition]int64{{topic, 0}: 100}}
	s := &lagSampler{
		lister:   lister,
		gauge:    newFakeGauge(),
		group:    "g",
		topics:   []string{topic},
		interval: time.Millisecond,
		timeout:  time.Second,
		log:      discardLogger(),
		positions: func() map[topicPartition]int64 {
			return map[topicPartition]int64{{topic, 0}: 0}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(ctx)
	}()
	waitFor(t, 5*time.Second, func() bool { return lister.callCount() >= 3 })
	cancel()
	waitClosed(t, done, 5*time.Second)
}

// TestLagSamplerIdleWhenNothingOwned keeps the gauge off the broker when
// this member owns nothing, so a scaled-out fleet does not hammer
// ListOffsets for no reason.
func TestLagSamplerIdleWhenNothingOwned(t *testing.T) {
	lister := &fakeLister{}
	s := &lagSampler{
		lister:    lister,
		gauge:     newFakeGauge(),
		group:     "g",
		interval:  time.Hour,
		timeout:   time.Second,
		log:       discardLogger(),
		positions: func() map[topicPartition]int64 { return nil },
	}
	s.sample(context.Background())
	if got := lister.callCount(); got != 0 {
		t.Fatalf("listed offsets %d times with no partitions owned, want 0", got)
	}
}

// TestRevokeForgetsLagSeries: a partition that moves to another member
// must lose its series here, or the value freezes forever on an instance
// that no longer owns it and §4.7's alert reads a lie.
func TestRevokeForgetsLagSeries(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "messages.v1"
	gauge := newFakeGauge()
	gauge.SetLag(topic, 0, "g", 97379)

	c := &Consumer{
		group:    "g",
		cfg:      ConsumerConfig{LagGauge: gauge, Logger: discardLogger(), lagGaugeSet: true},
		lagGauge: gauge,
	}
	d := newDispatcher(context.Background(), newFakeClient(),
		func(context.Context, *kgo.Record) error { return nil }, "g", testConfig())
	d.onForget = c.forgetLag
	defer d.shutdown()

	d.assign(map[string][]int32{topic: {0}})
	d.releasePartitions(map[string][]int32{topic: {0}})

	if _, ok := gauge.get(lagKey{topic, 0, "g"}); ok {
		t.Fatal("lag series survived the revoke")
	}
	if got := gauge.forgotten(); len(got) != 1 || got[0] != (lagKey{topic, 0, "g"}) {
		t.Fatalf("forgotten = %v, want the revoked partition", got)
	}
}

// TestPrometheusLagGauge checks the adapter that carries lag onto §4.5's
// kafka_consumer_lag_messages, including P4: the labels are topic,
// partition and group, and nothing else.
func TestPrometheusLagGauge(t *testing.T) {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag_messages",
	}, []string{"topic", "partition", "group"})
	g := PrometheusLagGauge(vec)

	g.SetLag("messages.v1", 7, "moderation", 1234)
	got := testutil.ToFloat64(vec.WithLabelValues("messages.v1", "7", "moderation"))
	if got != 1234 {
		t.Fatalf("gauge = %v, want 1234", got)
	}
	g.ForgetLag("messages.v1", 7, "moderation")
	if n := testutil.CollectAndCount(vec); n != 0 {
		t.Fatalf("%d series left after ForgetLag, want 0", n)
	}
	if PrometheusLagGauge(nil) != nil {
		t.Fatal("PrometheusLagGauge(nil) must be nil so a consumer can tell there is no gauge")
	}
}

// TestObsDefaultLagGaugeIsPublished is the wiring that makes F1 fixed
// rather than merely fixable: a service that builds obs.Metrics gets the
// gauge populated without changing its call site.
func TestObsDefaultLagGaugeIsPublished(t *testing.T) {
	prev := obs.DefaultKafkaConsumerLag()
	t.Cleanup(func() { obs.SetDefaultKafkaConsumerLag(prev) })

	m := obs.NewMetrics(prometheus.NewRegistry())
	if obs.DefaultKafkaConsumerLag() != m.KafkaConsumerLag {
		t.Fatal("obs.NewMetrics did not publish kafka_consumer_lag_messages as the process default")
	}
	if PrometheusLagGauge(obs.DefaultKafkaConsumerLag()) == nil {
		t.Fatal("the process default gauge did not adapt to a LagGauge")
	}
}
