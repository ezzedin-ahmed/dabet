package kafkax

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

// LagGauge receives the per-partition consumer lag samples of docs §4.5's
// kafka_consumer_lag_messages. Labels are (topic, partition, group) only —
// P4 forbids ids in metric labels, and none are passed here.
type LagGauge interface {
	// SetLag publishes the current lag for one owned partition.
	SetLag(topic string, partition int32, group string, lag float64)
	// ForgetLag drops the series for a partition this member no longer
	// owns, so a rebalance does not leave a frozen value behind forever.
	ForgetLag(topic string, partition int32, group string)
}

// PrometheusLagGauge adapts a (topic, partition, group) gauge vector — the
// shape obs.Metrics.KafkaConsumerLag has — to LagGauge. A nil vec yields a
// nil LagGauge, which the consumer treats as "no gauge".
func PrometheusLagGauge(vec *prometheus.GaugeVec) LagGauge {
	if vec == nil {
		return nil
	}
	return promLagGauge{vec: vec}
}

type promLagGauge struct{ vec *prometheus.GaugeVec }

func (g promLagGauge) SetLag(topic string, partition int32, group string, lag float64) {
	g.vec.WithLabelValues(topic, strconv.Itoa(int(partition)), group).Set(lag)
}

func (g promLagGauge) ForgetLag(topic string, partition int32, group string) {
	g.vec.DeleteLabelValues(topic, strconv.Itoa(int(partition)), group)
}

// endOffsetLister is the slice of kadm.Client the lag sampler needs. It is
// an interface so the sampler — including its failure path — is testable
// without a broker.
type endOffsetLister interface {
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
}

// lagSampler polls high watermarks on an interval and publishes
// kafka_consumer_lag_messages for the partitions this member owns.
//
// It is deliberately off the hot path: nothing per-message touches it, and
// per P2 a broker hiccup while sampling is logged and counted, never
// propagated — consumption is not disturbed by a failed sample.
type lagSampler struct {
	lister   endOffsetLister
	gauge    LagGauge
	group    string
	topics   []string
	interval time.Duration
	timeout  time.Duration
	log      *slog.Logger

	// positions returns the next offset this member will process for each
	// partition it currently owns.
	positions func() map[topicPartition]int64

	samples  atomic.Uint64
	failures atomic.Uint64
}

// run samples until ctx is cancelled. It never returns an error: a sample
// is telemetry, and telemetry must not be able to stop the moderation path.
func (s *lagSampler) run(ctx context.Context) {
	if s.interval <= 0 {
		return
	}
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sample(ctx)
		}
	}
}

// sample takes one reading. Errors are logged once per sample and counted;
// a partial listing still publishes the partitions that did resolve.
func (s *lagSampler) sample(ctx context.Context) {
	owned := s.positions()
	if len(owned) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	ends, err := s.lister.ListEndOffsets(ctx, s.topicsOf(owned)...)
	if err != nil {
		s.failures.Add(1)
		s.log.Warn("kafka lag sample failed; consumption unaffected",
			"group", s.group, "error", err.Error())
		return
	}
	s.samples.Add(1)
	if s.gauge == nil {
		return
	}
	for tp, next := range owned {
		end, ok := ends.Lookup(tp.topic, tp.partition)
		if !ok || end.Err != nil || end.Offset < 0 {
			continue
		}
		lag := end.Offset - next
		if lag < 0 {
			lag = 0
		}
		s.gauge.SetLag(tp.topic, tp.partition, s.group, float64(lag))
	}
}

// topicsOf returns the distinct topics of the owned set, sorted so the
// request is stable. It falls back to the configured topics when the owned
// set is somehow empty of names.
func (s *lagSampler) topicsOf(owned map[topicPartition]int64) []string {
	seen := make(map[string]struct{}, len(s.topics))
	for tp := range owned {
		seen[tp.topic] = struct{}{}
	}
	if len(seen) == 0 {
		return s.topics
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
