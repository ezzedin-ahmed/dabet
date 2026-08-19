package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CountRow is one increment row for the topic_counts SummingMergeTree
// (docs §8.7). ThemeID is ZeroUUID when the assignment is topic-level only.
type CountRow struct {
	CreatorID  string
	ContentID  string
	TopicID    string
	ThemeID    string
	BucketHour time.Time
	Count      uint64
}

// CountSink writes increment batches to ClickHouse. Faked in tests.
type CountSink interface {
	InsertCounts(ctx context.Context, rows []CountRow) error
}

type countKey struct {
	creatorID  string
	contentID  string
	topicID    string
	themeID    string
	bucketHour time.Time
}

// CountBuffer accumulates topic_counts increments in-process and flushes
// them as one batched insert — ClickHouse hates single-row inserts. A
// flush happens every interval, and early when the buffer holds maxRows
// distinct rows. A failed insert drops the increments (§4.7 — fail open,
// never block): counters are a lossy aggregate and the next reclustering
// rebuilds them anyway.
type CountBuffer struct {
	sink     CountSink
	maxRows  int
	interval time.Duration
	timeout  time.Duration
	failOpen *prometheus.CounterVec // component, reason
	depUp    *prometheus.GaugeVec   // dependency

	mu   sync.Mutex
	rows map[countKey]uint64
	kick chan struct{}
}

// NewCountBuffer builds a CountBuffer flushing every interval or at
// maxRows distinct rows, whichever comes first, with timeout per insert.
func NewCountBuffer(sink CountSink, maxRows int, interval, timeout time.Duration, failOpen *prometheus.CounterVec, depUp *prometheus.GaugeVec) *CountBuffer {
	return &CountBuffer{
		sink:     sink,
		maxRows:  maxRows,
		interval: interval,
		timeout:  timeout,
		failOpen: failOpen,
		depUp:    depUp,
		rows:     make(map[countKey]uint64),
		kick:     make(chan struct{}, 1),
	}
}

// BucketHour truncates t to its UTC hour — the base granularity of
// topic_counts (§8.7); days and months are query-time rollups.
func BucketHour(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// Add records one assignment increment. It never blocks on ClickHouse: a
// full buffer only nudges the flusher goroutine.
func (b *CountBuffer) Add(creatorID, contentID, topicID, themeID string, at time.Time) {
	key := countKey{
		creatorID:  creatorID,
		contentID:  contentID,
		topicID:    topicID,
		themeID:    themeID,
		bucketHour: BucketHour(at),
	}
	b.mu.Lock()
	b.rows[key]++
	full := len(b.rows) >= b.maxRows
	b.mu.Unlock()
	if full {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}
}

// Len returns the number of distinct buffered rows.
func (b *CountBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows)
}

// Run flushes on the interval and on size nudges until ctx is cancelled,
// then performs a final drain flush.
func (b *CountBuffer) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Final drain on its own context; the run context is dead.
			b.Flush(context.Background())
			return
		case <-ticker.C:
			b.Flush(ctx)
		case <-b.kick:
			b.Flush(ctx)
		}
	}
}

// Flush swaps the buffer out and writes it as a single batched insert. On
// failure the batch is dropped and counted on
// fail_open_total{component="clickhouse"}.
func (b *CountBuffer) Flush(ctx context.Context) {
	b.mu.Lock()
	rows := b.rows
	b.rows = make(map[countKey]uint64)
	b.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	out := make([]CountRow, 0, len(rows))
	for k, n := range rows {
		out = append(out, CountRow{
			CreatorID:  k.creatorID,
			ContentID:  k.contentID,
			TopicID:    k.topicID,
			ThemeID:    k.themeID,
			BucketHour: k.bucketHour,
			Count:      n,
		})
	}
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	if err := b.sink.InsertCounts(ctx, out); err != nil {
		b.depUp.WithLabelValues("clickhouse").Set(0)
		b.failOpen.WithLabelValues("clickhouse", "insert_failed").Add(float64(len(out)))
		return
	}
	b.depUp.WithLabelValues("clickhouse").Set(1)
}
