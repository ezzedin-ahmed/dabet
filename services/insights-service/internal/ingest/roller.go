package ingest

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ObjectStore is the minimal object-storage surface the roller needs. The
// production implementation is S3Store; tests use an in-memory fake.
type ObjectStore interface {
	// Put writes data at key in the embeddings bucket.
	Put(ctx context.Context, key string, data []byte) error
}

// Roller accumulates embedding records into per-partition parquet files and
// uploads them to the object store, rolling by size or age (docs §8.4).
// Object keys are Hive-partitioned by creator and day:
//
//	creator_id=<id>/date=<YYYY-MM-DD>/embeddings-<unixnano>-<seq>.parquet
//
// Failure policy (§4.7): an upload failure drops the file's records —
// counted on fail_open_total{component="s3"} — and never blocks or crashes
// the pipeline.
//
// Roller is not safe for concurrent use; it is owned by the pipeline loop.
type Roller struct {
	store     ObjectStore
	rollBytes int
	rollAge   time.Duration
	open      map[string]*openFile // partition prefix → accumulating file
	metrics   *Metrics
	failOpen  *prometheus.CounterVec
	seq       atomic.Uint64
	nowNano   func() int64 // for unique object names; overridable in tests
}

type openFile struct {
	recs        []EmbeddingRecord
	approxBytes int
	openedAt    time.Time
}

// NewRoller builds a roller that uploads a partition's file once it holds
// roughly rollBytes of records or has been open for rollAge.
func NewRoller(store ObjectStore, rollBytes int, rollAge time.Duration, m *Metrics, failOpen *prometheus.CounterVec) *Roller {
	return &Roller{
		store:     store,
		rollBytes: rollBytes,
		rollAge:   rollAge,
		open:      make(map[string]*openFile),
		metrics:   m,
		failOpen:  failOpen,
		nowNano:   func() int64 { return time.Now().UnixNano() },
	}
}

// Add appends records to their partitions, uploading any partition that
// reaches the size threshold.
func (r *Roller) Add(ctx context.Context, recs []EmbeddingRecord, now time.Time) {
	for _, rec := range recs {
		prefix := fmt.Sprintf("creator_id=%s/date=%s", rec.CreatorID, rec.EmbeddedAt.UTC().Format("2006-01-02"))
		f, ok := r.open[prefix]
		if !ok {
			f = &openFile{openedAt: now}
			r.open[prefix] = f
		}
		f.recs = append(f.recs, rec)
		f.approxBytes += rec.approxEncodedSize()
		if f.approxBytes >= r.rollBytes {
			r.upload(ctx, prefix, f)
			delete(r.open, prefix)
		}
	}
}

// FlushDue uploads every partition whose file has been open for rollAge.
func (r *Roller) FlushDue(ctx context.Context, now time.Time) {
	for prefix, f := range r.open {
		if now.Sub(f.openedAt) >= r.rollAge {
			r.upload(ctx, prefix, f)
			delete(r.open, prefix)
		}
	}
}

// FlushAll uploads every open partition; called on shutdown.
func (r *Roller) FlushAll(ctx context.Context) {
	for prefix, f := range r.open {
		r.upload(ctx, prefix, f)
		delete(r.open, prefix)
	}
}

func (r *Roller) upload(ctx context.Context, prefix string, f *openFile) {
	if len(f.recs) == 0 {
		return
	}
	var buf bytes.Buffer
	if err := WriteParquet(&buf, f.recs); err != nil {
		// Encoding failure loses the records; count and continue (§4.7).
		r.failOpen.WithLabelValues("s3", "encode_failed").Add(float64(len(f.recs)))
		return
	}
	key := fmt.Sprintf("%s/embeddings-%d-%d.parquet", prefix, r.nowNano(), r.seq.Add(1))
	if err := r.store.Put(ctx, key, buf.Bytes()); err != nil {
		r.failOpen.WithLabelValues("s3", "put_failed").Add(float64(len(f.recs)))
		return
	}
	r.metrics.S3BytesWritten.Add(float64(buf.Len()))
}
