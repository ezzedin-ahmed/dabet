// Command clustering-service performs live classification (docs §8.5): for
// each embedded message posted by insights-service to the internal assign
// API, it finds the nearest topic centroid for that creator in Milvus,
// assigns at >= 0.75 cosine (A23) with a theme sub-assignment, and buffers
// topic_counts increments toward ClickHouse in batches. Below threshold is
// unassigned — a normal outcome; the vector is already in S3.
//
// Milvus holds centroids only, in one collection partitioned by creator_id
// (A22). ClickHouse tables are created on startup per §8.7. Either store
// being down fails open: batches are dropped and counted, the process never
// blocks and never crashes (§4.7).
//
// Environment (beyond the shared names of §4.4 — MILVUS_ADDR,
// CLICKHOUSE_DSN, HTTP_ADDR, METRICS_ADDR):
//
//	CLUSTER_ASSIGN_THRESHOLD       cosine similarity floor    (default 0.75, A23)
//	CLUSTER_COUNTS_FLUSH_INTERVAL  counter flush interval     (default 5s)
//	CLUSTER_COUNTS_FLUSH_MAX_ROWS  early-flush row count      (default 10000)
//	CLUSTER_COUNTS_INSERT_TIMEOUT  per-flush insert timeout   (default 10s)
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"dabet/pkg/config"
	"dabet/pkg/service"

	"dabet/services/clustering-service/internal/chsink"
	"dabet/services/clustering-service/internal/cluster"
	"dabet/services/clustering-service/internal/httpapi"
	"dabet/services/clustering-service/internal/milvusx"
)

func main() {
	svc := service.New("clustering-service")
	if err := run(svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	thresholdStr := config.GetDefault("CLUSTER_ASSIGN_THRESHOLD", "0.75")
	threshold, err := strconv.ParseFloat(thresholdStr, 32)
	if err != nil {
		return fmt.Errorf("environment variable CLUSTER_ASSIGN_THRESHOLD: %w", err)
	}
	flushInterval, err := config.GetDuration("CLUSTER_COUNTS_FLUSH_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}
	flushMaxRows, err := config.GetInt("CLUSTER_COUNTS_FLUSH_MAX_ROWS", 10_000)
	if err != nil {
		return err
	}
	insertTimeout, err := config.GetDuration("CLUSTER_COUNTS_INSERT_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	milvusAddr := config.GetDefault(config.EnvMilvusAddr, "localhost:19530")
	chDSN := config.GetDefault(config.EnvClickhouseDSN, "clickhouse://localhost:9002/dabet")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink, err := chsink.Open(chDSN)
	if err != nil {
		return err // malformed DSN, not ClickHouse being down — config error
	}
	defer sink.Close()

	metrics := cluster.NewMetrics(svc.Registry)
	counts := cluster.NewCountBuffer(sink, flushMaxRows, flushInterval, insertTimeout,
		svc.Metrics.FailOpenTotal, svc.Metrics.DependencyUp)
	index := &lazyIndex{}
	assigner := cluster.NewAssigner(index, counts, float32(threshold), metrics,
		svc.Metrics.FailOpenTotal, svc.Metrics.DependencyUp)
	httpapi.Register(svc.Mux, assigner)

	// Both stores fail open, so the service starts (and stays ready, §4.5)
	// with either one down: connection and schema/collection bootstrap
	// retry in the background while assignments drop and are counted.
	svc.Metrics.DependencyUp.WithLabelValues("milvus").Set(0)
	svc.Metrics.DependencyUp.WithLabelValues("clickhouse").Set(0)
	go connectMilvus(ctx, svc, milvusAddr, index)
	go ensureClickhouse(ctx, svc, sink)
	go counts.Run(ctx)

	err = svc.Run(ctx)
	cancel()
	return err
}

// lazyIndex is a CentroidIndex that becomes real once the background
// connect loop reaches Milvus. Before that every search fails, which the
// Assigner turns into a dropped, fail-open-counted batch.
type lazyIndex struct {
	ptr atomic.Pointer[milvusx.Index]
}

type notConnectedError struct{}

func (notConnectedError) Error() string { return "milvus: not connected yet" }

func (l *lazyIndex) Nearest(ctx context.Context, creatorID, parentID string, vec []float32) (cluster.Match, bool, error) {
	idx := l.ptr.Load()
	if idx == nil {
		return cluster.Match{}, false, notConnectedError{}
	}
	return idx.Nearest(ctx, creatorID, parentID, vec)
}

// connectMilvus dials Milvus and creates/loads the centroid collection,
// retrying with backoff until it succeeds or ctx ends (§4.7 — a dependency
// being down must not take the process down).
func connectMilvus(ctx context.Context, svc *service.Service, addr string, holder *lazyIndex) {
	const backoff = 5 * time.Second
	for ctx.Err() == nil {
		idx, err := milvusx.Connect(ctx, addr)
		if err == nil {
			err = idx.EnsureCollection(ctx)
			if err != nil {
				_ = idx.Close()
			}
		}
		if err == nil {
			holder.ptr.Store(idx)
			svc.Metrics.DependencyUp.WithLabelValues("milvus").Set(1)
			svc.Logger.Info("milvus ready", "collection", milvusx.Collection)
			return
		}
		svc.Logger.Error("milvus bootstrap retrying", "error", err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// ensureClickhouse creates the §8.7 tables, retrying with backoff until it
// succeeds or ctx ends. Inserts before the schema exists fail open.
func ensureClickhouse(ctx context.Context, svc *service.Service, sink *chsink.Sink) {
	const backoff = 5 * time.Second
	for ctx.Err() == nil {
		if err := sink.EnsureSchema(ctx); err == nil {
			svc.Metrics.DependencyUp.WithLabelValues("clickhouse").Set(1)
			svc.Logger.Info("clickhouse schema ready")
			return
		} else {
			svc.Logger.Error("clickhouse bootstrap retrying", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}
