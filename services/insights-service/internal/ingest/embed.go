package ingest

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/embeddings"
)

// EmbedClient is the slice of the pkg embeddings client the pipeline uses,
// abstracted so tests can fake it. dabet/pkg/embeddings.Client satisfies it.
type EmbedClient interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Embedder turns a batch of surviving messages into embedding records.
//
// Failure policy (§4.7): on any embedding-service failure the whole batch is
// dropped — fail_open_total{component="embedding"} counts the lost messages
// and embedding_requests_total{outcome="error"} counts the request. The
// pipeline never blocks and never crashes on the embedding dependency.
type Embedder struct {
	client   EmbedClient
	metrics  *Metrics
	failOpen *prometheus.CounterVec // component, reason — the shared fail_open_total
}

// NewEmbedder builds an Embedder. failOpen is the standard fail_open_total
// counter vec from pkg/obs.
func NewEmbedder(client EmbedClient, m *Metrics, failOpen *prometheus.CounterVec) *Embedder {
	return &Embedder{client: client, metrics: m, failOpen: failOpen}
}

// EmbedBatch embeds msgs and returns one record per message, stamped
// embedded_at = now. On failure it returns nil after counting the batch as
// failed open. The returned records carry creator_id, content_id,
// embedded_at, and the vector only — never author_id, never text (§4.8).
func (e *Embedder) EmbedBatch(ctx context.Context, msgs []BufferedMessage, now time.Time) []EmbeddingRecord {
	if len(msgs) == 0 {
		return nil
	}
	texts := make([]string, len(msgs))
	for i, m := range msgs {
		texts[i] = m.Text
	}
	start := time.Now()
	vectors, err := e.client.Embed(ctx, texts)
	e.metrics.EmbedLatency.Observe(time.Since(start).Seconds())
	if err == nil {
		for _, v := range vectors {
			if len(v) != embeddings.Dimensions {
				err = errBadDimensions
				break
			}
		}
	}
	if err != nil {
		e.metrics.EmbedRequestsTotal.WithLabelValues("error").Inc()
		e.failOpen.WithLabelValues("embedding", "request_failed").Add(float64(len(msgs)))
		return nil
	}
	e.metrics.EmbedRequestsTotal.WithLabelValues("ok").Inc()
	recs := make([]EmbeddingRecord, len(msgs))
	for i, m := range msgs {
		recs[i] = EmbeddingRecord{
			CreatorID:  m.CreatorID,
			ContentID:  m.ContentID,
			EmbeddedAt: now,
			Vector:     vectors[i],
		}
	}
	return recs
}

type dimensionError struct{}

func (dimensionError) Error() string { return "embedding: unexpected vector dimensions" }

var errBadDimensions = dimensionError{}
