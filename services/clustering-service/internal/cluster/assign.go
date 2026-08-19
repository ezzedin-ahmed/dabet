// Package cluster implements live classification (docs §8.5): for each
// embedded message, find the nearest existing topic centroid for that
// creator, assign at >= threshold cosine similarity (A23), then test the
// topic's theme centroids for a sub-assignment. Below threshold the record
// stays unassigned — a normal outcome, not an error: the vector is already
// in S3 and will be considered at the next reclustering.
//
// P4: records carry no author_id and no text, and neither vectors nor ids
// are ever logged or put on a metric label.
package cluster

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ZeroUUID is the sentinel for "no theme" in topic_counts.theme_id and for
// "is a topic" in topics.parent_id (docs §8.7).
const ZeroUUID = "00000000-0000-0000-0000-000000000000"

// Record is one embedded message to classify — the same shape as the S3
// parquet record of §8.4: creator, content, timestamp, vector. Nothing else
// exists to carry (§4.8).
type Record struct {
	CreatorID  string
	ContentID  string
	EmbeddedAt time.Time
	Vector     []float32
}

// Match is a centroid search hit.
type Match struct {
	// TopicID is the centroid's id: a topic id for topic-level centroids,
	// a theme id for theme centroids.
	TopicID string
	// Score is the cosine similarity to the query vector, in [-1, 1].
	Score float32
}

// CentroidIndex searches the Milvus topic_centroids collection (A22 — one
// collection, partitioned by creator_id, centroids only). Faked in tests.
type CentroidIndex interface {
	// Nearest returns the most similar centroid whose parent_id equals
	// parentID — ZeroUUID for topic-level centroids, a topic id for that
	// topic's themes — within the creator's partition, by cosine
	// similarity. ok=false when the creator has no such centroids, which
	// is normal for a cold creator whose clusters have never been built.
	Nearest(ctx context.Context, creatorID, parentID string, vec []float32) (Match, bool, error)
}

// Assigner classifies records against a creator's centroids and buffers
// counter increments toward ClickHouse.
type Assigner struct {
	index     CentroidIndex
	counts    *CountBuffer
	threshold float32
	metrics   *Metrics
	failOpen  *prometheus.CounterVec // component, reason
	depUp     *prometheus.GaugeVec   // dependency
}

// NewAssigner builds an Assigner. threshold is the cosine similarity floor
// (default 0.75, A23). failOpen and depUp are the standard pkg/obs vecs.
func NewAssigner(index CentroidIndex, counts *CountBuffer, threshold float32, m *Metrics, failOpen *prometheus.CounterVec, depUp *prometheus.GaugeVec) *Assigner {
	return &Assigner{
		index:     index,
		counts:    counts,
		threshold: threshold,
		metrics:   m,
		failOpen:  failOpen,
		depUp:     depUp,
	}
}

// AssignBatch classifies recs in order. On a Milvus error the rest of the
// batch is dropped (§4.7 — never block, never crash): the remaining records
// are counted on fail_open_total{component="milvus"} and dependency_up is
// flipped. Dropped assignments are recovered by the next clusters-job run,
// which reads the full corpus from S3.
func (a *Assigner) AssignBatch(ctx context.Context, recs []Record) {
	for i, rec := range recs {
		if err := a.assign(ctx, rec); err != nil {
			a.depUp.WithLabelValues("milvus").Set(0)
			a.failOpen.WithLabelValues("milvus", "search_failed").Add(float64(len(recs) - i))
			return
		}
	}
	if len(recs) > 0 {
		a.depUp.WithLabelValues("milvus").Set(1)
	}
}

// assign classifies one record: nearest topic centroid, threshold test,
// then a theme sub-assignment within the matched topic (§8.5). A record is
// only counted once both searches succeed, so an error drops it entirely.
func (a *Assigner) assign(ctx context.Context, rec Record) error {
	topic, ok, err := a.index.Nearest(ctx, rec.CreatorID, ZeroUUID, rec.Vector)
	if err != nil {
		return err
	}
	if !ok || topic.Score < a.threshold {
		a.metrics.AssignmentsTotal.WithLabelValues(ResultUnassigned).Inc()
		return nil
	}
	themeID, result := ZeroUUID, ResultTopic
	theme, ok, err := a.index.Nearest(ctx, rec.CreatorID, topic.TopicID, rec.Vector)
	if err != nil {
		return err
	}
	if ok && theme.Score >= a.threshold {
		themeID, result = theme.TopicID, ResultTheme
	}
	a.counts.Add(rec.CreatorID, rec.ContentID, topic.TopicID, themeID, rec.EmbeddedAt)
	a.metrics.AssignmentsTotal.WithLabelValues(result).Inc()
	return nil
}
