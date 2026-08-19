// Package milvusx is the Milvus-backed CentroidIndex (docs §8.5, A22): one
// collection, topic_centroids, partitioned by creator_id via a partition
// key, holding topic and theme centroids ONLY — never per-message vectors.
// The full message corpus lives in S3, which is where batch clustering
// reads from.
package milvusx

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	"dabet/pkg/embeddings"

	"dabet/services/clustering-service/internal/cluster"
)

// Collection is the single centroid collection (A22).
const Collection = "topic_centroids"

// Field names in the collection schema.
const (
	FieldTopicID   = "topic_id"   // primary key: topic or theme id
	FieldParentID  = "parent_id"  // cluster.ZeroUUID for a topic; topic_id for a theme
	FieldCreatorID = "creator_id" // partition key
	FieldVector    = "vector"     // centroid, embeddings.Dimensions wide, cosine
)

const uuidLen = 36

// Index implements cluster.CentroidIndex on a Milvus client.
type Index struct {
	c client.Client
}

// Connect dials Milvus at addr. It fails when Milvus is unreachable, so the
// caller retries in the background rather than crashing (§4.7).
func Connect(ctx context.Context, addr string) (*Index, error) {
	c, err := client.NewClient(ctx, client.Config{Address: addr})
	if err != nil {
		return nil, fmt.Errorf("milvus connect: %w", err)
	}
	return &Index{c: c}, nil
}

// EnsureCollection creates the collection and its index if absent, then
// loads it for search. Idempotent across restarts.
func (x *Index) EnsureCollection(ctx context.Context) error {
	has, err := x.c.HasCollection(ctx, Collection)
	if err != nil {
		return fmt.Errorf("milvus has collection: %w", err)
	}
	if !has {
		schema := entity.NewSchema().
			WithName(Collection).
			WithDescription("topic and theme centroids, partitioned by creator (A22); centroids only, never per-message vectors").
			WithField(entity.NewField().WithName(FieldTopicID).WithDataType(entity.FieldTypeVarChar).WithMaxLength(uuidLen).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName(FieldParentID).WithDataType(entity.FieldTypeVarChar).WithMaxLength(uuidLen)).
			WithField(entity.NewField().WithName(FieldCreatorID).WithDataType(entity.FieldTypeVarChar).WithMaxLength(uuidLen).WithIsPartitionKey(true)).
			WithField(entity.NewField().WithName(FieldVector).WithDataType(entity.FieldTypeFloatVector).WithDim(int64(embeddings.Dimensions)))
		if err := x.c.CreateCollection(ctx, schema, entity.DefaultShardNumber); err != nil {
			return fmt.Errorf("milvus create collection: %w", err)
		}
		// A few hundred centroids per active creator (§8.5): HNSW keeps
		// searches fast and the index small.
		idx, err := entity.NewIndexHNSW(entity.COSINE, 16, 200)
		if err != nil {
			return fmt.Errorf("milvus index params: %w", err)
		}
		if err := x.c.CreateIndex(ctx, Collection, FieldVector, idx, false); err != nil {
			return fmt.Errorf("milvus create index: %w", err)
		}
	}
	if err := x.c.LoadCollection(ctx, Collection, false); err != nil {
		return fmt.Errorf("milvus load collection: %w", err)
	}
	return nil
}

// Nearest implements cluster.CentroidIndex: top-1 cosine search filtered to
// the creator's partition (the partition-key expression prunes to it) and
// the given parent level.
func (x *Index) Nearest(ctx context.Context, creatorID, parentID string, vec []float32) (cluster.Match, bool, error) {
	expr := fmt.Sprintf("%s == %q && %s == %q", FieldCreatorID, creatorID, FieldParentID, parentID)
	sp, err := entity.NewIndexHNSWSearchParam(64)
	if err != nil {
		return cluster.Match{}, false, fmt.Errorf("milvus search params: %w", err)
	}
	res, err := x.c.Search(ctx, Collection, nil, expr, []string{FieldTopicID},
		[]entity.Vector{entity.FloatVector(vec)}, FieldVector, entity.COSINE, 1, sp)
	if err != nil {
		return cluster.Match{}, false, fmt.Errorf("milvus search: %w", err)
	}
	if len(res) == 0 || res[0].ResultCount == 0 || res[0].IDs == nil {
		return cluster.Match{}, false, nil
	}
	id, err := res[0].IDs.GetAsString(0)
	if err != nil {
		return cluster.Match{}, false, fmt.Errorf("milvus result id: %w", err)
	}
	return cluster.Match{TopicID: id, Score: res[0].Scores[0]}, true, nil
}

// Close releases the client connection.
func (x *Index) Close() error { return x.c.Close() }
