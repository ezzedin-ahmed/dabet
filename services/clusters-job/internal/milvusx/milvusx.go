// Package milvusx implements job.CentroidStore on the shared Milvus
// collection topic_centroids (docs §8.5, A22), which clustering-service
// owns for live search. clusters-job writes it: a run replaces the
// creator's centroids wholesale (delete-by-creator, then insert). The
// collection DDL here is identical to clustering-service's so whichever
// service starts first creates the same collection, idempotently.
package milvusx

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	"dabet/pkg/embeddings"

	"dabet/services/clusters-job/internal/job"
)

// Collection is the single centroid collection (A22).
const Collection = "topic_centroids"

// Field names in the collection schema.
const (
	FieldTopicID   = "topic_id"   // primary key: topic or theme id
	FieldParentID  = "parent_id"  // job.ZeroUUID for a topic; topic_id for a theme
	FieldCreatorID = "creator_id" // partition key
	FieldVector    = "vector"     // centroid, embeddings.Dimensions wide, cosine
)

const uuidLen = 36

// Store implements job.CentroidStore on a Milvus client.
type Store struct {
	c client.Client
}

// Connect dials Milvus at addr. It fails when Milvus is unreachable, so
// the caller retries in the background rather than crashing (§4.7).
func Connect(ctx context.Context, addr string) (*Store, error) {
	c, err := client.NewClient(ctx, client.Config{Address: addr})
	if err != nil {
		return nil, fmt.Errorf("milvus connect: %w", err)
	}
	return &Store{c: c}, nil
}

// EnsureCollection creates the collection and its index if absent, then
// loads it. Identical to clustering-service's bootstrap; idempotent.
func (s *Store) EnsureCollection(ctx context.Context) error {
	has, err := s.c.HasCollection(ctx, Collection)
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
		if err := s.c.CreateCollection(ctx, schema, entity.DefaultShardNumber); err != nil {
			return fmt.Errorf("milvus create collection: %w", err)
		}
		idx, err := entity.NewIndexHNSW(entity.COSINE, 16, 200)
		if err != nil {
			return fmt.Errorf("milvus index params: %w", err)
		}
		if err := s.c.CreateIndex(ctx, Collection, FieldVector, idx, false); err != nil {
			return fmt.Errorf("milvus create index: %w", err)
		}
	}
	if err := s.c.LoadCollection(ctx, Collection, false); err != nil {
		return fmt.Errorf("milvus load collection: %w", err)
	}
	return nil
}

// ListByCreator implements job.CentroidStore: the creator's current
// centroids, read from the creator's partition.
func (s *Store) ListByCreator(ctx context.Context, creatorID string) ([]job.Centroid, error) {
	expr := fmt.Sprintf("%s == %q", FieldCreatorID, creatorID)
	rs, err := s.c.Query(ctx, Collection, nil, expr, []string{FieldTopicID, FieldParentID, FieldVector})
	if err != nil {
		return nil, fmt.Errorf("milvus query centroids: %w", err)
	}
	var ids, parents *entity.ColumnVarChar
	var vecs *entity.ColumnFloatVector
	for _, col := range rs {
		switch col.Name() {
		case FieldTopicID:
			ids, _ = col.(*entity.ColumnVarChar)
		case FieldParentID:
			parents, _ = col.(*entity.ColumnVarChar)
		case FieldVector:
			vecs, _ = col.(*entity.ColumnFloatVector)
		}
	}
	if ids == nil || parents == nil || vecs == nil {
		if len(rs) == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("milvus query centroids: unexpected result columns")
	}
	out := make([]job.Centroid, 0, ids.Len())
	for i := 0; i < ids.Len(); i++ {
		id, err := ids.GetAsString(i)
		if err != nil {
			return nil, fmt.Errorf("milvus centroid id: %w", err)
		}
		parent, err := parents.GetAsString(i)
		if err != nil {
			return nil, fmt.Errorf("milvus centroid parent: %w", err)
		}
		out = append(out, job.Centroid{TopicID: id, ParentID: parent, Vector: vecs.Data()[i]})
	}
	return out, nil
}

// ReplaceCreator implements job.CentroidStore: delete the creator's
// previous centroids by expression, insert the new set, flush.
func (s *Store) ReplaceCreator(ctx context.Context, creatorID string, cs []job.Centroid) error {
	expr := fmt.Sprintf("%s == %q", FieldCreatorID, creatorID)
	if err := s.c.Delete(ctx, Collection, "", expr); err != nil {
		return fmt.Errorf("milvus delete centroids: %w", err)
	}
	if len(cs) > 0 {
		ids := make([]string, len(cs))
		parents := make([]string, len(cs))
		creators := make([]string, len(cs))
		vecs := make([][]float32, len(cs))
		for i, c := range cs {
			ids[i] = c.TopicID
			parents[i] = c.ParentID
			creators[i] = creatorID
			vecs[i] = c.Vector
		}
		_, err := s.c.Insert(ctx, Collection, "",
			entity.NewColumnVarChar(FieldTopicID, ids),
			entity.NewColumnVarChar(FieldParentID, parents),
			entity.NewColumnVarChar(FieldCreatorID, creators),
			entity.NewColumnFloatVector(FieldVector, embeddings.Dimensions, vecs),
		)
		if err != nil {
			return fmt.Errorf("milvus insert centroids: %w", err)
		}
	}
	if err := s.c.Flush(ctx, Collection, false); err != nil {
		return fmt.Errorf("milvus flush: %w", err)
	}
	return nil
}

// Close releases the client connection.
func (s *Store) Close() error { return s.c.Close() }
