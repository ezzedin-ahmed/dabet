// Package httpapi serves the internal assign endpoint. The transport
// between insights-service and clustering-service is unspecified in the
// docs; both are Area D services, so a synchronous HTTP hop between them is
// an in-area internal call permitted by P1 (which forbids synchronous hops
// BETWEEN areas). The endpoint lives under /internal/ and carries no JWT —
// it is service-to-service, not creator-facing.
//
// P4: request bodies hold vectors, never text or author ids; nothing from
// the body is ever logged.
package httpapi

import (
	"context"
	"net/http"
	"time"

	"dabet/pkg/embeddings"
	"dabet/pkg/httpx"

	"dabet/services/clustering-service/internal/cluster"
)

// AssignPath is the internal assign endpoint path.
const AssignPath = "/internal/v1/assign"

// AssignRecord mirrors the S3 parquet record of §8.4 on the wire.
type AssignRecord struct {
	CreatorID  string    `json:"creator_id"`
	ContentID  string    `json:"content_id"`
	EmbeddedAt time.Time `json:"embedded_at"`
	Vector     []float32 `json:"vector"`
}

// AssignRequest is the POST /internal/v1/assign body.
type AssignRequest struct {
	Records []AssignRecord `json:"records"`
}

// Assigner is the slice of cluster.Assigner the handler needs.
type Assigner interface {
	AssignBatch(ctx context.Context, recs []cluster.Record)
}

// Register mounts the internal routes on mux.
func Register(mux *http.ServeMux, assigner Assigner) {
	mux.Handle("POST "+AssignPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req AssignRequest
		if !httpx.Decode(w, r, &req) {
			return
		}
		recs := make([]cluster.Record, 0, len(req.Records))
		for _, rec := range req.Records {
			if rec.CreatorID == "" || len(rec.Vector) != embeddings.Dimensions {
				httpx.WriteError(w, r, httpx.CodeValidationFailed,
					"records require creator_id and a vector of the embedding dimension", nil)
				return
			}
			recs = append(recs, cluster.Record{
				CreatorID:  rec.CreatorID,
				ContentID:  rec.ContentID,
				EmbeddedAt: rec.EmbeddedAt,
				Vector:     rec.Vector,
			})
		}
		// Assignment fails open internally (drop + fail_open_total), so the
		// caller always gets 204 for a well-formed batch: unassigned and
		// dropped are both normal, non-error outcomes for the sender.
		assigner.AssignBatch(r.Context(), recs)
		w.WriteHeader(http.StatusNoContent)
	}))
}
