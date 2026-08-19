package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// AssignSender forwards freshly embedded batches to clustering-service for
// live classification (§8.5). Implementations must never block the caller:
// the parquet path is the durable one, assignment is best-effort.
type AssignSender interface {
	Send(recs []EmbeddingRecord)
}

// assignPath is clustering-service's internal assign endpoint. Both
// services are Area D, so this synchronous hop is an in-area internal call
// permitted by P1 (which forbids synchronous hops BETWEEN areas).
const assignPath = "/internal/v1/assign"

// assignRecord mirrors the parquet record of §8.4 on the wire — creator,
// content, timestamp, vector. No author_id, no text (P4).
type assignRecord struct {
	CreatorID  string    `json:"creator_id"`
	ContentID  string    `json:"content_id"`
	EmbeddedAt time.Time `json:"embedded_at"`
	Vector     []float32 `json:"vector"`
}

type assignRequest struct {
	Records []assignRecord `json:"records"`
}

// AsyncAssigner posts embedded batches to clustering-service fire-and-
// forget: Send enqueues onto a bounded queue served by one worker with a
// request timeout. A full queue, a connection failure, or a non-2xx drops
// the batch and counts fail_open_total{component="clustering"} — the
// parquet path is never delayed and never fails because clustering is down
// (§4.7).
type AsyncAssigner struct {
	endpoint string
	httpc    *http.Client
	queue    chan []EmbeddingRecord
	failOpen *prometheus.CounterVec // component, reason
	depUp    *prometheus.GaugeVec   // dependency
}

// NewAsyncAssigner builds an AsyncAssigner posting to endpoint with the
// given per-request timeout and queue depth (in batches). failOpen and
// depUp are the standard pkg/obs vecs.
func NewAsyncAssigner(endpoint string, timeout time.Duration, queueLen int, failOpen *prometheus.CounterVec, depUp *prometheus.GaugeVec) *AsyncAssigner {
	return &AsyncAssigner{
		endpoint: strings.TrimSuffix(endpoint, "/"),
		httpc:    &http.Client{Timeout: timeout},
		queue:    make(chan []EmbeddingRecord, queueLen),
		failOpen: failOpen,
		depUp:    depUp,
	}
}

// Send enqueues a batch without blocking; a full queue drops it.
func (a *AsyncAssigner) Send(recs []EmbeddingRecord) {
	if len(recs) == 0 {
		return
	}
	select {
	case a.queue <- recs:
	default:
		a.failOpen.WithLabelValues("clustering", "queue_full").Add(float64(len(recs)))
	}
}

// Run posts queued batches until ctx is cancelled. Anything still queued at
// shutdown is dropped — assignment is lossy by design; the vectors are
// already in S3 and the next reclustering recovers them.
func (a *AsyncAssigner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case recs := <-a.queue:
			a.post(recs)
		}
	}
}

func (a *AsyncAssigner) post(recs []EmbeddingRecord) {
	req := assignRequest{Records: make([]assignRecord, len(recs))}
	for i, r := range recs {
		req.Records[i] = assignRecord{
			CreatorID:  r.CreatorID,
			ContentID:  r.ContentID,
			EmbeddedAt: r.EmbeddedAt,
			Vector:     r.Vector,
		}
	}
	body, err := json.Marshal(req)
	if err == nil {
		var resp *http.Response
		resp, err = a.httpc.Post(a.endpoint+assignPath, "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				err = fmt.Errorf("assign: status %d", resp.StatusCode)
			}
		}
	}
	if err != nil {
		a.depUp.WithLabelValues("clustering").Set(0)
		a.failOpen.WithLabelValues("clustering", "request_failed").Add(float64(len(recs)))
		return
	}
	a.depUp.WithLabelValues("clustering").Set(1)
}
