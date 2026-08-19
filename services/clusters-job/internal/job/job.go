// Package job is the clusters-job batch pipeline (docs §8.6): for one
// (creator, window) it reads the creator's parquet embeddings from S3,
// runs a two-pass HDBSCAN (coarse → topics, fine within each topic →
// themes), computes L2-normalised mean centroids, labels clusters with an
// LLM from still-in-retention message text, replaces the creator's
// centroids in Milvus, and upserts versioned topic rows in ClickHouse.
//
// P4: message text is radioactive. It is held in memory for the duration
// of a run only, and must never be logged, persisted, or carried in
// errors.
package job

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// ZeroUUID is the nil parent for topic-level rows (§8.7).
const ZeroUUID = "00000000-0000-0000-0000-000000000000"

// Trigger names — the clusters_job_runs_total{trigger} label values and
// the §8.6 trigger table.
const (
	TriggerBootstrap  = "bootstrap"
	TriggerDoubled    = "doubled"
	TriggerUnassigned = "unassigned"
	TriggerOnDemand   = "on_demand"
	TriggerManual     = "manual" // RUN_ONCE invocation
)

// Decision is one resolved run request: recluster this creator over
// [From, To).
type Decision struct {
	CreatorID string
	Trigger   string
	From, To  time.Time
	// JobID identifies an on-demand run; deterministic for a given
	// (creator, window) so a retried request converges on the same job.
	JobID string
}

// Result summarises a completed run.
type Result struct {
	PointsRead      int // points found in S3 for the window
	PointsProcessed int // after the cap
	Topics          int
	Themes          int
}

// DeterministicUUID derives a stable UUID-formatted id from parts. Used
// for topic/theme/job ids so that re-running the same window is
// idempotent: the same input names the same entities. The bytes are a
// SHA-256 prefix with RFC 4122 version/variant bits set so every consumer
// (ClickHouse UUID columns included) parses it.
func DeterministicUUID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)[:16]
	sum[6] = (sum[6] & 0x0f) | 0x40 // version 4
	sum[8] = (sum[8] & 0x3f) | 0x80 // RFC 4122 variant
	dst := hex.EncodeToString(sum)
	return dst[0:8] + "-" + dst[8:12] + "-" + dst[12:16] + "-" + dst[16:20] + "-" + dst[20:32]
}

// ReclusterJobID names an on-demand recluster job for (creator, window).
func ReclusterJobID(creatorID string, from, to time.Time) string {
	return DeterministicUUID("recluster", creatorID,
		from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
}

// UsageIdempotencyKey is the deterministic usage.v1 key (§4.2): a
// redelivered event is recognised by credits-service and discarded rather
// than double-charged. On-demand runs are keyed by their job id,
// scheduled runs by their window.
func UsageIdempotencyKey(d Decision) string {
	if d.Trigger == TriggerOnDemand {
		return fmt.Sprintf("recluster:%s:%s", d.JobID, d.CreatorID)
	}
	runWindow := d.From.UTC().Format(time.RFC3339) + "/" + d.To.UTC().Format(time.RFC3339)
	return fmt.Sprintf("job:%s:%s", runWindow, d.CreatorID)
}
