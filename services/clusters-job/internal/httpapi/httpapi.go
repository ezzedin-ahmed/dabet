// Package httpapi serves the on-demand recluster endpoint (docs §8.6):
//
//	POST /v1/topics/recluster {from, to} -> 202 {job_id}
//
// JWT-authed; the creator can only recluster their own history — the
// creator id comes exclusively from the token, never from the body.
// Windows older than 7 days are explicitly allowed: the feature exists to
// rewrite historical data, and doing so changes that creator's dashboard
// for the window (§8.6 — label it in the UI).
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"dabet/pkg/httpx"

	"dabet/services/clusters-job/internal/job"
)

// Enqueuer queues an on-demand run; false means the queue is full.
type Enqueuer interface {
	Enqueue(d job.Decision) bool
}

// API handles the recluster route.
type API struct {
	enq Enqueuer
	log *slog.Logger
}

// NewAPI builds the handler.
func NewAPI(enq Enqueuer, log *slog.Logger) *API {
	return &API{enq: enq, log: log}
}

// Register mounts the route on mux behind JWT auth.
func Register(mux *http.ServeMux, verifier *httpx.Verifier, enq Enqueuer, log *slog.Logger) {
	a := NewAPI(enq, log)
	auth := httpx.Auth(verifier)
	mux.Handle("POST /v1/topics/recluster", auth(http.HandlerFunc(a.Recluster)))
}

type reclusterRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type reclusterResponse struct {
	JobID string `json:"job_id"`
}

// Recluster validates the window, derives the deterministic job id, and
// queues the run. 202: the job runs asynchronously on the scheduler loop.
func (a *API) Recluster(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	var req reclusterRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	from, err := time.Parse(time.RFC3339, req.From)
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "from must be RFC 3339", nil)
		return
	}
	to, err := time.Parse(time.RFC3339, req.To)
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "to must be RFC 3339", nil)
		return
	}
	from, to = from.UTC(), to.UTC()
	if !from.Before(to) {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "from must be before to", nil)
		return
	}
	d := job.Decision{
		CreatorID: creatorID,
		Trigger:   job.TriggerOnDemand,
		From:      from,
		To:        to,
		JobID:     job.ReclusterJobID(creatorID, from, to),
	}
	if !a.enq.Enqueue(d) {
		httpx.WriteError(w, r, httpx.CodeUnavailable, "recluster queue is full, retry later", nil)
		return
	}
	a.log.Info("recluster queued", "creator_id", creatorID, "job_id", d.JobID,
		"from", from.Format(time.RFC3339), "to", to.Format(time.RFC3339))
	httpx.WriteJSON(w, http.StatusAccepted, reclusterResponse{JobID: d.JobID})
}
