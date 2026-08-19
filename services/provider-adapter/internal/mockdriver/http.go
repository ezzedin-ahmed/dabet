package mockdriver

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"dabet/pkg/httpx"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
)

// injectRequest is the POST /mock/messages body. connection_id is
// optional: omitted, a deterministic per-creator mock connection is used
// (and auto-registered with the connection source so its watch loop
// starts).
type injectRequest struct {
	ConnectionID string `json:"connection_id,omitempty"`
	CreatorID    string `json:"creator_id"`
	Channel      string `json:"channel"`
	Author       string `json:"author"`
	Text         string `json:"text"`
}

// Handlers exposes the mock platform's HTTP surface on the service mux.
type Handlers struct {
	drv    *Driver
	source *connsource.Static
	seq    atomic.Uint64
}

// NewHandlers wires the injection endpoints against drv and source.
func NewHandlers(drv *Driver, source *connsource.Static) *Handlers {
	return &Handlers{drv: drv, source: source}
}

// Register mounts the endpoints:
//
//	POST /mock/messages  — inject one chat message into the mock stream
//	GET  /mock/deletions — list deletions the mock platform has received
func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /mock/messages", h.postMessage)
	mux.HandleFunc("GET /mock/deletions", h.getDeletions)
}

func (h *Handlers) postMessage(w http.ResponseWriter, r *http.Request) {
	var req injectRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if req.CreatorID == "" || req.Channel == "" || req.Author == "" || req.Text == "" {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, "creator_id, channel, author, and text are required", nil)
		return
	}
	connID := req.ConnectionID
	if connID == "" {
		connID = "mock-" + req.CreatorID
	}
	// Ensure the connection exists so the ingest manager starts (or keeps)
	// a watch loop for it. Add is idempotent for an identical connection.
	if _, ok := h.source.Get(connID); !ok {
		h.source.Add(driver.Connection{
			ID:        connID,
			CreatorID: req.CreatorID,
			Platform:  PlatformName,
		})
	}
	nativeMessageID := fmt.Sprintf("mockmsg-%06d", h.seq.Add(1))
	if err := h.drv.Inject(connID, driver.Message{
		NativeChannelID: req.Channel,
		NativeAuthorID:  req.Author,
		NativeMessageID: nativeMessageID,
		Text:            req.Text,
	}); err != nil {
		httpx.WriteError(w, r, httpx.CodeTooManyRequests, "injection queue full", nil)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{
		"connection_id":     connID,
		"native_message_id": nativeMessageID,
	})
}

func (h *Handlers) getDeletions(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"deletions": h.drv.Deletions(),
	})
}
