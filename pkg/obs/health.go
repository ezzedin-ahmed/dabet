package obs

import (
	"net/http"
	"sync/atomic"
)

// Health backs the /healthz and /readyz endpoints (docs §4.5).
//
// Readiness starts true and, per P2, a moderation-path service that has
// lost a dependency must STAY ready: it fails open and keeps consuming.
// Flipping /readyz to 503 would remove the service from rotation and stop
// chat — exactly the wrong outcome. SetReady(false) is reserved for states
// where the process genuinely cannot serve (e.g. still starting up).
type Health struct {
	ready atomic.Bool
}

// NewHealth returns a Health that reports ready.
func NewHealth() *Health {
	h := &Health{}
	h.ready.Store(true)
	return h
}

// SetReady flips readiness. See the type comment before using false.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// Healthz returns 200 while the process is alive.
func (h *Health) Healthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// Readyz returns 200 when the service can serve, 503 otherwise.
func (h *Health) Readyz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !h.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
