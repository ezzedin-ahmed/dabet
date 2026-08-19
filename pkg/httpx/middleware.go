package httpx

import (
	"context"
	"crypto/rand"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"

	"dabet/pkg/obs"
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxCreatorID
)

// HeaderRequestID is the request ID header (docs §4.1).
const HeaderRequestID = "X-Request-Id"

// RequestID ensures every request carries an X-Request-Id: it reuses the
// incoming header or generates a ULID, echoes it on the response, and puts
// it in the context for logs and downstream calls.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if id == "" {
			id = ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
	})
}

// RequestIDFrom returns the request ID stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxRequestID).(string)
	return id
}

// ContextLogger returns l with the context's request_id attached, if any,
// plus trace_id/span_id when the context carries a recorded span — which
// is what lets an operator pivot from a log line to its trace and back
// (§4.5). With tracing off nothing is added.
func ContextLogger(ctx context.Context, l *slog.Logger) *slog.Logger {
	if id := RequestIDFrom(ctx); id != "" {
		l = l.With("request_id", id)
	}
	return withTrace(ctx, l)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Instrument records http_requests_total and http_request_duration_seconds.
// The route label is the matched ServeMux pattern, never the raw URL, so
// cardinality stays bounded (docs §4.5).
func Instrument(m *obs.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.HTTPRequestsTotal.WithLabelValues(route, r.Method, strconv.Itoa(sw.status)).Inc()
		m.HTTPRequestDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}
