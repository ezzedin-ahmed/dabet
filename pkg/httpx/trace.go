package httpx

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"dabet/pkg/tracing"
)

// Trace is the server-side tracing middleware: it continues an incoming
// W3C trace (or starts one), opens a SERVER span for the request, and
// links the span to the X-Request-Id of §4.1 in both directions —
// request_id goes on the span, trace_id goes on the log records via
// ContextLogger — so a log line and a trace can each be used to find the
// other.
//
// It must run inside RequestID so the request id is already in the
// context, and outside the application mux so the matched route pattern
// is available when the handler returns.
//
// P4: the span carries method, matched route pattern, and status. The URL
// path is recorded but never the body, and Dabet has no endpoint that
// takes chat text in a path or query parameter.
func Trace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// With tracing off this middleware is one atomic load.
		if !tracing.Active() {
			next.ServeHTTP(w, r)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(),
			propagation.HeaderCarrier(r.Header))

		// The route pattern is only known after the mux has matched, so
		// the span opens with the method and is renamed below.
		ctx, span := tracing.Tracer().Start(ctx, r.Method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodOriginal(r.Method),
				semconv.URLPath(r.URL.Path),
				semconv.ServerAddress(r.Host),
			),
		)
		defer span.End()

		if id := RequestIDFrom(r.Context()); id != "" {
			span.SetAttributes(tracing.RequestID(id))
		}

		sw := &traceStatusWriter{ResponseWriter: w, status: http.StatusOK}
		// ServeMux fills in Pattern on the request value it is handed,
		// so the route has to be read back off the derived request,
		// not off r.
		traced := r.WithContext(ctx)
		next.ServeHTTP(sw, traced)

		route := routeTemplate(traced.Pattern)
		span.SetName(r.Method + " " + route)
		span.SetAttributes(
			semconv.HTTPRoute(route),
			semconv.HTTPResponseStatusCode(sw.status),
		)
		// Per the HTTP semantic conventions a server span is an error
		// only for 5xx; 4xx is the client's problem and marking it
		// Error would drown the backend in false positives.
		if sw.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(sw.status))
		}
	})
}

// routeTemplate turns a matched ServeMux pattern ("GET /v1/reviews/{id}",
// or "/v1/reviews/{id}" when registered without a method) into the path
// template semconv wants for http.route. An unmatched request gets the
// bounded literal "unmatched", never the raw path: a span attribute is
// not a metric label, but an unbounded operation name still makes a
// tracing backend's per-operation views useless.
func routeTemplate(pattern string) string {
	if pattern == "" {
		return "unmatched"
	}
	if i := strings.LastIndex(pattern, " "); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

type traceStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *traceStatusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *traceStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// withTrace adds trace_id/span_id to l when ctx carries a recorded span.
// ContextLogger calls it, so every log line already tagged with a
// request_id also becomes pivotable to its trace, with no change at any
// call site and nothing at all added when tracing is off.
func withTrace(ctx context.Context, l *slog.Logger) *slog.Logger {
	id := tracing.TraceIDFrom(ctx)
	if id == "" {
		return l
	}
	l = l.With("trace_id", id)
	if sid := tracing.SpanIDFrom(ctx); sid != "" {
		l = l.With("span_id", sid)
	}
	return l
}
