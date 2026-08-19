package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"dabet/pkg/tracing"
)

func withRecordingTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	rec := tracetest.NewSpanRecorder()
	tracing.UseTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	))
	t.Cleanup(func() { tracing.UseTracerProvider(noop.NewTracerProvider()) })
	return rec
}

// traced builds the same middleware stack service.Run uses.
func traced(h http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/reviews/{id}", h)
	return RequestID(Trace(mux))
}

func TestTraceSpanRecordsRouteMethodStatus(t *testing.T) {
	sr := withRecordingTracer(t)
	h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	req := httptest.NewRequest("GET", "/v1/reviews/abc", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want server", s.SpanKind())
	}
	// The name must be the matched PATTERN, not the raw path — otherwise
	// every review id becomes its own operation in the backend.
	if s.Name() != "GET /v1/reviews/{id}" {
		t.Errorf("span name = %q", s.Name())
	}
	attrs := map[string]string{}
	for _, a := range s.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	if attrs["http.route"] != "/v1/reviews/{id}" {
		t.Errorf("http.route = %q", attrs["http.route"])
	}
	if attrs["http.response.status_code"] != "418" {
		t.Errorf("http.response.status_code = %q", attrs["http.response.status_code"])
	}
	if attrs["http.request.method_original"] != "GET" {
		t.Errorf("http.request.method_original = %q", attrs["http.request.method_original"])
	}
	if attrs["dabet.request_id"] == "" {
		t.Error("the span must carry the X-Request-Id of §4.1")
	}
}

func TestTraceSpanCarriesGeneratedRequestID(t *testing.T) {
	sr := withRecordingTracer(t)
	h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	req := httptest.NewRequest("GET", "/v1/reviews/abc", nil)
	req.Header.Set(HeaderRequestID, "req-fixed-1")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)

	if got := resp.Header().Get(HeaderRequestID); got != "req-fixed-1" {
		t.Errorf("response request id = %q", got)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans", len(spans))
	}
	var found string
	for _, a := range spans[0].Attributes() {
		if string(a.Key) == "dabet.request_id" {
			found = a.Value.AsString()
		}
	}
	if found != "req-fixed-1" {
		t.Errorf("span request_id = %q, want the incoming header", found)
	}
}

// TestTraceContinuesIncomingTrace covers the client-propagation half: a
// caller's traceparent must be adopted, not replaced.
func TestTraceContinuesIncomingTrace(t *testing.T) {
	sr := withRecordingTracer(t)
	h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest("GET", "/v1/reviews/abc", nil)
	upstream := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req.Header.Set("traceparent", upstream)
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans", len(spans))
	}
	if got := spans[0].SpanContext().TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace id = %s, want the caller's", got)
	}
	if got := spans[0].Parent().SpanID().String(); got != "00f067aa0ba902b7" {
		t.Errorf("parent span id = %s, want the caller's", got)
	}
}

func TestTraceMarks5xxOnly(t *testing.T) {
	for _, tc := range []struct {
		status  int
		wantErr bool
	}{
		{http.StatusNotFound, false},
		{http.StatusUnauthorized, false},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
	} {
		sr := withRecordingTracer(t)
		h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/reviews/abc", nil))
		spans := sr.Ended()
		if len(spans) != 1 {
			t.Fatalf("status %d: recorded %d spans", tc.status, len(spans))
		}
		gotErr := spans[0].Status().Code.String() == "Error"
		if gotErr != tc.wantErr {
			t.Errorf("status %d: span error = %v, want %v", tc.status, gotErr, tc.wantErr)
		}
	}
}

func TestTraceUnmatchedRouteIsBounded(t *testing.T) {
	sr := withRecordingTracer(t)
	h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/nope/12345", nil))
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans", len(spans))
	}
	if spans[0].Name() != "GET unmatched" {
		t.Errorf("span name = %q, want the bounded placeholder", spans[0].Name())
	}
}

// TestContextLoggerCorrelation is the logs<->traces half of §4.5: the
// same log line carries request_id and trace_id, so either one finds the
// other.
func TestContextLoggerCorrelation(t *testing.T) {
	withRecordingTracer(t)

	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	var line map[string]any
	h := traced(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ContextLogger(r.Context(), base).Info("handled")
	}))
	req := httptest.NewRequest("GET", "/v1/reviews/abc", nil)
	req.Header.Set(HeaderRequestID, "req-corr-1")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, buf.String())
	}
	if line["request_id"] != "req-corr-1" {
		t.Errorf("request_id = %v", line["request_id"])
	}
	if line["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace_id = %v, want the request's trace", line["trace_id"])
	}
	if sid, _ := line["span_id"].(string); len(sid) != 16 {
		t.Errorf("span_id = %v, want 16 hex chars", line["span_id"])
	}
}

// TestContextLoggerWithTracingOff proves the correlation fields are
// simply absent when tracing is disabled — no empty strings, no noise.
func TestContextLoggerWithTracingOff(t *testing.T) {
	tracing.UseTracerProvider(noop.NewTracerProvider())
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	ctx := context.WithValue(context.Background(), ctxRequestID, "req-off-1")
	ContextLogger(ctx, base).Info("handled")

	s := buf.String()
	if !strings.Contains(s, `"request_id":"req-off-1"`) {
		t.Errorf("request_id missing: %s", s)
	}
	if strings.Contains(s, "trace_id") || strings.Contains(s, "span_id") {
		t.Errorf("trace fields must be absent with tracing off: %s", s)
	}
}
