package tracing

import (
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
)

// Transport wraps base (nil means http.DefaultTransport) so outbound
// requests carry a CLIENT span and W3C traceparent headers. Everything
// Dabet calls out to — the embedding service, vLLM, credits-ok, the
// clustering assign API, Stripe, OAuth providers — should go through a
// client built from this, otherwise the trace stops at the process edge.
//
// With tracing disabled the wrapped RoundTripper still runs, but the span
// it starts is the no-op span and no headers are injected.
//
// P4: the transport records method, URL and status. Dabet URLs never
// carry message text (bodies do, and bodies are never recorded).
func Transport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base)
}

// HTTPClient returns an *http.Client with the given timeout and an
// instrumented transport.
func HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: Transport(nil)}
}

// GRPCServerOptions returns the grpc.ServerOption set that turns an
// incoming RPC into a SERVER span continuing the caller's trace. Used by
// policy-service for the GetPolicy hot path (§6.7).
func GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
}

// GRPCDialOptions returns the grpc.DialOption set that propagates the
// caller's trace into an outgoing RPC. Used by moderation-service's
// policy client.
func GRPCDialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
}
