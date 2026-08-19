// Package tracing is the OpenTelemetry trace half of the docs §4.5
// observability surface ("Prometheus metrics on :9090/metrics,
// OpenTelemetry traces, structured JSON logs to stdout").
//
// # Off by default
//
// Tracing is opt-in. With no OTLP endpoint configured, Init installs the
// no-op tracer provider, allocates no exporter, starts no goroutine, and
// returns a no-op shutdown with a nil error. Every service therefore
// starts and runs — and e2e passes — with no tracing configuration at all
// and with zero per-request overhead beyond a nil-ish interface call.
//
// # P4 — text is radioactive
//
// Message text is NEVER put on a span name, a span attribute, a span
// event, or a recorded error. Not truncated, not hashed, not "just this
// once for debugging". §4.8 states it directly: no service writes text to
// a database, a file, a log, a metric label, or a trace attribute.
// See the Attr* helpers below for the only IDs that are permitted.
//
// # Configuration
//
// The standard OpenTelemetry environment variables are honoured, and are
// the only way to turn tracing on:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT         collector base URL; enables tracing
//	OTEL_EXPORTER_OTLP_TRACES_ENDPOINT  signal-specific override
//	OTEL_EXPORTER_OTLP_PROTOCOL         "grpc" (default) or "http/protobuf"
//	OTEL_EXPORTER_OTLP_HEADERS          passed through by the exporter
//	OTEL_SERVICE_NAME                   overrides the name passed to Init
//	OTEL_RESOURCE_ATTRIBUTES            merged into the resource
//	OTEL_TRACES_SAMPLER                 see SamplerFromEnv
//	OTEL_TRACES_SAMPLER_ARG             ratio, default 0.01
//	OTEL_SDK_DISABLED                   "true" forces the no-op provider
package tracing

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Environment variable names. These are the OpenTelemetry standard names,
// not Dabet-prefixed ones (docs §4.4 asks for names prefixed by concern;
// the concern here already has an industry-standard spelling).
const (
	EnvEndpoint       = "OTEL_EXPORTER_OTLP_ENDPOINT"
	EnvTracesEndpoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	EnvProtocol       = "OTEL_EXPORTER_OTLP_PROTOCOL"
	EnvTracesProtocol = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	EnvServiceName    = "OTEL_SERVICE_NAME"
	EnvSampler        = "OTEL_TRACES_SAMPLER"
	EnvSamplerArg     = "OTEL_TRACES_SAMPLER_ARG"
	EnvSDKDisabled    = "OTEL_SDK_DISABLED"
)

// DefaultSampleRatio is the head-sampling ratio when OTEL_TRACES_SAMPLER_ARG
// is unset: 1 %.
//
// N6 sizes the system at 500 000 messages/second. One trace per message
// with a handful of spans is tens of millions of spans per second, which
// no collector or backend absorbs and which would cost more than the
// system it observes. 1 % of 500 000/s is still 5 000 traces/second —
// ample for latency work against the §4.6 SLI, whose p95 is a Prometheus
// histogram (`moderation_e2e_latency_seconds`) and does not depend on
// traces at all. Traces are for "why is this one slow", not for counting.
//
// Sampling is parent-based, so the decision is made once at the head of a
// message's journey and every downstream hop honours it: a sampled
// message is sampled through the whole cascade, never half a trace.
const DefaultSampleRatio = 0.01

// ScopeName is the instrumentation scope for spans created by Dabet's own
// helpers.
const ScopeName = "dabet/pkg/tracing"

// ShutdownFunc flushes and stops the tracer provider. It is always
// non-nil, and is a no-op when tracing is disabled.
type ShutdownFunc func(context.Context) error

// Config is the resolved tracing configuration.
type Config struct {
	// Enabled is false when no OTLP endpoint is configured (or the SDK is
	// explicitly disabled), which is the default.
	Enabled bool
	// ServiceName is the resource service.name.
	ServiceName string
	// Protocol is "grpc" or "http/protobuf".
	Protocol string
	// Sampler is the resolved head sampler.
	Sampler sdktrace.Sampler
	// SamplerName and SampleRatio record what Sampler was built from, for
	// logging and tests.
	SamplerName string
	SampleRatio float64
}

// ConfigFromEnv resolves the configuration for a service named name from
// getenv (os.Getenv in production, a map lookup in tests). It never
// touches the network and never returns an error for "tracing is off";
// it errors only on genuinely malformed configuration.
func ConfigFromEnv(getenv func(string) string, name string) (Config, error) {
	cfg := Config{ServiceName: name}
	if v := strings.TrimSpace(getenv(EnvServiceName)); v != "" {
		cfg.ServiceName = v
	}

	endpoint := strings.TrimSpace(getenv(EnvTracesEndpoint))
	if endpoint == "" {
		endpoint = strings.TrimSpace(getenv(EnvEndpoint))
	}
	disabled, err := parseBool(getenv(EnvSDKDisabled), false)
	if err != nil {
		return cfg, fmt.Errorf("environment variable %s: %w", EnvSDKDisabled, err)
	}
	cfg.Enabled = endpoint != "" && !disabled

	cfg.Protocol = strings.TrimSpace(getenv(EnvTracesProtocol))
	if cfg.Protocol == "" {
		cfg.Protocol = strings.TrimSpace(getenv(EnvProtocol))
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "grpc"
	}
	switch cfg.Protocol {
	case "grpc", "http/protobuf", "http/json":
	default:
		cfg.Enabled = false
		return cfg, fmt.Errorf("environment variable %s: unsupported protocol %q (want grpc or http/protobuf)", EnvProtocol, cfg.Protocol)
	}

	cfg.Sampler, cfg.SamplerName, cfg.SampleRatio, err = SamplerFromEnv(getenv)
	if err != nil {
		cfg.Enabled = false
		return cfg, err
	}
	return cfg, nil
}

// SamplerFromEnv builds the head sampler from OTEL_TRACES_SAMPLER and
// OTEL_TRACES_SAMPLER_ARG. Supported names are the OpenTelemetry standard
// set minus the remote/jaeger variants:
//
//	parentbased_traceidratio  (default)  ratio from _ARG, default 0.01
//	traceidratio                         ratio from _ARG, ignores parent
//	parentbased_always_on                sample everything with no parent
//	parentbased_always_off
//	always_on
//	always_off
//
// The returned ratio is meaningful only for the ratio samplers.
func SamplerFromEnv(getenv func(string) string) (sdktrace.Sampler, string, float64, error) {
	name := strings.ToLower(strings.TrimSpace(getenv(EnvSampler)))
	if name == "" {
		name = "parentbased_traceidratio"
	}

	ratio := DefaultSampleRatio
	if raw := strings.TrimSpace(getenv(EnvSamplerArg)); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, name, 0, fmt.Errorf("environment variable %s: %w", EnvSamplerArg, err)
		}
		if v < 0 || v > 1 {
			return nil, name, 0, fmt.Errorf("environment variable %s: ratio %v out of range [0,1]", EnvSamplerArg, v)
		}
		ratio = v
	}

	switch name {
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), name, ratio, nil
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio), name, ratio, nil
	case "always_on":
		return sdktrace.AlwaysSample(), name, 1, nil
	case "always_off":
		return sdktrace.NeverSample(), name, 0, nil
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), name, 1, nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), name, 0, nil
	default:
		return nil, name, 0, fmt.Errorf("environment variable %s: unsupported sampler %q", EnvSampler, name)
	}
}

// Init configures the global tracer provider and propagator for a service
// named name, and returns a shutdown that flushes buffered spans.
//
// With no OTLP endpoint configured — the default — it installs the no-op
// provider: no exporter, no batch processor, no background goroutine, no
// log spam, and a shutdown that does nothing. The W3C propagator is
// installed either way, which is free: a no-op span has an invalid span
// context, so Inject writes no headers.
func Init(ctx context.Context, name string) (Config, ShutdownFunc, error) {
	return initWith(ctx, os.Getenv, name)
}

func initWith(ctx context.Context, getenv func(string) string, name string) (Config, ShutdownFunc, error) {
	// W3C trace context + baggage, always. This is what makes a Kafka
	// record's traceparent header interoperable with every other system.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	// Start from the no-op provider so every early return below — config
	// error included — leaves the process in the "tracing is off" state
	// rather than in whatever state it happened to be in.
	UseTracerProvider(noop.NewTracerProvider())

	cfg, err := ConfigFromEnv(getenv, name)
	if err != nil {
		return cfg, noopShutdown, err
	}
	if !cfg.Enabled {
		return cfg, noopShutdown, nil
	}

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithProcessPID(),
		// OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME.
		resource.WithFromEnv(),
		// Applied last so an explicit service name always wins over the
		// SDK's "unknown_service" default; ConfigFromEnv has already let
		// OTEL_SERVICE_NAME override the parameter.
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		// Partial resources are usable (a host detector may fail in a
		// container); a nil one is not.
		if res == nil {
			cfg.Enabled = false
			return cfg, noopShutdown, fmt.Errorf("tracing resource: %w", err)
		}
	}

	exp, err := newExporter(ctx, cfg.Protocol)
	if err != nil {
		cfg.Enabled = false
		return cfg, noopShutdown, fmt.Errorf("tracing exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(cfg.Sampler),
	)
	UseTracerProvider(tp)
	return cfg, tp.Shutdown, nil
}

func newExporter(ctx context.Context, protocol string) (sdktrace.SpanExporter, error) {
	// The exporters read OTEL_EXPORTER_OTLP_* themselves; passing no
	// options is what keeps the standard variables authoritative.
	if protocol == "grpc" {
		return otlptracegrpc.New(ctx)
	}
	return otlptracehttp.New(ctx)
}

func noopShutdown(context.Context) error { return nil }

func parseBool(raw string, def bool) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}
	return strconv.ParseBool(raw)
}

// active records whether a real (non-no-op) tracer provider is installed.
// It is what lets the hot paths — every Kafka produce and consume, every
// HTTP request — skip building span options at all when tracing is off,
// rather than building them and handing them to a tracer that discards
// them. Reading an atomic bool is the entire cost of tracing being
// disabled.
var active atomic.Bool

// UseTracerProvider installs tp as the global provider and records
// whether it is a real one. Init calls it; so must any test that installs
// its own SDK provider, otherwise the hot-path fast paths will still
// think tracing is off and the test will see no spans.
func UseTracerProvider(tp trace.TracerProvider) {
	otel.SetTracerProvider(tp)
	_, isNoop := tp.(noop.TracerProvider)
	active.Store(!isNoop)
}

// Active reports whether spans are actually being recorded anywhere. Use
// it to guard work that exists only to populate a span.
func Active() bool { return active.Load() }

// Tracer returns the tracer for Dabet's own instrumentation. When tracing
// is disabled this is the no-op tracer.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// noopSpan is the span the fast paths hand back when tracing is off: End,
// SetAttributes, RecordError and friends on it are all no-ops, so callers
// need no branching of their own beyond the single Active() check.
var noopSpan = func() trace.Span {
	_, s := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	return s
}()

// NoopSpan returns a shared non-recording span, for the disabled fast
// path of instrumentation helpers.
func NoopSpan() trace.Span { return noopSpan }

// TraceIDFrom returns the 32-hex-character trace id of the span in ctx, or
// "" when there is none (which is the normal case with tracing off). It is
// what puts trace_id on a log line so logs and traces correlate.
func TraceIDFrom(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanIDFrom returns the 16-hex-character span id of the span in ctx, or "".
func SpanIDFrom(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasSpanID() {
		return ""
	}
	return sc.SpanID().String()
}

// ---------------------------------------------------------------------
// Span attribute helpers
//
// P4 — TEXT IS RADIOACTIVE. There is deliberately no AttrText, no
// AttrMessageBody, no AttrPreview and no AttrNormalized. Message text must
// never reach a span name, attribute, event, or recorded error, because a
// span is exported to a third-party backend with a retention policy Dabet
// does not control — which is exactly what §4.8 forbids. If you find
// yourself wanting the text on a span to debug a verdict, add a counter or
// reproduce locally instead.
//
// Identifiers are a narrower question and the answer here is:
//
//   - message_id  ALLOWED, and the one to reach for. It is the join key
//     between the adapter's ingest span, the moderation cascade, the
//     verdict, and the review row; without it a trace cannot be matched to
//     a report of "this message was missed". It is opaque, short-lived
//     (text ages out of Kafka in 24 h, §4.8) and not attributable to a
//     person on its own.
//   - content_id  ALLOWED on stream/channel-scoped spans where the
//     question is "which stream", e.g. the review queue. Low cardinality,
//     opaque (P5).
//   - creator_id  ALLOWED. It is the account we bill and the tenant a
//     trace belongs to.
//   - author_id   NOT USED. It identifies a viewer — a third party who is
//     not a Dabet customer and never consented to anything. §4.8 keeps it
//     out of the indefinitely-retained embedding store for exactly this
//     reason, and a trace backend is no different. Nothing in Dabet needs
//     to know which viewer a span belongs to; the moderation decision is
//     per message.
//
// Metric cardinality rules (§4.5) are untouched by any of this: these are
// span attributes, which are per-trace and sampled, not metric labels.
// Do not mirror them into labels.
// ---------------------------------------------------------------------

// Attribute keys used on Dabet spans.
const (
	AttrKeyMessageID = attribute.Key("dabet.message_id")
	AttrKeyContentID = attribute.Key("dabet.content_id")
	AttrKeyCreatorID = attribute.Key("dabet.creator_id")
	AttrKeyRequestID = attribute.Key("dabet.request_id")
	AttrKeyPlatform  = attribute.Key("dabet.platform")
	AttrKeyOutcome   = attribute.Key("dabet.outcome")
	AttrKeyDetector  = attribute.Key("dabet.detector")
)

// MessageID returns the message_id span attribute. See the P4 note above:
// this is an opaque id, never text.
func MessageID(id string) attribute.KeyValue { return AttrKeyMessageID.String(id) }

// ContentID returns the content_id (stream/channel) span attribute.
func ContentID(id string) attribute.KeyValue { return AttrKeyContentID.String(id) }

// CreatorID returns the creator_id span attribute.
func CreatorID(id string) attribute.KeyValue { return AttrKeyCreatorID.String(id) }

// RequestID returns the X-Request-Id span attribute (docs §4.1), which is
// what ties an HTTP access log to its trace.
func RequestID(id string) attribute.KeyValue { return AttrKeyRequestID.String(id) }

// Platform returns the platform-driver span attribute ("youtube", ...).
func Platform(p string) attribute.KeyValue { return AttrKeyPlatform.String(p) }

// Outcome returns a bounded outcome span attribute ("clean", "flagged",
// "skipped", ...). Never an error string containing text.
func Outcome(o string) attribute.KeyValue { return AttrKeyOutcome.String(o) }

// Detector returns the detector-name span attribute ("word", "rate",
// "duplicate", "semantic", "llm"). The NAME only — never the match.
func Detector(d string) attribute.KeyValue { return AttrKeyDetector.String(d) }
