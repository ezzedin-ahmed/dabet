package tracing

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestNoopSpanIsInert guards the disabled fast path: the shared span the
// helpers hand back must swallow everything without panicking.
func TestNoopSpanIsInert(t *testing.T) {
	s := NoopSpan()
	if s.IsRecording() {
		t.Error("NoopSpan is recording")
	}
	if s.SpanContext().IsValid() {
		t.Error("NoopSpan has a valid span context")
	}
	s.SetAttributes(MessageID("m1"))
	s.SetName("whatever")
	s.RecordError(context.Canceled)
	s.End()
}

// TestDisabledPathAllocatesNothing is the "zero overhead when off" claim,
// measured rather than asserted in a comment.
func TestDisabledPathAllocatesNothing(t *testing.T) {
	UseTracerProvider(noop.NewTracerProvider())
	got := testing.AllocsPerRun(100, func() {
		if !Active() {
			_ = NoopSpan()
		}
	})
	if got != 0 {
		t.Errorf("disabled fast path allocated %v times per call, want 0", got)
	}
}

// envMap turns a map into a getenv function, so config parsing is tested
// without mutating the process environment.
func envMap(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

type attrCase struct {
	kv   attribute.KeyValue
	want string
}

func TestConfigDisabledByDefault(t *testing.T) {
	cfg, err := ConfigFromEnv(envMap(nil), "moderation-service")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.Enabled {
		t.Error("tracing must be OFF with no OTLP endpoint configured")
	}
	if cfg.ServiceName != "moderation-service" {
		t.Errorf("service name = %q", cfg.ServiceName)
	}
	if cfg.Protocol != "grpc" {
		t.Errorf("protocol = %q, want grpc", cfg.Protocol)
	}
	if cfg.SampleRatio != DefaultSampleRatio {
		t.Errorf("ratio = %v, want %v", cfg.SampleRatio, DefaultSampleRatio)
	}
}

func TestConfigEnabledByEndpoint(t *testing.T) {
	for _, name := range []string{EnvEndpoint, EnvTracesEndpoint} {
		cfg, err := ConfigFromEnv(envMap(map[string]string{name: "http://collector:4317"}), "policy-service")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !cfg.Enabled {
			t.Errorf("%s should enable tracing", name)
		}
	}
}

func TestConfigSDKDisabledWins(t *testing.T) {
	cfg, err := ConfigFromEnv(envMap(map[string]string{
		EnvEndpoint:    "http://collector:4317",
		EnvSDKDisabled: "true",
	}), "policy-service")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.Enabled {
		t.Error("OTEL_SDK_DISABLED=true must win over a configured endpoint")
	}
}

func TestConfigServiceNameOverride(t *testing.T) {
	cfg, err := ConfigFromEnv(envMap(map[string]string{EnvServiceName: "renamed"}), "user-service")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.ServiceName != "renamed" {
		t.Errorf("service name = %q, want renamed", cfg.ServiceName)
	}
}

func TestConfigProtocolValidation(t *testing.T) {
	if _, err := ConfigFromEnv(envMap(map[string]string{EnvProtocol: "carrier-pigeon"}), "x"); err == nil {
		t.Fatal("want error for an unsupported protocol")
	}
	cfg, err := ConfigFromEnv(envMap(map[string]string{
		EnvEndpoint: "http://collector:4318",
		EnvProtocol: "http/protobuf",
	}), "x")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.Protocol != "http/protobuf" {
		t.Errorf("protocol = %q", cfg.Protocol)
	}
	// The signal-specific variable wins over the generic one.
	cfg, err = ConfigFromEnv(envMap(map[string]string{
		EnvProtocol:       "http/protobuf",
		EnvTracesProtocol: "grpc",
	}), "x")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.Protocol != "grpc" {
		t.Errorf("protocol = %q, want the traces-specific grpc", cfg.Protocol)
	}
}

func TestSamplerFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		wantDesc  string
		wantRatio float64
	}{
		{"default", nil, "ParentBased{root:TraceIDRatioBased{0.01}", 0.01},
		{"explicit ratio", map[string]string{EnvSamplerArg: "0.25"}, "ParentBased{root:TraceIDRatioBased{0.25}", 0.25},
		{"always_on", map[string]string{EnvSampler: "always_on"}, "AlwaysOnSampler", 1},
		{"always_off", map[string]string{EnvSampler: "always_off"}, "AlwaysOffSampler", 0},
		{"traceidratio", map[string]string{EnvSampler: "traceidratio", EnvSamplerArg: "0.5"}, "TraceIDRatioBased{0.5}", 0.5},
		{"parentbased_always_on", map[string]string{EnvSampler: "parentbased_always_on"}, "ParentBased{root:AlwaysOnSampler", 1},
		{"case insensitive", map[string]string{EnvSampler: "ALWAYS_ON"}, "AlwaysOnSampler", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _, ratio, err := SamplerFromEnv(envMap(tc.env))
			if err != nil {
				t.Fatalf("SamplerFromEnv: %v", err)
			}
			if !strings.HasPrefix(s.Description(), tc.wantDesc) {
				t.Errorf("sampler = %q, want prefix %q", s.Description(), tc.wantDesc)
			}
			if math.Abs(ratio-tc.wantRatio) > 1e-9 {
				t.Errorf("ratio = %v, want %v", ratio, tc.wantRatio)
			}
		})
	}
}

func TestSamplerFromEnvErrors(t *testing.T) {
	for _, env := range []map[string]string{
		{EnvSamplerArg: "not-a-number"},
		{EnvSamplerArg: "1.5"},
		{EnvSamplerArg: "-0.1"},
		{EnvSampler: "jaeger_remote"},
	} {
		if _, _, _, err := SamplerFromEnv(envMap(env)); err == nil {
			t.Errorf("want error for %v", env)
		}
	}
}

// TestInitNoopPath is the important one: with no endpoint, Init must
// install a provider that is not the SDK's, must not error, and must
// return a shutdown that succeeds instantly. A no-op provider allocates
// no exporter and starts no batch-processor goroutine, which is what
// "zero overhead when off" means in practice.
func TestInitNoopPath(t *testing.T) {
	cfg, shutdown, err := initWith(context.Background(), envMap(nil), "review-service")
	if err != nil {
		t.Fatalf("Init with tracing off must not error: %v", err)
	}
	if cfg.Enabled {
		t.Fatal("cfg.Enabled = true with no endpoint")
	}
	if shutdown == nil {
		t.Fatal("shutdown must never be nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown: %v", err)
	}

	tp := otel.GetTracerProvider()
	if _, isSDK := tp.(*sdktrace.TracerProvider); isSDK {
		t.Fatal("the SDK provider was installed with tracing disabled")
	}
	if Active() {
		t.Fatal("Active() must be false with tracing disabled — the hot-path fast paths depend on it")
	}

	// A no-op span is not recording and has an invalid span context, which
	// is what makes header injection a no-op downstream.
	ctx, span := Tracer().Start(context.Background(), "probe")
	defer span.End()
	if span.IsRecording() {
		t.Error("no-op span must not record")
	}
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("no-op span context must be invalid")
	}
	if TraceIDFrom(ctx) != "" {
		t.Errorf("TraceIDFrom = %q, want empty with tracing off", TraceIDFrom(ctx))
	}
	if SpanIDFrom(ctx) != "" {
		t.Errorf("SpanIDFrom = %q, want empty with tracing off", SpanIDFrom(ctx))
	}
}

func TestInitPropagatorAlwaysInstalled(t *testing.T) {
	if _, _, err := initWith(context.Background(), envMap(nil), "x"); err != nil {
		t.Fatal(err)
	}
	fields := otel.GetTextMapPropagator().Fields()
	var haveTraceparent bool
	for _, f := range fields {
		if f == "traceparent" {
			haveTraceparent = true
		}
	}
	if !haveTraceparent {
		t.Errorf("propagator fields = %v, want traceparent", fields)
	}
}

func TestInitBadConfigDoesNotPanic(t *testing.T) {
	cfg, shutdown, err := initWith(context.Background(), envMap(map[string]string{
		EnvEndpoint:   "http://collector:4317",
		EnvSamplerArg: "banana",
	}), "x")
	if err == nil {
		t.Fatal("want a config error")
	}
	if cfg.Enabled {
		t.Error("a broken config must not report tracing as enabled")
	}
	if shutdown == nil || shutdown(context.Background()) != nil {
		t.Error("shutdown must be a working no-op on the error path")
	}
	if !strings.Contains(err.Error(), EnvSamplerArg) {
		t.Errorf("error should name the offending variable: %v", err)
	}
}

// TestSampledInitEndToEnd checks the enabled path builds a real SDK
// provider without contacting the collector (the OTLP exporters connect
// lazily), and that shutdown is clean.
func TestSampledInitEndToEnd(t *testing.T) {
	// No collector is running, so the exporter's dial fails; silence the
	// global error handler rather than printing a scary line per test run.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {}))
	t.Cleanup(func() {
		// Leave the global provider as the no-op one for other tests.
		_, _, _ = initWith(context.Background(), envMap(nil), "x")
	})
	cfg, shutdown, err := initWith(context.Background(), envMap(map[string]string{
		EnvEndpoint:   "http://127.0.0.1:4317",
		EnvSampler:    "always_on",
		EnvSamplerArg: "1.0",
	}), "moderation-service")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !cfg.Enabled {
		t.Fatal("cfg.Enabled = false with an endpoint set")
	}
	if _, isSDK := otel.GetTracerProvider().(*sdktrace.TracerProvider); !isSDK {
		t.Fatal("the SDK provider was not installed")
	}
	if !Active() {
		t.Fatal("Active() must be true once a real provider is installed")
	}
	ctx, span := Tracer().Start(context.Background(), "probe")
	if !span.IsRecording() {
		t.Error("span should record with always_on")
	}
	if len(TraceIDFrom(ctx)) != 32 {
		t.Errorf("TraceIDFrom = %q, want 32 hex chars", TraceIDFrom(ctx))
	}
	span.End()
	// Bounded, because the export will never succeed here.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Logf("shutdown (no collector is running, which is expected here): %v", err)
	}
}

// TestAttributeHelpersAreIDsOnly is a P4 guard: the exported helpers must
// only ever produce attribute keys that carry opaque identifiers, and
// there must be no helper that takes message text or an author id.
func TestAttributeHelpersAreIDsOnly(t *testing.T) {
	allowed := map[string]bool{
		"dabet.message_id": true,
		"dabet.content_id": true,
		"dabet.creator_id": true,
		"dabet.request_id": true,
		"dabet.platform":   true,
		"dabet.outcome":    true,
		"dabet.detector":   true,
	}
	for _, kv := range []attrCase{
		{MessageID("m1"), "dabet.message_id"},
		{ContentID("c1"), "dabet.content_id"},
		{CreatorID("cr1"), "dabet.creator_id"},
		{RequestID("r1"), "dabet.request_id"},
		{Platform("youtube"), "dabet.platform"},
		{Outcome("flagged"), "dabet.outcome"},
		{Detector("word"), "dabet.detector"},
	} {
		if string(kv.kv.Key) != kv.want {
			t.Errorf("key = %q, want %q", kv.kv.Key, kv.want)
		}
		if !allowed[string(kv.kv.Key)] {
			t.Errorf("attribute key %q is not on the P4 allow-list", kv.kv.Key)
		}
	}
	if len(allowed) != 7 {
		t.Fatal("allow-list changed; re-read the P4 note in tracing.go before adding an attribute")
	}
}
