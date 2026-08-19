package kafkax

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"dabet/pkg/tracing"
)

// withRecordingTracer installs a real SDK provider recording into an
// in-memory exporter, and restores the no-op provider afterwards.
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

func headerValue(rec *kgo.Record, key string) (string, bool) {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func TestCarrierSetGetKeysOnEmptyRecord(t *testing.T) {
	rec := &kgo.Record{Topic: "messages.v1"}
	c := NewRecordCarrier(rec)
	if got := c.Get("traceparent"); got != "" {
		t.Errorf("Get on a header-less record = %q", got)
	}
	if got := c.Keys(); len(got) != 0 {
		t.Errorf("Keys = %v, want none", got)
	}
	c.Set("traceparent", "abc")
	if got, ok := headerValue(rec, "traceparent"); !ok || got != "abc" {
		t.Fatalf("after Set: %q ok=%v", got, ok)
	}
	c.Set("traceparent", "def")
	if len(rec.Headers) != 1 {
		t.Errorf("Set must replace, not append: %v", rec.Headers)
	}
	if got, _ := headerValue(rec, "traceparent"); got != "def" {
		t.Errorf("value = %q, want def", got)
	}
}

func TestCarrierPreservesPreExistingHeaders(t *testing.T) {
	rec := &kgo.Record{
		Topic: "flagged.v1",
		Headers: []kgo.RecordHeader{
			{Key: "x-legacy", Value: []byte("keep-me")},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}
	NewRecordCarrier(rec).Set("traceparent", "tp")
	if len(rec.Headers) != 3 {
		t.Fatalf("headers = %v, want the two originals plus traceparent", rec.Headers)
	}
	if v, _ := headerValue(rec, "x-legacy"); v != "keep-me" {
		t.Errorf("pre-existing header was clobbered: %q", v)
	}
	if v, _ := headerValue(rec, "content-type"); v != "application/json" {
		t.Errorf("pre-existing header was clobbered: %q", v)
	}
	keys := NewRecordCarrier(rec).Keys()
	if len(keys) != 3 {
		t.Errorf("Keys = %v", keys)
	}
}

func TestCarrierNilRecordIsSafe(t *testing.T) {
	c := NewRecordCarrier(nil)
	c.Set("traceparent", "x") // must not panic
	if c.Get("traceparent") != "" || c.Keys() != nil {
		t.Error("nil-record carrier should be inert")
	}
}

// TestInjectExtractRoundTrip is the cross-service contract: a record
// produced in one process must resume the same trace when consumed in
// another. The two halves deliberately use fresh contexts, as they would
// in different processes.
func TestInjectExtractRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers []kgo.RecordHeader
	}{
		{"no pre-existing headers", nil},
		{"with pre-existing headers", []kgo.RecordHeader{{Key: "x-app", Value: []byte("v")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sr := withRecordingTracer(t)

			rec := &kgo.Record{Topic: "messages.v1", Value: []byte(`{"message_id":"m1"}`), Headers: tc.headers}
			produceCtx, produceSpan := StartProduceSpan(context.Background(), rec)
			produceTraceID := trace.SpanContextFromContext(produceCtx).TraceID()
			produceSpanID := trace.SpanContextFromContext(produceCtx).SpanID()
			produceSpan.End()

			tp, ok := headerValue(rec, "traceparent")
			if !ok {
				t.Fatalf("no traceparent header on the produced record: %v", rec.Headers)
			}
			if !strings.Contains(tp, produceTraceID.String()) {
				t.Errorf("traceparent %q does not carry trace id %s", tp, produceTraceID)
			}
			if v, _ := headerValue(rec, "x-app"); tc.headers != nil && v != "v" {
				t.Error("producing clobbered an application header")
			}

			// Fresh context: this is the consuming process.
			rec.Partition, rec.Offset = 2, 41
			consumeCtx, consumeSpan := StartConsumeSpan(context.Background(), rec, "moderation-service")
			consumeSC := trace.SpanContextFromContext(consumeCtx)
			consumeSpan.End()

			if consumeSC.TraceID() != produceTraceID {
				t.Errorf("consume trace id = %s, want %s — the journey split into two traces",
					consumeSC.TraceID(), produceTraceID)
			}

			spans := sr.Ended()
			if len(spans) != 2 {
				t.Fatalf("recorded %d spans, want 2", len(spans))
			}
			var producer, consumer sdktrace.ReadOnlySpan
			for _, s := range spans {
				switch s.SpanKind() {
				case trace.SpanKindProducer:
					producer = s
				case trace.SpanKindConsumer:
					consumer = s
				}
			}
			if producer == nil || consumer == nil {
				t.Fatal("want one PRODUCER and one CONSUMER span")
			}
			if producer.Name() != "messages.v1 send" {
				t.Errorf("producer span name = %q", producer.Name())
			}
			if consumer.Name() != "messages.v1 process" {
				t.Errorf("consumer span name = %q", consumer.Name())
			}
			if consumer.Parent().SpanID() != produceSpanID {
				t.Errorf("consumer parent = %s, want the producer span %s",
					consumer.Parent().SpanID(), produceSpanID)
			}
			assertNoText(t, producer)
			assertNoText(t, consumer)
			assertAttr(t, consumer, "messaging.consumer.group.name", "moderation-service")
			assertAttr(t, consumer, "messaging.destination.name", "messages.v1")
		})
	}
}

// TestInjectIsNoopWhenTracingDisabled is the "must not break existing
// consumers" half: with the no-op provider (the default) a produced
// record is byte-identical to one from before tracing existed.
func TestInjectIsNoopWhenTracingDisabled(t *testing.T) {
	tracing.UseTracerProvider(noop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	rec := &kgo.Record{Topic: "messages.v1", Value: []byte("{}")}
	_, span := StartProduceSpan(context.Background(), rec)
	span.End()
	if len(rec.Headers) != 0 {
		t.Errorf("headers = %v, want none with tracing off", rec.Headers)
	}
}

func TestExtractWithoutTraceparentStartsFresh(t *testing.T) {
	sr := withRecordingTracer(t)
	rec := &kgo.Record{
		Topic:   "usage.v1",
		Headers: []kgo.RecordHeader{{Key: "x-unknown", Value: []byte("ignored")}},
	}
	ctx, span := StartConsumeSpan(context.Background(), rec, "credits-service")
	span.End()
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("a record without traceparent should still get a fresh root span")
	}
	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Parent().IsValid() {
		t.Error("want exactly one root span")
	}
}

func TestExtractIgnoresMalformedTraceparent(t *testing.T) {
	sr := withRecordingTracer(t)
	rec := &kgo.Record{
		Topic:   "deletions.v1",
		Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("not-a-traceparent")}},
	}
	_, span := StartConsumeSpan(context.Background(), rec, "provider-adapter")
	span.End()
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Error("a malformed traceparent must be ignored, not adopted")
	}
}

// assertNoText is the P4 guard: no span attribute may contain the record
// value, and no attribute key may look like it holds text.
func assertNoText(t *testing.T, s sdktrace.ReadOnlySpan) {
	t.Helper()
	banned := []string{"text", "body.content", "payload", "message.value", "author"}
	for _, a := range s.Attributes() {
		key := strings.ToLower(string(a.Key))
		for _, b := range banned {
			if strings.Contains(key, b) {
				t.Errorf("span %q carries a suspect attribute %q (P4)", s.Name(), a.Key)
			}
		}
		if strings.Contains(a.Value.Emit(), "message_id") {
			t.Errorf("span %q attribute %q looks like a serialised record value (P4)", s.Name(), a.Key)
		}
	}
	if strings.Contains(s.Name(), "{") {
		t.Errorf("span name %q looks like a payload (P4)", s.Name())
	}
}

func assertAttr(t *testing.T, s sdktrace.ReadOnlySpan, key, want string) {
	t.Helper()
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			if a.Value.AsString() != want {
				t.Errorf("%s = %q, want %q", key, a.Value.AsString(), want)
			}
			return
		}
	}
	t.Errorf("span %q has no attribute %q", s.Name(), key)
}
