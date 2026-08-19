package kafkax

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"dabet/pkg/tracing"
)

// This file is what makes one message's journey one trace. Dabet's areas
// only talk over Kafka (P1), so without trace context in record headers
// every service would produce its own disconnected trace and the §4.6
// latency question ("where did the 900 ms go between ingest and verdict?")
// would be unanswerable.
//
// W3C trace context travels in the record headers, never in the value:
// the value is the §4.2 schema and adding fields to it would be a
// cross-area contract change. Consumers that do not know about tracing
// simply never look at headers — franz-go hands them the record and they
// read Key and Value — so unknown headers break nothing, which is the
// whole reason headers are the right place.
//
// P4: no span here ever carries message text. Not the value, not a hash of
// it, not its first N bytes. messaging.message.body.size (a byte count) is
// the closest we go, and semconv's messaging.kafka.message.key is
// deliberately NOT set because Dabet's record keys are content_id and
// author_id (§4.2) and author_id must not reach a trace backend.

// RecordCarrier adapts a franz-go record's headers to the OpenTelemetry
// TextMapCarrier interface so the standard W3C propagator can inject and
// extract through them.
type RecordCarrier struct {
	rec *kgo.Record
}

// NewRecordCarrier returns a carrier over rec's headers.
func NewRecordCarrier(rec *kgo.Record) RecordCarrier { return RecordCarrier{rec: rec} }

var _ propagation.TextMapCarrier = RecordCarrier{}

// Get returns the first header value for key, or "".
func (c RecordCarrier) Get(key string) string {
	if c.rec == nil {
		return ""
	}
	for i := range c.rec.Headers {
		if c.rec.Headers[i].Key == key {
			return string(c.rec.Headers[i].Value)
		}
	}
	return ""
}

// Set replaces the value of key, appending the header when absent. It
// preserves every header it does not own, so a record that already
// carries application headers keeps them.
func (c RecordCarrier) Set(key, value string) {
	if c.rec == nil {
		return
	}
	for i := range c.rec.Headers {
		if c.rec.Headers[i].Key == key {
			c.rec.Headers[i].Value = []byte(value)
			return
		}
	}
	c.rec.Headers = append(c.rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys lists the header keys present on the record.
func (c RecordCarrier) Keys() []string {
	if c.rec == nil {
		return nil
	}
	out := make([]string, 0, len(c.rec.Headers))
	for i := range c.rec.Headers {
		out = append(out, c.rec.Headers[i].Key)
	}
	return out
}

// Inject writes the trace context of ctx into rec's headers. With tracing
// disabled the span context is invalid and the propagator writes nothing,
// so records are byte-identical to before this package learned about
// traces.
func Inject(ctx context.Context, rec *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, NewRecordCarrier(rec))
}

// Extract returns ctx with the trace context found in rec's headers, or
// ctx unchanged when there is none.
func Extract(ctx context.Context, rec *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, NewRecordCarrier(rec))
}

// StartProduceSpan opens the PRODUCER span for one record and injects the
// resulting context into its headers. The caller must End the span.
func StartProduceSpan(ctx context.Context, rec *kgo.Record) (context.Context, trace.Span) {
	// Fast path: with tracing off this is one atomic load per produce, and
	// no span options, no attributes, and no propagator call are built at
	// all. At the N6 rate of 500 000 msg/s that difference is the whole
	// argument for the check.
	if !tracing.Active() {
		return ctx, tracing.NoopSpan()
	}
	ctx, span := tracing.Tracer().Start(ctx, rec.Topic+" send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingOperationName("send"),
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingMessageBodySize(len(rec.Value)),
		),
	)
	// Injected after Start so the header carries this span, making the
	// consumer span its child.
	Inject(ctx, rec)
	return ctx, span
}

// StartConsumeSpan continues the producer's trace from rec's headers and
// opens the CONSUMER span for handling it. The caller must End the span.
func StartConsumeSpan(ctx context.Context, rec *kgo.Record, group string) (context.Context, trace.Span) {
	if !tracing.Active() {
		return ctx, tracing.NoopSpan()
	}
	ctx = Extract(ctx, rec)
	return tracing.Tracer().Start(ctx, rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingOperationName("process"),
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
			semconv.MessagingKafkaOffset(int(rec.Offset)),
			semconv.MessagingConsumerGroupName(group),
			semconv.MessagingMessageBodySize(len(rec.Value)),
		),
	)
}

// recordError marks span failed without ever putting a payload on it. The
// error strings Dabet produces on this path are transport errors ("kafka
// commit: ..."), never anything derived from a record value (P4).
func recordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
