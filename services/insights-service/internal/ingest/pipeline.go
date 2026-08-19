package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/pkg/obs"
)

// ConsumerGroup is the insights consumer group on messages.v1 and
// flagged.v1 — deliberately distinct from moderation's group so both areas
// receive every message independently (§4.2).
const ConsumerGroup = "insights-service"

// Pipeline glues the stages together:
//
//	messages.v1 ─┐
//	             ├─► exclusion buffer ─► sampler ─► batcher ─► embedder ─► roller ─► S3
//	flagged.v1 ──┘        (2s)
//
// Kafka handlers feed the buffer; Run owns everything downstream of it on a
// single goroutine, so only Buffer is locked.
type Pipeline struct {
	buffer   *Buffer
	sampler  *Sampler
	batcher  *Batcher
	embedder *Embedder
	roller   *Roller
	assign   AssignSender
	metrics  *Metrics
	std      *obs.Metrics
	logger   *slog.Logger
	tick     time.Duration
}

// NewPipeline wires the stages. std is the standard pkg/obs metric set of
// the service runner. assign may be nil to run without live classification;
// when set, every successfully embedded batch is also handed to it
// fire-and-forget (§8.5) after the parquet path has taken it.
func NewPipeline(buffer *Buffer, sampler *Sampler, batcher *Batcher, embedder *Embedder, roller *Roller, assign AssignSender, m *Metrics, std *obs.Metrics, logger *slog.Logger, tick time.Duration) *Pipeline {
	return &Pipeline{
		buffer:   buffer,
		sampler:  sampler,
		batcher:  batcher,
		embedder: embedder,
		roller:   roller,
		assign:   assign,
		metrics:  m,
		std:      std,
		logger:   logger,
		tick:     tick,
	}
}

// HandleMessage consumes one messages.v1 record into the exclusion buffer.
// It never returns an error: a malformed record is counted and skipped, not
// redelivered forever (§4.7 — never block). Per P4, neither text nor opaque
// IDs are ever logged.
func (p *Pipeline) HandleMessage(_ context.Context, rec *kgo.Record) error {
	var msg contracts.Message
	if err := json.Unmarshal(rec.Value, &msg); err != nil || msg.MessageID == "" {
		p.std.KafkaConsumedTotal.WithLabelValues(contracts.TopicMessages, ConsumerGroup, "error").Inc()
		return nil
	}
	// author_id is dropped here and never travels further (§4.8).
	p.buffer.Add(BufferedMessage{
		MessageID: msg.MessageID,
		CreatorID: msg.CreatorID,
		ContentID: msg.ContentID,
		Text:      msg.Text,
	}, time.Now())
	p.std.KafkaConsumedTotal.WithLabelValues(contracts.TopicMessages, ConsumerGroup, "ok").Inc()
	return nil
}

// HandleFlagged consumes one flagged.v1 record. Any flag — auto_delete or
// review — excludes the message from Insights (§8.3): Dabet builds a picture
// of the community, not a dossier on offenders.
func (p *Pipeline) HandleFlagged(_ context.Context, rec *kgo.Record) error {
	var fl contracts.Flagged
	if err := json.Unmarshal(rec.Value, &fl); err != nil || fl.MessageID == "" {
		p.std.KafkaConsumedTotal.WithLabelValues(contracts.TopicFlagged, ConsumerGroup, "error").Inc()
		return nil
	}
	p.buffer.Flag(fl.MessageID)
	p.std.KafkaConsumedTotal.WithLabelValues(contracts.TopicFlagged, ConsumerGroup, "ok").Inc()
	return nil
}

// Run drives releases, batching, embedding, and file rolling until ctx is
// cancelled, then drains: remaining buffered messages are counted as dropped
// (reason "restart", §8.3), pending batches are embedded, and open files are
// flushed to S3.
func (p *Pipeline) Run(ctx context.Context) {
	ticker := time.NewTicker(p.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.shutdown()
			return
		case now := <-ticker.C:
			p.step(ctx, now)
		}
	}
}

// step advances every clocked stage once.
func (p *Pipeline) step(ctx context.Context, now time.Time) {
	for _, msg := range p.buffer.PopDue(now) {
		if !p.sampler.Allow(msg.ContentID, now) {
			p.metrics.DroppedTotal.WithLabelValues(DropReasonSampled).Inc()
			continue
		}
		if batch := p.batcher.Add(msg, now); batch != nil {
			p.process(ctx, batch, now)
		}
	}
	if batch := p.batcher.Due(now); batch != nil {
		p.process(ctx, batch, now)
	}
	p.roller.FlushDue(ctx, now)
}

func (p *Pipeline) process(ctx context.Context, batch []BufferedMessage, now time.Time) {
	if recs := p.embedder.EmbedBatch(ctx, batch, now); recs != nil {
		p.roller.Add(ctx, recs, now)
		if p.assign != nil {
			p.assign.Send(recs)
		}
	}
}

// shutdown performs the drain described on Run. Uploads get a short grace
// context of their own, since the run context is already cancelled.
func (p *Pipeline) shutdown() {
	dropped := p.buffer.Drain()
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now()
	if batch := p.batcher.Flush(); batch != nil {
		p.process(flushCtx, batch, now)
	}
	p.roller.FlushAll(flushCtx)
	p.logger.Info("pipeline drained", "buffered_dropped", dropped)
}
