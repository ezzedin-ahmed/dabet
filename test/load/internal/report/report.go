// Package report is the machine-readable result of one load run and
// the readable summary table that goes with it.
//
// The JSON shape is versioned (Schema) because these files are meant to
// be diffed across runs — a knee that moved is only interesting against
// the run it moved from.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"dabet/test/load/internal/hist"
	"dabet/test/load/internal/kadmlag"
	"dabet/test/load/internal/promx"
	"dabet/test/load/internal/tail"
)

// Schema is the version tag of the result document.
const Schema = "dabet.load.v1"

// N1Target is the p95 end-to-end latency budget of docs §N1.
const N1Target = 1500 * time.Millisecond

// Machine records what the numbers were measured on, because
// laptop-scale absolute throughput means nothing without it.
type Machine struct {
	CPUs       int    `json:"cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	Note       string `json:"note,omitempty"`
}

// Offered is what the schedule asked for and what actually went out.
type Offered struct {
	ProfileName    string       `json:"profile"`
	TargetPeakRate float64      `json:"target_peak_rate_per_s"`
	Scheduled      int64        `json:"scheduled"`
	Sent           int64        `json:"sent"`
	Acked          int64        `json:"acked"`
	Failed         int64        `json:"failed"`
	Bytes          int64        `json:"bytes"`
	DurationS      float64      `json:"duration_s"`
	AchievedRate   float64      `json:"achieved_rate_per_s"`
	SendLag        hist.Summary `json:"send_lag"`
	// GeneratorCeiling is the rate the same population reached against
	// the null sink on this machine. A run whose achieved rate is close
	// to it measured the generator, not Dabet.
	GeneratorCeiling float64 `json:"generator_ceiling_per_s,omitempty"`
}

// LatencyView is a histogram reported honestly: the interpolated
// quantile, plus the bucket bracket the quantile provably sits in, plus
// the raw fraction under the N1 target.
type LatencyView struct {
	Count            float64 `json:"count"`
	MeanS            float64 `json:"mean_s"`
	P50S             float64 `json:"p50_s_est"`
	P95S             float64 `json:"p95_s_est"`
	P99S             float64 `json:"p99_s_est"`
	P95LowerS        float64 `json:"p95_bucket_lower_s"`
	P95UpperS        float64 `json:"p95_bucket_upper_s"`
	P95Unbounded     bool    `json:"p95_above_last_bucket,omitempty"`
	FractionUnder1S  float64 `json:"fraction_le_1s"`
	FractionUnder2S5 float64 `json:"fraction_le_2p5s"`
}

// ViewOf renders a promx histogram.
//
// An empty histogram yields the zero view rather than a set of NaNs:
// encoding/json refuses to marshal NaN, so a single unobserved
// histogram would otherwise cost the run its whole result document.
// "No observations" is carried by Count == 0 and, for the SLI, by
// Moderation.E2ELatencyPresent — never by a NaN.
func ViewOf(h promx.Histogram) LatencyView {
	if h.Count == 0 {
		return LatencyView{}
	}
	lo, hi := h.Bounds(0.95)
	// The top bucket is +Inf, which encoding/json refuses. When the
	// quantile lands there the honest statement is "above the last
	// finite bound", so that bound is reported and P95Unbounded says
	// the real value is somewhere above it.
	unbounded := math.IsInf(hi, 1)
	if unbounded {
		hi = lastFiniteBound(h)
	}
	return LatencyView{
		Count:            h.Count,
		MeanS:            fin(h.Mean()),
		P50S:             fin(h.Quantile(0.50)),
		P95S:             fin(h.Quantile(0.95)),
		P99S:             fin(h.Quantile(0.99)),
		P95LowerS:        fin(lo),
		P95UpperS:        fin(hi),
		P95Unbounded:     unbounded,
		FractionUnder1S:  fin(h.FractionAtMost(1)),
		FractionUnder2S5: fin(h.FractionAtMost(2.5)),
	}
}

// fin maps NaN and infinities to 0 so the result document stays valid
// JSON. Every field it guards has a companion (Count, P95Unbounded,
// E2ELatencyPresent) that says whether the zero means "zero" or "not
// available", so nothing is silently rounded into a real-looking number.
func fin(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

func lastFiniteBound(h promx.Histogram) float64 {
	last := 0.0
	for _, b := range h.Buckets {
		if !math.IsInf(b.LE, 1) {
			last = b.LE
		}
	}
	return last
}

// Moderation is everything read off moderation-service's /metrics as a
// delta across the run.
type Moderation struct {
	MessagesByOutcome map[string]float64 `json:"messages_by_outcome"`
	ConsumedTotal     float64            `json:"kafka_messages_consumed_total"`
	ConsumeRate       float64            `json:"consume_rate_per_s"`
	DetectorHits      map[string]float64 `json:"detector_hits"`
	DetectorActions   map[string]float64 `json:"detector_actions"`
	E2ELatency        LatencyView        `json:"e2e_latency_seconds"`
	E2ELatencyPresent bool               `json:"e2e_latency_present"`
	StageP95S         map[string]float64 `json:"stage_p95_seconds"`
	SamplerSkipped    float64            `json:"sampler_skipped_total"`
	LLMBatchMean      float64            `json:"llm_batch_size_mean"`
	LLMBatchP50       float64            `json:"llm_batch_size_p50"`
	LLMBatches        float64            `json:"llm_batches"`
	LLMRequests       map[string]float64 `json:"llm_requests_total"`
	LLMLatencyP95S    float64            `json:"llm_latency_p95_s"`
	FailOpen          map[string]float64 `json:"fail_open_total"`
	FailOpenTotal     float64            `json:"fail_open_grand_total"`
	// Instances is one entry per moderation-service replica scraped, so
	// a run with the load fragment's extra consumers can show how work
	// split between them.
	Instances map[string]float64 `json:"consumed_by_instance,omitempty"`
}

// KafkaView is the broker-side picture.
type KafkaView struct {
	MessagesPartitions int              `json:"messages_v1_partitions"`
	LagSamples         []kadmlag.Sample `json:"lag_samples"`
	FinalLag           int64            `json:"final_lag"`
	PeakLag            int64            `json:"peak_lag"`
	// LagSlopePerS is the least-squares slope of total lag over the
	// steady part of the run. Positive and sustained is the §4.7
	// overload signal: the verdict still arrives, just later and later.
	LagSlopePerS float64 `json:"lag_slope_per_s"`
	// PartitionImbalance is max/mean of the records THIS run produced
	// per partition: 1.0 is perfectly even, higher means the key
	// distribution concentrated the run on a few partitions.
	PartitionImbalance float64                `json:"partition_imbalance"`
	BusiestPartitions  []kadmlag.PartitionLag `json:"busiest_partitions,omitempty"`
}

// ServiceView is the shared §4.5 surface of any scraped service.
type ServiceView struct {
	FailOpen                 map[string]float64 `json:"fail_open_total,omitempty"`
	DependencyDown           []string           `json:"dependencies_down,omitempty"`
	ConsumerLagFamilyPresent bool               `json:"consumer_lag_metric_present"`
	ScrapeError              string             `json:"scrape_error,omitempty"`
}

// Check is one pass/fail criterion evaluated against the run.
type Check struct {
	Name string `json:"name"`
	Want string `json:"want"`
	Got  string `json:"got"`
	Pass bool   `json:"pass"`
	// Fatal marks a check whose failure invalidates the run rather than
	// reporting a property of the system (e.g. the generator was the
	// bottleneck).
	Fatal bool `json:"fatal,omitempty"`
}

// Result is one scenario run.
type Result struct {
	Schema     string    `json:"schema"`
	Scenario   string    `json:"scenario"`
	Hypothesis string    `json:"hypothesis"`
	RunID      string    `json:"run_id"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at"`
	Machine    Machine   `json:"machine"`

	Config  json.RawMessage `json:"config,omitempty"`
	Offered Offered         `json:"offered"`

	Moderation Moderation             `json:"moderation"`
	Kafka      KafkaView              `json:"kafka"`
	Verdicts   tail.Result            `json:"verdicts"`
	Services   map[string]ServiceView `json:"services"`

	Checks []Check  `json:"checks"`
	Pass   bool     `json:"pass"`
	Notes  []string `json:"notes,omitempty"`

	// Extra carries scenario-specific findings (the sampler coverage
	// table, the drill timeline).
	Extra map[string]any `json:"extra,omitempty"`
}

// New builds an empty result.
func New(scenario, hypothesis, runID string, m Machine) *Result {
	return &Result{
		Schema:     Schema,
		Scenario:   scenario,
		Hypothesis: hypothesis,
		RunID:      runID,
		StartedAt:  time.Now(),
		Machine:    m,
		Services:   map[string]ServiceView{},
		Extra:      map[string]any{},
		Pass:       true,
	}
}

// Check records a criterion and folds it into the overall verdict.
func (r *Result) Check(name, want, got string, pass bool) {
	r.Checks = append(r.Checks, Check{Name: name, Want: want, Got: got, Pass: pass})
	if !pass {
		r.Pass = false
	}
}

// Fatal records a criterion whose failure means the run did not measure
// what it claims to measure.
func (r *Result) Fatal(name, want, got string, pass bool) {
	r.Checks = append(r.Checks, Check{Name: name, Want: want, Got: got, Pass: pass, Fatal: true})
	if !pass {
		r.Pass = false
	}
}

// Note appends free text to the result.
func (r *Result) Note(format string, args ...any) {
	r.Notes = append(r.Notes, fmt.Sprintf(format, args...))
}

// WriteJSON emits the machine-readable document.
func (r *Result) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteTable emits the readable summary.
func (r *Result) WriteTable(w io.Writer) {
	p := func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
	line := strings.Repeat("─", 78)

	p("%s", line)
	p("scenario  %s   run %s", r.Scenario, r.RunID)
	p("why       %s", r.Hypothesis)
	p("machine   %d cpus, GOMAXPROCS=%d, %s/%s%s", r.Machine.CPUs, r.Machine.GOMAXPROCS,
		r.Machine.OS, r.Machine.Arch, prefixed("  ", r.Machine.Note))
	p("window    %s .. %s (%.1fs)", r.StartedAt.Format(time.TimeOnly), r.EndedAt.Format(time.TimeOnly),
		r.EndedAt.Sub(r.StartedAt).Seconds())
	p("%s", line)

	o := r.Offered
	p("OFFERED")
	p("  profile %-24s peak target %10s msg/s", o.ProfileName, num(o.TargetPeakRate))
	p("  sent    %-24s achieved    %10s msg/s", num(float64(o.Sent)), num(o.AchievedRate))
	p("  acked   %-24s failed      %10s", num(float64(o.Acked)), num(float64(o.Failed)))
	p("  send lag p50/p95/max  %8.1f / %8.1f / %8.1f ms   (generator backlog)",
		o.SendLag.P50MS, o.SendLag.P95MS, o.SendLag.MaxMS)
	if o.GeneratorCeiling > 0 {
		p("  generator ceiling     %10s msg/s (null sink, same population)", num(o.GeneratorCeiling))
	}

	m := r.Moderation
	p("")
	p("MODERATION  (moderation-service /metrics delta)")
	p("  consumed %10s   rate %10s msg/s", num(m.ConsumedTotal), num(m.ConsumeRate))
	p("  outcomes %s", kvline(m.MessagesByOutcome))
	if m.E2ELatencyPresent {
		l := m.E2ELatency
		p("  moderation_e2e_latency_seconds  n=%s (flagged only, §4.6)", num(l.Count))
		p("    p50 %6.3fs   p95 %6.3fs   p99 %6.3fs   [p95 provably in %.3f..%.3f s]",
			l.P50S, l.P95S, l.P99S, l.P95LowerS, l.P95UpperS)
		p("    fraction <= 1.0s %6.2f%%      <= 2.5s %6.2f%%      N1 target p95 < %.1fs",
			100*l.FractionUnder1S, 100*l.FractionUnder2S5, N1Target.Seconds())
	} else {
		p("  moderation_e2e_latency_seconds  ABSENT — nothing was flagged, so there is no SLI")
	}
	p("  detectors %s", kvline(m.DetectorHits))
	p("  sampler_skipped_total %s", num(m.SamplerSkipped))
	p("  llm batches %s  mean batch %.1f  p50 batch %.0f  requests %s",
		num(m.LLMBatches), m.LLMBatchMean, m.LLMBatchP50, kvline(m.LLMRequests))
	p("  llm_latency p95 %.3fs", m.LLMLatencyP95S)
	p("  stage p95 %s", kvlineF(m.StageP95S, "%.4fs"))
	p("  fail_open_total %s  (MUST be 0 in steady state, §4.5)", kvline(m.FailOpen))
	if len(m.Instances) > 1 {
		p("  consumed by instance %s", kvline(m.Instances))
	}

	v := r.Verdicts
	p("")
	p("VERDICTS  (flagged.v1 tailed directly, full resolution)")
	p("  verdicts %s   rate %s /s", num(float64(v.Verdicts)), num(v.VerdictRate))
	p("  flagged_at - intended_send  p50 %8.1f  p95 %8.1f  p99 %8.1f  max %8.1f ms",
		v.SLILatency.P50MS, v.SLILatency.P95MS, v.SLILatency.P99MS, v.SLILatency.MaxMS)
	p("  arrival at harness          p50 %8.1f  p95 %8.1f  p99 %8.1f  max %8.1f ms",
		v.ArrivalLatency.P50MS, v.ArrivalLatency.P95MS, v.ArrivalLatency.P99MS, v.ArrivalLatency.MaxMS)
	p("  fraction under 1.5s %.2f%%", 100*v.FractionUnder1s5)
	if v.ClockSkewedSamples > 0 {
		p("  !! %s samples discarded as clock skew: the host clock moved during the run,",
			num(float64(v.ClockSkewedSamples)))
		p("     so the latency above describes only the samples that survived. Re-run.")
	}
	p("  by detector %s", kvlineI(v.ByDetector))

	k := r.Kafka
	p("")
	p("KAFKA")
	p("  messages.v1 partitions %d   (§4.2 target 512; local compose default 3)", k.MessagesPartitions)
	p("  lag  peak %s   final %s   slope %+.1f msg/s over the run", num(float64(k.PeakLag)),
		num(float64(k.FinalLag)), k.LagSlopePerS)
	p("  partition imbalance (max/mean produced by THIS run) %.2fx", k.PartitionImbalance)
	for _, pl := range k.BusiestPartitions {
		p("    p%-3d produced %10s  lag %10s", pl.Partition, num(float64(pl.End)), num(float64(pl.Lag)))
	}

	if len(r.Services) > 0 {
		p("")
		p("SERVICES")
		names := make([]string, 0, len(r.Services))
		for n := range r.Services {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			s := r.Services[n]
			switch {
			case s.ScrapeError != "":
				p("  %-20s scrape failed: %s", n, s.ScrapeError)
			default:
				p("  %-20s fail_open %s%s", n, kvline(s.FailOpen), prefixed("  deps down: ", strings.Join(s.DependencyDown, ",")))
			}
		}
	}

	if len(r.Notes) > 0 {
		p("")
		p("NOTES")
		for _, n := range r.Notes {
			p("  - %s", n)
		}
	}

	p("")
	p("CHECKS")
	for _, c := range r.Checks {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
		}
		tag := ""
		if c.Fatal {
			tag = " [run validity]"
		}
		p("  [%s] %-38s want %-22s got %s%s", mark, c.Name, c.Want, c.Got, tag)
	}
	verdict := "PASS"
	if !r.Pass {
		verdict = "FAIL"
	}
	p("%s", line)
	p("RESULT: %s", verdict)
	p("%s", line)
}

func prefixed(prefix, s string) string {
	if s == "" {
		return ""
	}
	return prefix + s
}

func num(f float64) string {
	if math.IsNaN(f) {
		return "n/a"
	}
	switch {
	case math.Abs(f) >= 1e6:
		return fmt.Sprintf("%.2fM", f/1e6)
	case math.Abs(f) >= 1e4:
		return fmt.Sprintf("%.1fk", f/1e3)
	case f == math.Trunc(f):
		return fmt.Sprintf("%.0f", f)
	default:
		return fmt.Sprintf("%.1f", f)
	}
}

func kvline(m map[string]float64) string { return kvlineF(m, "") }

func kvlineF(m map[string]float64, format string) string {
	if len(m) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if format == "" {
			parts = append(parts, k+"="+num(m[k]))
		} else {
			parts = append(parts, k+"="+fmt.Sprintf(format, m[k]))
		}
	}
	return strings.Join(parts, " ")
}

func kvlineI(m map[string]int64) string {
	f := make(map[string]float64, len(m))
	for k, v := range m {
		f[k] = float64(v)
	}
	return kvline(f)
}
