package promx

import (
	"math"
	"strings"
	"testing"
	"time"
)

// A real fragment of moderation-service's /metrics: histograms with
// +Inf buckets, label sets in several orders, a NaN summary quantile,
// and a label value containing a comma and a quote.
const sample = `# HELP moderation_messages_total Messages consumed from messages.v1 by outcome.
# TYPE moderation_messages_total counter
moderation_messages_total{outcome="clean"} 12345
moderation_messages_total{outcome="flagged"} 678
moderation_messages_total{outcome="skipped"} 9
# HELP moderation_e2e_latency_seconds flagged_at - ingested_at for flagged messages (the SLI, §4.6).
# TYPE moderation_e2e_latency_seconds histogram
moderation_e2e_latency_seconds_bucket{le="0.05"} 100
moderation_e2e_latency_seconds_bucket{le="0.1"} 200
moderation_e2e_latency_seconds_bucket{le="0.25"} 400
moderation_e2e_latency_seconds_bucket{le="0.5"} 600
moderation_e2e_latency_seconds_bucket{le="1"} 900
moderation_e2e_latency_seconds_bucket{le="2.5"} 980
moderation_e2e_latency_seconds_bucket{le="5"} 1000
moderation_e2e_latency_seconds_bucket{le="+Inf"} 1000
moderation_e2e_latency_seconds_sum 512.5
moderation_e2e_latency_seconds_count 1000
# HELP fail_open_total Messages that went unmoderated because something was broken.
# TYPE fail_open_total counter
fail_open_total{component="llm",reason=""} 32
fail_open_total{reason="no_credits",component="credits"} 4
fail_open_total{component="redis",reason=""} 0
# TYPE moderation_stage_duration_seconds histogram
moderation_stage_duration_seconds_bucket{stage="llm",le="1"} 5
moderation_stage_duration_seconds_bucket{stage="llm",le="2.5"} 10
moderation_stage_duration_seconds_bucket{stage="llm",le="+Inf"} 10
moderation_stage_duration_seconds_sum{stage="llm"} 9.5
moderation_stage_duration_seconds_count{stage="llm"} 10
# TYPE dependency_up gauge
dependency_up{dependency="redis"} 1
dependency_up{dependency="llm"} 0
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{route="/v1/x,y",quantile="0.5"} NaN
# TYPE weird_metric gauge
weird_metric{note="he said \"hi\", loudly"} 7
this line is not a metric
`

func mustParse(t *testing.T) *Snapshot {
	t.Helper()
	s, err := Parse("test", time.Now(), strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParseCountersAndLabels(t *testing.T) {
	s := mustParse(t)
	if got := s.Sum("moderation_messages_total", nil); got != 13032 {
		t.Errorf("total messages = %v, want 13032", got)
	}
	if got := s.Sum("moderation_messages_total", map[string]string{"outcome": "flagged"}); got != 678 {
		t.Errorf("flagged = %v, want 678", got)
	}
	if got := s.Types["moderation_e2e_latency_seconds"]; got != "histogram" {
		t.Errorf("TYPE not captured: %q", got)
	}
	// Label order must not matter: the fail_open series are written
	// with component and reason in different orders.
	by := s.ByLabel("fail_open_total", "component", nil)
	if by["llm"] != 32 || by["credits"] != 4 || by["redis"] != 0 {
		t.Errorf("fail_open by component = %v", by)
	}
}

func TestParseHandlesQuotedLabelValues(t *testing.T) {
	s := mustParse(t)
	ser := s.Series("weird_metric", nil)
	if len(ser) != 1 {
		t.Fatalf("weird_metric series = %d, want 1", len(ser))
	}
	if got := ser[0].Labels["note"]; got != `he said "hi", loudly` {
		t.Errorf("label value = %q", got)
	}
	// A comma inside a quoted value must not split the label set.
	route := s.Series("http_request_duration_seconds", map[string]string{"route": "/v1/x,y"})
	if len(route) != 1 {
		t.Fatalf("summary series with a comma in the label = %d, want 1", len(route))
	}
	if !math.IsNaN(route[0].Value) {
		t.Errorf("NaN value = %v", route[0].Value)
	}
}

func TestHistogramReassembly(t *testing.T) {
	s := mustParse(t)
	h, ok := s.Histogram("moderation_e2e_latency_seconds", nil)
	if !ok {
		t.Fatal("histogram not found")
	}
	if h.Count != 1000 || h.Sum != 512.5 {
		t.Fatalf("count=%v sum=%v", h.Count, h.Sum)
	}
	if len(h.Buckets) != 8 {
		t.Fatalf("buckets = %d, want 8", len(h.Buckets))
	}
	if !math.IsInf(h.Buckets[7].LE, 1) {
		t.Errorf("last bucket bound = %v, want +Inf", h.Buckets[7].LE)
	}
	if got := h.Mean(); math.Abs(got-0.5125) > 1e-9 {
		t.Errorf("mean = %v", got)
	}
}

// Quantiles must match Prometheus's histogram_quantile, and the bucket
// bracket must be reported honestly: the SLI histogram straddles the
// 1.5 s N1 target between its 1 s and 2.5 s bounds, so an interpolated
// p95 near the target is decided by interpolation, not by the data.
func TestHistogramQuantileAndBounds(t *testing.T) {
	s := mustParse(t)
	h, _ := s.Histogram("moderation_e2e_latency_seconds", nil)

	// Rank 950 falls in (900, 980] i.e. the (1, 2.5] bucket:
	// 1 + (2.5-1)*(950-900)/(980-900) = 1.9375.
	if got := h.Quantile(0.95); math.Abs(got-1.9375) > 1e-6 {
		t.Errorf("p95 = %v, want 1.9375", got)
	}
	lo, hi := h.Bounds(0.95)
	if lo != 1 || hi != 2.5 {
		t.Errorf("p95 bounds = (%v, %v), want (1, 2.5)", lo, hi)
	}
	// p50: rank 500 in (400, 600] i.e. (0.25, 0.5]:
	// 0.25 + 0.25*(500-400)/200 = 0.375.
	if got := h.Quantile(0.50); math.Abs(got-0.375) > 1e-6 {
		t.Errorf("p50 = %v, want 0.375", got)
	}
	if got := h.FractionAtMost(1); got != 0.9 {
		t.Errorf("fraction <= 1s = %v, want 0.9", got)
	}
	if got := h.FractionAtMost(2.5); got != 0.98 {
		t.Errorf("fraction <= 2.5s = %v, want 0.98", got)
	}
	// 1.5 is not a bucket bound: the honest answer is the largest bound
	// at or below it, a lower bound on the true fraction.
	if got := h.FractionAtMost(1.5); got != 0.9 {
		t.Errorf("fraction <= 1.5s = %v, want the 0.9 lower bound", got)
	}
	empty := Histogram{}
	if got := empty.Quantile(0.95); !math.IsNaN(got) {
		t.Errorf("quantile of an empty histogram = %v, want NaN", got)
	}
}

func TestHistogramWithSelector(t *testing.T) {
	s := mustParse(t)
	h, ok := s.Histogram("moderation_stage_duration_seconds", map[string]string{"stage": "llm"})
	if !ok {
		t.Fatal("stage histogram not found")
	}
	if h.Count != 10 || h.Sum != 9.5 {
		t.Fatalf("count=%v sum=%v", h.Count, h.Sum)
	}
	if _, ok := s.Histogram("moderation_stage_duration_seconds", map[string]string{"stage": "nope"}); ok {
		t.Error("selector matched a stage that does not exist")
	}
}

// A metric family that is declared but never observed exports nothing.
// Telling that apart from "it read zero" is the difference between
// "lag was fine" and "lag was never measured", so Has() is load-bearing.
func TestHasDistinguishesAbsentFromZero(t *testing.T) {
	s := mustParse(t)
	if !s.Has("moderation_e2e_latency_seconds") {
		t.Error("histogram family reported absent")
	}
	if !s.Has("fail_open_total") {
		t.Error("counter family reported absent")
	}
	if s.Has("kafka_consumer_lag_messages") {
		t.Error("a family that is not in the payload reported present")
	}
}

func TestDelta(t *testing.T) {
	before, err := Parse("t", time.Now(), strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	after, err := Parse("t", time.Now(), strings.NewReader(strings.NewReplacer(
		`moderation_messages_total{outcome="clean"} 12345`, `moderation_messages_total{outcome="clean"} 12445`,
		`moderation_e2e_latency_seconds_count 1000`, `moderation_e2e_latency_seconds_count 1100`,
		`dependency_up{dependency="llm"} 0`, `dependency_up{dependency="llm"} 1`,
	).Replace(sample)))
	if err != nil {
		t.Fatal(err)
	}
	d, restarted := Delta(before, after)
	if restarted {
		t.Error("false restart detected")
	}
	if got := d.Sum("moderation_messages_total", map[string]string{"outcome": "clean"}); got != 100 {
		t.Errorf("counter delta = %v, want 100", got)
	}
	if got := d.Sum("moderation_e2e_latency_seconds_count", nil); got != 100 {
		t.Errorf("histogram count delta = %v, want 100", got)
	}
	// Gauges pass through as their current value, not a difference.
	if got := d.Sum("dependency_up", map[string]string{"dependency": "llm"}); got != 1 {
		t.Errorf("gauge delta = %v, want the current value 1", got)
	}
}

// A counter that went backwards means the process restarted mid-run.
// The run has to be told, or it silently reports a negative delta as a
// number.
func TestDeltaDetectsRestart(t *testing.T) {
	before, _ := Parse("t", time.Now(), strings.NewReader(sample))
	after, _ := Parse("t", time.Now(), strings.NewReader(strings.Replace(sample,
		`moderation_messages_total{outcome="clean"} 12345`,
		`moderation_messages_total{outcome="clean"} 7`, 1)))
	d, restarted := Delta(before, after)
	if !restarted {
		t.Fatal("restart not detected")
	}
	if got := d.Sum("moderation_messages_total", map[string]string{"outcome": "clean"}); got != 7 {
		t.Errorf("post-restart value = %v, want the raw 7", got)
	}
}

func TestSubHistogram(t *testing.T) {
	before, _ := Parse("t", time.Now(), strings.NewReader(sample))
	after, _ := Parse("t", time.Now(), strings.NewReader(strings.NewReplacer(
		`moderation_e2e_latency_seconds_bucket{le="1"} 900`, `moderation_e2e_latency_seconds_bucket{le="1"} 1000`,
		`moderation_e2e_latency_seconds_bucket{le="2.5"} 980`, `moderation_e2e_latency_seconds_bucket{le="2.5"} 1080`,
		`moderation_e2e_latency_seconds_bucket{le="5"} 1000`, `moderation_e2e_latency_seconds_bucket{le="5"} 1100`,
		`moderation_e2e_latency_seconds_bucket{le="+Inf"} 1000`, `moderation_e2e_latency_seconds_bucket{le="+Inf"} 1100`,
		`moderation_e2e_latency_seconds_count 1000`, `moderation_e2e_latency_seconds_count 1100`,
		`moderation_e2e_latency_seconds_sum 512.5`, `moderation_e2e_latency_seconds_sum 612.5`,
	).Replace(sample)))
	bh, _ := before.Histogram("moderation_e2e_latency_seconds", nil)
	ah, _ := after.Histogram("moderation_e2e_latency_seconds", nil)
	d := SubHistogram(bh, ah)
	if d.Count != 100 || math.Abs(d.Sum-100) > 1e-9 {
		t.Fatalf("delta count=%v sum=%v", d.Count, d.Sum)
	}
	// All 100 new observations landed at or below 1 s.
	if got := d.FractionAtMost(1); got != 1 {
		t.Errorf("fraction <= 1s in the delta = %v, want 1", got)
	}
}

func TestParseRejectsNothingUseful(t *testing.T) {
	s, err := Parse("t", time.Now(), strings.NewReader("# HELP only\n\n   \nbroken line\nfoo 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Sum("foo", nil); got != 1 {
		t.Errorf("a good line after a broken one was lost: %v", got)
	}
}
