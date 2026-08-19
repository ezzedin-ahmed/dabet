package report

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"dabet/test/load/internal/promx"
)

func sampleResult() *Result {
	r := New("baseline", "the cascade keeps up", "abc123",
		Machine{CPUs: 8, GOMAXPROCS: 8, OS: "linux", Arch: "amd64"})
	r.Offered.ProfileName = "steady 2 000 msg/s"
	r.Offered.TargetPeakRate = 2000
	r.Offered.Sent = 180_000
	r.Offered.Acked = 180_000
	r.Offered.AchievedRate = 1998
	r.Offered.GeneratorCeiling = 900_000
	r.Moderation.ConsumedTotal = 179_000
	r.Moderation.MessagesByOutcome = map[string]float64{"clean": 170_000, "flagged": 9_000}
	r.Moderation.FailOpen = map[string]float64{}
	r.Moderation.DetectorHits = map[string]float64{"duplicate": 5000}
	r.Moderation.StageP95S = map[string]float64{"llm": 1.1}
	r.Moderation.LLMRequests = map[string]float64{"ok": 300}
	r.Moderation.E2ELatencyPresent = true
	r.Kafka.MessagesPartitions = 64
	r.Check("fail_open_total", "<= 0", "0", true)
	return r
}

// The JSON document is the thing that gets diffed across runs, so its
// shape is a contract: schema tag present, every top-level section
// present, and no NaN (which is not valid JSON and would make the file
// unreadable by anything downstream).
func TestJSONShape(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleResult().WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("result is not valid JSON: %v\n%s", err, buf.String())
	}
	if doc["schema"] != Schema {
		t.Errorf("schema = %v, want %q", doc["schema"], Schema)
	}
	for _, key := range []string{
		"scenario", "hypothesis", "run_id", "started_at", "machine",
		"offered", "moderation", "kafka", "verdicts", "services", "checks", "pass",
	} {
		if _, ok := doc[key]; !ok {
			t.Errorf("result is missing the %q section", key)
		}
	}
	off := doc["offered"].(map[string]any)
	for _, key := range []string{"scheduled", "sent", "achieved_rate_per_s", "send_lag"} {
		if _, ok := off[key]; !ok {
			t.Errorf("offered is missing %q", key)
		}
	}
	mod := doc["moderation"].(map[string]any)
	for _, key := range []string{
		"messages_by_outcome", "detector_hits", "e2e_latency_seconds",
		"sampler_skipped_total", "llm_batch_size_mean", "llm_requests_total", "fail_open_total",
	} {
		if _, ok := mod[key]; !ok {
			t.Errorf("moderation is missing %q", key)
		}
	}
}

// A run in which nothing was flagged has no SLI at all — that is a
// materially different statement from "the SLI was zero", and the
// document must not encode NaN to say it.
func TestJSONHasNoNaN(t *testing.T) {
	r := sampleResult()
	r.Moderation.E2ELatencyPresent = false
	r.Moderation.E2ELatency = ViewOf(promx.Histogram{}) // nothing observed
	if math.IsNaN(r.Moderation.E2ELatency.P95S) || r.Moderation.E2ELatency.Count != 0 {
		t.Fatalf("empty histogram must render as the zero view, got %+v", r.Moderation.E2ELatency)
	}
	var buf bytes.Buffer
	if err := r.WriteJSON(&buf); err != nil {
		t.Fatalf("result would not encode: %v", err)
	}
	if strings.Contains(buf.String(), "NaN") {
		t.Fatal("result JSON contains a bare NaN token, which no JSON parser accepts")
	}
	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	// The absence must still be legible, and it is — via the flag, not
	// via a poisoned number.
	var out bytes.Buffer
	r.WriteTable(&out)
	if !strings.Contains(out.String(), "ABSENT") {
		t.Error("table does not say the SLI histogram was absent")
	}
}

// The table is what a human reads, so the numbers that decide the run
// have to be in it.
func TestTableContainsTheDecidingNumbers(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().WriteTable(&buf)
	out := buf.String()
	for _, want := range []string{
		"scenario  baseline", "OFFERED", "MODERATION", "VERDICTS", "KAFKA", "CHECKS",
		"fail_open_total", "moderation_e2e_latency_seconds", "generator ceiling",
		"partitions", "RESULT: PASS",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary table is missing %q\n---\n%s", want, out)
		}
	}
}

func TestFailingCheckFlipsTheResult(t *testing.T) {
	r := sampleResult()
	r.Check("p95 latency", "< 1.5s", "3.2s", false)
	if r.Pass {
		t.Fatal("a failed check did not flip Pass")
	}
	var buf bytes.Buffer
	r.WriteTable(&buf)
	if !strings.Contains(buf.String(), "RESULT: FAIL") {
		t.Error("table does not report the failure")
	}
	r2 := sampleResult()
	r2.Fatal("generator kept its schedule", "< 250ms", "4000ms", false)
	if r2.Pass {
		t.Fatal("a failed validity check did not flip Pass")
	}
	buf.Reset()
	r2.WriteTable(&buf)
	if !strings.Contains(buf.String(), "[run validity]") {
		t.Error("table does not mark the validity check")
	}
}

// The latency view must report the bucket bracket alongside the
// interpolated quantile, because the SLI histogram straddles the 1.5 s
// N1 target between its 1 s and 2.5 s bounds.
func TestViewOfReportsBounds(t *testing.T) {
	h := promx.Histogram{
		Count: 1000, Sum: 500,
		Buckets: []promx.Bucket{
			{LE: 0.5, Count: 600}, {LE: 1, Count: 900},
			{LE: 2.5, Count: 980}, {LE: math.Inf(1), Count: 1000},
		},
	}
	v := ViewOf(h)
	if v.P95LowerS != 1 || v.P95UpperS != 2.5 {
		t.Errorf("p95 bracket = (%v, %v), want (1, 2.5)", v.P95LowerS, v.P95UpperS)
	}
	if v.FractionUnder1S != 0.9 {
		t.Errorf("fraction <= 1s = %v", v.FractionUnder1S)
	}
	if v.P95S <= v.P95LowerS || v.P95S > v.P95UpperS {
		t.Errorf("interpolated p95 %v outside its own bracket", v.P95S)
	}
}

func TestNumFormatting(t *testing.T) {
	for _, c := range []struct {
		in   float64
		want string
	}{
		{0, "0"}, {12, "12"}, {1234.5, "1234.5"}, {12345, "12.3k"}, {2_500_000, "2.50M"},
		{math.NaN(), "n/a"},
	} {
		if got := num(c.in); got != c.want {
			t.Errorf("num(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
