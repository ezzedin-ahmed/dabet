// Package scenario is the catalogue of load runs, each with a written
// hypothesis and pass/fail criteria.
//
// A scenario is data, not code: the runner reads one of these and does
// the same thing for all of them, so adding a run means adding a
// Scenario value (or a JSON file with the same shape), not a new code
// path that might measure something subtly different.
package scenario

import (
	"time"

	"dabet/test/load/internal/gen"
	"dabet/test/load/internal/sched"
	"dabet/test/load/internal/setup"
)

// Mode selects where generated messages go.
type Mode string

const (
	// ModeKafka produces messages.v1 directly, bypassing
	// provider-adapter, so what is measured is moderation throughput.
	ModeKafka Mode = "kafka"
	// ModeAdapter drives POST /mock/messages, one HTTP request per
	// message, to measure that hop on its own.
	ModeAdapter Mode = "adapter"
	// ModeSelfBench runs against the null sink to establish the
	// generator's own ceiling on this machine.
	ModeSelfBench Mode = "selfbench"
)

// Criteria are the pass/fail conditions the runner evaluates.
//
// Every field is optional; a zero value means "do not check". This is
// deliberate — a scenario should assert only what its hypothesis is
// about, so a failure names the thing that actually broke.
type Criteria struct {
	// MaxP95Seconds fails the run if the SLI p95 exceeds it. The N1
	// budget is 1.5.
	MaxP95Seconds float64 `json:"max_p95_seconds,omitempty"`

	// MaxFailOpen fails the run above this many fail_open_total events.
	// §4.5 calls the metric the single most important in the system and
	// says it must be zero in steady state, so the steady-state
	// scenarios set 0 and only the drills set a positive number.
	MaxFailOpen float64 `json:"max_fail_open,omitempty"`
	// AllowFailOpen skips the fail-open check entirely (the drills,
	// where fail-opens are the point).
	AllowFailOpen bool `json:"allow_fail_open,omitempty"`

	// MaxFinalLag fails the run if the backlog has not drained to this
	// within the drain window. §4.7 says to accept lag, so this is not
	// "lag is forbidden" — it is "the system caught up afterwards".
	MaxFinalLag int64 `json:"max_final_lag,omitempty"`

	// MaxLagSlope fails when total lag grew faster than this over the
	// run, which is the §4.7 unbounded-growth signal.
	MaxLagSlope float64 `json:"max_lag_slope,omitempty"`

	// MinConsumeFraction fails when moderation consumed less than this
	// fraction of what was produced.
	MinConsumeFraction float64 `json:"min_consume_fraction,omitempty"`

	// MaxSendLagP95Millis is the run-validity check: if the generator
	// itself was this far behind its own schedule, the run measured the
	// generator.
	MaxSendLagP95Millis float64 `json:"max_send_lag_p95_ms,omitempty"`
}

// Scenario is one runnable load test.
type Scenario struct {
	Name       string `json:"name"`
	Hypothesis string `json:"hypothesis"`
	// Proves is the one-line "what this run is evidence for", printed
	// in the summary and reproduced in the README.
	Proves string `json:"proves"`

	Mode    Mode          `json:"mode"`
	Profile sched.Profile `json:"profile"`
	// ProfileName is a human label for the profile shape.
	ProfileName string `json:"profile_name"`

	Population gen.Config        `json:"population"`
	Policy     setup.PolicyShape `json:"policy"`

	// Workers is the number of send goroutines. 0 means GOMAXPROCS.
	Workers int `json:"workers,omitempty"`

	// SampleEvery is how often lag and metrics are sampled during the
	// run.
	SampleEvery time.Duration `json:"sample_every,omitempty"`

	// DrainFor is how long the runner keeps sampling after the last
	// message was sent, so the report can say whether the backlog
	// drained.
	DrainFor time.Duration `json:"drain_for,omitempty"`

	// TrackPerContent turns on the per-content verdict tally the
	// sampler-coverage scenario needs.
	TrackPerContent bool `json:"track_per_content,omitempty"`

	// Drills are §4.7 fault injections executed mid-run.
	Drills []Drill `json:"drills,omitempty"`

	Criteria Criteria `json:"criteria"`
}

// Drill is one fault injected at a point in the run.
type Drill struct {
	// At is the offset from the start of the run.
	At time.Duration `json:"at"`
	// Container is the compose service to act on.
	Container string `json:"container"`
	// Action is stop or start. A drill normally comes in pairs.
	Action string `json:"action"`
	// Expect names the fail_open_total component that must climb while
	// the container is down, per the normative table in §4.7. Empty
	// means no fail-open is expected (Redis, for instance, degrades to
	// skipped stages plus an in-process sampler fallback).
	Expect string `json:"expect,omitempty"`
	// Note explains what the drill is testing.
	Note string `json:"note,omitempty"`
}
