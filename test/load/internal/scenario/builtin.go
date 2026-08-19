package scenario

import (
	"fmt"
	"time"

	"dabet/test/load/internal/gen"
	"dabet/test/load/internal/sched"
	"dabet/test/load/internal/setup"
)

// hotSpotted is the N6 population: a thousand contents at Zipf 1.1, so
// the busiest content takes ~13% of all traffic and the bottom half is
// effectively idle. Author counts scale with a content's share, which
// is what keeps a busy stream from collapsing onto one Redis key and
// what makes the rate-limit stage fire on the generator's deliberate
// bursts rather than on everybody in the hot room.
func hotSpotted() gen.Config {
	c := gen.DefaultConfig()
	c.Contents = 1000
	c.Skew = 1.1
	c.AuthorsPerContent = 40
	c.AuthorSkew = 0.8
	return c
}

// Builtin returns the scenario catalogue.
//
// Rates are laptop-scale on purpose. The deliverable is the shape — the
// knee, the imbalance, the fail-open behaviour — not an absolute
// throughput number that a single-broker Kafka on eight shared cores
// could never make representative.
func Builtin() []Scenario {
	return []Scenario{
		selfBench(),
		baseline(),
		ramp(),
		hotspot(),
		sampler(),
		failOpen(),
		adapterIngress(),
	}
}

// ByName looks a scenario up.
func ByName(name string) (Scenario, error) {
	for _, s := range Builtin() {
		if s.Name == name {
			return s, nil
		}
	}
	return Scenario{}, fmt.Errorf("unknown scenario %q", name)
}

// Names lists the catalogue.
func Names() []string {
	var out []string
	for _, s := range Builtin() {
		out = append(out, s.Name)
	}
	return out
}

// selfBench establishes the generator's own ceiling. Every other run
// compares its achieved rate against this; a run that gets close to it
// measured the harness, not Dabet.
func selfBench() Scenario {
	return Scenario{
		Name:        "selfbench",
		Hypothesis:  "the generator can offer at least an order of magnitude more load than the stack can absorb, so no other scenario is measuring the harness",
		Proves:      "generator ceiling on this machine, with no broker and no consumers",
		Mode:        ModeSelfBench,
		ProfileName: "steady, unthrottled",
		// A very high nominal rate with a short duration: the driver
		// will simply run flat out and the achieved rate is the answer.
		Profile:     sched.Steady(5_000_000, 10*time.Second),
		Population:  hotSpotted(),
		Policy:      setup.DefaultPolicy(),
		SampleEvery: time.Second,
		Criteria:    Criteria{},
	}
}

// baseline is steady state at a rate this machine sustains.
func baseline() Scenario {
	p := hotSpotted()
	return Scenario{
		Name: "baseline",
		Hypothesis: "at a rate the laptop sustains, the cascade keeps up: consumer lag stays flat, " +
			"fail_open_total is zero, and the §4.6 SLI p95 is comfortably inside the 1.5 s N1 budget",
		Proves:      "steady-state health and the latency floor of the pipeline",
		Mode:        ModeKafka,
		ProfileName: "steady 2 000 msg/s for 90 s",
		Profile:     sched.Steady(2000, 90*time.Second),
		Population:  p,
		Policy:      setup.DefaultPolicy(),
		SampleEvery: 2 * time.Second,
		DrainFor:    45 * time.Second,
		Criteria: Criteria{
			MaxP95Seconds:       1.5,
			MaxFailOpen:         0,
			MaxFinalLag:         5000,
			MinConsumeFraction:  0.95,
			MaxSendLagP95Millis: 250,
		},
	}
}

// ramp is the knee hunt. A staircase rather than a smooth ramp: each
// plateau reaches its own steady state, so the knee can be attributed
// to a rate instead of being smeared across the climb.
func ramp() Scenario {
	return Scenario{
		Name: "ramp",
		Hypothesis: "there is a rate above which the cascade cannot keep up; past it consumer lag grows " +
			"without bound (§4.7 says accept the lag, so lag — not errors — is the failure signal) " +
			"and the SLI p95 crosses 1.5 s",
		Proves:      "the knee: the highest offered rate at which lag stays flat and p95 stays under budget",
		Mode:        ModeKafka,
		ProfileName: "staircase 1 000 -> 15 000 msg/s, 6 plateaus of 30 s",
		Profile:     sched.Steps(1000, 15000, 6, 30*time.Second),
		Population:  hotSpotted(),
		Policy:      setup.DefaultPolicy(),
		SampleEvery: 2 * time.Second,
		DrainFor:    60 * time.Second,
		Criteria: Criteria{
			// No latency criterion: the ramp is EXPECTED to cross the
			// budget. Its output is where, not whether.
			MaxFailOpen:         0,
			MaxSendLagP95Millis: 1000,
		},
	}
}

// hotspot is the skew test: one enormous content with very few senders
// against a long tail of quiet ones.
//
// The pathology being hunted is specific. messages.v1 is keyed
// hash(author_id, content_id) (§4.2), so a hot content with MANY
// senders spreads evenly across partitions and causes no imbalance at
// all — but a hot content driven by a handful of senders (a bot, a
// raid, a relay) collapses onto as many partitions as it has senders,
// and those partitions carry the whole firehose while the rest idle.
func hotspot() Scenario {
	p := hotSpotted()
	// One content at 60% of all traffic with only 4 senders, then 999
	// quiet contents sharing the remaining 40%.
	const tail = 999
	weights := make([]float64, 0, tail+1)
	authors := make([]int, 0, tail+1)
	weights = append(weights, 0.60)
	authors = append(authors, 4)
	for range tail {
		weights = append(weights, 0.40/float64(tail))
		authors = append(authors, 8)
	}
	p.Weights = weights
	p.AuthorsPer = authors
	// The rate-limit stage would flag essentially all of the hot
	// content's traffic (4 senders at 300 msg/s each is spam by any
	// definition) and short-circuit the cascade at stage 4, which would
	// hide the partition behaviour this run is about. Turn it off and
	// let the messages travel the whole cascade.
	pol := setup.DefaultPolicy()
	pol.RateLimitMessages = 0
	pol.RateLimitSeconds = 0
	// Duplicate detection would do the same thing to the burst mix, so
	// keep the mix clean-heavy here.
	p.Mix = gen.Mix{RestrictedWord: 0.02, LLMFlag: 0.02}
	return Scenario{
		Name: "hotspot",
		Hypothesis: "a content carrying 60% of traffic behind only four senders concentrates that " +
			"traffic on ~4 partitions, so per-partition lag becomes grossly uneven while total " +
			"throughput looks fine",
		Proves:      "partition-level lag imbalance under the N6 hot-spot shape, and that hash(author,content) hides it when senders are many",
		Mode:        ModeKafka,
		ProfileName: "steady 2 000 msg/s for 90 s, 60% on one content with 4 senders",
		Profile:     sched.Steady(2000, 90*time.Second),
		Population:  p,
		Policy:      pol,
		SampleEvery: 2 * time.Second,
		DrainFor:    45 * time.Second,
		Criteria: Criteria{
			MaxFailOpen:         0,
			MaxSendLagP95Millis: 250,
		},
	}
}

// sampler verifies the §7.5 coverage table empirically.
//
// The trick is that every message in this run is LLM-flag text, so a
// message that reaches the LLM stage produces a flagged.v1 event
// carrying its content_id. Verdicts per content divided by messages
// sent to that content IS the LLM coverage for that content — which is
// otherwise unobservable, since §4.5's cardinality rule rightly forbids
// a content_id metric label.
//
// Contents are pinned to the exact traffic rates of the coverage table
// by explicit weights.
func sampler() Scenario {
	p := hotSpotted()
	// Target per-content rates, msg/s: the §7.5 table's rows.
	//   0.02/s  ~ 1 msg/min   (the "quiet content" row, 100% coverage)
	//   0.33/s  = 20 msg/min  (100%)
	//   1.67/s  = 100 msg/min (~30%)
	//   100/s   = 6000 msg/min (~0.5%)
	rates := []float64{0.02, 0.33, 1.67, 100}
	total := 0.0
	for _, r := range rates {
		total += r
	}
	p.Weights = rates
	// Enough senders per content that no cheap stage fires first.
	p.AuthorsPer = []int{4, 8, 32, 512}
	// Everything is LLM-bound flag text: no cheap detector may fire, or
	// the message never reaches stage 8 and the coverage measurement is
	// wrong.
	p.Mix = gen.Mix{LLMFlag: 1.0}
	pol := setup.DefaultPolicy()
	pol.RateLimitMessages = 0
	pol.RateLimitSeconds = 0
	pol.Spam = "off"
	pol.RestrictedWords = nil
	return Scenario{
		Name: "sampler",
		Hypothesis: "the §7.5 per-content token bucket (A17: 30/min, capacity 30) gives quiet content " +
			"~100% LLM coverage and a 6 000 msg/min firehose ~0.5%, bounding LLM load by the number " +
			"of active contents rather than by message volume",
		Proves:          "the §7.5 coverage table, measured rather than assumed — the primary lever on GPU spend",
		Mode:            ModeKafka,
		ProfileName:     fmt.Sprintf("steady %.0f msg/s for 180 s across 4 rate-pinned contents", total),
		Profile:         sched.Steady(total, 180*time.Second),
		Population:      p,
		Policy:          pol,
		SampleEvery:     5 * time.Second,
		DrainFor:        45 * time.Second,
		TrackPerContent: true,
		Criteria: Criteria{
			MaxFailOpen:         0,
			MaxSendLagP95Millis: 250,
		},
	}
}

// failOpen is the §4.7 drill. Each dependency is killed mid-run and
// restored, and the run asserts the normative table: the system keeps
// consuming, counts the fail-open, and never stops.
func failOpen() Scenario {
	p := hotSpotted()
	// LLM-heavy mix so the LLM drill has something to fail open on.
	p.Mix = gen.Mix{Duplicate: 0.02, RestrictedWord: 0.02, LLMFlag: 0.20}
	return Scenario{
		Name: "failopen",
		Hypothesis: "killing Redis, the LLM and policy-service mid-run degrades the pipeline per the " +
			"normative table in §4.7 — consumption continues, fail_open_total climbs by component, " +
			"and nothing stalls — rather than stopping the pipeline",
		Proves:      "N2: the service does not stop, it degrades; and that every degradation is counted",
		Mode:        ModeKafka,
		ProfileName: "steady 1 000 msg/s for 200 s with three fault windows",
		Profile:     sched.Steady(1000, 200*time.Second),
		Population:  p,
		Policy:      setup.DefaultPolicy(),
		SampleEvery: 2 * time.Second,
		DrainFor:    45 * time.Second,
		Drills: []Drill{
			{At: 20 * time.Second, Container: "redis", Action: "stop", Expect: "redis",
				Note: "§4.7: skip rate/dup/semantic, continue to word + LLM stages"},
			{At: 50 * time.Second, Container: "redis", Action: "start"},
			{At: 80 * time.Second, Container: "mockllm", Action: "stop", Expect: "llm",
				Note: "§4.7: message passes unmoderated, whole batch fails open, no retry (§7.9)"},
			{At: 110 * time.Second, Container: "mockllm", Action: "start"},
			{At: 140 * time.Second, Container: "policy-service", Action: "stop", Expect: "policy",
				Note: "§4.7: message passes unmoderated once the 60 s policy cache expires (A10)"},
			{At: 175 * time.Second, Container: "policy-service", Action: "start"},
		},
		Criteria: Criteria{
			AllowFailOpen:       true,
			MinConsumeFraction:  0.95,
			MaxSendLagP95Millis: 250,
		},
	}
}

// adapterIngress measures the HTTP hop on its own, at a rate the
// request-per-message shape can actually serve.
func adapterIngress() Scenario {
	p := hotSpotted()
	p.Contents = 50
	p.AuthorsPerContent = 20
	return Scenario{
		Name: "adapter",
		Hypothesis: "POST /mock/messages is one HTTP request per message and saturates far below the " +
			"rate moderation can absorb, which is why every other scenario produces to Kafka directly",
		Proves:      "the adapter ingress ceiling, measured separately from moderation throughput",
		Mode:        ModeAdapter,
		ProfileName: "staircase 200 -> 4 000 msg/s, 5 plateaus of 20 s",
		Profile:     sched.Steps(200, 4000, 5, 20*time.Second),
		Population:  p,
		Policy:      setup.DefaultPolicy(),
		Workers:     64,
		SampleEvery: 2 * time.Second,
		DrainFor:    30 * time.Second,
		Criteria: Criteria{
			MaxFailOpen: 0,
		},
	}
}
