package scenario

import (
	"encoding/json"
	"math"
	"testing"

	"dabet/test/load/internal/sched"
)

// Every catalogue entry must be runnable and must say what it is
// evidence for: a scenario without a hypothesis is a benchmark, not a
// test.
func TestCatalogueIsWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Builtin() {
		if s.Name == "" {
			t.Fatal("scenario with no name")
		}
		if seen[s.Name] {
			t.Errorf("duplicate scenario name %q", s.Name)
		}
		seen[s.Name] = true
		if s.Hypothesis == "" || s.Proves == "" {
			t.Errorf("%s: hypothesis and proves must both be set", s.Name)
		}
		if err := s.Profile.Validate(); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
		if _, err := sched.Compile(s.Profile); err != nil {
			t.Errorf("%s: profile does not compile: %v", s.Name, err)
		}
		if err := s.Population.Mix.Validate(); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
		if _, err := json.Marshal(s); err != nil {
			t.Errorf("%s: does not serialise into the result document: %v", s.Name, err)
		}
	}
	for _, want := range []string{"selfbench", "baseline", "ramp", "hotspot", "sampler", "failopen"} {
		if !seen[want] {
			t.Errorf("catalogue is missing the %q scenario", want)
		}
	}
}

func TestByName(t *testing.T) {
	if _, err := ByName("baseline"); err != nil {
		t.Fatal(err)
	}
	if _, err := ByName("nope"); err == nil {
		t.Fatal("unknown scenario accepted")
	}
	if len(Names()) != len(Builtin()) {
		t.Error("Names() and Builtin() disagree")
	}
}

// The hot-spot scenario only proves anything if its population really
// is hot-spotted behind few senders — that concentration is the whole
// pathology it hunts.
func TestHotspotIsActuallyHotSpotted(t *testing.T) {
	s, err := ByName("hotspot")
	if err != nil {
		t.Fatal(err)
	}
	w := s.Population.Weights
	if len(w) < 100 {
		t.Fatalf("hotspot population has only %d contents; there is no tail", len(w))
	}
	total := 0.0
	for _, x := range w {
		total += x
	}
	if share := w[0] / total; share < 0.5 {
		t.Errorf("hottest content carries %.1f%% of traffic, want a majority", 100*share)
	}
	if n := s.Population.AuthorsPer[0]; n > 8 {
		t.Errorf("hot content has %d senders; the point is that it has few", n)
	}
	// A rate limit would flag the hot content at stage 4 and hide the
	// partition behaviour entirely.
	if s.Policy.RateLimitMessages != 0 {
		t.Error("hotspot must not set a rate limit, or the cascade short-circuits at stage 4")
	}
}

// The sampler scenario's contents must land on the §7.5 table's rows,
// and every message must be LLM-bound or the coverage measurement is
// measuring a cheap detector instead.
func TestSamplerScenarioPinsTheCoverageTable(t *testing.T) {
	s, err := ByName("sampler")
	if err != nil {
		t.Fatal(err)
	}
	if s.Population.Mix.LLMFlag != 1.0 {
		t.Errorf("sampler mix must be entirely LLM-bound, got %+v", s.Population.Mix)
	}
	if s.Policy.RateLimitMessages != 0 || s.Policy.Spam != "none" || len(s.Policy.RestrictedWords) != 0 {
		t.Error("sampler policy must leave every cheap detector off, or messages never reach stage 8")
	}
	if !s.TrackPerContent {
		t.Error("sampler scenario must track verdicts per content")
	}
	total := 0.0
	for _, w := range s.Population.Weights {
		total += w
	}
	// The busiest content must be the ~6 000 msg/min row.
	perMin := s.Population.Weights[3] / total * s.Profile.PeakRate() * 60
	if math.Abs(perMin-6000) > 600 {
		t.Errorf("firehose content offered %.0f msg/min, want ~6000", perMin)
	}
	// And the quietest must be well under the 30/min refill.
	quiet := s.Population.Weights[0] / total * s.Profile.PeakRate() * 60
	if quiet > 5 {
		t.Errorf("quiet content offered %.1f msg/min, want well under the 30/min ceiling", quiet)
	}
}

// The drills must come in stop/start pairs, or a run leaves the stack
// with a dependency down.
func TestFailOpenDrillsArePaired(t *testing.T) {
	s, err := ByName("failopen")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Criteria.AllowFailOpen {
		t.Error("the fail-open drill must not assert fail_open_total == 0; fail-opens are the point")
	}
	state := map[string]int{}
	last := map[string]bool{}
	for _, d := range s.Drills {
		switch d.Action {
		case "stop":
			if last[d.Container] {
				t.Errorf("%s stopped twice without a start", d.Container)
			}
			last[d.Container] = true
			state[d.Container]++
		case "start":
			if !last[d.Container] {
				t.Errorf("%s started without having been stopped", d.Container)
			}
			last[d.Container] = false
			state[d.Container]--
		default:
			t.Errorf("unknown drill action %q", d.Action)
		}
		if d.At >= s.Profile.Duration() {
			t.Errorf("drill on %s at %v is past the end of the %v profile",
				d.Container, d.At, s.Profile.Duration())
		}
	}
	for c, n := range state {
		if n != 0 {
			t.Errorf("%s left in a stopped state at the end of the run", c)
		}
	}
	for _, want := range []string{"redis", "mockllm", "policy-service"} {
		if _, ok := state[want]; !ok {
			t.Errorf("the §4.7 drill never touches %s", want)
		}
	}
}
