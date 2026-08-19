package job

import (
	"regexp"
	"testing"
	"time"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestDeterministicUUID(t *testing.T) {
	a := DeterministicUUID("topic", "cr-1", "w1", "0")
	b := DeterministicUUID("topic", "cr-1", "w1", "0")
	if a != b {
		t.Errorf("same parts produced different ids: %s vs %s", a, b)
	}
	if !uuidRe.MatchString(a) {
		t.Errorf("id %q is not a v4-shaped UUID", a)
	}
	if c := DeterministicUUID("topic", "cr-1", "w1", "1"); c == a {
		t.Errorf("different parts produced the same id %s", a)
	}
	// The NUL separator keeps ("ab","c") distinct from ("a","bc").
	if DeterministicUUID("ab", "c") == DeterministicUUID("a", "bc") {
		t.Error("part boundaries are ambiguous")
	}
}

func TestUsageIdempotencyKey(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	onDemand := Decision{CreatorID: "cr-1", Trigger: TriggerOnDemand, From: from, To: to,
		JobID: ReclusterJobID("cr-1", from, to)}
	got := UsageIdempotencyKey(onDemand)
	want := "recluster:" + onDemand.JobID + ":cr-1"
	if got != want {
		t.Errorf("on-demand key = %q, want %q", got, want)
	}
	if again := UsageIdempotencyKey(onDemand); again != got {
		t.Errorf("key not deterministic: %q vs %q", again, got)
	}

	scheduled := Decision{CreatorID: "cr-1", Trigger: TriggerDoubled, From: from, To: to}
	got = UsageIdempotencyKey(scheduled)
	want = "job:2026-07-01T00:00:00Z/2026-08-01T00:00:00Z:cr-1"
	if got != want {
		t.Errorf("scheduled key = %q, want %q", got, want)
	}
}

func TestReclusterJobIDDeterministic(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	a := ReclusterJobID("cr-1", from, to)
	if b := ReclusterJobID("cr-1", from, to); b != a {
		t.Errorf("job id not deterministic: %s vs %s", a, b)
	}
	if c := ReclusterJobID("cr-2", from, to); c == a {
		t.Error("different creators share a job id")
	}
	if d := ReclusterJobID("cr-1", from, to.Add(time.Hour)); d == a {
		t.Error("different windows share a job id")
	}
}
