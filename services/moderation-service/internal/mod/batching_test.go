package mod

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/policyapi"
)

func (e *pipeEnv) trigger(t *testing.T, name BatchTrigger) float64 {
	t.Helper()
	return testutil.ToFloat64(e.met.LLMBatchTrigger.WithLabelValues(string(name)))
}

func (e *pipeEnv) promptChars(t *testing.T, part string) float64 {
	t.Helper()
	return testutil.ToFloat64(e.met.LLMPromptChars.WithLabelValues(part))
}

func llmEnv(t *testing.T, batchSize int, linger time.Duration) *pipeEnv {
	t.Helper()
	cfg := DefaultConfig()
	cfg.LLMBatchSize = batchSize
	cfg.LLMLinger = linger
	cfg.SamplerCapacity = 1000 // the sampler must not be the binding limit here
	cfg.SamplerPerMin = 60000
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	return env
}

// A18's size trigger fires at whatever MOD_LLM_BATCH_SIZE says, not only
// at the documented 32.
func TestBatchSizeTriggerFiresAtConfiguredSize(t *testing.T) {
	const size = 6
	env := llmEnv(t, size, time.Hour) // linger long enough that it cannot win
	env.llm.verdicts = make([]int, size)

	for i := 0; i < size-1; i++ {
		env.process(t, testMessage(fmt.Sprintf("m%d", i), "text"))
	}
	if env.llm.calls != 0 {
		t.Fatal("batch released before the configured size")
	}
	env.process(t, testMessage("last", "text"))
	env.pipe.wg.Wait()

	if env.llm.calls != 1 || len(env.llm.batches[0]) != size {
		t.Fatalf("llm calls = %d, batch sizes = %v, want one batch of %d", env.llm.calls, env.llm.batches, size)
	}
	if got := env.trigger(t, TriggerSize); got != 1 {
		t.Fatalf("llm_batch_trigger_total{size} = %v, want 1", got)
	}
	if got := env.trigger(t, TriggerLinger); got != 0 {
		t.Fatalf("llm_batch_trigger_total{linger} = %v, want 0", got)
	}
	if err := testutil.CollectAndCompare(env.met.LLMBatchSize, batchSizeExposition(size)); err != nil {
		t.Fatal(err)
	}
}

// And the linger trigger fires at whatever MOD_LLM_LINGER says. Both are
// §4.4 tunables with the spec's numbers as defaults.
func TestBatchLingerTriggerFiresAtConfiguredLinger(t *testing.T) {
	const linger = 250 * time.Millisecond
	env := llmEnv(t, 32, linger)
	env.llm.verdicts = []int{0}

	env.process(t, testMessage("m1", "text"))
	env.clock.Advance(linger - time.Millisecond)
	if due := env.pipe.batcher.Due(env.clock.Now()); len(due) != 0 {
		t.Fatal("batch released before the configured linger elapsed")
	}
	env.clock.Advance(time.Millisecond)
	due := env.pipe.batcher.Due(env.clock.Now())
	if len(due) != 1 || due[0].Trigger != TriggerLinger {
		t.Fatalf("due = %v, want one linger-triggered batch", due)
	}
	env.pipe.dispatch(context.Background(), due[0])

	if got := env.trigger(t, TriggerLinger); got != 1 {
		t.Fatalf("llm_batch_trigger_total{linger} = %v, want 1", got)
	}
	if got := env.trigger(t, TriggerSize); got != 0 {
		t.Fatalf("llm_batch_trigger_total{size} = %v, want 0", got)
	}
}

// This is F5 made measurable. The same traffic and the same policy, batched
// two different ways: the message half of the prompt is identical, the
// rubric half — the part §7.9 batches to amortise — scales with the number
// of batches, so half the batches means half the rubric spend.
func TestPromptCharsExposeRubricResendWaste(t *testing.T) {
	const messages = 8

	run := func(batchSize int) (rubric, msgs, batches float64) {
		env := llmEnv(t, batchSize, time.Hour)
		env.llm.verdicts = make([]int, batchSize)
		for i := 0; i < messages; i++ {
			env.process(t, testMessage(fmt.Sprintf("m%d", i), "an eight-word message of a fixed length here"))
		}
		env.pipe.wg.Wait()
		return env.promptChars(t, "rubric"), env.promptChars(t, "messages"), float64(env.llm.calls)
	}

	rub1, msg1, n1 := run(1) // the measured reality: a batch per message
	rub4, msg4, n4 := run(4) // four messages amortising one rubric

	if n1 != messages || n4 != messages/4 {
		t.Fatalf("batches: %v at size 1 and %v at size 4", n1, n4)
	}
	if msg1 != msg4 || msg1 == 0 {
		t.Fatalf("message chars = %v vs %v: the same traffic must cost the same", msg1, msg4)
	}
	if rub1 != 4*rub4 || rub4 == 0 {
		t.Fatalf("rubric chars = %v at size 1 and %v at size 4, want a 4× ratio", rub1, rub4)
	}
	// The number an operator actually reads: rubric spend as a multiple of
	// message spend. At batch size 1 the rubric dominates by far.
	if rub1/msg1 <= rub4/msg4 {
		t.Fatal("rubric:messages ratio must fall as batches fill; that ratio is the A18 trade-off")
	}
}

// rubricChars must track the policy it bills for, or the metric is
// decoration.
func TestRubricCharsScalesWithPolicy(t *testing.T) {
	small := rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	big := rcPolicy("pol_2", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	big.RestrictedContent = append(big.RestrictedContent, &policyapi.RestrictedContentEntry{
		Title:       "Crypto shilling",
		Description: "Unsolicited promotion of tokens, coins, or trading groups.",
		Examples:    []string{"join my signals group", "this coin is going to 100x"},
	})

	if rubricChars(small) <= len(systemPromptPreamble) {
		t.Fatal("rubricChars must count the rubric entries, not just the preamble")
	}
	if rubricChars(big) <= rubricChars(small) {
		t.Fatal("a longer policy must bill more rubric characters")
	}
	// It is an estimate of the rendered prompt, so it should be in the same
	// ballpark as actually rendering it — within 25%.
	rendered := float64(len(systemPrompt(big)))
	est := float64(rubricChars(big))
	if est < rendered*0.75 || est > rendered*1.25 {
		t.Fatalf("rubricChars = %v against a rendered prompt of %v: the estimate has drifted", est, rendered)
	}
}

// Both triggers come from the environment (§4.4), so nonsense must degrade
// rather than crash the moderation path (P2).
func TestBatcherClampsNonsenseTriggers(t *testing.T) {
	b := NewLLMBatcher(0, -time.Second)
	pol := rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	got := b.Add(testMessage("m1", "a"), pol, t0)
	if got == nil || len(got.Messages) != 1 || got.Trigger != TriggerSize {
		t.Fatalf("size 0 must clamp to 1, got %v", got)
	}
	b.Add(testMessage("m2", "b"), pol, t0)
	if due := b.Due(t0); len(due) != 0 {
		t.Fatal("a size-1 batcher leaves nothing pending")
	}
}

func TestShutdownBatchesAreLabelledAsSuch(t *testing.T) {
	env := llmEnv(t, 32, time.Hour)
	env.llm.verdicts = []int{0}
	env.process(t, testMessage("m1", "text"))
	env.pipe.Shutdown(context.Background())

	if got := env.trigger(t, TriggerShutdown); got != 1 {
		t.Fatalf("llm_batch_trigger_total{shutdown} = %v, want 1", got)
	}
}

// batchSizeExposition renders the llm_batch_size histogram for a single
// observation of n, so the size trigger's effect on the metric §7.11
// already publishes is asserted rather than assumed.
func batchSizeExposition(n int) io.Reader {
	bounds := []float64{1, 2, 4, 8, 16, 24, 32, 48, 64}
	out := "# HELP llm_batch_size Messages per LLM batch.\n# TYPE llm_batch_size histogram\n"
	for _, b := range bounds {
		c := 0
		if float64(n) <= b {
			c = 1
		}
		out += fmt.Sprintf("llm_batch_size_bucket{le=\"%g\"} %d\n", b, c)
	}
	out += fmt.Sprintf("llm_batch_size_bucket{le=\"+Inf\"} 1\nllm_batch_size_sum %d\nllm_batch_size_count 1\n", n)
	return strings.NewReader(out)
}
