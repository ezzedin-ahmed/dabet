package mod

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"dabet/pkg/contracts"
	"dabet/pkg/policyapi"
	"dabet/pkg/tracing"
)

// BatchTrigger names which of A18's two release conditions fired. It is a
// metric label, so the value set is closed and tiny (§4.5).
type BatchTrigger string

const (
	TriggerSize     BatchTrigger = "size"     // maxSize messages accumulated
	TriggerLinger   BatchTrigger = "linger"   // the linger window expired
	TriggerShutdown BatchTrigger = "shutdown" // drained by Shutdown (§7.10)
)

// LLMBatch is one dispatch unit: messages sharing a policy, so the policy
// rubric is sent once per batch rather than once per message (§7.9).
type LLMBatch struct {
	Policy   *policyapi.ResolvedPolicy
	Messages []contracts.Message
	Trigger  BatchTrigger
}

// LLMBatcher accumulates stage-8 messages per policy_id and releases a
// batch at maxSize messages or after linger, whichever first (§7.9, A18:
// 32 messages or 50 ms). Both triggers are configurable per §4.4 —
// MOD_LLM_BATCH_SIZE and MOD_LLM_LINGER — and the defaults here are the
// spec's numbers.
//
// # A18 IN PRACTICE: the size trigger does not fire
//
// Measured on the reference stack (test/load/README.md, F5): mean batch
// size 1.07–4.0 in every scenario, p50 of 1–3, never near 32. The linger
// always wins, so the rubric — which §7.9 itself calls "a large share of
// the prompt tokens" and which is the entire reason for batching by
// policy — is re-sent every one to four messages instead of every 32.
//
// The arithmetic. Batching is per policy_id, per instance, so what fills
// a batch is
//
//	mean_batch ≈ r × linger,  r = admitted msg/s, per instance, per policy
//
// Inverting it at the measured points: mean 4.0 at a 50 ms linger implies
// r ≈ 80/s, so filling 32 needs linger ≈ 32/80 = 400 ms; mean 1.07
// implies r ≈ 21/s and linger ≈ 1.5 s. That is the honest answer to "what
// linger would make A18's size trigger real": 0.4–1.5 s at these rates.
//
// What it costs. §4.6's indicative budget spends 231 ms outside the LLM
// (50 adapter + 50 consume + 1 policy + 10 Redis + 100 embedding + 20
// publish) and 1 000 ms on the LLM itself, against N1's 1.5 s p95 —
// leaving roughly 269 ms of headroom, and only if the LLM actually
// answers inside its own budget, which F4 shows it does not. So a linger
// long enough to fill 32 does not fit; a linger of ~250 ms does, and by
// the same arithmetic yields mean batches of ~5 (at r = 21) to ~20 (at
// r = 80) — a 5× cut in rubric resends for a quarter second of the SLI.
// Linger is added latency only for messages that reach stage 9, which is
// ~1.7% of traffic, but those are exactly the messages the SLI measures,
// since a stage-9 flag is a flag.
//
// Note what does NOT help: lowering MOD_LLM_BATCH_SIZE toward the
// observed mean makes the size trigger fire again, but changes nothing
// about GPU spend — the rubric is still sent once per batch and the
// batches are still the same size. Only the linger moves that number.
//
// The defaults here stay at the spec's 32 / 50 ms deliberately: this is a
// latency-versus-GPU-cost trade, and the numbers above are what an
// operator needs to make it. llm_batch_size, llm_batch_trigger_total and
// llm_prompt_chars_total expose both sides of it.
//
// Safe for concurrent use: several partition workers may Add while the
// background loop polls Due.
type LLMBatcher struct {
	maxSize int
	linger  time.Duration

	mu      sync.Mutex
	pending map[string]*llmPending
}

type llmPending struct {
	policy   *policyapi.ResolvedPolicy
	messages []contracts.Message
	oldest   time.Time
}

// NewLLMBatcher builds a batcher with the given size and linger triggers.
// Both are clamped rather than rejected: they come from the environment
// (§4.4) and a typo must degrade, not crash the moderation path (P2). A
// size below 1 becomes 1 (dispatch every message) and a negative linger
// becomes 0 (dispatch on the next background tick).
func NewLLMBatcher(maxSize int, linger time.Duration) *LLMBatcher {
	if maxSize < 1 {
		maxSize = 1
	}
	if linger < 0 {
		linger = 0
	}
	return &LLMBatcher{maxSize: maxSize, linger: linger, pending: make(map[string]*llmPending)}
}

// Add queues msg under its policy and returns a full batch when the size
// trigger fires, nil otherwise.
func (b *LLMBatcher) Add(msg contracts.Message, policy *policyapi.ResolvedPolicy, now time.Time) *LLMBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := policy.GetPolicyId()
	p, ok := b.pending[id]
	if !ok {
		p = &llmPending{policy: policy, oldest: now}
		b.pending[id] = p
	}
	p.messages = append(p.messages, msg)
	if len(p.messages) >= b.maxSize {
		delete(b.pending, id)
		return &LLMBatch{Policy: p.policy, Messages: p.messages, Trigger: TriggerSize}
	}
	return nil
}

// Due returns every batch whose oldest message has waited at least linger.
func (b *LLMBatcher) Due(now time.Time) []*LLMBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*LLMBatch
	for id, p := range b.pending {
		if !now.Before(p.oldest.Add(b.linger)) {
			delete(b.pending, id)
			out = append(out, &LLMBatch{Policy: p.policy, Messages: p.messages, Trigger: TriggerLinger})
		}
	}
	return out
}

// FlushAll drains everything pending, regardless of age (shutdown path).
func (b *LLMBatcher) FlushAll() []*LLMBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*LLMBatch
	for id, p := range b.pending {
		delete(b.pending, id)
		out = append(out, &LLMBatch{Policy: p.policy, Messages: p.messages, Trigger: TriggerShutdown})
	}
	return out
}

// rubricChars is the size of the policy half of the prompt: the part that
// is sent once per BATCH and is therefore what batching amortises. It is
// counted arithmetically rather than by rendering systemPrompt, so the
// accounting costs nothing on the hot path. Characters, not tokens —
// English averages roughly four characters per token, so divide by four
// for an order-of-magnitude token figure.
func rubricChars(policy *policyapi.ResolvedPolicy) int {
	n := len(systemPromptPreamble)
	for _, e := range policy.GetRestrictedContent() {
		n += len(e.GetTitle()) + len(e.GetDescription()) + 8 // "N. t — d\n"
		for _, ex := range e.GetExamples() {
			n += len(ex) + 5 // quotes, separator, indent
		}
	}
	return n
}

// Classifier is the LLM contract the pipeline dispatches against.
// Classify returns, per input text, the violated rule number (1-based) or
// 0 for none. Any error means the WHOLE batch fails open (§7.9).
type Classifier interface {
	Classify(ctx context.Context, policy *policyapi.ResolvedPolicy, texts []string) ([]int, error)
}

// LLMClient speaks OpenAI-compatible chat completions against
// VLLM_ENDPOINT (tools/mockllm implements the same shape). One request per
// batch; JSON response format requested; hard timeout; no retry.
type LLMClient struct {
	url     string
	model   string
	timeout time.Duration
	httpc   *http.Client
}

// NewLLMClient builds a client for the vLLM endpoint. maxIdleConns is the
// idle-connection pool kept for the endpoint; see the transport note
// below. Zero or less means the default.
func NewLLMClient(endpoint, model string, timeout time.Duration, maxIdleConns int) *LLMClient {
	if maxIdleConns <= 0 {
		maxIdleConns = DefaultLLMMaxIdleConns
	}
	// Instrumented transport: the LLM hop is the §4.6 budget's 1 000 ms
	// elephant, so it has to be visible in the trace.
	//
	// The pool sizing is the one cheap mitigation available for F4 that
	// does not touch §7.9. http.DefaultTransport keeps only 2 idle
	// connections per host, and batches dispatch concurrently — one
	// goroutine per batch — so beyond the third in flight every request
	// opened a fresh connection and paid the TCP (and, off-cluster, TLS)
	// handshake INSIDE the 1 000 ms model deadline. That is setup time
	// charged against the model's own budget. Sizing the pool to the
	// expected dispatch concurrency removes it. It is not a retry, it does
	// not extend the deadline, and it cannot change which batches fail
	// open — it only stops spending the deadline on connection setup.
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxIdleConns = maxIdleConns
	base.MaxIdleConnsPerHost = maxIdleConns
	return &LLMClient{
		url:     strings.TrimSuffix(endpoint, "/") + "/v1/chat/completions",
		model:   model,
		timeout: timeout,
		httpc:   &http.Client{Transport: tracing.Transport(base)},
	}
}

// DefaultLLMMaxIdleConns is the default idle-connection pool for the vLLM
// endpoint, sized for the concurrent-dispatch count a single instance
// reaches at its measured throughput ceiling.
const DefaultLLMMaxIdleConns = 64

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model          string          `json:"model"`
	Messages       []chatMessage   `json:"messages"`
	Temperature    float64         `json:"temperature"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

type verdictBody struct {
	Results []struct {
		I        int `json:"i"`
		Violates int `json:"violates"`
	} `json:"results"`
}

// Classify implements Classifier. The timeout is applied HERE, so it
// covers the model call and nothing else: the linger wait and the
// goroutine handoff happen before Classify is entered and are not charged
// against §7.9's 1 000 ms. On expiry the whole batch fails open with no
// retry, which is the caller's job (§7.9).
func (c *LLMClient) Classify(ctx context.Context, policy *policyapi.ResolvedPolicy, texts []string) ([]int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	body, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt(policy)},
			{Role: "user", Content: userPrompt(texts)},
		},
		Temperature:    0,
		ResponseFormat: &responseFormat{Type: "json_object"},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llm: unexpected status %d", resp.StatusCode)
	}
	var cr chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("llm: decode response: %w", err)
	}
	if len(cr.Choices) == 0 {
		return nil, fmt.Errorf("llm: no choices in response")
	}
	var vb verdictBody
	if err := json.Unmarshal([]byte(cr.Choices[0].Message.Content), &vb); err != nil {
		return nil, fmt.Errorf("llm: parse verdicts: %w", err)
	}
	out := make([]int, len(texts))
	seen := make([]bool, len(texts))
	for _, r := range vb.Results {
		if r.I < 1 || r.I > len(texts) {
			return nil, fmt.Errorf("llm: verdict index %d out of range", r.I)
		}
		out[r.I-1] = r.Violates
		seen[r.I-1] = true
	}
	for i, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("llm: incomplete response, missing verdict %d", i+1)
		}
	}
	return out, nil
}

// systemPromptPreamble is the fixed head of the §7.9 rubric, verbatim
// from the spec's prompt listing.
const systemPromptPreamble = "You are a chat moderation classifier. For each message, decide whether it\n" +
	"violates any of the numbered rules. Respond with JSON only.\n\nRules:\n"

// systemPrompt renders the §7.9 rubric: numbered rules built from the
// policy's restricted_content entries.
func systemPrompt(policy *policyapi.ResolvedPolicy) string {
	var b strings.Builder
	b.WriteString(systemPromptPreamble)
	for i, e := range policy.GetRestrictedContent() {
		fmt.Fprintf(&b, "%d. %s — %s\n", i+1, promptLine(e.GetTitle()), promptLine(e.GetDescription()))
		if exs := e.GetExamples(); len(exs) > 0 {
			quoted := make([]string, len(exs))
			for j, ex := range exs {
				quoted[j] = `"` + promptLine(ex) + `"`
			}
			fmt.Fprintf(&b, "   Examples: %s\n", strings.Join(quoted, " | "))
		}
	}
	return b.String()
}

// userPrompt renders the §7.9 numbered "Messages:" list plus the response
// contract line.
func userPrompt(texts []string) string {
	var b strings.Builder
	b.WriteString("Messages:\n")
	for i, t := range texts {
		fmt.Fprintf(&b, "%d. %s\n", i+1, promptLine(t))
	}
	b.WriteString("\nRespond: {\"results\":[{\"i\":1,\"violates\":0}]}\n")
	return b.String()
}

// promptLine flattens newlines so a message cannot break the numbered
// list structure the model (and mockllm) parses.
func promptLine(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	return strings.ReplaceAll(s, "\n", " ")
}
