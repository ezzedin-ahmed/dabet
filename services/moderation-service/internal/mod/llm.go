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
)

// LLMBatch is one dispatch unit: messages sharing a policy, so the policy
// rubric is sent once per batch rather than once per message (§7.9).
type LLMBatch struct {
	Policy   *policyapi.ResolvedPolicy
	Messages []contracts.Message
}

// LLMBatcher accumulates stage-8 messages per policy_id and releases a
// batch at maxSize messages or after linger, whichever first (§7.9, A18).
// Safe for concurrent use: the consumer goroutine Adds while the pipeline
// loop polls Due.
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
func NewLLMBatcher(maxSize int, linger time.Duration) *LLMBatcher {
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
		return &LLMBatch{Policy: p.policy, Messages: p.messages}
	}
	return nil
}

// Due returns every batch whose oldest message has waited at least linger.
func (b *LLMBatcher) Due(now time.Time) []*LLMBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*LLMBatch
	for id, p := range b.pending {
		if now.Sub(p.oldest) >= b.linger {
			delete(b.pending, id)
			out = append(out, &LLMBatch{Policy: p.policy, Messages: p.messages})
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
		out = append(out, &LLMBatch{Policy: p.policy, Messages: p.messages})
	}
	return out
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

// NewLLMClient builds a client for the vLLM endpoint.
func NewLLMClient(endpoint, model string, timeout time.Duration) *LLMClient {
	return &LLMClient{
		url:     strings.TrimSuffix(endpoint, "/") + "/v1/chat/completions",
		model:   model,
		timeout: timeout,
		httpc:   &http.Client{},
	}
}

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

// Classify implements Classifier.
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

// systemPrompt renders the §7.9 rubric: numbered rules built from the
// policy's restricted_content entries.
func systemPrompt(policy *policyapi.ResolvedPolicy) string {
	var b strings.Builder
	b.WriteString("You are a chat moderation classifier. For each message, decide whether it\n")
	b.WriteString("violates any of the numbered rules. Respond with JSON only.\n\nRules:\n")
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
