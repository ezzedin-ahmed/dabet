package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// LLMLabeler produces a short label and description for one cluster from
// sample message texts (§8.6, A25). priorLabel, when non-empty, is the
// topic's previous label and may be used as context. Any error means the
// caller falls open to the prior label or a generic one — labelling must
// never fail a run.
type LLMLabeler interface {
	Label(ctx context.Context, texts []string, priorLabel string) (label, description string, err error)
}

// VLLMLabeler speaks OpenAI-compatible chat completions against
// VLLM_ENDPOINT, asking for {"label": "...", "description": "..."} JSON.
// Hard timeout, no retry.
type VLLMLabeler struct {
	url     string
	model   string
	timeout time.Duration
	httpc   *http.Client
}

// NewVLLMLabeler builds a labeler for the vLLM endpoint.
func NewVLLMLabeler(endpoint, model string, timeout time.Duration) *VLLMLabeler {
	return &VLLMLabeler{
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

type labelBody struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

const labelSystemPrompt = `You name clusters of semantically similar chat messages from a creator's community.
Given sample messages from one cluster, respond with JSON only:
{"label": "...", "description": "..."}
The label is a short topic name (at most five words). The description is one
sentence summarising what the community is talking about.`

// Label implements LLMLabeler. Texts are sent to the model and are never
// logged or persisted here (P4).
func (l *VLLMLabeler) Label(ctx context.Context, texts []string, priorLabel string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	var b strings.Builder
	if priorLabel != "" {
		fmt.Fprintf(&b, "This cluster was previously labelled %q; relabel it from the current samples.\n\n", priorLabel)
	}
	b.WriteString("Sample messages:\n")
	for i, t := range texts {
		fmt.Fprintf(&b, "%d. %s\n", i+1, strings.ReplaceAll(t, "\n", " "))
	}

	body, err := json.Marshal(chatRequest{
		Model: l.model,
		Messages: []chatMessage{
			{Role: "system", Content: labelSystemPrompt},
			{Role: "user", Content: b.String()},
		},
		Temperature:    0,
		ResponseFormat: &responseFormat{Type: "json_object"},
	})
	if err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.url, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.httpc.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("llm: unexpected status %d", resp.StatusCode)
	}
	var cr chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", "", fmt.Errorf("llm: decode response: %w", err)
	}
	if len(cr.Choices) == 0 {
		return "", "", fmt.Errorf("llm: no choices in response")
	}
	var lb labelBody
	if err := json.Unmarshal([]byte(cr.Choices[0].Message.Content), &lb); err != nil {
		return "", "", fmt.Errorf("llm: parse label: %w", err)
	}
	if strings.TrimSpace(lb.Label) == "" {
		return "", "", fmt.Errorf("llm: empty label")
	}
	return strings.TrimSpace(lb.Label), strings.TrimSpace(lb.Description), nil
}
