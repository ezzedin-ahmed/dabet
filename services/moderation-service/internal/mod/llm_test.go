package mod

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dabet/pkg/policyapi"
)

func rcPolicy(id string, action policyapi.RestrictedContentAction) *policyapi.ResolvedPolicy {
	return &policyapi.ResolvedPolicy{
		PolicyId: id,
		RestrictedContent: []*policyapi.RestrictedContentEntry{{
			Title:       "Ticket scalping",
			Description: "Offers to resell event tickets, or requests to buy them.",
			Examples:    []string{"selling 2 tickets for tonight DM me", "anyone got a spare ticket"},
		}},
		RestrictedContentAction: action,
	}
}

func TestLLMBatcherSizeTrigger(t *testing.T) {
	b := NewLLMBatcher(2, 50*time.Millisecond)
	pol := rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)

	if got := b.Add(testMessage("m1", "a"), pol, t0); got != nil {
		t.Fatal("batch released before size trigger")
	}
	got := b.Add(testMessage("m2", "b"), pol, t0)
	if got == nil || len(got.Messages) != 2 {
		t.Fatalf("size trigger: got %+v, want batch of 2", got)
	}
	if got.Policy.GetPolicyId() != "pol_1" {
		t.Fatal("batch must carry its policy")
	}
	if extra := b.Due(t0.Add(time.Hour)); extra != nil {
		t.Fatal("released batch must not linger")
	}
}

func TestLLMBatcherLingerTrigger(t *testing.T) {
	b := NewLLMBatcher(32, 50*time.Millisecond)
	pol := rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	b.Add(testMessage("m1", "a"), pol, t0)
	b.Add(testMessage("m2", "b"), pol, t0.Add(30*time.Millisecond))

	if got := b.Due(t0.Add(49 * time.Millisecond)); got != nil {
		t.Fatal("must not dispatch before linger (measured from oldest)")
	}
	got := b.Due(t0.Add(50 * time.Millisecond))
	if len(got) != 1 || len(got[0].Messages) != 2 {
		t.Fatalf("linger trigger: got %+v, want one batch of 2", got)
	}
	if got := b.Due(t0.Add(time.Hour)); got != nil {
		t.Fatal("empty batcher must not dispatch")
	}
}

func TestLLMBatcherBatchesPerPolicy(t *testing.T) {
	b := NewLLMBatcher(2, 50*time.Millisecond)
	p1 := rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	p2 := rcPolicy("pol_2", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)

	if b.Add(testMessage("m1", "a"), p1, t0) != nil {
		t.Fatal("premature dispatch")
	}
	if got := b.Add(testMessage("m2", "b"), p2, t0); got != nil {
		t.Fatal("different policies must not share a batch")
	}
	got := b.Add(testMessage("m3", "c"), p1, t0)
	if got == nil || got.Policy.GetPolicyId() != "pol_1" || len(got.Messages) != 2 {
		t.Fatalf("per-policy batch wrong: %+v", got)
	}
	rest := b.FlushAll()
	if len(rest) != 1 || rest[0].Policy.GetPolicyId() != "pol_2" {
		t.Fatalf("FlushAll = %+v, want pol_2 remainder", rest)
	}
}

// mockVLLM mimics tools/mockllm: parses the numbered Messages list and
// flags entries containing FLAGME as violating rule 1.
func mockVLLM(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		var prompt strings.Builder
		for _, m := range req.Messages {
			prompt.WriteString(m.Content + "\n")
		}
		var results []map[string]int
		in := false
		n := 0
		for _, line := range strings.Split(prompt.String(), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "Messages:") {
				in = true
				continue
			}
			if !in || trimmed == "" {
				continue
			}
			if !strings.Contains(trimmed, ". ") && !strings.HasSuffix(trimmed, ".") {
				break
			}
			if strings.HasPrefix(trimmed, "Respond:") {
				break
			}
			n++
			v := 0
			if strings.Contains(trimmed, "FLAGME") {
				v = 1
			}
			results = append(results, map[string]int{"i": n, "violates": v})
		}
		content, _ := json.Marshal(map[string]any{"results": results})
		writeChatResponse(w, string(content))
	}))
}

func writeChatResponse(w http.ResponseWriter, content string) {
	resp := map[string]any{
		"choices": []map[string]any{{
			"message": map[string]any{"role": "assistant", "content": content},
		}},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func TestLLMClientVerdictMapping(t *testing.T) {
	srv := mockVLLM(t)
	defer srv.Close()
	c := NewLLMClient(srv.URL, "test-model", time.Second)

	got, err := c.Classify(context.Background(),
		rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
		[]string{"hello there", "FLAGME please", "another\nclean one"})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 1, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("verdicts = %v, want %v", got, want)
		}
	}
}

func TestLLMClientPromptFormat(t *testing.T) {
	var sys, user string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			ResponseFormat struct {
				Type string `json:"type"`
			} `json:"response_format"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.ResponseFormat.Type != "json_object" {
			t.Errorf("response_format = %q, want json_object", req.ResponseFormat.Type)
		}
		for _, m := range req.Messages {
			switch m.Role {
			case "system":
				sys = m.Content
			case "user":
				user = m.Content
			}
		}
		writeChatResponse(w, `{"results":[{"i":1,"violates":0}]}`)
	}))
	defer srv.Close()

	c := NewLLMClient(srv.URL, "m", time.Second)
	if _, err := c.Classify(context.Background(),
		rcPolicy("pol_1", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
		[]string{"line one\nline two"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sys, "1. Ticket scalping — Offers to resell event tickets, or requests to buy them.") {
		t.Errorf("system prompt missing numbered rule:\n%s", sys)
	}
	if !strings.Contains(sys, `Examples: "selling 2 tickets for tonight DM me" | "anyone got a spare ticket"`) {
		t.Errorf("system prompt missing examples:\n%s", sys)
	}
	if !strings.Contains(user, "Messages:\n1. line one line two\n") {
		t.Errorf("user prompt must number messages and flatten newlines:\n%q", user)
	}
	if !strings.Contains(user, `Respond: {"results":[{"i":1,"violates":0}]}`) {
		t.Errorf("user prompt missing response contract:\n%s", user)
	}
}

func TestLLMClientTimeoutFailsWholeBatch(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		writeChatResponse(w, `{"results":[{"i":1,"violates":0}]}`)
	}))
	defer srv.Close()
	defer close(release)

	c := NewLLMClient(srv.URL, "m", 20*time.Millisecond)
	_, err := c.Classify(context.Background(),
		rcPolicy("p", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
		[]string{"a", "b"})
	if err == nil {
		t.Fatal("timeout must surface as an error (whole batch fails open)")
	}
}

func TestLLMClientMalformedAndIncomplete(t *testing.T) {
	cases := []struct{ name, content string }{
		{"not json", "I think message 1 is fine actually"},
		{"missing verdict", `{"results":[{"i":1,"violates":0}]}`}, // 2 texts sent
		{"index out of range", `{"results":[{"i":1,"violates":0},{"i":7,"violates":1}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeChatResponse(w, tc.content)
			}))
			defer srv.Close()
			c := NewLLMClient(srv.URL, "m", time.Second)
			if _, err := c.Classify(context.Background(),
				rcPolicy("p", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
				[]string{"a", "b"}); err == nil {
				t.Fatal("malformed/incomplete response must error")
			}
		})
	}
}

func TestLLMClientTransportAndStatusErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	c := NewLLMClient(srv.URL, "m", time.Second)
	if _, err := c.Classify(context.Background(),
		rcPolicy("p", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
		[]string{"a"}); err == nil {
		t.Fatal("500 must error")
	}
	srv.Close()
	if _, err := c.Classify(context.Background(),
		rcPolicy("p", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO),
		[]string{"a"}); err == nil {
		t.Fatal("transport failure must error")
	}
}

func TestSystemPromptNumbersAllRules(t *testing.T) {
	pol := &policyapi.ResolvedPolicy{RestrictedContent: []*policyapi.RestrictedContentEntry{
		{Title: "A", Description: "first"},
		{Title: "B", Description: "second"},
	}}
	sys := systemPrompt(pol)
	for i, want := range []string{"1. A — first", "2. B — second"} {
		if !strings.Contains(sys, want) {
			t.Fatalf("rule %d missing from prompt:\n%s", i+1, sys)
		}
	}
}

func TestUserPromptNumbering(t *testing.T) {
	up := userPrompt([]string{"a", "b", "c"})
	for i := 1; i <= 3; i++ {
		if !strings.Contains(up, fmt.Sprintf("%d. ", i)) {
			t.Fatalf("message %d not numbered:\n%s", i, up)
		}
	}
}
