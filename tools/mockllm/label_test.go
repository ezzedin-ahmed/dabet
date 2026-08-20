package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The §8.6 prompt VLLMLabeler actually sends.
func labelPrompt(samples ...string) string {
	var b strings.Builder
	b.WriteString("You name clusters of semantically similar chat messages from a creator's community.\n")
	b.WriteString("Sample messages:\n")
	for i, s := range samples {
		b.WriteString(string(rune('1'+i)) + ". " + s + "\n")
	}
	return b.String()
}

func TestLabelRequestIsDistinguishedFromModeration(t *testing.T) {
	if isLabelRequest("Messages:\n1. hello\n") {
		t.Error("a §7.9 moderation prompt was taken for a labelling prompt")
	}
	if !isLabelRequest(labelPrompt("hello")) {
		t.Error("a §8.6 labelling prompt was not recognised")
	}
}

func TestLabelHonoursTheMarker(t *testing.T) {
	got := label(labelPrompt(
		"grabbing tickets tonight [[label:Ticket resale]] [[sim:tickets/resale:0.9/0.99:a]]",
		"anyone reselling [[label:Ticket resale]]",
		"queue is huge [[label:Ticket resale]]",
	))
	if got.Label != "Ticket resale" {
		t.Errorf("label = %q, want %q", got.Label, "Ticket resale")
	}
	if got.Description == "" {
		t.Error("description is empty")
	}
}

// A plurality wins, so a couple of contaminating samples from a neighbouring
// cluster do not rename the topic.
func TestLabelTakesThePlurality(t *testing.T) {
	got := label(labelPrompt(
		"a [[label:Merch drops]]",
		"b [[label:Ticket resale]]",
		"c [[label:Merch drops]]",
		"d [[label:Merch drops]]",
	))
	if got.Label != "Merch drops" {
		t.Errorf("label = %q, want Merch drops", got.Label)
	}
}

func TestLabelFallsBackToCommonWords(t *testing.T) {
	got := label(labelPrompt(
		"the speedrun record is unreal",
		"that speedrun was clean",
		"speedrun category rules changed",
	))
	if !strings.Contains(strings.ToLower(got.Label), "speedrun") {
		t.Errorf("label = %q, want it to mention the common word", got.Label)
	}
	// The fixture scaffolding must not leak into the name.
	marked := label(labelPrompt(
		"speedrun talk [[sim:runs:0.97:a]]",
		"speedrun chat [[sim:runs:0.97:b]]",
	))
	if strings.Contains(marked.Label, "sim") || strings.Contains(marked.Label, "[[") {
		t.Errorf("label = %q leaked the similarity marker", marked.Label)
	}
}

func TestLabelIsDeterministic(t *testing.T) {
	p := labelPrompt("apples and pears", "pears and apples", "apples again")
	if a, b := label(p), label(p); a != b {
		t.Errorf("label is not deterministic: %+v vs %+v", a, b)
	}
}

func TestLabelWithNothingInCommon(t *testing.T) {
	got := label(labelPrompt("a", "b", "c"))
	if got.Label != "Unlabelled cluster" {
		t.Errorf("label = %q, want the honest placeholder", got.Label)
	}
}

// End to end through the handler: clusters-job parses choices[0].message.content
// as {"label","description"} JSON, so the mock must produce exactly that.
func TestCompletionsReturnsLabelJSONForALabelPrompt(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"model": "moderation",
		"messages": []map[string]string{
			{"role": "system", "content": "You name clusters of semantically similar chat messages"},
			{"role": "user", "content": labelPrompt("tickets tonight [[label:Ticket resale]]")},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	completions(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var lb labelBody
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &lb); err != nil {
		t.Fatalf("content is not a label body: %v (content: %s)", err, resp.Choices[0].Message.Content)
	}
	if lb.Label != "Ticket resale" {
		t.Errorf("label = %q, want Ticket resale", lb.Label)
	}
}

// The §7.9 path must be untouched — test/e2e depends on FLAGME.
func TestCompletionsStillReturnsVerdictsForModeration(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"model": "moderation",
		"messages": []map[string]string{
			{"role": "user", "content": "Messages:\n1. FLAGME selling tickets\n2. hello everyone\n"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	completions(rec, req)

	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var vb verdictBody
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &vb); err != nil {
		t.Fatalf("content is not a verdict body: %v", err)
	}
	if len(vb.Results) != 2 || vb.Results[0].Violates != 1 || vb.Results[1].Violates != 0 {
		t.Fatalf("verdicts = %+v, want message 1 flagged and 2 clean", vb.Results)
	}
}
