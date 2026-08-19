package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func vllmServer(t *testing.T, handler func(w http.ResponseWriter, req chatRequest)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var req chatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		handler(w, req)
	}))
}

func chatReply(w http.ResponseWriter, content string) {
	resp := map[string]any{
		"choices": []map[string]any{{"message": map[string]any{"content": content}}},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func TestVLLMLabelerHappyPath(t *testing.T) {
	var gotReq chatRequest
	srv := vllmServer(t, func(w http.ResponseWriter, req chatRequest) {
		gotReq = req
		chatReply(w, `{"label": "Ticket resale", "description": "Viewers trading event tickets."}`)
	})
	defer srv.Close()

	l := NewVLLMLabeler(srv.URL, "test-model", time.Second)
	label, desc, err := l.Label(context.Background(), []string{"selling two tickets", "who needs tickets"}, "Old tickets")
	if err != nil {
		t.Fatal(err)
	}
	if label != "Ticket resale" || desc != "Viewers trading event tickets." {
		t.Errorf("got %q / %q", label, desc)
	}
	if gotReq.Model != "test-model" {
		t.Errorf("model = %q", gotReq.Model)
	}
	if gotReq.ResponseFormat == nil || gotReq.ResponseFormat.Type != "json_object" {
		t.Errorf("response_format = %+v, want json_object", gotReq.ResponseFormat)
	}
	if len(gotReq.Messages) != 2 {
		t.Fatalf("got %d messages", len(gotReq.Messages))
	}
	user := gotReq.Messages[1].Content
	if !strings.Contains(user, "selling two tickets") || !strings.Contains(user, "Old tickets") {
		t.Errorf("user prompt missing samples or prior label: %q", user)
	}
}

func TestVLLMLabelerTimeout(t *testing.T) {
	srv := vllmServer(t, func(w http.ResponseWriter, _ chatRequest) {
		time.Sleep(300 * time.Millisecond)
		chatReply(w, `{"label": "Too late"}`)
	})
	defer srv.Close()

	l := NewVLLMLabeler(srv.URL, "m", 30*time.Millisecond)
	if _, _, err := l.Label(context.Background(), []string{"hi"}, ""); err == nil {
		t.Fatal("want timeout error, got nil")
	}
}

func TestVLLMLabelerBadResponses(t *testing.T) {
	cases := map[string]func(w http.ResponseWriter, req chatRequest){
		"not json":    func(w http.ResponseWriter, _ chatRequest) { chatReply(w, "not json at all") },
		"empty label": func(w http.ResponseWriter, _ chatRequest) { chatReply(w, `{"label": "  "}`) },
		"no choices": func(w http.ResponseWriter, _ chatRequest) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"choices": []}`)
		},
		"http 500": func(w http.ResponseWriter, _ chatRequest) { w.WriteHeader(500) },
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			srv := vllmServer(t, h)
			defer srv.Close()
			l := NewVLLMLabeler(srv.URL, "m", time.Second)
			if _, _, err := l.Label(context.Background(), []string{"hi"}, ""); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}
