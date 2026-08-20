// Command mockllm is an OpenAI-compatible /v1/chat/completions mock for
// local development. It is deterministic: within the prompt's numbered
// "Messages:" list (docs §7.9), any message containing the literal string
// "FLAGME" violates rule 1; everything else violates nothing.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
)

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type verdict struct {
	I        int `json:"i"`
	Violates int `json:"violates"`
}

type verdictBody struct {
	Results []verdict `json:"results"`
}

var lineRe = regexp.MustCompile(`^\s*(\d+)\.\s?(.*)$`)

// classify extracts the numbered list following a "Messages:" line and
// returns one verdict per entry. If no list is found, the whole prompt is
// treated as a single message.
func classify(prompt string) []verdict {
	var results []verdict
	inMessages := false
	n := 0
	for _, line := range strings.Split(prompt, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Messages:") {
			inMessages = true
			continue
		}
		if !inMessages {
			continue
		}
		m := lineRe.FindStringSubmatch(line)
		if m == nil {
			if trimmed == "" {
				continue
			}
			break
		}
		n++
		v := 0
		if strings.Contains(m[2], "FLAGME") {
			v = 1
		}
		results = append(results, verdict{I: n, Violates: v})
	}
	if len(results) == 0 {
		v := 0
		if strings.Contains(prompt, "FLAGME") {
			v = 1
		}
		results = []verdict{{I: 1, Violates: v}}
	}
	return results
}

func completions(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	var prompt strings.Builder
	for _, m := range req.Messages {
		prompt.WriteString(m.Content)
		prompt.WriteString("\n")
	}
	content, _ := json.Marshal(verdictBody{Results: classify(prompt.String())})

	resp := map[string]any{
		"id":      "chatcmpl-mock",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   req.Model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": string(content)},
			"finish_reason": "stop",
		}},
		"usage": map[string]int{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockllm")
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8089"
	}
	// Latency injection is configured entirely by MOCKLLM_* environment
	// variables and is inert when none are set, so the deterministic
	// FLAGME behaviour test/e2e depends on is unchanged (see latency.go).
	inj := newInjector(logger)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", inj.wrap(completions))
	mux.HandleFunc("GET /mockllm/stats", inj.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	logger.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server failed", "error", err.Error())
		os.Exit(1)
	}
}
