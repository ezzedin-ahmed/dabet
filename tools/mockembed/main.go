// Command mockembed serves the shared embedding contract
// (POST /v1/embed {"texts":[...]} -> {"vectors":[[f32 x 384]]}) with
// deterministic vectors: each text's vector is derived from a SHA-256
// counter chain over the text and L2-normalised, so identical texts always
// embed identically.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const dimensions = 384

type embedRequest struct {
	Texts []string `json:"texts"`
}

type embedResponse struct {
	Vectors [][]float32 `json:"vectors"`
}

func embed(text string) []float32 {
	seed := sha256.Sum256([]byte(text))
	vec := make([]float32, 0, dimensions)
	var counter [4]byte
	for i := 0; len(vec) < dimensions; i++ {
		binary.BigEndian.PutUint32(counter[:], uint32(i))
		block := sha256.Sum256(append(seed[:], counter[:]...))
		for off := 0; off+4 <= len(block) && len(vec) < dimensions; off += 4 {
			u := binary.BigEndian.Uint32(block[off : off+4])
			vec = append(vec, float32(int32(u))/float32(math.MaxInt32))
		}
	}
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		norm = 1
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
	return vec
}

func handleEmbed(w http.ResponseWriter, r *http.Request) {
	var req embedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	vectors := make([][]float32, len(req.Texts))
	for i, t := range req.Texts {
		vectors[i] = embed(t)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(embedResponse{Vectors: vectors})
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockembed")
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8091"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/embed", handleEmbed)
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
