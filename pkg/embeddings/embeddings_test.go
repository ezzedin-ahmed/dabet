package embeddings

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEmbedContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != Path {
			t.Errorf("path = %q, want %q", r.URL.Path, Path)
		}
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		vectors := make([][]float32, len(req.Texts))
		for i := range req.Texts {
			vectors[i] = make([]float32, Dimensions)
		}
		_ = json.NewEncoder(w).Encode(Response{Vectors: vectors})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	vecs, err := c.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 2 || len(vecs[0]) != Dimensions {
		t.Errorf("got %d vectors of dim %d", len(vecs), len(vecs[0]))
	}
}

func TestEmbedCountMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(Response{Vectors: [][]float32{}})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	if _, err := c.Embed(context.Background(), []string{"one"}); err == nil {
		t.Error("want error on vector count mismatch")
	}
}
