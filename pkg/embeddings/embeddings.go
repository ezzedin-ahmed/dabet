// Package embeddings is the shared embedding-service contract:
// POST /v1/embed {"texts":[...]} -> {"vectors":[[f32 x 384]]}.
// One service is shared by semantic spam (§7.4) and Insights (§8.4) so a
// message is embedded at most once.
package embeddings

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"dabet/pkg/tracing"
)

// Dimensions is the embedding width (docs §8.4, A21).
const Dimensions = 384

// Path is the embed endpoint path.
const Path = "/v1/embed"

// Request is the embed request body.
type Request struct {
	Texts []string `json:"texts"`
}

// Response is the embed response body.
type Response struct {
	Vectors [][]float32 `json:"vectors"`
}

// Client calls the embedding service.
type Client struct {
	base  string
	httpc *http.Client
}

// NewClient builds a client for the embedding service at baseURL with the
// given request timeout. The transport is trace-instrumented, so an embed
// call made while handling a message shows up as a child span of that
// message's trace — which matters because §4.6 budgets 100 ms for it.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		base:  strings.TrimSuffix(baseURL, "/"),
		httpc: tracing.HTTPClient(timeout),
	}
}

// Embed returns one vector per input text, in order.
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(Request{Texts: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+Path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed: unexpected status %d", resp.StatusCode)
	}
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Vectors) != len(texts) {
		return nil, fmt.Errorf("embed: got %d vectors for %d texts", len(out.Vectors), len(texts))
	}
	return out.Vectors, nil
}
