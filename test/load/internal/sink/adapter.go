package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"dabet/test/load/internal/gen"
	"dabet/test/load/internal/hist"
)

// Adapter drives provider-adapter's POST /mock/messages ingress: one
// HTTP request per message. It exists to measure that hop separately,
// not to drive the system — at anything above a few thousand messages a
// second the request-per-message shape is the bottleneck long before
// moderation is, which is precisely the finding it is here to quantify.
//
// The adapter mints its own content_id / author_id / message_id from
// (channel, author), so records sent this way carry the generator's
// population shape but not its identifiers; the ingested_at clock also
// becomes the adapter's, not the harness's ideal clock. Both are
// documented limitations of adapter-ingress mode.
type Adapter struct {
	Counters
	base   string
	client *http.Client
	// Latency of the ingress call itself, which is the number this
	// mode exists to produce.
	lat *hist.Recorder
}

// NewAdapter builds an ingress sink against the adapter's base URL.
func NewAdapter(base string, concurrency int) *Adapter {
	if concurrency < 1 {
		concurrency = 1
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = concurrency * 2
	tr.MaxIdleConnsPerHost = concurrency * 2
	tr.MaxConnsPerHost = concurrency * 2
	return &Adapter{
		base:   base,
		client: &http.Client{Transport: tr, Timeout: 30 * time.Second},
		lat:    hist.New(),
	}
}

type injectRequest struct {
	CreatorID string `json:"creator_id"`
	Channel   string `json:"channel"`
	Author    string `json:"author"`
	Text      string `json:"text"`
}

// Send posts one message.
func (a *Adapter) Send(ctx context.Context, rec gen.Record) error {
	a.accepted.Add(1)
	body, err := json.Marshal(injectRequest{
		CreatorID: rec.Msg.CreatorID,
		Channel:   rec.Msg.ContentID,
		Author:    rec.Msg.AuthorID,
		Text:      rec.Msg.Text,
	})
	if err != nil {
		a.failed.Add(1)
		return err
	}
	a.bytes.Add(int64(len(body)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/mock/messages", bytes.NewReader(body))
	if err != nil {
		a.failed.Add(1)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.failed.Add(1)
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	a.lat.Record(time.Since(start))
	if resp.StatusCode != http.StatusAccepted {
		a.failed.Add(1)
		return fmt.Errorf("adapter ingress: status %d", resp.StatusCode)
	}
	a.acked.Add(1)
	return nil
}

// Flush is a no-op: the sink is synchronous.
func (a *Adapter) Flush(context.Context) error { return nil }

// Close releases idle connections.
func (a *Adapter) Close() { a.client.CloseIdleConnections() }

// Latency exposes the ingress call latency distribution.
func (a *Adapter) Latency() *hist.Recorder { return a.lat }
