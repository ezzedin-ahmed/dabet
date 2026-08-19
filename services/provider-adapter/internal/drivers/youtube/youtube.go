// Package youtube is the YouTube driver (docs §7.2 table, A14).
//
// Ingestion transport sketch: YouTube live chat is polled, not streamed —
// DiscoverLive polls liveBroadcasts.list for active broadcasts on the
// connected channel, then Watch polls liveChatMessages.list with the
// pollingIntervalMillis the API dictates, under a hard daily quota. The
// platform's own delivery delay is why the latency SLI starts at adapter
// ingress (§4.6). Both remain ErrNotImplemented until the live-API phase;
// verify against current provider documentation before implementing (A14).
//
// Delete is implemented against the documented REST endpoint:
// DELETE https://www.googleapis.com/youtube/v3/liveChat/messages?id={id}
// with the connection's user OAuth token.
package youtube

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"dabet/services/provider-adapter/internal/driver"
)

// DefaultBaseURL is the production API root.
const DefaultBaseURL = "https://www.googleapis.com/youtube/v3"

// Driver implements driver.Driver for YouTube.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// Resolver maps opaque message ids back to native LiveChatMessage ids.
	Resolver driver.Resolver
}

// New returns a YouTube driver using resolver for opaque-id resolution.
func New(resolver driver.Resolver) *Driver {
	return &Driver{HTTPClient: http.DefaultClient, BaseURL: DefaultBaseURL, Resolver: resolver}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "youtube" }

// Watch implements driver.Driver. Not wired until the live-API phase (see
// package comment for the polling sketch).
func (d *Driver) Watch(context.Context, driver.Connection, chan<- driver.Message) error {
	return fmt.Errorf("youtube watch (poll liveChatMessages.list): %w", driver.ErrNotImplemented)
}

// DiscoverLive implements driver.Driver. Not wired until the live-API
// phase (poll liveBroadcasts.list for active broadcasts).
func (d *Driver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, fmt.Errorf("youtube discovery (poll liveBroadcasts.list): %w", driver.ErrNotImplemented)
}

// Delete implements driver.Driver via liveChatMessages.delete.
func (d *Driver) Delete(ctx context.Context, conn driver.Connection, _, messageID string) error {
	native, ok := d.Resolver.NativeMessageID(messageID)
	if !ok {
		// Unresolvable on this instance (restart or rebalance since
		// ingest): best-effort semantics, treated as already gone.
		return driver.ErrNotFound
	}
	base := d.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	u := base + "/liveChat/messages?id=" + url.QueryEscape(native)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	client := d.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// YouTube reports an ended live chat as 403 liveChatEnded: the content
	// is gone, terminal drop (§7.2).
	if resp.StatusCode == http.StatusForbidden {
		return driver.ErrGone
	}
	return driver.FromHTTPStatus(resp.StatusCode)
}
