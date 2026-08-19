// Package twitch is the Twitch driver (docs §7.2 table, A14).
//
// Ingestion transport sketch: Watch holds an EventSub WebSocket session,
// subscribing to channel.chat.message for the connected broadcaster (or
// falls back to IRC); liveness comes from EventSub stream.online /
// stream.offline subscriptions, so DiscoverLive queries Get Streams once
// at startup and EventSub keeps it current. Subscription cost limits per
// socket are the scaling constraint. Both remain ErrNotImplemented until
// the live-API phase; verify against current provider documentation before
// implementing (A14).
//
// Delete is implemented against the documented Helix endpoint:
// DELETE https://api.twitch.tv/helix/moderation/chat
//
//	?broadcaster_id={..}&moderator_id={..}&message_id={..}
//
// with the connection's user OAuth token (the user must be a channel
// moderator) plus the app's Client-Id header.
package twitch

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"dabet/services/provider-adapter/internal/driver"
)

// DefaultBaseURL is the production Helix root.
const DefaultBaseURL = "https://api.twitch.tv/helix"

// Driver implements driver.Driver for Twitch.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// Resolver maps opaque content/message ids back to native ids.
	Resolver driver.Resolver
	// ClientID is the Twitch application client id, required on every
	// Helix call.
	ClientID string
}

// New returns a Twitch driver.
func New(resolver driver.Resolver, clientID string) *Driver {
	return &Driver{HTTPClient: http.DefaultClient, BaseURL: DefaultBaseURL, Resolver: resolver, ClientID: clientID}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "twitch" }

// Watch implements driver.Driver. Not wired until the live-API phase (see
// package comment for the EventSub sketch).
func (d *Driver) Watch(context.Context, driver.Connection, chan<- driver.Message) error {
	return fmt.Errorf("twitch watch (EventSub WebSocket channel.chat.message): %w", driver.ErrNotImplemented)
}

// DiscoverLive implements driver.Driver. Not wired until the live-API
// phase (Get Streams + EventSub stream.online).
func (d *Driver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, fmt.Errorf("twitch discovery (EventSub stream.online): %w", driver.ErrNotImplemented)
}

// Delete implements driver.Driver via Helix delete-chat-message.
func (d *Driver) Delete(ctx context.Context, conn driver.Connection, contentID, messageID string) error {
	broadcaster, ok := d.Resolver.NativeContentID(contentID)
	if !ok {
		return driver.ErrNotFound
	}
	message, ok := d.Resolver.NativeMessageID(messageID)
	if !ok {
		return driver.ErrNotFound
	}
	base := d.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	q := url.Values{
		"broadcaster_id": {broadcaster},
		"moderator_id":   {conn.NativeUserID},
		"message_id":     {message},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, base+"/moderation/chat?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	req.Header.Set("Client-Id", d.ClientID)
	client := d.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return driver.FromHTTPStatus(resp.StatusCode)
}
