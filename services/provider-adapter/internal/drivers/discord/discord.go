// Package discord is the Discord driver (docs §7.2 table, A14).
//
// Ingestion transport sketch: the Dabet bot is resident in the guild, so
// Watch holds a Gateway WebSocket session (identify with MESSAGE_CONTENT
// and GUILD_MESSAGES intents) and relays MESSAGE_CREATE dispatches; shard
// count scales with guild count. Liveness is trivial — the bot is always
// resident — so DiscoverLive reports the guild channels the bot can read.
// Both remain ErrNotImplemented until the live-API phase; verify against
// current provider documentation before implementing (A14).
//
// Delete is implemented against the documented REST endpoint:
// DELETE https://discord.com/api/v10/channels/{channel.id}/messages/{message.id}
// authenticated as the bot (requires MANAGE_MESSAGES).
package discord

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"dabet/services/provider-adapter/internal/driver"
)

// DefaultBaseURL is the production API root.
const DefaultBaseURL = "https://discord.com/api/v10"

// Driver implements driver.Driver for Discord.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// Resolver maps opaque content/message ids back to native ids.
	Resolver driver.Resolver
}

// New returns a Discord driver.
func New(resolver driver.Resolver) *Driver {
	return &Driver{HTTPClient: http.DefaultClient, BaseURL: DefaultBaseURL, Resolver: resolver}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "discord" }

// Watch implements driver.Driver. Not wired until the live-API phase (see
// package comment for the Gateway sketch).
func (d *Driver) Watch(context.Context, driver.Connection, chan<- driver.Message) error {
	return fmt.Errorf("discord watch (Gateway WebSocket MESSAGE_CREATE): %w", driver.ErrNotImplemented)
}

// DiscoverLive implements driver.Driver. Not wired until the live-API
// phase (bot is always resident; list readable guild channels).
func (d *Driver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, fmt.Errorf("discord discovery (resident bot channel list): %w", driver.ErrNotImplemented)
}

// Delete implements driver.Driver via the channel message delete endpoint.
func (d *Driver) Delete(ctx context.Context, conn driver.Connection, contentID, messageID string) error {
	channel, ok := d.Resolver.NativeContentID(contentID)
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
	u := fmt.Sprintf("%s/channels/%s/messages/%s", base, url.PathEscape(channel), url.PathEscape(message))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+conn.AccessToken)
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
