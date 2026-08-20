// Package discord is the Discord driver (docs §7.2 table, A14).
//
// # Verified against provider documentation (2026-08-19)
//
//   - Gateway — https://docs.discord.com/developers/events/gateway
//     Connect to wss://gateway.discord.gg/?v=10&encoding=json. The server
//     opens with Hello (opcode 10) carrying d.heartbeat_interval in
//     milliseconds. "Upon receiving the Hello event, your app should wait
//     heartbeat_interval * jitter where jitter is any random value between
//     0 and 1, then send its first Heartbeat", and every heartbeat_interval
//     thereafter. Heartbeat is opcode 1 with d set to the last sequence
//     number seen in a Dispatch's s field, or null if none. The server
//     answers with Heartbeat ACK (opcode 11); if no ACK arrives between
//     heartbeats the client "should immediately terminate the connection
//     with any close code besides 1000 or 1001, then reconnect". The server
//     may also send opcode 1 to demand an immediate heartbeat.
//   - Identify (opcode 2) carries d.token, d.intents and d.properties
//     ({os, browser, device}), optionally d.compress, d.large_threshold and
//     d.shard as [shard_id, num_shards].
//   - READY (opcode 0, t=READY) carries d.session_id and
//     d.resume_gateway_url, both of which must be cached: a resume goes to
//     resume_gateway_url, not the original gateway URL.
//   - Resume (opcode 6) carries d.token, d.session_id and d.seq, and no
//     Identify follows it.
//   - Reconnect (opcode 7) means close and resume. Invalid Session
//     (opcode 9) carries a boolean d: true means the session can still be
//     resumed, false means drop it and Identify afresh.
//   - Close codes —
//     https://docs.discord.com/developers/topics/opcodes-and-status-codes
//     Reconnectable: 4000 unknown error, 4001 unknown opcode, 4002 decode
//     error, 4003 not authenticated, 4005 already authenticated, 4007
//     invalid seq, 4008 rate limited, 4009 session timed out. Fatal: 4004
//     authentication failed, 4010 invalid shard, 4011 sharding required,
//     4012 invalid API version, 4013 invalid intents, 4014 disallowed
//     (unapproved privileged) intents.
//   - Intents — GUILD_MESSAGES is 1<<9 and MESSAGE_CONTENT is 1<<15.
//     MESSAGE_CONTENT is privileged and is what populates the content,
//     embeds, attachments, components and poll fields; without it
//     MESSAGE_CREATE arrives with an empty content string. Both are
//     required here: one to receive the event, the other to see the text
//     there is any point moderating.
//   - Rate limits: 120 gateway events per connection per 60 s, a concurrent
//     Identify limit per 5 s (exceeding it answers with Invalid Session),
//     and 1000 IDENTIFYs per 24 h before the bot token is reset — which is
//     why a failed session backs off with jitter rather than reconnecting
//     hot, and why RESUME is preferred over IDENTIFY wherever the protocol
//     allows it.
//   - Get Gateway Bot — GET /gateway/bot returns url, the recommended
//     shards, and session_start_limit. Get Current User Guilds — GET
//     /users/@me/guilds returns partial guilds; Get Guild Channels — GET
//     /guilds/{guild.id}/channels returns channel objects with type 0
//     GUILD_TEXT and 5 GUILD_ANNOUNCEMENT being the text channels a bot
//     moderates.
//
// # Identity: a bot install, not user OAuth (§5.5)
//
// Every Discord call here authenticates with `Authorization: Bot <token>`,
// and the token is the application's, installed into a guild with
// MANAGE_MESSAGES — not a per-creator OAuth grant. Two consequences the
// rest of the adapter has to live with. There is no refresh token, so a
// 4004 close (authentication failed) unwinds as ErrUnauthorized and the
// §5.6 path will find nothing to exchange and move the connection to
// expired, which is the correct outcome: a reset bot token needs a human.
// And liveness is trivial — the bot is resident whether or not anyone is
// talking — so DiscoverLive enumerates the text channels the bot can see
// rather than asking what is "live".
//
// # Where the live docs differ from §7.2
//
// §7.2 gives shard count scaling with guild count as the key constraint.
// That is still true, and Watch honours the shard count GET /gateway/bot
// recommends, running one Gateway session per shard under one Watch call.
// A very large bot would want shards spread across adapter instances rather
// than concentrated in the one that owns the connection; that is the same
// problem A13's coordinator solves for connections and is left to it.
//
// Delete is implemented against the documented REST endpoint:
// DELETE https://discord.com/api/v10/channels/{channel.id}/messages/{message.id}
// authenticated as the bot (requires MANAGE_MESSAGES).
package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"dabet/services/provider-adapter/internal/dedup"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/retry"
	"dabet/services/provider-adapter/internal/wsx"
)

// DefaultBaseURL is the production API root.
const DefaultBaseURL = "https://discord.com/api/v10"

// DefaultGatewayURL is the fallback gateway when GET /gateway/bot cannot be
// reached. The documented flow is to ask for the URL; this keeps a
// transient REST failure from stopping ingestion outright.
const DefaultGatewayURL = "wss://gateway.discord.gg"

// APIVersion is the gateway and REST version this driver speaks.
const APIVersion = "10"

// Gateway opcodes.
const (
	opDispatch            = 0
	opHeartbeat           = 1
	opIdentify            = 2
	opResume              = 6
	opReconnect           = 7
	opInvalidSession      = 9
	opHello               = 10
	opHeartbeatACK        = 11
	opRequestGuildMembers = 8 // unused; listed so the set reads complete
)

// Gateway intents (bit positions from the intents table).
const (
	// IntentGuildMessages (1<<9) delivers MESSAGE_CREATE.
	IntentGuildMessages = 1 << 9
	// IntentMessageContent (1<<15) is privileged and is what populates the
	// content field; without it MESSAGE_CREATE arrives with empty text.
	IntentMessageContent = 1 << 15
	// Intents is what this driver identifies with: the event, and the text.
	Intents = IntentGuildMessages | IntentMessageContent
)

// Channel types that carry moderatable text.
const (
	channelGuildText         = 0
	channelGuildAnnouncement = 5
)

// helloTimeout bounds the wait for the Hello frame after dialling.
const helloTimeout = 15 * time.Second

// Driver implements driver.Driver for Discord.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// GatewayURL overrides the URL GET /gateway/bot reports. Tests set it;
	// production leaves it empty so the documented discovery runs.
	GatewayURL string
	// Dialer opens gateway sockets; nil means a production dialer.
	Dialer wsx.Dialer
	// Resolver maps opaque content/message ids back to native ids.
	Resolver driver.Resolver
	// Shards overrides the shard count GET /gateway/bot recommends. 0 uses
	// the recommendation; the driver never runs fewer than one.
	Shards int
	// MaxShards caps how many gateway sockets one Watch will open, so a
	// surprising recommendation cannot exhaust an instance.
	MaxShards int
	// DedupWindow is how many recent message ids are remembered per shard.
	DedupWindow int
	// Backoff is the reconnect policy for transient failures.
	Backoff retry.Backoff
	// Jitter returns a uniform float in [0,1); nil means rand.Float64. It
	// seeds the documented first-heartbeat jitter.
	Jitter func() float64
	// Log receives operational events. P4: never message text.
	Log *slog.Logger
}

// DefaultMaxShards bounds a single Watch call's socket count.
const DefaultMaxShards = 16

// New returns a Discord driver.
func New(resolver driver.Resolver) *Driver {
	return &Driver{
		HTTPClient:  http.DefaultClient,
		BaseURL:     DefaultBaseURL,
		Dialer:      wsx.NewDialer(),
		Resolver:    resolver,
		MaxShards:   DefaultMaxShards,
		DedupWindow: dedup.DefaultCapacity,
		Backoff:     retry.DefaultBackoff(),
		Jitter:      rand.Float64,
		Log:         slog.Default(),
	}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "discord" }

func (d *Driver) baseURL() string {
	if d.BaseURL == "" {
		return DefaultBaseURL
	}
	return d.BaseURL
}

func (d *Driver) client() *http.Client {
	if d.HTTPClient == nil {
		return http.DefaultClient
	}
	return d.HTTPClient
}

func (d *Driver) dialer() wsx.Dialer {
	if d.Dialer == nil {
		return wsx.NewDialer()
	}
	return d.Dialer
}

func (d *Driver) log() *slog.Logger {
	if d.Log == nil {
		return slog.Default()
	}
	return d.Log
}

func (d *Driver) jitter() float64 {
	if d.Jitter == nil {
		return rand.Float64()
	}
	return d.Jitter()
}

// ---------------------------------------------------------------------
// Gateway wire types
// ---------------------------------------------------------------------

// payload is the gateway envelope: op, d, s and t.
type payload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s"`
	T  string          `json:"t"`
}

type helloData struct {
	HeartbeatInterval int64 `json:"heartbeat_interval"`
}

type readyData struct {
	V    int `json:"v"`
	User struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"user"`
	SessionID        string `json:"session_id"`
	ResumeGatewayURL string `json:"resume_gateway_url"`
}

// messageCreate is the MESSAGE_CREATE dispatch, narrowed. P5: none of this
// leaves the driver.
type messageCreate struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	GuildID   string `json:"guild_id"`
	Content   string `json:"content"`
	Author    struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
		System   bool   `json:"system"`
	} `json:"author"`
	// Type distinguishes a normal message (0) and a reply (19) from the
	// system notices (joins, pins, boosts) that share the event.
	Type int `json:"type"`
	// WebhookID is set for webhook posts, which have no real author.
	WebhookID string `json:"webhook_id"`
}

// Message types carrying user-authored text worth moderating.
const (
	messageTypeDefault = 0
	messageTypeReply   = 19
)

// ---------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------

// Watch implements driver.Driver.
//
// One connection is one bot install, which the gateway may require to be
// spread over several shards — so Watch supervises one gateway session per
// shard. Each session is independently resilient: it resumes where it can,
// re-identifies where it must, and reconnects with jittered backoff
// otherwise. Watch itself returns only when ctx is cancelled or when a
// shard hits something no reconnect can fix (a rejected token, intents the
// application was never approved for), because those failures are shared by
// every shard.
func (d *Driver) Watch(ctx context.Context, conn driver.Connection, out chan<- driver.Message) error {
	if conn.AccessToken == "" {
		return fmt.Errorf("discord: connection has no bot token: %w", driver.ErrPermanent)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gateway, shards, err := d.gatewayInfo(ctx, conn)
	if err != nil {
		// A REST hiccup must not stop ingestion; fall back to the
		// documented default gateway and a single shard.
		if errors.Is(err, driver.ErrUnauthorized) || driver.Terminal(err) {
			return err
		}
		d.log().Warn("discord gateway lookup failed; using defaults",
			"connection_id", conn.ID, "platform", "discord", "error", err.Error())
		gateway, shards = DefaultGatewayURL, 1
	}

	fatal := make(chan error, shards)
	var wg sync.WaitGroup
	for i := 0; i < shards; i++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			if err := d.runShard(ctx, conn, out, gateway, shard, shards); err != nil {
				select {
				case fatal <- err:
				default:
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-ctx.Done():
		cancel()
		<-done
		return nil
	case err := <-fatal:
		cancel()
		<-done
		return err
	case <-done:
		// Every shard ended without a fatal error, which only happens on
		// cancellation.
		return nil
	}
}

// runShard keeps one shard's gateway session alive until ctx is cancelled
// or a permanent failure occurs.
func (d *Driver) runShard(ctx context.Context, conn driver.Connection, out chan<- driver.Message, gateway string, shard, shards int) error {
	seen := dedup.New(d.DedupWindow)
	backoff := d.Backoff
	// Session state survives reconnects: it is what makes a RESUME possible.
	var st sessionState
	for {
		started := time.Now()
		err := d.session(ctx, conn, out, seen, gateway, shard, shards, &st)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, driver.ErrUnauthorized), driver.Terminal(err):
			return err
		case err != nil:
			d.log().Warn("discord gateway session ended; reconnecting",
				"connection_id", conn.ID, "platform", "discord", "shard", shard, "error", err.Error())
		}
		if time.Since(started) >= healthySession {
			backoff.Reset()
		}
		// Backing off always, even after a clean end, matters here: the
		// 1000-IDENTIFYs-per-day ceiling resets the bot token when crossed.
		if werr := backoff.Wait(ctx); werr != nil {
			return nil
		}
	}
}

// healthySession is how long a session must last before its failure counts
// as a new incident rather than a continuing one.
const healthySession = 60 * time.Second

// sessionState is what a RESUME needs, carried across reconnects.
type sessionState struct {
	sessionID string
	resumeURL string
	seq       int64
	haveSeq   bool
}

// resumable reports whether there is enough state to attempt a RESUME.
func (s *sessionState) resumable() bool { return s.sessionID != "" && s.haveSeq }

// invalidate drops the session so the next connection identifies afresh.
func (s *sessionState) invalidate() {
	s.sessionID = ""
	s.resumeURL = ""
	s.seq = 0
	s.haveSeq = false
}

// session runs one gateway connection: hello, identify-or-resume,
// heartbeat, dispatch relay, until the socket ends.
func (d *Driver) session(ctx context.Context, conn driver.Connection, out chan<- driver.Message, seen *dedup.Set, gateway string, shard, shards int, st *sessionState) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A resume goes to resume_gateway_url, not the original gateway URL.
	dialURL := gateway
	resuming := st.resumable()
	if resuming && st.resumeURL != "" {
		dialURL = st.resumeURL
	}

	c, err := d.dialer().Dial(ctx, gatewayURL(dialURL), nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(closeResume, "reconnecting") }()
	frames := wsx.Pump(ctx, c)

	// Hello first, always.
	interval, err := d.awaitHello(ctx, frames)
	if err != nil {
		return err
	}

	// "wait heartbeat_interval * jitter ... then send its first Heartbeat",
	// which is what stops every shard of every bot heartbeating in lockstep.
	first := time.Duration(float64(interval) * d.jitter())
	beat := time.NewTimer(first)
	defer beat.Stop()
	// acked tracks whether the last heartbeat was answered. A missed ACK
	// means a zombie connection and must close with a non-1000 code so the
	// session can be resumed.
	acked := true

	if resuming {
		if err := send(ctx, c, opResume, map[string]any{
			"token":      conn.AccessToken,
			"session_id": st.sessionID,
			"seq":        st.seq,
		}); err != nil {
			return err
		}
	} else {
		st.invalidate()
		if err := d.identify(ctx, c, conn, shard, shards); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-beat.C:
			if !acked {
				// Zombie: close with something other than 1000/1001 so the
				// session stays resumable, and reconnect.
				return errors.New("discord: heartbeat was not acknowledged")
			}
			acked = false
			if err := sendHeartbeat(ctx, c, st); err != nil {
				return err
			}
			beat.Reset(interval)

		case f, ok := <-frames:
			if !ok {
				return nil
			}
			if f.Err != nil {
				return classifyClose(f.Err)
			}
			var p payload
			if err := json.Unmarshal(f.Data, &p); err != nil {
				// P2: one bad frame is not worth dropping a guild's chat.
				d.log().Warn("discord: undecodable gateway frame",
					"connection_id", conn.ID, "platform", "discord", "shard", shard)
				continue
			}
			if p.S != nil {
				st.seq = *p.S
				st.haveSeq = true
			}

			switch p.Op {
			case opHeartbeatACK:
				acked = true

			case opHeartbeat:
				// The server can demand an immediate heartbeat.
				if err := sendHeartbeat(ctx, c, st); err != nil {
					return err
				}

			case opReconnect:
				// Close and resume against resume_gateway_url.
				return nil

			case opInvalidSession:
				var resumable bool
				_ = json.Unmarshal(p.D, &resumable)
				if !resumable {
					st.invalidate()
				}
				// Discord asks for a short randomised pause before the
				// retry; the shard's backoff supplies it.
				return nil

			case opDispatch:
				switch p.T {
				case "READY":
					var rd readyData
					if err := json.Unmarshal(p.D, &rd); err != nil {
						return fmt.Errorf("discord: undecodable READY")
					}
					st.sessionID = rd.SessionID
					st.resumeURL = rd.ResumeGatewayURL
					d.log().Info("discord gateway ready",
						"connection_id", conn.ID, "platform", "discord", "shard", shard)
				case "RESUMED":
					d.log().Info("discord gateway resumed",
						"connection_id", conn.ID, "platform", "discord", "shard", shard)
				case "MESSAGE_CREATE":
					var mc messageCreate
					if err := json.Unmarshal(p.D, &mc); err != nil {
						continue
					}
					if !moderatable(mc) {
						continue
					}
					if !seen.Add(mc.ID) {
						continue
					}
					if err := driver.Send(ctx, out, driver.Message{
						NativeChannelID: mc.ChannelID,
						NativeAuthorID:  mc.Author.ID,
						NativeMessageID: mc.ID,
						Text:            mc.Content,
						// The frame was read just now: the §4.6 clock
						// starts here, at adapter ingress.
						ReceivedAt: time.Now(),
					}); err != nil {
						return nil
					}
				}
			}
		}
	}
}

// moderatable reports whether a MESSAGE_CREATE carries user-authored text
// this system exists to moderate. Bots, webhooks, system notices and the
// join/pin/boost message types all arrive on the same event.
func moderatable(mc messageCreate) bool {
	switch {
	case mc.Content == "":
		// Either a pure attachment, or MESSAGE_CONTENT was not granted —
		// in which case there is nothing to moderate either way.
		return false
	case mc.Author.Bot || mc.Author.System || mc.WebhookID != "":
		return false
	case mc.Type != messageTypeDefault && mc.Type != messageTypeReply:
		return false
	}
	return true
}

// awaitHello consumes the Hello frame and returns the heartbeat interval.
func (d *Driver) awaitHello(ctx context.Context, frames <-chan wsx.Frame) (time.Duration, error) {
	t := time.NewTimer(helloTimeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-t.C:
		return 0, errors.New("discord: no Hello within timeout")
	case f, ok := <-frames:
		if !ok {
			return 0, errors.New("discord: socket closed before Hello")
		}
		if f.Err != nil {
			return 0, classifyClose(f.Err)
		}
		var p payload
		if err := json.Unmarshal(f.Data, &p); err != nil || p.Op != opHello {
			return 0, errors.New("discord: first frame was not Hello")
		}
		var h helloData
		if err := json.Unmarshal(p.D, &h); err != nil || h.HeartbeatInterval <= 0 {
			return 0, errors.New("discord: Hello carried no heartbeat_interval")
		}
		return time.Duration(h.HeartbeatInterval) * time.Millisecond, nil
	}
}

func (d *Driver) identify(ctx context.Context, c wsx.Conn, conn driver.Connection, shard, shards int) error {
	data := map[string]any{
		"token":   conn.AccessToken,
		"intents": Intents,
		"properties": map[string]any{
			"os":      "linux",
			"browser": "dabet",
			"device":  "dabet",
		},
		"compress":        false,
		"large_threshold": 250,
	}
	if shards > 1 {
		data["shard"] = []int{shard, shards}
	}
	return send(ctx, c, opIdentify, data)
}

// sendHeartbeat sends opcode 1 with the last sequence number, or null.
func sendHeartbeat(ctx context.Context, c wsx.Conn, st *sessionState) error {
	var d any
	if st.haveSeq {
		d = st.seq
	}
	return send(ctx, c, opHeartbeat, d)
}

func send(ctx context.Context, c wsx.Conn, op int, data any) error {
	raw, err := json.Marshal(map[string]any{"op": op, "d": data})
	if err != nil {
		return err
	}
	return c.Write(ctx, raw)
}

// closeResume is the close code used when the driver hangs up intending to
// resume. Anything other than 1000/1001 keeps the session resumable.
const closeResume = 4000

// classifyClose maps a gateway close code onto a driver error class,
// following the documented reconnect column.
func classifyClose(err error) error {
	code := wsx.CloseStatus(err)
	if code == wsx.StatusNoStatus {
		return err // transport failure; the shard retries with backoff
	}
	switch code {
	case 4004:
		// Authentication failed. There is no refresh token for a bot
		// install, so §5.6 will find nothing to exchange and expire the
		// connection — which is right: a reset token needs a human.
		return fmt.Errorf("discord: gateway rejected the bot token (4004): %w", driver.ErrUnauthorized)
	case 4010, 4011:
		return fmt.Errorf("discord: sharding rejected (%d): %w", code, driver.ErrPermanent)
	case 4012:
		return fmt.Errorf("discord: gateway API version %s rejected (4012): %w", APIVersion, driver.ErrPermanent)
	case 4013:
		return fmt.Errorf("discord: invalid intents (4013): %w", driver.ErrPermanent)
	case 4014:
		// The application was never approved for MESSAGE_CONTENT. Retrying
		// cannot help; a developer-portal change can.
		return fmt.Errorf("discord: disallowed privileged intent, MESSAGE_CONTENT is not approved (4014): %w", driver.ErrPermanent)
	case wsx.StatusNormalClosure:
		return nil
	default:
		// 4000-4003, 4005, 4007, 4008, 4009 and everything unlisted are
		// reconnectable.
		return fmt.Errorf("discord: gateway closed with code %d", code)
	}
}

// gatewayURL appends the version and encoding the driver speaks.
func gatewayURL(base string) string {
	sep := "?"
	if u, err := url.Parse(base); err == nil && u.RawQuery != "" {
		sep = "&"
	}
	return base + sep + "v=" + APIVersion + "&encoding=json"
}

// ---------------------------------------------------------------------
// REST: gateway info and discovery
// ---------------------------------------------------------------------

type gatewayBot struct {
	URL    string `json:"url"`
	Shards int    `json:"shards"`
}

// gatewayInfo asks GET /gateway/bot for the URL and recommended shard
// count, clamped to MaxShards.
func (d *Driver) gatewayInfo(ctx context.Context, conn driver.Connection) (string, int, error) {
	if d.GatewayURL != "" {
		return d.GatewayURL, d.shardCount(1), nil
	}
	var body gatewayBot
	if err := d.get(ctx, conn, "/gateway/bot", &body); err != nil {
		return "", 0, err
	}
	if body.URL == "" {
		body.URL = DefaultGatewayURL
	}
	return body.URL, d.shardCount(body.Shards), nil
}

// shardCount reconciles the recommendation with the configured override
// and cap.
func (d *Driver) shardCount(recommended int) int {
	n := recommended
	if d.Shards > 0 {
		n = d.Shards
	}
	if n < 1 {
		n = 1
	}
	maxShards := d.MaxShards
	if maxShards <= 0 {
		maxShards = DefaultMaxShards
	}
	return min(n, maxShards)
}

type partialGuild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type guildChannel struct {
	ID      string `json:"id"`
	Type    int    `json:"type"`
	GuildID string `json:"guild_id"`
	Name    string `json:"name"`
}

// DiscoverLive implements driver.Driver.
//
// Discord has no notion of "live": the bot is resident, so what is
// discoverable is the set of text channels it can see. Each becomes a
// ContentRef whose NativeChannelID is the channel snowflake — the same id
// the delete endpoint needs, so a deletion routes with no lookup.
//
// A guild that cannot be enumerated (the bot lost View Channel between the
// two calls, a transient 5xx) is skipped rather than failing the whole
// discovery: partial visibility beats none (P2).
func (d *Driver) DiscoverLive(ctx context.Context, conn driver.Connection) ([]driver.ContentRef, error) {
	var guilds []partialGuild
	if err := d.get(ctx, conn, "/users/@me/guilds?limit=200", &guilds); err != nil {
		return nil, err
	}
	var refs []driver.ContentRef
	for _, g := range guilds {
		var channels []guildChannel
		if err := d.get(ctx, conn, "/guilds/"+url.PathEscape(g.ID)+"/channels", &channels); err != nil {
			if errors.Is(err, driver.ErrUnauthorized) {
				return nil, err
			}
			d.log().Warn("discord: skipping guild whose channels could not be listed",
				"connection_id", conn.ID, "platform", "discord", "error", err.Error())
			continue
		}
		for _, ch := range channels {
			if ch.Type != channelGuildText && ch.Type != channelGuildAnnouncement {
				continue
			}
			refs = append(refs, driver.ContentRef{
				NativeChannelID: ch.ID,
				Title:           g.Name + "#" + ch.Name,
			})
		}
	}
	return refs, nil
}

// get performs one bot-authenticated GET.
func (d *Driver) get(ctx context.Context, conn driver.Connection, path string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL()+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+conn.AccessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := d.client().Do(req)
	if err != nil {
		return fmt.Errorf("discord: %s: %w", trimQuery(path), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("discord: %s: %w", trimQuery(path), err)
	}
	if resp.StatusCode/100 != 2 {
		// P4: the status classifies; the body never reaches a log.
		return fmt.Errorf("discord: %s: %w", trimQuery(path), driver.FromHTTPStatus(resp.StatusCode))
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("discord: %s: undecodable response", trimQuery(path))
	}
	return nil
}

// trimQuery keeps query strings out of error text.
func trimQuery(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		return path[:i]
	}
	return path
}

// ---------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------

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
	u := fmt.Sprintf("%s/channels/%s/messages/%s", d.baseURL(), url.PathEscape(channel), url.PathEscape(message))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+conn.AccessToken)
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return driver.FromHTTPStatus(resp.StatusCode)
}
