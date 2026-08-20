// Package twitch is the Twitch driver (docs §7.2 table, A14).
//
// # Verified against provider documentation (2026-08-19)
//
//   - EventSub WebSocket — https://dev.twitch.tv/docs/eventsub/handling-websocket-events/
//     Connect to wss://eventsub.wss.twitch.tv/ws, optionally with
//     ?keepalive_timeout_seconds=N (10-600, default 10). The server sends
//     session_welcome immediately, carrying payload.session.id and the
//     negotiated keepalive_timeout_seconds. A subscription must be created
//     within 10 seconds of the welcome or the socket is closed with 4003.
//     Subsequent messages are session_keepalive, notification,
//     session_reconnect and revocation, each with a metadata block
//     (message_id, message_type, message_timestamp, and for notification
//     and revocation also subscription_type / subscription_version).
//   - session_reconnect carries payload.session.reconnect_url and is sent
//     30 seconds before the forced disconnect. The documented procedure is:
//     connect to the new URL, wait for its welcome, and only then close the
//     old socket — the old one keeps delivering events until that point.
//     Subscriptions carry over; they are not recreated.
//   - Close codes: 4000 internal error, 4001 client sent inbound traffic,
//     4002 failed ping-pong, 4003 connection unused (no subscription in
//     10 s), 4004 reconnect grace expired, 4005 network timeout, 4006
//     network error, 4007 invalid reconnect URL.
//   - Create EventSub Subscription — https://dev.twitch.tv/docs/api/reference/
//     POST https://api.twitch.tv/helix/eventsub/subscriptions with
//     Authorization: Bearer <user token>, Client-Id and Content-Type
//     headers, and a body of {type, version, condition, transport:{method:
//     "websocket", session_id}}. WebSocket transport *requires* a user
//     access token; an app token is rejected. Responses: 202 accepted, 400
//     bad request, 401 unauthorized, 403 missing scopes, 409 a subscription
//     already exists for that type+condition, 410 type version removed, 429
//     too many subscriptions of that type+condition.
//   - channel.chat.message version 1 —
//     https://dev.twitch.tv/docs/eventsub/eventsub-subscription-types/
//     condition {broadcaster_user_id, user_id}; requires the user:read:chat
//     scope from the chatting user. The event carries broadcaster_user_id,
//     chatter_user_id, message_id, message.text, message.fragments,
//     message_type and the shared-chat source_* fields.
//   - Get Streams — GET https://api.twitch.tv/helix/streams?user_id=...
//     returns data[] with id, user_id, user_login, type and title; no scope
//     required, Client-Id header mandatory.
//
// # Subscription cost limits (§7.2's "key constraint")
//
// From https://dev.twitch.tv/docs/eventsub/manage-subscriptions/: with
// WebSocket transport a client may hold at most 3 enabled connections per
// (client id, user), each carrying at most 300 enabled subscriptions, with
// a max_total_cost of 10 across all of them. Cost is 0 for subscription
// types the target user has authorised your app for, and 1 otherwise —
// channel.chat.message always requires the chatter's user:read:chat scope
// and so costs 0. The binding limit for this driver is therefore the
// 300-subscriptions-per-socket ceiling, not the cost budget; the driver
// still tracks the cost the API reports and refuses to exceed either.
//
// # Where the live docs differ from §7.2
//
// §7.2 lists "EventSub WebSocket / IRC" for transport and EventSub
// stream.online for liveness. EventSub is implemented (IRC is legacy and
// gives no delete path). Liveness uses Get Streams rather than a
// stream.online subscription: chat exists on Twitch whether or not the
// channel is live, so the chat subscription is held continuously and
// DiscoverLive reports the live stream — a stream.online subscription
// would spend one of the ten cost units to learn something a single
// unscoped Helix call already answers.
//
// Delete is implemented against the documented Helix endpoint (verified
// 2026-08-19): DELETE https://api.twitch.tv/helix/moderation/chat
//
//	?broadcaster_id={..}&moderator_id={..}&message_id={..}
//
// requiring the moderator:manage:chat_messages scope, with the caveat that
// the message must have been created within the last 6 hours and must not
// belong to the broadcaster or another moderator.
package twitch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"dabet/services/provider-adapter/internal/dedup"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/retry"
	"dabet/services/provider-adapter/internal/wsx"
)

// DefaultBaseURL is the production Helix root.
const DefaultBaseURL = "https://api.twitch.tv/helix"

// DefaultEventSubURL is the production EventSub WebSocket endpoint.
const DefaultEventSubURL = "wss://eventsub.wss.twitch.tv/ws"

// SubscriptionType and SubscriptionVersion identify the chat subscription.
const (
	SubscriptionType    = "channel.chat.message"
	SubscriptionVersion = "1"
)

// Documented per-socket limits (see the package comment).
const (
	// MaxSubscriptionsPerSocket is Twitch's ceiling on enabled
	// subscriptions for one WebSocket connection.
	MaxSubscriptionsPerSocket = 300
	// MaxTotalCost is the max_total_cost for WebSocket transport.
	MaxTotalCost = 10
)

// Keepalive bounds. Twitch permits 10-600 s; 30 s trades a little
// detection latency for far fewer keepalive frames than the default 10 s.
const (
	DefaultKeepaliveTimeout = 30 * time.Second
	MinKeepaliveTimeout     = 10 * time.Second
	MaxKeepaliveTimeout     = 600 * time.Second
	// keepaliveGrace is added to the negotiated timeout before the driver
	// declares the socket dead, so ordinary jitter is not a reconnect.
	keepaliveGrace = 10 * time.Second
	// welcomeTimeout bounds the wait for session_welcome after dialling.
	welcomeTimeout = 15 * time.Second
	// subscribeDeadline is Twitch's 10 s window from welcome to first
	// subscription; the driver aims well inside it.
	subscribeDeadline = 8 * time.Second
)

// Driver implements driver.Driver for Twitch.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// EventSubURL overrides DefaultEventSubURL.
	EventSubURL string
	// Dialer opens the EventSub socket; nil means a production dialer.
	Dialer wsx.Dialer
	// Resolver maps opaque content/message ids back to native ids.
	Resolver driver.Resolver
	// ClientID is the Twitch application client id, required on every
	// Helix call.
	ClientID string
	// KeepaliveTimeout is the keepalive_timeout_seconds requested at dial.
	KeepaliveTimeout time.Duration
	// KeepaliveGrace is added to the timeout the server confirms before the
	// socket is declared dead; 0 means keepaliveGrace.
	KeepaliveGrace time.Duration
	// DedupWindow is how many recent notification ids are remembered.
	DedupWindow int
	// Backoff is the reconnect policy for transient failures.
	Backoff retry.Backoff
	// Broadcasters resolves which channels' chat this connection watches.
	// nil means the connected account's own channel, which is the §5.5
	// case: a creator moderating their own chat.
	Broadcasters func(ctx context.Context, conn driver.Connection) ([]string, error)
	// Log receives operational events. P4: never message text.
	Log *slog.Logger
}

// New returns a Twitch driver.
func New(resolver driver.Resolver, clientID string) *Driver {
	return &Driver{
		HTTPClient:       http.DefaultClient,
		BaseURL:          DefaultBaseURL,
		EventSubURL:      DefaultEventSubURL,
		Dialer:           wsx.NewDialer(),
		Resolver:         resolver,
		ClientID:         clientID,
		KeepaliveTimeout: DefaultKeepaliveTimeout,
		DedupWindow:      dedup.DefaultCapacity,
		Backoff:          retry.DefaultBackoff(),
		Log:              slog.Default(),
	}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "twitch" }

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

func (d *Driver) grace() time.Duration {
	if d.KeepaliveGrace <= 0 {
		return keepaliveGrace
	}
	return d.KeepaliveGrace
}

func (d *Driver) keepalive() time.Duration {
	k := d.KeepaliveTimeout
	if k <= 0 {
		k = DefaultKeepaliveTimeout
	}
	return min(max(k, MinKeepaliveTimeout), MaxKeepaliveTimeout)
}

// ---------------------------------------------------------------------
// EventSub wire types
// ---------------------------------------------------------------------

// EventSub message types (metadata.message_type).
const (
	msgWelcome   = "session_welcome"
	msgKeepalive = "session_keepalive"
	msgNotify    = "notification"
	msgReconnect = "session_reconnect"
	msgRevoke    = "revocation"
)

type envelope struct {
	Metadata struct {
		MessageID   string `json:"message_id"`
		MessageType string `json:"message_type"`
		Timestamp   string `json:"message_timestamp"`
		SubType     string `json:"subscription_type"`
		SubVersion  string `json:"subscription_version"`
	} `json:"metadata"`
	Payload struct {
		Session struct {
			ID                      string `json:"id"`
			Status                  string `json:"status"`
			KeepaliveTimeoutSeconds *int   `json:"keepalive_timeout_seconds"`
			ReconnectURL            string `json:"reconnect_url"`
			ConnectedAt             string `json:"connected_at"`
		} `json:"session"`
		Subscription struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Type   string `json:"type"`
			Cost   int    `json:"cost"`
		} `json:"subscription"`
		Event chatMessageEvent `json:"event"`
	} `json:"payload"`
}

// chatMessageEvent is the channel.chat.message payload, narrowed to what
// the adapter needs. P5: everything here stays inside the driver.
type chatMessageEvent struct {
	BroadcasterUserID string `json:"broadcaster_user_id"`
	ChatterUserID     string `json:"chatter_user_id"`
	MessageID         string `json:"message_id"`
	MessageType       string `json:"message_type"`
	Message           struct {
		Text string `json:"text"`
	} `json:"message"`
	// Shared chat: when a message originates in another channel's chat the
	// source_* fields identify it. The adapter attributes the message to
	// the channel it was delivered in, which is the one we can moderate.
	SourceBroadcasterUserID string `json:"source_broadcaster_user_id"`
}

// ---------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------

// Watch implements driver.Driver.
//
// One EventSub session per connection: dial, welcome, subscribe to
// channel.chat.message for every broadcaster this connection moderates,
// then relay notifications until something ends the session. Transient
// endings reconnect with backoff and jitter (P2); an auth failure returns
// wrapping driver.ErrUnauthorized so the ingest manager runs the §5.6
// refresh; a revoked authorization or a removed channel is terminal.
//
// The dedup window lives outside the session loop on purpose: its whole job
// is to suppress the redelivery that a reconnect causes, so it must survive
// the reconnect.
func (d *Driver) Watch(ctx context.Context, conn driver.Connection, out chan<- driver.Message) error {
	seen := dedup.New(d.DedupWindow)
	backoff := d.Backoff
	for {
		started := time.Now()
		err := d.session(ctx, conn, out, seen)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, driver.ErrUnauthorized), driver.Terminal(err):
			return err
		case err != nil:
			d.log().Warn("twitch eventsub session ended; reconnecting",
				"connection_id", conn.ID, "platform", "twitch", "error", err.Error())
		}
		// A session that ran for a while and then dropped is a fresh
		// incident, not a continuing outage — but a session that dies
		// immediately, over and over, must not become a reconnect loop, so
		// the backoff always applies.
		if time.Since(started) >= healthySession {
			backoff.Reset()
		}
		if werr := backoff.Wait(ctx); werr != nil {
			return nil
		}
	}
}

// healthySession is how long a session must last before its failure is
// treated as a new incident rather than a continuing one.
const healthySession = 60 * time.Second

// session holds one EventSub socket from dial to termination. It returns
// nil for an ordinary, retryable end of session.
func (d *Driver) session(ctx context.Context, conn driver.Connection, out chan<- driver.Message, seen *dedup.Set) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	broadcasters, err := d.broadcasters(ctx, conn)
	if err != nil {
		return err
	}
	if len(broadcasters) == 0 {
		return fmt.Errorf("twitch: connection has no broadcaster to watch: %w", driver.ErrPermanent)
	}
	if len(broadcasters) > MaxSubscriptionsPerSocket {
		// One socket cannot carry them all; refusing loudly beats silently
		// dropping channels (see the package comment on limits).
		return fmt.Errorf("twitch: %d channels exceeds the %d subscriptions one EventSub socket allows: %w",
			len(broadcasters), MaxSubscriptionsPerSocket, driver.ErrPermanent)
	}

	sock, sess, err := d.connect(ctx, d.eventSubURL())
	if err != nil {
		return err
	}
	defer func() { _ = sock.conn.Close(wsx.StatusNormalClosure, "") }()

	// Subscriptions must exist within 10 s of the welcome or Twitch closes
	// the socket with 4003.
	subCtx, subCancel := context.WithTimeout(ctx, subscribeDeadline)
	err = d.subscribeAll(subCtx, conn, sess.id, broadcasters)
	subCancel()
	if err != nil {
		return err
	}

	// pending holds a replacement socket being brought up in response to
	// session_reconnect; the old socket keeps delivering until it is ready.
	type handshake struct {
		sock *socket
		sess session
		err  error
	}
	var pending chan handshake

	timeout := sess.keepalive + d.grace()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(timeout)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			// No keepalive and no event inside the negotiated window: the
			// socket is dead even though TCP has not noticed.
			return fmt.Errorf("twitch: no keepalive within %s", timeout)

		case hs := <-pending:
			pending = nil
			if hs.err != nil {
				// Keep the old socket; Twitch will force-close it in 30 s
				// and the outer loop reconnects from scratch.
				d.log().Warn("twitch reconnect handshake failed; keeping current session",
					"connection_id", conn.ID, "platform", "twitch", "error", hs.err.Error())
				continue
			}
			// Subscriptions carry over to the new session; do not resubscribe.
			_ = sock.conn.Close(wsx.StatusNormalClosure, "replaced")
			sock = hs.sock
			sess = hs.sess
			timeout = sess.keepalive + d.grace()
			resetTimer()
			d.log().Info("twitch eventsub session migrated",
				"connection_id", conn.ID, "platform", "twitch")

		case f, ok := <-sock.frames:
			if !ok {
				return nil // pump ended with ctx cancellation
			}
			if f.Err != nil {
				return classifyClose(f.Err)
			}
			resetTimer()

			var env envelope
			if err := json.Unmarshal(f.Data, &env); err != nil {
				// A frame we cannot parse is not a reason to drop a live
				// chat stream (P2); skip it.
				d.log().Warn("twitch: undecodable eventsub frame",
					"connection_id", conn.ID, "platform", "twitch")
				continue
			}

			switch env.Metadata.MessageType {
			case msgKeepalive, msgWelcome:
				// Nothing to do; the timer reset above is the point.

			case msgNotify:
				if env.Metadata.SubType != SubscriptionType {
					continue
				}
				ev := env.Payload.Event
				// Deduplicate on the platform's own chat message id rather
				// than metadata.message_id: a redelivery across the
				// reconnect overlap arrives as a *different* envelope
				// carrying the *same* chat message.
				if !seen.Add(ev.MessageID) {
					continue
				}
				if ev.Message.Text == "" {
					continue
				}
				if err := driver.Send(ctx, out, driver.Message{
					NativeChannelID: ev.BroadcasterUserID,
					NativeAuthorID:  ev.ChatterUserID,
					NativeMessageID: ev.MessageID,
					Text:            ev.Message.Text,
					// f.Err == nil, so this frame was read just now: the
					// §4.6 clock starts here, at adapter ingress.
					ReceivedAt: time.Now(),
				}); err != nil {
					return nil
				}

			case msgReconnect:
				if pending != nil || env.Payload.Session.ReconnectURL == "" {
					continue
				}
				// Documented procedure: bring the new socket up and only
				// then close the old one, which keeps delivering meanwhile.
				// Any overlap is absorbed by the dedup window.
				pending = make(chan handshake, 1)
				reconnectURL := env.Payload.Session.ReconnectURL
				ch := pending
				go func() {
					s, ses, err := d.connect(ctx, reconnectURL)
					ch <- handshake{sock: s, sess: ses, err: err}
				}()

			case msgRevoke:
				return revocationError(env.Payload.Subscription.Status)
			}
		}
	}
}

// socket bundles a connection with its frame pump.
type socket struct {
	conn   wsx.Conn
	frames <-chan wsx.Frame
}

// session is what the welcome message told us.
type session struct {
	id        string
	keepalive time.Duration
}

// connect dials an EventSub URL and consumes the session_welcome.
func (d *Driver) connect(ctx context.Context, rawURL string) (*socket, session, error) {
	c, err := d.dialer().Dial(ctx, rawURL, nil)
	if err != nil {
		return nil, session{}, fmt.Errorf("twitch: eventsub dial: %w", err)
	}
	frames := wsx.Pump(ctx, c)

	t := time.NewTimer(welcomeTimeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		_ = c.Close(wsx.StatusNormalClosure, "")
		return nil, session{}, ctx.Err()
	case <-t.C:
		_ = c.Close(wsx.StatusNormalClosure, "")
		return nil, session{}, errors.New("twitch: no session_welcome within timeout")
	case f, ok := <-frames:
		if !ok || f.Err != nil {
			_ = c.Close(wsx.StatusNormalClosure, "")
			if ok && f.Err != nil {
				return nil, session{}, classifyClose(f.Err)
			}
			return nil, session{}, errors.New("twitch: socket closed before session_welcome")
		}
		var env envelope
		if err := json.Unmarshal(f.Data, &env); err != nil || env.Metadata.MessageType != msgWelcome {
			_ = c.Close(wsx.StatusNormalClosure, "")
			return nil, session{}, errors.New("twitch: first frame was not session_welcome")
		}
		ka := d.keepalive()
		if s := env.Payload.Session.KeepaliveTimeoutSeconds; s != nil && *s > 0 {
			ka = time.Duration(*s) * time.Second
		}
		return &socket{conn: c, frames: frames}, session{id: env.Payload.Session.ID, keepalive: ka}, nil
	}
}

// eventSubURL returns the dial URL with the requested keepalive timeout.
func (d *Driver) eventSubURL() string {
	base := d.EventSubURL
	if base == "" {
		base = DefaultEventSubURL
	}
	sep := "?"
	if u, err := url.Parse(base); err == nil && u.RawQuery != "" {
		sep = "&"
	}
	return fmt.Sprintf("%s%skeepalive_timeout_seconds=%d", base, sep, int(d.keepalive().Seconds()))
}

// classifyClose maps a socket read error onto a driver error class using
// the WebSocket close code. Every documented Twitch code is transient: the
// permanent failures arrive as revocation messages or Helix responses, not
// as close frames.
func classifyClose(err error) error {
	code := wsx.CloseStatus(err)
	if code == wsx.StatusNoStatus {
		return err // transport error; the caller retries with backoff
	}
	switch code {
	case wsx.StatusNormalClosure:
		return nil
	default:
		return fmt.Errorf("twitch: eventsub socket closed with code %d", code)
	}
}

// revocationError maps a revocation's subscription status onto a driver
// error class. authorization_revoked is the §5.6 signal: the creator (or
// Twitch) withdrew the grant, so the refresh path runs and, when it fails,
// the connection moves to expired and its streams are dropped.
func revocationError(status string) error {
	switch status {
	case "authorization_revoked":
		return fmt.Errorf("twitch: subscription revoked (%s): %w", status, driver.ErrUnauthorized)
	case "user_removed":
		return fmt.Errorf("twitch: subscription revoked (%s): %w", status, driver.ErrGone)
	default:
		// version_removed and anything new: retrying the same subscription
		// version cannot help.
		return fmt.Errorf("twitch: subscription revoked (%s): %w", status, driver.ErrPermanent)
	}
}

// ---------------------------------------------------------------------
// Helix: subscriptions and discovery
// ---------------------------------------------------------------------

type subscribeRequest struct {
	Type      string            `json:"type"`
	Version   string            `json:"version"`
	Condition map[string]string `json:"condition"`
	Transport struct {
		Method    string `json:"method"`
		SessionID string `json:"session_id"`
	} `json:"transport"`
}

type subscribeResponse struct {
	Data []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Cost   int    `json:"cost"`
	} `json:"data"`
	TotalCost    int `json:"total_cost"`
	MaxTotalCost int `json:"max_total_cost"`
}

// subscribeAll creates one channel.chat.message subscription per
// broadcaster on the given session, refusing to exceed the documented
// per-socket cost ceiling.
func (d *Driver) subscribeAll(ctx context.Context, conn driver.Connection, sessionID string, broadcasters []string) error {
	spent := 0
	for _, b := range broadcasters {
		cost, err := d.subscribe(ctx, conn, sessionID, b)
		if err != nil {
			return err
		}
		spent += cost
		if spent > MaxTotalCost {
			return fmt.Errorf("twitch: subscriptions cost %d, above the %d max_total_cost for websocket transport: %w",
				spent, MaxTotalCost, driver.ErrPermanent)
		}
	}
	return nil
}

func (d *Driver) subscribe(ctx context.Context, conn driver.Connection, sessionID, broadcasterID string) (int, error) {
	body := subscribeRequest{
		Type:    SubscriptionType,
		Version: SubscriptionVersion,
		Condition: map[string]string{
			"broadcaster_user_id": broadcasterID,
			// user_id is the authenticated reader — the account whose
			// user:read:chat scope authorises the subscription (§5.5).
			"user_id": conn.NativeUserID,
		},
	}
	body.Transport.Method = "websocket"
	body.Transport.SessionID = sessionID

	raw, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		d.baseURL()+"/eventsub/subscriptions", bytes.NewReader(raw))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	req.Header.Set("Client-Id", d.ClientID)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client().Do(req)
	if err != nil {
		return 0, fmt.Errorf("twitch: create subscription: %w", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch resp.StatusCode {
	case http.StatusAccepted, http.StatusOK:
		var out subscribeResponse
		_ = json.Unmarshal(payload, &out)
		cost := 0
		for _, s := range out.Data {
			cost += s.Cost
		}
		return cost, nil
	case http.StatusConflict:
		// A subscription for this type+condition already exists — on a
		// fresh session id that means a stale one Twitch has not reaped.
		// Nothing to create and nothing to fail over.
		return 0, nil
	case http.StatusUnauthorized:
		return 0, fmt.Errorf("twitch: create subscription: %w", driver.ErrUnauthorized)
	case http.StatusForbidden:
		// Missing scope: reconnecting the account (§5.5) is the only fix.
		return 0, fmt.Errorf("twitch: create subscription: missing scope: %w", driver.ErrPermanent)
	case http.StatusGone:
		return 0, fmt.Errorf("twitch: subscription type %s v%s was removed: %w",
			SubscriptionType, SubscriptionVersion, driver.ErrPermanent)
	case http.StatusTooManyRequests:
		return 0, fmt.Errorf("twitch: create subscription: %w", driver.ErrRateLimited)
	case http.StatusBadRequest:
		return 0, fmt.Errorf("twitch: create subscription rejected as invalid (status 400): %w", driver.ErrPermanent)
	default:
		return 0, fmt.Errorf("twitch: create subscription: provider returned status %d", resp.StatusCode)
	}
}

// broadcasters resolves the channels whose chat this connection watches.
func (d *Driver) broadcasters(ctx context.Context, conn driver.Connection) ([]string, error) {
	if d.Broadcasters != nil {
		return d.Broadcasters(ctx, conn)
	}
	if conn.NativeUserID == "" {
		return nil, fmt.Errorf("twitch: connection has no native user id: %w", driver.ErrPermanent)
	}
	return []string{conn.NativeUserID}, nil
}

type streamsResponse struct {
	Data []struct {
		ID        string `json:"id"`
		UserID    string `json:"user_id"`
		UserLogin string `json:"user_login"`
		Type      string `json:"type"`
		Title     string `json:"title"`
	} `json:"data"`
}

// DiscoverLive implements driver.Driver via Get Streams.
//
// The ContentRef's NativeChannelID is the broadcaster's user id, because
// that is what Helix delete-chat-message needs as broadcaster_id — minting
// content_id from it is what lets a deletion route without a lookup (§7.2).
func (d *Driver) DiscoverLive(ctx context.Context, conn driver.Connection) ([]driver.ContentRef, error) {
	ids, err := d.broadcasters(ctx, conn)
	if err != nil {
		return nil, err
	}
	q := url.Values{"first": {"100"}}
	for _, id := range ids {
		q.Add("user_id", id)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL()+"/streams?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	req.Header.Set("Client-Id", d.ClientID)

	resp, err := d.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("twitch: get streams: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("twitch: get streams: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("twitch: get streams: %w", driver.FromHTTPStatus(resp.StatusCode))
	}
	var body streamsResponse
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, errors.New("twitch: get streams: undecodable response")
	}
	refs := make([]driver.ContentRef, 0, len(body.Data))
	for _, s := range body.Data {
		if s.Type != "live" {
			continue
		}
		refs = append(refs, driver.ContentRef{NativeChannelID: s.UserID, Title: s.Title})
	}
	return refs, nil
}

// ---------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------

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
	q := url.Values{
		"broadcaster_id": {broadcaster},
		"moderator_id":   {conn.NativeUserID},
		"message_id":     {message},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, d.baseURL()+"/moderation/chat?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	req.Header.Set("Client-Id", d.ClientID)
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return driver.FromHTTPStatus(resp.StatusCode)
}
