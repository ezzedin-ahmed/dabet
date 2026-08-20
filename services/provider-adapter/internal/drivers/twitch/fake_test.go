package twitch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeTwitch is a local server that speaks the real EventSub WebSocket
// protocol over a real WebSocket handshake, plus the Helix endpoints the
// driver calls. Tests script what the socket says; the driver performs the
// full welcome / subscribe / notify / keepalive / reconnect / revocation
// sequence against it exactly as it would against Twitch.
type fakeTwitch struct {
	t *testing.T

	mu sync.Mutex
	// sessions counts accepted WebSocket connections, so a test can tell a
	// reconnect from the original.
	sessions int
	// subscribes records every Create EventSub Subscription body.
	subscribes []subscribeRequest
	// subSessions records the session id each subscription was made for.
	subSessions []string
	// subStatus overrides the reply to the nth (1-based) subscribe call.
	subStatus func(call int) (status int, body string, ok bool)
	// streams is what Get Streams reports.
	streams []fakeStream

	// script drives each accepted socket. Index 0 runs on the first
	// connection, 1 on the second, and so on; the last entry repeats.
	script []sessionScript

	server *httptest.Server
	// wsURL is the ws:// form of the test server's address.
	wsURL string
}

type fakeStream struct {
	UserID string
	Title  string
	Type   string
}

// sessionScript is what one accepted socket does after sending its welcome.
// steps run in order; the socket then idles until the client or the test
// closes it.
type sessionScript struct {
	// keepaliveSeconds goes into the welcome message. 0 means echo the
	// client's requested value.
	keepaliveSeconds int
	// sessionID overrides the generated id.
	sessionID string
	// steps run after the welcome, in order.
	steps []step
	// closeCode, when non-zero, closes the socket after the steps.
	closeCode int
	// closeReason accompanies closeCode.
	closeReason string
}

// step is one scripted server action.
type step struct {
	// delay before the step.
	delay time.Duration
	// notify sends a channel.chat.message notification.
	notify *chatMsg
	// keepalive sends a session_keepalive.
	keepalive bool
	// reconnectTo sends session_reconnect pointing at the fake's own URL.
	reconnect bool
	// revoke sends a revocation with this subscription status.
	revoke string
	// raw sends an arbitrary frame (used for undecodable-frame handling).
	raw string
}

type chatMsg struct {
	broadcaster string
	chatter     string
	id          string
	text        string
	// envelopeID overrides metadata.message_id; empty generates one.
	envelopeID string
}

func newFakeTwitch(t *testing.T) *fakeTwitch {
	t.Helper()
	f := &fakeTwitch{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", f.handleWS)
	mux.HandleFunc("/helix/eventsub/subscriptions", f.handleSubscribe)
	mux.HandleFunc("/helix/streams", f.handleStreams)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	f.wsURL = "ws" + strings.TrimPrefix(f.server.URL, "http") + "/ws"
	return f
}

func (f *fakeTwitch) helixURL() string { return f.server.URL + "/helix" }

func (f *fakeTwitch) setScript(scripts ...sessionScript) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = scripts
}

func (f *fakeTwitch) setStreams(s ...fakeStream) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams = s
}

func (f *fakeTwitch) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions
}

func (f *fakeTwitch) subscriptions() []subscribeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]subscribeRequest(nil), f.subscribes...)
}

func (f *fakeTwitch) subscriptionSessions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.subSessions...)
}

// ---------------------------------------------------------------------
// Helix
// ---------------------------------------------------------------------

func (f *fakeTwitch) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	// WebSocket transport requires a user access token and the Client-Id
	// header; a driver that forgot either would fail against the real API.
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		http.Error(w, "missing bearer", http.StatusUnauthorized)
		return
	}
	if r.Header.Get("Client-Id") == "" {
		http.Error(w, "missing Client-Id", http.StatusBadRequest)
		return
	}
	var body subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.subscribes = append(f.subscribes, body)
	f.subSessions = append(f.subSessions, body.Transport.SessionID)
	call := len(f.subscribes)
	hook := f.subStatus
	f.mu.Unlock()

	if hook != nil {
		if status, payload, ok := hook(call); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(payload))
			return
		}
	}
	if body.Transport.Method != "websocket" || body.Transport.SessionID == "" {
		http.Error(w, "websocket transport needs session_id", http.StatusBadRequest)
		return
	}
	resp := map[string]any{
		"data": []map[string]any{{
			"id":         "sub-" + body.Condition["broadcaster_user_id"],
			"status":     "enabled",
			"type":       body.Type,
			"version":    body.Version,
			"condition":  body.Condition,
			"created_at": time.Now().UTC().Format(time.RFC3339Nano),
			// channel.chat.message is authorised by the chatter's
			// user:read:chat scope, so it costs nothing.
			"cost": 0,
			"transport": map[string]any{
				"method":     "websocket",
				"session_id": body.Transport.SessionID,
			},
		}},
		"total":          1,
		"total_cost":     0,
		"max_total_cost": MaxTotalCost,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

func (f *fakeTwitch) handleStreams(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Client-Id") == "" {
		http.Error(w, "missing Client-Id", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	streams := append([]fakeStream(nil), f.streams...)
	f.mu.Unlock()

	data := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		typ := s.Type
		if typ == "" {
			typ = "live"
		}
		data = append(data, map[string]any{
			"id":         "stream-" + s.UserID,
			"user_id":    s.UserID,
			"user_login": "login-" + s.UserID,
			"user_name":  "Name " + s.UserID,
			"type":       typ,
			"title":      s.Title,
			"started_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "pagination": map[string]any{}})
}

// ---------------------------------------------------------------------
// EventSub WebSocket
// ---------------------------------------------------------------------

func (f *fakeTwitch) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		return
	}
	defer c.CloseNow()

	f.mu.Lock()
	f.sessions++
	n := f.sessions
	var sc sessionScript
	if len(f.script) > 0 {
		sc = f.script[min(n-1, len(f.script)-1)]
	}
	f.mu.Unlock()

	ctx := r.Context()
	sessionID := sc.sessionID
	if sessionID == "" {
		sessionID = fmt.Sprintf("session-%d", n)
	}
	keepalive := sc.keepaliveSeconds
	if keepalive == 0 {
		keepalive, _ = strconvAtoi(r.URL.Query().Get("keepalive_timeout_seconds"))
		if keepalive == 0 {
			keepalive = 10
		}
	}

	// session_welcome, exactly as documented.
	welcome := map[string]any{
		"metadata": map[string]any{
			"message_id":        fmt.Sprintf("welcome-%d", n),
			"message_type":      msgWelcome,
			"message_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		},
		"payload": map[string]any{
			"session": map[string]any{
				"id":                        sessionID,
				"status":                    "connected",
				"connected_at":              time.Now().UTC().Format(time.RFC3339Nano),
				"keepalive_timeout_seconds": keepalive,
				"reconnect_url":             nil,
			},
		},
	}
	if !f.send(ctx, c, welcome) {
		return
	}

	for _, s := range sc.steps {
		if s.delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.delay):
			}
		}
		switch {
		case s.raw != "":
			if c.Write(ctx, websocket.MessageText, []byte(s.raw)) != nil {
				return
			}
		case s.keepalive:
			if !f.send(ctx, c, map[string]any{
				"metadata": map[string]any{
					"message_id":        fmt.Sprintf("ka-%d-%d", n, time.Now().UnixNano()),
					"message_type":      msgKeepalive,
					"message_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
				},
				"payload": map[string]any{},
			}) {
				return
			}
		case s.notify != nil:
			if !f.send(ctx, c, notification(n, sessionID, *s.notify)) {
				return
			}
		case s.reconnect:
			if !f.send(ctx, c, map[string]any{
				"metadata": map[string]any{
					"message_id":        fmt.Sprintf("rc-%d", n),
					"message_type":      msgReconnect,
					"message_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
				},
				"payload": map[string]any{
					"session": map[string]any{
						"id":                        sessionID,
						"status":                    "reconnecting",
						"keepalive_timeout_seconds": nil,
						"reconnect_url":             f.wsURL + "?reconnect=1",
						"connected_at":              time.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			}) {
				return
			}
		case s.revoke != "":
			if !f.send(ctx, c, map[string]any{
				"metadata": map[string]any{
					"message_id":           fmt.Sprintf("rv-%d", n),
					"message_type":         msgRevoke,
					"message_timestamp":    time.Now().UTC().Format(time.RFC3339Nano),
					"subscription_type":    SubscriptionType,
					"subscription_version": SubscriptionVersion,
				},
				"payload": map[string]any{
					"subscription": map[string]any{
						"id":      "sub-1",
						"status":  s.revoke,
						"type":    SubscriptionType,
						"version": SubscriptionVersion,
						"cost":    0,
					},
				},
			}) {
				return
			}
		}
	}

	if sc.closeCode != 0 {
		_ = c.Close(websocket.StatusCode(sc.closeCode), sc.closeReason)
		return
	}
	// Idle until the client goes away.
	for {
		if _, _, err := c.Read(ctx); err != nil {
			return
		}
	}
}

func notification(session int, sessionID string, m chatMsg) map[string]any {
	envID := m.envelopeID
	if envID == "" {
		envID = fmt.Sprintf("env-%d-%s", session, m.id)
	}
	return map[string]any{
		"metadata": map[string]any{
			"message_id":           envID,
			"message_type":         msgNotify,
			"message_timestamp":    time.Now().UTC().Format(time.RFC3339Nano),
			"subscription_type":    SubscriptionType,
			"subscription_version": SubscriptionVersion,
		},
		"payload": map[string]any{
			"subscription": map[string]any{
				"id":      "sub-1",
				"status":  "enabled",
				"type":    SubscriptionType,
				"version": SubscriptionVersion,
				"cost":    0,
				"condition": map[string]any{
					"broadcaster_user_id": m.broadcaster,
					"user_id":             m.chatter,
				},
				"transport": map[string]any{"method": "websocket", "session_id": sessionID},
			},
			"event": map[string]any{
				"broadcaster_user_id":    m.broadcaster,
				"broadcaster_user_login": "login-" + m.broadcaster,
				"broadcaster_user_name":  "Name " + m.broadcaster,
				"chatter_user_id":        m.chatter,
				"chatter_user_login":     "login-" + m.chatter,
				"chatter_user_name":      "Name " + m.chatter,
				"message_id":             m.id,
				"message":                map[string]any{"text": m.text, "fragments": []any{}},
				"message_type":           "text",
				"badges":                 []any{},
				"color":                  "#FF0000",
			},
		},
	}
}

func (f *fakeTwitch) send(ctx context.Context, c *websocket.Conn, v any) bool {
	raw, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return c.Write(ctx, websocket.MessageText, raw) == nil
}

func strconvAtoi(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
