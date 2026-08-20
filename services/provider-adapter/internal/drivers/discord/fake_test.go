package discord

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeDiscord is a local server that speaks the real Discord Gateway
// protocol over a real WebSocket handshake, plus the REST endpoints the
// driver calls. It sends Hello, answers heartbeats, and records every frame
// the client sends so a test can assert on IDENTIFY, RESUME and heartbeat
// timing.
type fakeDiscord struct {
	t *testing.T

	mu sync.Mutex
	// connections counts accepted sockets.
	connections int
	// received records every client frame, in order, with arrival times.
	received []recordedFrame
	// script drives each accepted socket; the last entry repeats.
	script []gatewayScript
	// guilds and channels back DiscoverLive.
	guilds   []fakeGuild
	channels map[string][]fakeChannel
	// gatewayShards is what GET /gateway/bot recommends.
	gatewayShards int
	// dialQueries records the query string of every accepted socket, so a
	// test can prove a resume went to resume_gateway_url.
	dialQueries []string
	// restStatus overrides the reply for a REST path.
	restStatus map[string]int

	server *httptest.Server
	wsURL  string
}

type recordedFrame struct {
	Conn int
	Op   int
	D    json.RawMessage
	At   time.Time
}

type fakeGuild struct {
	ID   string
	Name string
}

type fakeChannel struct {
	ID   string
	Name string
	Type int
}

// gatewayScript is what one accepted socket does.
type gatewayScript struct {
	// heartbeatInterval goes into Hello. 0 means 60 s.
	heartbeatIntervalMS int64
	// suppressACK stops the server answering heartbeats, which is how a
	// zombie connection is simulated.
	suppressACK bool
	// afterIdentify runs once an IDENTIFY or RESUME arrives.
	afterIdentify []gwStep
	// closeCode, when non-zero, closes the socket after the steps.
	closeCode int
}

// gwStep is one scripted server action.
type gwStep struct {
	delay time.Duration
	// ready sends a READY dispatch with this session id.
	readySession string
	// resumed sends a RESUMED dispatch.
	resumed bool
	// message sends a MESSAGE_CREATE.
	message *gwMessage
	// reconnect sends opcode 7.
	reconnect bool
	// invalidSession sends opcode 9 with this resumable flag.
	invalidSession *bool
	// raw sends an arbitrary frame.
	raw string
}

type gwMessage struct {
	id        string
	channelID string
	guildID   string
	authorID  string
	content   string
	bot       bool
	system    bool
	webhookID string
	typ       int
}

func newFakeDiscord(t *testing.T) *fakeDiscord {
	t.Helper()
	f := &fakeDiscord{
		t:             t,
		channels:      make(map[string][]fakeChannel),
		gatewayShards: 1,
		restStatus:    make(map[string]int),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/gw", f.handleWS)
	mux.HandleFunc("/api/gateway/bot", f.handleGatewayBot)
	mux.HandleFunc("/api/users/@me/guilds", f.handleGuilds)
	mux.HandleFunc("/api/guilds/", f.handleGuildChannels)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	f.wsURL = "ws" + strings.TrimPrefix(f.server.URL, "http") + "/gw"
	return f
}

func (f *fakeDiscord) apiURL() string { return f.server.URL + "/api" }

func (f *fakeDiscord) setScript(scripts ...gatewayScript) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = scripts
}

func (f *fakeDiscord) setGuilds(gs ...fakeGuild) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.guilds = gs
}

func (f *fakeDiscord) setChannels(guildID string, cs ...fakeChannel) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channels[guildID] = cs
}

func (f *fakeDiscord) queries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dialQueries...)
}

func (f *fakeDiscord) connectionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections
}

// frames returns every recorded client frame.
func (f *fakeDiscord) frames() []recordedFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedFrame(nil), f.received...)
}

// framesOfOp returns the recorded frames with the given opcode.
func (f *fakeDiscord) framesOfOp(op int) []recordedFrame {
	var out []recordedFrame
	for _, fr := range f.frames() {
		if fr.Op == op {
			out = append(out, fr)
		}
	}
	return out
}

func (f *fakeDiscord) record(conn int, p payload) {
	f.mu.Lock()
	f.received = append(f.received, recordedFrame{Conn: conn, Op: p.Op, D: p.D, At: time.Now()})
	f.mu.Unlock()
}

// ---------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------

func (f *fakeDiscord) checkBotAuth(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bot ") {
		http.Error(w, "bot token required", http.StatusUnauthorized)
		return false
	}
	return true
}

func (f *fakeDiscord) handleGatewayBot(w http.ResponseWriter, r *http.Request) {
	if !f.checkBotAuth(w, r) {
		return
	}
	f.mu.Lock()
	status := f.restStatus["/gateway/bot"]
	shards := f.gatewayShards
	f.mu.Unlock()
	if status != 0 {
		http.Error(w, "boom", status)
		return
	}
	writeJSON(w, map[string]any{
		"url":    f.wsURL,
		"shards": shards,
		"session_start_limit": map[string]any{
			"total": 1000, "remaining": 999, "reset_after": 14400000, "max_concurrency": 1,
		},
	})
}

func (f *fakeDiscord) handleGuilds(w http.ResponseWriter, r *http.Request) {
	if !f.checkBotAuth(w, r) {
		return
	}
	f.mu.Lock()
	status := f.restStatus["/users/@me/guilds"]
	guilds := append([]fakeGuild(nil), f.guilds...)
	f.mu.Unlock()
	if status != 0 {
		http.Error(w, "boom", status)
		return
	}
	out := make([]map[string]any, 0, len(guilds))
	for _, g := range guilds {
		out = append(out, map[string]any{
			"id": g.ID, "name": g.Name, "icon": nil,
			"owner": false, "permissions": "8", "features": []string{},
		})
	}
	writeJSON(w, out)
}

func (f *fakeDiscord) handleGuildChannels(w http.ResponseWriter, r *http.Request) {
	if !f.checkBotAuth(w, r) {
		return
	}
	// /api/guilds/{id}/channels
	rest := strings.TrimPrefix(r.URL.Path, "/api/guilds/")
	guildID := strings.TrimSuffix(rest, "/channels")

	f.mu.Lock()
	status := f.restStatus["/guilds/"+guildID+"/channels"]
	channels := append([]fakeChannel(nil), f.channels[guildID]...)
	f.mu.Unlock()
	if status != 0 {
		http.Error(w, "boom", status)
		return
	}
	out := make([]map[string]any, 0, len(channels))
	for _, c := range channels {
		out = append(out, map[string]any{
			"id": c.ID, "type": c.Type, "guild_id": guildID, "name": c.Name, "position": 0,
		})
	}
	writeJSON(w, out)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------

func (f *fakeDiscord) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		return
	}
	defer c.CloseNow()

	f.mu.Lock()
	f.connections++
	n := f.connections
	f.dialQueries = append(f.dialQueries, r.URL.RawQuery)
	var sc gatewayScript
	if len(f.script) > 0 {
		sc = f.script[min(n-1, len(f.script)-1)]
	}
	f.mu.Unlock()

	// The driver must pass the version and encoding it speaks.
	if r.URL.Query().Get("v") != APIVersion || r.URL.Query().Get("encoding") != "json" {
		_ = c.Close(4012, "invalid API version")
		return
	}

	ctx := r.Context()
	interval := sc.heartbeatIntervalMS
	if interval == 0 {
		interval = 60000
	}
	if !wsSend(ctx, c, map[string]any{"op": opHello, "d": map[string]any{"heartbeat_interval": interval}}) {
		return
	}

	var seq int64
	dispatch := func(t string, d any) bool {
		seq++
		s := seq
		return wsSend(ctx, c, map[string]any{"op": opDispatch, "s": s, "t": t, "d": d})
	}

	started := make(chan struct{})
	var once sync.Once

	// Run the script once the client has identified or resumed.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-started:
		}
		for _, s := range sc.afterIdentify {
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
			case s.readySession != "":
				if !dispatch("READY", map[string]any{
					"v": 10,
					"user": map[string]any{
						"id": "bot-1", "username": "dabet", "bot": true,
					},
					"guilds":             []any{},
					"session_id":         s.readySession,
					"resume_gateway_url": f.wsURL + "?resume=1",
					"application":        map[string]any{"id": "app-1", "flags": 0},
				}) {
					return
				}
			case s.resumed:
				if !dispatch("RESUMED", map[string]any{}) {
					return
				}
			case s.message != nil:
				if !dispatch("MESSAGE_CREATE", messageCreatePayload(*s.message)) {
					return
				}
			case s.reconnect:
				if !wsSend(ctx, c, map[string]any{"op": opReconnect, "d": nil}) {
					return
				}
			case s.invalidSession != nil:
				if !wsSend(ctx, c, map[string]any{"op": opInvalidSession, "d": *s.invalidSession}) {
					return
				}
			}
		}
		if sc.closeCode != 0 {
			_ = c.Close(websocket.StatusCode(sc.closeCode), "scripted")
		}
	}()

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var p payload
		if json.Unmarshal(data, &p) != nil {
			continue
		}
		f.record(n, p)
		switch p.Op {
		case opIdentify, opResume:
			once.Do(func() { close(started) })
		case opHeartbeat:
			if sc.suppressACK {
				continue
			}
			if !wsSend(ctx, c, map[string]any{"op": opHeartbeatACK, "d": nil}) {
				return
			}
		}
	}
}

func messageCreatePayload(m gwMessage) map[string]any {
	return map[string]any{
		"id":         m.id,
		"channel_id": m.channelID,
		"guild_id":   m.guildID,
		"content":    m.content,
		"type":       m.typ,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
		"webhook_id": m.webhookID,
		"author": map[string]any{
			"id": m.authorID, "username": "user-" + m.authorID,
			"bot": m.bot, "system": m.system, "discriminator": "0",
		},
		"attachments": []any{},
		"embeds":      []any{},
		"mentions":    []any{},
	}
}

func wsSend(ctx context.Context, c *websocket.Conn, v any) bool {
	raw, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return c.Write(ctx, websocket.MessageText, raw) == nil
}

func boolPtr(b bool) *bool { return &b }
