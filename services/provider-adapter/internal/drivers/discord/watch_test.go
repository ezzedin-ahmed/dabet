package discord

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/opaque"
	"dabet/services/provider-adapter/internal/retry"
	"dabet/services/provider-adapter/internal/wsx"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func watchDriver(t *testing.T, f *fakeDiscord) *Driver {
	t.Helper()
	d := New(opaque.NewMinter())
	d.BaseURL = f.apiURL()
	d.HTTPClient = f.server.Client()
	d.Dialer = wsx.NewDialer()
	d.Backoff = retry.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond, Jitter: func() float64 { return 0.5 }}
	// Pinning the heartbeat jitter keeps the timing assertions honest: the
	// first beat lands at exactly half the interval.
	d.Jitter = func() float64 { return 0.5 }
	d.Log = quietLogger()
	return d
}

func testConn() driver.Connection {
	return driver.Connection{
		ID:          "conn-1",
		CreatorID:   "creator-1",
		Platform:    "discord",
		AccessToken: "bot-token",
	}
}

func runWatch(t *testing.T, d *Driver) (chan driver.Message, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan driver.Message, 64)
	errCh := make(chan error, 1)
	go func() { errCh <- d.Watch(ctx, testConn(), out) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("Watch did not return after cancellation")
		}
	})
	return out, errCh
}

func recv(t *testing.T, out chan driver.Message, within time.Duration) driver.Message {
	t.Helper()
	select {
	case m := <-out:
		return m
	case <-time.After(within):
		t.Fatalf("no message within %s", within)
		return driver.Message{}
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWatchIdentifiesWithTheRightIntentsAndRelaysMessages(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(gatewayScript{
		heartbeatIntervalMS: 60000,
		afterIdentify: []gwStep{
			{readySession: "sess-abc"},
			{message: &gwMessage{
				id: "msg-1", channelID: "chan-1", guildID: "guild-1",
				authorID: "user-9", content: "hello guild", typ: messageTypeDefault,
			}},
			// Everything the driver must ignore, on the same event.
			{message: &gwMessage{id: "bot-1", channelID: "chan-1", authorID: "b", content: "beep", bot: true}},
			{message: &gwMessage{id: "hook-1", channelID: "chan-1", authorID: "w", content: "posted", webhookID: "wh-1"}},
			{message: &gwMessage{
				id: "msg-2", channelID: "chan-1", guildID: "guild-1",
				authorID: "user-3", content: "second", typ: messageTypeReply,
			}},
		},
	})
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	first := recv(t, out, 3*time.Second)
	if first.NativeMessageID != "msg-1" || first.Text != "hello guild" {
		t.Errorf("message = %+v", first)
	}
	// content_id is minted from the channel snowflake, which is exactly
	// what DELETE /channels/{id}/messages/{id} needs.
	if first.NativeChannelID != "chan-1" || first.NativeAuthorID != "user-9" {
		t.Errorf("ids = %+v", first)
	}
	if first.ReceivedAt.IsZero() {
		t.Error("ReceivedAt unset; the §4.6 clock never started")
	}

	// The bot and webhook messages must have been skipped, so the next
	// message through is msg-2.
	second := recv(t, out, 3*time.Second)
	if second.NativeMessageID != "msg-2" {
		t.Errorf("second message = %+v, want the bot and webhook posts skipped", second)
	}

	ids := f.framesOfOp(opIdentify)
	if len(ids) != 1 {
		t.Fatalf("identify frames = %d, want 1", len(ids))
	}
	var idData struct {
		Token      string `json:"token"`
		Intents    int    `json:"intents"`
		Properties struct {
			OS      string `json:"os"`
			Browser string `json:"browser"`
			Device  string `json:"device"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(ids[0].D, &idData); err != nil {
		t.Fatalf("identify payload: %v", err)
	}
	if idData.Token != "bot-token" {
		t.Errorf("identify token = %q", idData.Token)
	}
	if idData.Intents != Intents {
		t.Errorf("intents = %d, want GUILD_MESSAGES|MESSAGE_CONTENT (%d)", idData.Intents, Intents)
	}
	if idData.Properties.OS == "" || idData.Properties.Browser == "" || idData.Properties.Device == "" {
		t.Errorf("properties = %+v, all three are required", idData.Properties)
	}
}

func TestHeartbeatsFireOnTheHelloInterval(t *testing.T) {
	const intervalMS = 120
	f := newFakeDiscord(t)
	f.setScript(gatewayScript{
		heartbeatIntervalMS: intervalMS,
		afterIdentify:       []gwStep{{readySession: "sess-hb"}},
	})
	d := watchDriver(t, f)
	start := time.Now()
	runWatch(t, d)

	// Four beats at 120 ms, the first at half that thanks to the pinned
	// jitter: about 420 ms of wall clock.
	waitFor(t, 3*time.Second, "four heartbeats", func() bool {
		return len(f.framesOfOp(opHeartbeat)) >= 4
	})
	beats := f.framesOfOp(opHeartbeat)

	// The documented rule: wait heartbeat_interval * jitter, then beat
	// every heartbeat_interval. With jitter pinned at 0.5 the first beat
	// must land near half the interval, well before a full one.
	firstGap := beats[0].At.Sub(start)
	if firstGap > intervalMS*time.Millisecond {
		t.Errorf("first heartbeat after %s, want ~half of %dms (jitter)", firstGap, intervalMS)
	}
	// Subsequent gaps must track the interval, not run free.
	for i := 1; i < 4; i++ {
		gap := beats[i].At.Sub(beats[i-1].At)
		if gap < intervalMS/2*time.Millisecond || gap > 3*intervalMS*time.Millisecond {
			t.Errorf("heartbeat gap %d = %s, want about %dms", i, gap, intervalMS)
		}
	}

	// A heartbeat carries the last sequence number seen; READY was
	// sequence 1, so the beats after it must not be null.
	var seq *int64
	for _, b := range beats {
		var v *int64
		if json.Unmarshal(b.D, &v) == nil && v != nil {
			seq = v
			break
		}
	}
	if seq == nil {
		t.Error("no heartbeat carried a sequence number after READY")
	}
}

func TestZombieConnectionReconnectsWhenAcksStop(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(
		// A server that never ACKs: the driver must notice after one
		// missed beat and reconnect rather than sit on a dead socket.
		gatewayScript{
			heartbeatIntervalMS: 80,
			suppressACK:         true,
			afterIdentify:       []gwStep{{readySession: "sess-zombie"}},
		},
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{resumed: true},
				{message: &gwMessage{id: "after-zombie", channelID: "c", authorID: "u", content: "alive"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	msg := recv(t, out, 5*time.Second)
	if msg.NativeMessageID != "after-zombie" {
		t.Errorf("message = %+v", msg)
	}
	if f.connectionCount() < 2 {
		t.Errorf("connections = %d, want the zombie socket replaced", f.connectionCount())
	}
}

func TestReconnectResumesWithSessionAndSequence(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{readySession: "sess-resume"},
				{message: &gwMessage{id: "m1", channelID: "c1", authorID: "u1", content: "before"}},
				// Opcode 7: close and come back on resume_gateway_url.
				{reconnect: true, delay: 30 * time.Millisecond},
			},
		},
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{resumed: true},
				{message: &gwMessage{id: "m2", channelID: "c1", authorID: "u2", content: "after"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	if got := recv(t, out, 3*time.Second); got.NativeMessageID != "m1" {
		t.Fatalf("first = %+v", got)
	}
	if got := recv(t, out, 5*time.Second); got.NativeMessageID != "m2" {
		t.Fatalf("second = %+v, want the resumed session to keep delivering", got)
	}

	resumes := f.framesOfOp(opResume)
	if len(resumes) != 1 {
		t.Fatalf("resume frames = %d, want exactly 1: %+v", len(resumes), resumes)
	}
	var rd struct {
		Token     string `json:"token"`
		SessionID string `json:"session_id"`
		Seq       int64  `json:"seq"`
	}
	if err := json.Unmarshal(resumes[0].D, &rd); err != nil {
		t.Fatalf("resume payload: %v", err)
	}
	if rd.SessionID != "sess-resume" {
		t.Errorf("resume session_id = %q, want the one READY handed out", rd.SessionID)
	}
	if rd.Token != "bot-token" {
		t.Errorf("resume token = %q", rd.Token)
	}
	// READY was s=1 and the message s=2, so the resume must ask to carry on
	// from 2 — resuming from 0 or 1 would replay or lose messages.
	if rd.Seq != 2 {
		t.Errorf("resume seq = %d, want 2 (the last dispatch seen)", rd.Seq)
	}
	// A resume must not be followed by an identify on the same socket.
	if ids := f.framesOfOp(opIdentify); len(ids) != 1 {
		t.Errorf("identify frames = %d, want 1: a RESUME replaces IDENTIFY", len(ids))
	}
	// The resume must go to resume_gateway_url, not the original gateway.
	qs := f.queries()
	if len(qs) < 2 || !strings.Contains(qs[1], "resume=1") {
		t.Errorf("second dial query = %v, want the resume_gateway_url from READY", qs)
	}
	// ...still carrying the version and encoding this driver speaks.
	if len(qs) >= 2 && (!strings.Contains(qs[1], "v="+APIVersion) || !strings.Contains(qs[1], "encoding=json")) {
		t.Errorf("resume dial lost v/encoding: %q", qs[1])
	}
}

func TestInvalidSessionFalseForcesAFreshIdentify(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{readySession: "sess-doomed"},
				// d=false: the session is unresumable, drop it.
				{invalidSession: boolPtr(false), delay: 20 * time.Millisecond},
			},
		},
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{readySession: "sess-new"},
				{message: &gwMessage{id: "reidentified", channelID: "c", authorID: "u", content: "fresh"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	if got := recv(t, out, 5*time.Second); got.NativeMessageID != "reidentified" {
		t.Fatalf("message = %+v", got)
	}
	// Two identifies, no resume: an unresumable session must not be
	// resumed, or the gateway just invalidates it again.
	if ids := f.framesOfOp(opIdentify); len(ids) < 2 {
		t.Errorf("identify frames = %d, want a second IDENTIFY", len(ids))
	}
	if rs := f.framesOfOp(opResume); len(rs) != 0 {
		t.Errorf("resume frames = %d, want none after INVALID_SESSION(false)", len(rs))
	}
}

func TestInvalidSessionTrueResumes(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{readySession: "sess-keep"},
				{invalidSession: boolPtr(true), delay: 20 * time.Millisecond},
			},
		},
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{resumed: true},
				{message: &gwMessage{id: "resumed-ok", channelID: "c", authorID: "u", content: "back"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	if got := recv(t, out, 5*time.Second); got.NativeMessageID != "resumed-ok" {
		t.Fatalf("message = %+v", got)
	}
	if rs := f.framesOfOp(opResume); len(rs) != 1 {
		t.Errorf("resume frames = %d, want 1: d=true means the session survives", len(rs))
	}
}

func TestDuplicateDispatchesAreSuppressedAcrossAResume(t *testing.T) {
	f := newFakeDiscord(t)
	f.setScript(
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{readySession: "sess-dup"},
				{message: &gwMessage{id: "dup-1", channelID: "c", authorID: "u", content: "once"}},
				{reconnect: true, delay: 30 * time.Millisecond},
			},
		},
		gatewayScript{
			heartbeatIntervalMS: 60000,
			afterIdentify: []gwStep{
				{resumed: true},
				// The replay overlap a resume can produce.
				{message: &gwMessage{id: "dup-1", channelID: "c", authorID: "u", content: "once"}},
				{message: &gwMessage{id: "new-1", channelID: "c", authorID: "u", content: "twice"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _ := runWatch(t, d)

	if got := recv(t, out, 3*time.Second); got.NativeMessageID != "dup-1" {
		t.Fatalf("first = %+v", got)
	}
	if got := recv(t, out, 5*time.Second); got.NativeMessageID != "new-1" {
		t.Fatalf("second = %+v, want the replayed dup-1 suppressed", got)
	}
}

func TestFatalCloseCodesEndTheWatch(t *testing.T) {
	cases := []struct {
		name string
		code int
		want error
	}{
		{"authentication failed", 4004, driver.ErrUnauthorized},
		{"disallowed privileged intent", 4014, driver.ErrPermanent},
		{"invalid intents", 4013, driver.ErrPermanent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeDiscord(t)
			f.setScript(gatewayScript{
				heartbeatIntervalMS: 60000,
				afterIdentify:       []gwStep{{readySession: "sess-x"}},
				closeCode:           c.code,
			})
			d := watchDriver(t, f)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := d.Watch(ctx, testConn(), make(chan driver.Message, 8))
			if !errors.Is(err, c.want) {
				t.Fatalf("Watch = %v, want %v", err, c.want)
			}
		})
	}
}

func TestWatchHonoursTheRecommendedShardCount(t *testing.T) {
	f := newFakeDiscord(t)
	f.gatewayShards = 3
	f.setScript(gatewayScript{
		heartbeatIntervalMS: 60000,
		afterIdentify:       []gwStep{{readySession: "sess-shard"}},
	})
	d := watchDriver(t, f)
	runWatch(t, d)

	waitFor(t, 3*time.Second, "three shards to identify", func() bool {
		return len(f.framesOfOp(opIdentify)) >= 3
	})
	seen := map[int]bool{}
	for _, fr := range f.framesOfOp(opIdentify) {
		var d struct {
			Shard []int `json:"shard"`
		}
		if json.Unmarshal(fr.D, &d) != nil || len(d.Shard) != 2 {
			t.Fatalf("identify carried no shard array: %s", fr.D)
		}
		if d.Shard[1] != 3 {
			t.Errorf("shard count = %d, want 3", d.Shard[1])
		}
		seen[d.Shard[0]] = true
	}
	for i := 0; i < 3; i++ {
		if !seen[i] {
			t.Errorf("shard %d never identified", i)
		}
	}
}

func TestWatchFallsBackWhenGatewayLookupFails(t *testing.T) {
	f := newFakeDiscord(t)
	f.restStatus["/gateway/bot"] = 500
	f.setScript(gatewayScript{heartbeatIntervalMS: 60000})
	d := watchDriver(t, f)
	// With the lookup broken, the configured gateway stands in — a REST
	// hiccup must not stop ingestion (P2).
	d.GatewayURL = f.wsURL

	f.setScript(gatewayScript{
		heartbeatIntervalMS: 60000,
		afterIdentify: []gwStep{
			{readySession: "sess-fallback"},
			{message: &gwMessage{id: "fallback", channelID: "c", authorID: "u", content: "hi"}},
		},
	})
	out, _ := runWatch(t, d)
	if got := recv(t, out, 3*time.Second); got.NativeMessageID != "fallback" {
		t.Errorf("message = %+v", got)
	}
}

func TestDiscoverLiveListsTheResidentBotsTextChannels(t *testing.T) {
	f := newFakeDiscord(t)
	f.setGuilds(fakeGuild{ID: "g1", Name: "Guild One"}, fakeGuild{ID: "g2", Name: "Guild Two"})
	f.setChannels("g1",
		fakeChannel{ID: "c1", Name: "general", Type: channelGuildText},
		fakeChannel{ID: "c2", Name: "voice", Type: 2},
		fakeChannel{ID: "c3", Name: "news", Type: channelGuildAnnouncement},
	)
	// g2 cannot be enumerated: partial visibility must beat none (P2).
	f.restStatus["/guilds/g2/channels"] = 403
	d := watchDriver(t, f)

	refs, err := d.DiscoverLive(context.Background(), testConn())
	if err != nil {
		t.Fatalf("DiscoverLive: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("refs = %+v, want the two text channels of g1", refs)
	}
	if refs[0].NativeChannelID != "c1" || refs[0].Title != "Guild One#general" {
		t.Errorf("ref = %+v", refs[0])
	}
	if refs[1].NativeChannelID != "c3" {
		t.Errorf("announcement channels should be included: %+v", refs[1])
	}
}

func TestDiscoverLiveSurfacesAuthFailure(t *testing.T) {
	f := newFakeDiscord(t)
	f.restStatus["/users/@me/guilds"] = 401
	d := watchDriver(t, f)
	if _, err := d.DiscoverLive(context.Background(), testConn()); !errors.Is(err, driver.ErrUnauthorized) {
		t.Errorf("DiscoverLive = %v, want ErrUnauthorized", err)
	}
}

func TestWatchReturnsPromptlyOnCancelWithoutLeaking(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions()...)

	f := newFakeDiscord(t)
	f.gatewayShards = 2
	f.setScript(gatewayScript{
		heartbeatIntervalMS: 50,
		afterIdentify: []gwStep{
			{readySession: "sess-cancel"},
			{message: &gwMessage{id: "m1", channelID: "c", authorID: "u", content: "one"}},
			{message: &gwMessage{id: "m2", channelID: "c", authorID: "u", content: "two"}},
			{message: &gwMessage{id: "m3", channelID: "c", authorID: "u", content: "three"}},
		},
	})
	d := watchDriver(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	// Unbuffered and drained once, so at least one shard is parked in a
	// channel send when the cancel arrives.
	out := make(chan driver.Message)
	errCh := make(chan error, 1)
	go func() { errCh <- d.Watch(ctx, testConn(), out) }()

	<-out
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Watch = %v, want nil on cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Watch did not return within 3s of cancellation")
	}
}

func goleakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	}
}
