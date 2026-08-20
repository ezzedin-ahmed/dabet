package twitch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.uber.org/goleak"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/opaque"
	"dabet/services/provider-adapter/internal/retry"
	"dabet/services/provider-adapter/internal/wsx"
)

const (
	testBroadcaster = "1234"
	testChatter     = "5678"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func watchDriver(t *testing.T, f *fakeTwitch) *Driver {
	t.Helper()
	d := New(opaque.NewMinter(), "client-id-1")
	d.BaseURL = f.helixURL()
	d.EventSubURL = f.wsURL
	d.HTTPClient = f.server.Client()
	d.Dialer = wsx.NewDialer()
	d.KeepaliveGrace = 200 * time.Millisecond
	d.Backoff = retry.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond, Jitter: func() float64 { return 0.5 }}
	d.Log = quietLogger()
	return d
}

func testConn() driver.Connection {
	return driver.Connection{
		ID:           "conn-1",
		CreatorID:    "creator-1",
		Platform:     "twitch",
		NativeUserID: testBroadcaster,
		AccessToken:  "user-token",
	}
}

// runWatch starts Watch and returns the message channel, the error channel
// and a cancel func.
func runWatch(t *testing.T, d *Driver) (chan driver.Message, chan error, context.CancelFunc) {
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
	return out, errCh, cancel
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

func TestWatchSubscribesAndRelaysNotifications(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(sessionScript{
		keepaliveSeconds: 30,
		steps: []step{
			{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "msg-1", text: "hello chat"}},
		},
	})
	d := watchDriver(t, f)
	out, _, _ := runWatch(t, d)

	msg := recv(t, out, 3*time.Second)
	if msg.NativeMessageID != "msg-1" || msg.Text != "hello chat" {
		t.Errorf("message = %+v", msg)
	}
	// content_id is minted from broadcaster_user_id, which is what Helix
	// delete-chat-message needs as broadcaster_id.
	if msg.NativeChannelID != testBroadcaster {
		t.Errorf("channel = %q, want the broadcaster user id", msg.NativeChannelID)
	}
	if msg.NativeAuthorID != testChatter {
		t.Errorf("author = %q", msg.NativeAuthorID)
	}
	if msg.ReceivedAt.IsZero() {
		t.Error("ReceivedAt is unset; the §4.6 clock never started")
	}

	subs := f.subscriptions()
	if len(subs) != 1 {
		t.Fatalf("subscriptions = %d, want 1: %+v", len(subs), subs)
	}
	s := subs[0]
	if s.Type != "channel.chat.message" || s.Version != "1" {
		t.Errorf("subscription = %s v%s", s.Type, s.Version)
	}
	if s.Condition["broadcaster_user_id"] != testBroadcaster {
		t.Errorf("condition broadcaster = %q", s.Condition["broadcaster_user_id"])
	}
	if s.Condition["user_id"] != testBroadcaster {
		t.Errorf("condition user_id = %q, want the authenticated reader", s.Condition["user_id"])
	}
	if s.Transport.Method != "websocket" {
		t.Errorf("transport method = %q", s.Transport.Method)
	}
	// The subscription must name the session the welcome handed out.
	if got := f.subscriptionSessions(); len(got) != 1 || got[0] != "session-1" {
		t.Errorf("subscription sessions = %v, want the welcome's session id", got)
	}
}

func TestWatchSurvivesKeepalivesAndSkipsUndecodableFrames(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(sessionScript{
		keepaliveSeconds: 1,
		steps: []step{
			{keepalive: true, delay: 100 * time.Millisecond},
			{raw: `{not json at all`, delay: 50 * time.Millisecond},
			{keepalive: true, delay: 100 * time.Millisecond},
			{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "msg-late", text: "still alive"}},
		},
	})
	d := watchDriver(t, f)
	out, _, _ := runWatch(t, d)

	msg := recv(t, out, 3*time.Second)
	if msg.NativeMessageID != "msg-late" {
		t.Errorf("message = %+v", msg)
	}
	// Keepalives reset the deadline, so the session must not have churned.
	if got := f.sessionCount(); got != 1 {
		t.Errorf("sessions = %d, want 1: keepalives should have kept the socket alive", got)
	}
}

func TestWatchReconnectsWithoutLossOrDuplication(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(
		// First socket: one message, then session_reconnect, then one more
		// message on the *old* socket — which the docs say keeps delivering
		// until the replacement is welcomed.
		sessionScript{
			keepaliveSeconds: 30,
			steps: []step{
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m1", text: "first"}},
				{reconnect: true, delay: 30 * time.Millisecond},
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m2", text: "second"}, delay: 20 * time.Millisecond},
			},
		},
		// Second socket: replays m2 (the overlap the dedup window exists
		// for) and then delivers m3.
		sessionScript{
			keepaliveSeconds: 30,
			steps: []step{
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m2", text: "second", envelopeID: "different-envelope"}, delay: 60 * time.Millisecond},
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m3", text: "third"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _, _ := runWatch(t, d)

	var got []string
	deadline := time.After(5 * time.Second)
	for len(got) < 3 {
		select {
		case m := <-out:
			got = append(got, m.NativeMessageID)
		case <-deadline:
			t.Fatalf("only got %v", got)
		}
	}
	want := []string{"m1", "m2", "m3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("messages = %v, want %v (no loss, in order)", got, want)
		}
	}
	// Nothing more: the duplicate m2 must have been suppressed even though
	// it arrived in a different envelope on a different socket.
	select {
	case extra := <-out:
		t.Errorf("duplicate delivered: %+v", extra)
	case <-time.After(300 * time.Millisecond):
	}

	// The migration used the reconnect_url and did *not* resubscribe:
	// subscriptions carry over to the new session.
	if got := f.sessionCount(); got < 2 {
		t.Errorf("sessions = %d, want the reconnect to have opened a second socket", got)
	}
	if subs := f.subscriptions(); len(subs) != 1 {
		t.Errorf("subscriptions = %d, want 1: they carry over across session_reconnect", len(subs))
	}
}

func TestWatchReconnectsAfterUnexpectedClose(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(
		// 4000 internal server error: transient, so reconnect.
		sessionScript{keepaliveSeconds: 30, closeCode: 4000, closeReason: "internal"},
		sessionScript{
			keepaliveSeconds: 30,
			steps: []step{
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "after-close", text: "back"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _, _ := runWatch(t, d)

	msg := recv(t, out, 5*time.Second)
	if msg.NativeMessageID != "after-close" {
		t.Errorf("message = %+v", msg)
	}
	if got := f.sessionCount(); got < 2 {
		t.Errorf("sessions = %d, want a reconnect", got)
	}
	// The new socket gets its own subscriptions, because a closed session
	// takes its subscriptions with it.
	if subs := f.subscriptions(); len(subs) < 2 {
		t.Errorf("subscriptions = %d, want the new session to resubscribe", len(subs))
	}
}

func TestWatchReconnectsWhenKeepalivesStop(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(
		// A 1 s keepalive window and a server that says nothing: the driver
		// must notice at 1 s + grace rather than hanging on a TCP socket
		// that will never report anything.
		sessionScript{keepaliveSeconds: 1},
		sessionScript{
			keepaliveSeconds: 30,
			steps: []step{
				{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "revived", text: "hi"}},
			},
		},
	)
	d := watchDriver(t, f)
	out, _, _ := runWatch(t, d)

	msg := recv(t, out, 6*time.Second)
	if msg.NativeMessageID != "revived" {
		t.Errorf("message = %+v", msg)
	}
}

func TestWatchRevocationClassification(t *testing.T) {
	cases := []struct {
		status string
		want   error
	}{
		{"authorization_revoked", driver.ErrUnauthorized},
		{"user_removed", driver.ErrGone},
		{"version_removed", driver.ErrPermanent},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			f := newFakeTwitch(t)
			f.setScript(sessionScript{keepaliveSeconds: 30, steps: []step{{revoke: c.status}}})
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

func TestWatchSubscribeFailureClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   error
	}{
		{"unauthorized", 401, driver.ErrUnauthorized},
		{"missing scope", 403, driver.ErrPermanent},
		{"version removed", 410, driver.ErrPermanent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeTwitch(t)
			f.setScript(sessionScript{keepaliveSeconds: 30})
			f.subStatus = func(int) (int, string, bool) { return c.status, `{"error":"nope"}`, true }
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

func TestWatchRefusesMoreChannelsThanOneSocketCarries(t *testing.T) {
	f := newFakeTwitch(t)
	f.setScript(sessionScript{keepaliveSeconds: 30})
	d := watchDriver(t, f)
	d.Broadcasters = func(context.Context, driver.Connection) ([]string, error) {
		ids := make([]string, MaxSubscriptionsPerSocket+1)
		for i := range ids {
			ids[i] = "b"
		}
		return ids, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := d.Watch(ctx, testConn(), make(chan driver.Message, 4))
	if !errors.Is(err, driver.ErrPermanent) {
		t.Fatalf("Watch = %v, want ErrPermanent rather than silently dropping channels", err)
	}
}

func TestDiscoverLiveReportsLiveStreams(t *testing.T) {
	f := newFakeTwitch(t)
	f.setStreams(
		fakeStream{UserID: testBroadcaster, Title: "Ranked grind"},
		// A non-live entry (a rerun) must not be reported as live.
		fakeStream{UserID: "9999", Title: "Rerun", Type: "rerun"},
	)
	d := watchDriver(t, f)

	refs, err := d.DiscoverLive(context.Background(), testConn())
	if err != nil {
		t.Fatalf("DiscoverLive: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %+v, want just the live one", refs)
	}
	if refs[0].NativeChannelID != testBroadcaster || refs[0].Title != "Ranked grind" {
		t.Errorf("ref = %+v", refs[0])
	}
}

func TestWatchReturnsPromptlyOnCancelWithoutLeaking(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions()...)

	f := newFakeTwitch(t)
	f.setScript(sessionScript{
		keepaliveSeconds: 30,
		steps: []step{
			{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m1", text: "one"}},
			{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m2", text: "two"}},
			{notify: &chatMsg{broadcaster: testBroadcaster, chatter: testChatter, id: "m3", text: "three"}},
		},
	})
	d := watchDriver(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	// Unbuffered and drained once: the driver ends up blocked in a channel
	// send, which is the backpressure case cancellation has to unwind.
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
