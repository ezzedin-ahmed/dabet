package youtube

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
	"dabet/services/provider-adapter/internal/quota"
	"dabet/services/provider-adapter/internal/retry"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// watchDriver wires a driver against the fake with test-scale timings:
// unlimited quota unless a test says otherwise, and short intervals so the
// suite does not sleep.
func watchDriver(t *testing.T, f *fakeYouTube) *Driver {
	t.Helper()
	d := New(opaque.NewMinter())
	d.BaseURL = f.URL()
	d.HTTPClient = f.server.Client()
	d.Budget = quota.Unlimited()
	d.MinPollInterval = time.Millisecond
	d.MaxPollInterval = 50 * time.Millisecond
	d.DiscoveryInterval = 20 * time.Millisecond
	d.Backoff = retry.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond, Jitter: func() float64 { return 0.5 }}
	d.Log = quietLogger()
	return d
}

func testConn() driver.Connection {
	return driver.Connection{
		ID:           "conn-1",
		CreatorID:    "creator-1",
		Platform:     "youtube",
		NativeUserID: "UC-channel",
		AccessToken:  "tok-live",
	}
}

// collect runs Watch and returns the messages it emitted plus its error.
// It stops as soon as want messages have arrived, or the deadline expires.
func collect(t *testing.T, d *Driver, want int, timeout time.Duration) ([]driver.Message, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan driver.Message, 128)
	errCh := make(chan error, 1)
	go func() { errCh <- d.Watch(ctx, testConn(), out) }()

	var got []driver.Message
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case msg := <-out:
			got = append(got, msg)
		case err := <-errCh:
			return got, err
		case <-deadline:
			cancel()
			return got, <-errCh
		}
	}
	cancel()
	return got, <-errCh
}

func TestDiscoverLiveReadsActiveBroadcasts(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(
		fakeBroadcast{ID: "b1", Title: "Morning stream", LiveChatID: "chat-1"},
		// A broadcast with chat disabled must not become a watched stream.
		fakeBroadcast{ID: "b2", Title: "No chat", LiveChatID: ""},
	)
	d := watchDriver(t, f)

	refs, err := d.DiscoverLive(context.Background(), testConn())
	if err != nil {
		t.Fatalf("DiscoverLive: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %d, want 1: %+v", len(refs), refs)
	}
	// The native id is the liveChatId, because that is what
	// liveChatMessages.list is keyed by.
	if refs[0].NativeChannelID != "chat-1" || refs[0].Title != "Morning stream" {
		t.Errorf("ref = %+v", refs[0])
	}
}

func TestWatchPaginatesAndHonoursPollingInterval(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.setPages("chat-1",
		fakePage{
			Items: []fakeItem{
				{ID: "msg-1", AuthorID: "UC-a", Text: "hello"},
				// Non-text events share the stream and must be skipped.
				{ID: "sc-1", Type: "superChatEvent", AuthorID: "UC-b", Text: "thanks"},
			},
			NextPageToken:         "tok-A",
			PollingIntervalMillis: 5,
		},
		fakePage{
			Items:                 []fakeItem{{ID: "msg-2", AuthorID: "UC-b", Text: "second"}},
			NextPageToken:         "tok-B",
			PollingIntervalMillis: 5,
		},
		// An empty page is normal in a quiet chat and must not end the poll.
		fakePage{Items: nil, NextPageToken: "tok-C", PollingIntervalMillis: 5},
		fakePage{
			Items:                 []fakeItem{{ID: "msg-3", AuthorID: "UC-a", Text: "third"}},
			NextPageToken:         "tok-D",
			PollingIntervalMillis: 5,
		},
	)
	d := watchDriver(t, f)

	before := time.Now()
	got, err := collect(t, d, 3, 5*time.Second)
	elapsed := time.Since(before)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3: %+v", len(got), got)
	}
	for i, want := range []string{"msg-1", "msg-2", "msg-3"} {
		if got[i].NativeMessageID != want {
			t.Errorf("message %d = %q, want %q", i, got[i].NativeMessageID, want)
		}
		if got[i].NativeChannelID != "chat-1" {
			t.Errorf("message %d channel = %q", i, got[i].NativeChannelID)
		}
		if got[i].ReceivedAt.IsZero() {
			t.Errorf("message %d has no ReceivedAt; the §4.6 clock never started", i)
		}
	}
	if got[0].NativeAuthorID != "UC-a" {
		t.Errorf("author = %q", got[0].NativeAuthorID)
	}
	if got[0].Text != "hello" {
		t.Errorf("text = %q", got[0].Text)
	}

	// The cursor must have advanced through the server's tokens: first call
	// bare, then each nextPageToken in turn.
	tokens := f.tokens()
	if len(tokens) < 4 {
		t.Fatalf("only %d list calls: %v", len(tokens), tokens)
	}
	if tokens[0] != "" {
		t.Errorf("first call carried pageToken %q; it must be bare", tokens[0])
	}
	for i, want := range []string{"tok-A", "tok-B", "tok-C"} {
		if tokens[i+1] != want {
			t.Errorf("call %d used pageToken %q, want %q", i+2, tokens[i+1], want)
		}
	}

	// pollingIntervalMillis=5 over three gaps is at least 15 ms; a driver
	// ignoring the server's instruction would finish far faster.
	if elapsed < 15*time.Millisecond {
		t.Errorf("collected in %s; pollingIntervalMillis was not honoured", elapsed)
	}
}

func TestWatchReturnsUnauthorizedForTheRefreshPath(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.setPages("chat-1", fakePage{Items: []fakeItem{{ID: "m1", AuthorID: "UC-a", Text: "hi"}}, PollingIntervalMillis: 5})
	// Every list call 401s: the token has expired mid-stream.
	f.listStatus = func(int) (int, string, bool) {
		return 401, errorJSON(401, "authError"), true
	}
	d := watchDriver(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := d.Watch(ctx, testConn(), make(chan driver.Message, 8))
	if !errors.Is(err, driver.ErrUnauthorized) {
		t.Fatalf("Watch = %v, want ErrUnauthorized so the ingest manager refreshes (§5.6)", err)
	}
}

func TestWatchSurfacesDiscoveryAuthFailure(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.broadcastStatus = func(int) (int, string, bool) {
		return 401, errorJSON(401, "authError"), true
	}
	d := watchDriver(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := d.Watch(ctx, testConn(), make(chan driver.Message, 4))
	if !errors.Is(err, driver.ErrUnauthorized) {
		t.Fatalf("Watch = %v, want ErrUnauthorized from discovery", err)
	}
}

func TestWatchRidesOutTransientDiscoveryFailures(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.setPages("chat-1", fakePage{Items: []fakeItem{{ID: "m1", AuthorID: "UC-a", Text: "still here"}}, PollingIntervalMillis: 5})
	// The first two discovery calls 503; a broker-side blip must not kill
	// the connection (P2, fail open).
	f.broadcastStatus = func(call int) (int, string, bool) {
		if call <= 2 {
			return 503, `{"error":{"code":503,"message":"backend error"}}`, true
		}
		return 0, "", false
	}
	d := watchDriver(t, f)

	got, err := collect(t, d, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("Watch = %v, want it to survive transient discovery failures", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d messages, want 1", len(got))
	}
}

func TestWatchTreatsQuotaExceededAsBackoffNotDeath(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.setPages("chat-1", fakePage{
		Items:                 []fakeItem{{ID: "m1", AuthorID: "UC-a", Text: "after the quota cleared"}},
		PollingIntervalMillis: 5,
	})
	// The first two polls are refused for quota, then the API recovers.
	f.listStatus = func(call int) (int, string, bool) {
		if call <= 2 {
			return 403, errorJSON(403, "quotaExceeded"), true
		}
		return 0, "", false
	}
	d := watchDriver(t, f)
	// A real budget, so the drain-on-quotaExceeded path is exercised: 1000
	// units/day refills fast enough at this scale not to stall the test.
	d.Budget = quota.New(1_000_000)

	got, err := collect(t, d, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if len(got) != 1 || got[0].NativeMessageID != "m1" {
		t.Fatalf("got %+v; the driver should have recovered after the quota error", got)
	}
	if f.listCallCount() < 3 {
		t.Errorf("only %d list calls; the driver did not retry past the quota errors", f.listCallCount())
	}
}

func TestWatchEndsTheChatWhenItGoesOffline(t *testing.T) {
	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"})
	f.setPages("chat-1",
		fakePage{Items: []fakeItem{{ID: "m1", AuthorID: "UC-a", Text: "last words"}}, NextPageToken: "t1", PollingIntervalMillis: 5},
		// offlineAt set: the broadcast ended.
		fakePage{OfflineAt: "2026-08-19T15:00:00Z"},
	)
	d := watchDriver(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make(chan driver.Message, 8)
	errCh := make(chan error, 1)
	go func() { errCh <- d.Watch(ctx, testConn(), out) }()

	select {
	case msg := <-out:
		if msg.NativeMessageID != "m1" {
			t.Errorf("message = %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message before the chat ended")
	}

	// A chat ending is not a connection failure: Watch keeps discovering,
	// so it stays alive until ctx is cancelled.
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Errorf("Watch = %v, want nil on cancellation", err)
	}
	// The offline chat must not still be polled after it ended: the
	// discovery loop reaps it and the poller stopped by itself.
	stable := f.listCallCount()
	time.Sleep(50 * time.Millisecond)
	if f.listCallCount() != stable {
		t.Error("polling continued after Watch returned")
	}
}

func TestWatchReturnsPromptlyOnCancelWithoutLeaking(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions()...)

	f := newFakeYouTube(t)
	f.setBroadcasts(fakeBroadcast{ID: "b1", LiveChatID: "chat-1"}, fakeBroadcast{ID: "b2", LiveChatID: "chat-2"})
	page := fakePage{Items: []fakeItem{{ID: "m", AuthorID: "UC-a", Text: "busy"}}, PollingIntervalMillis: 1}
	f.setPages("chat-1", page)
	f.setPages("chat-2", page)
	d := watchDriver(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	// out is deliberately unbuffered and never drained past the first
	// message, so the driver is blocked in a channel send when cancelled —
	// the case the backpressure contract has to unwind cleanly.
	out := make(chan driver.Message)
	errCh := make(chan error, 1)
	go func() { errCh <- d.Watch(ctx, testConn(), out) }()

	<-out // let it get going
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Watch = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return within 2s of cancellation")
	}
}

// goleakOptions ignores goroutines the test HTTP stack keeps alive, which
// are not the driver's to clean up.
func goleakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	}
}
