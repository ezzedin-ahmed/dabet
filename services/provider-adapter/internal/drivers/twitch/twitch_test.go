package twitch

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/opaque"
)

type captureRT struct {
	status int
	req    *http.Request
}

func (c *captureRT) RoundTrip(req *http.Request) (*http.Response, error) {
	c.req = req
	return &http.Response{StatusCode: c.status, Body: http.NoBody}, nil
}

func newDriver(status int) (*Driver, *captureRT, string, string) {
	rt := &captureRT{status: status}
	minter := opaque.NewMinter()
	contentID, _ := minter.ContentID("twitch", "44322889")
	messageID, _ := minter.MessageID("twitch", "abc-msg-uuid")
	d := New(minter, "client-abc")
	d.HTTPClient = &http.Client{Transport: rt}
	return d, rt, contentID, messageID
}

func TestDeleteBuildsDocumentedRequest(t *testing.T) {
	d, rt, contentID, messageID := newDriver(http.StatusNoContent)
	conn := driver.Connection{AccessToken: "tok-123", NativeUserID: "mod-777"}
	if err := d.Delete(context.Background(), conn, contentID, messageID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if rt.req.Method != http.MethodDelete {
		t.Errorf("method = %s", rt.req.Method)
	}
	if got := rt.req.URL.Path; got != "/helix/moderation/chat" {
		t.Errorf("path = %q", got)
	}
	q := rt.req.URL.Query()
	if q.Get("broadcaster_id") != "44322889" || q.Get("moderator_id") != "mod-777" || q.Get("message_id") != "abc-msg-uuid" {
		t.Errorf("query = %v", q)
	}
	if got := rt.req.Header.Get("Authorization"); got != "Bearer tok-123" {
		t.Errorf("authorization = %q", got)
	}
	if got := rt.req.Header.Get("Client-Id"); got != "client-abc" {
		t.Errorf("client-id = %q", got)
	}
}

func TestDeleteStatusMapping(t *testing.T) {
	for status, want := range map[int]error{
		204: nil,
		404: driver.ErrNotFound,
		429: driver.ErrRateLimited,
		401: driver.ErrUnauthorized,
	} {
		d, _, contentID, messageID := newDriver(status)
		if err := d.Delete(context.Background(), driver.Connection{}, contentID, messageID); !errors.Is(err, want) {
			t.Errorf("status %d: err = %v, want %v", status, err, want)
		}
	}
}

func TestDeleteUnresolvableContentIsNotFound(t *testing.T) {
	d, rt, _, messageID := newDriver(http.StatusNoContent)
	// A content id minted elsewhere: routable by tag, native id unknown here.
	foreign, _ := opaque.MintContentID("twitch", "some-other-broadcaster")
	err := d.Delete(context.Background(), driver.Connection{}, foreign, messageID)
	if !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	if rt.req != nil {
		t.Error("no provider call should be made for an unresolvable id")
	}
}

func TestWatchRejectsAConnectionWithNoBroadcaster(t *testing.T) {
	// Without a native user id there is no condition to subscribe with, and
	// no amount of retrying invents one.
	d := New(opaque.NewMinter(), "client-abc")
	err := d.Watch(context.Background(), driver.Connection{Platform: "twitch"}, nil)
	if !errors.Is(err, driver.ErrPermanent) {
		t.Errorf("Watch = %v, want ErrPermanent", err)
	}
}

func TestEventSubURLCarriesTheKeepaliveTimeout(t *testing.T) {
	d := New(opaque.NewMinter(), "client-abc")
	if got := d.eventSubURL(); got != DefaultEventSubURL+"?keepalive_timeout_seconds=30" {
		t.Errorf("url = %q", got)
	}
	// Twitch accepts 10-600 s; out-of-range requests are clamped rather
	// than sent and rejected.
	d.KeepaliveTimeout = time.Second
	if got := d.eventSubURL(); got != DefaultEventSubURL+"?keepalive_timeout_seconds=10" {
		t.Errorf("clamped low = %q", got)
	}
	d.KeepaliveTimeout = time.Hour
	if got := d.eventSubURL(); got != DefaultEventSubURL+"?keepalive_timeout_seconds=600" {
		t.Errorf("clamped high = %q", got)
	}
	// A reconnect URL already carries a query string.
	d.EventSubURL = "wss://eventsub.wss.twitch.tv/ws?challenge=abc"
	d.KeepaliveTimeout = 30 * time.Second
	if got := d.eventSubURL(); got != "wss://eventsub.wss.twitch.tv/ws?challenge=abc&keepalive_timeout_seconds=30" {
		t.Errorf("existing query = %q", got)
	}
}

func TestCloseClassification(t *testing.T) {
	// Every documented Twitch close code is transient; the permanent
	// failures arrive as revocations or Helix responses instead.
	for _, code := range []int{4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007} {
		err := classifyClose(websocket.CloseError{Code: websocket.StatusCode(code)})
		if err == nil {
			t.Errorf("close %d classified as a clean end", code)
		}
		if driver.Terminal(err) {
			t.Errorf("close %d should be retryable, got terminal %v", code, err)
		}
	}
	if err := classifyClose(websocket.CloseError{Code: websocket.StatusNormalClosure}); err != nil {
		t.Errorf("normal closure = %v, want nil", err)
	}
}
