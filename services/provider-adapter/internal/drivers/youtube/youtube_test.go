package youtube

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/opaque"
)

// captureRT fakes the provider without any network: it records the request
// and returns a canned status.
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
	contentID, _ := minter.ContentID("youtube", "live-chat-1")
	messageID, _ := minter.MessageID("youtube", "LCC.native-msg")
	d := New(minter)
	d.HTTPClient = &http.Client{Transport: rt}
	return d, rt, contentID, messageID
}

func TestDeleteBuildsDocumentedRequest(t *testing.T) {
	d, rt, contentID, messageID := newDriver(http.StatusNoContent)
	conn := driver.Connection{AccessToken: "tok-123"}
	if err := d.Delete(context.Background(), conn, contentID, messageID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if rt.req.Method != http.MethodDelete {
		t.Errorf("method = %s", rt.req.Method)
	}
	if got := rt.req.URL.Path; got != "/youtube/v3/liveChat/messages" {
		t.Errorf("path = %q", got)
	}
	if got := rt.req.URL.Query().Get("id"); got != "LCC.native-msg" {
		t.Errorf("id = %q, want the platform-native message id", got)
	}
	if got := rt.req.Header.Get("Authorization"); got != "Bearer tok-123" {
		t.Errorf("authorization = %q", got)
	}
}

func TestDeleteStatusMapping(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{204, nil},
		{404, driver.ErrNotFound},
		{403, driver.ErrGone}, // liveChatEnded
		{429, driver.ErrRateLimited},
		{401, driver.ErrUnauthorized},
	}
	for _, c := range cases {
		d, _, contentID, messageID := newDriver(c.status)
		err := d.Delete(context.Background(), driver.Connection{}, contentID, messageID)
		if !errors.Is(err, c.want) {
			t.Errorf("status %d: err = %v, want %v", c.status, err, c.want)
		}
	}
	d, _, contentID, messageID := newDriver(500)
	if err := d.Delete(context.Background(), driver.Connection{}, contentID, messageID); err == nil {
		t.Error("500 should be a transient error")
	}
}

func TestDeleteUnresolvableMessageIsNotFound(t *testing.T) {
	d, rt, contentID, _ := newDriver(http.StatusNoContent)
	// A hashed message id minted by another instance: platform routes, but
	// the native id is unknown here.
	unknown, _, _ := opaque.MintMessageID("youtube", string(make([]byte, 200)))
	err := d.Delete(context.Background(), driver.Connection{}, contentID, unknown)
	if !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	if rt.req != nil {
		t.Error("no provider call should be made for an unresolvable id")
	}
}

func TestWatchAndDiscoverAreNotImplemented(t *testing.T) {
	d := New(opaque.NewMinter())
	if err := d.Watch(context.Background(), driver.Connection{}, nil); !errors.Is(err, driver.ErrNotImplemented) {
		t.Errorf("Watch = %v", err)
	}
	if _, err := d.DiscoverLive(context.Background(), driver.Connection{}); !errors.Is(err, driver.ErrNotImplemented) {
		t.Errorf("DiscoverLive = %v", err)
	}
}
