package discord

import (
	"context"
	"errors"
	"net/http"
	"testing"

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
	contentID, _ := minter.ContentID("discord", "111222333")
	messageID, _ := minter.MessageID("discord", "999888777")
	d := New(minter)
	d.HTTPClient = &http.Client{Transport: rt}
	return d, rt, contentID, messageID
}

func TestDeleteBuildsDocumentedRequest(t *testing.T) {
	d, rt, contentID, messageID := newDriver(http.StatusNoContent)
	conn := driver.Connection{AccessToken: "bot-tok"}
	if err := d.Delete(context.Background(), conn, contentID, messageID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if rt.req.Method != http.MethodDelete {
		t.Errorf("method = %s", rt.req.Method)
	}
	if got := rt.req.URL.Path; got != "/api/v10/channels/111222333/messages/999888777" {
		t.Errorf("path = %q", got)
	}
	if got := rt.req.Header.Get("Authorization"); got != "Bot bot-tok" {
		t.Errorf("authorization = %q, want bot auth", got)
	}
}

func TestDeleteStatusMapping(t *testing.T) {
	for status, want := range map[int]error{
		204: nil,
		404: driver.ErrNotFound,
		410: driver.ErrGone,
		429: driver.ErrRateLimited,
		401: driver.ErrUnauthorized,
	} {
		d, _, contentID, messageID := newDriver(status)
		if err := d.Delete(context.Background(), driver.Connection{}, contentID, messageID); !errors.Is(err, want) {
			t.Errorf("status %d: err = %v, want %v", status, err, want)
		}
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
