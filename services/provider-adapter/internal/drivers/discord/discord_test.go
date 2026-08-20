package discord

import (
	"context"
	"errors"
	"net/http"
	"testing"

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

func TestWatchWithoutABotTokenIsPermanent(t *testing.T) {
	// Discord is a bot install, not user OAuth (§5.5): with no token there
	// is nothing to refresh and nothing to retry.
	d := New(opaque.NewMinter())
	err := d.Watch(context.Background(), driver.Connection{Platform: "discord"}, nil)
	if !errors.Is(err, driver.ErrPermanent) {
		t.Errorf("Watch = %v, want ErrPermanent", err)
	}
}

func TestIntentsCoverTheEventAndItsText(t *testing.T) {
	// GUILD_MESSAGES (1<<9) delivers MESSAGE_CREATE; MESSAGE_CONTENT
	// (1<<15) is what populates content. Identifying without the second
	// yields events with empty text, which is worse than useless here.
	if IntentGuildMessages != 512 {
		t.Errorf("GUILD_MESSAGES = %d, want 1<<9", IntentGuildMessages)
	}
	if IntentMessageContent != 32768 {
		t.Errorf("MESSAGE_CONTENT = %d, want 1<<15", IntentMessageContent)
	}
	if Intents != 33280 {
		t.Errorf("Intents = %d, want GUILD_MESSAGES|MESSAGE_CONTENT", Intents)
	}
}

func TestGatewayURLCarriesVersionAndEncoding(t *testing.T) {
	if got := gatewayURL("wss://gateway.discord.gg"); got != "wss://gateway.discord.gg?v=10&encoding=json" {
		t.Errorf("url = %q", got)
	}
	// A resume_gateway_url may already carry a query string.
	if got := gatewayURL("wss://gateway.discord.gg/?x=1"); got != "wss://gateway.discord.gg/?x=1&v=10&encoding=json" {
		t.Errorf("url with query = %q", got)
	}
}

func TestCloseCodeClassificationFollowsTheReconnectColumn(t *testing.T) {
	// Reconnectable per the documented table.
	for _, code := range []int{4000, 4001, 4002, 4003, 4005, 4007, 4008, 4009} {
		err := classifyClose(websocket.CloseError{Code: websocket.StatusCode(code)})
		if err == nil {
			t.Errorf("close %d read as a clean end", code)
			continue
		}
		if driver.Terminal(err) || errors.Is(err, driver.ErrUnauthorized) {
			t.Errorf("close %d should be retryable, got %v", code, err)
		}
	}
	// Fatal per the documented table.
	for _, code := range []int{4010, 4011, 4012, 4013, 4014} {
		err := classifyClose(websocket.CloseError{Code: websocket.StatusCode(code)})
		if !errors.Is(err, driver.ErrPermanent) {
			t.Errorf("close %d = %v, want ErrPermanent", code, err)
		}
	}
	// 4004 is an auth failure, so it takes the §5.6 path.
	if err := classifyClose(websocket.CloseError{Code: 4004}); !errors.Is(err, driver.ErrUnauthorized) {
		t.Errorf("close 4004 = %v, want ErrUnauthorized", err)
	}
	if err := classifyClose(websocket.CloseError{Code: websocket.StatusNormalClosure}); err != nil {
		t.Errorf("normal closure = %v, want nil", err)
	}
}

func TestModeratableFiltersNonUserText(t *testing.T) {
	base := messageCreate{ID: "1", ChannelID: "c", Content: "hello", Type: messageTypeDefault}
	base.Author.ID = "u1"
	if !moderatable(base) {
		t.Error("an ordinary message should be moderated")
	}
	reply := base
	reply.Type = messageTypeReply
	if !moderatable(reply) {
		t.Error("a reply should be moderated")
	}

	empty := base
	empty.Content = ""
	if moderatable(empty) {
		t.Error("empty content has nothing to moderate")
	}
	bot := base
	bot.Author.Bot = true
	if moderatable(bot) {
		t.Error("bot messages should be skipped")
	}
	sys := base
	sys.Author.System = true
	if moderatable(sys) {
		t.Error("system messages should be skipped")
	}
	hook := base
	hook.WebhookID = "wh-1"
	if moderatable(hook) {
		t.Error("webhook posts have no real author and should be skipped")
	}
	join := base
	join.Type = 7 // USER_JOIN
	if moderatable(join) {
		t.Error("join/pin/boost notices should be skipped")
	}
}
