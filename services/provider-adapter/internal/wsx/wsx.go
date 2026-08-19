// Package wsx is the injectable WebSocket transport the Twitch and Discord
// drivers are built against. Both platforms deliver chat over a long-lived
// socket (Twitch EventSub, Discord Gateway), and both need the same three
// things from a transport: context-aware reads so ctx cancellation unblocks
// a blocked reader immediately (the ingest manager starts and stops watch
// loops on every rebalance, §7.2 A13), context-aware writes, and the close
// code the peer sent — both protocols encode permanent-versus-transient
// failure in the close code alone.
//
// The interfaces exist so tests can drive a driver against a fake that
// speaks the real protocol without reaching the network. The production
// implementation is a thin shim over github.com/coder/websocket.
package wsx

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

// Conn is one open WebSocket connection. Read and Write take a context so
// a cancelled watch tears the socket down without waiting for a deadline.
type Conn interface {
	// Read returns the next complete message payload. It returns an error
	// once the connection is closed, ctx is done, or the peer sends a close
	// frame; a peer close carries its status code (see CloseStatus).
	Read(ctx context.Context) ([]byte, error)
	// Write sends one text message.
	Write(ctx context.Context, data []byte) error
	// Close closes the connection with a status code and reason. It is safe
	// to call more than once and never blocks for long.
	Close(code int, reason string) error
}

// Dialer opens WebSocket connections. Drivers hold one so tests can point
// them at an httptest server (or a fake) instead of the provider.
type Dialer interface {
	Dial(ctx context.Context, url string, header http.Header) (Conn, error)
}

// StatusNoStatus is what CloseStatus reports for an error that is not a
// peer-initiated close (a transport failure, a cancelled context).
const StatusNoStatus = -1

// StatusNormalClosure is RFC 6455 code 1000.
const StatusNormalClosure = 1000

// CloseStatus returns the WebSocket close code the peer sent, or
// StatusNoStatus when err is not a close error. Twitch encodes its whole
// failure taxonomy in codes 4000-4007 and Discord in 4000-4014, so drivers
// branch on this to decide reconnect-versus-give-up.
func CloseStatus(err error) int {
	return int(websocket.CloseStatus(err))
}

// ErrClosed reports whether err is any form of connection termination
// (peer close, transport failure) rather than a caller-side cancellation.
func ErrClosed(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// Frame is one inbound message, or the error that ended the stream.
type Frame struct {
	Data []byte
	Err  error
}

// Pump reads c in its own goroutine and publishes frames on the returned
// channel, which is closed after the first error (a peer close, a
// cancelled context, a transport failure) — so a ranging consumer
// terminates naturally.
//
// It exists because both WebSocket protocols need to select between an
// inbound frame and something else: a heartbeat tick (Discord), a keepalive
// deadline, or a second socket being brought up during a Twitch
// session_reconnect. A blocking Read cannot be selected on; a channel can.
//
// The goroutine ends when ctx is cancelled, because Conn.Read is
// context-aware. Callers must cancel ctx (or close the connection) to avoid
// leaking it.
func Pump(ctx context.Context, c Conn) <-chan Frame {
	// Capacity 1 keeps the reader one frame ahead of the consumer without
	// building an unbounded queue: backpressure still reaches the socket.
	out := make(chan Frame, 1)
	go func() {
		defer close(out)
		for {
			data, err := c.Read(ctx)
			select {
			case out <- Frame{Data: data, Err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return out
}

// DefaultReadLimit caps a single inbound message. Discord READY payloads
// for a bot in many guilds are large, and Twitch notifications carry
// message fragments; 8 MiB is generous for both and still bounds memory
// against a hostile or broken peer.
const DefaultReadLimit = 8 << 20

// Dial is the production Dialer over github.com/coder/websocket.
type Dial struct {
	// HTTPClient performs the opening handshake; nil uses the library
	// default. Tests point this at an httptest server's client.
	HTTPClient *http.Client
	// ReadLimit caps one inbound message; 0 means DefaultReadLimit.
	ReadLimit int64
}

// NewDialer returns a production dialer.
func NewDialer() *Dial { return &Dial{} }

// Dial implements Dialer.
func (d *Dial) Dial(ctx context.Context, url string, header http.Header) (Conn, error) {
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient: d.HTTPClient,
		HTTPHeader: header,
	})
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		// P4: the handshake response body may echo provider detail; only
		// the status is worth reporting.
		if resp != nil {
			return nil, fmt.Errorf("wsx: dial failed with status %d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("wsx: dial failed: %w", err)
	}
	limit := d.ReadLimit
	if limit == 0 {
		limit = DefaultReadLimit
	}
	c.SetReadLimit(limit)
	return &conn{c: c}, nil
}

type conn struct{ c *websocket.Conn }

func (c *conn) Read(ctx context.Context) ([]byte, error) {
	_, data, err := c.c.Read(ctx)
	return data, err
}

func (c *conn) Write(ctx context.Context, data []byte) error {
	return c.c.Write(ctx, websocket.MessageText, data)
}

func (c *conn) Close(code int, reason string) error {
	// CloseNow on a normal-closure request would skip the close handshake;
	// Close sends the frame and gives the peer a moment to answer. Either
	// way the underlying conn is released, so errors here are advisory.
	err := c.c.Close(websocket.StatusCode(code), reason)
	if err != nil {
		_ = c.c.CloseNow()
	}
	return err
}
