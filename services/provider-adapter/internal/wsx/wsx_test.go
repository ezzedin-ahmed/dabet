package wsx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// echoServer speaks a real WebSocket handshake and echoes what it is sent,
// so the shim is exercised over actual frames rather than a mock.
func echoServer(t *testing.T, onConn func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		onConn(r.Context(), c)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestDialReadWrite(t *testing.T) {
	url := echoServer(t, func(ctx context.Context, c *websocket.Conn) {
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			if c.Write(ctx, typ, data) != nil {
				return
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewDialer().Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(StatusNormalClosure, "")

	if err := c.Write(ctx, []byte(`{"op":1}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != `{"op":1}` {
		t.Errorf("echoed %q", data)
	}
}

func TestCloseStatusReportsThePeersCode(t *testing.T) {
	url := echoServer(t, func(_ context.Context, c *websocket.Conn) {
		_ = c.Close(4014, "disallowed intents")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewDialer().Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(StatusNormalClosure, "")

	_, readErr := c.Read(ctx)
	if readErr == nil {
		t.Fatal("read should have failed after the peer closed")
	}
	// Both protocols encode permanent-versus-transient in the close code
	// alone, so recovering it is load-bearing.
	if got := CloseStatus(readErr); got != 4014 {
		t.Errorf("CloseStatus = %d, want 4014", got)
	}
	if got := CloseStatus(context.Canceled); got != StatusNoStatus {
		t.Errorf("CloseStatus of a non-close error = %d, want %d", got, StatusNoStatus)
	}
}

func TestPumpDeliversFramesAndEndsOnClose(t *testing.T) {
	url := echoServer(t, func(ctx context.Context, c *websocket.Conn) {
		for _, m := range []string{"a", "b", "c"} {
			if c.Write(ctx, websocket.MessageText, []byte(m)) != nil {
				return
			}
		}
		_ = c.Close(websocket.StatusNormalClosure, "")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewDialer().Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(StatusNormalClosure, "")

	var got []string
	var sawErr bool
	for f := range Pump(ctx, c) {
		if f.Err != nil {
			sawErr = true
			break
		}
		got = append(got, string(f.Data))
	}
	if strings.Join(got, "") != "abc" {
		t.Errorf("frames = %v, want a b c", got)
	}
	if !sawErr {
		t.Error("the pump should surface the terminating error")
	}
}

func TestPumpStopsOnContextCancellation(t *testing.T) {
	// A server that says nothing: the only thing that can end the pump is
	// the context, which is exactly the watch-loop-teardown case.
	url := echoServer(t, func(ctx context.Context, c *websocket.Conn) {
		<-ctx.Done()
	})

	ctx, cancel := context.WithCancel(context.Background())
	c, err := NewDialer().Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(StatusNormalClosure, "")

	frames := Pump(ctx, c)
	cancel()
	select {
	case f, ok := <-frames:
		if ok && f.Err == nil {
			t.Error("expected the pump to end, not deliver a frame")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pump did not react to cancellation")
	}
}

func TestDialFailureCarriesNoProviderBody(t *testing.T) {
	// P4: a rejected handshake must report the status, never the body,
	// which can echo provider detail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "secret provider detail", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := NewDialer().Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err == nil {
		t.Fatal("dial should have failed")
	}
	if strings.Contains(err.Error(), "secret provider detail") {
		t.Errorf("error leaked the response body: %v", err)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should name the status: %v", err)
	}
}
