package credits

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newServer(t *testing.T, ok *atomic.Bool, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(Handler(func(_ context.Context, creatorID string) (bool, error) {
		hits.Add(1)
		if creatorID == "" {
			t.Error("empty creator id reached handler")
		}
		return ok.Load(), nil
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOKCachesWithinTTL(t *testing.T) {
	var ok atomic.Bool
	ok.Store(true)
	var hits atomic.Int64
	srv := newServer(t, &ok, &hits)

	c := NewClient(srv.URL, WithTTL(time.Hour))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if !c.OK(ctx, "creator-1") {
			t.Fatal("want ok=true")
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server hits = %d, want 1 (cached)", got)
	}

	// A different creator is a separate cache entry.
	c.OK(ctx, "creator-2")
	if got := hits.Load(); got != 2 {
		t.Errorf("server hits = %d, want 2", got)
	}
}

func TestOKRefetchesAfterTTL(t *testing.T) {
	var ok atomic.Bool
	ok.Store(true)
	var hits atomic.Int64
	srv := newServer(t, &ok, &hits)

	c := NewClient(srv.URL, WithTTL(time.Minute))
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()

	if !c.OK(ctx, "creator-1") {
		t.Fatal("want ok=true")
	}
	ok.Store(false)
	if !c.OK(ctx, "creator-1") {
		t.Fatal("cached value should still be true inside TTL")
	}

	now = now.Add(61 * time.Second)
	if c.OK(ctx, "creator-1") {
		t.Fatal("want ok=false after TTL expiry")
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("server hits = %d, want 2", got)
	}
}

func TestOKCachesFalse(t *testing.T) {
	var ok atomic.Bool // false
	var hits atomic.Int64
	srv := newServer(t, &ok, &hits)

	c := NewClient(srv.URL, WithTTL(time.Hour))
	ctx := context.Background()
	if c.OK(ctx, "creator-1") {
		t.Fatal("want ok=false")
	}
	if c.OK(ctx, "creator-1") {
		t.Fatal("want cached ok=false")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server hits = %d, want 1", got)
	}
}

func TestOKFailsOpenOnTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // connection refused from here on

	var failures atomic.Int64
	c := NewClient(url, WithTTL(time.Hour))
	c.OnFailOpen = func(err error) {
		if err == nil {
			t.Error("OnFailOpen called with nil error")
		}
		failures.Add(1)
	}
	ctx := context.Background()

	if !c.OK(ctx, "creator-1") {
		t.Fatal("must fail open (true) on transport error")
	}
	if !c.OK(ctx, "creator-1") {
		t.Fatal("must fail open again")
	}
	// Failures are not cached: each call retries and counts.
	if got := failures.Load(); got != 2 {
		t.Errorf("fail-open count = %d, want 2", got)
	}
}

func TestOKFailsOpenOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	var failures atomic.Int64
	c := NewClient(srv.URL, WithTTL(time.Hour))
	c.OnFailOpen = func(error) { failures.Add(1) }

	if !c.OK(context.Background(), "creator-1") {
		t.Fatal("must fail open on 500")
	}
	if failures.Load() != 1 {
		t.Errorf("fail-open count = %d, want 1", failures.Load())
	}
}

func TestHandlerContract(t *testing.T) {
	srv := httptest.NewServer(Handler(func(_ context.Context, creatorID string) (bool, error) {
		return creatorID == "rich", nil
	}))
	defer srv.Close()

	for _, tc := range []struct {
		id   string
		want string
	}{
		{"rich", `{"ok":true}`},
		{"poor", `{"ok":false}`},
	} {
		resp, err := http.Get(srv.URL + PathPrefix + tc.id)
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 64)
		n, _ := resp.Body.Read(body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s: status %d", tc.id, resp.StatusCode)
		}
		got := string(body[:n])
		if got != tc.want+"\n" && got != tc.want {
			t.Errorf("%s: body %q, want %q", tc.id, got, tc.want)
		}
	}
}
