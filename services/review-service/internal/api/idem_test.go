package api

import (
	"testing"
	"time"
)

func TestIdemStoreReplayAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	s := newIdemStore(func() time.Time { return now })

	if _, _, ok := s.Get("c1", "k1"); ok {
		t.Fatal("empty store returned a hit")
	}
	s.Put("c1", "k1", 200, []byte(`{"applied":2}`))

	status, body, ok := s.Get("c1", "k1")
	if !ok || status != 200 || string(body) != `{"applied":2}` {
		t.Fatalf("replay mismatch: ok=%v status=%d body=%s", ok, status, body)
	}

	// Keys are scoped per creator.
	if _, _, ok := s.Get("c2", "k1"); ok {
		t.Error("key leaked across creators")
	}

	// Entries expire after the 24h window.
	now = now.Add(idemTTL + time.Minute)
	if _, _, ok := s.Get("c1", "k1"); ok {
		t.Error("expired entry replayed")
	}
}
