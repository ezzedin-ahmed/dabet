package mockdriver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
)

func newMux(drv *Driver, src *connsource.Static) *http.ServeMux {
	mux := http.NewServeMux()
	NewHandlers(drv, src).Register(mux)
	return mux
}

func TestInjectionRegistersConnectionAndQueuesMessage(t *testing.T) {
	drv := New(nil)
	src := connsource.NewStatic()
	mux := newMux(drv, src)

	body := `{"creator_id":"creator-1","channel":"chan-A","author":"alice","text":"hi"}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/mock/messages", strings.NewReader(body)))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["connection_id"] != "mock-creator-1" {
		t.Errorf("connection_id = %q, want deterministic default", resp["connection_id"])
	}
	if resp["native_message_id"] != "mockmsg-000001" {
		t.Errorf("native_message_id = %q, want deterministic sequence", resp["native_message_id"])
	}

	conn, ok := src.Get("mock-creator-1")
	if !ok {
		t.Fatal("injection did not register the mock connection")
	}
	if conn.CreatorID != "creator-1" || conn.Platform != PlatformName {
		t.Errorf("registered connection = %+v", conn)
	}

	// The injected message is waiting on the connection's Watch stream.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make(chan driver.Message, 1)
	go func() { _ = drv.Watch(ctx, conn, out) }()
	select {
	case msg := <-out:
		if msg.NativeChannelID != "chan-A" || msg.NativeAuthorID != "alice" ||
			msg.NativeMessageID != "mockmsg-000001" || msg.Text != "hi" {
			t.Errorf("watched message = %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("injected message never reached Watch")
	}
}

func TestInjectionValidation(t *testing.T) {
	mux := newMux(New(nil), connsource.NewStatic())
	for _, body := range []string{
		`{"creator_id":"","channel":"c","author":"a","text":"t"}`,
		`{"creator_id":"x","channel":"c","author":"a"}`,
		`{"unknown_field":true}`,
		`not json`,
	} {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/mock/messages", strings.NewReader(body)))
		if rr.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, rr.Code)
		}
	}
}

func TestDeletionsEndpointExposesRecordedDeletes(t *testing.T) {
	now := time.Date(2026, 8, 19, 15, 0, 0, 0, time.UTC)
	drv := New(func() time.Time { return now })
	mux := newMux(drv, connsource.NewStatic())

	conn := driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: PlatformName}
	if err := drv.Delete(context.Background(), conn, "ct_abc", "pm_def"); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/mock/deletions", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		Deletions []DeletionRecord `json:"deletions"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Deletions) != 1 {
		t.Fatalf("deletions = %d, want 1", len(resp.Deletions))
	}
	got := resp.Deletions[0]
	want := DeletionRecord{ConnectionID: "conn-1", ContentID: "ct_abc", MessageID: "pm_def", DeletedAt: now}
	if got != want {
		t.Errorf("deletion = %+v, want %+v", got, want)
	}
}

func TestWatchStopsCleanlyOnCancel(t *testing.T) {
	drv := New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- drv.Watch(ctx, driver.Connection{ID: "conn-1"}, make(chan driver.Message))
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Watch returned %v on cancel, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after cancellation")
	}
}
