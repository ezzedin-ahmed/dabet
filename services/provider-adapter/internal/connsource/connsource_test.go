package connsource

import (
	"context"
	"testing"

	"dabet/services/provider-adapter/internal/driver"
)

func TestParseEnv(t *testing.T) {
	conns, err := ParseEnv(" mock:conn-1:creator-1 , twitch:conn-2:creator-2:44322889 ,")
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 {
		t.Fatalf("parsed %d connections, want 2", len(conns))
	}
	want0 := driver.Connection{Platform: "mock", ID: "conn-1", CreatorID: "creator-1"}
	want1 := driver.Connection{Platform: "twitch", ID: "conn-2", CreatorID: "creator-2", NativeUserID: "44322889"}
	if conns[0] != want0 || conns[1] != want1 {
		t.Errorf("parsed %+v, want [%+v %+v]", conns, want0, want1)
	}
	if got, err := ParseEnv(""); err != nil || len(got) != 0 {
		t.Errorf("empty env: %v, %v", got, err)
	}
	for _, bad := range []string{"mock:conn-1", "a:b:c:d:e", "::x", "mock::creator"} {
		if _, err := ParseEnv(bad); err == nil {
			t.Errorf("ParseEnv(%q) should fail", bad)
		}
	}
}

func TestStaticLifecycle(t *testing.T) {
	s := NewStatic(driver.Connection{ID: "conn-1", CreatorID: "creator-1", Platform: "mock"})

	if _, ok := s.Lookup("creator-1", "mock"); !ok {
		t.Error("seeded connection not found by creator+platform")
	}
	if _, ok := s.Lookup("creator-1", "twitch"); ok {
		t.Error("lookup should be platform-scoped")
	}

	s.Add(driver.Connection{ID: "conn-2", CreatorID: "creator-2", Platform: "mock"})
	select {
	case <-s.Changes():
	default:
		t.Fatal("Add did not signal a change")
	}

	conns, err := s.List(context.Background())
	if err != nil || len(conns) != 2 {
		t.Fatalf("List = %v, %v", conns, err)
	}

	s.Remove("conn-1")
	if _, ok := s.Get("conn-1"); ok {
		t.Error("removed connection still present")
	}
	select {
	case <-s.Changes():
	default:
		t.Fatal("Remove did not signal a change")
	}

	// Signals coalesce: many changes, at most one pending signal, and a
	// re-List sees the full current state.
	s.Add(driver.Connection{ID: "a", CreatorID: "ca", Platform: "mock"})
	s.Add(driver.Connection{ID: "b", CreatorID: "cb", Platform: "mock"})
	<-s.Changes()
	select {
	case <-s.Changes():
		t.Error("signals should coalesce to one pending notification")
	default:
	}
}
