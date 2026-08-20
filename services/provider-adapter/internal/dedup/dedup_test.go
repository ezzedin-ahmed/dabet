package dedup

import "testing"

func TestAddReportsFirstSightOnly(t *testing.T) {
	s := New(4)
	if !s.Add("a") {
		t.Error("first sight of a should be new")
	}
	if s.Add("a") {
		t.Error("second sight of a should be a duplicate")
	}
	if !s.Add("b") {
		t.Error("b should be new")
	}
	if s.Len() != 2 {
		t.Errorf("len = %d, want 2", s.Len())
	}
}

func TestWindowEvictsOldestFirst(t *testing.T) {
	s := New(3)
	for _, id := range []string{"1", "2", "3"} {
		s.Add(id)
	}
	// "4" evicts "1", the oldest.
	if !s.Add("4") {
		t.Fatal("4 should be new")
	}
	if s.Len() != 3 {
		t.Errorf("len = %d, want the window to stay bounded at 3", s.Len())
	}
	if !s.Add("1") {
		t.Error("1 should have aged out of the window")
	}
	if s.Add("3") {
		t.Error("3 is still inside the window and must be a duplicate")
	}
}

func TestEmptyIDIsNeverDeduplicated(t *testing.T) {
	// A provider that omitted the id has given us nothing to deduplicate
	// on; collapsing those would silently drop real chat.
	s := New(4)
	if !s.Add("") || !s.Add("") {
		t.Error("an empty id must always be reported as new")
	}
	if s.Len() != 0 {
		t.Errorf("len = %d, want empty ids not to consume the window", s.Len())
	}
}

func TestDefaultCapacity(t *testing.T) {
	if s := New(0); s.capacity != DefaultCapacity {
		t.Errorf("capacity = %d, want %d", s.capacity, DefaultCapacity)
	}
	if s := New(-5); s.capacity != DefaultCapacity {
		t.Errorf("negative capacity = %d, want %d", s.capacity, DefaultCapacity)
	}
}
