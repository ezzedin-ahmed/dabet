package migrate

import (
	"strings"
	"testing"
)

func TestLoadEmbeddedMigrations(t *testing.T) {
	ms, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i := 1; i < len(ms); i++ {
		if ms[i].Version <= ms[i-1].Version {
			t.Errorf("migrations not strictly ordered: %d then %d", ms[i-1].Version, ms[i].Version)
		}
	}
	first := ms[0]
	if first.Version != 1 {
		t.Errorf("first migration version %d, want 1", first.Version)
	}
	sql := first.SQL
	for _, want := range []string{
		"CREATE SCHEMA IF NOT EXISTS review",
		"review.review_cursors",
		"creator_id",
		"next_offset",
		"partition",
		"updated_at",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("migration %s missing %q", first.Name, want)
		}
	}
	// Repo convention: no cross-schema foreign key into identity. The
	// word may appear in comments; only executable lines matter.
	for _, line := range strings.Split(sql, "\n") {
		code, _, _ := strings.Cut(line, "--")
		if strings.Contains(code, "REFERENCES") {
			t.Errorf("migration %s must not declare cross-schema foreign keys: %q", first.Name, line)
		}
	}
}
