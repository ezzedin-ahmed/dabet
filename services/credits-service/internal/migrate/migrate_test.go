package migrate

import "testing"

func TestLoadEmbeddedMigrations(t *testing.T) {
	ms, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations embedded")
	}
	if ms[0].Version != 1 || ms[0].Name != "0001_billing.sql" {
		t.Fatalf("first migration = %+v", ms[0])
	}
	for i := 1; i < len(ms); i++ {
		if ms[i].Version <= ms[i-1].Version {
			t.Fatalf("migrations not strictly ordered at %d", i)
		}
	}
}
