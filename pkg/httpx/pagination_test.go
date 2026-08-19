package httpx

import (
	"net/http/httptest"
	"testing"
)

func TestParseLimit(t *testing.T) {
	cases := []struct {
		query   string
		want    int
		wantErr bool
	}{
		{"", DefaultLimit, false},
		{"limit=1", 1, false},
		{"limit=50", 50, false},
		{"limit=200", 200, false},
		{"limit=201", 200, false},  // clamped to max
		{"limit=9999", 200, false}, // clamped to max
		{"limit=0", 0, true},
		{"limit=-5", 0, true},
		{"limit=abc", 0, true},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/v1/reviews?"+c.query, nil)
		got, err := ParseLimit(r)
		if c.wantErr {
			if err == nil {
				t.Errorf("query %q: want error, got %d", c.query, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("query %q: unexpected error %v", c.query, err)
			continue
		}
		if got != c.want {
			t.Errorf("query %q: got %d, want %d", c.query, got, c.want)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	type cursor struct {
		Offset int64  `json:"o"`
		Shard  string `json:"s"`
	}
	in := cursor{Offset: 12395, Shard: "p7"}
	enc, err := EncodeCursor(in)
	if err != nil {
		t.Fatal(err)
	}
	if enc == "" {
		t.Fatal("empty cursor")
	}
	var out cursor
	if err := DecodeCursor(enc, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Errorf("round trip: got %+v, want %+v", out, in)
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	var out struct{}
	if err := DecodeCursor("!!!not-base64!!!", &out); err == nil {
		t.Error("accepted invalid base64")
	}
	if err := DecodeCursor("bm90LWpzb24", &out); err == nil { // "not-json"
		t.Error("accepted non-JSON payload")
	}
}
