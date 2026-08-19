package main

import (
	"math"
	"testing"
)

func TestEmbedDeterministicAndNormalised(t *testing.T) {
	a := embed("hello world")
	b := embed("hello world")
	c := embed("different text")

	if len(a) != dimensions {
		t.Fatalf("dim = %d, want %d", len(a), dimensions)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("embedding is not deterministic")
		}
	}
	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("distinct texts produced identical vectors")
	}

	var norm float64
	for _, v := range a {
		norm += float64(v) * float64(v)
	}
	if math.Abs(math.Sqrt(norm)-1) > 1e-4 {
		t.Errorf("vector norm = %f, want ~1", math.Sqrt(norm))
	}
}
