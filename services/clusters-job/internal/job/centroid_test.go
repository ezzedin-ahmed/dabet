package job

import (
	"math"
	"testing"
)

func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestMeanCentroid(t *testing.T) {
	got := MeanCentroid([][]float32{{2, 0}, {0, 2}})
	// Mean is (1,1); normalised to (1/√2, 1/√2).
	want := 1 / math.Sqrt2
	if !almostEqual(float64(got[0]), want) || !almostEqual(float64(got[1]), want) {
		t.Errorf("centroid = %v, want [%v %v]", got, want, want)
	}
	var norm float64
	for _, x := range got {
		norm += float64(x) * float64(x)
	}
	if !almostEqual(norm, 1) {
		t.Errorf("centroid norm² = %v, want 1", norm)
	}
	if MeanCentroid(nil) != nil {
		t.Error("empty input should yield nil")
	}
}

func TestMeanCentroidZeroMean(t *testing.T) {
	// Opposite vectors: mean is zero, which must not blow up.
	got := MeanCentroid([][]float32{{1, 0}, {-1, 0}})
	if got[0] != 0 || got[1] != 0 {
		t.Errorf("zero-mean centroid = %v, want [0 0]", got)
	}
}

func TestNormalize(t *testing.T) {
	v := Normalize([]float32{3, 4})
	if !almostEqual(float64(v[0]), 0.6) || !almostEqual(float64(v[1]), 0.8) {
		t.Errorf("Normalize(3,4) = %v, want [0.6 0.8]", v)
	}
	z := Normalize([]float32{0, 0})
	if z[0] != 0 || z[1] != 0 {
		t.Errorf("Normalize(0,0) = %v, want unchanged", z)
	}
}

func TestDot(t *testing.T) {
	if got := Dot([]float32{1, 0}, []float32{0, 1}); !almostEqual(got, 0) {
		t.Errorf("orthogonal dot = %v, want 0", got)
	}
	if got := Dot([]float32{0.6, 0.8}, []float32{0.6, 0.8}); !almostEqual(got, 1) {
		t.Errorf("parallel dot = %v, want 1", got)
	}
}
