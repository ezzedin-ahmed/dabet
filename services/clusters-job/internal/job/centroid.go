package job

import "math"

// MeanCentroid returns the L2-normalised mean of the given vectors — the
// §8.6 centroid definition. All vectors must share one dimensionality.
// A degenerate zero mean is returned un-normalised.
func MeanCentroid(vecs [][]float32) []float32 {
	if len(vecs) == 0 {
		return nil
	}
	dim := len(vecs[0])
	sum := make([]float64, dim)
	for _, v := range vecs {
		for i, x := range v {
			sum[i] += float64(x)
		}
	}
	out := make([]float32, dim)
	n := float64(len(vecs))
	for i := range sum {
		out[i] = float32(sum[i] / n)
	}
	return Normalize(out)
}

// Normalize L2-normalises v in place and returns it. Zero vectors are
// left unchanged.
func Normalize(v []float32) []float32 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	if s == 0 {
		return v
	}
	inv := 1 / math.Sqrt(s)
	for i := range v {
		v[i] = float32(float64(v[i]) * inv)
	}
	return v
}

// Dot is the cosine similarity of two L2-normalised vectors.
func Dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}
