package gen

import (
	"math"
	"math/rand/v2"
	"sort"
)

// Zipf draws ranks 0..n-1 with P(i) proportional to 1/(i+1)^s. s == 0 is
// uniform; s ~ 1 is the classic "a handful of enormous streams alongside
// a very long tail of quiet ones" of §7 that N6 calls heavily
// hot-spotted.
//
// This is a precomputed-CDF sampler rather than stdlib math/rand.Zipf
// because the load harness needs the exact per-rank weight as well as a
// draw: the hot-spot scenario asserts on the share of traffic the top
// content received, and rejection sampling cannot tell you that.
type Zipf struct {
	n      int
	s      float64
	cdf    []float64 // cumulative, cdf[n-1] == 1
	weight []float64
}

// NewZipf builds a sampler over n ranks with exponent s. n < 1 is
// treated as 1; a negative s is clamped to 0 (uniform).
func NewZipf(n int, s float64) *Zipf {
	if n < 1 {
		n = 1
	}
	if s < 0 || math.IsNaN(s) {
		s = 0
	}
	z := &Zipf{n: n, s: s, cdf: make([]float64, n), weight: make([]float64, n)}
	total := 0.0
	for i := range n {
		w := 1.0
		if s != 0 {
			w = math.Pow(float64(i+1), -s)
		}
		z.weight[i] = w
		total += w
		z.cdf[i] = total
	}
	for i := range n {
		z.weight[i] /= total
		z.cdf[i] /= total
	}
	// Guard against float drift leaving cdf[n-1] a hair under 1.
	z.cdf[n-1] = 1
	return z
}

// NewWeighted builds a sampler over explicit (unnormalised) weights.
// Negative weights are clamped to zero; an all-zero set degenerates to
// uniform so a misconfigured scenario still runs and says so in its
// distribution report rather than dividing by zero.
func NewWeighted(w []float64) *Zipf {
	n := len(w)
	if n < 1 {
		return NewZipf(1, 0)
	}
	z := &Zipf{n: n, cdf: make([]float64, n), weight: make([]float64, n)}
	total := 0.0
	for i, v := range w {
		if v < 0 || math.IsNaN(v) {
			v = 0
		}
		z.weight[i] = v
		total += v
		z.cdf[i] = total
	}
	if total <= 0 {
		return NewZipf(n, 0)
	}
	for i := range n {
		z.weight[i] /= total
		z.cdf[i] /= total
	}
	z.cdf[n-1] = 1
	return z
}

// N is the population size.
func (z *Zipf) N() int { return z.n }

// Weight is the probability mass of rank i.
func (z *Zipf) Weight(i int) float64 {
	if i < 0 || i >= z.n {
		return 0
	}
	return z.weight[i]
}

// Next draws one rank.
func (z *Zipf) Next(r *rand.Rand) int {
	u := r.Float64()
	i := sort.SearchFloat64s(z.cdf, u)
	if i >= z.n {
		i = z.n - 1
	}
	return i
}

// TopShare is the fraction of all draws expected to land in the k
// highest-weight ranks. The hot-spot scenario states its hypothesis in
// these terms.
func (z *Zipf) TopShare(k int) float64 {
	if k <= 0 {
		return 0
	}
	if k >= z.n {
		return 1
	}
	return z.cdf[k-1]
}
