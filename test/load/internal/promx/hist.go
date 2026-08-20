package promx

import (
	"math"
	"sort"
)

// Bucket is one cumulative histogram bucket: every observation <= LE.
type Bucket struct {
	LE    float64 `json:"le"`
	Count float64 `json:"count"`
}

// Histogram is a classic Prometheus histogram reassembled from its
// _bucket / _sum / _count series.
type Histogram struct {
	Name    string   `json:"name"`
	Buckets []Bucket `json:"buckets"`
	Sum     float64  `json:"sum"`
	Count   float64  `json:"count"`
}

// Histogram reassembles name from the snapshot. ok is false when the
// family is absent — which, for moderation_e2e_latency_seconds, means
// no message was flagged and there is no SLI to report, not that the
// SLI was zero.
func (s *Snapshot) Histogram(name string, sel map[string]string) (Histogram, bool) {
	h := Histogram{Name: name}
	found := false
	for _, x := range s.Samples {
		if !matches(x.Labels, sel) {
			continue
		}
		switch x.Name {
		case name + "_bucket":
			le, err := parseValue(x.Labels["le"])
			if err != nil {
				continue
			}
			h.Buckets = append(h.Buckets, Bucket{LE: le, Count: x.Value})
			found = true
		case name + "_sum":
			h.Sum += x.Value
			found = true
		case name + "_count":
			h.Count += x.Value
			found = true
		}
	}
	if !found {
		return h, false
	}
	sort.Slice(h.Buckets, func(i, j int) bool { return h.Buckets[i].LE < h.Buckets[j].LE })
	return h, true
}

// Mean is Sum/Count, or NaN when nothing was observed.
func (h Histogram) Mean() float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	return h.Sum / h.Count
}

// Quantile estimates the q-quantile the way Prometheus's
// histogram_quantile does: find the bucket containing the rank, then
// interpolate linearly inside it.
//
// This is an ESTIMATE and the report says so. moderation-service's SLI
// histogram straddles the 1.5 s N1 target between its 1 s and 2.5 s
// buckets, so an interpolated p95 anywhere near 1.5 s is decided by the
// interpolation, not by the data. Bounds() is the honest companion.
func (h Histogram) Quantile(q float64) float64 {
	if h.Count == 0 || len(h.Buckets) == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return h.Buckets[0].LE
	}
	rank := q * h.Count
	prevLE, prevCount := 0.0, 0.0
	for _, b := range h.Buckets {
		if b.Count >= rank {
			if math.IsInf(b.LE, 1) {
				return prevLE // nothing above the last finite bound to interpolate into
			}
			if b.Count == prevCount {
				return b.LE
			}
			return prevLE + (b.LE-prevLE)*(rank-prevCount)/(b.Count-prevCount)
		}
		prevLE, prevCount = b.LE, b.Count
	}
	return prevLE
}

// Bounds returns the bucket edges the q-quantile provably falls between.
// lo is the largest bucket bound with fewer than q*Count observations at
// or below it, hi the smallest bound with at least that many. This is
// what a histogram can actually assert, and it is what the pass/fail
// check against the 1.5 s target uses.
func (h Histogram) Bounds(q float64) (lo, hi float64) {
	if h.Count == 0 || len(h.Buckets) == 0 {
		return math.NaN(), math.NaN()
	}
	rank := q * h.Count
	lo = 0
	for _, b := range h.Buckets {
		if b.Count >= rank {
			return lo, b.LE
		}
		lo = b.LE
	}
	return lo, math.Inf(1)
}

// FractionAtMost is the share of observations that landed at or below
// le, taken straight from the bucket counts with no interpolation. The
// SLI check reduces to FractionAtMost(1.5) >= 0.95 whenever 1.5 is an
// actual bucket bound, and to a bracket otherwise.
func (h Histogram) FractionAtMost(le float64) float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	for _, b := range h.Buckets {
		if b.LE >= le {
			if b.LE == le {
				return b.Count / h.Count
			}
			break
		}
	}
	// le is not a bucket bound: report the fraction at the largest bound
	// at or below it, which is a lower bound on the true fraction.
	frac := 0.0
	for _, b := range h.Buckets {
		if b.LE <= le {
			frac = b.Count / h.Count
		}
	}
	return frac
}

// SubHistogram is the difference between two scrapes of the same
// histogram, i.e. the distribution of what happened during the run
// rather than since the process started.
func SubHistogram(before, after Histogram) Histogram {
	prev := make(map[float64]float64, len(before.Buckets))
	for _, b := range before.Buckets {
		prev[b.LE] = b.Count
	}
	out := Histogram{
		Name:  after.Name,
		Sum:   after.Sum - before.Sum,
		Count: after.Count - before.Count,
	}
	for _, b := range after.Buckets {
		out.Buckets = append(out.Buckets, Bucket{LE: b.LE, Count: b.Count - prev[b.LE]})
	}
	sort.Slice(out.Buckets, func(i, j int) bool { return out.Buckets[i].LE < out.Buckets[j].LE })
	return out
}
