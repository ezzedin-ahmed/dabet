// Package hist is a lock-light log-linear latency histogram.
//
// The harness needs exact-enough quantiles for two things the services'
// own histograms cannot give it: the generator's send lag (which is how
// the run proves it is not itself the bottleneck) and the verdict
// latency measured from flagged.v1 directly (whose resolution is not
// limited to moderation-service's eleven SLI buckets). Both are
// recorded from many goroutines at up to hundreds of thousands of
// observations a second, so a sorted-sample reservoir is out.
//
// Layout: 40 octaves of 32 linear sub-buckets each, over microsecond
// resolution — so 1 µs to several days. Worst-case relative error
// inside a bucket is 1/32 ≈ 3 %, far finer than anything else in the
// run can resolve, and quantiles round UP to the bucket's upper edge so
// the harness never flatters the system it is measuring.
package hist

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

const (
	subBits = 5
	subs    = 1 << subBits // 32 linear sub-buckets per octave
	octaves = 40           // 1 µs .. ~1e6 s
	nBucket = octaves * subs
)

// Recorder accumulates durations.
type Recorder struct {
	buckets [nBucket]atomic.Int64
	count   atomic.Int64
	sum     atomic.Int64 // nanoseconds
	max     atomic.Int64
	min     atomic.Int64
}

// New returns an empty recorder.
func New() *Recorder {
	r := &Recorder{}
	r.min.Store(math.MaxInt64)
	return r
}

// bucketOf maps a value in microseconds to a bucket index. Values below
// 1 µs (including negatives, which a clock step can produce) land in
// bucket 0.
func bucketOf(micros int64) int {
	if micros < subs {
		if micros < 0 {
			return 0
		}
		return int(micros)
	}
	oct := 63 - bits.LeadingZeros64(uint64(micros))
	shift := oct - subBits
	idx := (oct-subBits+1)*subs + int((micros>>shift)-subs)
	if idx >= nBucket {
		idx = nBucket - 1
	}
	return idx
}

// bucketUpper is the inclusive upper edge, in microseconds, of bucket i.
func bucketUpper(i int) float64 {
	if i < subs {
		return float64(i) + 1
	}
	oct := i/subs - 1 + subBits
	shift := oct - subBits
	base := int64(i%subs) + subs
	return float64((base + 1) << shift)
}

// Record adds one observation.
func (r *Recorder) Record(d time.Duration) {
	ns := int64(d)
	r.count.Add(1)
	r.sum.Add(ns)
	for {
		cur := r.max.Load()
		if ns <= cur || r.max.CompareAndSwap(cur, ns) {
			break
		}
	}
	for {
		cur := r.min.Load()
		if ns >= cur || r.min.CompareAndSwap(cur, ns) {
			break
		}
	}
	r.buckets[bucketOf(ns/1000)].Add(1)
}

// Count is the number of observations.
func (r *Recorder) Count() int64 { return r.count.Load() }

// Mean is the arithmetic mean, or 0 when empty.
func (r *Recorder) Mean() time.Duration {
	n := r.count.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(r.sum.Load() / n)
}

// Max is the largest observation.
func (r *Recorder) Max() time.Duration { return time.Duration(r.max.Load()) }

// Min is the smallest observation, or 0 when empty.
func (r *Recorder) Min() time.Duration {
	v := r.min.Load()
	if v == math.MaxInt64 {
		return 0
	}
	return time.Duration(v)
}

// Quantile returns the q-quantile, rounded up to the containing
// bucket's upper edge (so it never under-reports latency).
func (r *Recorder) Quantile(q float64) time.Duration {
	n := r.count.Load()
	if n == 0 {
		return 0
	}
	want := int64(math.Ceil(q * float64(n)))
	if want < 1 {
		want = 1
	}
	var seen int64
	for i := range nBucket {
		seen += r.buckets[i].Load()
		if seen >= want {
			return time.Duration(bucketUpper(i) * 1000)
		}
	}
	return r.Max()
}

// FractionAtMost is the share of observations at or below d, taken
// straight from bucket counts.
func (r *Recorder) FractionAtMost(d time.Duration) float64 {
	n := r.count.Load()
	if n == 0 {
		return math.NaN()
	}
	limit := float64(d / time.Microsecond)
	var seen int64
	for i := range nBucket {
		if bucketUpper(i) > limit {
			break
		}
		seen += r.buckets[i].Load()
	}
	return float64(seen) / float64(n)
}

// Summary is the JSON-friendly view.
type Summary struct {
	Count  int64   `json:"count"`
	MinMS  float64 `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

// Summarize snapshots the recorder.
func (r *Recorder) Summarize() Summary {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return Summary{
		Count:  r.Count(),
		MinMS:  ms(r.Min()),
		MeanMS: ms(r.Mean()),
		P50MS:  ms(r.Quantile(0.50)),
		P95MS:  ms(r.Quantile(0.95)),
		P99MS:  ms(r.Quantile(0.99)),
		MaxMS:  ms(r.Max()),
	}
}
