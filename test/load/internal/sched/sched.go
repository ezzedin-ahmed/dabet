// Package sched turns a rate profile into an *ideal clock*: the exact
// instant at which message i was supposed to be sent, independent of
// whether the generator or the system under test kept up.
//
// This is the whole coordinated-omission story. A naive generator does
// "send, wait for the response, sleep a bit, repeat", so when the system
// stalls the generator stalls with it and simply stops producing the
// samples that would have been slow. Here the send schedule is fixed
// before the run starts: message i is due at Start+At(i) whatever
// happens, and both the generator's own backlog (send lag) and the
// service-side latency (which is measured against the *intended* send
// time, carried in ingested_at) include the queueing the stall caused.
package sched

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Segment is one leg of a rate profile: the rate moves linearly from
// From to To over Duration. A steady leg has From == To.
type Segment struct {
	Duration time.Duration `json:"duration"`
	From     float64       `json:"from"` // messages/second at the start
	To       float64       `json:"to"`   // messages/second at the end
}

// Profile is a sequence of segments.
type Profile struct {
	Segments []Segment `json:"segments"`
}

// Steady is a constant-rate profile.
func Steady(rate float64, d time.Duration) Profile {
	return Profile{Segments: []Segment{{Duration: d, From: rate, To: rate}}}
}

// Ramp climbs linearly from lo to hi over d. Used to find the knee: the
// rate at which p95 crosses the N1 budget or lag starts growing without
// bound (§4.7).
func Ramp(lo, hi float64, d time.Duration) Profile {
	return Profile{Segments: []Segment{{Duration: d, From: lo, To: hi}}}
}

// Steps climbs from lo to hi in n equal plateaus of d each, which is
// what you want when the knee has to be attributed to a specific rate:
// a continuous ramp smears the queueing across rates, a staircase lets
// each plateau reach its own steady state.
func Steps(lo, hi float64, n int, d time.Duration) Profile {
	if n < 1 {
		n = 1
	}
	p := Profile{}
	for i := range n {
		r := lo
		if n > 1 {
			r = lo + (hi-lo)*float64(i)/float64(n-1)
		}
		p.Segments = append(p.Segments, Segment{Duration: d, From: r, To: r})
	}
	return p
}

// Spike holds base for pre, jumps to peak for burst, then returns to
// base for post. The jump is a step, not a ramp: the point is to see
// what a sudden hot-spot does to lag and to the sampler.
func Spike(base, peak float64, pre, burst, post time.Duration) Profile {
	return Profile{Segments: []Segment{
		{Duration: pre, From: base, To: base},
		{Duration: burst, From: peak, To: peak},
		{Duration: post, From: base, To: base},
	}}
}

// Validate rejects profiles that cannot be scheduled.
func (p Profile) Validate() error {
	if len(p.Segments) == 0 {
		return errors.New("profile has no segments")
	}
	for i, s := range p.Segments {
		if s.Duration <= 0 {
			return fmt.Errorf("segment %d: duration %v, want > 0", i, s.Duration)
		}
		if s.From < 0 || s.To < 0 || math.IsNaN(s.From) || math.IsNaN(s.To) {
			return fmt.Errorf("segment %d: rates (%v -> %v) must be finite and >= 0", i, s.From, s.To)
		}
	}
	return nil
}

// Duration is the profile's wall-clock length.
func (p Profile) Duration() time.Duration {
	var d time.Duration
	for _, s := range p.Segments {
		d += s.Duration
	}
	return d
}

// PeakRate is the highest instantaneous rate the profile reaches.
func (p Profile) PeakRate() float64 {
	m := 0.0
	for _, s := range p.Segments {
		m = math.Max(m, math.Max(s.From, s.To))
	}
	return m
}

// Schedule is a compiled profile: an O(log n) map from message index to
// its due offset from the start of the run.
type Schedule struct {
	segs  []compiled
	total int64
	dur   time.Duration
}

type compiled struct {
	startOff time.Duration
	dur      float64 // seconds
	from, to float64
	startN   float64 // cumulative messages before this segment
	countN   float64 // messages within this segment
}

// Compile builds a Schedule. The number of messages in a segment is the
// integral of the rate over it — the trapezoid (from+to)/2 * duration —
// so a ramp really does deliver the area under its own curve.
func Compile(p Profile) (*Schedule, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	s := &Schedule{}
	var off time.Duration
	var cum float64
	for _, seg := range p.Segments {
		d := seg.Duration.Seconds()
		n := (seg.From + seg.To) / 2 * d
		s.segs = append(s.segs, compiled{
			startOff: off, dur: d, from: seg.From, to: seg.To,
			startN: cum, countN: n,
		})
		cum += n
		off += seg.Duration
	}
	s.total = int64(cum)
	s.dur = off
	return s, nil
}

// Total is how many messages the profile schedules.
func (s *Schedule) Total() int64 { return s.total }

// Duration is the profile's wall-clock length.
func (s *Schedule) Duration() time.Duration { return s.dur }

// At returns the offset from the run start at which message i is due.
//
// Within a segment the cumulative count is n(t) = from*t + (to-from)/(2*dur)*t^2,
// so inverting for t is a quadratic; the linear case (from == to) is
// solved directly to avoid a 0/0.
func (s *Schedule) At(i int64) time.Duration {
	x := float64(i)
	for _, c := range s.segs {
		if x >= c.startN+c.countN && c.countN > 0 {
			continue
		}
		if c.countN <= 0 {
			continue
		}
		local := x - c.startN
		var t float64
		a := (c.to - c.from) / (2 * c.dur) // half the slope
		switch {
		case math.Abs(a) < 1e-12:
			if c.from <= 0 {
				t = c.dur
			} else {
				t = local / c.from
			}
		default:
			// a*t^2 + from*t - local = 0
			disc := c.from*c.from + 4*a*local
			if disc < 0 {
				disc = 0
			}
			t = (-c.from + math.Sqrt(disc)) / (2 * a)
		}
		if t < 0 {
			t = 0
		}
		if t > c.dur {
			t = c.dur
		}
		return c.startOff + time.Duration(t*float64(time.Second))
	}
	return s.dur
}

// RateAt reports the instantaneous scheduled rate at offset off, which
// is what the report labels each latency window with.
func (s *Schedule) RateAt(off time.Duration) float64 {
	for i, c := range s.segs {
		end := c.startOff + time.Duration(c.dur*float64(time.Second))
		if off < end || i == len(s.segs)-1 {
			if c.dur <= 0 {
				return c.from
			}
			f := (off - c.startOff).Seconds() / c.dur
			f = math.Max(0, math.Min(1, f))
			return c.from + (c.to-c.from)*f
		}
	}
	return 0
}
