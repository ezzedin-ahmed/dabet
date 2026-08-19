package sched

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Emit is called once per scheduled message. intended is the ideal-clock
// instant the message was due; lag is how far behind the generator
// actually was when it got to it. Emit must not block for long: a slow
// Emit shows up as lag on the following messages, which is exactly the
// signal the run needs to keep honest.
type Emit func(idx int64, intended time.Time, lag time.Duration)

// Driver runs a Schedule across n goroutines. Goroutine w owns indices
// w, w+n, w+2n, … so the ideal clock is shared and no coordination is
// needed between workers.
type Driver struct {
	Schedule *Schedule
	Workers  int

	// Granularity is the shortest sleep the driver will take. Below it,
	// due messages are released in one burst rather than one syscall
	// each. Sub-granularity jitter is NOT hidden: it lands in the lag
	// measurement like any other delay.
	Granularity time.Duration

	// Now and SleepUntil are injectable for tests.
	Now        func() time.Time
	SleepUntil func(ctx context.Context, t time.Time)
}

// Stats is what a run of the driver reports about itself.
type Stats struct {
	Sent    int64
	Start   time.Time
	End     time.Time
	MaxLag  time.Duration
	SumLag  time.Duration
	Skipped int64 // scheduled but not emitted because ctx ended
}

func sleepUntil(ctx context.Context, t time.Time) {
	d := time.Until(t)
	if d <= 0 {
		return
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
	case <-tm.C:
	}
}

// Run drives the schedule to completion or until ctx ends.
func (d *Driver) Run(ctx context.Context, emit Emit) Stats {
	workers := d.Workers
	if workers < 1 {
		workers = 1
	}
	gran := d.Granularity
	if gran <= 0 {
		gran = 250 * time.Microsecond
	}
	now := d.Now
	if now == nil {
		now = time.Now
	}
	sleep := d.SleepUntil
	if sleep == nil {
		sleep = sleepUntil
	}

	total := d.Schedule.Total()
	start := now()
	var sent, maxLagNs, sumLagNs atomic.Int64

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var localSent, localSum, localMax int64
			defer func() {
				sent.Add(localSent)
				sumLagNs.Add(localSum)
				for {
					cur := maxLagNs.Load()
					if localMax <= cur || maxLagNs.CompareAndSwap(cur, localMax) {
						break
					}
				}
			}()
			for i := int64(w); i < total; {
				if ctx.Err() != nil {
					return
				}
				target := start.Add(d.Schedule.At(i))
				t := now()
				if gap := target.Sub(t); gap > gran {
					sleep(ctx, target)
					if ctx.Err() != nil {
						return
					}
					t = now()
				}
				// Release everything already due in one burst.
				burst := 0
				for i < total {
					due := start.Add(d.Schedule.At(i))
					if due.After(t) {
						break
					}
					lag := t.Sub(due)
					emit(i, due, lag)
					localSent++
					localSum += int64(lag)
					if int64(lag) > localMax {
						localMax = int64(lag)
					}
					i += int64(workers)
					burst++
					if burst%128 == 0 {
						t = now()
						if ctx.Err() != nil {
							return
						}
					}
				}
			}
		}(w)
	}
	wg.Wait()

	st := Stats{
		Sent:   sent.Load(),
		Start:  start,
		End:    now(),
		MaxLag: time.Duration(maxLagNs.Load()),
		SumLag: time.Duration(sumLagNs.Load()),
	}
	st.Skipped = total - st.Sent
	return st
}
