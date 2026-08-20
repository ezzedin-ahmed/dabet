package shard

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
)

func members(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("adapter-%d", i)
	}
	return out
}

func connIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		// Deliberately not sequential-looking: real connection IDs are
		// ULIDs, and a hash that only looks good on "conn-0000".."conn-9999"
		// is a hash that has not been tested.
		out[i] = fmt.Sprintf("01K%08X-conn-%d", i*2654435761&0xffffffff, i)
	}
	return out
}

// ownership maps key -> owner for the whole key set.
func ownership(r *Ring, keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		owner, ok := r.Owner(k)
		if !ok {
			continue
		}
		out[k] = owner
	}
	return out
}

// TestRingMinimalDisruptionOnJoin is the property A13 exists for: an
// instance joining takes roughly its fair share and *nothing else moves*.
// A ring that reshuffled more would reconnect live chat sockets that had
// no reason to move, which N2 forbids.
func TestRingMinimalDisruptionOnJoin(t *testing.T) {
	const M = 20000
	keys := connIDs(M)

	for _, n := range []int{1, 2, 3, 5, 8, 16} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			before := NewRing(DefaultReplicas, members(n))
			after := NewRing(DefaultReplicas, members(n+1))
			joiner := fmt.Sprintf("adapter-%d", n)

			ob := ownership(before, keys)
			oa := ownership(after, keys)

			var moved, movedToJoiner int
			for _, k := range keys {
				if ob[k] == oa[k] {
					continue
				}
				moved++
				if oa[k] != joiner {
					// The exact-set assertion: the ONLY keys allowed to
					// change hands are the ones the joiner took. A key
					// moving between two incumbents is a reshuffle.
					t.Fatalf("connection %q moved from %q to %q; only moves to the joining instance %q are permitted",
						k, ob[k], oa[k], joiner)
				}
				movedToJoiner++
			}
			// And every key the joiner now owns must be a key that moved:
			// no key may arrive at the joiner without having left someone.
			for _, k := range keys {
				if oa[k] == joiner && ob[k] == joiner {
					t.Fatalf("connection %q owned by the joiner before it joined", k)
				}
			}

			want := float64(M) / float64(n+1)
			if lo, hi := want*0.5, want*1.75; float64(moved) < lo || float64(moved) > hi {
				t.Errorf("joining moved %d of %d connections; want roughly M/(N+1)=%.0f (accepted %.0f..%.0f)",
					moved, M, want, lo, hi)
			}
		})
	}
}

// TestRingMinimalDisruptionOnLeave is the deploy/crash case: only the
// departing instance's connections move, and they scatter across the
// survivors rather than all landing on one.
func TestRingMinimalDisruptionOnLeave(t *testing.T) {
	const M = 20000
	keys := connIDs(M)

	for _, n := range []int{2, 3, 5, 8, 16} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			all := members(n)
			leaver := all[n-1]
			before := NewRing(DefaultReplicas, all)
			after := NewRing(DefaultReplicas, all[:n-1])

			ob := ownership(before, keys)
			oa := ownership(after, keys)

			moved := 0
			for _, k := range keys {
				if ob[k] == oa[k] {
					continue
				}
				moved++
				if ob[k] != leaver {
					t.Fatalf("connection %q moved from %q to %q; only the departing instance %q may lose connections",
						k, ob[k], oa[k], leaver)
				}
			}
			// Exact set equality: the moved set IS the leaver's old set.
			leaverHad := 0
			for _, k := range keys {
				if ob[k] == leaver {
					leaverHad++
				}
			}
			if moved != leaverHad {
				t.Fatalf("moved %d connections but the departing instance held %d", moved, leaverHad)
			}
			if n > 2 {
				// The leaver's share must not all land on its single ring
				// neighbour — that is the failure mode virtual nodes exist
				// to prevent, and it would push one survivor over the cap.
				recipients := make(map[string]int)
				for _, k := range keys {
					if ob[k] == leaver {
						recipients[oa[k]]++
					}
				}
				if len(recipients) != n-1 {
					t.Errorf("the departing instance's connections landed on %d of %d survivors; virtual nodes should spread them over all",
						len(recipients), n-1)
				}
			}
		})
	}
}

// TestRingBalance asserts distribution quality.
//
// The asserted factor is 1.35x the mean for the busiest instance and
// 0.65x for the quietest. Measured over 50 000 connections at
// DefaultReplicas=160 the real spread is 1.05–1.21x / 0.87–0.94x for N
// from 2 to 32, so the bound has roughly 15 points of headroom for hash
// luck. It is chosen tight enough to fail a regression that weakens
// virtual nodes: at one point per member the same measurement gives
// 1.99–4.58x / 0.01–0.03x, i.e. one instance holding four times its share
// while another holds almost nothing — which at a 5 000 cap is refusals
// on one instance and idle capacity on another.
//
// 1.35x is also the number to size a fleet against: provision so that the
// mean share is at most cap/1.35 ≈ 3 700 connections.
func TestRingBalance(t *testing.T) {
	const M = 50000
	keys := connIDs(M)

	for _, n := range []int{2, 3, 5, 10, 16, 32} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			r := NewRing(DefaultReplicas, members(n))
			counts := make(map[string]int, n)
			for _, k := range keys {
				owner, _ := r.Owner(k)
				counts[owner]++
			}
			if len(counts) != n {
				t.Fatalf("only %d of %d instances received any connection", len(counts), n)
			}
			mean := float64(M) / float64(n)
			maxN, minN := 0, math.MaxInt
			for _, c := range counts {
				maxN = max(maxN, c)
				minN = min(minN, c)
			}
			if ratio := float64(maxN) / mean; ratio > 1.35 {
				t.Errorf("busiest instance holds %d connections, %.2fx the mean %.0f; want <= 1.35x", maxN, ratio, mean)
			}
			if ratio := float64(minN) / mean; ratio < 0.65 {
				t.Errorf("quietest instance holds %d connections, %.2fx the mean %.0f; want >= 0.65x", minN, ratio, mean)
			}
		})
	}
}

// TestRingBalanceDegradesWithoutVirtualNodes is the control for
// TestRingBalance: it proves the asserted factor is actually sensitive to
// the virtual node count rather than a bound anything would pass. Both
// rings are deterministic, so this is an assertion, not a sample.
func TestRingBalanceDegradesWithoutVirtualNodes(t *testing.T) {
	const M = 50000
	keys := connIDs(M)
	r := NewRing(1, members(10))
	counts := make(map[string]int)
	for _, k := range keys {
		owner, _ := r.Owner(k)
		counts[owner]++
	}
	mean := float64(M) / 10
	maxN := 0
	for _, c := range counts {
		maxN = max(maxN, c)
	}
	if ratio := float64(maxN) / mean; ratio <= 1.35 {
		t.Fatalf("a single-replica ring balanced to %.2fx the mean, inside the bound TestRingBalance asserts; that bound no longer proves virtual nodes are doing anything", ratio)
	}
}

// TestRingDeterminism: every instance must derive the identical
// assignment from the same membership view, since nothing hands one out.
// Order of the input members must not matter, and neither must
// concurrency.
func TestRingDeterminism(t *testing.T) {
	keys := connIDs(2000)
	base := members(7)

	shuffled := []string{"adapter-3", "adapter-0", "adapter-6", "adapter-1", "adapter-5", "adapter-4", "adapter-2"}
	dupes := append(slices.Clone(shuffled), "adapter-3", "adapter-0")

	r1 := NewRing(DefaultReplicas, base)
	r2 := NewRing(DefaultReplicas, shuffled)
	r3 := NewRing(DefaultReplicas, dupes)

	for _, k := range keys {
		o1, _ := r1.Owner(k)
		o2, _ := r2.Owner(k)
		o3, _ := r3.Owner(k)
		if o1 != o2 || o1 != o3 {
			t.Fatalf("connection %q owned by %q / %q / %q across equivalent membership views", k, o1, o2, o3)
		}
	}

	// Same again from many goroutines over independently built rings, the
	// way N processes would do it.
	const workers = 8
	results := make([]map[string]string, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = ownership(NewRing(DefaultReplicas, base), keys)
		}(i)
	}
	wg.Wait()
	for i := 1; i < workers; i++ {
		for _, k := range keys {
			if results[i][k] != results[0][k] {
				t.Fatalf("instance %d assigned %q to %q, instance 0 assigned it to %q",
					i, k, results[i][k], results[0][k])
			}
		}
	}
}

// TestRingEmptyAndDegenerate covers the ring shapes the coordinator can
// legitimately hand over.
func TestRingEmptyAndDegenerate(t *testing.T) {
	empty := NewRing(DefaultReplicas, nil)
	if _, ok := empty.Owner("conn-1"); ok {
		t.Error("empty ring reported an owner")
	}
	if empty.Has("adapter-0") {
		t.Error("empty ring claims a member")
	}

	// Blank instance IDs are dropped rather than colliding with each other.
	blanks := NewRing(DefaultReplicas, []string{"", "", "adapter-0"})
	if got := blanks.Members(); len(got) != 1 || got[0] != "adapter-0" {
		t.Errorf("members with blanks = %v, want [adapter-0]", got)
	}

	solo := NewRing(DefaultReplicas, []string{"adapter-0"})
	for _, k := range connIDs(500) {
		if owner, ok := solo.Owner(k); !ok || owner != "adapter-0" {
			t.Fatalf("single-member ring gave %q to %q (ok=%v)", k, owner, ok)
		}
	}

	if got := NewRing(0, members(3)).Replicas(); got != DefaultReplicas {
		t.Errorf("replicas 0 = %d, want the default %d", got, DefaultReplicas)
	}
}
