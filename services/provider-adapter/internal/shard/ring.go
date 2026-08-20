package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"strconv"
)

// DefaultReplicas is the number of virtual nodes each instance places on
// the ring.
//
// 160 is libketama's number and it is chosen for the same reason here.
// Virtual nodes are what make the ring *fair*: with one point per member
// the arc lengths are exponentially distributed, so the unluckiest of ten
// instances routinely draws three times the mean. Load spread scales as
// roughly 1/sqrt(replicas), so 160 points per member lands the per-member
// share within about ±10 % of the mean for realistic member counts, which
// is well inside the factor the tests assert.
//
// Raising it further buys diminishing fairness and costs ring build time
// linearly (replicas × members SHA-256s per rebalance) plus a log(n)
// widening of every lookup. 160 is the knee.
const DefaultReplicas = 160

// Ring is an immutable consistent-hash ring over a set of instance IDs.
//
// The whole point of the ring — over, say, hash(connection) % len(members)
// — is minimal disruption: adding or removing one member moves only the
// keys that fall in that member's own arcs. Every other key keeps its
// owner, which for the adapter means it keeps its live socket. Modulo
// hashing would reshuffle nearly everything on every deploy and drop every
// stream in the system, which N2 forbids.
//
// A Ring is safe for concurrent use because it is never mutated after
// NewRing returns; membership changes build a new one.
type Ring struct {
	members  []string
	replicas int

	// hashes is sorted ascending; owners is parallel to it.
	hashes []uint64
	owners []string
}

// NewRing builds a ring placing replicas virtual nodes per member.
// Members are de-duplicated and sorted, so two instances handed the same
// membership view build byte-identical rings — the determinism the whole
// scheme rests on, since there is no authority that hands out
// assignments: every instance derives the same one independently.
//
// replicas <= 0 means DefaultReplicas. An empty member list is legal and
// yields a ring that owns nothing (see Owner).
func NewRing(replicas int, members []string) *Ring {
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	uniq := slices.Clone(members)
	slices.Sort(uniq)
	uniq = slices.Compact(uniq)
	// Drop empties: an unnamed instance would collide with every other
	// unnamed instance and silently steal their segments.
	uniq = slices.DeleteFunc(uniq, func(s string) bool { return s == "" })

	r := &Ring{members: uniq, replicas: replicas}
	if len(uniq) == 0 {
		return r
	}

	type point struct {
		hash  uint64
		owner string
	}
	points := make([]point, 0, len(uniq)*replicas)
	var buf []byte
	for _, m := range uniq {
		for i := range replicas {
			buf = append(buf[:0], m...)
			buf = append(buf, 0)
			buf = strconv.AppendInt(buf, int64(i), 10)
			points = append(points, point{hash: hash64(buf), owner: m})
		}
	}
	// Sort by hash, then by owner: a 64-bit collision between two members'
	// virtual nodes is vanishingly unlikely but would otherwise leave the
	// order dependent on the sort's stability, and every instance must
	// agree on it exactly.
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		return points[i].owner < points[j].owner
	})

	r.hashes = make([]uint64, len(points))
	r.owners = make([]string, len(points))
	for i, p := range points {
		r.hashes[i] = p.hash
		r.owners[i] = p.owner
	}
	return r
}

// Owner returns the instance that owns key, and false if the ring is
// empty. It is the first virtual node clockwise of hash(key), wrapping at
// the top of the ring.
func (r *Ring) Owner(key string) (string, bool) {
	if len(r.hashes) == 0 {
		return "", false
	}
	h := hash64([]byte(key))
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.owners[i], true
}

// Owns reports whether self owns key.
func (r *Ring) Owns(self, key string) bool {
	owner, ok := r.Owner(key)
	return ok && owner == self
}

// Members returns the sorted, de-duplicated membership the ring was built
// from. The returned slice must not be modified.
func (r *Ring) Members() []string { return r.members }

// Replicas returns the virtual nodes per member.
func (r *Ring) Replicas() int { return r.replicas }

// Has reports whether self is a member of this ring.
func (r *Ring) Has(self string) bool {
	_, found := slices.BinarySearch(r.members, self)
	return found
}

// hash64 is the ring's hash function: the first 8 bytes of SHA-256, big
// endian.
//
// It must be stable across processes, releases, and architectures,
// because every adapter instance hashes independently and they must
// agree. That rules out hash/maphash (per-process seed) and makes FNV a
// poor choice (weak avalanche on the short, highly-similar keys virtual
// nodes generate — "instance-a\x000", "instance-a\x001", ... — which
// clumps arcs and defeats the point of virtual nodes). SHA-256 costs a
// few hundred nanoseconds and is only paid on rebalance.
func hash64(b []byte) uint64 {
	sum := sha256.Sum256(b)
	return binary.BigEndian.Uint64(sum[:8])
}
