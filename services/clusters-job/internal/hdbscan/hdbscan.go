// Package hdbscan is a pure-Go implementation of the HDBSCAN* clustering
// algorithm (Campello, Moulavi, Sander), used by clusters-job for batch
// topic discovery (docs §8.6). HDBSCAN is chosen because it does not
// require k up front and treats sparse points as noise rather than forcing
// them into a cluster — both properties are preserved faithfully here.
//
// The implementation follows the reference algorithm:
//
//  1. Core distances: for each point, the distance to its
//     min_samples-th nearest neighbour (the point itself excluded).
//  2. Mutual reachability distance:
//     d_mreach(a,b) = max(core(a), core(b), d(a,b)).
//  3. Minimum spanning tree of the complete mutual-reachability graph,
//     built with Prim's algorithm (O(n²) distance evaluations, no
//     materialised distance matrix — the sizes clusters-job feeds are
//     bounded by its point cap).
//  4. Single-linkage hierarchy from the sorted MST edges (union-find).
//  5. Condensed tree with min_cluster_size: at each split, a side with
//     fewer than min_cluster_size points "falls out" of its cluster as
//     individual points at λ = 1/distance; a split into two large sides
//     births two new clusters.
//  6. Stability-based excess-of-mass extraction: a cluster is selected
//     when its stability meets or exceeds the summed stability of its
//     selected descendants.
//  7. Points not covered by any selected cluster are noise, labelled -1.
//
// Documented deviations from the reference implementation (both faithful
// simplifications, neither affects the no-k-required or noise-stays-noise
// properties):
//
//   - AllowSingleCluster: the reference excludes the hierarchy root from
//     selection by default, so a dataset that never splits into two
//     min_cluster_size children yields all noise. With AllowSingleCluster
//     the root competes on stability like any other cluster (equivalent to
//     the reference's allow_single_cluster=True), which is what
//     clusters-job wants: one big conversation is one topic, not nothing.
//     When the root is selected, a per-point λ threshold keeps noise as
//     noise: a point attached directly to the root is a member only when
//     its departure λ is within rootMembershipRatio of the root's densest
//     scale (its maximum entry λ) — points an order of magnitude sparser
//     than the cluster core (far-away sub-min_cluster_size clumps, uniform
//     background) stay -1. The reference applies a per-point λ threshold
//     in this situation too, with a different (stricter) cutoff.
//   - Zero distances are clamped: λ = 1/d with d < 1e-12 uses λ = 1e12,
//     so datasets containing duplicate points stay finite.
//
// Determinism: given the same points in the same order, the output is
// identical — ties in Prim's and in edge sorting break on point index.
package hdbscan

import (
	"math"
	"sort"
)

// Options tunes the algorithm. Zero values fall back to the documented
// defaults (docs §8.6: min_cluster_size 15, min_samples 5, both
// env-tunable by the caller).
type Options struct {
	// MinClusterSize is the smallest point count a cluster may have.
	// Values below 2 are treated as 2.
	MinClusterSize int
	// MinSamples is k for the core distance. Values below 1 are treated
	// as 1; values above n-1 are clamped to n-1.
	MinSamples int
	// AllowSingleCluster lets the hierarchy root be selected when its
	// stability beats its children's (or when it never splits), so a
	// dataset that is one dense blob yields one cluster instead of none.
	AllowSingleCluster bool
}

// DefaultMinClusterSize and DefaultMinSamples are the §8.6 defaults.
const (
	DefaultMinClusterSize = 15
	DefaultMinSamples     = 5
)

// maxLambda caps λ = 1/d for near-zero distances.
const maxLambda = 1e12

// rootMembershipRatio is the λ fraction of the root's densest scale below
// which a directly-root-attached point is noise when the root itself is
// the selected cluster (see the package comment on AllowSingleCluster).
const rootMembershipRatio = 0.1

// Cluster labels every point: 0..k-1 for cluster membership, -1 for
// noise. All vectors must share the same dimensionality. Distances are
// Euclidean; callers clustering by cosine should L2-normalise first
// (Euclidean order on the unit sphere is cosine order).
func Cluster(points [][]float32, opt Options) []int {
	n := len(points)
	labels := make([]int, n)
	for i := range labels {
		labels[i] = -1
	}
	mcs := opt.MinClusterSize
	if mcs == 0 {
		mcs = DefaultMinClusterSize
	}
	if mcs < 2 {
		mcs = 2
	}
	ms := opt.MinSamples
	if ms == 0 {
		ms = DefaultMinSamples
	}
	if ms < 1 {
		ms = 1
	}
	// Fewer points than min_cluster_size can never form a cluster.
	if n < mcs || n < 2 {
		return labels
	}
	if ms > n-1 {
		ms = n - 1
	}

	core := coreDistances(points, ms)
	edges := mstEdges(points, core)
	merges := buildHierarchy(n, edges)
	clusters := condense(n, merges, mcs)
	selected := extract(clusters, opt.AllowSingleCluster)
	assign(clusters, selected, labels)
	return labels
}

func euclidean(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		s += d * d
	}
	return math.Sqrt(s)
}

// coreDistances returns, per point, the distance to its k-th nearest
// neighbour (self excluded). k is small, so a sorted running window
// beats a heap.
func coreDistances(pts [][]float32, k int) []float64 {
	n := len(pts)
	core := make([]float64, n)
	nearest := make([]float64, k)
	for i := 0; i < n; i++ {
		count := 0
		for j := 0; j < n; j++ {
			if j == i {
				continue
			}
			d := euclidean(pts[i], pts[j])
			switch {
			case count < k:
				nearest[count] = d
				count++
				if count == k {
					sort.Float64s(nearest[:k])
				}
			case d < nearest[k-1]:
				pos := sort.SearchFloat64s(nearest[:k], d)
				copy(nearest[pos+1:k], nearest[pos:k-1])
				nearest[pos] = d
			}
		}
		core[i] = nearest[k-1]
	}
	return core
}

type edge struct {
	u, v int
	w    float64
}

// mstEdges runs Prim's algorithm over the implicit complete graph of
// mutual reachability distances. Ties select the lowest index, keeping
// the tree deterministic.
func mstEdges(pts [][]float32, core []float64) []edge {
	n := len(pts)
	inTree := make([]bool, n)
	minW := make([]float64, n)
	minFrom := make([]int, n)
	for i := range minW {
		minW[i] = math.Inf(1)
		minFrom[i] = -1
	}
	inTree[0] = true
	cur := 0
	edges := make([]edge, 0, n-1)
	for len(edges) < n-1 {
		for j := 0; j < n; j++ {
			if inTree[j] {
				continue
			}
			d := euclidean(pts[cur], pts[j])
			if core[cur] > d {
				d = core[cur]
			}
			if core[j] > d {
				d = core[j]
			}
			if d < minW[j] {
				minW[j] = d
				minFrom[j] = cur
			}
		}
		best := -1
		for j := 0; j < n; j++ {
			if inTree[j] {
				continue
			}
			if best == -1 || minW[j] < minW[best] {
				best = j
			}
		}
		edges = append(edges, edge{u: minFrom[best], v: best, w: minW[best]})
		inTree[best] = true
		cur = best
	}
	return edges
}

// merge is one internal node of the single-linkage dendrogram. Hierarchy
// node ids: 0..n-1 are points, n+i is merges[i]. The last merge is the
// root.
type merge struct {
	left, right int // hierarchy node ids
	dist        float64
	size        int // points under this node
}

// buildHierarchy sorts the MST edges ascending (ties on endpoint index)
// and merges via union-find into a dendrogram.
func buildHierarchy(n int, edges []edge) []merge {
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].w != edges[j].w {
			return edges[i].w < edges[j].w
		}
		if edges[i].u != edges[j].u {
			return edges[i].u < edges[j].u
		}
		return edges[i].v < edges[j].v
	})
	parent := make([]int, 2*n-1)
	for i := range parent {
		parent[i] = i
	}
	var find func(x int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	merges := make([]merge, 0, n-1)
	sizeOf := func(node int) int {
		if node < n {
			return 1
		}
		return merges[node-n].size
	}
	for _, e := range edges {
		ra, rb := find(e.u), find(e.v)
		node := n + len(merges)
		merges = append(merges, merge{left: ra, right: rb, dist: e.w, size: sizeOf(ra) + sizeOf(rb)})
		parent[ra] = node
		parent[rb] = node
	}
	return merges
}

// condCluster is one node of the condensed tree.
type condCluster struct {
	parent   int // condensed cluster id, -1 for root
	birth    float64
	size     int   // points under the cluster at birth
	children []int // condensed cluster ids, birth λ is their birth field
	points   []int // point indices fallen out of this cluster
	pointL   []float64
	// filled by extract:
	stability float64
}

func lambdaOf(d float64) float64 {
	if d < 1e-12 {
		return maxLambda
	}
	return 1 / d
}

// condense walks the dendrogram top-down producing the condensed tree.
// Cluster 0 is the root (birth λ 0).
func condense(n int, merges []merge, mcs int) []condCluster {
	sizeOf := func(node int) int {
		if node < n {
			return 1
		}
		return merges[node-n].size
	}
	clusters := []condCluster{{parent: -1, birth: 0, size: n}}

	// fall records every point under hierarchy node as leaving cluster cl
	// at λ.
	fall := func(node, cl int, lam float64) {
		stack := []int{node}
		for len(stack) > 0 {
			t := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if t < n {
				clusters[cl].points = append(clusters[cl].points, t)
				clusters[cl].pointL = append(clusters[cl].pointL, lam)
				continue
			}
			m := merges[t-n]
			stack = append(stack, m.right, m.left)
		}
	}

	type frame struct{ node, cl int }
	stack := []frame{{node: n + len(merges) - 1, cl: 0}}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if f.node < n {
			// Only reachable when a child of size >= mcs is a leaf, which
			// needs mcs <= 1 — excluded by the clamp. Kept as a guard.
			fall(f.node, f.cl, maxLambda)
			continue
		}
		m := merges[f.node-n]
		lam := lambdaOf(m.dist)
		ls, rs := sizeOf(m.left), sizeOf(m.right)
		switch {
		case ls >= mcs && rs >= mcs:
			cl := len(clusters)
			clusters = append(clusters, condCluster{parent: f.cl, birth: lam, size: ls})
			cr := len(clusters)
			clusters = append(clusters, condCluster{parent: f.cl, birth: lam, size: rs})
			clusters[f.cl].children = append(clusters[f.cl].children, cl, cr)
			// Push right first so the left branch is processed (and its
			// clusters numbered) first — deterministic ids.
			stack = append(stack, frame{node: m.right, cl: cr}, frame{node: m.left, cl: cl})
		case ls >= mcs:
			fall(m.right, f.cl, lam)
			stack = append(stack, frame{node: m.left, cl: f.cl})
		case rs >= mcs:
			fall(m.left, f.cl, lam)
			stack = append(stack, frame{node: m.right, cl: f.cl})
		default:
			fall(m.left, f.cl, lam)
			fall(m.right, f.cl, lam)
		}
	}
	return clusters
}

// extract computes stabilities and selects clusters by excess of mass.
// Children always carry larger ids than their parent, so a reverse pass
// is leaves-first. Selection conflicts (an ancestor and a descendant both
// flagged) are resolved during assignment: the shallowest selected
// cluster on a path wins.
func extract(clusters []condCluster, allowSingle bool) []bool {
	for ci := range clusters {
		c := &clusters[ci]
		s := 0.0
		for _, l := range c.pointL {
			s += l - c.birth
		}
		for _, ch := range c.children {
			s += (clusters[ch].birth - c.birth) * float64(clusters[ch].size)
		}
		c.stability = s
	}
	selected := make([]bool, len(clusters))
	subtree := make([]float64, len(clusters))
	for ci := len(clusters) - 1; ci >= 0; ci-- {
		c := &clusters[ci]
		if len(c.children) == 0 {
			if ci != 0 || allowSingle {
				selected[ci] = true
			}
			subtree[ci] = c.stability
			continue
		}
		childSum := 0.0
		for _, ch := range c.children {
			childSum += subtree[ch]
		}
		if ci == 0 && !allowSingle {
			subtree[ci] = childSum
			continue
		}
		if c.stability >= childSum {
			selected[ci] = true
			subtree[ci] = c.stability
		} else {
			subtree[ci] = childSum
		}
	}
	return selected
}

// assign labels points by walking the condensed tree from the root: the
// first selected cluster on a path claims its whole subtree; points
// attached above every selected cluster stay -1.
//
// When the selected cluster is the root itself (AllowSingleCluster), its
// directly-attached points additionally pass a λ threshold: a point is a
// member only when its departure λ reaches rootMembershipRatio of the
// root's maximum entry λ, so structure an order of magnitude sparser than
// the cluster's densest scale stays noise. Subtrees under selected
// descendants are unaffected.
func assign(clusters []condCluster, selected []bool, labels []int) {
	rootMax := math.Inf(-1)
	for _, l := range clusters[0].pointL {
		rootMax = math.Max(rootMax, l)
	}
	for _, ch := range clusters[0].children {
		rootMax = math.Max(rootMax, clusters[ch].birth)
	}
	rootCut := rootMax * rootMembershipRatio

	next := 0
	type frame struct{ cl, win int }
	stack := []frame{{cl: 0, win: -1}}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		win := f.win
		if win == -1 && selected[f.cl] {
			win = next
			next++
		}
		if win != -1 {
			for k, p := range clusters[f.cl].points {
				if f.cl == 0 && clusters[0].pointL[k] < rootCut {
					continue // far below the root's density scale: noise
				}
				labels[p] = win
			}
		}
		ch := clusters[f.cl].children
		// Reverse push keeps pre-order (left first) for deterministic
		// label numbering.
		for i := len(ch) - 1; i >= 0; i-- {
			stack = append(stack, frame{cl: ch[i], win: win})
		}
	}
}
