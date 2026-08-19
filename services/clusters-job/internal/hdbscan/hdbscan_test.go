package hdbscan

import (
	"math/rand"
	"testing"
)

// gaussianBlob draws n points around center with the given sigma.
func gaussianBlob(rng *rand.Rand, n int, center []float64, sigma float64) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, len(center))
		for d := range center {
			v[d] = float32(center[d] + rng.NormFloat64()*sigma)
		}
		out[i] = v
	}
	return out
}

// uniformNoise draws n points uniformly in [lo,hi]^dims.
func uniformNoise(rng *rand.Rand, n, dims int, lo, hi float64) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dims)
		for d := range v {
			v[d] = float32(lo + rng.Float64()*(hi-lo))
		}
		out[i] = v
	}
	return out
}

// TestThreeBlobsWithNoise: three well-separated Gaussian blobs plus
// uniform noise must produce exactly three clusters, with each blob
// homogeneous and the noise mostly labelled -1.
func TestThreeBlobsWithNoise(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	centers := [][]float64{
		{0, 0, 0, 0},
		{15, 0, 0, 0},
		{0, 15, 15, 0},
	}
	var pts [][]float32
	blobRanges := make([][2]int, len(centers))
	for i, c := range centers {
		start := len(pts)
		pts = append(pts, gaussianBlob(rng, 60, c, 0.4)...)
		blobRanges[i] = [2]int{start, len(pts)}
	}
	noiseStart := len(pts)
	pts = append(pts, uniformNoise(rng, 30, 4, -8, 23)...)

	labels := Cluster(pts, Options{MinClusterSize: 15, MinSamples: 5, AllowSingleCluster: true})

	// Exactly three cluster ids among the blob points.
	seen := map[int]bool{}
	for _, r := range blobRanges {
		for i := r[0]; i < r[1]; i++ {
			if labels[i] >= 0 {
				seen[labels[i]] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("blob points span %d clusters, want 3 (labels: %v)", len(seen), labels)
	}

	// Each blob is dominated by a single label, and the three differ.
	dominant := make([]int, len(blobRanges))
	for bi, r := range blobRanges {
		counts := map[int]int{}
		for i := r[0]; i < r[1]; i++ {
			counts[labels[i]]++
		}
		best, bestN := -2, 0
		for l, n := range counts {
			if n > bestN {
				best, bestN = l, n
			}
		}
		if best < 0 {
			t.Fatalf("blob %d dominated by noise: %v", bi, counts)
		}
		if bestN < 54 { // >= 90% of 60
			t.Errorf("blob %d: dominant label %d covers only %d/60 points", bi, best, bestN)
		}
		dominant[bi] = best
	}
	if dominant[0] == dominant[1] || dominant[1] == dominant[2] || dominant[0] == dominant[2] {
		t.Errorf("blobs share labels: %v", dominant)
	}

	// Noise is mostly -1.
	noiseAsNoise := 0
	for i := noiseStart; i < len(pts); i++ {
		if labels[i] == -1 {
			noiseAsNoise++
		}
	}
	if noiseAsNoise < 21 { // >= 70% of 30
		t.Errorf("only %d/30 noise points labelled -1", noiseAsNoise)
	}
}

// TestSingleBlob: one dense blob must come back as one cluster, not zero
// (AllowSingleCluster) and not more.
func TestSingleBlob(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	pts := gaussianBlob(rng, 60, []float64{1, 2, 3, 4}, 0.5)
	labels := Cluster(pts, Options{MinClusterSize: 15, MinSamples: 5, AllowSingleCluster: true})
	clusters := map[int]int{}
	for _, l := range labels {
		if l >= 0 {
			clusters[l]++
		}
	}
	if len(clusters) != 1 {
		t.Fatalf("single blob yields %d clusters, want 1: %v", len(clusters), clusters)
	}
	if clusters[0] < 54 {
		t.Errorf("cluster holds %d/60 points, want >= 54", clusters[0])
	}
}

// TestNoSingleClusterWithoutOptIn: with AllowSingleCluster false, a
// dataset that never splits yields all noise — the reference default.
func TestNoSingleClusterWithoutOptIn(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	pts := gaussianBlob(rng, 40, []float64{0, 0, 0, 0}, 0.5)
	labels := Cluster(pts, Options{MinClusterSize: 30, MinSamples: 5})
	for i, l := range labels {
		if l != -1 {
			t.Fatalf("point %d labelled %d, want all -1", i, l)
		}
	}
}

// TestTinyInputs: pathological sizes must not panic and must label
// everything noise when no cluster can form.
func TestTinyInputs(t *testing.T) {
	opts := Options{MinClusterSize: 15, MinSamples: 5, AllowSingleCluster: true}
	if got := Cluster(nil, opts); len(got) != 0 {
		t.Errorf("empty input: got %v", got)
	}
	if got := Cluster([][]float32{{1, 2}}, opts); len(got) != 1 || got[0] != -1 {
		t.Errorf("single point: got %v, want [-1]", got)
	}
	two := [][]float32{{0, 0}, {1, 1}}
	if got := Cluster(two, opts); got[0] != -1 || got[1] != -1 {
		t.Errorf("two points under min_cluster_size: got %v, want noise", got)
	}
	// min_cluster_size 2 lets a pair cluster.
	if got := Cluster(two, Options{MinClusterSize: 2, MinSamples: 1, AllowSingleCluster: true}); got[0] != 0 || got[1] != 0 {
		t.Errorf("two points with mcs=2: got %v, want [0 0]", got)
	}
	// Identical points: degenerate zero distances must stay finite.
	same := make([][]float32, 20)
	for i := range same {
		same[i] = []float32{3, 3, 3}
	}
	got := Cluster(same, Options{MinClusterSize: 5, MinSamples: 3, AllowSingleCluster: true})
	for i, l := range got {
		if l != 0 {
			t.Fatalf("identical points: point %d labelled %d, want 0", i, l)
		}
	}
}

// TestMinClusterSizeRespected: a blob smaller than min_cluster_size next
// to a bigger one must not surface as its own cluster.
func TestMinClusterSizeRespected(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	big := gaussianBlob(rng, 40, []float64{0, 0}, 0.3)
	small := gaussianBlob(rng, 5, []float64{20, 20}, 0.3)
	pts := append(append([][]float32{}, big...), small...)
	labels := Cluster(pts, Options{MinClusterSize: 15, MinSamples: 5, AllowSingleCluster: true})
	for i := 40; i < 45; i++ {
		if labels[i] != -1 {
			// The small blob may only ever be noise; it cannot be a cluster
			// of its own, and it is too far away to join the big one.
			t.Errorf("small-blob point %d labelled %d, want -1", i, labels[i])
		}
	}
}

// TestDeterminism: the same input yields byte-identical labels across runs.
func TestDeterminism(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	var pts [][]float32
	pts = append(pts, gaussianBlob(rng, 50, []float64{0, 0, 0}, 0.5)...)
	pts = append(pts, gaussianBlob(rng, 50, []float64{12, 0, 0}, 0.5)...)
	pts = append(pts, uniformNoise(rng, 20, 3, -6, 18)...)
	opts := Options{MinClusterSize: 10, MinSamples: 5, AllowSingleCluster: true}
	first := Cluster(pts, opts)
	for run := 0; run < 3; run++ {
		again := Cluster(pts, opts)
		for i := range first {
			if first[i] != again[i] {
				t.Fatalf("run %d: label[%d] = %d, first run had %d", run, i, again[i], first[i])
			}
		}
	}
}
