package main

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

func cosine(t *testing.T, a, b []float32) float64 {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("dimension mismatch: %d vs %d", len(a), len(b))
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func norm(v []float32) float64 {
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	return math.Sqrt(n)
}

// The whole point of the marker is that the cosine to the centroid is the
// number the caller asked for, not merely "closer than before".
func TestSimilarityMarkerHitsTheRequestedCosine(t *testing.T) {
	centroid := embed("[[sim:topic-a]]")
	if math.Abs(norm(centroid)-1) > 1e-4 {
		t.Fatalf("centroid norm = %f, want ~1", norm(centroid))
	}

	for _, want := range []float64{0.99, 0.97, 0.95, 0.8, 0.5, 0, -0.5, -1} {
		text := fmt.Sprintf("[[sim:topic-a:%g:v1]]", want)
		got := cosine(t, embed(text), centroid)
		if math.Abs(got-want) > 1e-4 {
			t.Errorf("cos(%q, centroid) = %f, want %f", text, got, want)
		}
		if n := norm(embed(text)); math.Abs(n-1) > 1e-4 {
			t.Errorf("%q: norm = %f, want ~1", text, n)
		}
	}
}

// A pair of differently-worded texts in one cluster must land above the §7.4
// threshold of 0.95, which is the property the semantic-spam e2e depends on.
// The documented approximation is cos(a,b) ~= cos_a * cos_b.
func TestSameClusterPairIsAboveTheSemanticThreshold(t *testing.T) {
	const c = 0.99
	a := embed("free followers over at my channel [[sim:promo:0.99:a]]")
	b := embed("come grab free follows on my page [[sim:promo:0.99:b]]")

	got := cosine(t, a, b)
	if want := c * c; math.Abs(got-want) > 0.01 {
		t.Errorf("pairwise cosine = %f, want ~%f (cos_a*cos_b)", got, want)
	}
	if got <= 0.95 {
		t.Errorf("pairwise cosine = %f, must exceed the §7.4 threshold of 0.95", got)
	}
	if got >= 0.9999 {
		t.Errorf("pairwise cosine = %f: the two vectors are effectively identical, "+
			"which would prove nothing the identical-duplicate detector does not", got)
	}
}

// Different clusters must stay near-orthogonal, so a test can assert a
// negative (a genuinely dissimilar message is not flagged) and so batch
// clustering has separable structure to find.
func TestDifferentClustersAreNearOrthogonal(t *testing.T) {
	a := embed("[[sim:topic-a:0.97:x]]")
	b := embed("[[sim:topic-b:0.97:x]]")
	if got := cosine(t, a, b); math.Abs(got) > 0.2 {
		t.Errorf("cross-cluster cosine = %f, want ~0", got)
	}
}

// A cluster at 0.97 must be tight enough to be one cluster and spread enough
// not to be one point — the shape §8.6's HDBSCAN pass is given.
func TestClusterMembersAreTightButDistinct(t *testing.T) {
	var members [][]float32
	for i := range 12 {
		members = append(members, embed(fmt.Sprintf("chatter number %d [[sim:tight:0.97:m%d]]", i, i)))
	}
	minCos, maxCos := 1.0, -1.0
	for i := range members {
		for j := i + 1; j < len(members); j++ {
			c := cosine(t, members[i], members[j])
			minCos = math.Min(minCos, c)
			maxCos = math.Max(maxCos, c)
		}
	}
	// 0.97*0.97 = 0.9409, with the residual product contributing ~1/sqrt(384).
	if minCos < 0.93 || maxCos > 0.95 {
		t.Errorf("pairwise cosines span [%f, %f], want all near 0.9409", minCos, maxCos)
	}
}

func TestSimilarityIsDeterministic(t *testing.T) {
	const text = "same text [[sim:topic-a:0.97:v]]"
	a, b := embed(text), embed(text)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("marked embedding is not deterministic at index %d", i)
		}
	}
}

// Two texts sharing a cluster and a variant but differing elsewhere embed
// identically: the variant, when given, is the only thing that steers the
// residual. That is what lets a caller pin a vector independently of wording.
func TestExplicitVariantOverridesWording(t *testing.T) {
	a := embed("one wording [[sim:topic-a:0.9:fixed]]")
	b := embed("a completely different wording [[sim:topic-a:0.9:fixed]]")
	if got := cosine(t, a, b); math.Abs(got-1) > 1e-4 {
		t.Errorf("cosine = %f, want 1: an explicit variant pins the vector", got)
	}
}

// Without a variant the whole text is the variant, so distinct wordings do
// not collapse onto one vector.
func TestMissingVariantFallsBackToTheText(t *testing.T) {
	a := embed("one wording [[sim:topic-a:0.9]]")
	b := embed("a completely different wording [[sim:topic-a:0.9]]")
	if got := cosine(t, a, b); got > 0.9 {
		t.Errorf("cosine = %f, want ~0.81: the wording should steer the residual", got)
	}
}

// The default path must be untouched, byte for byte — test/e2e and the load
// harness both depend on the existing vectors.
func TestUnmarkedTextIsUnchanged(t *testing.T) {
	for _, text := range []string{
		"great stream today, thanks for having us",
		"same message sent twice in a row",
		"",
		"a [[sim marker that is not one",
		"[[sim:]] empty cluster is not a marker",
		"[[sim:topic-a:not-a-number]] bad cosine is not a marker",
		"[[sim:topic-a:2]] out-of-range cosine is not a marker",
	} {
		want := hashEmbed(text)
		got := embed(text)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("embed(%q) diverged from hashEmbed at index %d", text, i)
				break
			}
		}
	}
}

func TestParseSim(t *testing.T) {
	cases := []struct {
		text    string
		ok      bool
		path    []string
		cosines []float64
		variant string
	}{
		{"[[sim:promo]]", true, []string{"promo"}, []float64{1}, "[[sim:promo]]"},
		{"hello [[sim:promo:0.97]] there", true, []string{"promo"}, []float64{0.97}, "hello [[sim:promo:0.97]] there"},
		{"[[sim:promo:0.97:v3]]", true, []string{"promo"}, []float64{0.97}, "v3"},
		{"[[sim:promo::v3]]", true, []string{"promo"}, []float64{1}, "v3"},
		{"[[sim:promo:-1:v]]", true, []string{"promo"}, []float64{-1}, "v"},
		{"[[sim:a/b:0.9/0.99:v]]", true, []string{"a", "b"}, []float64{0.9, 0.99}, "v"},
		{"[[sim:a/b::v]]", true, []string{"a", "b"}, []float64{1, 1}, "v"},
		{"nothing here", false, nil, nil, ""},
		{"[[sim:]]", false, nil, nil, ""},
		{"[[sim:promo:1.5]]", false, nil, nil, ""},
		{"[[sim:promo:abc]]", false, nil, nil, ""},
		// A cosine list that does not match the path depth is a typo, not a
		// marker: guessing which level it meant would embed silently wrong.
		{"[[sim:a/b:0.9:v]]", false, nil, nil, ""},
		{"[[sim:a:0.9/0.99:v]]", false, nil, nil, ""},
		{"[[sim:a//b:0.9/0.9/0.9:v]]", false, nil, nil, ""},
	}
	for _, c := range cases {
		spec, ok := parseSim(c.text)
		if ok != c.ok {
			t.Errorf("parseSim(%q) ok = %v, want %v", c.text, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if !slices.Equal(spec.path, c.path) || !slices.Equal(spec.cosines, c.cosines) ||
			spec.variant != c.variant {
			t.Errorf("parseSim(%q) = %+v, want {path:%v cosines:%v variant:%s}",
				c.text, spec, c.path, c.cosines, c.variant)
		}
	}
}

// A hierarchical marker must produce a cluster that is simultaneously one
// topic and two themes: §8.6 runs HDBSCAN coarse to find the topic, then again
// within it to find themes, and a fixture with no sub-structure proves only
// the first pass.
func TestHierarchyIsOneTopicAndTwoThemes(t *testing.T) {
	member := func(theme string, i int) []float32 {
		return embed(fmt.Sprintf("message %d [[sim:tickets/%s:0.90/0.99:m%d]]", i, theme, i))
	}
	var s1, s2 [][]float32
	for i := range 8 {
		s1 = append(s1, member("resale", i))
		s2 = append(s2, member("queue", i))
	}

	within, across := 1.0, 0.0
	for i := range s1 {
		for j := i + 1; j < len(s1); j++ {
			within = math.Min(within, cosine(t, s1[i], s1[j]))
			within = math.Min(within, cosine(t, s2[i], s2[j]))
		}
	}
	for i := range s1 {
		for j := range s2 {
			across = math.Max(across, cosine(t, s1[i], s2[j]))
		}
	}
	// 0.99^2 = 0.9801 within a theme; 0.99^2 * 0.90^2 = 0.7939 across themes.
	if within < 0.97 {
		t.Errorf("within-theme cosine floor = %f, want >= ~0.98", within)
	}
	if across > 0.85 {
		t.Errorf("across-theme cosine ceiling = %f, want ~0.79", across)
	}
	if within <= across {
		t.Fatalf("themes are not separable: within %f <= across %f", within, across)
	}
	// The whole thing must still be far from a different topic.
	other := embed("[[sim:merch/hoodies:0.90/0.99:m0]]")
	if got := cosine(t, s1[0], other); math.Abs(got) > 0.2 {
		t.Errorf("cross-topic cosine = %f, want ~0", got)
	}
}

// A theme's centroid is fixed by the path, not by the wording, so the level
// really is shared.
func TestThemeCentroidIsSharedAcrossVariants(t *testing.T) {
	a := embed("[[sim:tickets/resale:0.9/1:x]]")
	b := embed("[[sim:tickets/resale:0.9/1:y]]")
	if got := cosine(t, a, b); math.Abs(got-1) > 1e-4 {
		t.Errorf("cosine = %f, want 1: cos=1 at the leaf lands on the theme centroid", got)
	}
	c := embed("[[sim:tickets/queue:0.9/1:x]]")
	if got := cosine(t, a, c); math.Abs(got-0.81) > 0.05 {
		t.Errorf("cosine between two theme centroids = %f, want ~0.81 (0.9*0.9)", got)
	}
}

func TestLedgerCountsPerText(t *testing.T) {
	l := newLedger()
	l.record([]string{"a", "b", "a"})
	l.record([]string{"a"})
	if got := l.count("a"); got != 3 {
		t.Errorf("count(a) = %d, want 3", got)
	}
	if got := l.count("b"); got != 1 {
		t.Errorf("count(b) = %d, want 1", got)
	}
	if got := l.count("missing"); got != 0 {
		t.Errorf("count(missing) = %d, want 0", got)
	}
	snap := l.snapshot()
	if snap["requests"] != 2 || snap["texts"] != 4 || snap["distinct"] != 2 {
		t.Errorf("snapshot = %v, want requests=2 texts=4 distinct=2", snap)
	}
	l.reset()
	if got := l.count("a"); got != 0 {
		t.Errorf("count(a) after reset = %d, want 0", got)
	}
}

// The ledger is bounded: a load run must not turn it into a memory leak.
func TestLedgerStopsAdmittingWhenFull(t *testing.T) {
	l := newLedger()
	for i := range maxTrackedTexts + 100 {
		l.record([]string{fmt.Sprintf("text-%d", i)})
	}
	snap := l.snapshot()
	if snap["distinct"] != maxTrackedTexts {
		t.Errorf("distinct = %d, want the cap %d", snap["distinct"], maxTrackedTexts)
	}
	if snap["dropped"] != 100 {
		t.Errorf("dropped = %d, want 100", snap["dropped"])
	}
	if snap["texts"] != maxTrackedTexts+100 {
		t.Errorf("texts = %d, want every text counted in the total", snap["texts"])
	}
	// An already-admitted key keeps counting after the cap is reached.
	l.record([]string{"text-0"})
	if got := l.count("text-0"); got != 2 {
		t.Errorf("count(text-0) = %d, want 2", got)
	}
}
