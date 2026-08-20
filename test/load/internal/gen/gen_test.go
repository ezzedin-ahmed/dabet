package gen

import (
	"math"
	"strings"
	"testing"
	"time"

	"dabet/pkg/contracts"
)

// The partition key must be byte-identical to what production computes,
// or the run measures a different partition assignment from the one the
// system is designed around (§4.2, §7.3).
func TestKeyMatchesContracts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CreatorID = "9d4e0000-0000-0000-0000-000000000000"
	cfg.RunID = "t1"
	g := NewGenerator(cfg, 0)
	for range 500 {
		rec := g.Next(time.Now())
		want := contracts.MessagesKey(rec.Msg.AuthorID, rec.Msg.ContentID)
		if string(rec.Key) != string(want) {
			t.Fatalf("key for (%s,%s) = %q, want %q",
				rec.Msg.AuthorID, rec.Msg.ContentID, rec.Key, want)
		}
	}
}

// Opaque ids must stay inside the 64-character cap of §4.2 even at the
// far end of the population.
func TestOpaqueIDsAreBounded(t *testing.T) {
	for _, id := range []string{
		ContentID(0), ContentID(1 << 20),
		AuthorID(1<<20, 1<<20),
		MintMessageID("abc123", 1<<10, 1<<40, time.Now()),
	} {
		if len(id) > 64 {
			t.Errorf("id %q is %d chars, over the 64-char cap", id, len(id))
		}
	}
}

// The message_id round-trips the ideal-clock send time, which is what
// lets the verdict tailer compute the SLI from flagged.v1 (which
// carries no ingested_at).
func TestMessageIDRoundTrip(t *testing.T) {
	want := time.Unix(0, 1_700_000_000_123_456_789)
	id := MintMessageID("run42", 3, 99, want)
	got, ok := DecodeIntendedSend(id)
	if !ok {
		t.Fatalf("DecodeIntendedSend(%q) failed", id)
	}
	if !got.Equal(want) {
		t.Fatalf("round trip = %v, want %v", got, want)
	}
	if !strings.HasPrefix(id, RunPrefix("run42")) {
		t.Fatalf("id %q does not carry the run prefix %q", id, RunPrefix("run42"))
	}
	for _, bad := range []string{"", "ytc_01J8XQ7K2M4N", "ldrun42", "ldrun42-zzz", "ldrun42-0-1"} {
		if _, ok := DecodeIntendedSend(bad); ok {
			t.Errorf("DecodeIntendedSend(%q) unexpectedly succeeded", bad)
		}
	}
}

// A foreign message id must not be mistaken for one of ours: the tailer
// filters on the prefix so a previous run's verdicts, still on the
// topic under 7-day retention (§4.8), cannot contaminate the numbers.
func TestRunPrefixIsolatesRuns(t *testing.T) {
	a := MintMessageID("aaa", 0, 1, time.Now())
	if strings.HasPrefix(a, RunPrefix("bbb")) {
		t.Fatalf("id %q matched another run's prefix", a)
	}
}

// The whole point of the population is that it is hot-spotted (N6). The
// empirical draw must track the analytic weights, or the run is
// measuring a uniform distribution and hiding the failure mode.
func TestZipfMatchesAnalyticWeights(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Contents = 200
	cfg.Skew = 1.1
	cfg.RunID = "z"
	g := NewGenerator(cfg, 0)

	const n = 400_000
	counts := make([]float64, cfg.Contents)
	index := map[string]int{}
	for i := range cfg.Contents {
		index[ContentID(i)] = i
	}
	for range n {
		counts[index[g.Next(time.Now()).Msg.ContentID]]++
	}

	z := NewZipf(cfg.Contents, cfg.Skew)
	// The top rank carries the most mass and is the one the hot-spot
	// story rests on; check it tightly and the head of the tail loosely.
	for _, rank := range []int{0, 1, 2, 5, 20} {
		want := z.Weight(rank)
		got := counts[rank] / n
		if math.Abs(got-want) > 0.15*want+0.002 {
			t.Errorf("rank %d share = %.4f, want ~%.4f", rank, got, want)
		}
	}
	// And the shape must be monotone in the head.
	for i := range 20 {
		if counts[i] < counts[i+1]*0.7 {
			t.Errorf("rank %d (%v) is not meaningfully busier than rank %d (%v)",
				i, counts[i], i+1, counts[i+1])
		}
	}
	// Top decile should dominate: that is what "heavily hot-spotted"
	// means and what makes the skew scenario worth running.
	head := 0.0
	for i := range cfg.Contents / 10 {
		head += counts[i]
	}
	if share := head / n; share < 0.5 {
		t.Errorf("top decile carries %.1f%% of traffic; expected a hot-spotted population", 100*share)
	}
}

// Explicit weights pin individual contents to exact traffic shares,
// which is how the sampler scenario reproduces the §7.5 coverage table.
func TestExplicitWeights(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Weights = []float64{0.02, 0.33, 1.67, 100}
	cfg.AuthorsPer = []int{4, 8, 32, 512}
	cfg.RunID = "w"
	g := NewGenerator(cfg, 0)

	const n = 200_000
	counts := map[string]float64{}
	for range n {
		counts[g.Next(time.Now()).Msg.ContentID]++
	}
	total := 0.0
	for _, w := range cfg.Weights {
		total += w
	}
	for i, w := range cfg.Weights {
		want := w / total
		got := counts[ContentID(i)] / n
		if math.Abs(got-want) > 0.1*want+0.0005 {
			t.Errorf("content %d share = %.5f, want ~%.5f", i, got, want)
		}
	}
}

// A hot content with few senders must actually produce few distinct
// keys: that concentration is the pathology the hot-spot scenario
// hunts, and it only exists because the key is hash(author, content).
func TestAuthorsPerBoundsDistinctKeys(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Weights = []float64{1}
	cfg.AuthorsPer = []int{4}
	cfg.RunID = "k"
	g := NewGenerator(cfg, 0)
	keys := map[string]bool{}
	for range 20_000 {
		keys[string(g.Next(time.Now()).Key)] = true
	}
	if len(keys) != 4 {
		t.Fatalf("distinct partition keys = %d, want 4 (one per sender)", len(keys))
	}
}

// The mix must produce the requested proportions, since every scenario
// chooses which cascade stage it drives by choosing the mix.
func TestMixProportions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Contents = 50
	cfg.Mix = Mix{Duplicate: 0.10, RateBurst: 0.05, RestrictedWord: 0.15, LLMFlag: 0.20}
	cfg.RateBurstLen = 4
	cfg.RunID = "m"
	g := NewGenerator(cfg, 0)

	const n = 200_000
	counts := map[Category]float64{}
	for range n {
		counts[g.Next(time.Now()).Category]++
	}
	// rate_burst expands: each selection emits RateBurstLen messages,
	// so its share is scaled by the burst length. Everything else is
	// diluted by the same factor.
	if got := counts[CatRateBurst] / n; got < 0.10 || got > 0.30 {
		t.Errorf("rate_burst share = %.3f; expected the burst expansion to inflate 0.05", got)
	}
	ratio := counts[CatRestrictedWord] / counts[CatDuplicate]
	if ratio < 1.2 || ratio > 1.8 {
		t.Errorf("restricted_word:duplicate = %.2f, want ~1.5 (0.15 vs 0.10)", ratio)
	}
	if counts[CatClean] == 0 {
		t.Error("no clean messages generated")
	}
}

func TestMixValidate(t *testing.T) {
	if err := (Mix{Duplicate: 0.5, LLMFlag: 0.6}).Validate(); err == nil {
		t.Error("over-subscribed mix accepted")
	}
	if err := (Mix{Duplicate: -0.1}).Validate(); err == nil {
		t.Error("negative fraction accepted")
	}
	m := Mix{Duplicate: 0.1, RateBurst: 0.1, RestrictedWord: 0.1, LLMFlag: 0.1}
	if err := m.Validate(); err != nil {
		t.Errorf("valid mix rejected: %v", err)
	}
	if got := m.Clean(); math.Abs(got-0.6) > 1e-9 {
		t.Errorf("Clean() = %v, want 0.6", got)
	}
}

// A duplicate must be a byte-identical replay of what that sender sent
// before, or the §7.4 hash (over normalised text) will not match and
// the detector will never fire.
func TestDuplicateReplaysExactText(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Weights = []float64{1}
	cfg.AuthorsPer = []int{1}
	cfg.Mix = Mix{Duplicate: 0.5}
	cfg.RunID = "d"
	g := NewGenerator(cfg, 0)

	seen := map[string]int{}
	dupes := 0
	for range 400 {
		rec := g.Next(time.Now())
		if rec.Category == CatDuplicate && seen[rec.Msg.Text] > 0 {
			dupes++
		}
		seen[rec.Msg.Text]++
	}
	if dupes == 0 {
		t.Fatal("no duplicate ever replayed an earlier text; the duplicate detector would never fire")
	}
}

// Restricted-word text must carry the token whole, since §7.4 matches
// whole tokens rather than substrings (the Scunthorpe problem).
func TestRestrictedWordIsAWholeToken(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mix = Mix{RestrictedWord: 1}
	cfg.RunID = "r"
	g := NewGenerator(cfg, 0)
	rec := g.Next(time.Now())
	fields := strings.Fields(rec.Msg.Text)
	found := false
	for _, f := range fields {
		if f == cfg.RestrictedWord {
			found = true
		}
	}
	if !found {
		t.Fatalf("restricted word %q is not a whole token in %q", cfg.RestrictedWord, rec.Msg.Text)
	}
}

// Every generated message must carry the run's creator and a plausible
// body length, since text size drives the record size and therefore the
// throughput number.
func TestRecordShape(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CreatorID = "creator-1"
	cfg.TextBytes = 120
	cfg.RunID = "s"
	g := NewGenerator(cfg, 2)
	at := time.Unix(1700000000, 0)
	rec := g.Next(at)
	if rec.Msg.CreatorID != "creator-1" {
		t.Errorf("creator_id = %q", rec.Msg.CreatorID)
	}
	if !rec.Msg.IngestedAt.Equal(at) {
		t.Errorf("ingested_at = %v, want the ideal-clock time %v", rec.Msg.IngestedAt, at)
	}
	if len(rec.Msg.Text) < 100 {
		t.Errorf("text is %d bytes, want ~%d", len(rec.Msg.Text), cfg.TextBytes)
	}
}

// Shards must not produce colliding message ids, or the redelivery
// guard (§7.4) would drop a large share of the run as duplicates.
func TestShardsDoNotCollide(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RunID = "c"
	at := time.Unix(1700000000, 0)
	ids := map[string]bool{}
	for shard := range 8 {
		g := NewGenerator(cfg, shard)
		for range 1000 {
			id := g.Next(at).Msg.MessageID
			if ids[id] {
				t.Fatalf("duplicate message_id %q across shards", id)
			}
			ids[id] = true
		}
	}
}

func TestZipfTopShare(t *testing.T) {
	z := NewZipf(1000, 1.1)
	if s := z.TopShare(1); s < 0.05 || s > 0.30 {
		t.Errorf("top content share = %.3f, expected a hot spot", s)
	}
	if s := z.TopShare(1000); s != 1 {
		t.Errorf("TopShare(all) = %v, want 1", s)
	}
	if s := z.TopShare(0); s != 0 {
		t.Errorf("TopShare(0) = %v, want 0", s)
	}
	u := NewZipf(10, 0)
	for i := range 10 {
		if math.Abs(u.Weight(i)-0.1) > 1e-9 {
			t.Fatalf("skew 0 is not uniform: weight %d = %v", i, u.Weight(i))
		}
	}
}
