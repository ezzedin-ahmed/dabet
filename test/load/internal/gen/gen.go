// Package gen synthesises messages.v1 records with a realistic,
// heavily hot-spotted population (N6) and a configurable mix of texts
// that trip each stage of the §7.3 cascade.
//
// Nothing here talks to the network: a Generator is a pure function of
// its config and its PRNG stream, which is what makes the distribution
// and the partition keying unit-testable.
package gen

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"dabet/pkg/contracts"
)

// Mix is the fraction of generated messages that should trip each stage
// of the cascade. Fractions are of the whole population and must sum to
// at most 1; the remainder is clean LLM-bound text (survives every cheap
// stage, reaches the sampler, and is classified as non-violating).
//
// The categories map onto the cascade of §7.3:
//
//	Duplicate     — the author repeats a text they just sent (stage 5)
//	RateBurst     — the author sends a burst well over the policy limit (stage 4)
//	RestrictedWord— the text contains the policy's restricted word (stage 7)
//	LLMFlag       — the text trips the LLM rubric (stage 9, a real flag)
//	(remainder)   — clean text: LLM-bound, verdict "no violation"
type Mix struct {
	Duplicate      float64 `json:"duplicate"`
	RateBurst      float64 `json:"rate_burst"`
	RestrictedWord float64 `json:"restricted_word"`
	LLMFlag        float64 `json:"llm_flag"`
}

// Clean is the implied remainder.
func (m Mix) Clean() float64 {
	c := 1 - (m.Duplicate + m.RateBurst + m.RestrictedWord + m.LLMFlag)
	if c < 0 {
		return 0
	}
	return c
}

// Validate rejects a mix that over-subscribes the population.
func (m Mix) Validate() error {
	sum := m.Duplicate + m.RateBurst + m.RestrictedWord + m.LLMFlag
	for name, v := range map[string]float64{
		"duplicate": m.Duplicate, "rate_burst": m.RateBurst,
		"restricted_word": m.RestrictedWord, "llm_flag": m.LLMFlag,
	} {
		if v < 0 || v > 1 {
			return fmt.Errorf("mix.%s = %v, want [0,1]", name, v)
		}
	}
	if sum > 1.0000001 {
		return fmt.Errorf("mix fractions sum to %v, want <= 1", sum)
	}
	return nil
}

// Config describes the synthetic population.
type Config struct {
	// CreatorID is the Dabet UUID every generated message is billed and
	// policy-resolved against. The adapter resolves this at ingest, so a
	// generator bypassing the adapter has to supply it (§4.2).
	CreatorID string `json:"creator_id"`

	// Contents is the number of distinct content_ids (streams) in the
	// population, and Skew is the Zipf exponent over them. Skew 0 is
	// uniform; the N6 "heavily hot-spotted" shape is ~1.0-1.4.
	Contents int     `json:"contents"`
	Skew     float64 `json:"skew"`

	// Weights, when non-empty, replaces the Zipf draw with explicit
	// per-content shares (normalised). This is how the sampler scenario
	// pins individual contents to the exact traffic rates of the §7.5
	// coverage table, and how the hot-spot scenario builds one
	// enormous content against a flat tail rather than a smooth curve.
	Weights []float64 `json:"weights,omitempty"`

	// AuthorsPer, when non-empty, gives the exact number of senders in
	// each content, overriding the AuthorsPerContent heuristic. A hot
	// content with very few senders is the pathological case: the
	// hash(author_id, content_id) key then lands its whole firehose on
	// a handful of partitions.
	AuthorsPer []int `json:"authors_per,omitempty"`

	// AuthorsPerContent is the mean number of distinct senders in a
	// content; the actual count per content scales with the content's
	// rank so that busy streams also have more people in them (which is
	// what real chat looks like, and what keeps a hot content from
	// collapsing onto one Redis key).
	AuthorsPerContent int `json:"authors_per_content"`

	// AuthorSkew is the Zipf exponent over authors within one content:
	// a few loud people, a long tail of lurkers.
	AuthorSkew float64 `json:"author_skew"`

	Mix Mix `json:"mix"`

	// RestrictedWord must match the policy the run provisions.
	RestrictedWord string `json:"restricted_word"`

	// LLMFlagToken is the token the LLM stage classifies as violating.
	// tools/mockllm (and its latency-injecting mode) treats any listed
	// message containing this literal as a rule-1 violation.
	LLMFlagToken string `json:"llm_flag_token"`

	// TextBytes is the approximate rendered length of a message body.
	TextBytes int `json:"text_bytes"`

	// RateBurstLen is how many back-to-back messages one author sends
	// when the mix selects a rate-limit burst.
	RateBurstLen int `json:"rate_burst_len"`

	// Seed makes a run reproducible.
	Seed uint64 `json:"seed"`

	// RunID tags every message_id this run mints, so a verdict left on
	// flagged.v1 by an earlier run (retention is 7 days, §4.8) cannot
	// contaminate this run's numbers.
	RunID string `json:"run_id"`
}

// DefaultConfig is a sane hot-spotted population: 1 000 contents at Zipf
// 1.1, so the top content takes roughly an eighth of all traffic and the
// bottom decile is effectively idle.
func DefaultConfig() Config {
	return Config{
		Contents:          1000,
		Skew:              1.1,
		AuthorsPerContent: 50,
		AuthorSkew:        0.8,
		Mix: Mix{
			Duplicate:      0.05,
			RateBurst:      0.05,
			RestrictedWord: 0.05,
			LLMFlag:        0.05,
		},
		RestrictedWord: "bannedword",
		LLMFlagToken:   "FLAGME",
		TextBytes:      80,
		RateBurstLen:   12,
		Seed:           1,
	}
}

// Category labels what a generated message was meant to trip. It is
// recorded by the generator so a run can compare intent against the
// detector hits the service actually reported.
type Category string

const (
	CatClean          Category = "clean"
	CatDuplicate      Category = "duplicate"
	CatRateBurst      Category = "rate_burst"
	CatRestrictedWord Category = "restricted_word"
	CatLLMFlag        Category = "llm_flag"
)

// Categories is the enumeration, in report order.
var Categories = []Category{CatClean, CatDuplicate, CatRateBurst, CatRestrictedWord, CatLLMFlag}

// Record is one synthetic message plus the partition key it must carry
// and the category it was generated as.
type Record struct {
	Msg      contracts.Message
	Key      []byte
	Category Category
}

// Generator produces Records. It is NOT safe for concurrent use: give
// each sending goroutine its own via Config.Generator(shard), which
// decorrelates the PRNG streams while keeping the population shared.
type Generator struct {
	cfg      Config
	rng      *rand.Rand
	shard    int
	contents *Zipf
	authors  []*Zipf // one per content, lazily built

	seq uint64

	// lastText remembers, per author slot, the previous text so the
	// duplicate category can replay it exactly (§7.4 hashes normalised
	// text, so an exact replay is the honest way to trip it).
	lastText map[string]string

	// burst carries an in-progress rate-limit burst.
	burstLeft int
	burstC    int
	burstA    int

	filler string
}

// NewGenerator builds generator shard i of n. Shards share the content
// and author populations but not their PRNG streams.
func NewGenerator(cfg Config, shard int) *Generator {
	if cfg.Contents <= 0 {
		cfg.Contents = 1
	}
	if cfg.AuthorsPerContent <= 0 {
		cfg.AuthorsPerContent = 1
	}
	if cfg.RateBurstLen <= 0 {
		cfg.RateBurstLen = 2
	}
	if len(cfg.Weights) > 0 {
		cfg.Contents = len(cfg.Weights)
	}
	contents := NewZipf(cfg.Contents, cfg.Skew)
	if len(cfg.Weights) > 0 {
		contents = NewWeighted(cfg.Weights)
	}
	g := &Generator{
		cfg:      cfg,
		rng:      rand.New(rand.NewPCG(cfg.Seed, uint64(shard)+0x9E3779B97F4A7C15)),
		shard:    shard,
		contents: contents,
		authors:  make([]*Zipf, cfg.Contents),
		lastText: make(map[string]string, 4096),
		filler:   strings.Repeat("lorem ipsum chat filler ", 16),
	}
	return g
}

// ContentID renders content rank i as an opaque adapter-style id. The
// format is deliberately opaque-looking: no service outside the adapter
// may parse it (P5), and the load generator must not be the exception
// that makes a parser look viable.
func ContentID(i int) string { return "ct_ld" + strconv.FormatInt(int64(i), 36) }

// AuthorID renders author j of content i.
func AuthorID(content, author int) string {
	return "sd_ld" + strconv.FormatInt(int64(content), 36) + "x" + strconv.FormatInt(int64(author), 36)
}

// authorsIn is the number of distinct senders in content rank i. Busy
// streams have more people in them: the count scales with the content's
// share of traffic, floored at 2 so even the quietest content has a pair.
func (g *Generator) authorsIn(i int) int {
	if i < len(g.cfg.AuthorsPer) {
		if n := g.cfg.AuthorsPer[i]; n > 0 {
			return n
		}
	}
	share := g.contents.Weight(i) * float64(g.cfg.Contents)
	n := int(float64(g.cfg.AuthorsPerContent) * (0.25 + 0.75*share))
	if n < 2 {
		n = 2
	}
	return n
}

func (g *Generator) authorZipf(i int) *Zipf {
	if g.authors[i] == nil {
		g.authors[i] = NewZipf(g.authorsIn(i), g.cfg.AuthorSkew)
	}
	return g.authors[i]
}

// Next produces the next record with the given ingest timestamp. at is
// the *intended* send time on the ideal clock, not the wall clock at
// which the record is actually handed to Kafka — this is what keeps the
// service-side moderation_e2e_latency_seconds histogram free of
// coordinated omission (see internal/sched).
func (g *Generator) Next(at time.Time) Record {
	var (
		c, a int
		cat  Category
	)
	if g.burstLeft > 0 {
		g.burstLeft--
		c, a, cat = g.burstC, g.burstA, CatRateBurst
	} else {
		c = g.contents.Next(g.rng)
		a = g.authorZipf(c).Next(g.rng)
		cat = g.pickCategory()
		if cat == CatRateBurst {
			g.burstC, g.burstA = c, a
			g.burstLeft = g.cfg.RateBurstLen - 1
		}
	}

	contentID := ContentID(c)
	authorID := AuthorID(c, a)
	text := g.text(cat, contentID, authorID)

	g.seq++
	return Record{
		Msg: contracts.Message{
			MessageID:  MintMessageID(g.cfg.RunID, g.shard, g.seq, at),
			ContentID:  contentID,
			AuthorID:   authorID,
			CreatorID:  g.cfg.CreatorID,
			Text:       text,
			IngestedAt: at,
		},
		Key:      contracts.MessagesKey(authorID, contentID),
		Category: cat,
	}
}

func (g *Generator) pickCategory() Category {
	u := g.rng.Float64()
	m := g.cfg.Mix
	switch {
	case u < m.Duplicate:
		return CatDuplicate
	case u < m.Duplicate+m.RateBurst:
		return CatRateBurst
	case u < m.Duplicate+m.RateBurst+m.RestrictedWord:
		return CatRestrictedWord
	case u < m.Duplicate+m.RateBurst+m.RestrictedWord+m.LLMFlag:
		return CatLLMFlag
	default:
		return CatClean
	}
}

// text renders the body for a category. Bodies are padded to roughly
// TextBytes so the Kafka record size — and therefore the throughput
// measurement — is representative rather than trivially small.
func (g *Generator) text(cat Category, contentID, authorID string) string {
	slot := contentID + "\x00" + authorID
	switch cat {
	case CatDuplicate:
		if prev, ok := g.lastText[slot]; ok {
			return prev
		}
		// No previous text from this sender yet: emit one and let the
		// next duplicate draw replay it. This message is honestly a
		// clean one, and is reported as such.
		return g.remember(slot, g.pad("hello everyone, first message in the room"))
	case CatRestrictedWord:
		return g.remember(slot, g.pad("check this out "+g.cfg.RestrictedWord+" right now"))
	case CatLLMFlag:
		return g.remember(slot, g.pad(g.cfg.LLMFlagToken+" selling two tickets for tonight, DM me"))
	default: // clean, rate_burst
		return g.remember(slot, g.pad("nice play there "+strconv.FormatUint(g.rng.Uint64(), 36)))
	}
}

func (g *Generator) remember(slot, text string) string {
	g.lastText[slot] = text
	if len(g.lastText) > 1<<16 {
		clear(g.lastText)
	}
	return text
}

func (g *Generator) pad(s string) string {
	if len(s) >= g.cfg.TextBytes {
		return s
	}
	need := g.cfg.TextBytes - len(s)
	if need > len(g.filler) {
		need = len(g.filler)
	}
	return s + " " + g.filler[:need]
}
