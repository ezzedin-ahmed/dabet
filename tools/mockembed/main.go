// Command mockembed serves the shared embedding contract
// (POST /v1/embed {"texts":[...]} -> {"vectors":[[f32 x 384]]}) with
// deterministic vectors: each text's vector is derived from a SHA-256
// counter chain over the text and L2-normalised, so identical texts always
// embed identically.
//
// # Similarity markers
//
// Hash-seeded vectors are near-orthogonal for any two distinct texts, which
// makes it impossible to drive anything that reasons about *semantic*
// closeness — the §7.4 semantic-spam detector (cosine >= 0.95) and the §8.6
// batch clustering both need texts that are worded differently but sit close
// together in embedding space.
//
// A text may therefore carry a marker
//
//	[[sim:<cluster>]]
//	[[sim:<cluster>:<cosine>]]
//	[[sim:<cluster>:<cosine>:<variant>]]
//
// which places its vector at exactly <cosine> from the cluster's centroid,
// in a direction fixed by <variant>. The centroid itself is just the ordinary
// hash embedding of the cluster id, so different clusters are near-orthogonal
// exactly as different texts are.
//
// Because the residual directions of two variants are independent and 384
// dimensions is enough for them to be all but orthogonal, two marked texts in
// the same cluster satisfy
//
//	cos(a, b) ~= cos_a * cos_b   (+/- ~1/sqrt(384) of the residual product)
//
// so a test that wants a pair above the 0.95 threshold asks for 0.99 each and
// gets ~0.980; a test that wants a tight but non-degenerate cluster asks for
// 0.97 and gets members ~0.941 apart.
//
// The cluster may be a `/`-separated path with one cosine per level,
//
//	[[sim:topic/theme:0.90/0.99:<variant>]]
//
// which nests: the theme's centroid sits at 0.90 from the topic's, and the
// text at 0.99 from the theme's. Every text under one theme shares that
// theme's centroid, so a cluster built this way holds together as one topic
// (~0.79 pairwise across themes) while still splitting cleanly into themes
// (~0.98 pairwise within one) — the shape §8.6's two HDBSCAN passes look for.
//
// Text carrying no marker takes the original code path byte for byte, so the
// existing end-to-end suite and the load harness are unaffected.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const dimensions = 384

type embedRequest struct {
	Texts []string `json:"texts"`
}

type embedResponse struct {
	Vectors [][]float32 `json:"vectors"`
}

// simMarker matches [[sim:cluster]], [[sim:cluster:cosine]] and
// [[sim:cluster:cosine:variant]] anywhere in the text. Colons separate the
// fields, so none of them may contain one.
var simMarker = regexp.MustCompile(`\[\[sim:([^\]\[:]*(?::[^\]\[:]*)*)\]\]`)

// simSpec is a parsed similarity marker. path and cosines are always the same
// length: one cosine per level of the hierarchy.
type simSpec struct {
	path    []string
	cosines []float64
	variant string
}

// parseSim extracts the first well-formed marker in text. Anything malformed —
// an empty cluster, a cosine that is not a number in [-1, 1], a path and a
// cosine list of different lengths — is not a marker: the text falls back to
// the ordinary hash embedding rather than silently embedding somewhere
// unintended.
func parseSim(text string) (simSpec, bool) {
	m := simMarker.FindStringSubmatch(text)
	if m == nil {
		return simSpec{}, false
	}
	fields := strings.Split(m[1], ":")

	path := strings.Split(fields[0], "/")
	for _, level := range path {
		if level == "" {
			return simSpec{}, false
		}
	}

	cosines := make([]float64, len(path))
	for i := range cosines {
		cosines[i] = 1
	}
	if len(fields) > 1 && fields[1] != "" {
		parts := strings.Split(fields[1], "/")
		if len(parts) != len(path) {
			return simSpec{}, false
		}
		for i, p := range parts {
			c, err := strconv.ParseFloat(p, 64)
			if err != nil || math.IsNaN(c) || c < -1 || c > 1 {
				return simSpec{}, false
			}
			cosines[i] = c
		}
	}

	spec := simSpec{path: path, cosines: cosines}
	if len(fields) > 2 {
		spec.variant = strings.Join(fields[2:], ":")
	} else {
		// No explicit variant: the whole text is the variant, so two
		// differently-worded messages in one cluster still get distinct
		// residuals instead of collapsing onto the same vector.
		spec.variant = text
	}
	return spec, true
}

// rotate returns a unit vector whose cosine with axis is exactly cos, in the
// direction fixed by seed. The offset is the component of an independent hash
// vector orthogonal to axis (Gram-Schmidt), which is what makes the
// decomposition exact rather than approximate.
func rotate(axis []float32, cos float64, seed string) []float32 {
	if cos >= 1 {
		return axis
	}
	raw := hashEmbed(seed)
	var dot float64
	for i := range raw {
		dot += float64(raw[i]) * float64(axis[i])
	}
	perp := make([]float64, dimensions)
	var perpNorm float64
	for i := range raw {
		perp[i] = float64(raw[i]) - dot*float64(axis[i])
		perpNorm += perp[i] * perp[i]
	}
	perpNorm = math.Sqrt(perpNorm)
	if perpNorm == 0 {
		// Only reachable if the two independent hash chains produced parallel
		// vectors. Degrade to the axis rather than divide by zero.
		return axis
	}

	scale := math.Sqrt(1 - cos*cos)
	vec := make([]float32, dimensions)
	var norm float64
	for i := range vec {
		v := cos*float64(axis[i]) + scale*(perp[i]/perpNorm)
		vec[i] = float32(v)
		norm += v * v
	}
	// Renormalise in float32 space so the emitted vector is unit length to the
	// precision the caller actually receives.
	norm = math.Sqrt(norm)
	if norm == 0 {
		return axis
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
	return vec
}

// embedSimilar walks the hierarchy: the head of the path names a centroid,
// each further level is a sub-centroid rotated off its parent, and the text
// itself is one more rotation off the deepest centroid. Sub-centroid
// directions are seeded by the path so every text under a/s1 shares one
// sub-centroid, which is what gives §8.6's second HDBSCAN pass something to
// find.
func embedSimilar(spec simSpec) []float32 {
	vec := hashEmbed("mockembed/sim/centroid/" + spec.path[0])
	for i := 1; i < len(spec.path); i++ {
		vec = rotate(vec, spec.cosines[i-1],
			"mockembed/sim/level/"+strings.Join(spec.path[:i+1], "/"))
	}
	full := strings.Join(spec.path, "/")
	return rotate(vec, spec.cosines[len(spec.cosines)-1],
		"mockembed/sim/residual/"+full+"/"+spec.variant)
}

func embed(text string) []float32 {
	if spec, ok := parseSim(text); ok {
		return embedSimilar(spec)
	}
	return hashEmbed(text)
}

// hashEmbed is the original deterministic embedding: a SHA-256 counter chain
// over the text, L2-normalised. Unmarked text still goes through here
// unchanged.
func hashEmbed(text string) []float32 {
	seed := sha256.Sum256([]byte(text))
	vec := make([]float32, 0, dimensions)
	var counter [4]byte
	for i := 0; len(vec) < dimensions; i++ {
		binary.BigEndian.PutUint32(counter[:], uint32(i))
		block := sha256.Sum256(append(seed[:], counter[:]...))
		for off := 0; off+4 <= len(block) && len(vec) < dimensions; off += 4 {
			u := binary.BigEndian.Uint32(block[off : off+4])
			vec = append(vec, float32(int32(u))/float32(math.MaxInt32))
		}
	}
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		norm = 1
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
	return vec
}

// maxTrackedTexts bounds the per-text ledger. A load run embeds millions of
// distinct texts and this process has no eviction policy worth the name, so
// the ledger simply stops admitting new keys once it is full. Totals keep
// counting regardless.
const maxTrackedTexts = 16384

// ledger records how many times each text has been embedded. §8.4 says a
// message is embedded at most once and the vector reused between semantic
// spam (§7.4) and Insights; without a count on this side there is no way for
// a test to tell whether that actually holds.
type ledger struct {
	mu       sync.Mutex
	perText  map[string]int
	requests int
	texts    int
	dropped  int // distinct texts not admitted because the ledger was full
}

func newLedger() *ledger { return &ledger{perText: map[string]int{}} }

func (l *ledger) record(texts []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.requests++
	l.texts += len(texts)
	for _, t := range texts {
		if _, known := l.perText[t]; !known && len(l.perText) >= maxTrackedTexts {
			l.dropped++
			continue
		}
		l.perText[t]++
	}
}

func (l *ledger) count(text string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.perText[text]
}

func (l *ledger) snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]int{
		"requests": l.requests,
		"texts":    l.texts,
		"distinct": len(l.perText),
		"dropped":  l.dropped,
	}
}

func (l *ledger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.perText = map[string]int{}
	l.requests, l.texts, l.dropped = 0, 0, 0
}

type server struct{ led *ledger }

func (s *server) handleEmbed(w http.ResponseWriter, r *http.Request) {
	var req embedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	s.led.record(req.Texts)
	vectors := make([][]float32, len(req.Texts))
	for i, t := range req.Texts {
		vectors[i] = embed(t)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(embedResponse{Vectors: vectors})
}

// handleEmbeds reports the ledger. With ?text= it answers how many times that
// exact string was embedded; without, it answers the totals.
func (s *server) handleEmbeds(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if text := r.URL.Query().Get("text"); text != "" {
		_ = json.NewEncoder(w).Encode(map[string]int{"count": s.led.count(text)})
		return
	}
	_ = json.NewEncoder(w).Encode(s.led.snapshot())
}

func (s *server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.led.reset()
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/embed", s.handleEmbed)
	mux.HandleFunc("GET /admin/embeds", s.handleEmbeds)
	mux.HandleFunc("DELETE /admin/embeds", s.handleReset)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockembed")
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8091"
	}
	srv := &http.Server{Addr: addr, Handler: (&server{led: newLedger()}).routes()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	logger.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server failed", "error", err.Error())
		os.Exit(1)
	}
}
