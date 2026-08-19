package job

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"dabet/pkg/contracts"
	"dabet/pkg/embeddings"

	"dabet/services/clusters-job/internal/hdbscan"
)

// Centroid is one topic or theme centroid destined for Milvus.
type Centroid struct {
	TopicID  string
	ParentID string // ZeroUUID for a topic; the topic_id for a theme
	Vector   []float32
}

// CentroidStore is the Milvus surface (collection topic_centroids, A22).
type CentroidStore interface {
	// ListByCreator returns the creator's current centroids.
	ListByCreator(ctx context.Context, creatorID string) ([]Centroid, error)
	// ReplaceCreator atomically-enough replaces the creator's centroids:
	// delete-by-creator, then insert.
	ReplaceCreator(ctx context.Context, creatorID string, cs []Centroid) error
}

// TopicRow is one §8.7 topics row.
type TopicRow struct {
	TopicID     string
	ParentID    string
	Label       string
	Description string
	Version     uint32
	UpdatedAt   time.Time
}

// TopicStore is the ClickHouse topics surface.
type TopicStore interface {
	// PriorTopics returns the creator's current (latest-version) rows.
	PriorTopics(ctx context.Context, creatorID string) ([]TopicRow, error)
	// UpsertTopics inserts rows; ReplacingMergeTree(version) collapses
	// them over prior versions of the same topic_id.
	UpsertTopics(ctx context.Context, creatorID string, rows []TopicRow) error
	// DeleteTopicsExcept removes the creator's rows whose topic_id is not
	// in keep, so topics that dissolved in a recluster do not linger.
	DeleteTopicsExcept(ctx context.Context, creatorID string, keep []string) error
}

// Embedder embeds label-sample texts; pkg/embeddings.Client satisfies it.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Config are the pipeline tunables. Every documented number is a default,
// not a constant (§4.4); main wires them from the environment.
type Config struct {
	MinClusterSize      int     // coarse pass (default 15)
	MinSamples          int     // coarse pass (default 5)
	ThemeMinClusterSize int     // fine pass (default 5)
	ThemeMinSamples     int     // fine pass (default 3)
	MaxPoints           int     // per-run cap, uniform sampling above it (default 200000)
	AssignThreshold     float64 // cosine floor matching sample text to a cluster (default 0.75, A23)
	ReuseThreshold      float64 // cosine floor matching a new centroid to a prior topic (default 0.75)
	LabelPoints         int     // texts per cluster sent to the LLM (default 20, A25)
	TextSampleMax       int     // recent texts sampled per run (default 2000)
	EmbedBatch          int     // embedding request batch size (default 64)
	// BackfillCounts is off | on_demand | always (default on_demand); see
	// ValidBackfillMode.
	BackfillCounts string
	// BackfillLag is how far behind now the counts rewrite stops, bounding
	// the concurrent-live-writer race (default 2h). See backfillCounts.
	BackfillLag time.Duration
}

// Runner executes clustering runs. All dependencies are interfaces; tests
// wire fakes.
type Runner struct {
	Store     ObjectStore
	Centroids CentroidStore
	Topics    TopicStore
	Counts    CountStore // nil disables the topic_counts backfill
	Texts     TextSource
	Embed     Embedder
	LLM       LLMLabeler
	Usage     UsagePublisher
	Metrics   *Metrics
	Log       *slog.Logger
	Cfg       Config
	Now       func() time.Time
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Run executes one clustering run for d. Idempotent for a given
// (creator, window): topic identity is either reused from a matching
// prior topic or derived deterministically from the window.
func (r *Runner) Run(ctx context.Context, d Decision) (res Result, err error) {
	start := r.now()
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		if r.Metrics != nil {
			r.Metrics.Runs.WithLabelValues(d.Trigger, outcome).Inc()
			r.Metrics.Duration.WithLabelValues(d.Trigger).Observe(r.now().Sub(start).Seconds())
		}
	}()
	log := r.Log.With("creator_id", d.CreatorID, "trigger", d.Trigger,
		"from", d.From.UTC().Format(time.RFC3339), "to", d.To.UTC().Format(time.RFC3339))

	points, err := r.readPoints(ctx, d)
	if err != nil {
		return Result{}, err
	}
	res.PointsRead = len(points)
	if len(points) == 0 {
		log.Info("no embeddings in window, nothing to do")
		return res, nil
	}

	// Cap with uniform (stride) sampling — deterministic given the S3
	// listing order.
	if r.Cfg.MaxPoints > 0 && len(points) > r.Cfg.MaxPoints {
		kept := stride(points, r.Cfg.MaxPoints)
		log.Info("capped points for run",
			"read", len(points), "kept", len(kept), "dropped", len(points)-len(kept))
		points = kept
	}
	res.PointsProcessed = len(points)

	vecs := make([][]float32, len(points))
	for i, p := range points {
		vecs[i] = Normalize(append([]float32(nil), p.Vector...))
	}

	// Coarse pass → topics.
	labels := hdbscan.Cluster(vecs, hdbscan.Options{
		MinClusterSize:     r.Cfg.MinClusterSize,
		MinSamples:         r.Cfg.MinSamples,
		AllowSingleCluster: true,
	})
	topicMembers := groupMembers(labels)
	if len(topicMembers) == 0 {
		// Below min_cluster_size or pure noise: keep the creator's
		// existing clusters rather than wiping them on a thin window.
		log.Info("no clusters found, keeping existing topics", "points", len(points))
		return res, r.emitUsage(ctx, d, res.PointsProcessed, log)
	}

	// Fine pass within each topic → themes. AllowSingleCluster stays off:
	// a topic that does not subdivide has no themes, rather than one
	// theme equal to itself.
	var topics []clusterOut
	var themes []clusterOut
	for ti, members := range topicMembers {
		topics = append(topics, clusterOut{members: members, centroid: centroidOf(vecs, members), themeOf: -1})
		sub := make([][]float32, len(members))
		for i, m := range members {
			sub[i] = vecs[m]
		}
		subLabels := hdbscan.Cluster(sub, hdbscan.Options{
			MinClusterSize: r.Cfg.ThemeMinClusterSize,
			MinSamples:     r.Cfg.ThemeMinSamples,
		})
		for _, subMembers := range groupMembers(subLabels) {
			abs := make([]int, len(subMembers))
			for i, m := range subMembers {
				abs[i] = members[m]
			}
			themes = append(themes, clusterOut{members: abs, centroid: centroidOf(vecs, abs), themeOf: ti})
		}
	}
	res.Topics, res.Themes = len(topics), len(themes)

	// Prior state: centroids from Milvus, labels/versions from ClickHouse.
	prior, err := r.Centroids.ListByCreator(ctx, d.CreatorID)
	if err != nil {
		return res, fmt.Errorf("list prior centroids: %w", err)
	}
	priorRows, err := r.Topics.PriorTopics(ctx, d.CreatorID)
	if err != nil {
		return res, fmt.Errorf("prior topics: %w", err)
	}
	priorByID := make(map[string]TopicRow, len(priorRows))
	var version uint32 = 1
	for _, row := range priorRows {
		priorByID[row.TopicID] = row
		if row.Version >= version {
			version = row.Version + 1
		}
	}

	// Identity: reuse the prior topic id when the new centroid is
	// centroid-similar to it (greedy best-match), otherwise mint a
	// deterministic id from the window. Reuse is what lets labels carry
	// over past text retention (§10 known gap 6).
	var priorTopicCents, priorThemeCents []Centroid
	for _, c := range prior {
		c.Vector = Normalize(append([]float32(nil), c.Vector...))
		if c.ParentID == ZeroUUID {
			priorTopicCents = append(priorTopicCents, c)
		} else {
			priorThemeCents = append(priorThemeCents, c)
		}
	}
	windowKey := []string{d.CreatorID, d.From.UTC().Format(time.RFC3339), d.To.UTC().Format(time.RFC3339)}
	topicIDs := r.assignIdentity(topics, priorTopicCents, func(i int) string {
		return DeterministicUUID(append([]string{"topic"}, append(windowKey, strconv.Itoa(i))...)...)
	})
	themeIDs := r.assignIdentity(themes, priorThemeCents, func(i int) string {
		return DeterministicUUID(append([]string{"theme"}, append(windowKey, topicIDs[themes[i].themeOf], strconv.Itoa(i))...)...)
	})

	// Labelling (§8.6, A25): sample in-retention text, embed it, match it
	// to clusters by nearest centroid, and let the LLM name each cluster
	// from up to LabelPoints matched texts. Everything here fails open —
	// to the prior label when identity was reused, else to a generic
	// label. Text never reaches logs or stores (P4).
	texts, tvecs := r.sampleTexts(ctx, d.CreatorID, log)
	topicTexts := make([][]indexedSim, len(topics))
	themeTexts := make([][]indexedSim, len(themes))
	for ti, tv := range tvecs {
		best, bestSim := -1, r.Cfg.AssignThreshold
		for ci, c := range topics {
			if sim := Dot(tv, c.centroid); sim >= bestSim {
				best, bestSim = ci, sim
			}
		}
		if best < 0 {
			continue
		}
		topicTexts[best] = append(topicTexts[best], indexedSim{ti, bestSim})
		tBest, tBestSim := -1, r.Cfg.AssignThreshold
		for ci, c := range themes {
			if c.themeOf != best {
				continue
			}
			if sim := Dot(tv, c.centroid); sim >= tBestSim {
				tBest, tBestSim = ci, sim
			}
		}
		if tBest >= 0 {
			themeTexts[tBest] = append(themeTexts[tBest], indexedSim{ti, tBestSim})
		}
	}

	rows := make([]TopicRow, 0, len(topics)+len(themes))
	cents := make([]Centroid, 0, len(topics)+len(themes))
	nowTS := r.now().UTC()
	for i := range topics {
		label, desc := r.labelCluster(ctx, texts, topicTexts[i], priorByID[topicIDs[i]],
			fmt.Sprintf("Topic %d", i+1), log)
		rows = append(rows, TopicRow{TopicID: topicIDs[i], ParentID: ZeroUUID,
			Label: label, Description: desc, Version: version, UpdatedAt: nowTS})
		cents = append(cents, Centroid{TopicID: topicIDs[i], ParentID: ZeroUUID, Vector: topics[i].centroid})
	}
	for i := range themes {
		label, desc := r.labelCluster(ctx, texts, themeTexts[i], priorByID[themeIDs[i]],
			fmt.Sprintf("Theme %d", i+1), log)
		rows = append(rows, TopicRow{TopicID: themeIDs[i], ParentID: topicIDs[themes[i].themeOf],
			Label: label, Description: desc, Version: version, UpdatedAt: nowTS})
		cents = append(cents, Centroid{TopicID: themeIDs[i], ParentID: topicIDs[themes[i].themeOf], Vector: themes[i].centroid})
	}

	// Writes: replace the creator's centroids, upsert versioned rows,
	// then drop rows for topics that no longer exist.
	if err := r.Centroids.ReplaceCreator(ctx, d.CreatorID, cents); err != nil {
		return res, fmt.Errorf("replace centroids: %w", err)
	}
	if err := r.Topics.UpsertTopics(ctx, d.CreatorID, rows); err != nil {
		return res, fmt.Errorf("upsert topics: %w", err)
	}
	keep := make([]string, 0, len(rows))
	for _, row := range rows {
		keep = append(keep, row.TopicID)
	}
	if err := r.Topics.DeleteTopicsExcept(ctx, d.CreatorID, keep); err != nil {
		return res, fmt.Errorf("delete stale topics: %w", err)
	}

	// Rewrite this creator's topic_counts for the window so the §8.8
	// dashboard attributes history to the ids just written, instead of to
	// the topic ids that were superseded a moment ago. Scoped to this
	// creator and this window (§8.6).
	deleted, written, backfillErr := r.backfillCounts(
		ctx, d, points, topics, themes, topicIDs, themeIDs, log)
	res.CountsDeleted, res.CountsWritten = deleted, written

	log.Info("clustering run complete", "points", res.PointsProcessed,
		"topics", res.Topics, "themes", res.Themes, "version", version,
		"counts_deleted", res.CountsDeleted, "counts_written", res.CountsWritten)

	// usage.v1 is emitted for the compute that happened regardless of the
	// backfill outcome, with the same quantity (points processed) and the
	// same deterministic idempotency key (§4.2). The backfill is part of
	// the same recluster, so it emits no event of its own.
	if err := r.emitUsage(ctx, d, res.PointsProcessed, log); err != nil {
		return res, err
	}
	if backfillErr != nil {
		return res, fmt.Errorf("backfill counts: %w", backfillErr)
	}
	return res, nil
}

// Point is one embedding read from S3.
type Point struct {
	ContentID  string
	EmbeddedAt time.Time
	Vector     []float32
}

func (r *Runner) readPoints(ctx context.Context, d Decision) ([]Point, error) {
	objs, err := r.Store.ListCreatorObjects(ctx, d.CreatorID, d.From, d.To)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	var pts []Point
	for _, o := range objs {
		data, err := r.Store.Get(ctx, o.Key)
		if err != nil {
			return nil, fmt.Errorf("get object: %w", err)
		}
		recs, err := ReadEmbeddings(data)
		if err != nil {
			return nil, fmt.Errorf("parquet %s: %w", o.Key, err)
		}
		for _, rec := range recs {
			at := rec.EmbeddedAt.UTC()
			if at.Before(d.From) || !at.Before(d.To) {
				continue
			}
			if len(rec.Vector) != embeddings.Dimensions {
				continue
			}
			pts = append(pts, Point{ContentID: rec.ContentID, EmbeddedAt: at, Vector: rec.Vector})
		}
	}
	return pts, nil
}

// stride keeps max points spread uniformly over the input order.
func stride(pts []Point, max int) []Point {
	out := make([]Point, 0, max)
	step := float64(len(pts)) / float64(max)
	for i := 0; i < max; i++ {
		out = append(out, pts[int(float64(i)*step)])
	}
	return out
}

// groupMembers collects point indices per cluster label, ordered by label.
func groupMembers(labels []int) [][]int {
	byLabel := map[int][]int{}
	maxLabel := -1
	for i, l := range labels {
		if l < 0 {
			continue
		}
		byLabel[l] = append(byLabel[l], i)
		if l > maxLabel {
			maxLabel = l
		}
	}
	out := make([][]int, 0, len(byLabel))
	for l := 0; l <= maxLabel; l++ {
		if members, ok := byLabel[l]; ok {
			out = append(out, members)
		}
	}
	return out
}

func centroidOf(vecs [][]float32, members []int) []float32 {
	sub := make([][]float32, len(members))
	for i, m := range members {
		sub[i] = vecs[m]
	}
	return MeanCentroid(sub)
}

type indexedSim struct {
	idx int
	sim float64
}

// clusterOut is one discovered cluster: its member point indices, its
// centroid, and — for themes — the index of its parent topic.
type clusterOut struct {
	members  []int
	centroid []float32
	themeOf  int // -1 for topics, else topic index
}

// assignIdentity greedily matches new centroids to prior ones by cosine
// similarity (best pairs first, each side used once) at ReuseThreshold,
// reusing the prior topic_id on a match and minting a deterministic fresh
// id otherwise.
func (r *Runner) assignIdentity(clusters []clusterOut, prior []Centroid, freshID func(i int) string) []string {
	type pair struct {
		newIdx, priorIdx int
		sim              float64
	}
	var pairs []pair
	for ni := range clusters {
		for pi := range prior {
			if sim := Dot(clusters[ni].centroid, prior[pi].Vector); sim >= r.Cfg.ReuseThreshold {
				pairs = append(pairs, pair{ni, pi, sim})
			}
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].sim != pairs[j].sim {
			return pairs[i].sim > pairs[j].sim
		}
		if pairs[i].newIdx != pairs[j].newIdx {
			return pairs[i].newIdx < pairs[j].newIdx
		}
		return pairs[i].priorIdx < pairs[j].priorIdx
	})
	ids := make([]string, len(clusters))
	usedPrior := make(map[int]bool, len(prior))
	for _, p := range pairs {
		if ids[p.newIdx] != "" || usedPrior[p.priorIdx] {
			continue
		}
		ids[p.newIdx] = prior[p.priorIdx].TopicID
		usedPrior[p.priorIdx] = true
	}
	for i := range ids {
		if ids[i] == "" {
			ids[i] = freshID(i)
		}
	}
	return ids
}

// sampleTexts pulls the bounded recent text sample and embeds it,
// normalised. Both steps fail open to an empty sample: labelling then
// degrades per §8.6 instead of failing the run.
func (r *Runner) sampleTexts(ctx context.Context, creatorID string, log *slog.Logger) ([]string, [][]float32) {
	texts, err := r.Texts.Sample(ctx, creatorID, r.Cfg.TextSampleMax)
	if err != nil {
		log.Warn("text sample failed, labels will degrade", "error", err.Error())
		return nil, nil
	}
	if len(texts) == 0 {
		return nil, nil
	}
	batch := r.Cfg.EmbedBatch
	if batch <= 0 {
		batch = 64
	}
	vecs := make([][]float32, 0, len(texts))
	for lo := 0; lo < len(texts); lo += batch {
		hi := min(lo+batch, len(texts))
		vs, err := r.Embed.Embed(ctx, texts[lo:hi])
		if err != nil {
			log.Warn("embedding text sample failed, labels will degrade", "error", err.Error())
			return nil, nil
		}
		vecs = append(vecs, vs...)
	}
	for i := range vecs {
		vecs[i] = Normalize(vecs[i])
	}
	return texts, vecs
}

// labelCluster implements the §8.6 labelling ladder with the §10.6
// degradation: LLM over up to LabelPoints matched in-retention texts →
// prior label when the cluster kept a prior identity (centroid-similar)
// → generic. LLM failure is logged without any text content (P4).
func (r *Runner) labelCluster(ctx context.Context, texts []string, cands []indexedSim, prior TopicRow, generic string, log *slog.Logger) (string, string) {
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].sim != cands[j].sim {
			return cands[i].sim > cands[j].sim
		}
		return cands[i].idx < cands[j].idx
	})
	limit := r.Cfg.LabelPoints
	if limit <= 0 {
		limit = 20
	}
	if len(cands) > limit {
		cands = cands[:limit]
	}
	if len(cands) > 0 {
		sample := make([]string, len(cands))
		for i, c := range cands {
			sample[i] = texts[c.idx]
		}
		label, desc, err := r.LLM.Label(ctx, sample, prior.Label)
		if err == nil {
			return label, desc
		}
		log.Warn("llm labelling failed, falling back", "error", err.Error())
	}
	if prior.Label != "" {
		return prior.Label, prior.Description
	}
	return generic, ""
}

func (r *Runner) emitUsage(ctx context.Context, d Decision, quantity int, log *slog.Logger) error {
	if quantity == 0 {
		return nil
	}
	u := contracts.Usage{
		CreatorID:      d.CreatorID,
		EventType:      contracts.EventMessagesReclustered,
		Quantity:       int64(quantity),
		WindowStart:    d.From.UTC(),
		WindowEnd:      d.To.UTC(),
		IdempotencyKey: UsageIdempotencyKey(d),
	}
	if err := r.Usage.PublishUsage(ctx, u); err != nil {
		// Fail open (§4.7): the clustering result is written; losing one
		// usage event must not fail the run or roll anything back.
		log.Error("usage publish failed", "error", err.Error(), "idempotency_key", u.IdempotencyKey)
	}
	return nil
}
