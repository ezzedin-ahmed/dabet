// Package trigger evaluates the §8.6 trigger table per creator and runs
// clustering jobs — periodic sweeps and on-demand requests — one at a
// time on a scheduler loop.
//
// # Approximations, documented
//
// Nothing in the system stores per-creator embedded-message counts, so
// the triggers derive them from what exists:
//
//   - Corpus size is estimated from the S3 listing: summed object sizes
//     over the creator's date partitions divided by an approximate bytes
//     per record (env-tunable; a §8.4 record is ~1.5 KB of fp32 parquet).
//     Both the bootstrap threshold (first ≥100 messages embedded) and the
//     count-doubled trigger use this estimate.
//   - The unassigned rate is approximated as
//     1 - assigned/embedded over the last hour, where assigned is the
//     ClickHouse topic_counts sum written by clustering-service's live
//     path and embedded is the S3 size estimate over objects modified in
//     the last hour. clustering-service does not persist unassigned
//     counts, so this derivation is the documented substitute (A24).
//
// Per-creator job state (last run, last count, last version) is persisted
// in the documented clusters_job_state ClickHouse table so triggers
// survive restarts.
package trigger

import (
	"context"
	"log/slog"
	"time"

	"dabet/services/clusters-job/internal/chstore"
	"dabet/services/clusters-job/internal/job"
)

// CreatorLister enumerates creators present in the embeddings bucket.
type CreatorLister interface {
	ListCreators(ctx context.Context) ([]string, error)
}

// CorpusStats estimates embedded-point counts from the object store.
type CorpusStats interface {
	// EstimatePoints estimates the creator's embedded points over the
	// window's date partitions.
	EstimatePoints(ctx context.Context, creatorID string, from, to time.Time) (int64, error)
	// EstimateRecentPoints estimates points in objects modified since.
	EstimateRecentPoints(ctx context.Context, creatorID string, since time.Time) (int64, error)
}

// AssignedCounts sums live topic_counts assignments (chstore.Store).
type AssignedCounts interface {
	AssignedSince(ctx context.Context, creatorID string, since time.Time) (int64, error)
}

// StateStore persists per-creator job state (chstore.Store).
type StateStore interface {
	GetState(ctx context.Context, creatorID string) (chstore.State, bool, error)
	PutState(ctx context.Context, st chstore.State) error
}

// Runner executes one clustering run (job.Runner).
type Runner interface {
	Run(ctx context.Context, d job.Decision) (job.Result, error)
}

// Config are the trigger tunables (§8.6 table; defaults per A24 and the
// documented assumptions).
type Config struct {
	Interval           time.Duration // sweep period (default 5m)
	WindowDays         int           // periodic run window (default 30)
	BootstrapMin       int64         // first-run threshold (default 100)
	UnassignedRate     float64       // A24 (default 0.30)
	UnassignedMinBase  int64         // min embedded points in the hour before the rate is meaningful (default 100)
	Cooldown           time.Duration // min gap between periodic runs per creator (default 30m)
	OnDemandQueueDepth int           // recluster queue (default 64)
}

// Scheduler owns the loop. Construct with New.
type Scheduler struct {
	cfg      Config
	lister   CreatorLister
	stats    CorpusStats
	assigned AssignedCounts
	state    StateStore
	runner   Runner
	log      *slog.Logger
	now      func() time.Time
	ondemand chan job.Decision
}

// New builds a scheduler. now is injectable for tests.
func New(cfg Config, lister CreatorLister, stats CorpusStats, assigned AssignedCounts,
	state StateStore, runner Runner, log *slog.Logger, now func() time.Time) *Scheduler {
	if now == nil {
		now = time.Now
	}
	depth := cfg.OnDemandQueueDepth
	if depth <= 0 {
		depth = 64
	}
	return &Scheduler{
		cfg: cfg, lister: lister, stats: stats, assigned: assigned,
		state: state, runner: runner, log: log, now: now,
		ondemand: make(chan job.Decision, depth),
	}
}

// Enqueue queues an on-demand recluster; false when the queue is full.
func (s *Scheduler) Enqueue(d job.Decision) bool {
	select {
	case s.ondemand <- d:
		return true
	default:
		return false
	}
}

// Loop runs until ctx is cancelled: on-demand jobs as they arrive,
// periodic sweeps every Interval. Runs execute sequentially — clustering
// is CPU-bound and correctness never depends on parallelism.
func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-s.ondemand:
			s.execute(ctx, d)
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Scheduler) sweep(ctx context.Context) {
	creators, err := s.lister.ListCreators(ctx)
	if err != nil {
		s.log.Error("trigger sweep: list creators", "error", err.Error())
		return
	}
	for _, creator := range creators {
		if ctx.Err() != nil {
			return
		}
		// Drain any on-demand requests between creators so a recluster is
		// not stuck behind a long sweep.
		for {
			select {
			case d := <-s.ondemand:
				s.execute(ctx, d)
				continue
			default:
			}
			break
		}
		d, ok, err := s.Evaluate(ctx, creator)
		if err != nil {
			s.log.Error("trigger evaluation failed", "creator_id", creator, "error", err.Error())
			continue
		}
		if ok {
			s.execute(ctx, d)
		}
	}
}

// Evaluate applies the §8.6 trigger table for one creator, in priority
// order: bootstrap, count doubled, unassigned rate.
func (s *Scheduler) Evaluate(ctx context.Context, creatorID string) (job.Decision, bool, error) {
	now := s.now().UTC().Truncate(time.Minute)
	to := now
	from := to.AddDate(0, 0, -s.cfg.WindowDays)
	d := job.Decision{CreatorID: creatorID, From: from, To: to}

	st, ran, err := s.state.GetState(ctx, creatorID)
	if err != nil {
		return job.Decision{}, false, err
	}
	total, err := s.stats.EstimatePoints(ctx, creatorID, from, to)
	if err != nil {
		return job.Decision{}, false, err
	}

	if !ran {
		if total >= s.cfg.BootstrapMin {
			d.Trigger = job.TriggerBootstrap
			return d, true, nil
		}
		return job.Decision{}, false, nil
	}
	if now.Sub(st.LastRunAt) < s.cfg.Cooldown {
		return job.Decision{}, false, nil
	}
	if st.LastPointCount > 0 && total >= 2*st.LastPointCount {
		d.Trigger = job.TriggerDoubled
		return d, true, nil
	}

	hourAgo := now.Add(-time.Hour)
	embedded, err := s.stats.EstimateRecentPoints(ctx, creatorID, hourAgo)
	if err != nil {
		return job.Decision{}, false, err
	}
	if embedded >= s.cfg.UnassignedMinBase {
		assigned, err := s.assigned.AssignedSince(ctx, creatorID, hourAgo)
		if err != nil {
			return job.Decision{}, false, err
		}
		rate := 1 - float64(assigned)/float64(embedded)
		if rate > s.cfg.UnassignedRate {
			d.Trigger = job.TriggerUnassigned
			return d, true, nil
		}
	}
	return job.Decision{}, false, nil
}

func (s *Scheduler) execute(ctx context.Context, d job.Decision) {
	res, err := s.runner.Run(ctx, d)
	if err != nil {
		s.log.Error("clustering run failed", "creator_id", d.CreatorID,
			"trigger", d.Trigger, "error", err.Error())
		return
	}
	// On-demand runs rewrite a historical window; they do not reset the
	// periodic baseline.
	if d.Trigger == job.TriggerOnDemand {
		return
	}
	if err := s.state.PutState(ctx, chstore.State{
		CreatorID:      d.CreatorID,
		LastRunAt:      s.now().UTC(),
		LastPointCount: int64(res.PointsRead),
	}); err != nil {
		s.log.Error("persisting job state failed", "creator_id", d.CreatorID, "error", err.Error())
	}
}

// S3Stats implements CorpusStats over the job.S3Store listing, estimating
// record counts as size / bytesPerRecord (see the package comment).
type S3Stats struct {
	Store          *job.S3Store
	BytesPerRecord int64
}

// EstimatePoints implements CorpusStats.
func (s *S3Stats) EstimatePoints(ctx context.Context, creatorID string, from, to time.Time) (int64, error) {
	objs, err := s.Store.ListCreatorObjects(ctx, creatorID, from, to)
	if err != nil {
		return 0, err
	}
	var bytes int64
	for _, o := range objs {
		bytes += o.Size
	}
	return bytes / s.BytesPerRecord, nil
}

// EstimateRecentPoints implements CorpusStats over objects modified since
// (listing only the date partitions the window can touch).
func (s *S3Stats) EstimateRecentPoints(ctx context.Context, creatorID string, since time.Time) (int64, error) {
	objs, err := s.Store.ListCreatorObjects(ctx, creatorID, since, since.Add(48*time.Hour))
	if err != nil {
		return 0, err
	}
	var bytes int64
	for _, o := range objs {
		if o.LastModified.Before(since) {
			continue
		}
		bytes += o.Size
	}
	return bytes / s.BytesPerRecord, nil
}
