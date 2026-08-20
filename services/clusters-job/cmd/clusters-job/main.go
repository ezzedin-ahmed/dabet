// Command clusters-job is the batch topic-discovery service (docs §8.6):
// a long-running scheduler that, per creator, reads embeddings parquet
// from S3, runs two-pass HDBSCAN (topics, then themes within each topic),
// labels clusters with an LLM from still-in-retention message text, and
// writes centroids to Milvus and versioned topic rows to ClickHouse. It
// also serves the JWT-authed on-demand recluster endpoint:
//
//	POST /v1/topics/recluster {from, to} -> 202 {job_id}
//
// Runs are triggered per the §8.6 table (bootstrap at >=100 embedded
// messages, count doubled since last run, >30% unassigned over the last
// hour, on demand) and emit usage.v1 messages_reclustered events with
// deterministic idempotency keys (§4.2).
//
// A run that rewrites a creator's topics also rewrites that creator's
// topic_counts for the window, so the §8.8 dashboard stops attributing
// history to superseded topic ids (§8.6 — reclustering rewrites history
// for the requesting creator). That backfill is on for on-demand and
// RUN_ONCE runs and off for periodic triggers by default; see
// CLUSTERS_BACKFILL_COUNTS and job.ValidBackfillMode.
//
// Environment (beyond the shared names of §4.4 — KAFKA_BROKERS,
// EMBEDDING_ENDPOINT, VLLM_ENDPOINT, MILVUS_ADDR, CLICKHOUSE_DSN,
// S3_ENDPOINT, JWT_SECRET, HTTP_ADDR, METRICS_ADDR):
//
//	CLUSTERS_MIN_CLUSTER_SIZE        HDBSCAN coarse min cluster size   (default 15)
//	CLUSTERS_MIN_SAMPLES             HDBSCAN coarse min samples        (default 5)
//	CLUSTERS_THEME_MIN_CLUSTER_SIZE  HDBSCAN theme-pass size           (default 5)
//	CLUSTERS_THEME_MIN_SAMPLES       HDBSCAN theme-pass samples        (default 3)
//	CLUSTERS_MAX_POINTS              per-run point cap                 (default 200000)
//	CLUSTERS_ASSIGN_THRESHOLD        text->cluster cosine floor        (default 0.75, A23)
//	CLUSTERS_REUSE_THRESHOLD         prior-topic identity cosine floor (default 0.75)
//	CLUSTERS_LABEL_POINTS            texts per cluster to the LLM      (default 20, A25)
//	CLUSTERS_TEXT_SAMPLE_MAX         recent texts sampled per run      (default 2000)
//	CLUSTERS_TEXT_SCAN_MAX           messages.v1 records scanned       (default 200000)
//	CLUSTERS_TEXT_LOOKBACK           messages.v1 scan lookback         (default 24h)
//	CLUSTERS_TEXT_BUDGET             messages.v1 scan wall-time budget (default 20s)
//	CLUSTERS_EMBED_TIMEOUT           embedding request timeout         (default 10s)
//	LLM_MODEL                        vLLM model name                   (default "moderation")
//	CLUSTERS_LLM_TIMEOUT             labelling request timeout         (default 15s)
//	CLUSTERS_TRIGGER_INTERVAL        trigger sweep period              (default 5m)
//	CLUSTERS_WINDOW_DAYS             periodic run window, days         (default 30)
//	CLUSTERS_BOOTSTRAP_MIN           bootstrap threshold               (default 100)
//	CLUSTERS_UNASSIGNED_RATE         A24 unassigned-rate threshold     (default 0.30)
//	CLUSTERS_UNASSIGNED_MIN_POINTS   min embedded points in the hour   (default 100)
//	CLUSTERS_RUN_COOLDOWN            min gap between periodic runs     (default 30m)
//	CLUSTERS_BACKFILL_COUNTS         topic_counts rewrite scope        (off|on_demand|always, default on_demand)
//	CLUSTERS_BACKFILL_LAG            newest bucket the rewrite touches (default 2h behind now)
//	CLUSTERS_S3_BYTES_PER_RECORD     listing-size -> point estimate    (default 1600)
//	S3_BUCKET                        embeddings bucket                 (default "embeddings")
//	S3_ACCESS_KEY / S3_SECRET_KEY    object store credentials          (default minioadmin
//	                                 for the static source, empty otherwise)
//	S3_SESSION_TOKEN                 static session token              (default empty)
//	S3_REGION                        bucket region                     (default empty)
//	S3_CREDENTIALS_SOURCE            static (default) | auto | chain | env | web-identity | iam
//	                                 — anything but "static" lets the pod use IRSA or an
//	                                 instance role instead of a static IAM user key
//	S3_ADDRESSING_STYLE              auto (default) | path | virtual   — auto is path style
//	                                 for MinIO and virtual-host style for S3
//
// Manual invocation: RUN_ONCE=<creator_id>:<from>:<to> (dates as
// YYYY-MM-DD, e.g. RUN_ONCE=9d4e...:2026-07-01:2026-08-01) runs exactly
// one clustering job for that creator and window — trigger label
// "manual" — then exits instead of starting the scheduler and HTTP API.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"dabet/pkg/config"
	"dabet/pkg/embeddings"
	"dabet/pkg/httpx"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

	"dabet/services/clusters-job/internal/chstore"
	"dabet/services/clusters-job/internal/httpapi"
	"dabet/services/clusters-job/internal/job"
	"dabet/services/clusters-job/internal/milvusx"
	"dabet/services/clusters-job/internal/trigger"
)

func main() {
	svc := service.New("clusters-job")
	if err := run(svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	jobCfg, err := loadJobConfig()
	if err != nil {
		return err
	}
	trigCfg, err := loadTriggerConfig()
	if err != nil {
		return err
	}
	textLookback, err := config.GetDuration("CLUSTERS_TEXT_LOOKBACK", 24*time.Hour)
	if err != nil {
		return err
	}
	textScanMax, err := config.GetInt("CLUSTERS_TEXT_SCAN_MAX", 200_000)
	if err != nil {
		return err
	}
	textBudget, err := config.GetDuration("CLUSTERS_TEXT_BUDGET", 20*time.Second)
	if err != nil {
		return err
	}
	embedTimeout, err := config.GetDuration("CLUSTERS_EMBED_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	llmTimeout, err := config.GetDuration("CLUSTERS_LLM_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}
	bytesPerRecord, err := config.GetInt("CLUSTERS_S3_BYTES_PER_RECORD", 1600)
	if err != nil {
		return err
	}
	// §5.4 access tokens: HS256 (JWT_SECRET) by default, RS256
	// (JWT_PUBLIC_KEY / JWT_PUBLIC_KEY_FILE) when JWT_ALG says so.
	verifier, err := httpx.VerifierFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	brokers := strings.Split(config.GetDefault(config.EnvKafkaBrokers, "localhost:9092"), ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Object store: static MinIO keys locally, and on AWS whatever the pod
	// actually has — S3_CREDENTIALS_SOURCE picks the credential chain so
	// IRSA works without a static IAM user key (§3, §8.4).
	s3Cfg, err := job.DefaultS3Config()
	if err != nil {
		return err
	}
	store, err := job.NewS3StoreFromConfig(s3Cfg)
	if err != nil {
		return err
	}
	svc.Logger.Info("object store configured", "s3", s3Cfg)
	ch, err := chstore.Open(config.GetDefault(config.EnvClickhouseDSN, "clickhouse://localhost:9002/dabet"))
	if err != nil {
		return err // malformed DSN, not ClickHouse being down — config error
	}
	defer ch.Close()

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	milvusAddr := config.GetDefault(config.EnvMilvusAddr, "localhost:19530")
	centroids := &lazyCentroids{}

	runner := &job.Runner{
		Store:     store,
		Centroids: centroids,
		Topics:    ch,
		Counts:    ch,
		Texts:     job.NewKafkaTextSource(brokers, textLookback, textScanMax, textBudget),
		Embed:     embeddings.NewClient(config.GetDefault(config.EnvEmbeddingEndpoint, "http://localhost:8088"), embedTimeout),
		LLM: job.NewVLLMLabeler(
			config.GetDefault(config.EnvVLLMEndpoint, "http://localhost:8089"),
			config.GetDefault("LLM_MODEL", "moderation"),
			llmTimeout,
		),
		Usage:   job.NewKafkaUsage(producer),
		Metrics: job.NewMetrics(svc.Registry),
		Log:     svc.Logger,
		Cfg:     jobCfg,
	}

	// Milvus and ClickHouse bootstrap retry in the background (§4.7): the
	// process starts with either down; runs fail (and are counted) until
	// the stores are reachable.
	svc.Metrics.DependencyUp.WithLabelValues("milvus").Set(0)
	svc.Metrics.DependencyUp.WithLabelValues("clickhouse").Set(0)
	go connectMilvus(ctx, svc, milvusAddr, centroids)
	go ensureClickhouse(ctx, svc, ch)

	if spec := os.Getenv("RUN_ONCE"); spec != "" {
		return runOnce(ctx, svc, runner, centroids, spec)
	}

	stats := &trigger.S3Stats{Store: store, BytesPerRecord: int64(bytesPerRecord)}
	sched := trigger.New(trigCfg, store, stats, ch, ch, runner, svc.Logger, nil)
	go sched.Loop(ctx)

	httpapi.Register(svc.Mux, verifier, sched, svc.Logger)

	err = svc.Run(ctx)
	cancel()
	return err
}

func loadJobConfig() (job.Config, error) {
	cfg := job.Config{}
	var err error
	if cfg.MinClusterSize, err = config.GetInt("CLUSTERS_MIN_CLUSTER_SIZE", 15); err != nil {
		return cfg, err
	}
	if cfg.MinSamples, err = config.GetInt("CLUSTERS_MIN_SAMPLES", 5); err != nil {
		return cfg, err
	}
	if cfg.ThemeMinClusterSize, err = config.GetInt("CLUSTERS_THEME_MIN_CLUSTER_SIZE", 5); err != nil {
		return cfg, err
	}
	if cfg.ThemeMinSamples, err = config.GetInt("CLUSTERS_THEME_MIN_SAMPLES", 3); err != nil {
		return cfg, err
	}
	if cfg.MaxPoints, err = config.GetInt("CLUSTERS_MAX_POINTS", 200_000); err != nil {
		return cfg, err
	}
	if cfg.AssignThreshold, err = getFloat("CLUSTERS_ASSIGN_THRESHOLD", 0.75); err != nil {
		return cfg, err
	}
	if cfg.ReuseThreshold, err = getFloat("CLUSTERS_REUSE_THRESHOLD", 0.75); err != nil {
		return cfg, err
	}
	if cfg.LabelPoints, err = config.GetInt("CLUSTERS_LABEL_POINTS", 20); err != nil {
		return cfg, err
	}
	if cfg.TextSampleMax, err = config.GetInt("CLUSTERS_TEXT_SAMPLE_MAX", 2000); err != nil {
		return cfg, err
	}
	cfg.BackfillCounts = config.GetDefault("CLUSTERS_BACKFILL_COUNTS", job.BackfillOnDemand)
	if !job.ValidBackfillMode(cfg.BackfillCounts) {
		return cfg, fmt.Errorf("environment variable CLUSTERS_BACKFILL_COUNTS: want %s, %s or %s, got %q",
			job.BackfillOff, job.BackfillOnDemand, job.BackfillAlways, cfg.BackfillCounts)
	}
	if cfg.BackfillLag, err = config.GetDuration("CLUSTERS_BACKFILL_LAG", 2*time.Hour); err != nil {
		return cfg, err
	}
	if cfg.BackfillLag < 0 {
		return cfg, fmt.Errorf("environment variable CLUSTERS_BACKFILL_LAG must not be negative")
	}
	cfg.EmbedBatch = 64
	return cfg, nil
}

func loadTriggerConfig() (trigger.Config, error) {
	cfg := trigger.Config{}
	var err error
	if cfg.Interval, err = config.GetDuration("CLUSTERS_TRIGGER_INTERVAL", 5*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.WindowDays, err = config.GetInt("CLUSTERS_WINDOW_DAYS", 30); err != nil {
		return cfg, err
	}
	bootstrapMin, err := config.GetInt("CLUSTERS_BOOTSTRAP_MIN", 100)
	if err != nil {
		return cfg, err
	}
	cfg.BootstrapMin = int64(bootstrapMin)
	if cfg.UnassignedRate, err = getFloat("CLUSTERS_UNASSIGNED_RATE", 0.30); err != nil {
		return cfg, err
	}
	minBase, err := config.GetInt("CLUSTERS_UNASSIGNED_MIN_POINTS", 100)
	if err != nil {
		return cfg, err
	}
	cfg.UnassignedMinBase = int64(minBase)
	if cfg.Cooldown, err = config.GetDuration("CLUSTERS_RUN_COOLDOWN", 30*time.Minute); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func getFloat(name string, def float64) (float64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s: %w", name, err)
	}
	return f, nil
}

// runOnce executes a single manual run (RUN_ONCE=creator:from:to, dates
// as YYYY-MM-DD) and exits. It waits up to two minutes for the Milvus
// bootstrap first.
func runOnce(ctx context.Context, svc *service.Service, runner *job.Runner, centroids *lazyCentroids, spec string) error {
	waitUntil := time.Now().Add(2 * time.Minute)
	for centroids.ptr.Load() == nil {
		if time.Now().After(waitUntil) {
			return fmt.Errorf("RUN_ONCE: milvus not reachable within two minutes")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	parts := strings.Split(spec, ":")
	if len(parts) != 3 {
		return fmt.Errorf("RUN_ONCE must be <creator_id>:<YYYY-MM-DD>:<YYYY-MM-DD>, got %q", spec)
	}
	from, err := time.Parse("2006-01-02", parts[1])
	if err != nil {
		return fmt.Errorf("RUN_ONCE from: %w", err)
	}
	to, err := time.Parse("2006-01-02", parts[2])
	if err != nil {
		return fmt.Errorf("RUN_ONCE to: %w", err)
	}
	if !from.Before(to) {
		return fmt.Errorf("RUN_ONCE: from must be before to")
	}
	d := job.Decision{CreatorID: parts[0], Trigger: job.TriggerManual, From: from.UTC(), To: to.UTC()}
	svc.Logger.Info("manual run starting", "creator_id", d.CreatorID,
		"from", parts[1], "to", parts[2])
	res, err := runner.Run(ctx, d)
	if err != nil {
		return fmt.Errorf("manual run: %w", err)
	}
	svc.Logger.Info("manual run complete", "points", res.PointsProcessed,
		"topics", res.Topics, "themes", res.Themes)
	return nil
}

// lazyCentroids is a job.CentroidStore that becomes real once the
// background connect loop reaches Milvus. Before that every call fails,
// which fails the run — it is retried on a later trigger sweep.
type lazyCentroids struct {
	ptr atomic.Pointer[milvusx.Store]
}

var errMilvusNotReady = fmt.Errorf("milvus: not connected yet")

func (l *lazyCentroids) ListByCreator(ctx context.Context, creatorID string) ([]job.Centroid, error) {
	s := l.ptr.Load()
	if s == nil {
		return nil, errMilvusNotReady
	}
	return s.ListByCreator(ctx, creatorID)
}

func (l *lazyCentroids) ReplaceCreator(ctx context.Context, creatorID string, cs []job.Centroid) error {
	s := l.ptr.Load()
	if s == nil {
		return errMilvusNotReady
	}
	return s.ReplaceCreator(ctx, creatorID, cs)
}

func connectMilvus(ctx context.Context, svc *service.Service, addr string, lazy *lazyCentroids) {
	backoff := time.Second
	for {
		mc, err := milvusx.Connect(ctx, addr)
		if err == nil {
			if err = mc.EnsureCollection(ctx); err == nil {
				lazy.ptr.Store(mc)
				svc.Metrics.DependencyUp.WithLabelValues("milvus").Set(1)
				svc.Logger.Info("milvus ready", "addr", addr)
				return
			}
			_ = mc.Close()
		}
		svc.Logger.Warn("milvus not ready, retrying", "error", err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func ensureClickhouse(ctx context.Context, svc *service.Service, ch *chstore.Store) {
	backoff := time.Second
	for {
		if err := ch.EnsureSchema(ctx); err == nil {
			svc.Metrics.DependencyUp.WithLabelValues("clickhouse").Set(1)
			svc.Logger.Info("clickhouse schema ready")
			return
		} else {
			svc.Logger.Warn("clickhouse not ready, retrying", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
