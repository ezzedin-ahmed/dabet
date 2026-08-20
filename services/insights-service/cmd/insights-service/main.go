// Command insights-service runs the insights ingestion pipeline (docs
// §8.2–8.4): it consumes messages.v1 and flagged.v1, holds every message in
// a short exclusion buffer so flags can veto it, samples per content,
// embeds the survivors in batches, and writes creator/date-partitioned
// parquet embedding files to S3. It stores no text and no author_id,
// anywhere, ever (§4.8, P4).
//
// Each successfully embedded batch is also handed fire-and-forget to
// clustering-service's internal assign API for live classification (§8.5) —
// clustering being down never blocks or fails the parquet path. And it
// serves the creator-facing topics API (§8.8) over ClickHouse.
//
// Environment (beyond the shared names of §4.4 — KAFKA_BROKERS,
// EMBEDDING_ENDPOINT, S3_ENDPOINT, CLICKHOUSE_DSN, JWT_SECRET, HTTP_ADDR,
// METRICS_ADDR):
//
//	CLUSTERING_ENDPOINT            clustering-service base URL (default
//	                               http://localhost:8085; set empty to
//	                               disable live assignment entirely)
//	INSIGHTS_ASSIGN_TIMEOUT_MS     assign request timeout    (default 2000)
//	INSIGHTS_ASSIGN_QUEUE_BATCHES  assign queue depth        (default 256)
//	INSIGHTS_BUFFER_SECONDS        exclusion window          (default 2, §8.3/A20)
//	INSIGHTS_BUFFER_MAX_MESSAGES   buffer size bound         (default 200000)
//	INSIGHTS_SAMPLE_PER_MINUTE     per-content refill        (default 60, §8.4)
//	INSIGHTS_SAMPLE_CAPACITY       per-content burst         (default 60)
//	INSIGHTS_EMBED_BATCH_SIZE      embed batch size          (default 64)
//	INSIGHTS_EMBED_LINGER_MS       embed batch linger        (default 250)
//	INSIGHTS_EMBED_TIMEOUT_MS      embed request timeout     (default 5000)
//	INSIGHTS_S3_ROLL_BYTES         parquet roll size         (default 8388608)
//	INSIGHTS_S3_ROLL_SECONDS       parquet roll age          (default 60)
//	S3_BUCKET                      embeddings bucket         (default "embeddings")
//	S3_ACCESS_KEY / S3_SECRET_KEY  object store credentials  (default minioadmin
//	                               for the static source, empty otherwise)
//	S3_SESSION_TOKEN               static session token      (default empty)
//	S3_REGION                      bucket region             (default empty)
//	S3_CREDENTIALS_SOURCE          static (default) | auto | chain | env |
//	                               web-identity | iam. Anything but "static"
//	                               lets the pod use IRSA or an instance role
//	                               instead of a static IAM user key.
//	S3_ADDRESSING_STYLE            auto (default) | path | virtual — auto is
//	                               path style for MinIO, virtual-host for S3
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/embeddings"
	"dabet/pkg/httpx"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

	"dabet/services/insights-service/internal/ingest"
	"dabet/services/insights-service/internal/topics"
)

func main() {
	svc := service.New("insights-service")
	if err := run(svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	bufferSecs, err := config.GetInt("INSIGHTS_BUFFER_SECONDS", 2)
	if err != nil {
		return err
	}
	bufferMax, err := config.GetInt("INSIGHTS_BUFFER_MAX_MESSAGES", 200_000)
	if err != nil {
		return err
	}
	samplePerMinute, err := config.GetInt("INSIGHTS_SAMPLE_PER_MINUTE", 60)
	if err != nil {
		return err
	}
	sampleCapacity, err := config.GetInt("INSIGHTS_SAMPLE_CAPACITY", 60)
	if err != nil {
		return err
	}
	batchSize, err := config.GetInt("INSIGHTS_EMBED_BATCH_SIZE", 64)
	if err != nil {
		return err
	}
	lingerMS, err := config.GetInt("INSIGHTS_EMBED_LINGER_MS", 250)
	if err != nil {
		return err
	}
	embedTimeoutMS, err := config.GetInt("INSIGHTS_EMBED_TIMEOUT_MS", 5_000)
	if err != nil {
		return err
	}
	rollBytes, err := config.GetInt("INSIGHTS_S3_ROLL_BYTES", 8<<20)
	if err != nil {
		return err
	}
	rollSecs, err := config.GetInt("INSIGHTS_S3_ROLL_SECONDS", 60)
	if err != nil {
		return err
	}
	assignTimeoutMS, err := config.GetInt("INSIGHTS_ASSIGN_TIMEOUT_MS", 2_000)
	if err != nil {
		return err
	}
	assignQueue, err := config.GetInt("INSIGHTS_ASSIGN_QUEUE_BATCHES", 256)
	if err != nil {
		return err
	}

	brokers := strings.Split(config.GetDefault(config.EnvKafkaBrokers, "localhost:9092"), ",")
	embedEndpoint := config.GetDefault(config.EnvEmbeddingEndpoint, "http://localhost:8091")
	// Object store: static MinIO keys locally, and on AWS whatever the
	// pod actually has — S3_CREDENTIALS_SOURCE picks the credential chain
	// so IRSA works without a static IAM user key (§3, §8.4).
	s3Cfg, err := ingest.DefaultS3Config()
	if err != nil {
		return err
	}
	// CLUSTERING_ENDPOINT set-but-empty means "live classification (§8.5)
	// is not part of this deployment", which is different from unset.
	// config.GetDefault cannot express that distinction — it substitutes
	// the default for an empty value — so read the variable directly.
	clusteringEndpoint, clusteringSet := os.LookupEnv("CLUSTERING_ENDPOINT")
	if !clusteringSet {
		clusteringEndpoint = "http://localhost:8085"
	}
	clusteringEndpoint = strings.TrimSpace(clusteringEndpoint)
	chDSN := config.GetDefault(config.EnvClickhouseDSN, "clickhouse://localhost:9002/dabet")
	// §5.4 access tokens: HS256 (JWT_SECRET) by default, RS256
	// (JWT_PUBLIC_KEY / JWT_PUBLIC_KEY_FILE) when JWT_ALG says so.
	verifier, err := httpx.VerifierFromEnv(os.Getenv)
	if err != nil {
		return err
	}

	store, err := ingest.NewS3StoreFromConfig(s3Cfg)
	if err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	svc.Logger.Info("object store configured", "s3", s3Cfg)
	topicStore, err := topics.OpenCH(chDSN)
	if err != nil {
		return fmt.Errorf("topic store: %w", err)
	}
	defer topicStore.Close()
	topics.Register(svc.Mux, verifier, topicStore, svc.Logger)

	metrics := ingest.NewMetrics(svc.Registry)
	embedClient := embeddings.NewClient(embedEndpoint, time.Duration(embedTimeoutMS)*time.Millisecond)
	// An empty CLUSTERING_ENDPOINT means live classification is not part
	// of this deployment (§8.5 is optional; the vectors are in S3 either
	// way and the next clusters-job run picks them up). Posting to a
	// service that was never deployed would climb fail_open_total forever
	// and make the §4.5 alert useless, so it is switched off instead.
	var assigner ingest.AssignSender = ingest.NoopAssigner{}
	if clusteringEndpoint != "" {
		assigner = ingest.NewAsyncAssigner(clusteringEndpoint,
			time.Duration(assignTimeoutMS)*time.Millisecond, assignQueue,
			svc.Metrics.FailOpenTotal, svc.Metrics.DependencyUp)
	} else {
		svc.Logger.Info("CLUSTERING_ENDPOINT is empty: live centroid assignment is disabled")
	}
	pipeline := ingest.NewPipeline(
		ingest.NewBuffer(time.Duration(bufferSecs)*time.Second, bufferMax, metrics),
		ingest.NewSampler(float64(samplePerMinute), float64(sampleCapacity), 500_000),
		ingest.NewBatcher(batchSize, time.Duration(lingerMS)*time.Millisecond),
		ingest.NewEmbedder(embedClient, metrics, svc.Metrics.FailOpenTotal),
		ingest.NewRoller(store, rollBytes, time.Duration(rollSecs)*time.Second, metrics, svc.Metrics.FailOpenTotal),
		assigner,
		metrics,
		svc.Metrics,
		svc.Logger,
		50*time.Millisecond,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		assigner.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, svc, contracts.TopicMessages, brokers, pipeline.HandleMessage)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, svc, contracts.TopicFlagged, brokers, pipeline.HandleFlagged)
	}()
	go func() {
		defer wg.Done()
		pipeline.Run(ctx)
	}()

	// Blocks until SIGTERM/SIGINT, then the consumers stop and the pipeline
	// drains (counting the lost buffer as restart drops) before we return.
	err = svc.Run(ctx)
	cancel()
	wg.Wait()
	return err
}

// runConsumer keeps one consumer-group member alive on topic until ctx is
// cancelled. Kafka being unreachable must not take the process down (§4.7):
// failures are logged, dependency_up is flipped, and the consumer rejoins
// with backoff. Handlers never return errors, so any Run error is transport.
func runConsumer(ctx context.Context, svc *service.Service, topic string, brokers []string, h kafkax.Handler) {
	const backoff = 2 * time.Second
	for ctx.Err() == nil {
		c, err := kafkax.NewConsumer(brokers, ingest.ConsumerGroup, []string{topic}, h)
		if err == nil {
			svc.Metrics.DependencyUp.WithLabelValues("kafka").Set(1)
			err = c.Run(ctx)
			c.Close()
		}
		if ctx.Err() != nil {
			return
		}
		svc.Metrics.DependencyUp.WithLabelValues("kafka").Set(0)
		svc.Logger.Error("kafka consumer restarting", "topic", topic, "error", err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}
