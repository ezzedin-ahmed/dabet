// Command moderation-service runs the §7.3 moderation cascade: a consumer
// group on messages.v1, the cheap-first first-hit-wins detector chain,
// batched LLM classification, verdict publishing to flagged.v1 /
// deletions.v1, and per-minute usage emission to usage.v1.
//
// Per §4.5 the service STAYS ready with dead dependencies: everything
// fails open (counted in fail_open_total) and consumption continues.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/credits"
	"dabet/pkg/embeddings"
	"dabet/pkg/kafkax"
	"dabet/pkg/policyapi"
	"dabet/pkg/service"

	"dabet/services/moderation-service/internal/mod"
)

// Service-specific environment variables; shared names live in pkg/config.
const (
	envPolicyEndpoint  = "POLICY_ENDPOINT"  // policy-service gRPC addr
	envCreditsEndpoint = "CREDITS_ENDPOINT" // credits-service base URL (§5.8; ships later, client tolerates it down)
	envConsumerGroup   = "MOD_CONSUMER_GROUP"
	envInstanceID      = "INSTANCE_ID"
	envLLMModel        = "LLM_MODEL"
	envLLMTimeout      = "MOD_LLM_TIMEOUT"
	envLLMBatchSize    = "MOD_LLM_BATCH_SIZE"
	envLLMLinger       = "MOD_LLM_LINGER"
	envDupDepth        = "MOD_DUP_DEPTH"
	envEmbDepth        = "MOD_EMB_DEPTH"
	envSemThreshold    = "MOD_SEMANTIC_THRESHOLD"
	envSamplerCapacity = "MOD_SAMPLER_CAPACITY"
	envSamplerPerMin   = "MOD_SAMPLER_REFILL_PER_MIN"
	envPolicyTTL       = "MOD_POLICY_CACHE_TTL"
	envPolicyCacheSize = "MOD_POLICY_CACHE_SIZE"
	envEmbedTimeout    = "MOD_EMBED_TIMEOUT"
	envPublishRetryMax = "MOD_PUBLISH_RETRY_MAX"
)

// getFloat parses an optional float env var, returning def when unset.
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

func main() {
	svc := service.New("moderation-service")
	log := svc.Logger
	if err := run(svc); err != nil {
		log.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	log := svc.Logger

	cfg := mod.DefaultConfig()
	var err error
	if cfg.LLMBatchSize, err = config.GetInt(envLLMBatchSize, cfg.LLMBatchSize); err != nil {
		return err
	}
	if cfg.LLMLinger, err = config.GetDuration(envLLMLinger, cfg.LLMLinger); err != nil {
		return err
	}
	if cfg.DupDepth, err = config.GetInt(envDupDepth, cfg.DupDepth); err != nil {
		return err
	}
	if cfg.EmbDepth, err = config.GetInt(envEmbDepth, cfg.EmbDepth); err != nil {
		return err
	}
	if cfg.SemanticThreshold, err = getFloat(envSemThreshold, cfg.SemanticThreshold); err != nil {
		return err
	}
	if cfg.SamplerCapacity, err = getFloat(envSamplerCapacity, cfg.SamplerCapacity); err != nil {
		return err
	}
	if cfg.SamplerPerMin, err = getFloat(envSamplerPerMin, cfg.SamplerPerMin); err != nil {
		return err
	}
	llmTimeout, err := config.GetDuration(envLLMTimeout, time.Second)
	if err != nil {
		return err
	}
	policyTTL, err := config.GetDuration(envPolicyTTL, 60*time.Second)
	if err != nil {
		return err
	}
	policySize, err := config.GetInt(envPolicyCacheSize, 100_000)
	if err != nil {
		return err
	}
	embedTimeout, err := config.GetDuration(envEmbedTimeout, time.Second)
	if err != nil {
		return err
	}
	publishRetryMax, err := config.GetDuration(envPublishRetryMax, 30*time.Second)
	if err != nil {
		return err
	}

	brokers := strings.Split(config.GetDefault(config.EnvKafkaBrokers, "localhost:9092"), ",")
	group := config.GetDefault(envConsumerGroup, "moderation-service")
	instanceID := config.GetDefault(envInstanceID, "")
	if instanceID == "" {
		if instanceID, err = os.Hostname(); err != nil {
			return err
		}
	}

	met := mod.NewMetrics(svc.Registry, svc.Metrics)

	// Redis (go-redis v9). Connection failures surface per operation and
	// fail open; nothing blocks startup.
	rdb := redis.NewClient(&redis.Options{Addr: config.GetDefault(config.EnvRedisAddr, "localhost:6379")})
	state := mod.NewRedisState(rdb)

	// Policy gRPC client + in-process LRU (§6.8).
	policyConn, err := grpc.NewClient(
		config.GetDefault(envPolicyEndpoint, "localhost:7101"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer policyConn.Close()
	policies := mod.NewPolicyCache(policyapi.NewPolicyServiceClient(policyConn), policyTTL, policySize, time.Second, time.Now)

	// Credits advisory flag (§5.8). The endpoint ships later; the client
	// fails open (true) while it is absent. Transport fail-opens are
	// counted separately from the deliberate no_credits pass.
	creditsClient := credits.NewClient(config.GetDefault(envCreditsEndpoint, "http://localhost:7201"))
	creditsClient.OnFailOpen = func(error) {
		svc.Metrics.FailOpenTotal.WithLabelValues("credits", "transport").Inc()
	}

	embedClient := embeddings.NewClient(config.GetDefault(config.EnvEmbeddingEndpoint, "http://localhost:8088"), embedTimeout)
	llmClient := mod.NewLLMClient(
		config.GetDefault(config.EnvVLLMEndpoint, "http://localhost:8089"),
		config.GetDefault(envLLMModel, "moderation"),
		llmTimeout,
	)

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	pub := mod.NewPublisher(producer, publishRetryMax, func() {
		svc.Metrics.FailOpenTotal.WithLabelValues("kafka", "").Inc()
	})
	usage := mod.NewUsageAggregator(instanceID, pub, time.Now)

	pipe := mod.NewPipeline(cfg, policies, creditsClient, state, embedClient, llmClient, pub, usage, met, time.Now)

	consumer, err := kafkax.NewConsumer(brokers, group, []string{contracts.TopicMessages}, pipe.Handler(group))
	if err != nil {
		return err
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pipe.RunBackground(ctx)
	go func() {
		// The consumer restarts on transient errors: losing Kafka must not
		// kill the process, and readiness stays true throughout (§4.5).
		for ctx.Err() == nil {
			if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("consumer error, restarting", "error", err.Error())
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
				}
			}
		}
	}()

	runErr := svc.Run(ctx)

	// Graceful drain: dispatch pending LLM batches, flush usage (§7.10).
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	pipe.Shutdown(shutdownCtx)
	return runErr
}
