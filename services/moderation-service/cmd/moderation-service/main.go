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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/credits"
	"dabet/pkg/embeddings"
	"dabet/pkg/kafkax"
	"dabet/pkg/policyapi"
	"dabet/pkg/service"
	"dabet/pkg/tracing"

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
	envLLMBatchSize    = "MOD_LLM_BATCH_SIZE" // A18 size trigger (default 32)
	envLLMLinger       = "MOD_LLM_LINGER"     // A18 linger trigger (default 50 ms)
	envLLMMaxIdleConns = "MOD_LLM_MAX_IDLE_CONNS"
	envDupDepth        = "MOD_DUP_DEPTH"
	envEmbDepth        = "MOD_EMB_DEPTH"
	envSemThreshold    = "MOD_SEMANTIC_THRESHOLD"
	envSamplerCapacity = "MOD_SAMPLER_CAPACITY"
	envSamplerPerMin   = "MOD_SAMPLER_REFILL_PER_MIN"
	envPolicyTTL       = "MOD_POLICY_CACHE_TTL"
	envPolicyCacheSize = "MOD_POLICY_CACHE_SIZE"
	envEmbedTimeout    = "MOD_EMBED_TIMEOUT"
	envPublishRetryMax = "MOD_PUBLISH_RETRY_MAX"

	// Redis degradation (§4.7). The breaker knobs are described on
	// mod.Breaker; the client timeouts bound what one probe can cost.
	envRedisBreakerThreshold = "MOD_REDIS_BREAKER_THRESHOLD"
	envRedisBreakerCooldown  = "MOD_REDIS_BREAKER_COOLDOWN"
	envRedisBreakerMaxCool   = "MOD_REDIS_BREAKER_MAX_COOLDOWN"
	envRedisTimeout          = "MOD_REDIS_TIMEOUT"
	envRedisMaxRetries       = "MOD_REDIS_MAX_RETRIES"
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
	if cfg.RedisBreakerThreshold, err = config.GetInt(envRedisBreakerThreshold, cfg.RedisBreakerThreshold); err != nil {
		return err
	}
	if cfg.RedisBreakerCooldown, err = config.GetDuration(envRedisBreakerCooldown, cfg.RedisBreakerCooldown); err != nil {
		return err
	}
	if cfg.RedisBreakerMaxCooldown, err = config.GetDuration(envRedisBreakerMaxCool, cfg.RedisBreakerMaxCooldown); err != nil {
		return err
	}
	// A18 documents 1 s, which is also the whole LLM allowance in §4.6's
	// indicative budget. Every batch cut off at that mark is a fail-open,
	// i.e. unmoderated messages, so the timeout must clear the model's
	// slow tail rather than sit inside it.
	//
	// Measured against a stand-in whose p99 is 2.5 s: 1 s left 42% of
	// batches timing out, 1.5 s left 3 757 fail-opens in a 90 s run, 3 s
	// left 91. Cost is nil at the SLI: only ~1.7% of messages reach this
	// stage, so p95 end to end stayed at 30 ms throughout — well inside
	// the 2 s target, which is a p95 target and not a per-message cap.
	// Retune against the real model's tail, not this number.
	llmTimeout, err := config.GetDuration(envLLMTimeout, 3*time.Second)
	if err != nil {
		return err
	}
	llmMaxIdleConns, err := config.GetInt(envLLMMaxIdleConns, mod.DefaultLLMMaxIdleConns)
	if err != nil {
		return err
	}
	// §4.6 gives the whole Redis cascade 10 ms. go-redis defaults to a 5 s
	// dial and 3 s read with three internal retries, which is three orders
	// of magnitude past that budget and is most of what a Redis outage
	// used to cost the consumer goroutine. Bound it, and let the breaker
	// (not the client) decide when to try again.
	redisTimeout, err := config.GetDuration(envRedisTimeout, 500*time.Millisecond)
	if err != nil {
		return err
	}
	redisMaxRetries, err := config.GetInt(envRedisMaxRetries, 1)
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
	// fail open; nothing blocks startup. The shared breaker inside the
	// pipeline turns a sustained outage into "skip the stage" rather than
	// "pay the failure latency again" (§4.7, and see mod.Breaker).
	//
	// Single node locally, ElastiCache in cluster mode when
	// REDIS_CLUSTER_ENABLED says so or REDIS_ADDR lists more than one
	// endpoint — §3's sharded row. §4.3's hash tags already put every key
	// of a (content, author) pair in one slot, and every Lua script here
	// touches exactly one key, so the cluster client needs nothing else.
	redisCfg, err := mod.DefaultRedisConfig(
		config.GetDefault(config.EnvRedisAddr, "localhost:6379"), redisTimeout, redisMaxRetries)
	if err != nil {
		return err
	}
	rdb, err := mod.NewRedisClient(redisCfg)
	if err != nil {
		return err
	}
	defer rdb.Close()
	log.Info("redis configured", "redis", redisCfg)
	state := mod.NewRedisState(rdb)

	// Policy gRPC client + in-process LRU (§6.8).
	policyConn, err := grpc.NewClient(
		config.GetDefault(envPolicyEndpoint, "localhost:7101"),
		append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, tracing.GRPCDialOptions()...)...,
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
		llmMaxIdleConns,
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
