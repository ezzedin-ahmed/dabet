// Command provider-adapter is the only component that knows a platform
// exists (docs §7.2): it turns platform chat streams into messages.v1 and
// deletions.v1 back into platform delete calls. Config from env, JSON
// logs, /healthz, /readyz, Prometheus /metrics, graceful shutdown — all
// from the shared runner.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

	"dabet/services/provider-adapter/internal/connpg"
	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/deletion"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/drivers/discord"
	"dabet/services/provider-adapter/internal/drivers/twitch"
	"dabet/services/provider-adapter/internal/drivers/youtube"
	"dabet/services/provider-adapter/internal/ingest"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/mockdriver"
	"dabet/services/provider-adapter/internal/opaque"
	"dabet/services/provider-adapter/internal/quota"
	"dabet/services/provider-adapter/internal/refresh"
	"dabet/services/provider-adapter/internal/shard"
)

func main() {
	svc := service.New("provider-adapter")
	if err := run(svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokersEnv, err := config.Get(config.EnvKafkaBrokers)
	if err != nil {
		return err
	}
	brokers := splitCSV(brokersEnv)

	buffer, err := config.GetInt("ADAPTER_INGEST_BUFFER", 256)
	if err != nil {
		return err
	}
	watchRetry, err := config.GetDuration("ADAPTER_WATCH_RETRY", 2*time.Second)
	if err != nil {
		return err
	}
	deleteBackoff, err := config.GetDuration("ADAPTER_DELETE_BACKOFF_BASE", 200*time.Millisecond)
	if err != nil {
		return err
	}

	m := metrics.New(svc.Registry)
	minter := opaque.NewMinter()

	// Drivers. Adding a platform is one implementation plus one Register
	// call and one opaque tag (N3).
	mock := mockdriver.New(nil)
	// Like the real drivers, the mock resolves opaque ids back to native
	// ones before "calling the platform" — which is what makes an
	// injected message correlatable with its deletion.
	mock.Resolver = minter
	registry := driver.NewRegistry()
	registry.Register(mock)

	// YouTube's daily quota is the binding constraint on how often it can
	// poll, so it is the one ingestion knob worth exposing: raise it when
	// Google raises the project allowance, set 0 for a quota-exempt
	// deployment. See the youtube package comment for the cost model.
	ytQuota, err := config.GetInt("ADAPTER_YOUTUBE_DAILY_QUOTA", youtube.DefaultDailyQuota)
	if err != nil {
		return err
	}
	ytDiscovery, err := config.GetDuration("ADAPTER_YOUTUBE_DISCOVERY_INTERVAL", youtube.DefaultDiscoveryInterval)
	if err != nil {
		return err
	}
	yt := youtube.New(minter)
	yt.Budget = quota.New(ytQuota)
	yt.DiscoveryInterval = ytDiscovery
	yt.Log = svc.Logger
	registry.Register(yt)

	tw := twitch.New(minter, config.GetDefault("TWITCH_CLIENT_ID", ""))
	tw.Log = svc.Logger
	registry.Register(tw)

	// 0 shards means "use whatever GET /gateway/bot recommends", which is
	// the right answer until a bot outgrows one instance's socket budget.
	dcShards, err := config.GetInt("ADAPTER_DISCORD_SHARDS", 0)
	if err != nil {
		return err
	}
	dc := discord.New(minter)
	dc.Shards = dcShards
	dc.Log = svc.Logger
	registry.Register(dc)

	// Connection assignment: env-seeded Static source (also the mock
	// injection endpoint's registry), plus — when POSTGRES_DSN is set —
	// the Postgres-backed poller over Area A's identity.connections
	// (§5.5/§5.6). The consistent-hash coordinator (A13) slots in behind
	// the same interface.
	seed, err := connsource.ParseEnv(config.GetDefault("ADAPTER_CONNECTIONS", ""))
	if err != nil {
		return err
	}
	static := connsource.NewStatic(seed...)

	// Mock platform HTTP surface for local e2e.
	mockdriver.NewHandlers(mock, static).Register(svc.Mux)

	var source connsource.Source = static
	var refresher deletion.TokenRefresher = deletion.StubRefresher{}
	if dsn := config.GetDefault(config.EnvPostgresDSN, ""); dsn != "" {
		pollInterval, err := config.GetDuration("ADAPTER_CONNSOURCE_POLL", 15*time.Second)
		if err != nil {
			return err
		}
		pool, err := connectWithRetry(ctx, svc, dsn)
		if err != nil {
			return err
		}
		defer pool.Close()

		store := connpg.New(pool)
		poller := connsource.NewPoller(store, pollInterval, svc.Logger)
		if err := poller.Load(ctx); err != nil {
			svc.Logger.Warn("initial connection load failed; will retry on poll", "error", err.Error())
		}
		go func() {
			if err := poller.Run(ctx); err != nil && ctx.Err() == nil {
				svc.Logger.Error("connection poller exited", "error", err.Error())
			}
		}()

		multi := connsource.NewMulti(poller, static)
		multi.Forward(ctx)
		source = multi

		// §5.6 lazy refresh, in place of the stub: advisory-locked
		// re-read + refresh-token exchange, expired-on-auth-error.
		refresher = refresh.New(store, refresh.EndpointsFromEnv(config.GetDefault),
			m.ConnectionRefreshTotal, svc.Logger, poller.Evict, poller.Update)
	}

	// A13 sharding (§7.2). Off by default: with it off `source` reaches the
	// ingest manager untouched and the adapter is byte-for-byte the
	// single-instance service it was, which is what `make up`, `make e2e`
	// and the load harness rely on. On, a shard.Filter narrows the same
	// Source to this instance's ring segment.
	//
	// Only ingestion is sharded. The deletion consumer keeps the unfiltered
	// source: deletions.v1 is partitioned by content_id and its consumer
	// group's assignment is unrelated to the ring, so any instance can be
	// handed a deletion for a connection another instance watches and must
	// still be able to look up its credentials.
	ingestSource := source
	if sharding, err := envBool("ADAPTER_SHARDING_ENABLED", false); err != nil {
		return err
	} else if sharding {
		filter, closeCoord, err := startSharding(ctx, svc, m, brokers, source)
		if err != nil {
			return err
		}
		defer closeCoord()
		ingestSource = filter
	}

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	manager := ingest.NewManager(registry, ingestSource, producer, minter, m, svc.Logger)
	manager.Buffer = buffer
	manager.WatchRetry = watchRetry
	// A watch is long-lived, so an access token can expire while a stream
	// is running; the same §5.6 refresher the deletion consumer uses turns
	// that 401 into a reconnect instead of a dead stream.
	if r, ok := refresher.(ingest.TokenRefresher); ok {
		manager.Refresher = r
	}

	processor := deletion.NewProcessor(registry, source, refresher, opaque.Platform, m, svc.Metrics, svc.Logger)
	processor.BaseBackoff = deleteBackoff

	consumer, err := kafkax.NewConsumer(brokers, deletion.Group, []string{contracts.TopicDeletions}, processor.Handle)
	if err != nil {
		return err
	}
	defer consumer.Close()

	go func() {
		if err := manager.Run(ctx); err != nil && ctx.Err() == nil {
			svc.Logger.Error("ingest manager exited", "error", err.Error())
		}
	}()
	go func() {
		// Deletion is best-effort (P2): a broken broker must not kill the
		// process, so the consumer restarts with backoff until shutdown.
		for ctx.Err() == nil {
			if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
				svc.Logger.Error("deletion consumer failed; restarting", "error", err.Error())
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
			}
		}
	}()

	err = svc.Run(ctx)
	cancel()
	return err
}

// startSharding wires the A13 coordinator and the segment filter, and
// returns the filter plus a close func for the coordinator's client.
//
// Everything here fails fast at startup: a misconfigured shard cap or an
// unresolvable instance ID is an operator error, and starting anyway would
// mean an instance silently watching the wrong set of connections. That is
// not a P2 situation — P2 governs a *running* service losing a dependency,
// which is handled in shard.KafkaCoordinator by holding the last view.
func startSharding(ctx context.Context, svc *service.Service, m *metrics.Metrics, brokers []string, source connsource.Source) (*shard.Filter, func(), error) {
	self := config.GetDefault("ADAPTER_INSTANCE_ID", "")
	if self == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, nil, fmt.Errorf("adapter sharding: no ADAPTER_INSTANCE_ID and hostname unavailable: %w", err)
		}
		self = host
	}
	maxConns, err := config.GetInt("ADAPTER_SHARD_MAX_CONNECTIONS", shard.DefaultMaxConnections)
	if err != nil {
		return nil, nil, err
	}
	replicas, err := config.GetInt("ADAPTER_SHARD_REPLICAS", shard.DefaultReplicas)
	if err != nil {
		return nil, nil, err
	}
	sessionTimeout, err := config.GetDuration("ADAPTER_SHARD_SESSION_TIMEOUT", shard.DefaultSessionTimeout)
	if err != nil {
		return nil, nil, err
	}

	coord, err := shard.NewKafkaCoordinator(shard.KafkaConfig{
		Brokers:        brokers,
		Group:          config.GetDefault("ADAPTER_SHARD_GROUP", shard.DefaultGroup),
		Topic:          config.GetDefault("ADAPTER_SHARD_TOPIC", shard.DefaultTopic),
		Self:           self,
		SessionTimeout: sessionTimeout,
		Up:             svc.Metrics.DependencyUp.WithLabelValues(shard.DependencyName),
		Log:            svc.Logger,
	})
	if err != nil {
		return nil, nil, err
	}
	// Report the coordinator down until the first successful sync, so
	// dependency_up is never optimistically 1 during a cold start that
	// never completes.
	svc.Metrics.DependencyUp.WithLabelValues(shard.DependencyName).Set(0)

	go func() {
		if err := coord.Run(ctx); err != nil && ctx.Err() == nil {
			svc.Logger.Error("shard coordinator exited", "error", err.Error())
		}
	}()

	filter := shard.NewFilter(source, coord, replicas, maxConns, m, svc.Logger)
	filter.Forward(ctx)
	svc.Logger.Info("connection sharding enabled",
		"instance_id", self, "max_connections", maxConns, "virtual_nodes", replicas)
	return filter, coord.Close, nil
}

// envBool parses a boolean env var. pkg/config has no bool accessor and
// this is the only service that needs one.
func envBool(name string, def bool) (bool, error) {
	raw := config.GetDefault(name, "")
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("config: %s must be a boolean: %w", name, err)
	}
	return v, nil
}

// connectWithRetry tolerates Compose start ordering: Postgres may accept
// connections a few seconds after provider-adapter starts.
func connectWithRetry(ctx context.Context, svc *service.Service, dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		svc.Logger.Warn("postgres not ready, retrying", "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return nil, lastErr
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
