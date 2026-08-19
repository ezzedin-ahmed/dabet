// Command provider-adapter is the only component that knows a platform
// exists (docs §7.2): it turns platform chat streams into messages.v1 and
// deletions.v1 back into platform delete calls. Config from env, JSON
// logs, /healthz, /readyz, Prometheus /metrics, graceful shutdown — all
// from the shared runner.
package main

import (
	"context"
	"os"
	"strings"
	"time"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

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
	registry := driver.NewRegistry()
	registry.Register(mock)
	registry.Register(youtube.New(minter))
	registry.Register(twitch.New(minter, config.GetDefault("TWITCH_CLIENT_ID", "")))
	registry.Register(discord.New(minter))

	// Connection assignment: single-instance Static source for v1; the
	// consistent-hash coordinator (A13) slots in behind the same interface.
	seed, err := connsource.ParseEnv(config.GetDefault("ADAPTER_CONNECTIONS", ""))
	if err != nil {
		return err
	}
	source := connsource.NewStatic(seed...)

	// Mock platform HTTP surface for local e2e.
	mockdriver.NewHandlers(mock, source).Register(svc.Mux)

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	manager := ingest.NewManager(registry, source, producer, minter, m, svc.Logger)
	manager.Buffer = buffer
	manager.WatchRetry = watchRetry

	processor := deletion.NewProcessor(registry, source, deletion.StubRefresher{}, opaque.Platform, m, svc.Metrics, svc.Logger)
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

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
