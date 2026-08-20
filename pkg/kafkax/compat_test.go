package kafkax_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/kafkax"
)

// This file is deliberately an external test package: it may only use
// kafkax's exported API, so it fails to compile if the public surface
// stops being source-compatible with the call sites in services/.

// consumerFactory is the exact signature every existing caller uses:
// four arguments, no options. moderation-service, insights-service,
// credits-service and provider-adapter all call NewConsumer this way, so
// this assignment is the compile-time half of the compatibility check.
var consumerFactory func([]string, string, []string, kafkax.Handler) (*kafkax.Consumer, error) = func(
	brokers []string, group string, topics []string, h kafkax.Handler,
) (*kafkax.Consumer, error) {
	return kafkax.NewConsumer(brokers, group, topics, h)
}

// handlerShape mirrors mod.Pipeline.Handler and deletion.Processor.Handle:
// a func(context.Context, *kgo.Record) error assigned straight to
// kafkax.Handler.
var handlerShape kafkax.Handler = func(ctx context.Context, rec *kgo.Record) error { return nil }

// TestExistingCallShapesStillWork runs the unchanged four-argument call
// against a broker and checks it behaves: every record handled, and Run
// returning cleanly on cancellation.
func TestExistingCallShapesStillWork(t *testing.T) {
	const (
		topic = "messages.v1"
		group = "moderation"
		total = 30
	)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	defer cluster.Close()
	addrs := cluster.ListenAddrs()

	// Produce through the package's own Producer, the other half of the
	// public API services depend on.
	prod, err := kafkax.NewProducer(addrs)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < total; i++ {
		if err := prod.Produce(ctx, topic, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	prod.Close()

	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	c, err := consumerFactory(addrs, group, []string{topic},
		func(_ context.Context, rec *kgo.Record) error {
			_ = handlerShape(ctx, rec)
			if seen.Add(1) == total {
				once.Do(func() { close(done) })
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	runCtx, stop := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	select {
	case <-done:
	case err := <-runErr:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("consumed %d of %d records with the legacy call shape", seen.Load(), total)
	}

	stop()
	select {
	case <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestDefaultsAreDocumented pins the numbers the README and the ops
// runbook quote, so changing one is a deliberate edit here too.
func TestDefaultsAreDocumented(t *testing.T) {
	t.Setenv(kafkax.EnvConsumerPartitionConcurrency, "")
	cfg, err := kafkax.DefaultConsumerConfig()
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		got  any
		want any
	}{
		{kafkax.EnvConsumerPartitionConcurrency, cfg.PartitionConcurrency, 64},
		{kafkax.EnvConsumerQueueDepth, cfg.QueueDepth, 2},
		{kafkax.EnvConsumerMaxPollRecords, cfg.MaxPollRecords, 1000},
		{kafkax.EnvConsumerDrainTimeout, cfg.DrainTimeout, 30 * time.Second},
		{kafkax.EnvCommitInterval, cfg.CommitInterval, time.Second},
		{kafkax.EnvCommitRecords, cfg.CommitRecords, 1000},
		{kafkax.EnvLagInterval, cfg.LagInterval, 15 * time.Second},
		{kafkax.EnvLagTimeout, cfg.LagTimeout, 5 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s default = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestEnvironmentOverrides is §4.4: every documented number is an
// environment variable, and a bad value is an error rather than a silent
// fallback to the default.
func TestEnvironmentOverrides(t *testing.T) {
	t.Setenv(kafkax.EnvConsumerPartitionConcurrency, "8")
	t.Setenv(kafkax.EnvConsumerQueueDepth, "5")
	t.Setenv(kafkax.EnvConsumerMaxPollRecords, "250")
	t.Setenv(kafkax.EnvConsumerDrainTimeout, "3s")
	t.Setenv(kafkax.EnvCommitInterval, "2s")
	t.Setenv(kafkax.EnvCommitRecords, "50")
	t.Setenv(kafkax.EnvLagInterval, "1s")
	t.Setenv(kafkax.EnvLagTimeout, "250ms")

	cfg, err := kafkax.DefaultConsumerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PartitionConcurrency != 8 || cfg.QueueDepth != 5 || cfg.MaxPollRecords != 250 {
		t.Errorf("counts not overridden: %+v", cfg)
	}
	if cfg.DrainTimeout != 3*time.Second || cfg.CommitInterval != 2*time.Second ||
		cfg.LagInterval != time.Second || cfg.LagTimeout != 250*time.Millisecond {
		t.Errorf("durations not overridden: %+v", cfg)
	}
	if cfg.CommitRecords != 50 {
		t.Errorf("CommitRecords = %d, want 50", cfg.CommitRecords)
	}

	t.Setenv(kafkax.EnvCommitInterval, "not-a-duration")
	if _, err := kafkax.DefaultConsumerConfig(); err == nil {
		t.Error("an unparseable duration must be an error, not a silent default")
	}
}

// TestOptionsBeatTheEnvironment pins the precedence rule.
func TestOptionsBeatTheEnvironment(t *testing.T) {
	t.Setenv(kafkax.EnvConsumerPartitionConcurrency, "8")
	cfg, err := kafkax.DefaultConsumerConfig()
	if err != nil {
		t.Fatal(err)
	}
	kafkax.WithPartitionConcurrency(3)(&cfg)
	if cfg.PartitionConcurrency != 3 {
		t.Fatalf("PartitionConcurrency = %d, want the option's 3", cfg.PartitionConcurrency)
	}
}
