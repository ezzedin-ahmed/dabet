// Command policy-service serves Area B (docs §6): the /v1/policies CRUD
// API, the GetPolicy gRPC hot path with its Memcached read-through, and
// the Area B metrics — on the shared service runner (config from env,
// JSON logs, /healthz, /readyz, Prometheus /metrics, graceful shutdown).
package main

import (
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"dabet/pkg/config"
	"dabet/pkg/httpx"
	"dabet/pkg/policyapi"
	"dabet/pkg/service"
	"dabet/pkg/tracing"

	"dabet/services/policy-service/internal/cache"
	"dabet/services/policy-service/internal/grpcapi"
	"dabet/services/policy-service/internal/httpapi"
	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/resolver"
	"dabet/services/policy-service/internal/store"
)

// EnvGRPCAddr configures the internal GetPolicy listener, separate from
// the public HTTP address.
const EnvGRPCAddr = "GRPC_ADDR"

func main() {
	svc := service.New("policy-service")
	log := svc.Logger
	ctx := context.Background()

	fatal := func(msg string, err error) {
		log.Error(msg, "error", err.Error())
		os.Exit(1)
	}

	// §5.4 access tokens: HS256 (JWT_SECRET) by default, RS256
	// (JWT_PUBLIC_KEY / JWT_PUBLIC_KEY_FILE) when JWT_ALG says so.
	verifier, err := httpx.VerifierFromEnv(os.Getenv)
	if err != nil {
		fatal("config", err)
	}
	dsn, err := config.Get(config.EnvPostgresDSN)
	if err != nil {
		fatal("config", err)
	}
	cacheTTL, err := config.GetDuration("POLICY_MEMCACHED_TTL", resolver.DefaultTTL)
	if err != nil {
		fatal("config", err)
	}
	grpcAddr := config.GetDefault(EnvGRPCAddr, ":7101")
	mcAddrs := strings.Split(config.GetDefault(config.EnvMemcachedAddrs, "127.0.0.1:11211"), ",")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fatal("postgres connect", err)
	}
	defer pool.Close()
	migrateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = store.Migrate(migrateCtx, pool)
	cancel()
	if err != nil {
		fatal("postgres migrate", err)
	}
	svc.Metrics.DependencyUp.WithLabelValues("postgres").Set(1)

	m := metrics.New(svc.Registry)
	repo := store.NewPG(pool)
	mc := cache.NewMemcached(mcAddrs...)
	res := resolver.New(repo, mc, m, svc.Metrics, cacheTTL)

	httpapi.Register(svc.Mux, verifier, repo, m, log)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		fatal("grpc listen", err)
	}
	// otelgrpc server handler: GetPolicy is a hop inside a message's
	// journey (moderation -> policy, §6.7), so the RPC must continue the
	// caller's trace rather than start its own.
	grpcSrv := grpc.NewServer(tracing.GRPCServerOptions()...)
	policyapi.RegisterPolicyServiceServer(grpcSrv, grpcapi.New(res, log))
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- grpcSrv.Serve(lis) }()
	log.Info("grpc listening", "grpc_addr", grpcAddr)

	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(ctx) }()

	select {
	case err := <-grpcErr:
		if err != nil {
			fatal("grpc server exited", err)
		}
	case err := <-runErr:
		grpcSrv.GracefulStop()
		if err != nil {
			fatal("service exited", err)
		}
	}
}
