// Package service is the shared skeleton runner for Dabet services: env
// config, slog JSON logging, /healthz, /readyz, Prometheus /metrics,
// OpenTelemetry tracing, and graceful shutdown on SIGTERM.
package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"dabet/pkg/config"
	"dabet/pkg/httpx"
	"dabet/pkg/obs"
	"dabet/pkg/tracing"
)

// Service wires the shared plumbing. Handlers are added to Mux before Run.
type Service struct {
	Name     string
	Logger   *slog.Logger
	Registry *prometheus.Registry
	Metrics  *obs.Metrics
	Health   *obs.Health
	Mux      *http.ServeMux

	httpAddr    string
	metricsAddr string

	traceCfg      tracing.Config
	traceShutdown tracing.ShutdownFunc
}

// New builds a Service named name. HTTP_ADDR (default :8080) serves the
// application mux plus /healthz and /readyz; METRICS_ADDR (default :9090)
// serves Prometheus /metrics. Both are env-configurable because Compose
// runs many services side by side.
//
// It also initialises OpenTelemetry tracing (docs §4.5). Tracing is off
// unless OTEL_EXPORTER_OTLP_ENDPOINT is set, in which case the no-op
// tracer provider is installed and nothing is exported, allocated, or
// logged. A misconfigured endpoint is a warning, never a startup failure:
// per P2 the observability stack must not be able to take the moderation
// path down.
func New(name string) *Service {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	health := obs.NewHealth()
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.Healthz())
	mux.Handle("GET /readyz", health.Readyz())
	logger := obs.NewLogger(name)

	traceCfg, traceShutdown, err := tracing.Init(context.Background(), name)
	if err != nil {
		logger.Warn("tracing disabled: initialisation failed", "error", err.Error())
	} else if traceCfg.Enabled {
		logger.Info("tracing enabled",
			"protocol", traceCfg.Protocol,
			"sampler", traceCfg.SamplerName,
			"sample_ratio", traceCfg.SampleRatio)
	}

	return &Service{
		Name:          name,
		Logger:        logger,
		Registry:      reg,
		Metrics:       obs.NewMetrics(reg),
		Health:        health,
		Mux:           mux,
		httpAddr:      config.GetDefault(config.EnvHTTPAddr, ":8080"),
		metricsAddr:   config.GetDefault(config.EnvMetricsAddr, ":9090"),
		traceCfg:      traceCfg,
		traceShutdown: traceShutdown,
	}
}

// TracingEnabled reports whether an OTLP exporter was configured.
func (s *Service) TracingEnabled() bool { return s.traceCfg.Enabled }

// Run serves until ctx is cancelled or SIGTERM/SIGINT arrives, then shuts
// both servers down gracefully.
func (s *Service) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{}))

	// Opt-in profiler on the metrics listener, never the application one, so
	// it is not reachable from an ingress. Off unless DEBUG_PPROF is set:
	// attributing CPU cost to a function needs a profile, and guessing from
	// the source is how you end up optimising the wrong thing.
	if config.GetDefault("DEBUG_PPROF", "") != "" {
		metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
		metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		s.Logger.Warn("pprof enabled on the metrics listener", "addr", s.metricsAddr)
	}

	// RequestID first so the trace middleware can put request_id on the
	// span; Trace outside Instrument so the metric labels and the span
	// see the same matched route.
	appSrv := &http.Server{
		Addr:    s.httpAddr,
		Handler: httpx.RequestID(httpx.Trace(httpx.Instrument(s.Metrics, s.Mux))),
	}
	metricsSrv := &http.Server{Addr: s.metricsAddr, Handler: metricsMux}

	errc := make(chan error, 2)
	go func() {
		if err := appSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	go func() {
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	s.Logger.Info("service started", "http_addr", s.httpAddr, "metrics_addr", s.metricsAddr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	s.Logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := appSrv.Shutdown(shutdownCtx)
	if merr := metricsSrv.Shutdown(shutdownCtx); err == nil {
		err = merr
	}
	// Flush whatever the batch span processor is still holding. A failure
	// here is telemetry loss, not a service failure, so it is logged and
	// not returned.
	if s.traceShutdown != nil {
		if terr := s.traceShutdown(shutdownCtx); terr != nil {
			s.Logger.Warn("tracing shutdown", "error", terr.Error())
		}
	}
	return err
}
