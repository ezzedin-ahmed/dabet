// Package service is the shared skeleton runner for Dabet services: env
// config, slog JSON logging, /healthz, /readyz, Prometheus /metrics, and
// graceful shutdown on SIGTERM.
package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
}

// New builds a Service named name. HTTP_ADDR (default :8080) serves the
// application mux plus /healthz and /readyz; METRICS_ADDR (default :9090)
// serves Prometheus /metrics. Both are env-configurable because Compose
// runs many services side by side.
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
	return &Service{
		Name:        name,
		Logger:      obs.NewLogger(name),
		Registry:    reg,
		Metrics:     obs.NewMetrics(reg),
		Health:      health,
		Mux:         mux,
		httpAddr:    config.GetDefault(config.EnvHTTPAddr, ":8080"),
		metricsAddr: config.GetDefault(config.EnvMetricsAddr, ":9090"),
	}
}

// Run serves until ctx is cancelled or SIGTERM/SIGINT arrives, then shuts
// both servers down gracefully.
func (s *Service) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{}))

	appSrv := &http.Server{
		Addr:    s.httpAddr,
		Handler: httpx.RequestID(httpx.Instrument(s.Metrics, s.Mux)),
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
	return err
}
