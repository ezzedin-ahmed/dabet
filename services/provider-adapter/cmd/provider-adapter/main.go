// Command provider-adapter is the provider-adapter skeleton: config from env, JSON logs,
// /healthz, /readyz, Prometheus /metrics, graceful shutdown.
package main

import (
	"context"
	"os"

	"dabet/pkg/service"
)

func main() {
	svc := service.New("provider-adapter")
	if err := svc.Run(context.Background()); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}
