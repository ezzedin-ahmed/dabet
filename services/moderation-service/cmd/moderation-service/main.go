// Command moderation-service is the moderation-service skeleton: config from env, JSON logs,
// /healthz, /readyz, Prometheus /metrics, graceful shutdown.
package main

import (
	"context"
	"os"

	"dabet/pkg/service"
)

func main() {
	svc := service.New("moderation-service")
	if err := svc.Run(context.Background()); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}
