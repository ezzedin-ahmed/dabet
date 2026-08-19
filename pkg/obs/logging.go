package obs

import (
	"log/slog"
	"os"
)

// NewLogger returns a JSON slog logger writing to stdout with a "service"
// attribute. Per P4, message text and opaque platform IDs must never be
// logged.
func NewLogger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", service)
}
