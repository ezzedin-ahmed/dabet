// Package config reads service configuration from environment variables.
// Names are prefixed by concern, not by service (see docs §4.4), so shared
// values are literally identical strings across services.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Canonical environment variable names shared across services.
const (
	EnvKafkaBrokers      = "KAFKA_BROKERS"
	EnvRedisAddr         = "REDIS_ADDR"
	EnvPostgresDSN       = "POSTGRES_DSN"
	EnvMemcachedAddrs    = "MEMCACHED_ADDRS"
	EnvVLLMEndpoint      = "VLLM_ENDPOINT"
	EnvMilvusAddr        = "MILVUS_ADDR"
	EnvClickhouseDSN     = "CLICKHOUSE_DSN"
	EnvS3Endpoint        = "S3_ENDPOINT"
	EnvEmbeddingEndpoint = "EMBEDDING_ENDPOINT"
	EnvJWTSecret         = "JWT_SECRET"
	EnvHTTPAddr          = "HTTP_ADDR"
	EnvMetricsAddr       = "METRICS_ADDR"
)

// Get returns the value of a required variable, erroring when unset or empty.
func Get(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("required environment variable %s is not set", name)
	}
	return v, nil
}

// GetDefault returns the variable's value, or def when unset or empty.
func GetDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// GetInt parses the variable as an integer, returning def when unset.
func GetInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s: %w", name, err)
	}
	return n, nil
}

// GetDuration parses the variable as a time.Duration (e.g. "60s"),
// returning def when unset.
func GetDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s: %w", name, err)
	}
	return d, nil
}
