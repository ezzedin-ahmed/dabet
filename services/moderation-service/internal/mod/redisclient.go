package mod

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis connectivity for §3's two topologies. Locally REDIS_ADDR is one
// address and the client is a single-node one, exactly as before. Against
// ElastiCache in cluster mode REDIS_CLUSTER_ENABLED turns on the
// cluster client, which follows MOVED/ASK redirections and keeps a slot
// map — the piece that was missing, even though §4.3 put hash tags on
// every key from day one so that this would be a config change.
//
// Everything here is unset by default; with no variable set the client is
// the single-node, plaintext one the Compose profile has always had.
const (
	// EnvRedisClusterEnabled selects Redis Cluster ("true"/"1"/"yes").
	// A REDIS_ADDR with more than one comma-separated address implies it,
	// so a chart that lists the configuration endpoints needs nothing else.
	EnvRedisClusterEnabled = "REDIS_CLUSTER_ENABLED"
	// EnvRedisTLSEnabled wraps the connection in TLS — ElastiCache's
	// encryption in transit, which is mandatory on a cluster created with
	// it enabled.
	EnvRedisTLSEnabled = "REDIS_TLS_ENABLED"
	// EnvRedisTLSCAFile is a PEM bundle replacing the system roots.
	EnvRedisTLSCAFile = "REDIS_TLS_CA_FILE"
	// EnvRedisTLSSkipVerify disables certificate verification. Escape
	// hatch for a self-signed endpoint; never for production.
	EnvRedisTLSSkipVerify = "REDIS_TLS_SKIP_VERIFY"
	// EnvRedisTLSServerName overrides the SNI / verification hostname.
	EnvRedisTLSServerName = "REDIS_TLS_SERVER_NAME"
	// EnvRedisUsername and EnvRedisPassword are ElastiCache's RBAC user or
	// AUTH token. EnvRedisPasswordFile reads the password from a mounted
	// secret instead and wins over the inline variable (§4.4: secrets from
	// the environment in v1, a secret manager in the k8s target).
	EnvRedisUsername     = "REDIS_USERNAME"
	EnvRedisPassword     = "REDIS_PASSWORD"
	EnvRedisPasswordFile = "REDIS_PASSWORD_FILE"
	// EnvRedisDB selects the logical database. Cluster mode has only
	// database 0, so a non-zero value with clustering on is an error.
	EnvRedisDB = "REDIS_DB"
	// EnvRedisRouteByLatency and EnvRedisRouteRandomly spread reads over
	// replicas in cluster mode. Both default off: every Dabet Redis
	// operation is a read-modify-write, so a stale replica read would be
	// wrong, and only a deliberate operator choice can turn them on.
	EnvRedisRouteByLatency = "REDIS_ROUTE_BY_LATENCY"
	EnvRedisRouteRandomly  = "REDIS_ROUTE_RANDOMLY"
)

// RedisConfig is the connection half of the Redis configuration; the
// timeouts and retry count come from the caller, which already reads them
// from MOD_REDIS_*.
type RedisConfig struct {
	// Addrs is one address for a single node, or the cluster's
	// configuration endpoints. Never empty after DefaultRedisConfig.
	Addrs []string
	// Cluster selects the cluster client.
	Cluster bool
	// Username, Password authenticate. P4: never logged — see LogValue.
	Username string
	Password string
	// DB is the logical database; must be 0 when Cluster is true.
	DB int

	// TLSEnabled wraps the connection in TLS.
	TLSEnabled bool
	// CAFile is a PEM bundle replacing the system roots.
	CAFile string
	// ServerName overrides SNI / verification hostname.
	ServerName string
	// SkipVerify disables certificate verification.
	SkipVerify bool

	// RouteByLatency and RouteRandomly allow replica reads in cluster
	// mode. Off by default; see the constants above.
	RouteByLatency bool
	RouteRandomly  bool

	// DialTimeout, ReadTimeout, WriteTimeout and MaxRetries are passed
	// straight through, so the single-node client is configured exactly as
	// it was before this file existed.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxRetries   int
}

// DefaultRedisConfig reads REDIS_* from the environment. addr is the
// already-resolved REDIS_ADDR (so the caller keeps its own default), and
// timeout/maxRetries are the MOD_REDIS_* values.
//
// With no REDIS_* variable beyond REDIS_ADDR set, the result is a
// single-node, plaintext, unauthenticated configuration — byte-identical
// in effect to the redis.NewClient call this replaces.
func DefaultRedisConfig(addr string, timeout time.Duration, maxRetries int) (RedisConfig, error) {
	cfg := RedisConfig{
		Addrs:        splitAddrs(addr),
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		MaxRetries:   maxRetries,
	}
	if len(cfg.Addrs) == 0 {
		return RedisConfig{}, fmt.Errorf("environment variable REDIS_ADDR: no address")
	}

	var err error
	// More than one address only makes sense for a cluster, so it is the
	// default answer; REDIS_CLUSTER_ENABLED can still say otherwise.
	if cfg.Cluster, err = redisEnvBool(EnvRedisClusterEnabled, len(cfg.Addrs) > 1); err != nil {
		return RedisConfig{}, err
	}
	if cfg.TLSEnabled, err = redisEnvBool(EnvRedisTLSEnabled, false); err != nil {
		return RedisConfig{}, err
	}
	if cfg.SkipVerify, err = redisEnvBool(EnvRedisTLSSkipVerify, false); err != nil {
		return RedisConfig{}, err
	}
	if cfg.RouteByLatency, err = redisEnvBool(EnvRedisRouteByLatency, false); err != nil {
		return RedisConfig{}, err
	}
	if cfg.RouteRandomly, err = redisEnvBool(EnvRedisRouteRandomly, false); err != nil {
		return RedisConfig{}, err
	}
	cfg.CAFile = os.Getenv(EnvRedisTLSCAFile)
	cfg.ServerName = os.Getenv(EnvRedisTLSServerName)
	cfg.Username = os.Getenv(EnvRedisUsername)
	cfg.Password = os.Getenv(EnvRedisPassword)
	if f := os.Getenv(EnvRedisPasswordFile); f != "" {
		b, rerr := os.ReadFile(f)
		if rerr != nil {
			return RedisConfig{}, fmt.Errorf("environment variable %s: %w", EnvRedisPasswordFile, rerr)
		}
		cfg.Password = strings.TrimRight(string(b), "\r\n")
	}
	if v := os.Getenv(EnvRedisDB); v != "" {
		if cfg.DB, err = strconv.Atoi(v); err != nil {
			return RedisConfig{}, fmt.Errorf("environment variable %s: %w", EnvRedisDB, err)
		}
	}
	return cfg, nil
}

// LogValue redacts the credentials (P4).
func (c RedisConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("addrs", len(c.Addrs)),
		slog.Bool("cluster", c.Cluster),
		slog.Bool("tls_enabled", c.TLSEnabled),
		slog.Bool("tls_skip_verify", c.SkipVerify),
		slog.Bool("authenticated", c.Password != ""),
	)
}

// String satisfies fmt.Stringer with the same redaction, so a %v cannot
// leak the AUTH token into a log line or an error.
func (c RedisConfig) String() string {
	return fmt.Sprintf("redis{addrs=%d cluster=%t tls=%t authenticated=%t credentials=redacted}",
		len(c.Addrs), c.Cluster, c.TLSEnabled, c.Password != "")
}

// UniversalOptions renders the configuration as go-redis's universal
// options. NewRedisClient turns these into either a *redis.Client or a
// *redis.ClusterClient; the split is here so a test can assert which one
// would be built without a server to connect to.
func (c RedisConfig) UniversalOptions() (*redis.UniversalOptions, error) {
	if len(c.Addrs) == 0 {
		return nil, fmt.Errorf("redis: no address configured")
	}
	if c.Cluster && c.DB != 0 {
		return nil, fmt.Errorf("redis: %s=%d is not possible with %s: cluster mode has only database 0",
			EnvRedisDB, c.DB, EnvRedisClusterEnabled)
	}
	opt := &redis.UniversalOptions{
		Addrs:          append([]string(nil), c.Addrs...),
		Username:       c.Username,
		Password:       c.Password,
		DB:             c.DB,
		DialTimeout:    c.DialTimeout,
		ReadTimeout:    c.ReadTimeout,
		WriteTimeout:   c.WriteTimeout,
		MaxRetries:     c.MaxRetries,
		RouteByLatency: c.RouteByLatency,
		RouteRandomly:  c.RouteRandomly,
	}
	if !c.Cluster {
		// UniversalClient picks a cluster client whenever it sees more
		// than one address; IsClusterMode pins the choice to ours.
		opt.Addrs = opt.Addrs[:1]
	} else {
		opt.IsClusterMode = true
	}
	if c.TLSEnabled {
		tlsCfg, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}
		opt.TLSConfig = tlsCfg
	} else if c.CAFile != "" || c.ServerName != "" || c.SkipVerify {
		return nil, fmt.Errorf("redis: TLS material is set but %s is not; it would be silently ignored", EnvRedisTLSEnabled)
	}
	return opt, nil
}

func (c RedisConfig) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.SkipVerify, //nolint:gosec // opt-in via REDIS_TLS_SKIP_VERIFY, documented as non-production
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("redis: %s: %w", EnvRedisTLSCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("redis: %s %q contains no PEM certificates", EnvRedisTLSCAFile, c.CAFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// NewRedisClient builds the client. In cluster mode it is a
// *redis.ClusterClient, which follows MOVED/ASK and routes each command by
// the CRC16 of its key's hash tag — which is why §4.3's tags matter and
// why every Lua script in this package touches exactly one key (see
// redisstate.go and the hash-tag test beside it).
//
// Nothing here connects: a misconfiguration is an error now, and a dead
// Redis is still discovered per operation and failed open (§4.7).
func NewRedisClient(cfg RedisConfig) (redis.UniversalClient, error) {
	opt, err := cfg.UniversalOptions()
	if err != nil {
		return nil, err
	}
	if cfg.Cluster {
		return redis.NewClusterClient(opt.Cluster()), nil
	}
	return redis.NewClient(opt.Simple()), nil
}

// splitAddrs parses a comma-separated REDIS_ADDR, ignoring empty entries
// and surrounding spaces so a Helm-rendered list is accepted as written.
func splitAddrs(addr string) []string {
	var out []string
	for _, a := range strings.Split(addr, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// redisEnvBool parses a boolean environment variable; unset is def and an
// unparseable value is an error, matching config.GetInt's contract.
func redisEnvBool(name string, def bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "yes", "on":
		return true, nil
	case "no", "off":
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("environment variable %s: %q is not a boolean", name, v)
	}
	return b, nil
}
