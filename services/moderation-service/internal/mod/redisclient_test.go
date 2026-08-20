package mod

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisEnv is every variable redisclient.go reads.
var redisEnv = []string{
	EnvRedisClusterEnabled, EnvRedisTLSEnabled, EnvRedisTLSCAFile,
	EnvRedisTLSSkipVerify, EnvRedisTLSServerName,
	EnvRedisUsername, EnvRedisPassword, EnvRedisPasswordFile, EnvRedisDB,
	EnvRedisRouteByLatency, EnvRedisRouteRandomly,
}

func clearRedisEnv(t *testing.T) {
	t.Helper()
	for _, k := range redisEnv {
		t.Setenv(k, "")
	}
}

// TestDefaultRedisConfigIsTodaysSingleNodeClient is the compatibility pin:
// with no REDIS_* variable beyond REDIS_ADDR, the configuration is exactly
// the single-node, plaintext, unauthenticated one that redis.NewClient was
// given before this file existed — same address, same timeouts, same retry
// count, no TLS, database 0.
func TestDefaultRedisConfigIsTodaysSingleNodeClient(t *testing.T) {
	clearRedisEnv(t)

	cfg, err := DefaultRedisConfig("localhost:6379", 200*time.Millisecond, -1)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster || cfg.TLSEnabled || cfg.SkipVerify || cfg.RouteByLatency || cfg.RouteRandomly {
		t.Fatalf("something is on by default: %+v", cfg)
	}
	if cfg.Username != "" || cfg.Password != "" || cfg.DB != 0 {
		t.Fatalf("credentials or database defaulted to non-empty: %+v", cfg)
	}

	opt, err := cfg.UniversalOptions()
	if err != nil {
		t.Fatal(err)
	}
	simple := opt.Simple()
	if simple.Addr != "localhost:6379" {
		t.Errorf("Addr = %q", simple.Addr)
	}
	if simple.DialTimeout != 200*time.Millisecond ||
		simple.ReadTimeout != 200*time.Millisecond ||
		simple.WriteTimeout != 200*time.Millisecond {
		t.Errorf("timeouts not passed through: %v/%v/%v",
			simple.DialTimeout, simple.ReadTimeout, simple.WriteTimeout)
	}
	if simple.MaxRetries != -1 {
		t.Errorf("MaxRetries = %d, want the caller's -1", simple.MaxRetries)
	}
	if simple.TLSConfig != nil {
		t.Error("TLS is on by default")
	}

	cl, err := NewRedisClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, ok := cl.(*redis.Client); !ok {
		t.Fatalf("built a %T, want the single-node *redis.Client", cl)
	}
}

// TestClusterSelection covers every way cluster mode can be asked for, and
// the ways it must not be assumed.
func TestClusterSelection(t *testing.T) {
	cases := []struct {
		name        string
		addr        string
		clusterEnv  string
		wantCluster bool
		wantAddrs   int
	}{
		{name: "single address, nothing set", addr: "localhost:6379", wantCluster: false, wantAddrs: 1},
		{name: "explicit cluster on one endpoint", addr: "clustercfg.dabet.use1.cache.amazonaws.com:6379",
			clusterEnv: "true", wantCluster: true, wantAddrs: 1},
		{name: "comma-separated addresses imply cluster",
			addr: "node-1:6379,node-2:6379,node-3:6379", wantCluster: true, wantAddrs: 3},
		{name: "comma-separated with spaces", addr: " node-1:6379 , node-2:6379 ",
			wantCluster: true, wantAddrs: 2},
		{name: "explicit off beats the address count", addr: "node-1:6379,node-2:6379",
			clusterEnv: "false", wantCluster: false, wantAddrs: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearRedisEnv(t)
			if tc.clusterEnv != "" {
				t.Setenv(EnvRedisClusterEnabled, tc.clusterEnv)
			}
			cfg, err := DefaultRedisConfig(tc.addr, time.Second, 3)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Cluster != tc.wantCluster {
				t.Fatalf("Cluster = %v, want %v", cfg.Cluster, tc.wantCluster)
			}
			opt, err := cfg.UniversalOptions()
			if err != nil {
				t.Fatal(err)
			}
			if len(opt.Addrs) != tc.wantAddrs {
				t.Fatalf("Addrs = %v, want %d of them", opt.Addrs, tc.wantAddrs)
			}
			cl, err := NewRedisClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			switch cl.(type) {
			case *redis.ClusterClient:
				if !tc.wantCluster {
					t.Fatal("built a cluster client for a single-node configuration")
				}
			case *redis.Client:
				if tc.wantCluster {
					t.Fatal("built a single-node client for a cluster configuration; " +
						"it cannot follow MOVED/ASK redirections")
				}
			default:
				t.Fatalf("built an unexpected %T", cl)
			}
		})
	}
}

// TestClusterClientCarriesTheConfiguration checks that the values survive
// the trip into go-redis's cluster options, since that is where a silently
// dropped timeout would hide.
func TestClusterClientCarriesTheConfiguration(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv(EnvRedisClusterEnabled, "true")
	t.Setenv(EnvRedisUsername, "dabet")
	t.Setenv(EnvRedisPassword, "auth-token")

	cfg, err := DefaultRedisConfig("a:6379,b:6379", 250*time.Millisecond, 2)
	if err != nil {
		t.Fatal(err)
	}
	opt, err := cfg.UniversalOptions()
	if err != nil {
		t.Fatal(err)
	}
	cluster := opt.Cluster()
	if len(cluster.Addrs) != 2 {
		t.Fatalf("cluster Addrs = %v", cluster.Addrs)
	}
	if cluster.Username != "dabet" || cluster.Password != "auth-token" {
		t.Error("credentials were not passed to the cluster client")
	}
	if cluster.ReadTimeout != 250*time.Millisecond || cluster.MaxRetries != 2 {
		t.Errorf("timeouts/retries not passed: %v %d", cluster.ReadTimeout, cluster.MaxRetries)
	}
	// Replica reads stay off unless asked for: every Dabet Redis operation
	// is a read-modify-write and a stale replica read would be wrong.
	if cluster.RouteByLatency || cluster.RouteRandomly {
		t.Error("replica routing must be off by default")
	}
}

// TestRedisTLS covers the ElastiCache encryption-in-transit paths.
func TestRedisTLS(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeRedisTestCA(t, caPath)

	t.Run("plain tls", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisTLSEnabled, "true")
		cfg, err := DefaultRedisConfig("primary.dabet.use1.cache.amazonaws.com:6379", time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		opt, err := cfg.UniversalOptions()
		if err != nil {
			t.Fatal(err)
		}
		if opt.TLSConfig == nil {
			t.Fatal("REDIS_TLS_ENABLED had no effect")
		}
		if opt.TLSConfig.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %x", opt.TLSConfig.MinVersion)
		}
		if opt.TLSConfig.InsecureSkipVerify {
			t.Error("verification is off by default")
		}
		if opt.TLSConfig.RootCAs != nil {
			t.Error("no CA file given but RootCAs was set")
		}
	})

	t.Run("custom ca, server name and skip verify", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisTLSEnabled, "1")
		t.Setenv(EnvRedisTLSCAFile, caPath)
		t.Setenv(EnvRedisTLSServerName, "primary.dabet")
		t.Setenv(EnvRedisTLSSkipVerify, "yes")
		cfg, err := DefaultRedisConfig("primary:6379", time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		opt, err := cfg.UniversalOptions()
		if err != nil {
			t.Fatal(err)
		}
		if opt.TLSConfig.RootCAs == nil {
			t.Error("CA file not loaded")
		}
		if opt.TLSConfig.ServerName != "primary.dabet" {
			t.Errorf("ServerName = %q", opt.TLSConfig.ServerName)
		}
		if !opt.TLSConfig.InsecureSkipVerify {
			t.Error("REDIS_TLS_SKIP_VERIFY had no effect")
		}
	})

	t.Run("cluster with tls", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisClusterEnabled, "true")
		t.Setenv(EnvRedisTLSEnabled, "true")
		cfg, err := DefaultRedisConfig("clustercfg:6379", time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := NewRedisClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		if _, ok := cl.(*redis.ClusterClient); !ok {
			t.Fatalf("built a %T", cl)
		}
		opt, _ := cfg.UniversalOptions()
		if opt.Cluster().TLSConfig == nil {
			t.Fatal("TLS was dropped on the way to the cluster client")
		}
	})
}

// TestRedisMisconfigurationFailsAtStartup: §4.7 fails open at runtime, but
// a configuration mistake must be loud and immediate.
func TestRedisMisconfigurationFailsAtStartup(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeRedisTestCA(t, caPath)
	junk := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("empty address", func(t *testing.T) {
		clearRedisEnv(t)
		if _, err := DefaultRedisConfig("  ,, ", time.Second, 3); err == nil {
			t.Fatal("an empty REDIS_ADDR was accepted")
		}
	})
	t.Run("bad boolean", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisClusterEnabled, "perhaps")
		if _, err := DefaultRedisConfig("localhost:6379", time.Second, 3); err == nil {
			t.Fatal("an unparseable boolean was accepted")
		}
	})
	t.Run("bad database", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisDB, "three")
		if _, err := DefaultRedisConfig("localhost:6379", time.Second, 3); err == nil {
			t.Fatal("an unparseable REDIS_DB was accepted")
		}
	})
	t.Run("database with cluster", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisClusterEnabled, "true")
		t.Setenv(EnvRedisDB, "3")
		cfg, err := DefaultRedisConfig("localhost:6379", time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewRedisClient(cfg); err == nil {
			t.Fatal("REDIS_DB=3 with cluster mode must be rejected: cluster has only database 0")
		}
	})
	t.Run("missing ca", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisTLSEnabled, "true")
		t.Setenv(EnvRedisTLSCAFile, filepath.Join(dir, "absent.pem"))
		cfg, _ := DefaultRedisConfig("localhost:6379", time.Second, 3)
		if _, err := NewRedisClient(cfg); err == nil {
			t.Fatal("a missing CA file was accepted")
		}
	})
	t.Run("ca with no certificates", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisTLSEnabled, "true")
		t.Setenv(EnvRedisTLSCAFile, junk)
		cfg, _ := DefaultRedisConfig("localhost:6379", time.Second, 3)
		if _, err := NewRedisClient(cfg); err == nil {
			t.Fatal("a CA file with no PEM certificates was accepted")
		}
	})
	t.Run("tls material without tls", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisTLSCAFile, caPath)
		cfg, _ := DefaultRedisConfig("localhost:6379", time.Second, 3)
		if _, err := NewRedisClient(cfg); err == nil {
			t.Fatal("a CA file without REDIS_TLS_ENABLED would be silently ignored")
		}
	})
	t.Run("missing password file", func(t *testing.T) {
		clearRedisEnv(t)
		t.Setenv(EnvRedisPasswordFile, filepath.Join(dir, "absent"))
		if _, err := DefaultRedisConfig("localhost:6379", time.Second, 3); err == nil {
			t.Fatal("a missing password file was accepted")
		}
	})
}

// TestRedisPasswordFileWins is how a Kubernetes secret volume presents an
// AUTH token, trailing newline and all.
func TestRedisPasswordFileWins(t *testing.T) {
	clearRedisEnv(t)
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("auth-token-from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRedisPassword, "inline")
	t.Setenv(EnvRedisPasswordFile, path)

	cfg, err := DefaultRedisConfig("localhost:6379", time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Password != "auth-token-from-file" {
		t.Fatalf("password not read from the file (%d bytes)", len(cfg.Password))
	}
}

// TestRedisCredentialsAreNeverRendered is P4.
func TestRedisCredentialsAreNeverRendered(t *testing.T) {
	cfg := RedisConfig{
		Addrs:    []string{"primary:6379"},
		Username: "dabet-rbac-user",
		Password: "elasticache-auth-token",
	}
	var b strings.Builder
	for _, a := range cfg.LogValue().Group() {
		b.WriteString(a.Key + "=" + a.Value.String() + " ")
	}
	for _, s := range []string{cfg.String(), b.String()} {
		for _, secret := range []string{"elasticache-auth-token", "dabet-rbac-user"} {
			if strings.Contains(s, secret) {
				t.Fatalf("rendered form leaked %q: %s", secret, s)
			}
		}
	}
	var _ slog.LogValuer = cfg
}

// writeRedisTestCA writes a throwaway self-signed certificate.
func writeRedisTestCA(t *testing.T, path string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dabet-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
