package migrate

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLoadEmbeddedMigrations(t *testing.T) {
	migrations, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i, m := range migrations {
		if i > 0 && migrations[i-1].Version >= m.Version {
			t.Fatalf("migrations not strictly ordered at %s", m.Name)
		}
		if strings.TrimSpace(m.SQL) == "" {
			t.Fatalf("migration %s is empty", m.Name)
		}
	}

	// The first migration must ship the full §5.2 identity schema plus the
	// documented deviations (family_id, email_verifications).
	first := migrations[0].SQL
	for _, want := range []string{
		"identity.creators",
		"identity.platform_t",
		"identity.connection_status_t",
		"identity.connections",
		"connections_active_uniq",
		"connections_creator_idx",
		"identity.oauth_states",
		"identity.refresh_tokens",
		"family_id",
		"identity.email_verifications",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("migration 0001 missing %q", want)
		}
	}
}

// TestRunAgainstPostgres applies the migrations to a real Postgres. It is
// gated behind POSTGRES_DSN and creates (and drops) a throwaway database
// so it never touches existing schemas. It does not start any container.
func TestRunAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping real-database migration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)

	dbName := fmt.Sprintf("user_service_migtest_%d", rand.Int63())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create throwaway database: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	}()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to throwaway database: %v", err)
	}
	defer pool.Close()

	// Applying twice must be idempotent.
	if err := Run(ctx, pool); err != nil {
		t.Fatalf("Run (first): %v", err)
	}
	if err := Run(ctx, pool); err != nil {
		t.Fatalf("Run (second): %v", err)
	}

	for _, table := range []string{"creators", "connections", "oauth_states", "refresh_tokens", "email_verifications"} {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'identity' AND table_name = $1)`, table).Scan(&exists)
		if err != nil || !exists {
			t.Errorf("table identity.%s missing (err=%v)", table, err)
		}
	}
	var indexes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'identity'
		  AND indexname IN ('connections_active_uniq', 'connections_creator_idx', 'refresh_tokens_family_idx')`,
	).Scan(&indexes); err != nil || indexes != 3 {
		t.Errorf("expected 3 identity indexes, got %d (err=%v)", indexes, err)
	}
}
