// Package migrate applies the embedded identity-schema migrations at
// startup using a tiny version-table runner. Each migration runs once,
// inside a transaction, serialised across replicas by an advisory lock.
package migrate

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// advisoryLockKey serialises concurrent migration runs (e.g. two service
// replicas starting at once). Arbitrary but stable.
const advisoryLockKey = 0x1dab_e7_1d

// Migration is one embedded SQL file, named NNNN_description.sql.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Load returns the embedded migrations sorted by version.
func Load() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must be NNNN_description.sql", name)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %q: bad version prefix: %w", name, err)
		}
		body, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", name, err)
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			return nil, fmt.Errorf("migration %q is empty", name)
		}
		out = append(out, Migration{Version: version, Name: name, SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i := 1; i < len(out); i++ {
		if out[i].Version == out[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].Version)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no embedded migrations found")
	}
	return out, nil
}

// Run applies all unapplied migrations. It is safe to call on every
// startup and from multiple replicas concurrently.
func Run(ctx context.Context, pool *pgxpool.Pool) error {
	migrations, err := Load()
	if err != nil {
		return err
	}

	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS identity`); err != nil {
		return fmt.Errorf("ensure identity schema: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS identity.schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	for _, m := range migrations {
		if err := apply(ctx, pool, m); err != nil {
			return fmt.Errorf("migration %s: %w", m.Name, err)
		}
	}
	return nil
}

func apply(ctx context.Context, pool *pgxpool.Pool, m Migration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey); err != nil {
		return err
	}
	var applied bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM identity.schema_migrations WHERE version = $1)`,
		m.Version).Scan(&applied)
	if err != nil {
		return err
	}
	if applied {
		return nil
	}
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO identity.schema_migrations (version, name) VALUES ($1, $2)`,
		m.Version, m.Name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
