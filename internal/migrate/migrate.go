// Package migrate applies the project's embedded .up.sql migrations directly
// against a pgx pool.
//
// This exists alongside — not instead of — the golang-migrate container step
// in docker-compose.yml. Compose deliberately applies migrations as a
// separate, deliberate step before any replica starts (see the comment on
// the migrate service), because several API replicas booting at once would
// otherwise race to apply the same migration.
//
// A single-instance PaaS deployment (this project's free-tier Render
// deployment) has exactly one replica by construction, so that race cannot
// happen, and there is no shell in the distroless production image to run a
// separate migrate container against. Run, gated behind an explicit
// RUN_MIGRATIONS_ON_BOOT environment variable, lets that one process embed
// its own schema instead. It is idempotent: each migration runs at most
// once, tracked the same way golang-migrate tracks it, so a routine restart
// of an already-migrated database is a fast no-op.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anjani-kr-singh-ai/taskforge/migrations"
)

// file is one parsed *.up.sql migration.
type file struct {
	version int64
	name    string
	sql     string
}

// Run applies every embedded .up.sql migration not yet recorded in
// schema_migrations, in version order, each in its own transaction.
func Run(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     BIGINT PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	files, err := loadUpMigrations()
	if err != nil {
		return fmt.Errorf("migrate: load embedded migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return fmt.Errorf("migrate: read schema_migrations: %w", err)
	}

	for _, f := range files {
		if applied[f.version] {
			log.Debug("migration already applied", "version", f.version, "name", f.name)
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("migrate: begin %d_%s: %w", f.version, f.name, err)
		}

		// One round trip for the whole file: pgx sends a query with no
		// parameters and multiple ;-separated statements using the simple
		// protocol, which is exactly what a hand-written .sql migration file
		// needs — no per-statement splitting of our own to get subtly wrong.
		if _, err := tx.Exec(ctx, f.sql); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate: apply %d_%s: %w", f.version, f.name, err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
			f.version, f.name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate: record %d_%s: %w", f.version, f.name, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate: commit %d_%s: %w", f.version, f.name, err)
		}

		log.Info("migration applied", "version", f.version, "name", f.name)
	}

	return nil
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int64]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// loadUpMigrations parses every embedded NNNNNN_name.up.sql file, sorted by
// version. Down migrations are never embedded here — Run only moves forward.
func loadUpMigrations() ([]file, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}

	var files []file
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		version, label, ok := parseFilename(name)
		if !ok {
			return nil, fmt.Errorf("unrecognised migration filename %q", name)
		}

		contents, err := migrations.FS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", name, err)
		}

		files = append(files, file{version: version, name: label, sql: string(contents)})
	}

	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}

// parseFilename splits "000001_create_jobs_table.up.sql" into its version
// number and descriptive label, matching golang-migrate's own convention so
// the two tools agree on what "version 1" means.
func parseFilename(name string) (version int64, label string, ok bool) {
	base := strings.TrimSuffix(name, ".up.sql")
	prefix, label, found := strings.Cut(base, "_")
	if !found {
		return 0, "", false
	}
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return version, label, true
}
