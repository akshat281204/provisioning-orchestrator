package store

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/migrations"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migration is one embedded, ordered SQL file.
type migration struct {
	version int
	name    string
	sql     string
}

// listMigrations reads and orders the embedded migration files. Versions are
// the numeric leading token of the file name (e.g. 0001_init.sql), so adding
// migrations/0002_....sql extends the sequence without code changes.
func listMigrations() ([]migration, error) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("store: read embedded migrations: %w", err)
	}
	var ms []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(e.Name(), "%d", &version); err != nil {
			return nil, fmt.Errorf("store: migration file %q must start with its version: %w", e.Name(), err)
		}
		sqlBytes, err := fs.ReadFile(migrations.FS, e.Name())
		if err != nil {
			return nil, fmt.Errorf("store: read migration %q: %w", e.Name(), err)
		}
		ms = append(ms, migration{version: version, name: e.Name(), sql: string(sqlBytes)})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	for _, m := range ms {
		if err := validateNoTransactionControl(m.name, m.sql); err != nil {
			return nil, err
		}
	}
	return ms, nil
}

// migrationTableName is the migration tool's own ledger. It is not a domain
// table: it records which embedded files have been applied.
const migrationTableName = "schema_migrations"

// MigrateOptions carries the tunables for ApplyMigrations.
type MigrateOptions struct {
	// LockTimeout bounds waiting for the exclusive advisory lock held while
	// applying; zero waits without a deadline.
	LockTimeout time.Duration
}

// AppliedMigration reports one applied file, for command output.
type AppliedMigration struct {
	Version int
	Name    string
}

// ApplyMigrations applies every unapplied embedded migration in order,
// idempotently. Re-running with everything already applied is a no-op. It is
// safe to run concurrently from multiple processes: each run holds an
// exclusive advisory lock, and each file is recorded in schema_migrations in
// the same transaction that applies it — a crash mid-run leaves the database
// at the last fully applied version, and the next run resumes.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, opts MigrateOptions) ([]AppliedMigration, error) {
	ms, err := listMigrations()
	if err != nil {
		return nil, err
	}

	if opts.LockTimeout > 0 {
		lockCtx, cancel := context.WithTimeout(ctx, opts.LockTimeout)
		defer cancel()
		ctx = lockCtx
	}

	if pool == nil {
		return nil, fmt.Errorf("store: migrate: nil pool")
	}

	// pg_advisory_lock is session-scoped. Acquire one dedicated connection and
	// keep it for the whole run so lock, DDL, and unlock all happen on that
	// same session; with pool.Exec the lock could be held (or released) on an
	// arbitrary pooled connection, leaking it under concurrency.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, lockMigrationsSQL); err != nil {
		return nil, fmt.Errorf("store: acquire migration advisory lock: %w", err)
	}
	defer func() {
		// Advisory locks die with the session; release on the same conn and
		// only after unlock is the connection returned to the pool.
		_, _ = conn.Exec(ctx, unlockMigrationsSQL)
	}()

	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		    version    bigint PRIMARY KEY,
		    name       text NOT NULL,
		    applied_at timestamptz NOT NULL DEFAULT now()
		)`, migrationTableName)); err != nil {
		return nil, fmt.Errorf("store: create %s: %w", migrationTableName, err)
	}

	applied, err := readAppliedVersions(ctx, conn)
	if err != nil {
		return nil, err
	}

	var appliedNow []AppliedMigration
	for _, m := range ms {
		if applied[m.version] {
			continue
		}
		if err := applyOne(ctx, conn, m); err != nil {
			return appliedNow, err
		}
		appliedNow = append(appliedNow, AppliedMigration{Version: m.version, Name: m.name})
	}
	return appliedNow, nil
}

func readAppliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM `+migrationTableName)
	if err != nil {
		return nil, fmt.Errorf("store: read %s: %w", migrationTableName, err)
	}
	defer rows.Close()
	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("store: scan %s row: %w", migrationTableName, err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// applyOne runs one migration file inside a transaction with its ledger row,
// so a crash leaves the database at the last fully applied version. The file
// must not contain its own transaction control (see validateNoTransactionControl).
func applyOne(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin migration %q: %w", m.name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("store: apply migration %q: %w", m.name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO `+migrationTableName+` (version, name) VALUES ($1, $2)`,
		m.version, m.name); err != nil {
		return fmt.Errorf("store: record migration %q: %w", m.name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit migration %q: %w", m.name, err)
	}
	return nil
}

const (
	lockMigrationsSQL   = `SELECT pg_advisory_lock(hashtext('provisioning_orchestrator_migrations'))`
	unlockMigrationsSQL = `SELECT pg_advisory_unlock(hashtext('provisioning_orchestrator_migrations'))`
)

// validateNoTransactionControl fails if a migration file contains explicit
// transaction control outside dollar-quoted bodies. applyOne owns the
// transaction for each file; an embedded BEGIN/COMMIT would commit the outer
// transaction and split the DDL from the schema_migrations ledger row.
// Keywords inside DO $$ ... $$ blocks are plpgsql code, not transaction
// control, so dollar-quoted spans are stripped first.
func validateNoTransactionControl(name, sql string) error {
	stripped := dollarQuotedRe.ReplaceAllString(sql, " ")
	if loc := transactionControlRe.FindStringIndex(stripped); loc != nil {
		return fmt.Errorf("store: migration %q contains transaction control %q; applyOne owns the transaction",
			name, strings.TrimSpace(stripped[loc[0]:loc[1]]))
	}
	return nil
}

var (
	dollarQuotedRe       = regexp.MustCompile(`\$[A-Za-z_]*\$[\s\S]*?\$[A-Za-z_]*\$`)
	transactionControlRe = regexp.MustCompile(`(?i)\b(BEGIN|COMMIT|START\s+TRANSACTION|END TRANSACTION|SAVEPOINT|RELEASE|ROLLBACK|PREPARE TRANSACTION)\b`)
)

// listMigrations reads and validates the embedded migration files.
