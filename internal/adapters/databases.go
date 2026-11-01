package adapters

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Databases is the database provision/deprovision concern. Names passed here
// are deterministic per environment (see naming.go) so a retry and the
// original attempt address the same database, and Ensure is idempotent.
type Databases interface {
	Ensure(ctx context.Context, name string) error
	Exists(ctx context.Context, name string) (bool, error)
	Drop(ctx context.Context, name string) error
}

// LocalDatabases is the Postgres-backed Databases implementation. It drives
// DDL through the orchestrator's own credentials (the admin connection).
type LocalDatabases struct {
	pool    *pgxpool.Pool
	adminCS adminConnString
	log     *slog.Logger
}

// NewLocalDatabases builds a Databases adapter. pool is the orchestrator's
// own pool (used for catalog queries and CREATE DATABASE, which cannot run
// inside a transaction); adminCS is the full connection string so
// schema-level statements can be run connected to the provisioned database.
func NewLocalDatabases(pool *pgxpool.Pool, adminCS string, log *slog.Logger) *LocalDatabases {
	return &LocalDatabases{pool: pool, adminCS: adminCS, log: log}
}

// Ensure creates the database if it is missing and hardens it, idempotently:
// a second Ensure leaves exactly one database and returns success. Hardening
// revokes the PUBLIC-connection defaults so a freshly created database is not
// connectable by roles that were never granted access — least privilege, in
// particular no nascent environment can connect to another one's database
// until its own role is explicitly granted (grant_privileges step).
func (d *LocalDatabases) Ensure(ctx context.Context, name string) error {
	// Make sure the databases the provisioned role must never reach stay
	// unreachable. Roles are cluster-wide and every database keeps its default
	// PUBLIC CONNECT grant, so hardening only the new database would still let
	// the role log in to the control-plane and system databases. Sealing runs
	// before anything else and is idempotent.
	if err := d.sealControlDatabases(ctx); err != nil {
		return err
	}

	exists, err := d.Exists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		d.logger().Info("database already present, ensuring hardened", "database", name, "action", "ensure_database")
		return d.harden(ctx, name)
	}

	// CREATE DATABASE cannot run in a transaction block; a pool.Exec with no
	// surrounding tx is one statement, non-transactional by nature. The
	// duplicate_database race (another worker created it after our check) is
	// success: check-then-create tolerates the concurrent winner.
	if _, err := d.pool.Exec(ctx,
		`CREATE DATABASE `+quoteID(name)); err != nil {
		if isSQLState(err, sqlStateDuplicateDatabase) {
			d.logger().Info("database created concurrently, treating as success",
				"database", name, "action", "ensure_database")
			return d.harden(ctx, name)
		}
		return fmt.Errorf("adapters: create database %s: %w", name, err)
	}
	if err := d.harden(ctx, name); err != nil {
		return err
	}
	d.logger().Info("database created", "database", name, "action", "ensure_database")
	return nil
}

// sealControlDatabases removes PUBLIC's default CONNECT from every database a
// provisioned environment role must not reach: the cluster system databases
// (postgres, template1) and the orchestrator's own control database. Without
// this, a freshly created login role inherits CONNECT on all of them and the
// least-privilege guarantee ("connect and usage on its own database and
// nothing else") is fiction.
//
// The admin's own explicit CONNECT grant is (re)asserted first when the admin
// is not a superuser, so the orchestrator never loses the ability to reach
// its control database mid-provisioning. A superuser bypasses CONNECT checks
// entirely, so for one the grant is unnecessary as well as skipped. The
// REVOKE is a no-op once done, which makes the whole step safe to repeat.
func (d *LocalDatabases) sealControlDatabases(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(d.adminCS)
	if err != nil {
		return fmt.Errorf("adapters: seal control databases: parse admin connection string: %w", err)
	}

	dbs := controlDatabasesFor(cfg.ConnConfig.Database)
	for _, db := range dbs {
		super, err := adminIsSuperuser(ctx, d.pool)
		if err != nil {
			return err
		}
		if !super {
			if _, err := d.pool.Exec(ctx, fmt.Sprintf(
				`GRANT CONNECT ON DATABASE %s TO %s`,
				quoteID(db), quoteID(cfg.ConnConfig.User))); err != nil {
				return fmt.Errorf("adapters: seal control databases: grant connect on %s to %s: %w",
					db, cfg.ConnConfig.User, err)
			}
		}
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(
			`REVOKE CONNECT ON DATABASE %s FROM PUBLIC`, quoteID(db))); err != nil {
			return fmt.Errorf("adapters: seal control databases: revoke connect on %s from PUBLIC: %w", db, err)
		}
		d.logger().Info("control database sealed against PUBLIC", "database", db, "action", "ensure_database")
	}
	return nil
}

// controlDatabasesFor lists the databases PUBLIC must lose CONNECT on: the
// two cluster system databases plus the orchestrator's control database.
func controlDatabasesFor(controlDB string) []string {
	dbs := []string{"postgres", "template1"}
	if controlDB != "" && controlDB != "postgres" && controlDB != "template1" {
		dbs = append(dbs, controlDB)
	}
	return dbs
}

// adminIsSuperuser queries whether the connecting role is a cluster
// superuser. Superusers bypass every CONNECT check, so nothing needs to be
// re-granted to them while sealing.
func adminIsSuperuser(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var super bool
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(rolsuper, false) FROM pg_roles WHERE rolname = current_user`).Scan(&super); err != nil {
		return false, fmt.Errorf("adapters: look up current role in pg_roles: %w", err)
	}
	return super, nil
}

// harden revokes PUBLIC's default permissions on the new database. It runs
// connected to that database because REVOKE ... ON SCHEMA is per-database;
// the GRANTs to the provisioned role happen in the Roles adapter, and
// REVOKE from PUBLIC is legal even when nothing was granted.
func (d *LocalDatabases) harden(ctx context.Context, name string) error {
	target, err := connectTargetDB(ctx, d.adminCS, name)
	if err != nil {
		return fmt.Errorf("adapters: harden database %s: %w", name, err)
	}
	defer target.Close()

	if _, err := target.Exec(ctx, `REVOKE ALL ON DATABASE `+quoteID(name)+` FROM PUBLIC`); err != nil {
		return fmt.Errorf("adapters: harden database %s: revoke PUBLIC database grants: %w", name, err)
	}
	// USAGE on the public schema is granted to PUBLIC too; revoking CREATE
	// keeps unprivileged roles from installing objects. USAGE remains because
	// public-schema readability is expected, and the environment role's own
	// privileges are set explicitly by the Roles adapter regardless.
	if _, err := target.Exec(ctx, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`); err != nil {
		return fmt.Errorf("adapters: harden database %s: revoke PUBLIC schema grants: %w", name, err)
	}
	return nil
}

// Exists queries the catalog: one database row with that name, visible.
func (d *LocalDatabases) Exists(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := d.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("adapters: check database %s exists: %w", name, err)
	}
	return exists, nil
}

// Drop removes the database. Drop must succeed when nothing is there
// (cleanup runs on every retry); WITH (FORCE) also lets the drop win over
// lingering role connections instead of wedging cleanup forever.
func (d *LocalDatabases) Drop(ctx context.Context, name string) error {
	if _, err := d.pool.Exec(ctx,
		`DROP DATABASE IF EXISTS `+quoteID(name)+` WITH (FORCE)`); err != nil {
		return fmt.Errorf("adapters: drop database %s: %w", name, err)
	}
	d.logger().Info("database dropped", "database", name, "action", "drop_database")
	return nil
}

// logger substitutes a discard handler so log calls are nil-safe.
func (d *LocalDatabases) logger() *slog.Logger {
	if d.log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return d.log
}

var _ Databases = (*LocalDatabases)(nil)
