package adapters

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Roles is the login-role concern. Ensure is idempotent and password-bearing;
// the password is used only at the Postgres DDL boundary — adapters never log
// or persist it (the secret lives in Vault, written by the store_credentials
// step).
type Roles interface {
	Ensure(ctx context.Context, name, password string) error
	Exists(ctx context.Context, name string) (bool, error)
	GrantConnect(ctx context.Context, role, database string) error
	GrantUsage(ctx context.Context, role, database string) error
	Drop(ctx context.Context, role string) error
}

// LocalRoles is the Postgres-backed Roles implementation, driven by the
// orchestrator's own credentials.
type LocalRoles struct {
	pool    *pgxpool.Pool
	adminCS adminConnString
	log     *slog.Logger
}

// NewLocalRoles builds a Roles adapter. adminCS lets schema-level grants run
// connected to the provisioned database.
func NewLocalRoles(pool *pgxpool.Pool, adminCS string, log *slog.Logger) *LocalRoles {
	return &LocalRoles{pool: pool, adminCS: adminCS, log: log}
}

// Ensure creates the login role if it is missing and (re)sets its password,
// so a retry after a crash converges: even if the first attempt died between
// CREATE ROLE and the password write, the second Ensure leaves the role
// reachable with the password this attempt knows. A plain CREATE ROLE has no
// superuser, CREATEDB, CREATEROLE, or REPLICATION right — least privilege.
// The duplicate_object race (another worker created it after our check) is
// success: check-then-create tolerates the concurrent winner.
func (r *LocalRoles) Ensure(ctx context.Context, name, password string) error {
	// The password touches logs or errors nowhere: only the password literal
	// inside the statement Postgres parses.
	create := fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD %s`,
		quoteID(name), quotePasswordLiteral(password))

	if _, err := r.pool.Exec(ctx, create); err != nil {
		if isSQLState(err, sqlStateDuplicateObject) {
			// The role already exists — still converge the password, this
			// invocation's password is the one this env must work with.
			return r.setPassword(ctx, name, password)
		}
		// The statement string carries the password literal; a raw wrap could
		// leak it through an error message. Summarize Postgres errors.
		if e := pgErrSummary(err); e != "" {
			return fmt.Errorf("adapters: create role %s: %s", name, e)
		}
		return fmt.Errorf("adapters: create role %s: %w", name, err)
	}
	r.logger().Info("role created", "role", name, "action", "ensure_role")
	return nil
}

// setPassword re-asserts the login and password of an existing role.
func (r *LocalRoles) setPassword(ctx context.Context, name, password string) error {
	if _, err := r.pool.Exec(ctx, fmt.Sprintf(
		`ALTER ROLE %s LOGIN PASSWORD %s`, quoteID(name), quotePasswordLiteral(password))); err != nil {
		if e := pgErrSummary(err); e != "" {
			return fmt.Errorf("adapters: set password for role %s: %s", name, e)
		}
		return fmt.Errorf("adapters: set password for role %s: %w", name, err)
	}
	r.logger().Info("role already present, password reset", "role", name, "action", "ensure_role")
	return nil
}

// Exists queries the catalog for the login role.
func (r *LocalRoles) Exists(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("adapters: check role %s exists: %w", name, err)
	}
	return exists, nil
}

// GrantConnect grants the role CONNECT on exactly one database. GRANT is
// idempotent in Postgres: repeating it is a no-op, which makes the step safe
// to retry.
func (r *LocalRoles) GrantConnect(ctx context.Context, role, database string) error {
	if _, err := r.pool.Exec(ctx, fmt.Sprintf(
		`GRANT CONNECT ON DATABASE %s TO %s`, quoteID(database), quoteID(role))); err != nil {
		return fmt.Errorf("adapters: grant connect on %s to %s: %w", database, role, err)
	}
	r.logger().Info("connect granted", "role", role, "database", database, "action", "grant_connect")
	return nil
}

// GrantUsage grants the role USAGE on the provisioned database's public
// schema. Schema-level grants run connected to that database, since Postgres
// has no cross-database GRANT.
func (r *LocalRoles) GrantUsage(ctx context.Context, role, database string) error {
	target, err := connectTargetDB(ctx, r.adminCS, database)
	if err != nil {
		return fmt.Errorf("adapters: grant usage on %s to %s: %w", database, role, err)
	}
	defer target.Close()

	if _, err := target.Exec(ctx, fmt.Sprintf(
		`GRANT USAGE ON SCHEMA public TO %s`, quoteID(role))); err != nil {
		return fmt.Errorf("adapters: grant usage on %s to %s: %w", database, role, err)
	}
	r.logger().Info("usage granted", "role", role, "database", database, "action", "grant_usage")
	return nil
}

// Drop removes the role. Dropping a role that is not there is success:
// cleanup steps retry after crashes and must not wedge on absent resources.
// The environment's database is dropped before its role (cleanup order), so
// the role holds no object privileges it would need to be dropped first.
func (r *LocalRoles) Drop(ctx context.Context, role string) error {
	if _, err := r.pool.Exec(ctx, `DROP ROLE IF EXISTS `+quoteID(role)); err != nil {
		return fmt.Errorf("adapters: drop role %s: %w", role, err)
	}
	r.logger().Info("role dropped", "role", role, "action", "drop_role")
	return nil
}

// logger substitutes a discard handler so log calls are nil-safe.
func (r *LocalRoles) logger() *slog.Logger {
	if r.log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return r.log
}

var _ Roles = (*LocalRoles)(nil)
