package adapters

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// adminConnString is the URL the local Postgres-backed adapters drive their
// DDL through: the orchestrator's own credentials, which own every database
// and role they provision.
type adminConnString = string

// SQLState codes treated as "already there" by the check-then-create paths:
// every step can be retried after a crash, and another worker having created
// the resource between our check and create is success, not failure.
const (
	sqlStateDuplicateDatabase = "42P04" // duplicate_database
	sqlStateDuplicateObject   = "42710" // duplicate_object
)

// connectTargetDB opens a dedicated pool against `database` named by the
// admin connection string, keeping its credentials and TLS settings. Needed
// because schema-level statements (GRANT/REVOKE ... ON SCHEMA) always run in
// the caller's current database — there is no cross-database GRANT.
func connectTargetDB(ctx context.Context, cs adminConnString, database string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(cs)
	if err != nil {
		return nil, fmt.Errorf("adapters: parse admin connection string: %w", err)
	}
	cfg.ConnConfig.Database = database
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("adapters: connect to database %q: %w", database, err)
	}
	return pool, nil
}

// isSQLState reports whether err is a Postgres server error with `code`.
func isSQLState(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// quoteID renders name as a quoted, escaped SQL identifier. Generated names
// are letter-led, lowercase, and alphanumeric, but quoting is defense in
// depth for any string a caller hands the adapters.
func quoteID(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// quotePasswordLiteral renders s as a single-quoted SQL string literal for
// the CREATE/ALTER ROLE password clause where the extended query protocol
// does not reliably take bind parameters. Generated passwords are
// alphanumeric (see newPassword), but quoting is defense in depth.
func quotePasswordLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// pgErrSummary reduces a Postgres server error to its severity-free core.
// It returns "" for anything that is not a server error, because driver-side
// errors (network, context) carry no statement text and are safe to wrap.
// This exists because the role DDL statements embed the password literal:
// pgx renders the full statement on a server error, and that must never
// reach a log line or a persisted last_error.
func pgErrSummary(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("postgres %s: %s", pgErr.Code, pgErr.Message)
	}
	return ""
}
