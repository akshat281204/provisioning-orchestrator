package adapters

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Verifier is the readiness probe: it proves a provisioned login role can
// actually use its provisioned database, not merely that every provisioning
// call returned success. It always dials a fresh connection — a pooled,
// admin-privileged connection would prove nothing about the role.
type Verifier interface {
	// VerifyUsable connects to `database` as `user` with `password` and runs
	// `select 1` plus a catalog check that the grants (CONNECT, USAGE on
	// public) are effective. A failure caused by wrong credentials or
	// missing grants is permanent forever: callers must not retry it.
	VerifyUsable(ctx context.Context, database, user, password string) error
}

// Sentinel errors callers can branch on to tell fatal from transient.
var (
	// ErrVerificationAuth means the role rejected the credentials. The
	// retry budget is worthless against a password mismatch, so verification
	// must stop immediately.
	ErrVerificationAuth = errors.New("adapters: authentication for the provisioned role failed")
	// ErrVerificationGrants means the role connected but lacks the grants
	// (CONNECT or USAGE on public); readiness cannot be claimed.
	ErrVerificationGrants = errors.New("adapters: provisioned role lacks the required grants")
)

// LocalVerifier implements Verifier against the same server the adapters
// provision: it dials the admin connection string's host with the
// provisioned role's credentials, never with the admin ones.
type LocalVerifier struct {
	adminCS adminConnString
}

// NewLocalVerifier builds a Verifier on the orchestrator's connection URL.
func NewLocalVerifier(adminCS string) *LocalVerifier { return &LocalVerifier{adminCS: adminCS} }

// Verifier is the catalog query run as the provisioned role. has_*_privilege
// answers from the live catalog, so a role whose grants were never applied
// (or were revoked while PUBLIC access was sealed) fails here even though a
// bare `select 1` would have succeeded.
const verifyGrantsQuery = `SELECT 1,
       has_database_privilege(current_user, current_database(), 'CONNECT'),
       has_schema_privilege(current_user, 'public', 'USAGE')`

// VerifyUsable implements Verifier with one fresh connection per probe.
func (v *LocalVerifier) VerifyUsable(ctx context.Context, database, user, password string) error {
	cfg, err := pgx.ParseConfig(v.adminCS)
	if err != nil {
		return fmt.Errorf("adapters: verify: parse admin connection string: %w", err)
	}
	cfg.Database = database
	cfg.User = user
	cfg.Password = password
	cfg.ConnectTimeout = 0 // the caller's ctx bounds the attempt

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return wrappedVerifyErr("connect", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	var one int
	var connectPriv, schemaPriv bool
	if err := conn.QueryRow(ctx, verifyGrantsQuery).Scan(&one, &connectPriv, &schemaPriv); err != nil {
		return wrappedVerifyErr("probe", err)
	}
	if one != 1 {
		return fmt.Errorf("adapters: verify: probe query returned %d, want 1", one)
	}
	if !connectPriv || !schemaPriv {
		return fmt.Errorf("adapters: verify: connect=%t usage=%t: %w", connectPriv, schemaPriv, ErrVerificationGrants)
	}
	return nil
}

// wrappedVerifyErr classifies a driver error: Postgres class-28 codes
// (invalid_authorization_specification, invalid_password, ...) are
// authentication failures — permanent; 42501 (insufficient_privilege) means
// the role was refused access at connect time (e.g. no CONNECT grant on the
// target database, surfaced as "permission denied for database") —
// also permanent, the grants problem this verifier exists to prove; anything
// else is transient.
func wrappedVerifyErr(op string, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return fmt.Errorf("adapters: verify: %s: %w", op, err)
	}
	switch {
	case pgErr.Code == "42501":
		return fmt.Errorf("adapters: verify: %s: %w: insufficient privilege for the provisioned role", op, ErrVerificationGrants)
	case strings.HasPrefix(pgErr.Code, "28"):
		return fmt.Errorf("adapters: verify: %s: %w", op, ErrVerificationAuth)
	}
	return fmt.Errorf("adapters: verify: %s: %w", op, err)
}

var _ Verifier = (*LocalVerifier)(nil)
