package adapters

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNamesAreDeterministic(t *testing.T) {
	envID := "00000000-0000-4000-8000-000000000001"
	// Determinism means repeated derivation from the same id yields identical
	// output: the second call is compared against the first, not to itself.
	builds := map[string]func(string) string{
		"database":    DatabaseName,
		"role":        RoleName,
		"secret path": SecretPath,
	}
	for kind, build := range builds {
		first := build(envID)
		if second := build(envID); first != second {
			t.Errorf("%s name not deterministic: %q vs %q", kind, first, second)
		}
	}
}

func TestNamesDistinctAcrossEnvironments(t *testing.T) {
	envs := []string{
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		"tenant A/ curtis", // sanitizes to the same readable middle as nothing else
	}
	dbNames := map[string]bool{}
	roleNames := map[string]bool{}
	for _, id := range envs {
		for m, got := range map[string]map[string]bool{
			"database": dbNames, "role": roleNames,
		} {
			name := got
			var n string
			if m == "database" {
				n = DatabaseName(id)
			} else {
				n = RoleName(id)
			}
			if name[n] {
				t.Errorf("%s name %q collides between environments", m, n)
			}
			if len(n) > 63 {
				t.Errorf("%s name %q exceeds Postgres's 63-byte limit (%d bytes)", m, n, len(n))
			}
			name[n] = true
		}
	}
}

func TestNamesNeverExceedLimitEvenForAdversarialIDs(t *testing.T) {
	caps := map[string]int{
		"database": maxIdentifierLen, "role": maxIdentifierLen,
		"secret path": len("provisioning/env/") + maxSecretPathSegment,
	}
	builders := map[string]func(string) string{
		"database": DatabaseName, "role": RoleName, "secret path": SecretPath,
	}
	for _, id := range []string{
		"", " ", strings.Repeat("\"/\\\\'--\t\n", 100),
		strings.Repeat("aaaaaaaaaa", 100),
		"Ümlaut-Fröhlich-Ærø-" + strings.Repeat("x", 300),
	} {
		for kind, build := range builders {
			n := build(id)
			if got, cap := len(n), caps[kind]; got > cap { //nolint:govet // shadows builtin deliberately for the local cap
				t.Errorf("%s name for %q is %d bytes, over the %d cap: %q(%d)", kind, id, got, cap, n, cap)
			}
		}
	}
}

func TestNamesAreSafeIdentifiers(t *testing.T) {
	// Untrusted characters must never survive into a name: everything outside
	// [a-z0-9_-] (and the fixed prefix/suffix) is mapped out.
	legal := regexp.MustCompile(`^(px|pu)_[a-z0-9_-]+_[0-9a-f]{8}$`)
	for _, id := range []string{
		"00000000-0000-4000-8000-000000000001",
		"DROP TABLE users; --",
		"quote\"log-in\\\\places",
	} {
		for _, n := range []string{DatabaseName(id), RoleName(id)} {
			if !legal.MatchString(n) {
				t.Errorf("name %q for %q is not a safe identifier shape", n, id)
			}
		}
	}
}

func TestSecretPathShape(t *testing.T) {
	p := SecretPath("00000000-0000-4000-8000-000000000001")
	if !strings.HasPrefix(p, "provisioning/env/") {
		t.Errorf("secret path %q lacks the provisioning/env/ prefix", p)
	}
	// The fixed prefix is controlled; only the per-environment segment can
	// carry hostile characters, so that is what gets vetted.
	segment := strings.TrimPrefix(p, "provisioning/env/")
	if strings.ContainsAny(segment, ":/#?\"\\") {
		t.Errorf("secret path %q contains path-hostile characters", p)
	}
	if len(segment) > maxSecretPathSegment {
		t.Errorf("secret path segment is %d bytes, over the %d cap", len(segment), maxSecretPathSegment)
	}
}

func TestQuotePasswordLiteralEscapesQuotes(t *testing.T) {
	if got := quotePasswordLiteral("it's"); got != `'it''s'` {
		t.Errorf("quotePasswordLiteral = %q, want %q", got, `'it''s'`)
	}
	if got := quotePasswordLiteral(""); got != `''` {
		t.Errorf("quotePasswordLiteral(\"\") = %q, want ''", got)
	}
}

func TestIsSQLState(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "42P04"})
	if !isSQLState(err, sqlStateDuplicateDatabase) {
		t.Error("isSQLState did not match a wrapped pg error")
	}
	if isSQLState(err, sqlStateDuplicateObject) {
		t.Error("isSQLState matched the wrong code")
	}
	if isSQLState(fmt.Errorf("plain"), sqlStateDuplicateDatabase) {
		t.Error("isSQLState matched a non-pg error")
	}
}

func TestRoleDDLFailureNeverSurfacesPassword(t *testing.T) {
	const password = "s3cr3t-vault-pass"
	// The password only ever exists as the DDL statement literal; a server
	// error reports code and message, never the statement. pgErrSummary is
	// the sink role DDL errors pass through, so its output must be both
	// present (so errors are legible) and password-free.
	stmt := fmt.Sprintf("CREATE ROLE pu_x LOGIN PASSWORD %s", quotePasswordLiteral(password))
	failures := []struct {
		name string
		err  error
	}{
		{"server error", &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}},
		{"wrapped server error", fmt.Errorf("exec %q: %w", stmt, &pgconn.PgError{Code: "28P01", Message: "failed"})},
		{"driver error with statement in wrap", fmt.Errorf("exec %s: %w", stmt, os.ErrDeadlineExceeded)},
	}
	for _, tc := range failures {
		summarized := pgErrSummary(tc.err)
		if summarized != "" && strings.Contains(summarized, password) {
			t.Errorf("%s: summary %q contains the password", tc.name, summarized)
		}
		if strings.Contains(summarized, quotePasswordLiteral(password)) {
			t.Errorf("%s: summary %q contains the password literal", tc.name, summarized)
		}
	}
	// Sanity: a server error is summarized (not dropped), so the path is real.
	base := &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
	if got := pgErrSummary(base); got != "postgres 28P01: password authentication failed" {
		t.Errorf("pgErrSummary(server error) = %q", got)
	}
}
