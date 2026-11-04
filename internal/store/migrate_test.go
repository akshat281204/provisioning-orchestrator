package store

import (
	"strings"
	"testing"
)

func TestListMigrationsRejectsTransactionControl(t *testing.T) {
	// The real embedded files must be free of explicit transaction control:
	// applyOne wraps each file in the transaction that also records the
	// schema_migrations ledger row, and an embedded BEGIN/COMMIT would commit
	// that outer transaction early.
	ms, err := listMigrations()
	if err != nil {
		t.Fatalf("listMigrations: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("listMigrations returned no migrations")
	}
}

func TestValidateNoTransactionControl(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{name: "plain ddl", sql: "CREATE TABLE t (id int);", wantErr: false},
		{
			name: "begin commit",
			sql:  "BEGIN;\nCREATE TABLE t (id int);\nCOMMIT;",
			// The first offending statement is reported.
			wantErr: true,
		},
		{
			name:    "commit only",
			sql:     "CREATE TABLE t (id int);\nCOMMIT;",
			wantErr: true,
		},
		{
			name:    "start transaction / rollback pair",
			sql:     "START TRANSACTION;\nROLLBACK;",
			wantErr: true,
		},
		{
			name:    "savepoint release",
			sql:     "SAVEPOINT s;\nRELEASE SAVEPOINT s;",
			wantErr: true,
		},
		{
			// BEGIN/END inside a DO block is plpgsql code, not transaction
			// control, and must be allowed.
			name:    "plpgsql dollar quotes allowed",
			sql:     "DO $$ BEGIN PERFORM 1; END $$;\nCREATE TABLE t (id int);",
			wantErr: false,
		},
		{
			// Transaction control after a dollar-quoted block is still an
			// error, so stripping must not hide the rest of the file.
			name:    "plpgsql then commit",
			sql:     "DO $$ BEGIN PERFORM 1; END $$;\nCOMMIT;",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNoTransactionControl("0000_test.sql", tc.sql)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q", strings.TrimSpace(tc.sql))
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
