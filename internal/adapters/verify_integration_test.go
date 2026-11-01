//go:build integration

// Integration tests for the readiness probe: a provisioned role with grants
// verifies; a bad password is an immediate auth failure; a role without
// grants must never verify — "ready" means proven usable.
package adapters_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
)

// provisionForVerify creates a database, role, and grants the way the
// forward plan does, and registers cleanup in the crash-safe order.
func provisionForVerify(t *testing.T, envID, password string) *adapters.LocalVerifier {
	t.Helper()
	pool := newTestPool(t, true)
	adminURL := testAdminURL(t)
	dbs := adapters.NewLocalDatabases(pool, adminURL, nil)
	roles := adapters.NewLocalRoles(pool, adminURL, nil)
	ctx := context.Background()

	dbName := adapters.DatabaseName(envID)
	roleName := adapters.RoleName(envID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = dbs.Drop(cctx, dbName)
		_ = dbs.Drop(cctx, adapters.DatabaseName("verify-other-"+envID))
		_ = roles.Drop(cctx, roleName)
	})

	if err := dbs.Ensure(ctx, dbName); err != nil {
		t.Fatalf("ensure database: %v", err)
	}
	if err := roles.Ensure(ctx, roleName, password); err != nil {
		t.Fatalf("ensure role: %v", err)
	}
	return adapters.NewLocalVerifier(adminURL)
}

func TestVerifierProvesUsability(t *testing.T) {
	pool := newTestPool(t, true)
	if _, err := pool.Exec(context.Background(), `SELECT 1`); err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	envID := uniqueEnvID(t)
	password := "ver1fy-me-plaintext"
	verifier := provisionForVerify(t, envID, password)
	adminURL := testAdminURL(t)
	dbName, roleName := adapters.DatabaseName(envID), adapters.RoleName(envID)
	roles := adapters.NewLocalRoles(pool, adminURL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Without grants the probe must fail with the grants sentinel — an
	// environment in this shape may never reach ready.
	err := verifier.VerifyUsable(ctx, dbName, roleName, password)
	if err == nil || !errors.Is(err, adapters.ErrVerificationGrants) {
		t.Fatalf("verify without grants: %v, want ErrVerificationGrants", err)
	}

	if err := roles.GrantConnect(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant connect: %v", err)
	}
	if err := roles.GrantUsage(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant usage: %v", err)
	}

	// Now proven usable.
	if err := verifier.VerifyUsable(ctx, dbName, roleName, password); err != nil {
		t.Fatalf("verify with grants: %v", err)
	}
}

func TestVerifierAuthFailureIsPermanent(t *testing.T) {
	pool := newTestPool(t, true)
	if _, err := pool.Exec(context.Background(), `SELECT 1`); err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	envID := uniqueEnvID(t)
	verifier := provisionForVerify(t, envID, "correct-password-9")
	dbName, roleName := adapters.DatabaseName(envID), adapters.RoleName(envID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := verifier.VerifyUsable(ctx, dbName, roleName, "wrong-password-0")
	if err == nil || !errors.Is(err, adapters.ErrVerificationAuth) {
		t.Fatalf("verify with a wrong password: %v, want ErrVerificationAuth", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("auth failure took %s; it must fail fast", time.Since(start))
	}
}
