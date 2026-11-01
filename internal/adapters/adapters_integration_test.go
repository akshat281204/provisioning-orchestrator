//go:build integration

// Integration tests for the Postgres-backed adapters. They need the compose
// stack (make up && make test-integration) and run migrations for real.
package adapters_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

const testAdminURLKey = "TEST_DATABASE_URL"

func testAdminURL(t *testing.T) string {
	t.Helper()
	url := "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"
	if v := os.Getenv(testAdminURLKey); v != "" {
		url = v
	}
	return url
}

// hostOf extracts "host:port" from the admin URL so per-role test
// connections target the same server.
func hostOf(adminURL string) string {
	if u, err := url.Parse(adminURL); err == nil && u.Host != "" {
		return u.Host
	}
	return "localhost:5432"
}

func newTestPool(t *testing.T, migrate bool) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testAdminURL(t))
	if err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	t.Cleanup(pool.Close)
	if migrate {
		mctx, mcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer mcancel()
		if _, err := store.ApplyMigrations(mctx, pool, store.MigrateOptions{LockTimeout: 10 * time.Second}); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}
	}
	return pool
}

// uniqueEnvID yields a fresh UUID-shaped id so deterministic names derived
// from it never collide with resources of an earlier test run.
func uniqueEnvID(t *testing.T) string {
	t.Helper()
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("generate env id: %v", err)
	}
	raw[6] = 0x40 // version nibble
	h := hex.EncodeToString(raw[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func createEnvRow(t *testing.T, pool *pgxpool.Pool, envID string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO environments (id, tenant_id, name, state, idempotency_key)
		 VALUES ($1::uuid, 'adapters-test', $1::text, 'provisioning', $1::text)`, envID)
	if err != nil {
		t.Fatalf("create environments row %s: %v", envID, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM environments WHERE id = $1`, envID)
	})
}

func TestCapacityReserveIsIdempotentAndBounded(t *testing.T) {
	pool := newTestPool(t, true)
	cap := adapters.NewLocalCapacity(pool, nil)
	cap.Limit = 2
	ctx := context.Background()

	e1, e2, e3 := uniqueEnvID(t), uniqueEnvID(t), uniqueEnvID(t)
	createEnvRow(t, pool, e1)
	createEnvRow(t, pool, e2)
	createEnvRow(t, pool, e3)

	// Two reserves for one env consume one slot and succeed twice.
	for i := 0; i < 2; i++ {
		if err := cap.Reserve(ctx, e1); err != nil {
			t.Fatalf("reserve e1 (attempt %d): %v", i+1, err)
		}
	}
	if err := cap.Reserve(ctx, e2); err != nil {
		t.Fatalf("reserve e2: %v", err)
	}

	err := cap.Reserve(ctx, e3)
	if !errors.Is(err, adapters.ErrCapacityExhausted) {
		t.Fatalf("reserve e3 at the limit: got %v, want ErrCapacityExhausted", err)
	}

	// A release frees the slot, and repeated release stays success.
	if err := cap.Release(ctx, e1); err != nil {
		t.Fatalf("release e1: %v", err)
	}
	if err := cap.Release(ctx, e1); err != nil {
		t.Fatalf("second release e1 must be idempotent: %v", err)
	}
	if err := cap.Reserve(ctx, e3); err != nil {
		t.Fatalf("reserve e3 after release: %v", err)
	}
	if err := cap.Release(ctx, e3); err != nil {
		t.Fatalf("release e3: %v", err)
	}
	if err := cap.Release(ctx, e2); err != nil {
		t.Fatalf("release e2: %v", err)
	}
}

func TestCapacityConcurrentReserveLastSlot(t *testing.T) {
	pool := newTestPool(t, true)
	cap := adapters.NewLocalCapacity(pool, nil)
	cap.Limit = 1
	ctx := context.Background()

	e1, e2 := uniqueEnvID(t), uniqueEnvID(t)
	createEnvRow(t, pool, e1)
	createEnvRow(t, pool, e2)

	var (
		wg      sync.WaitGroup
		success int64
	)
	errCh := make(chan error, 2)
	for _, id := range []string{e1, e2} {
		wg.Add(1)
		go func(envID string) {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := cap.Reserve(rctx, envID); err != nil {
				errCh <- err
				return
			}
			atomic.AddInt64(&success, 1)
		}(id)
	}
	wg.Wait()
	close(errCh)

	if success != 1 {
		t.Errorf("%d reserves succeeded at the last slot, want exactly 1", success)
	}
	for err := range errCh {
		if !errors.Is(err, adapters.ErrCapacityExhausted) {
			t.Errorf("unexpected reserve error: %v", err)
		}
	}
	var held int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM capacity_reservations WHERE environment_id IN ($1, $2)`, e1, e2).Scan(&held); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if held != 1 {
		t.Errorf("%d reservations rows persisted, want exactly 1", held)
	}
	cleanupCapacity(t, pool, e1, e2)
}

func cleanupCapacity(t *testing.T, pool *pgxpool.Pool, envIDs ...string) {
	t.Helper()
	for _, id := range envIDs {
		if _, err := pool.Exec(context.Background(),
			`DELETE FROM capacity_reservations WHERE environment_id = $1`, id); err != nil {
			t.Fatalf("cleanup reservation %s: %v", id, err)
		}
	}
}

func TestDatabasesEnsureExistsDrop(t *testing.T) {
	pool := newTestPool(t, true)
	dbs := adapters.NewLocalDatabases(pool, testAdminURL(t), nil)
	ctx := context.Background()
	envID := uniqueEnvID(t)
	// The database name is derived only from the env id, deterministic.
	name := adapters.DatabaseName(envID)
	if got := adapters.DatabaseName(envID); got != name {
		t.Fatalf("name not deterministic")
	}

	for i := 0; i < 2; i++ {
		if err := dbs.Ensure(ctx, name); err != nil {
			t.Fatalf("ensure database (attempt %d): %v", i+1, err)
		}
	}
	exists, err := dbs.Exists(ctx, name)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("database missing after two Ensures")
	}
	// Exactly one row in the catalog.
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_database WHERE datname = $1`, name).Scan(&count); err != nil {
		t.Fatalf("count database rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d database rows after two Ensures, want exactly 1", count)
	}

	if err := dbs.Drop(ctx, name); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	if exists, _ := dbs.Exists(ctx, name); exists {
		t.Fatal("database still exists after Drop")
	}
	// Second drop: cleanup retries must not wedge on absent resources.
	if err := dbs.Drop(ctx, name); err != nil {
		t.Fatalf("second drop must be idempotent: %v", err)
	}
}

func TestRolesAndIsolation(t *testing.T) {
	pool := newTestPool(t, true)
	adminURL := testAdminURL(t)
	dbs := adapters.NewLocalDatabases(pool, adminURL, nil)
	roles := adapters.NewLocalRoles(pool, adminURL, nil)
	ctx := context.Background()

	envID := uniqueEnvID(t)
	dbName := adapters.DatabaseName(envID)
	roleName := adapters.RoleName(envID)
	password := "t3St-P4ss" // alphanumeric-ish test-only credential

	if err := dbs.Ensure(ctx, dbName); err != nil {
		t.Fatalf("ensure own database: %v", err)
	}
	other := uniqueEnvID(t)
	otherDB := adapters.DatabaseName(other)
	if err := dbs.Ensure(ctx, otherDB); err != nil {
		t.Fatalf("ensure other database: %v", err)
	}
	t.Cleanup(func() {
		_ = dbs.Drop(context.Background(), dbName)
		_ = dbs.Drop(context.Background(), otherDB)
		_ = roles.Drop(context.Background(), roleName)
	})

	// Ensure twice: exactly one role, success both times.
	for i := 0; i < 2; i++ {
		if err := roles.Ensure(ctx, roleName, password); err != nil {
			t.Fatalf("ensure role (attempt %d): %v", i+1, err)
		}
	}
	exists, err := roles.Exists(ctx, roleName)
	if err != nil || !exists {
		t.Fatalf("role exists = %v, err %v; want true, nil", exists, err)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_roles WHERE rolname = $1`, roleName).Scan(&count); err != nil {
		t.Fatalf("count role rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d role rows after two Ensures, want exactly 1", count)
	}

	if err := roles.GrantConnect(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant connect: %v", err)
	}
	// Grants repeat safely.
	if err := roles.GrantConnect(ctx, roleName, dbName); err != nil {
		t.Fatalf("second grant connect: %v", err)
	}
	if err := roles.GrantUsage(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant usage: %v", err)
	}
	if err := roles.GrantUsage(ctx, roleName, dbName); err != nil {
		t.Fatalf("second grant usage: %v", err)
	}
	// Role drop happens after the dependent databases are gone (cleanup
	// order): grants on a still-existing database make the role dependent,
	// exactly as the forward plan leaves it mid-flight.
	if err := dbs.Drop(ctx, dbName); err != nil {
		t.Fatalf("drop own database: %v", err)
	}
	if err := dbs.Drop(ctx, otherDB); err != nil {
		t.Fatalf("drop other database: %v", err)
	}
	// Drop role twice: idempotent.
	if err := roles.Drop(ctx, roleName); err != nil {
		t.Fatalf("drop role: %v", err)
	}
	if err := roles.Drop(ctx, roleName); err != nil {
		t.Fatalf("second drop role must be idempotent: %v", err)
	}
}

func TestProvisionedRoleCannotConnectElsewhere(t *testing.T) {
	pool := newTestPool(t, true)
	adminURL := testAdminURL(t)
	dbs := adapters.NewLocalDatabases(pool, adminURL, nil)
	roles := adapters.NewLocalRoles(pool, adminURL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	envID := uniqueEnvID(t)
	dbName := adapters.DatabaseName(envID)
	roleName := adapters.RoleName(envID)
	password := "only-own-db-Pl3ase"

	if err := dbs.Ensure(ctx, dbName); err != nil {
		t.Fatalf("ensure database: %v", err)
	}
	if err := roles.Ensure(ctx, roleName, password); err != nil {
		t.Fatalf("ensure role: %v", err)
	}
	if err := roles.GrantConnect(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant connect: %v", err)
	}
	if err := roles.GrantUsage(ctx, roleName, dbName); err != nil {
		t.Fatalf("grant usage: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_ = dbs.Drop(bg, dbName)
		_ = roles.Drop(bg, roleName)
	})

	// Connect as the provisioned role to its own database: succeeds.
	ownURL := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", roleName, password, hostOf(adminURL), dbName)
	if own, err := pgxpool.New(ctx, ownURL); err != nil || own.Ping(ctx) != nil {
		t.Fatalf("role cannot connect to its own database: %v", err)
	} else {
		own.Close()
	}

	// Another provisioned database (hardened, no grant for this role): the
	// connect must fail; otherwise two tenants could share databases.
	otherDB := adapters.DatabaseName(uniqueEnvID(t))
	if err := dbs.Ensure(ctx, otherDB); err != nil {
		t.Fatalf("ensure other database: %v", err)
	}
	t.Cleanup(func() { _ = dbs.Drop(context.Background(), otherDB) })

	otherURL := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", roleName, password, hostOf(adminURL), otherDB)
	if other, err := pgxpool.New(ctx, otherURL); err == nil {
		if pingErr := other.Ping(ctx); pingErr == nil {
			other.Close()
			t.Fatalf("role connected to a database it was never granted: %s", otherDB)
		}
	}

	// Every database PUBLIC can default-connect to is the isolation hole the
	// control databases are: the role must fail to reach the cluster system
	// databases and the orchestrator's own control database, not only other
	// provisioned databases.
	controlDB := databaseOf(adminURL)
	for _, forbidden := range []string{"postgres", "template1", controlDB} {
		if forbidden == "" {
			continue
		}
		mustNotConnect(t, ctx, adminURL, roleName, password, forbidden)
	}
}

// mustNotConnect asserts a connect attempt by role against database fails:
// pgxpool defers opening a connection to Ping, so a denied CONNECT surfaces
// there.
func mustNotConnect(t *testing.T, ctx context.Context, adminURL, role, password, database string) {
	t.Helper()
	dbURL := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", role, password, hostOf(adminURL), database)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return
	}
	defer pool.Close()
	if pingErr := pool.Ping(ctx); pingErr == nil {
		t.Fatalf("provisioned role connected to %s, which it must not reach", database)
	}
}

// databaseOf extracts the database name from the admin URL path.
func databaseOf(adminURL string) string {
	if u, err := url.Parse(adminURL); err == nil {
		return strings.TrimPrefix(u.Path, "/")
	}
	return ""
}
