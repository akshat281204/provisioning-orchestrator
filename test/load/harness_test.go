//go:build integration

// harness_test.go — the load rig: the real store, adapters, API, and worker
// pool on the compose stack. Deliberately a trimmed copy of the fault
// rig so both integration packages stay independent.
package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	vault "github.com/hashicorp/vault/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/httpapi"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

var databaseURLCandidates = []string{
	"postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable",
	"postgres://orchestrator:orchestrator@localhost:15432/orchestrator?sslmode=disable",
}

const loadCaller = "load-operator"

// loadRig is one load run's world.
type loadRig struct {
	api      *httpapi.API
	st       *store.PostgresStore
	pool     *pgxpool.Pool
	database string
	vaultSec *adapters.VaultSecrets
	capacity *adapters.LocalCapacity
	log      *slog.Logger
	logs     *bytes.Buffer
}

// newLoadRig connects to the compose stack (skipping when it is down),
// applies migrations, and wires the API and adapters.
func newLoadRig(t *testing.T) *loadRig {
	t.Helper()
	database := pickDatabaseURL(t)
	pool, err := pgxpool.New(context.Background(), database)
	if err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	t.Cleanup(pool.Close)
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := pool.Ping(pctx); err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	mctx, mcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer mcancel()
	if _, err := store.ApplyMigrations(mctx, pool, store.MigrateOptions{LockTimeout: 10 * time.Second}); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	logs := &bytes.Buffer{}
	log := slog.New(telemetry.NewRedactingHandler(slog.NewJSONHandler(logs,
		&slog.HandlerOptions{Level: slog.LevelDebug})))

	st := store.NewPostgresStore(pool)
	vaultSec := newVaultSecrets(t)
	roles := adapters.NewLocalRoles(pool, database, log)
	capacity := adapters.NewLocalCapacity(pool, log)

	api := httpapi.New(st)
	api.AttachCredentials(vaultSec, roles, adapters.NewLocalVerifier(database),
		httpapi.NewCredentialPolicy(loadCaller), log)

	return &loadRig{api: api, st: st, pool: pool, database: database,
		vaultSec: vaultSec, capacity: capacity, log: log, logs: logs}
}

// pickDatabaseURL honors TEST_DATABASE_URL, then pings the known compose ports.
func pickDatabaseURL(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		return url
	}
	for _, url := range databaseURLCandidates {
		pool, err := pgxpool.New(context.Background(), url)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = pool.Ping(ctx)
		cancel()
		pool.Close()
		if err == nil {
			return url
		}
	}
	t.Skip("postgres not reachable on any known port, skipping")
	return ""
}

// newVaultSecrets builds the Vault adapter on the dev-mode client.
func newVaultSecrets(t *testing.T) *adapters.VaultSecrets {
	t.Helper()
	vcfg := vault.DefaultConfig()
	vcfg.Address = "http://127.0.0.1:8200"
	if addr := os.Getenv("TEST_VAULT_ADDR"); addr != "" {
		vcfg.Address = addr
	}
	vclient, err := vault.NewClient(vcfg)
	if err != nil {
		t.Skipf("vault client error, skipping: %v", err)
	}
	if token := os.Getenv("TEST_VAULT_TOKEN"); token != "" {
		vclient.SetToken(token)
	} else {
		vclient.SetToken("root")
	}
	hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer hcancel()
	if _, err := vclient.Sys().HealthWithContext(hctx); err != nil {
		t.Skipf("vault not reachable, skipping: %v", err)
	}
	return adapters.NewVaultSecrets(vclient, "secret")
}

// startPool launches the load worker pool and stops it at cleanup.
func (r *loadRig) startPool(t *testing.T, workers int) {
	t.Helper()
	if workers < 1 {
		workers = 1
	}
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	p := &worker.Pool{
		Concurrency:   workers,
		LeaseDuration: 5 * time.Second,
		StepTimeout:   5 * time.Second,
		Store:         r.st,
		Executor: &worker.AdapterExecutor{
			Capacity:  r.capacity,
			Databases: adapters.NewLocalDatabases(r.pool, r.database, r.logger()),
			Roles:     adapters.NewLocalRoles(r.pool, r.database, r.logger()),
			Secrets:   r.vaultSec,
			Verifier:  adapters.NewLocalVerifier(r.database),
			Envs:      r.st,
		},
		Reconciler: &adapters.Reconciler{
			Capacity:  r.capacity,
			Databases: adapters.NewLocalDatabases(r.pool, r.database, r.logger()),
			Roles:     adapters.NewLocalRoles(r.pool, r.database, r.logger()),
			Secrets:   r.vaultSec,
			Log:       r.logger(),
		},
		ReconcileInterval: 500 * time.Millisecond,
		Log:               r.logger(),
	}
	go func() {
		defer close(done)
		_ = p.Run(wctx)
	}()
	t.Cleanup(func() {
		wcancel()
		<-done
	})
}

// logger returns the rig's redacting logger, writing into r.logs; when the
// buffer is absent it discards.
func (r *loadRig) logger() *slog.Logger {
	if r.log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return r.log
}

// createEnv creates one environment through the API and registers cleanup
// of its external resources.
func (r *loadRig) createEnv(t *testing.T, name string) string {
	t.Helper()
	tenant := fmt.Sprintf("load-%d-%s", time.Now().UnixNano(), name)
	code, created := r.call(t, http.MethodPost, "/environments", "",
		fmt.Sprintf(`{"tenantId":%q,"name":%q}`, tenant, name))
	if code != http.StatusAccepted {
		t.Fatalf("create %s: %d %v", name, code, created)
	}
	envID, _ := created["environmentId"].(string)
	if envID == "" {
		t.Fatalf("create %s: missing environmentId: %v", name, created)
	}
	t.Cleanup(func() { dropEnvResources(t, r.pool, r.vaultSec, envID) })
	return envID
}

// call issues one request against the API.
func (r *loadRig) call(t *testing.T, method, path, caller, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if caller != "" {
		req.Header.Set("X-Caller-Id", caller)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("load-%d-%s", time.Now().UnixNano(), path))
	rec := httptest.NewRecorder()
	r.api.Handler().ServeHTTP(rec, req)
	var out map[string]any
	if raw := rec.Body.String(); raw != "" && strings.Contains(raw, "{") {
		_ = json.Unmarshal([]byte(raw), &out)
	}
	if out == nil {
		out = map[string]any{}
	}
	return rec.Code, out
}

// envState reads the environment row's state directly.
func (r *loadRig) envState(t *testing.T, envID string) domain.EnvState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var state string
	if err := r.pool.QueryRow(ctx,
		`SELECT state FROM environments WHERE id = $1`, envID).Scan(&state); err != nil {
		t.Fatalf("read environment state: %v", err)
	}
	return domain.EnvState(state)
}

// countDatabase / countRole / prefixDatabases / prefixRoles /
// totalReservations read the real catalogs.
//
// The two count* helpers are existence checks by name: datname and rolname
// are UNIQUE, so they can only return 0 or 1 and are structurally incapable
// of seeing a duplicate. The duplicate detection lives in the prefix
// snapshots (prefixDatabases / prefixRoles): the load test snapshots every
// `px_`/`pu_` resource before and after the run and requires the delta to be
// fully attributable to the run's environments, one resource each — so an
// orphaned or unexplained resource fails the run even though it can never
// share a name with a counted one.
func (r *loadRig) countDatabase(t *testing.T, name string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_database WHERE datname = $1`, name).Scan(&n); err != nil {
		t.Fatalf("count databases: %v", err)
	}
	return n
}

func (r *loadRig) countRole(t *testing.T, name string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_roles WHERE rolname = $1`, name).Scan(&n); err != nil {
		t.Fatalf("count roles: %v", err)
	}
	return n
}

// prefixDatabases counts every provisioned-prefixed database in the catalog.
// `px\_` matches the literal prefix `px_` (the underscore is escaped, so it
// is not a LIKE wildcard).
func (r *loadRig) prefixDatabases(t *testing.T) int {
	t.Helper()
	return r.countCatalog(t, `SELECT count(*) FROM pg_database WHERE datname LIKE $1`, `px\_%`)
}

// prefixRoles counts every provisioned-prefixed login role in the catalog.
func (r *loadRig) prefixRoles(t *testing.T) int {
	t.Helper()
	return r.countCatalog(t, `SELECT count(*) FROM pg_roles WHERE rolname LIKE $1`, `pu\_%`)
}

func (r *loadRig) countCatalog(t *testing.T, query, arg string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx, query, arg).Scan(&n); err != nil {
		t.Fatalf("catalog count: %v", err)
	}
	return n
}

func (r *loadRig) totalReservations(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM capacity_reservations`).Scan(&n); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	return n
}

// databaseName / roleName re-derive the external names from the env id.
func (r *loadRig) databaseName(envID string) string { return adapters.DatabaseName(envID) }

func (r *loadRig) roleName(envID string) string { return adapters.RoleName(envID) }

// dropEnvResources removes everything one created environment holds. A
// failed drop fails the test: a leaked reservation or database would sit in
// the prefix snapshots and perturb later capacity and duplicate assertions,
// so a cleanup error must never pass silently.
func dropEnvResources(t *testing.T, pool *pgxpool.Pool, vaultSec *adapters.VaultSecrets, envID string) {
	t.Helper()
	db, role := adapters.DatabaseName(envID), adapters.RoleName(envID)
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(cctx, `DELETE FROM environments WHERE id = $1`, envID); err != nil {
		t.Errorf("cleanup: delete environment row: %v", err)
	}
	if _, err := pool.Exec(cctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{db}.Sanitize()); err != nil {
		t.Errorf("cleanup: drop database: %v", err)
	}
	if _, err := pool.Exec(cctx, `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Errorf("cleanup: drop role: %v", err)
	}
	if err := vaultSec.Delete(cctx, envID); err != nil {
		t.Errorf("cleanup: delete vault secret: %v", err)
	}
}
