//go:build integration

// harness_test.go — the shared rig for the fault scenarios. It wires the
// real store, real adapters, and the real worker pool on the compose stack,
// plus small fault-injection wrappers around the adapter interfaces so a
// scenario can fail a controlled number of external calls — the same code
// paths production uses, with one dependency misbehaving.
package faults

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
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

// databaseURLCandidates are tried in order; the compose host port is
// configurable so the rig still runs when 5432 is occupied.
var databaseURLCandidates = []string{
	"postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable",
	"postgres://orchestrator:orchestrator@localhost:15432/orchestrator?sslmode=disable",
}

// faultCaller is the identity the credential policy allows.
const faultCaller = "fault-operator"

// shortRetry is the retry policy every fault rig uses: aggressive enough
// that a transient injected failure is retried within the test's patience,
// still bounded like production.
var shortRetry = worker.RetryPolicy{MaxAttempts: 6, BaseBackoff: 50 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}

// rig is one fault scenario's world: the real store and adapters plus the
// fault knobs that scenario needs.
type rig struct {
	api      *httpapi.API
	st       *store.PostgresStore
	pool     *pgxpool.Pool
	database string // admin connection string
	vaultSec *adapters.VaultSecrets
	roles    *adapters.LocalRoles
	dbs      *adapters.LocalDatabases
	capacity *adapters.LocalCapacity
	log      *slog.Logger
	logs     *bytes.Buffer
}

// newRig connects to the compose stack (skipping when it is not up), applies
// migrations, and wires the API + real adapters.
func newRig(t *testing.T) *rig {
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
	dbs := adapters.NewLocalDatabases(pool, database, log)
	capacity := adapters.NewLocalCapacity(pool, log)

	api := httpapi.New(st)
	api.AttachCredentials(vaultSec, roles, adapters.NewLocalVerifier(database),
		httpapi.NewCredentialPolicy(faultCaller), log)

	return &rig{
		api: api, st: st, pool: pool, database: database,
		vaultSec: vaultSec, roles: roles, dbs: dbs, capacity: capacity,
		log: log, logs: logs,
	}
}

// pickDatabaseURL honors TEST_DATABASE_URL, then pings the known compose
// ports until one answers.
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

// newVaultSecrets builds the Vault adapter on the dev-mode client, skipping
// when Vault is not reachable.
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

// poolConfig is the worker pool wiring for one scenario.
type poolConfig struct {
	// retry overrides shortRetry when set.
	retry *worker.RetryPolicy
	exec  *worker.AdapterExecutor
	rec   *adapters.Reconciler
}

// startPool launches a worker pool against the rig, registers its stop as
// test cleanup, and returns the stop function for scenarios that swap pools
// mid-test.
func (r *rig) startPool(t *testing.T, cfg poolConfig) func() {
	t.Helper()
	retry := shortRetry
	if cfg.retry != nil {
		retry = *cfg.retry
	}
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	p := &worker.Pool{
		Concurrency:       2,
		LeaseDuration:     5 * time.Second,
		StepTimeout:       5 * time.Second,
		Store:             r.st,
		Executor:          cfg.exec,
		Reconciler:        cfg.rec,
		ReconcileInterval: 300 * time.Millisecond,
		RetryPolicy:       retry,
		Log:               r.log,
	}
	if cfg.exec != nil {
		p.Executor = cfg.exec
	} else {
		p.Executor = r.executor()
	}
	if cfg.rec != nil {
		p.Reconciler = cfg.rec
	} else {
		p.Reconciler = r.reconciler()
	}
	go func() {
		defer close(done)
		_ = p.Run(wctx)
	}()
	stop := func() {
		wcancel()
		<-done
	}
	t.Cleanup(stop)
	return stop
}

// executor builds the real-step executor wired to the rig's adapters.
func (r *rig) executor() *worker.AdapterExecutor {
	return &worker.AdapterExecutor{
		Capacity:  r.capacity,
		Databases: r.dbs,
		Roles:     r.roles,
		Secrets:   r.vaultSec,
		Verifier:  adapters.NewLocalVerifier(r.database),
		Envs:      r.st,
	}
}

// reconciler builds the real reconciler wired to the rig's probes.
func (r *rig) reconciler() *adapters.Reconciler {
	return &adapters.Reconciler{
		Capacity:  r.capacity,
		Databases: r.dbs,
		Roles:     r.roles,
		Secrets:   r.vaultSec,
		Log:       r.log,
	}
}

// call issues one request against the API and returns status plus decoded
// body. An empty caller sends no X-Caller-Id header.
func (r *rig) call(t *testing.T, method, path, caller, body string) (int, map[string]any) {
	t.Helper()
	payload := strings.NewReader(body)
	req := httptest.NewRequest(method, path, payload)
	if caller != "" {
		req.Header.Set("X-Caller-Id", caller)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("fault-%d-%s", time.Now().UnixNano(), path))
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

// createEnv creates an environment through the API and registers full
// external cleanup (row cascades, database, role, secret).
func (r *rig) createEnv(t *testing.T, name string) string {
	t.Helper()
	tenant := fmt.Sprintf("tenant-%d-%s", time.Now().UnixNano(), name)
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

// waitFor polls until f returns true or the deadline lapses.
func waitFor(t *testing.T, f func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// envState reads the environment's state directly from the row.
func (r *rig) envState(t *testing.T, envID string) domain.EnvState {
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

// envIsReady reports the row state through the API surface.
func (r *rig) envIsReady(t *testing.T, envID string) bool {
	t.Helper()
	_, status := r.call(t, http.MethodGet, "/environments/"+envID, "", "")
	return status["status"] == "ready"
}

// stepRow is one persisted workflow-step observation.
type stepRow struct {
	Kind     string
	State    string
	Attempts int
}

// stepsOf reads every workflow_step row for the environment, ordered by index.
func (r *rig) stepsOf(t *testing.T, envID string) []stepRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `
SELECT kind, state, attempts FROM workflow_steps
WHERE environment_id = $1 ORDER BY step_index`, envID)
	if err != nil {
		t.Fatalf("read workflow steps: %v", err)
	}
	defer rows.Close()
	var out []stepRow
	for rows.Next() {
		var s stepRow
		if err := rows.Scan(&s.Kind, &s.State, &s.Attempts); err != nil {
			t.Fatalf("scan workflow step: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate workflow steps: %v", err)
	}
	return out
}

// stepBy finds one step row by kind, failing when absent.
func (r *rig) stepBy(t *testing.T, envID string, kind domain.StepKind) stepRow {
	t.Helper()
	for _, s := range r.stepsOf(t, envID) {
		if s.Kind == string(kind) {
			return s
		}
	}
	t.Fatalf("no %s step row for %s", kind, envID)
	return stepRow{}
}

// countDatabase / countRole read the real catalogs — the external truth.
// Both are existence checks by name: datname and rolname are UNIQUE, so
// they can only return 0 or 1. They prove a named resource is there (or
// gone); the orphan detection is assertResourcesOwned below, which catches
// extra resources these per-name counts are structurally blind to.
func (r *rig) countDatabase(t *testing.T, name string) int {
	return r.countCatalog(t, `SELECT count(*) FROM pg_database WHERE datname = $1`, name)
}

func (r *rig) countRole(t *testing.T, name string) int {
	return r.countCatalog(t, `SELECT count(*) FROM pg_roles WHERE rolname = $1`, name)
}

func (r *rig) countCatalog(t *testing.T, query, arg string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx, query, arg).Scan(&n); err != nil {
		t.Fatalf("catalog count: %v", err)
	}
	return n
}

// assertResourcesOwned attributes every provisioned resource in the shared
// catalogs — every `px_%` database and `pu_%` role — to an environment row:
// the name must be exactly the deterministic name some environment maps to.
// Anything else is an orphan leaked by a crashed or half-failed cleanup, and
// fails the test. This is the check that makes the per-environment "exactly
// one" assertions above able to see extras: a duplicate or orphan can never
// share a name with a counted resource, but it also cannot exist without
// standing outside every environment's name.
func (r *rig) assertResourcesOwned(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbrs, err := r.pool.Query(ctx, `SELECT datname FROM pg_database WHERE datname LIKE $1`, `px\_%`)
	if err != nil {
		t.Fatalf("list provisioned databases: %v", err)
	}
	defer dbrs.Close()
	roles, err := r.pool.Query(ctx, `SELECT rolname FROM pg_roles WHERE rolname LIKE $1`, `pu\_%`)
	if err != nil {
		t.Fatalf("list provisioned roles: %v", err)
	}
	defer roles.Close()
	envs, err := r.pool.Query(ctx, `SELECT id FROM environments`)
	if err != nil {
		t.Fatalf("list environments: %v", err)
	}
	defer envs.Close()

	expected := map[string]bool{}
	for envs.Next() {
		var envID string
		if err := envs.Scan(&envID); err != nil {
			t.Fatalf("scan environment id: %v", err)
		}
		expected[adapters.DatabaseName(envID)] = true
		expected[adapters.RoleName(envID)] = true
	}
	if err := envs.Err(); err != nil {
		t.Fatalf("iterate environments: %v", err)
	}

	check := func(rows pgx.Rows, what string) {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan %s name: %v", what, err)
			}
			if !expected[name] {
				t.Errorf("orphaned %s %q: no environment maps to it — a leaked resource "+
					"from a crashed or half-failed cleanup", what, name)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate %s: %v", what, err)
		}
	}
	check(dbrs, "database")
	check(roles, "role")
}

// countReservations reads the capacity reservation rows for one environment.
func (r *rig) countReservations(t *testing.T, envID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM capacity_reservations WHERE environment_id = $1`, envID).Scan(&n); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	return n
}

// totalReservations counts every reservation row (the shared table).
func (r *rig) totalReservations(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM capacity_reservations`).Scan(&n); err != nil {
		t.Fatalf("count all reservations: %v", err)
	}
	return n
}

// countTimeline counts the environment metadata timeline entries whose state
// matches (e.g. how many times `ready` was recorded).
func (r *rig) countTimeline(t *testing.T, envID string, state string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := r.pool.QueryRow(ctx, `
SELECT count(*) FROM environments e, jsonb_array_elements(e.metadata->'timeline') t
WHERE e.id = $1 AND t->>'state' = $2`, envID, state).Scan(&n); err != nil {
		t.Fatalf("count timeline entries: %v", err)
	}
	return n
}

// walk executes plan steps 1..n manually with completions recorded — the
// precise-state setup every crash-style scenario needs before the fault.
func (r *rig) walk(t *testing.T, envID string, kinds []domain.StepKind, owner string, lease time.Duration) {
	t.Helper()
	exec := r.executor()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i, kind := range kinds {
		step, err := r.st.ClaimNextStep(ctx, owner, lease)
		if err != nil {
			t.Fatalf("claim %d (%s): %v", i+1, kind, err)
		}
		if step.Kind != kind {
			t.Fatalf("claim %d = %s, want %s", i+1, step.Kind, kind)
		}
		if err := exec.Execute(ctx, step); err != nil {
			t.Fatalf("execute %s: %v", kind, err)
		}
		if err := r.st.CompleteStep(ctx, step.ID, owner, domain.StepDone, ""); err != nil {
			t.Fatalf("complete %s: %v", kind, err)
		}
	}
}

// connectOutside proves the credential works from outside the service.
func connectOutside(t *testing.T, dsn, database, user, password string) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.Database, cfg.User, cfg.Password = database, user, password
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect with returned credential: %v", err)
	}
	defer conn.Close(context.Background())
	var one int
	if err := conn.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("external probe: %v (one=%d)", err, one)
	}
}

// dropEnvResources removes everything a scenario created so tests do not
// starve each other on the shared stack: the environment row (steps and
// reservation cascade), the database, the role, and the Vault secret. A
// failed drop fails the test — a silently leaked reservation or database
// would sit in the shared catalogs and perturb later capacity and
// ownership assertions.
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

// ---- fault-injection wrappers ------------------------------------------------

// transientErr is the error the wrappers inject: a dependency-shaped
// transient failure the retry policy is expected to absorb.
var transientErr = errors.New("simulated transient dependency failure")

// flakyRoles fails the first N GrantConnect/GrantUsage/Drop calls.
type flakyRoles struct {
	adapters.Roles
	connectLeft atomic.Int32
	usageLeft   atomic.Int32
	dropLeft    atomic.Int32
}

func (f *flakyRoles) GrantConnect(ctx context.Context, role, database string) error {
	if f.connectLeft.Add(-1) >= 0 {
		return transientErr
	}
	return f.Roles.GrantConnect(ctx, role, database)
}

func (f *flakyRoles) GrantUsage(ctx context.Context, role, database string) error {
	if f.usageLeft.Add(-1) >= 0 {
		return transientErr
	}
	return f.Roles.GrantUsage(ctx, role, database)
}

func (f *flakyRoles) Drop(ctx context.Context, role string) error {
	if f.dropLeft.Add(-1) >= 0 {
		return transientErr
	}
	return f.Roles.Drop(ctx, role)
}

// flakySecrets fails the first N Get/Put calls (secrets-service outage).
type flakySecrets struct {
	adapters.Secrets
	getLeft atomic.Int32
	putLeft atomic.Int32
}

func (f *flakySecrets) Get(ctx context.Context, envID string) (map[string]string, error) {
	if f.getLeft.Add(-1) >= 0 {
		return nil, fmt.Errorf("adapters: get secret for %s: %w", envID, transientErr)
	}
	return f.Secrets.Get(ctx, envID)
}

func (f *flakySecrets) Put(ctx context.Context, envID string, payload map[string]string) error {
	if f.putLeft.Add(-1) >= 0 {
		return fmt.Errorf("adapters: put secret for %s: %w", envID, transientErr)
	}
	return f.Secrets.Put(ctx, envID, payload)
}

// uncommittedStore fails the first CompleteRotation call — the "rotation
// succeeded remotely, the commit/response was lost" fault.
type uncommittedStore struct {
	*store.PostgresStore
	failRotation atomic.Bool
}

func (u *uncommittedStore) CompleteRotation(ctx context.Context, environmentID string, entry []byte) error {
	if u.failRotation.CompareAndSwap(true, false) {
		return fmt.Errorf("simulated lost rotation commit: %w", transientErr)
	}
	return u.PostgresStore.CompleteRotation(ctx, environmentID, entry)
}
