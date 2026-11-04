//go:build integration

// Phase-6 end-to-end integration test: provision for real (compose stack:
// postgres + vault), watch the worker walk every step through the readiness
// gate, then read the credential back through the authorized endpoint and
// connect from outside the service with it. Tests confirm the status and
// history surfaces never carry the secret, and that rotation converges.
//
// Requires `make up && make test-integration`.
package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"log/slog"

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

const (
	dbURL     = "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"
	e2eCaller = "e2e-operator"
)

type rigFixture struct {
	api      *httpapi.API
	st       *store.PostgresStore
	pool     *pgxpool.Pool
	vaultSec *adapters.VaultSecrets
	roles    *adapters.LocalRoles
	log      *slog.Logger
	logs     *bytes.Buffer
}

func newRig(t *testing.T) *rigFixture {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
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

	st := store.NewPostgresStore(pool)
	vaultSec := adapters.NewVaultSecrets(vclient, "secret")
	vaultSec.Log = log
	roles := adapters.NewLocalRoles(pool, dbURL, log)

	api := httpapi.New(st)
	api.AttachCredentials(vaultSec, roles, adapters.NewLocalVerifier(dbURL),
		httpapi.NewCredentialPolicy(e2eCaller), log)

	return &rigFixture{api: api, st: st, pool: pool, vaultSec: vaultSec, roles: roles, log: log, logs: logs}
}

// startWorker starts after the test environment is inserted, so this isolated
// test worker cannot race unrelated pending rows in the shared integration DB.
func (r *rigFixture) startWorker(t *testing.T) {
	t.Helper()
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	workers := &worker.Pool{
		Concurrency:   1,
		LeaseDuration: 10 * time.Second,
		StepTimeout:   5 * time.Second,
		Store:         r.st,
		Executor: &worker.AdapterExecutor{
			Capacity:  adapters.NewLocalCapacity(r.pool, r.log),
			Databases: adapters.NewLocalDatabases(r.pool, dbURL, r.log),
			Roles:     r.roles,
			Secrets:   r.vaultSec,
			Verifier:  adapters.NewLocalVerifier(dbURL),
			Envs:      r.st,
		},
		// The reconciler resolves ambiguous (in_doubt) steps — the worker is
		// wired like production wiring, so a timed-out step in these tests
		// is actually resolved against the real stack.
		Reconciler: &adapters.Reconciler{
			Capacity:  adapters.NewLocalCapacity(r.pool, r.log),
			Databases: adapters.NewLocalDatabases(r.pool, dbURL, r.log),
			Roles:     r.roles,
			Secrets:   r.vaultSec,
			Log:       r.log,
		},
		Log: r.log,
	}
	go func() {
		defer close(done)
		_ = workers.Run(wctx)
	}()
	t.Cleanup(func() {
		wcancel()
		<-done
	})
}

// executor builds the worker's real-step executor for manual, non-pool
// driving of plan steps (the crash-restart and cancel scenarios need to
// place the world in a precise state before the pool starts).
func (r *rigFixture) executor(t *testing.T) *worker.AdapterExecutor {
	t.Helper()
	return &worker.AdapterExecutor{
		Capacity:  adapters.NewLocalCapacity(r.pool, r.log),
		Databases: adapters.NewLocalDatabases(r.pool, dbURL, r.log),
		Roles:     r.roles,
		Secrets:   r.vaultSec,
		Verifier:  adapters.NewLocalVerifier(dbURL),
		Envs:      r.st,
	}
}

// call issues a request against the API's handler and returns the decoded
// body plus the HTTP status under the "_status" key.
func (r *rigFixture) call(t *testing.T, method, path, caller, body string) (int, map[string]any) {
	t.Helper()
	var payload *strings.Reader
	if body != "" {
		payload = strings.NewReader(body)
	} else {
		payload = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, payload)
	if caller != "" {
		req.Header.Set(callerIdentityTestHeader, caller)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("k-%d-%s", time.Now().UnixNano(), path))
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

// callJSON returns the raw response body (for leak assertions).
func (r *rigFixture) callJSON(t *testing.T, method, path string) string {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(""))
	rec := httptest.NewRecorder()
	r.api.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

const callerIdentityTestHeader = "X-Caller-Id"

// waitFor polls until f returns true or the deadline lapses.
func waitFor(t *testing.T, f func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// dropEnvResources cleans up the deterministic resources the test created:
// the environment row (its plan steps, verify-step rotation log, and
// capacity reservation all cascade from it), the provisioned database and
// role, and the Vault secret. Without the row delete the capacity slot stays
// held and pre-existing capacity-focused tests starve on a shared stack.
func dropEnvResources(t *testing.T, pool *pgxpool.Pool, vaultSec *adapters.VaultSecrets, envID string) {
	t.Helper()
	db, role := adapters.DatabaseName(envID), adapters.RoleName(envID)
	cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := pool.Exec(cctx,
		`DELETE FROM environments WHERE id = $1`, envID); err != nil {
		t.Logf("cleanup: delete environment row: %v", err)
	}
	if _, err := pool.Exec(cctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{db}.Sanitize()); err != nil {
		t.Logf("cleanup: drop database: %v", err)
	}
	if _, err := pool.Exec(cctx, `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Logf("cleanup: drop role: %v", err)
	}
	if err := vaultSec.Delete(cctx, envID); err != nil {
		t.Logf("cleanup: delete vault secret: %v", err)
	}
}

// TestProvisionEndToEndConnectsExternally provisions an environment, waits
// for the readiness gate, retrieves the credential through the authorized
// path, and proves it usable by connecting from this test — entirely outside
// the provisioning service's internals.
func TestProvisionEndToEndConnectsExternally(t *testing.T) {
	rig := newRig(t)
	tenant := fmt.Sprintf("tenant-%d", time.Now().UnixNano())

	code, created := rig.call(t, http.MethodPost, "/environments", "",
		fmt.Sprintf(`{"tenantId":%q,"name":"e2e"}`, tenant))
	if code != http.StatusAccepted {
		t.Fatalf("create: %d %v", code, created)
	}
	envID, _ := created["environmentId"].(string)
	if envID == "" {
		t.Fatalf("missing environmentId: %v", created)
	}
	t.Cleanup(func() { dropEnvResources(t, rig.pool, rig.vaultSec, envID) })
	rig.startWorker(t)

	waitFor(t, func() bool {
		_, st := rig.call(t, http.MethodGet, "/environments/"+envID, "", "")
		return st["status"] == "ready"
	}, "the environment to become ready")

	// The ready response carries the non-secret connection facts.
	_, status := rig.call(t, http.MethodGet, "/environments/"+envID, "", "")
	if status["credentialRef"] == nil || status["database"] == nil {
		t.Fatalf("status response lacks connection facts: %v", status)
	}

	// Credential retrieval: denied without the permission, allowed and
	// audited with the caller identity otherwise.
	code, denied := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", "mallory", "")
	if code != http.StatusForbidden {
		t.Fatalf("unauthorized retrieval: %d %v", code, denied)
	}
	code, creds := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", e2eCaller, "")
	if code != http.StatusOK {
		t.Fatalf("allowed retrieval: %d %v", code, creds)
	}
	password, _ := creds["password"].(string)
	user, _ := creds["username"].(string)
	database, _ := creds["database"].(string)
	if password == "" || user == "" || database == "" {
		t.Fatalf("credential payload incomplete: %v", creds)
	}

	// Connect from outside the service, as an external user of the env, and
	// confirm the grants are effective through a real query.
	connectOutside(t, dbURL, database, user, password, true)

	// Status and history must not name the secret the caller just used.
	for _, p := range []string{"/environments/" + envID, "/environments/" + envID + "/history"} {
		if body := rig.callJSON(t, http.MethodGet, p); strings.Contains(body, password) {
			t.Errorf("%s leaked the provisioned password: %s", p, body)
		}
	}
	// No log line carries it either.
	if strings.Contains(rig.logs.String(), password) {
		t.Error("captured logs leaked the provisioned password")
	}
	if !strings.Contains(rig.logs.String(), e2eCaller) {
		t.Error("allowed credential retrieval was not audited with the caller identity")
	}

	// Rotation twice in a row leaves the environment ready and usable with
	// exactly one current stored credential.
	for i := 0; i < 2; i++ {
		code, r := rig.call(t, http.MethodPost, "/environments/"+envID+"/rotate", e2eCaller, "")
		if code != http.StatusOK {
			t.Fatalf("rotation %d: %d %v", i+1, code, r)
		}
		_, got := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", e2eCaller, "")
		rotated, _ := got["password"].(string)
		if rotated == "" {
			t.Fatalf("rotation %d left no retrievable password: %v", i+1, got)
		}
		connectOutside(t, dbURL, database, user, rotated, false)
		_, status := rig.call(t, http.MethodGet, "/environments/"+envID, "", "")
		if status["status"] != "ready" {
			t.Fatalf("environment after rotation %d: %v", i+1, status)
		}
		password = rotated
	}
	_, c := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", e2eCaller, "")
	if c["password"] != password {
		t.Errorf("final retrieval mismatch: %v", c)
	}
}

// connectOutside reconnects as the provisioned role with `password`; probe
// decides whether a `select 1` result is asserted (first connect) or only
// connectivity (rotation re-checks).
func connectOutside(t *testing.T, dsn, database, user, password string, probe bool) {
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
	if !probe {
		return
	}
	var one int
	if err := conn.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("external probe: %v (one=%d)", err, one)
	}
}

// TestCrashMidStepRestartYieldsOneDatabaseAndOneRole drives the headline
// crash/restart criterion on the real worker and the real stack: a worker
// claims create_database, its side effect (the database) lands, and it dies
// without recording anything. After the lease expires a fresh worker pool
// reclaims the row, re-executes the idempotent Ensure, and finishes the plan
// — exactly one database, exactly one role, every step done, and the
// re-execution visible in the row's attempts.
func TestCrashMidStepRestartYieldsOneDatabaseAndOneRole(t *testing.T) {
	rig := newRig(t)
	tenant := fmt.Sprintf("tenant-%d", time.Now().UnixNano())
	code, created := rig.call(t, http.MethodPost, "/environments", "",
		fmt.Sprintf(`{"tenantId":%q,"name":"crash-restart"}`, tenant))
	if code != http.StatusAccepted {
		t.Fatalf("create: %d %v", code, created)
	}
	envID, _ := created["environmentId"].(string)
	if envID == "" {
		t.Fatalf("missing environmentId: %v", created)
	}
	t.Cleanup(func() { dropEnvResources(t, rig.pool, rig.vaultSec, envID) })

	exec := rig.executor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// reserve_capacity lands AND is recorded; create_database's side effect
	// lands under a short lease, and the "crash" eats the record. Nothing is
	// released or completed for it.
	prep := []domain.StepKind{domain.KindReserveCapacity, domain.KindCreateDatabase}
	for i, kind := range prep {
		step, err := rig.st.ClaimNextStep(ctx, "w-crash", 300*time.Millisecond)
		if err != nil {
			t.Fatalf("claim %s: %v", kind, err)
		}
		if step.Kind != kind {
			t.Fatalf("claim %d = %s, want %s", i+1, step.Kind, kind)
		}
		if err := exec.Execute(ctx, step); err != nil {
			t.Fatalf("execute %s: %v", kind, err)
		}
		if kind == domain.KindReserveCapacity {
			if err := rig.st.CompleteStep(ctx, step.ID, "w-crash", domain.StepDone, ""); err != nil {
				t.Fatalf("complete %s: %v", kind, err)
			}
		}
	}
	// The crash leaves create_database running under a lease 300ms wide;
	// wait it out so the restarted worker may reclaim the abandoned row.
	time.Sleep(700 * time.Millisecond)

	rig.startWorker(t)
	waitFor(t, func() bool {
		_, st := rig.call(t, http.MethodGet, "/environments/"+envID, "", "")
		return st["status"] == "ready"
	}, "the restarted worker to re-provision and make the environment ready")

	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	var databases, roles int
	if err := rig.pool.QueryRow(cctx,
		`SELECT count(*) FROM pg_database WHERE datname = $1`,
		adapters.DatabaseName(envID)).Scan(&databases); err != nil {
		t.Fatalf("count databases: %v", err)
	}
	if databases != 1 {
		t.Fatalf("a crash mid-step must yield exactly one database, found %d", databases)
	}
	if err := rig.pool.QueryRow(cctx,
		`SELECT count(*) FROM pg_roles WHERE rolname = $1`,
		adapters.RoleName(envID)).Scan(&roles); err != nil {
		t.Fatalf("count roles: %v", err)
	}
	if roles != 1 {
		t.Fatalf("a crash mid-step must yield exactly one role, found %d", roles)
	}
	// The whole plan finished: one completed row per forward kind, and the
	// reclaimed create_database row shows exactly two attempts (crash + the
	// restart's single re-execution).
	var done int
	if err := rig.pool.QueryRow(cctx,
		`SELECT count(*) FROM workflow_steps WHERE environment_id = $1 AND state = 'done'`, envID).Scan(&done); err != nil {
		t.Fatalf("count done steps: %v", err)
	}
	if done != len(domain.Plan()) {
		t.Fatalf("steps done after restart = %d, want %d", done, len(domain.Plan()))
	}
	var attempts int
	if err := rig.pool.QueryRow(cctx,
		`SELECT attempts FROM workflow_steps WHERE environment_id = $1 AND kind = $2`,
		envID, domain.KindCreateDatabase).Scan(&attempts); err != nil {
		t.Fatalf("read create_database attempts: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("create_database attempts = %d, want 2 (crash + restart)", attempts)
	}
}

// TestCancelWhileProvisioningCleansUpOnRealStack drives cancel through the
// real API into the real store and then lets the real worker run the
// compensation plan: the plan is walked manually to the brink of verification
// (capacity reserved, database/role/secret/grants live), the environment is
// cancelled, and the cleanup must end in `cancelled` with none of those
// resources left behind.
func TestCancelWhileProvisioningCleansUpOnRealStack(t *testing.T) {
	rig := newRig(t)
	tenant := fmt.Sprintf("tenant-%d", time.Now().UnixNano())
	code, created := rig.call(t, http.MethodPost, "/environments", "",
		fmt.Sprintf(`{"tenantId":%q,"name":"cancel-cleanup"}`, tenant))
	if code != http.StatusAccepted {
		t.Fatalf("create: %d %v", code, created)
	}
	envID, _ := created["environmentId"].(string)
	if envID == "" {
		t.Fatalf("missing environmentId: %v", created)
	}
	t.Cleanup(func() { dropEnvResources(t, rig.pool, rig.vaultSec, envID) })

	exec := rig.executor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Walk the forward plan until the verify step is the only work left: the
	// world holds a reservation, a database, a role, a secret, and grants.
	for _, kind := range []domain.StepKind{
		domain.KindReserveCapacity,
		domain.KindCreateDatabase,
		domain.KindStoreCreds,
		domain.KindCreateRole,
		domain.KindGrantPrivs,
	} {
		step, err := rig.st.ClaimNextStep(ctx, "w-prep", time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", kind, err)
		}
		if step.Kind != kind {
			t.Fatalf("claim = %s, want %s", step.Kind, kind)
		}
		if err := exec.Execute(ctx, step); err != nil {
			t.Fatalf("execute %s: %v", kind, err)
		}
		if err := rig.st.CompleteStep(ctx, step.ID, "w-prep", domain.StepDone, ""); err != nil {
			t.Fatalf("complete %s: %v", kind, err)
		}
	}

	// Cancel while provisioning: 202 immediately, then the worker runs the
	// compensation plan to its terminal verdict.
	code, resp := rig.call(t, http.MethodPost, "/environments/"+envID+"/cancel", "", "")
	if code != http.StatusAccepted {
		t.Fatalf("cancel: %d %v", code, resp)
	}
	rig.startWorker(t)

	waitFor(t, func() bool {
		_, st := rig.call(t, http.MethodGet, "/environments/"+envID, "", "")
		return st["status"] == "cancelled"
	}, "the cancelled environment to conclude its cleanup")

	// No orphaned resources: the database is gone, the role is gone, the
	// credential secret is gone, and the capacity slot is released.
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	for name, query := range map[string]string{
		"database": `SELECT count(*) FROM pg_database WHERE datname = $1`,
		"role":     `SELECT count(*) FROM pg_roles WHERE rolname = $1`,
	} {
		val := adapters.DatabaseName(envID)
		if name == "role" {
			val = adapters.RoleName(envID)
		}
		var n int
		if err := rig.pool.QueryRow(cctx, query, val).Scan(&n); err != nil {
			t.Fatalf("count %ss: %v", name, err)
		}
		if n != 0 {
			t.Fatalf("cancel left the %s behind", name)
		}
	}
	if _, err := rig.vaultSec.Get(cctx, envID); !errors.Is(err, adapters.ErrSecretNotFound) {
		t.Fatalf("cancel left the credential secret behind: %v", err)
	}
	var reservations int
	if err := rig.pool.QueryRow(cctx,
		`SELECT count(*) FROM capacity_reservations WHERE environment_id = $1`, envID).Scan(&reservations); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if reservations != 0 {
		t.Fatalf("cancel left a capacity reservation held")
	}
}
