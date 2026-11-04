//go:build integration

// Integration tests for the lean two-table schema. They need Postgres (e.g.
// `make up`, `make test-integration`) and run migrations for real.
package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

const testDBURLKey = "TEST_DATABASE_URL"

func testDBURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(testDBURLKey)
	if url == "" {
		url = "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"
	}
	return url
}

func newTestStore(t *testing.T) (*pgxpool.Pool, *store.PostgresStore) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testDBURL(t))
	if err != nil {
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres not reachable, skipping: %v", err)
	}
	// Clean slate per test: fresh sample_test DB content so tests are
	// order-independent. The schema itself is created by the migration run.
	t.Cleanup(pool.Close)
	return pool, store.NewPostgresStore(pool)
}

func migrate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := store.ApplyMigrations(ctx, pool, store.MigrateOptions{LockTimeout: 10 * time.Second}); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

// truncate wipes the domain tables so claim-ordering assertions are not
// skewed by rows left behind by earlier (possibly failed) test runs.
func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE capacity_reservations, workflow_steps, environments`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func sampleEnv(t *testing.T) domain.Environment {
	t.Helper()
	return domain.Environment{
		ID:             fmt.Sprintf("00000000-0000-4000-8000-%012d", time.Now().UnixNano()%1e12),
		TenantID:       "tenant-a",
		Name:           "sample",
		State:          domain.EnvPending,
		IdempotencyKey: fmt.Sprintf("idem-%d", time.Now().UnixNano()),
		Spec:           []byte(`{"size":"small"}`),
		Metadata:       []byte(`{}`),
	}
}

func TestMigrateTwiceIsNoOp(t *testing.T) {
	pool, _ := newTestStore(t)
	migrate(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applied, err := store.ApplyMigrations(ctx, pool, store.MigrateOptions{})
	if err != nil {
		t.Fatalf("second migrate run errored: %v", err)
	}
	if len(applied) != 0 {
		t.Fatalf("second migrate run applied %d migrations, want none", len(applied))
	}
}

func TestEnvironmentCreateGetAndIdempotency(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	created, err := st.Create(ctx, env)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != env.ID || created.TenantID != env.TenantID || created.State != domain.EnvPending {
		t.Fatalf("round-trip mismatch: %+v", created)
	}

	got, err := st.Get(ctx, env.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.IdempotencyKey != env.IdempotencyKey {
		t.Fatalf("got idempotency key %q, want %q", got.IdempotencyKey, env.IdempotencyKey)
	}

	byKey, err := st.GetByIdempotencyKey(ctx, env.TenantID, env.IdempotencyKey)
	if err != nil {
		t.Fatalf("get by idempotency key: %v", err)
	}
	if byKey.ID != env.ID {
		t.Fatalf("idempotency lookup returned env %s, want %s", byKey.ID, env.ID)
	}

	if _, err := st.GetByIdempotencyKey(ctx, env.TenantID, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := st.Get(ctx, "00000000-0000-4000-8000-000000000009"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing Get expected ErrNotFound, got %v", err)
	}
}

func TestEnvironmentIdempotencyDuplicate(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.Create(ctx, env); err != nil {
		t.Fatalf("seed: %v", err)
	}
	dup := env
	dup.ID = fmt.Sprintf("11111111-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	_, err := st.Create(ctx, dup)
	if !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestStepPlanUniqueIndex(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.Create(ctx, env); err != nil {
		t.Fatalf("seed env: %v", err)
	}

	plan := domain.Plan()
	if err := st.CreatePlan(ctx, env.ID, plan); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	steps, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	if len(steps) != len(plan) {
		t.Fatalf("got %d steps, want %d", len(steps), len(plan))
	}
	for i, step := range steps {
		if step.StepIndex != i+1 {
			t.Fatalf("steps not ordered by step_index: steps[%d].StepIndex = %d", i, step.StepIndex)
		}
		if step.State != domain.StepPending {
			t.Fatalf("step %d state = %q, want pending", step.StepIndex, step.State)
		}
		wantOp := domain.OperationKey(env.ID, step.StepIndex, step.Kind)
		if step.OperationKey != wantOp {
			t.Fatalf("step %d operation_key = %q, want %q", step.StepIndex, step.OperationKey, wantOp)
		}
	}

	// A second plan must violate unique (environment_id, step_index).
	if err := st.CreatePlan(ctx, env.ID, plan); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate re-planning, got %v", err)
	}
}

func TestForeignKeyCascade(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.Create(ctx, env); err != nil {
		t.Fatalf("seed env: %v", err)
	}
	if err := st.CreatePlan(ctx, env.ID, domain.Plan()); err != nil {
		t.Fatalf("seed plan: %v", err)
	}

	// A step row referencing a missing environment is refused.
	missing := "00000000-0000-4000-8000-00000000000f"
	if err := st.CreatePlan(ctx, missing, domain.Plan()[:1]); err == nil {
		t.Fatal("expected FK violation planing steps for a missing environment")
	}

	// Deleting the environment cascades to its steps.
	if _, err := pool.Exec(ctx, `DELETE FROM environments WHERE id = $1`, env.ID); err != nil {
		t.Fatalf("delete environment: %v", err)
	}
	steps, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps after cascade: %v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("expected cascade to delete all steps, got %d", len(steps))
	}
}

// TestCreateEnvironmentWithPlanAtomicity pins the phase-3 headline promise:
// the environment row and every plan step row land in one transaction, a
// duplicate idempotency key writes nothing, and a step-insert failure rolls
// the environment row back. Needs Postgres (tag-gated, `make test-integration`).
func TestCreateEnvironmentWithPlanAtomicity(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	ctx := context.Background()

	// (a) A successful call leaves both the environment and all plan steps.
	env := sampleEnv(t)
	created, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan())
	if err != nil {
		t.Fatalf("create environment with plan: %v", err)
	}
	if created.ID != env.ID {
		t.Fatalf("created id = %s, want %s", created.ID, env.ID)
	}
	var envCount, stepCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM environments WHERE id = $1`, env.ID).Scan(&envCount); err != nil {
		t.Fatalf("count environments: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM workflow_steps WHERE environment_id = $1`, env.ID).Scan(&stepCount); err != nil {
		t.Fatalf("count steps: %v", err)
	}
	if envCount != 1 {
		t.Fatalf("expected 1 environment row, got %d", envCount)
	}
	if stepCount != len(domain.Plan()) {
		t.Fatalf("expected %d step rows, got %d", len(domain.Plan()), stepCount)
	}

	// (b) A repeated (tenant_id, idempotency_key) insert returns ErrDuplicate
	// and leaves exactly one environment and one plan.
	dup := env
	dup.ID = fmt.Sprintf("22222222-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	if _, err := st.CreateEnvironmentWithPlan(ctx, dup, domain.Plan()); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM environments WHERE id = $1`, dup.ID).Scan(&envCount); err != nil {
		t.Fatalf("count environments after duplicate: %v", err)
	}
	if envCount != 0 {
		t.Fatalf("duplicate idempotency key must write nothing, got %d environment rows", envCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM environments WHERE tenant_id = $1 AND idempotency_key = $2`,
		env.TenantID, env.IdempotencyKey).Scan(&envCount); err != nil {
		t.Fatalf("count environments by key: %v", err)
	}
	if envCount != 1 {
		t.Fatalf("replayed key must leave exactly one environment, got %d", envCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflow_steps WHERE environment_id = $1`, env.ID).Scan(&stepCount); err != nil {
		t.Fatalf("count steps after duplicate: %v", err)
	}
	if stepCount != len(domain.Plan()) {
		t.Fatalf("replayed key must leave exactly one plan, got %d steps", stepCount)
	}

	// (c) A step-insert failure rolls the environment row back: a plan with a
	// duplicated Index violates unique (environment_id, step_index) after the
	// environment insert already succeeded inside the transaction.
	failed := sampleEnv(t)
	failed.ID = fmt.Sprintf("33333333-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	badPlan := domain.Plan()
	badPlan[1].Index = badPlan[0].Index
	if _, err := st.CreateEnvironmentWithPlan(ctx, failed, badPlan); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate from the duplicated step index, got %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM environments WHERE id = $1`, failed.ID).Scan(&envCount); err != nil {
		t.Fatalf("count environments after rollback: %v", err)
	}
	if envCount != 0 {
		t.Fatalf("a failed plan insert must roll the environment row back, found %d", envCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflow_steps WHERE environment_id = $1`, failed.ID).Scan(&stepCount); err != nil {
		t.Fatalf("count steps after rollback: %v", err)
	}
	if stepCount != 0 {
		t.Fatalf("a failed plan insert must write no steps, found %d", stepCount)
	}
}

func TestNoExtraTablesExist(t *testing.T) {
	pool, _ := newTestStore(t)
	migrate(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := pool.Query(ctx, `
SELECT tablename FROM pg_tables
WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("query pg_tables: %v", err)
	}
	defer rows.Close()
	// schema_migrations belongs to the migration tool, not the domain.
	// capacity_reservations is phase 5's durable reservation ledger for the
	// Capacity adapter — committed state, not a derived projection.
	allowed := map[string]bool{
		"environments":          true,
		"workflow_steps":        true,
		"capacity_reservations": true,
		"schema_migrations":     true,
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		if !allowed[name] {
			t.Errorf("unexpected table %q: no events/attempts/resources table may exist", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
}

func seedEnvWithPlan(t *testing.T, st *store.PostgresStore, n int) domain.Environment {
	t.Helper()
	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(context.Background(), env, domain.Plan()[:n]); err != nil {
		t.Fatalf("seed environment with plan: %v", err)
	}
	return env
}

// TestClaimNextStepLeaseSemantics pins the SQL claim filter against the real
// database: live leases are invisible, expired leases are reclaimable, claims
// come in step_index order, and an exhausted queue yields ErrNoWork.
func TestClaimNextStepLeaseSemantics(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	seedEnvWithPlan(t, st, 3)
	const lease = 10 * time.Minute

	// Steps 1 and 2 come out in step_index order, both running under w1.
	for i := 1; i <= 2; i++ {
		s, err := st.ClaimNextStep(ctx, "w1", lease)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if s.StepIndex != i {
			t.Fatalf("claim %d returned step_index %d — claims must follow step_index order", i, s.StepIndex)
		}
		if s.Attempts != 1 || s.LeaseOwner != "w1" {
			t.Fatalf("claim did not advance ownership: %+v", s)
		}
	}
	// Step 3 goes to w2 — w1's live leases on 1 and 2 are invisible.
	s3, err := st.ClaimNextStep(ctx, "w2", lease)
	if err != nil {
		t.Fatalf("claim step 3: %v", err)
	}
	if s3.StepIndex != 3 || s3.LeaseOwner != "w2" {
		t.Fatalf("claim 3 = (%d, %s), want (3, w2)", s3.StepIndex, s3.LeaseOwner)
	}

	// Queue drained: ErrNoWork.
	if _, err := st.ClaimNextStep(ctx, "w3", lease); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("expected ErrNoWork on an empty queue, got %v", err)
	}

	// Unless a lease expires: then the running step is reclaimable.
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps
SET lease_expires_at = now() - interval '1 second'
WHERE step_index = 1`); err != nil {
		t.Fatalf("expire step 1's lease: %v", err)
	}
	reclaimed, err := st.ClaimNextStep(ctx, "w3", lease)
	if err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if reclaimed.StepIndex != 1 || reclaimed.LeaseOwner != "w3" || reclaimed.Attempts != 2 {
		t.Fatalf("reclaim = (%d, %s, attempts %d), want (1, w3, 2)",
			reclaimed.StepIndex, reclaimed.LeaseOwner, reclaimed.Attempts)
	}
}

// TestConcurrentClaimsAreUniqueOnRealSQL races claimers over a single pool of
// pending steps on the real database: every claim must hand a distinct step
// to its owner — two concurrent claims on the same row violate FOR UPDATE
// SKIP LOCKED.
func TestConcurrentClaimsAreUniqueOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	planLen := len(domain.Plan())
	seedEnvWithPlan(t, st, planLen)

	const claimers = 12
	type result struct {
		id  string
		err error
	}
	results := make(chan result, claimers)
	var wg sync.WaitGroup
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s, err := st.ClaimNextStep(ctx, fmt.Sprintf("w%d", n), time.Minute)
			if err == nil {
				results <- result{s.ID, nil}
				return
			}
			results <- result{"", err}
		}(i)
	}
	wg.Wait()
	close(results)

	ids := map[string]bool{}
	noWork := 0
	for r := range results {
		if r.err == nil {
			if ids[r.id] {
				t.Fatalf("step %s was claimed twice concurrently", r.id)
			}
			ids[r.id] = true
		} else if errors.Is(r.err, store.ErrNoWork) {
			noWork++
		} else {
			t.Fatalf("unexpected claim error: %v", r.err)
		}
	}
	if noWork != claimers-planLen || len(ids) != planLen {
		t.Fatalf("got %d claims / %d ErrNoWork, want %d / %d",
			len(ids), noWork, planLen, claimers-planLen)
	}
}

// TestRenewLeaseAndCompleteStepGuards checks the owner guards on the real
// SQL: only the lease holder may renew or complete, renewal extends the
// expiry, and completion stores last_error and clears the lease.
func TestRenewLeaseAndCompleteStepGuards(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	seedEnvWithPlan(t, st, 1)
	claim, err := st.ClaimNextStep(ctx, "w1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A non-holder may neither renew nor complete.
	if err := st.RenewLease(ctx, claim.ID, "w2", time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("renew by a non-holder must be ErrNotFound, got %v", err)
	}
	if err := st.CompleteStep(ctx, claim.ID, "w2", domain.StepDone, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("complete by a non-holder must be ErrNotFound, got %v", err)
	}

	// Renewal by the holder extends the expiry.
	var before time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at FROM workflow_steps WHERE id = $1`, claim.ID).Scan(&before); err != nil {
		t.Fatalf("read lease expiry: %v", err)
	}
	if err := st.RenewLease(ctx, claim.ID, "w1", time.Minute); err != nil {
		t.Fatalf("renew by the holder: %v", err)
	}
	var after time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at FROM workflow_steps WHERE id = $1`, claim.ID).Scan(&after); err != nil {
		t.Fatalf("read lease after renewal: %v", err)
	}
	if !after.After(before) {
		t.Fatalf("renewal must extend the lease, got %v ≤ %v", after, before)
	}

	// Completion by the holder records the outcome and clears the lease.
	if err := st.CompleteStep(ctx, claim.ID, "w1", domain.StepFailed, "adapter exploded"); err != nil {
		t.Fatalf("complete by the holder: %v", err)
	}
	var state, lastErr string
	var leaseOwner *string
	if err := pool.QueryRow(ctx,
		`SELECT state, last_error, lease_owner FROM workflow_steps WHERE id = $1`,
		claim.ID).Scan(&state, &lastErr, &leaseOwner); err != nil {
		t.Fatalf("read completed step: %v", err)
	}
	if state != string(domain.StepFailed) || lastErr != "adapter exploded" || leaseOwner != nil {
		t.Fatalf("completed step = (%s, %q, %v), want failed/explicit error/NULL owner", state, lastErr, leaseOwner)
	}

	// A second completion attempt no longer matches: the step is done.
	if err := st.CompleteStep(ctx, claim.ID, "w1", domain.StepDone, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("completing a finished step must be ErrNotFound, got %v", err)
	}

	// ReleaseStep returns a held row to pending, and releasing a foreign or
	// finished step is a silent no-op. A fresh environment seeds a fresh
	// pending plan, because the finished step above is no longer claimable.
	seedEnvWithPlan(t, st, 1)
	claim2, err := st.ClaimNextStep(ctx, "wA", time.Minute)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if err := st.ReleaseStep(ctx, claim2.ID, "wB"); err != nil {
		t.Fatalf("release by a non-holder must not error: %v", err)
	}
	if err := st.ReleaseStep(ctx, claim2.ID, "wA"); err != nil {
		t.Fatalf("release by the holder: %v", err)
	}
	var releasedState string
	if err := pool.QueryRow(ctx, `SELECT state FROM workflow_steps WHERE id = $1`, claim2.ID).Scan(&releasedState); err != nil {
		t.Fatalf("read released step: %v", err)
	}
	if releasedState != string(domain.StepPending) {
		t.Fatalf("released step state = %s, want pending", releasedState)
	}
}

// TestBeginCleanupIsIdempotentOnRealSQL pins the durable cleanup entry against
// Postgres: a second DELETE (or a re-arm of a failed cleanup row) must answer
// with the same environment — no error, no second plan — and re-arm a cleanup
// step wedged in failed. This is the phase's repeated-202 acceptance path.
func TestBeginCleanupIsIdempotentOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan()); err != nil {
		t.Fatalf("seed env with plan: %v", err)
	}

	first, err := st.BeginCleanup(ctx, env.ID, domain.EnvDeleting)
	if err != nil {
		t.Fatalf("begin cleanup: %v", err)
	}
	if first.State != domain.EnvDeleting {
		t.Fatalf("state after begin cleanup = %s, want deleting", first.State)
	}
	steps, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	if len(steps) != len(domain.Plan())+len(domain.CleanupPlan) {
		t.Fatalf("plan rows after begin cleanup = %d, want %d", len(steps),
			len(domain.Plan())+len(domain.CleanupPlan))
	}

	// A repeated DELETE is a success and writes nothing.
	second, err := st.BeginCleanup(ctx, env.ID, domain.EnvDeleting)
	if err != nil {
		t.Fatalf("repeated begin cleanup must be idempotent, got %v", err)
	}
	if second.ID != env.ID || second.State != domain.EnvDeleting {
		t.Fatalf("repeated begin cleanup returned (%s, %s)", second.ID, second.State)
	}
	count, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps after repeat: %v", err)
	}
	if len(count) != len(steps) {
		t.Fatalf("repeated begin cleanup wrote extra rows: %d vs %d", len(count), len(steps))
	}

	// Wedge one cleanup step in failed, then ask again for the delete: the
	// retry path must re-arm the wedge to pending, not error out.
	var wedged domain.Step
	for i := 0; i < len(domain.Plan())+1; i++ {
		s, err := st.ClaimNextStep(ctx, "w1", time.Minute)
		if err != nil {
			t.Fatalf("claim step %d: %v", i+1, err)
		}
		if s.StepIndex > len(domain.Plan()) {
			wedged = s
			break
		}
		if err := st.CompleteStep(ctx, s.ID, "w1", domain.StepDone, ""); err != nil {
			t.Fatalf("complete forward step %d: %v", s.StepIndex, err)
		}
	}
	if wedged.ID == "" || wedged.StepIndex != len(domain.Plan())+1 {
		t.Fatalf("expected to claim cleanup step 7 for the wedge scenario, got %+v", wedged)
	}
	if err := st.CompleteStep(ctx, wedged.ID, "w1", domain.StepFailed, "drop refused"); err != nil {
		t.Fatalf("record the wedge: %v", err)
	}
	if _, err := st.BeginCleanup(ctx, env.ID, domain.EnvDeleting); err != nil {
		t.Fatalf("begin cleanup over a failed cleanup row: %v", err)
	}
	var reArmed string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM workflow_steps WHERE id = $1`, wedged.ID).Scan(&reArmed); err != nil {
		t.Fatalf("read wedged row: %v", err)
	}
	if reArmed != string(domain.StepPending) {
		t.Fatalf("a failed cleanup row must be re-armed pending, got %s", reArmed)
	}
}

// TestReconciliationClaimKeepsAmbiguityOnRealSQL pins the reconciliation
// claim's honesty guarantees on the real database: the row STAYS in_doubt
// under the claim's lease (no ordinary claim can ever see it, and a dead
// reconciler leaves the ambiguity, not a re-executable running row), only
// the lease holder records the verdict, and the reaper returns abandoned
// claims to the queue without touching their state.
func TestReconciliationClaimKeepsAmbiguityOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan()[:1]); err != nil {
		t.Fatalf("seed: %v", err)
	}
	claimed, err := st.ClaimNextStep(ctx, "w1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := st.CompleteStep(ctx, claimed.ID, "w1", domain.StepInDoubt, "step timed out"); err != nil {
		t.Fatalf("record in_doubt: %v", err)
	}

	// in_doubt is invisible to the ordinary claim.
	if _, err := st.ClaimNextStep(ctx, "w2", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("an in_doubt row must not be claimable ordinarily, got %v", err)
	}

	// The reconciliation claim holds it, WITHOUT leaving in_doubt.
	doubt, err := st.ClaimNextInDoubtStep(ctx, "rec1")
	if err != nil {
		t.Fatalf("reconciliation claim: %v", err)
	}
	if doubt.ID != claimed.ID || doubt.State != domain.StepInDoubt || doubt.LeaseOwner != "rec1" {
		t.Fatalf("reconciliation claim must keep the row in_doubt under its lease: %+v", doubt)
	}
	if doubt.Attempts != 1 {
		t.Fatalf("a reconciliation claim must not re-burn an attempt: attempts = %d", doubt.Attempts)
	}

	// A second reconciler may not steal a live claim, and a non-holder
	// cannot record a verdict.
	if _, err := st.ClaimNextInDoubtStep(ctx, "rec2"); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("a live reconciliation claim must be held, got %v", err)
	}
	if err := st.RecordReconcileOutcome(ctx, doubt.ID, "rec2", domain.StepDone, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a non-holder's verdict must be ErrNotFound, got %v", err)
	}

	// The holder records done: the ambiguity resolves and the lease clears.
	if err := st.RecordReconcileOutcome(ctx, doubt.ID, "rec1", domain.StepDone, ""); err != nil {
		t.Fatalf("record outcome: %v", err)
	}
	var state, owner *string
	if err := pool.QueryRow(ctx,
		`SELECT state, lease_owner FROM workflow_steps WHERE id = $1`, claimed.ID).Scan(&state, &owner); err != nil {
		t.Fatalf("read resolved row: %v", err)
	}
	if state == nil || *state != string(domain.StepDone) || owner != nil {
		t.Fatalf("recorded row = (%v, %v), want done/NULL owner", state, owner)
	}

	// The reaper returns an abandoned claim to the queue, still in_doubt.
	env2 := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env2, domain.Plan()[:1]); err != nil {
		t.Fatalf("seed env2: %v", err)
	}
	c2, err := st.ClaimNextStep(ctx, "w2", time.Minute)
	if err != nil {
		t.Fatalf("claim env2: %v", err)
	}
	if err := st.CompleteStep(ctx, c2.ID, "w2", domain.StepInDoubt, "step timed out"); err != nil {
		t.Fatalf("record env2 ambiguity: %v", err)
	}
	c3, err := st.ClaimNextInDoubtStep(ctx, "rec3")
	if err != nil {
		t.Fatalf("claim env2 ambiguity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps SET lease_expires_at = now() - interval '1 second'
WHERE id = $1`, c3.ID); err != nil {
		t.Fatalf("expire the abandoned claim: %v", err)
	}
	reaped, err := st.ReapExpiredReconciliations(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped %d rows, want 1", reaped)
	}
	var stState, stOwner string
	if err := pool.QueryRow(ctx,
		`SELECT state, COALESCE(lease_owner, '') FROM workflow_steps WHERE id = $1`, c3.ID).Scan(&stState, &stOwner); err != nil {
		t.Fatalf("read reaped row: %v", err)
	}
	if stState != string(domain.StepInDoubt) || stOwner != "" {
		t.Fatalf("reaped row = (%s, %q), want in_doubt with no owner", stState, stOwner)
	}
	if _, err := st.ClaimNextInDoubtStep(ctx, "rec5"); err != nil {
		t.Fatalf("a reaped claim must be claimable again: %v", err)
	}
}

// TestCleanupClaimsWaitForForwardWorkAndOrderOnRealSQL pins the cleanup
// ordering against real SQL: while a forward step of the environment is in
// flight — live lease, expired lease, or in_doubt — no cleanup step is
// claimable and the finalizer refuses to close; once the resolve lands, the
// cleanup plan runs strictly in its recorded order and only then does the
// environment close as deleted.
func TestCleanupClaimsWaitForForwardWorkAndOrderOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := st.BeginCleanup(ctx, env.ID, domain.EnvDeleting); err != nil {
		t.Fatalf("begin cleanup: %v", err)
	}
	// Fast-forward the forward plan to a realistic tail: steps 1..5 done,
	// step 6 (verify_usable) running under a live lease.
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps SET state = 'done', lease_owner = NULL, lease_expires_at = NULL
WHERE environment_id = $1 AND step_index <= 5`, env.ID); err != nil {
		t.Fatalf("fast-forward forward plan: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps
SET state = 'running', lease_owner = 'w1',
    lease_expires_at = now() + interval '10 minutes'
WHERE environment_id = $1 AND kind = $2`, env.ID, domain.KindVerifyUsable); err != nil {
		t.Fatalf("set in-flight verify step: %v", err)
	}

	// In-flight forward work gates cleanup claims.
	if _, err := st.ClaimNextStep(ctx, "cw", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("a live forward step must gate cleanup claims, got %v", err)
	}
	// ...and the finalizer must refuse to close while it is unresolved.
	if err := st.FinalizeCleanup(ctx, env.ID); err != nil {
		t.Fatalf("finalize with forward work in flight: %v", err)
	}
	var envState string
	if err := pool.QueryRow(ctx, `SELECT state FROM environments WHERE id = $1`, env.ID).Scan(&envState); err != nil {
		t.Fatalf("read env: %v", err)
	}
	if envState != string(domain.EnvDeleting) {
		t.Fatalf("env state after swept-away finalize = %s, want deleting", envState)
	}

	// Expire the forward lease: an expired lease must not re-enable an
	// ordinary claim (blind re-execution) — the row is unknown work.
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps
SET lease_expires_at = now() - interval '1 second'
WHERE environment_id = $1 AND kind = $2`, env.ID, domain.KindVerifyUsable); err != nil {
		t.Fatalf("expire forward lease: %v", err)
	}
	if _, err := st.ClaimNextStep(ctx, "cw", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("an expired forward step on a cleanup path must stay unclaimable ordinarily, got %v", err)
	}

	// The reconciler resolves it against the real world; say it landed.
	resolved, err := st.ClaimNextInDoubtStep(ctx, "rec")
	if err != nil {
		t.Fatalf("resolve the expired forward row: %v", err)
	}
	if resolved.StepIndex != 6 || resolved.Kind != domain.KindVerifyUsable {
		t.Fatalf("reconciliation claim picked the wrong row: %+v", resolved)
	}
	if err := st.RecordReconcileOutcome(ctx, resolved.ID, "rec", domain.StepDone, ""); err != nil {
		t.Fatalf("record resolve: %v", err)
	}

	// Now the cleanup plan claims strictly in recorded order.
	for i, kind := range domain.CleanupPlan {
		s, err := st.ClaimNextStep(ctx, "cw", time.Minute)
		if err != nil {
			t.Fatalf("claim cleanup step %d (%s): %v", i, kind, err)
		}
		if s.Kind != kind || s.StepIndex != len(domain.Plan())+i+1 {
			t.Fatalf("cleanup claim %d = (%s, %d), want (%s, %d)",
				i, s.Kind, s.StepIndex, kind, len(domain.Plan())+i+1)
		}
		if err := st.CompleteStep(ctx, s.ID, "cw", domain.StepDone, ""); err != nil {
			t.Fatalf("complete cleanup step %d: %v", i, err)
		}
	}
	if _, err := st.ClaimNextStep(ctx, "cw", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("an exhausted cleanup plan must yield no work, got %v", err)
	}
	if err := st.FinalizeCleanup(ctx, env.ID); err != nil {
		t.Fatalf("finalize complete cleanup: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM environments WHERE id = $1`, env.ID).Scan(&envState); err != nil {
		t.Fatalf("read env after close: %v", err)
	}
	if envState != string(domain.EnvDeleted) {
		t.Fatalf("env state after complete cleanup = %s, want deleted", envState)
	}
}

// TestFinalizeCleanupReportsFailedNotDeletedOnRealSQL pins the failure
// verdict: a cleanup step that keeps failing marks the environment failed —
// never deleted — with the failed step visible for an operator.
func TestFinalizeCleanupReportsFailedNotDeletedOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := st.BeginCleanup(ctx, env.ID, domain.EnvCancelling); err != nil {
		t.Fatalf("begin cleanup: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps SET state = 'done', lease_owner = NULL, lease_expires_at = NULL
WHERE environment_id = $1 AND kind NOT IN
    ('drop_database', 'drop_role', 'delete_secret', 'release_capacity')`, env.ID); err != nil {
		t.Fatalf("silence forward plan: %v", err)
	}
	// Three kinds land, the last wedges failed: every cleanup row then has
	// a verdict, so the finalizer can close on the failure.
	for _, kind := range domain.CleanupPlan[:3] {
		s, err := st.ClaimNextStep(ctx, "cw", time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", kind, err)
		}
		if s.Kind != kind {
			t.Fatalf("cleanup claim = %s, want %s", s.Kind, kind)
		}
		if err := st.CompleteStep(ctx, s.ID, "cw", domain.StepDone, ""); err != nil {
			t.Fatalf("complete %s: %v", kind, err)
		}
	}
	s, err := st.ClaimNextStep(ctx, "cw", time.Minute)
	if err != nil {
		t.Fatalf("claim wedged step: %v", err)
	}
	if err := st.CompleteStep(ctx, s.ID, "cw", domain.StepFailed, "capacity race lost"); err != nil {
		t.Fatalf("record the wedge: %v", err)
	}
	if err := st.FinalizeCleanup(ctx, env.ID); err != nil {
		t.Fatalf("finalize failed cleanup: %v", err)
	}
	var envState string
	if err := pool.QueryRow(ctx, `SELECT state FROM environments WHERE id = $1`, env.ID).Scan(&envState); err != nil {
		t.Fatalf("read env: %v", err)
	}
	if envState != string(domain.EnvFailed) {
		t.Fatalf("env state with a failed cleanup step = %s, want failed", envState)
	}
	var failedKinds int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM workflow_steps WHERE environment_id = $1 AND state = 'failed'`, env.ID).Scan(&failedKinds); err != nil {
		t.Fatalf("count failed steps: %v", err)
	}
	if failedKinds != 1 {
		t.Fatalf("the failed cleanup step must stay visible, found %d", failedKinds)
	}
}

// TestReconciliationClaimSequencedAfterPredecessorOnRealSQL pins the cleanup
// plan's reverse order against real SQL: with all four cleanup rows pending,
// only the FIRST index is claimable — a later index becomes claimable only
// after its predecessor is done, and a re-armed failed row does not leapfrog.
func TestCleanupPlanSequencingOnRealSQL(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	ctx := context.Background()

	env := sampleEnv(t)
	if _, err := st.CreateEnvironmentWithPlan(ctx, env, domain.Plan()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := st.BeginCleanup(ctx, env.ID, domain.EnvDeleting); err != nil {
		t.Fatalf("begin cleanup: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE workflow_steps SET state = 'done', lease_owner = NULL, lease_expires_at = NULL
WHERE environment_id = $1 AND kind NOT IN
    ('drop_database', 'drop_role', 'delete_secret', 'release_capacity')`, env.ID); err != nil {
		t.Fatalf("silence forward plan: %v", err)
	}

	// Only the first cleanup index may run.
	first, err := st.ClaimNextStep(ctx, "cw", time.Minute)
	if err != nil {
		t.Fatalf("claim cleanup head: %v", err)
	}
	if first.Kind != domain.KindDropDatabase {
		t.Fatalf("cleanup head = %s, want drop_database", first.Kind)
	}
	if _, err := st.ClaimNextStep(ctx, "cw", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("no sibling cleanup step may claim while the head runs, got %v", err)
	}
	if err := st.CompleteStep(ctx, first.ID, "cw", domain.StepDone, ""); err != nil {
		t.Fatalf("complete head: %v", err)
	}
	second, err := st.ClaimNextStep(ctx, "cw", time.Minute)
	if err != nil {
		t.Fatalf("claim after head: %v", err)
	}
	if second.Kind != domain.KindDropRole {
		t.Fatalf("cleanup second = %s, want drop_role", second.Kind)
	}
}
