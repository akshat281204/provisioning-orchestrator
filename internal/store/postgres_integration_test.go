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
	if _, err := pool.Exec(context.Background(), `TRUNCATE workflow_steps, environments`); err != nil {
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
	allowed := map[string]bool{"environments": true, "workflow_steps": true, "schema_migrations": true}
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
