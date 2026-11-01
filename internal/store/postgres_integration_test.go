//go:build integration

// Integration tests for the lean two-table schema. They need Postgres (e.g.
// `make up`, `make test-integration`) and run migrations for real.
package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
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
