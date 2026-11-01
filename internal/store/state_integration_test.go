//go:build integration

// Integration tests for the phase-6 state machine on the real store: the
// guarded CAS transitions, the verification-completion transaction (ready
// exists only inside it), and the rotation-completion transaction.
package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// claimVerifyStep claims the (only) planned step while the tests run each
// scenario against a freshly truncated table, so the pool's oldest-first
// claim always returns the verify step.
func claimVerifyStep(t *testing.T, st *store.PostgresStore, owner string) domain.Step {
	t.Helper()
	step, err := st.ClaimNextStep(context.Background(), owner, time.Minute)
	if err != nil {
		t.Fatalf("claim step: %v", err)
	}
	if step.Kind != domain.KindVerifyUsable {
		t.Fatalf("claim returned kind %q, want verify_usable", step.Kind)
	}
	return step
}

// envState reads the environment state back through the store.
func envState(t *testing.T, st *store.PostgresStore, envID string) domain.EnvState {
	t.Helper()
	env, err := st.Get(context.Background(), envID)
	if err != nil {
		t.Fatalf("get env %s: %v", envID, err)
	}
	return env.State
}

// newEnvWithVerifyPlan creates a pending environment plus a one-step plan
// (the verify step) so state moves can be exercised in isolation.
func newEnvWithVerifyPlan(t *testing.T, st *store.PostgresStore) domain.Environment {
	t.Helper()
	env := sampleEnv(t)
	if _, err := st.Create(context.Background(), env); err != nil {
		t.Fatalf("create env: %v", err)
	}
	if err := st.CreatePlan(context.Background(), env.ID, []domain.PlannedStep{
		{Index: 1, Kind: domain.KindVerifyUsable},
	}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	return env
}

func TestStateWalksPendingThroughVerifying(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)
	ctx := context.Background()

	if err := st.BeginProvisioning(ctx, env.ID); err != nil {
		t.Fatalf("begin provisioning: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvProvisioning {
		t.Fatalf("state after BeginProvisioning: %q", got)
	}
	// A second begin is a no-op (idempotent re-assertion of the same hop).
	if err := st.BeginProvisioning(ctx, env.ID); err != nil {
		t.Fatalf("second begin provisioning: %v", err)
	}

	if err := st.BeginVerification(ctx, env.ID); err != nil {
		t.Fatalf("begin verification: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvVerifying {
		t.Fatalf("state after BeginVerification: %q", got)
	}
}

func TestBeginVerificationWalksBothHopsFromPending(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)

	if err := st.BeginVerification(context.Background(), env.ID); err != nil {
		t.Fatalf("begin verification from pending: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvVerifying {
		t.Fatalf("state: %q, want verifying", got)
	}
}

func TestTransitionEnvironmentRejectsReady(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)

	// A direct jump into ready is rejected by the store guard — no state
	// transition writes ready; only the two completion transactions do.
	if err := st.TransitionEnvironment(context.Background(), env.ID, domain.EnvReady); err == nil {
		t.Fatal("TransitionEnvironment wrote ready; it must never")
	}
	// The domain table refuses the same move first.
	if _, err := domain.Transition(domain.EnvPending, domain.EnvReady); err == nil {
		t.Fatal("domain accepted pending -> ready")
	}
}

func TestCompleteVerificationIsTheOnlyReadyWriter(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)
	const owner = "verification-owner"
	ctx := context.Background()

	// Claim-less attempt: the step is not running → nothing is written.
	if err := st.CompleteVerification(ctx, "no-step", owner, env.ID); err == nil {
		t.Fatal("completing an unheld verification step must fail")
	}

	if err := st.BeginVerification(ctx, env.ID); err != nil {
		t.Fatalf("begin verification: %v", err)
	}
	verify := claimVerifyStep(t, st, owner)

	if err := st.CompleteVerification(ctx, verify.ID, owner, env.ID); err != nil {
		t.Fatalf("complete verification: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvReady {
		t.Fatalf("state after verification: %q, want ready", got)
	}
	steps, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	if steps[0].State != domain.StepDone {
		t.Errorf("verify step state: %q, want done", steps[0].State)
	}
}

// The transaction must be all-or-nothing: an environment not in verifying
// records nothing, and the step's done never lands without it.
func TestCompleteVerificationRefusesWrongPriorState(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)
	const owner = "verification-owner"

	// The environment stays pending (verification never began) while the
	// step runs under owner: ready cannot be granted.
	verify := claimVerifyStep(t, st, owner)
	if err := st.CompleteVerification(context.Background(), verify.ID, owner, env.ID); err == nil {
		t.Fatal("ready granted from a non-verifying environment")
	}
	if got := envState(t, st, env.ID); got == domain.EnvReady {
		t.Fatal("environment became ready from pending")
	}
}

func TestCompleteRotationRecordsAndReturnsToReady(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	truncate(t, pool)
	env := newEnvWithVerifyPlan(t, st)
	const owner = "rotation-owner"
	ctx := context.Background()

	if err := st.BeginVerification(ctx, env.ID); err != nil {
		t.Fatalf("begin verification: %v", err)
	}
	verify := claimVerifyStep(t, st, owner)
	if err := st.CompleteVerification(ctx, verify.ID, owner, env.ID); err != nil {
		t.Fatalf("complete verification: %v", err)
	}

	if err := st.WriteConnectionMeta(ctx, env.ID, []byte(
		`{"database":"db1","username":"u1","credentialRef":"secret/p"}`)); err != nil {
		t.Fatalf("write connection metadata: %v", err)
	}

	// Rotation: ready -> rotating, apply externally, then CompleteRotation
	// returns the environment to ready and appends the audit entry.
	if err := st.TransitionEnvironment(ctx, env.ID, domain.EnvRotating); err != nil {
		t.Fatalf("start rotation: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvRotating {
		t.Fatalf("state while rotating: %q", got)
	}
	if err := st.CompleteRotation(ctx, env.ID, []byte(`{"at":"2026-01-01T00:00:00Z","by":"operator"}`)); err != nil {
		t.Fatalf("complete rotation: %v", err)
	}
	if got := envState(t, st, env.ID); got != domain.EnvReady {
		t.Fatalf("state after rotation: %q, want ready", got)
	}
	steps, err := st.ListForEnvironment(ctx, env.ID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	if steps[0].State != domain.StepDone {
		t.Fatalf("verify step after rotation: state=%q, want done", steps[0].State)
	}
	if len(adapters.SecretPath(env.ID)) == 0 || len(steps[0].Metadata) == 0 {
		t.Error("rotation metadata not recorded on the verify step")
	}
}

// TestRotationLockBusyAndRecovery exercises AcquireRotationLock on the real
// advisory lock. It pins the two failure modes the implementation's design
// exists to prevent: a bounded wait under a held lock answers ErrRotationBusy
// (never deadlocks, never reports "busy" while actually leaving the lock
// granted on a pooled session), and after release the lock is gone — every
// later rotation can still acquire it.
func TestRotationLockBusyAndRecovery(t *testing.T) {
	pool, st := newTestStore(t)
	migrate(t, pool)
	lockID := fmt.Sprintf("00000000-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)

	release, err := st.AcquireRotationLock(context.Background(), lockID)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// While the lock is held, a second acquirer with its own short deadline
	// must bound out as busy — and must not succeed silently.
	busyAttempt := func() error {
		wctx, wcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer wcancel()
		_, err := st.AcquireRotationLock(wctx, lockID)
		return err
	}
	if err := busyAttempt(); !errors.Is(err, store.ErrRotationBusy) {
		t.Fatalf("wait on a held lock: %v, want ErrRotationBusy", err)
	}
	// Further attempts, again and again: a leaked lock (granted on a pooled
	// session that was "released") would flip later acquires into
	// unexplained errors or permanent busy with no recovery; a clean pool
	// keeps answering a consistent busy here.
	for i := 0; i < 3; i++ {
		if err := busyAttempt(); !errors.Is(err, store.ErrRotationBusy) {
			t.Fatalf("bounded acquire attempt %d: %v, want ErrRotationBusy", i+2, err)
		}
	}

	// Release, then a fresh acquirer must still get the lock: the failed
	// acquirers must not have leaked or broken pool state.
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	after, err := st.AcquireRotationLock(context.Background(), lockID)
	if err != nil {
		t.Fatalf("acquire after release (lock must be free): %v", err)
	}
	if err := after(); err != nil {
		t.Fatalf("second release: %v", err)
	}
}
