//go:build integration

// coordination_faults_test.go — scenarios 3, 6, 9: faults that strike the
// coordination machinery itself — the readiness gate, the capacity limit,
// and the lease. Every assertion lands on persisted rows and external
// resources, not HTTP codes.
package faults

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// TestFault3ReadinessDuplicatedAndAfterCancel drives failure case 3: a
// readiness event that is recorded twice, and one that tries to land after
// the environment was cancelled. Ready is only ever written by the
// transaction that completes a verification step on a `verifying`
// environment — both paths must fail closed, and cancellation must still
// clean every external resource up.
func TestFault3ReadinessDuplicatedAndAfterCancel(t *testing.T) {
	// Part A: a duplicated readiness event.
	rig := newRig(t)
	envID := rig.createEnv(t, "fault3-duplicate-ready")

	rig.walk(t, envID, []domain.StepKind{
		domain.KindReserveCapacity,
		domain.KindCreateDatabase,
		domain.KindStoreCreds,
		domain.KindCreateRole,
		domain.KindGrantPrivs,
	}, "w-prep", time.Minute)

	// Claim and complete verification manually, twice.
	ctx := context30sCtx(t)
	step, err := rig.st.ClaimNextStep(ctx, "w-verify", time.Minute)
	if err != nil {
		t.Fatalf("claim verify: %v", err)
	}
	if step.Kind != domain.KindVerifyUsable {
		t.Fatalf("claim = %s, want verify_usable", step.Kind)
	}
	exec := rig.executor()
	if err := exec.Execute(ctx, step); err != nil {
		t.Fatalf("execute verify: %v", err)
	}
	if err := rig.st.CompleteVerification(ctx, step.ID, "w-verify", envID); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment = %s, want ready after the first completion", rig.envState(t, envID))
	}

	// The duplicate: the step is done and its lease cleared, so the second
	// completion cannot land — ready must never be recorded twice.
	if err := rig.st.CompleteVerification(ctx, step.ID, "w-verify", envID); err == nil {
		t.Fatalf("a duplicate readiness event must be refused, got success")
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment = %s after the refused duplicate, want ready", rig.envState(t, envID))
	}
	if n := rig.countTimeline(t, envID, "ready"); n != 1 {
		t.Fatalf("ready recorded %d times in the timeline, want 1", n)
	}
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 1 {
		t.Fatalf("duplicate readiness must not duplicate the database, found %d", n)
	}
	rig.assertResourcesOwned(t)

	// Part B: readiness arriving after cancellation.
	rig2 := newRig(t)
	env2 := rig2.createEnv(t, "fault3-ready-after-cancel")
	rig2.walk(t, env2, []domain.StepKind{
		domain.KindReserveCapacity,
		domain.KindCreateDatabase,
		domain.KindStoreCreds,
		domain.KindCreateRole,
		domain.KindGrantPrivs,
	}, "w-prep", time.Minute)

	vstep, err := rig2.st.ClaimNextStep(ctx, "w-old", time.Minute)
	if err != nil {
		t.Fatalf("claim verify: %v", err)
	}
	// Use rig2's own executor for rig2's environment: the adapters derive
	// everything from the env id today, but wiring the owning rig keeps the
	// scenario honest if that ever changes.
	exec2 := rig2.executor()
	if err := exec2.Execute(ctx, vstep); err != nil {
		t.Fatalf("execute verify: %v", err)
	}
	if rig2.envState(t, env2) != domain.EnvVerifying {
		t.Fatalf("environment = %s, want verifying before the cancel", rig2.envState(t, env2))
	}

	// Cancel while the verification step is still in flight (its lease held).
	code, resp := rig2.call(t, http.MethodPost, "/environments/"+env2+"/cancel", "", "")
	if code != http.StatusAccepted {
		t.Fatalf("cancel: %d %v", code, resp)
	}
	// The late readiness event: the old worker finally tries to record the
	// verification. The environment is no longer verifying — ready must be
	// refused even though the step is still running under a live lease.
	if err := rig2.st.CompleteVerification(ctx, vstep.ID, "w-old", env2); err == nil {
		t.Fatalf("a readiness event arriving after cancellation must be refused")
	}
	if rig2.envState(t, env2) == domain.EnvReady {
		t.Fatalf("ready was granted after cancellation")
	}

	// Shutdown releases the in-flight step; the pool then runs the cleanup
	// plan to its terminal verdict.
	if err := rig2.st.ReleaseStep(ctx, vstep.ID, "w-old"); err != nil {
		t.Fatalf("release the in-flight verify step: %v", err)
	}
	rig2.startPool(t, poolConfig{})

	waitFor(t, func() bool { return rig2.envState(t, env2) == domain.EnvCancelled },
		"the cancelled environment to conclude its cleanup")
	if n := rig2.countTimeline(t, env2, "ready"); n != 0 {
		t.Fatalf("ready appears %d times after cancellation, want 0", n)
	}
	if n := rig2.countDatabase(t, adapters.DatabaseName(env2)); n != 0 {
		t.Fatalf("cancellation after a late readiness attempt left the database behind")
	}
	if n := rig2.countRole(t, adapters.RoleName(env2)); n != 0 {
		t.Fatalf("cancellation after a late readiness attempt left the role behind")
	}
	if _, err := rig2.vaultSec.Get(ctx, env2); !errIsNotFoundSecret(err) {
		t.Fatalf("cancellation left the credential secret behind: %v", err)
	}
	if n := rig2.countReservations(t, env2); n != 0 {
		t.Fatalf("cancellation left a capacity reservation held")
	}
}

// errIsNotFoundSecret reports whether err reports an absent Vault secret.
func errIsNotFoundSecret(err error) bool {
	return errors.Is(err, adapters.ErrSecretNotFound)
}

// TestFault6CapacityLastSlotRace drives failure case 6: two environments
// race for the last capacity slot. Exactly one may hold it — the loser's
// reserve step fails durably and retries until its budget is spent, its
// environment must hold no external resources, and the winner must become
// ready with its one slot.
func TestFault6CapacityLastSlotRace(t *testing.T) {
	rig := newRig(t)
	envA := rig.createEnv(t, "fault6-race-a")
	envB := rig.createEnv(t, "fault6-race-b")

	// Leave exactly one free slot: the shared table's current rows plus one.
	rig.capacity.Limit = rig.totalReservations(t) + 1

	// Bounded retries: the loser must exhaust its budget quickly.
	retry := shortRetry
	retry.MaxAttempts = 3
	rig.startPool(t, poolConfig{retry: &retry})

	waitFor(t, func() bool {
		return rig.envIsReady(t, envA) || rig.envIsReady(t, envB)
	}, "the slot winner to become ready")

	waitFor(t, func() bool {
		a, b := rig.stepBy(t, envA, domain.KindReserveCapacity),
			rig.stepBy(t, envB, domain.KindReserveCapacity)
		return (a.State == "done" || a.State == "failed") &&
			(b.State == "done" || b.State == "failed")
	}, "both reserve steps to reach a verdict")

	// Exactly one winner, one loser.
	var winner, loser string
	if rig.stepBy(t, envA, domain.KindReserveCapacity).State == "done" {
		winner, loser = envA, envB
	} else {
		winner, loser = envB, envA
	}
	if rig.stepBy(t, loser, domain.KindReserveCapacity).State != "failed" {
		t.Fatalf("the slot loser's reserve step must rest failed")
	}
	loserRow := rig.stepBy(t, loser, domain.KindReserveCapacity)
	if loserRow.Attempts != 3 {
		t.Fatalf("loser reserve attempts = %d, want 3 (its whole retry budget)", loserRow.Attempts)
	}
	// External truth: exactly one reservation between the two environments.
	if rig.countReservations(t, winner) != 1 {
		t.Fatalf("the winner must hold exactly one reservation")
	}
	if rig.countReservations(t, loser) != 0 {
		t.Fatalf("the loser must hold no reservation")
	}

	// The loser's environment carries no other external resource: its plan
	// never got past reserve_capacity.
	if n := rig.countDatabase(t, adapters.DatabaseName(loser)); n != 0 {
		t.Fatalf("the loser must have created no database, found %d", n)
	}
	if n := rig.countRole(t, adapters.RoleName(loser)); n != 0 {
		t.Fatalf("the loser must have created no role, found %d", n)
	}
	if _, err := rig.vaultSec.Get(context30sCtx(t), loser); !errIsNotFoundSecret(err) {
		t.Fatalf("the loser must have stored no secret: %v", err)
	}

	// The loser's workflow rests honestly: an operator decision (cancel)
	// moves it to the terminal cleanup path, and the pool cleans it up.
	code, resp := rig.call(t, http.MethodPost, "/environments/"+loser+"/cancel", "", "")
	if code != http.StatusAccepted {
		t.Fatalf("cancel the loser: %d %v", code, resp)
	}
	waitFor(t, func() bool { return rig.envState(t, loser) == domain.EnvCancelled },
		"the loser's cleanup to conclude")
	if n := rig.countReservations(t, loser); n != 0 {
		t.Fatalf("the loser's cleanup must leave no reservation")
	}
	// The winner is untouched and still holds its slot.
	if rig.envState(t, winner) != domain.EnvReady {
		t.Fatalf("winner state = %s, want ready", rig.envState(t, winner))
	}
	if rig.countReservations(t, winner) != 1 {
		t.Fatalf("the winner's reservation must survive the loser's cleanup")
	}
}

// TestFault9LeaseExpiresWhileWorkerDelayed drives failure case 9: a worker
// claims a step, is delayed past its lease, and another worker reclaims and
// finishes the work. The delayed worker's late completion must be fenced
// out, the step must show exactly two attempts, and exactly one database
// must exist.
func TestFault9LeaseExpiresWhileWorkerDelayed(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault9-lease-expiry")

	rig.walk(t, envID, []domain.StepKind{domain.KindReserveCapacity}, "w-prep", time.Minute)

	// The slow worker claims create_database under a 400ms lease and is
	// "delayed" (its side effect lands; its record does not, yet).
	ctx := context30sCtx(t)
	step, err := rig.st.ClaimNextStep(ctx, "w-slow", 400*time.Millisecond)
	if err != nil {
		t.Fatalf("claim create_database: %v", err)
	}
	if step.Kind != domain.KindCreateDatabase {
		t.Fatalf("claim = %s, want create_database", step.Kind)
	}
	if err := rig.executor().Execute(ctx, step); err != nil {
		t.Fatalf("execute create_database: %v", err)
	}

	// The lease expires while w-slow is still delayed; a fresh pool reclaims
	// the row and completes the plan.
	time.Sleep(700 * time.Millisecond)
	rig.startPool(t, poolConfig{})
	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the reclaiming pool to finish the plan")

	// The delayed worker finally tries to record its outcome — the lease is
	// long gone, so the store must refuse it.
	if err := rig.st.CompleteStep(ctx, step.ID, "w-slow", domain.StepDone, ""); err == nil {
		t.Fatalf("a completion from an expired lease must be refused")
	}

	// External truth: the delayed execution and the reclaiming execution
	// collapsed onto exactly one database.
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 1 {
		t.Fatalf("a lease handoff must yield exactly one database, found %d", n)
	}
	// Persisted truth: two attempts (the delayed one and the reclaim), done.
	got := rig.stepBy(t, envID, domain.KindCreateDatabase)
	if got.State != "done" || got.Attempts != 2 {
		t.Fatalf("create_database = %s with %d attempts, want done/2", got.State, got.Attempts)
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment state = %s, want ready", rig.envState(t, envID))
	}
	// Every provisioned resource on the stack must belong to an environment:
	// an extra orphaned database would evade the per-name count above.
	rig.assertResourcesOwned(t)
}
