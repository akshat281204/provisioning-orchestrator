//go:build integration

// provisioning_faults_test.go — scenarios 1, 2, 4, 5: faults that strike
// during forward provisioning. Each test asserts both the persisted workflow
// state (environment state, step rows, attempt counts) and the external
// resources (databases, roles, secrets, grants proven by a real connection).
package faults

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// TestFault1DatabaseCreatedResponseLost drives failure case 1: the worker
// created the database and the completion record was lost (the "response"
// the worker owes the store never lands). The lease expires, a fresh pool
// reclaims the row, and the idempotent Ensure re-runs — exactly one database
// must exist, and the plan must finish ready.
func TestFault1DatabaseCreatedResponseLost(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault1-response-lost")

	// reserve_capacity lands and is recorded; create_database's side effect
	// lands under a 300ms lease, and the lost record eats the completion.
	rig.walk(t, envID, []domain.StepKind{domain.KindReserveCapacity}, "w-prep", time.Minute)

	exec := rig.executor()
	ctx := context30sCtx(t)
	step, err := rig.st.ClaimNextStep(ctx, "w-crash", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("claim create_database: %v", err)
	}
	if step.Kind != domain.KindCreateDatabase {
		t.Fatalf("claim = %s, want create_database", step.Kind)
	}
	if err := exec.Execute(ctx, step); err != nil {
		t.Fatalf("execute create_database: %v", err)
	}
	// No CompleteStep: the record is lost. The database exists externally;
	// the step still reads running under a 300ms lease.

	// The persisted state while the fault is live: running, not done.
	if got := rig.stepBy(t, envID, domain.KindCreateDatabase); got.State != "running" {
		t.Fatalf("create_database state = %s, want running while the record is lost", got.State)
	}
	if rig.countDatabase(t, adapters.DatabaseName(envID)) != 1 {
		t.Fatalf("the database must already exist while the record is lost")
	}

	time.Sleep(700 * time.Millisecond) // let the lease expire
	rig.startPool(t, poolConfig{})

	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the restarted worker to re-run create_database and finish the plan")

	// External truth: exactly one database despite two executions.
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 1 {
		t.Fatalf("a lost completion record must yield exactly one database, found %d", n)
	}
	// Persisted truth: every forward step done; the reclaimed row shows two
	// attempts (crash + restart).
	if got := rig.stepBy(t, envID, domain.KindCreateDatabase); got.State != "done" || got.Attempts != 2 {
		t.Fatalf("create_database = %s with %d attempts, want done/2", got.State, got.Attempts)
	}
	for _, s := range rig.stepsOf(t, envID) {
		if s.State != "done" {
			t.Fatalf("step %s rests in %s after recovery, want done", s.Kind, s.State)
		}
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment state = %s, want ready", rig.envState(t, envID))
	}
	// Every provisioned resource on the stack must belong to an environment:
	// an extra orphaned database would evade the per-name count above.
	rig.assertResourcesOwned(t)
}

// TestFault2WorkerCrashesAfterCreateRole drives failure case 2: the worker
// created the login role and died before recording completion. Recovery must
// converge on exactly one role (idempotent Ensure, not a duplicate) and a
// ready environment.
func TestFault2WorkerCrashesAfterCreateRole(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault2-crash-after-role")

	rig.walk(t, envID, []domain.StepKind{
		domain.KindReserveCapacity,
		domain.KindCreateDatabase,
		domain.KindStoreCreds,
	}, "w-prep", time.Minute)

	exec := rig.executor()
	ctx := context30sCtx(t)
	step, err := rig.st.ClaimNextStep(ctx, "w-crash", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("claim create_role: %v", err)
	}
	if step.Kind != domain.KindCreateRole {
		t.Fatalf("claim = %s, want create_role", step.Kind)
	}
	if err := exec.Execute(ctx, step); err != nil {
		t.Fatalf("execute create_role: %v", err)
	}
	// Crash: the role exists, the record does not.
	if rig.countRole(t, adapters.RoleName(envID)) != 1 {
		t.Fatalf("the role must exist while the completion is lost")
	}

	time.Sleep(700 * time.Millisecond)
	rig.startPool(t, poolConfig{})

	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the restarted worker to re-run create_role and finish the plan")

	if n := rig.countRole(t, adapters.RoleName(envID)); n != 1 {
		t.Fatalf("a crash after create_role must yield exactly one role, found %d", n)
	}
	if got := rig.stepBy(t, envID, domain.KindCreateRole); got.State != "done" || got.Attempts != 2 {
		t.Fatalf("create_role = %s with %d attempts, want done/2", got.State, got.Attempts)
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment state = %s, want ready", rig.envState(t, envID))
	}

	// The recovered credential is usable from outside the service.
	_, creds := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", faultCaller, "")
	password, _ := creds["password"].(string)
	user, _ := creds["username"].(string)
	database, _ := creds["database"].(string)
	if password == "" {
		t.Fatalf("no retrievable credential after recovery: %v", creds)
	}
	connectOutside(t, rig.database, database, user, password)

	// Every provisioned resource on the stack must belong to an environment:
	// an extra orphaned role would evade the per-name count above.
	rig.assertResourcesOwned(t)
}

// TestFault4GrantsPartiallyFail drives failure case 4: the grant_privileges
// step fails midway (connect lands, usage does not, and vice versa). The
// retry policy must absorb the transient failure; the step must show the
// repeated attempts; the environment must end ready with grants a real
// external connection proves.
func TestFault4GrantsPartiallyFail(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault4-partial-grants")

	faulted := &flakyRoles{Roles: rig.roles}
	faulted.connectLeft.Store(1) // first GrantConnect call fails
	faulted.usageLeft.Store(1)   // first GrantUsage call fails

	exec := &worker.AdapterExecutor{
		Capacity:  rig.capacity,
		Databases: rig.dbs,
		Roles:     faulted,
		Secrets:   rig.vaultSec,
		Verifier:  adapters.NewLocalVerifier(rig.database),
		Envs:      rig.st,
	}
	rig.startPool(t, poolConfig{exec: exec})

	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the pool to absorb the partial grant failures and reach ready")

	// Persisted truth: grant_privileges failed at least twice before it was
	// recorded done — the attempts are on the row, not hidden.
	grant := rig.stepBy(t, envID, domain.KindGrantPrivs)
	if grant.State != "done" {
		t.Fatalf("grant_privileges = %s, want done", grant.State)
	}
	if grant.Attempts < 3 {
		t.Fatalf("grant_privileges attempts = %d, want >= 3 (connect fail, usage fail, success)", grant.Attempts)
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment state = %s, want ready", rig.envState(t, envID))
	}

	// External truth: the role exists once and its grants are effective —
	// proven by connecting as the provisioned role from outside.
	_, creds := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", faultCaller, "")
	password, _ := creds["password"].(string)
	user, _ := creds["username"].(string)
	database, _ := creds["database"].(string)
	if password == "" {
		t.Fatalf("no retrievable credential: %v", creds)
	}
	if n := rig.countRole(t, adapters.RoleName(envID)); n != 1 {
		t.Fatalf("grants recovery must not duplicate the role, found %d", n)
	}
	connectOutside(t, rig.database, database, user, password)
}

// TestFault5SecretsUnavailableAfterDatabase drives failure case 5: the
// secrets service (Vault) is unavailable once the database exists, so
// store_credentials fails repeatedly; when the service returns, the retry
// policy converges. The database must not be leaked or re-created, and the
// stored secret must match the plan.
func TestFault5SecretsUnavailableAfterDatabase(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault5-vault-down")

	faulted := &flakySecrets{Secrets: rig.vaultSec}
	faulted.getLeft.Store(2) // first two Get calls fail
	faulted.putLeft.Store(1) // first Put call fails

	exec := &worker.AdapterExecutor{
		Capacity:  rig.capacity,
		Databases: rig.dbs,
		Roles:     rig.roles,
		Secrets:   faulted,
		Verifier:  adapters.NewLocalVerifier(rig.database),
		Envs:      rig.st,
	}
	rig.startPool(t, poolConfig{exec: exec})

	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the pool to store credentials after the outage and reach ready")

	// The database was created before the outage and was never duplicated.
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 1 {
		t.Fatalf("an outage after create_database must leave exactly one database, found %d", n)
	}
	// Persisted truth: store_credentials shows the outage as attempts.
	stored := rig.stepBy(t, envID, domain.KindStoreCreds)
	if stored.State != "done" {
		t.Fatalf("store_credentials = %s, want done", stored.State)
	}
	if stored.Attempts < 3 {
		t.Fatalf("store_credentials attempts = %d, want >= 3 (two Get failures + one Put failure + success)",
			stored.Attempts)
	}
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment state = %s, want ready", rig.envState(t, envID))
	}

	// External truth: the secret exists, matches the plan, and the password
	// inside it opens a real connection.
	payload, err := rig.vaultSec.Get(context30sCtx(t), envID)
	if err != nil {
		t.Fatalf("read stored secret: %v", err)
	}
	if payload["database"] != adapters.DatabaseName(envID) ||
		payload["username"] != adapters.RoleName(envID) || payload["password"] == "" {
		t.Fatalf("stored payload does not match the plan: keys %v", payload)
	}
	connectOutside(t, rig.database, payload["database"], payload["username"], payload["password"])
}

// context30sCtx is a pre-cancelled-at-cleanup context for short store calls.
func context30sCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
