// Package store provides durable persistence for environments and workflow
// steps. The schema is deliberately lean: two tables (environments,
// workflow_steps) enforced by constraints — there is no events table, no
// attempts table (attempts live on the step row), and no resources table.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// Sentinel errors callers can branch on.
var (
	// ErrNotFound means the requested row does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicate means a uniqueness constraint was hit — for environments
	// that is (tenant_id, idempotency_key).
	ErrDuplicate = errors.New("store: duplicate")
	// ErrNoWork means no step is claimable right now: nothing is pending and
	// no claimed step's lease has expired.
	ErrNoWork = errors.New("store: no claimable step")
	// ErrRotationBusy means another rotation already holds the per-environment
	// rotation lock; the caller must not mutate Vault or the role concurrently
	// with it.
	ErrRotationBusy = errors.New("store: rotation already in progress")
	// ErrIllegalTransition means the environment's persisted state does not
	// allow the requested move (e.g. deleting a deleted or rolled-back
	// environment). HTTP surfaces map it onto a 409, not a 500.
	ErrIllegalTransition = errors.New("store: illegal state transition")
)

// CleanupStore begins the durable cleanup workflow for an environment.
type CleanupStore interface {
	// BeginCleanup moves the environment to `target` (cancelling or
	// deleting) and records the compensation steps (the cleanup plan) as
	// rows in the same transaction, so an environment is never reported
	// deleted while cleanup is pending and a cancellation never loses its
	// compensation plan. It is idempotent: repeating the call re-routes or
	// completes, never writes a second plan. A move the state machine
	// rejects wraps ErrIllegalTransition.
	BeginCleanup(ctx context.Context, environmentID string, target domain.EnvState) (domain.Environment, error)
}

// EnvironmentStore persists environments.
type EnvironmentStore interface {
	// Create persists a new environment. A (tenant_id, idempotency_key)
	// collision returns an error wrapping ErrDuplicate.
	Create(ctx context.Context, env domain.Environment) (domain.Environment, error)
	// Get returns one environment by id; a missing id returns ErrNotFound.
	Get(ctx context.Context, id string) (domain.Environment, error)
	// GetByIdempotencyKey looks an environment up by its (tenant, key) pair;
	// a miss returns ErrNotFound.
	GetByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (domain.Environment, error)
}

// StepStore persists the workflow_steps rows of an environment.
type StepStore interface {
	// CreatePlan inserts the full step plan for an environment atomically;
	// any (environment_id, step_index) collision errors.
	CreatePlan(ctx context.Context, environmentID string, steps []domain.PlannedStep) error
	// ListForEnvironment returns the steps ordered by step_index; a missing
	// environment yields an empty slice, not NotFound.
	ListForEnvironment(ctx context.Context, environmentID string) ([]domain.Step, error)
}

// LeaseStore picks steps from the shared work queue and coordinates leases.
// A step may be claimed when it is pending, or when it is running but its
// lease has expired — the latter is the whole crash recovery mechanism.
type LeaseStore interface {
	// ClaimNextStep atomically claims the oldest claimable step
	// (by step_index) for owner: pending, or running with an expired
	// lease. It advances the step to running, sets lease_owner and
	// lease_expires_at to now()+lease, and increments attempts. When nothing
	// is claimable it returns an error wrapping ErrNoWork.
	ClaimNextStep(ctx context.Context, owner string, lease time.Duration) (domain.Step, error)
	// RenewLease extends owner's lease on stepID to now()+lease. It fails
	// wrapping ErrNotFound when the step is not running under that owner
	// (the lease was lost or the step finished) — renewers must stop.
	RenewLease(ctx context.Context, stepID, owner string, lease time.Duration) error
	// CompleteStep records an executed outcome (done or failed) for stepID
	// while it is running under owner, clearing the lease and storing
	// lastErr verbatim. It fails wrapping ErrNotFound when the caller does
	// not hold the step — e.g. the lease expired and another owner took it.
	CompleteStep(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error
	// ReleaseStep returns stepID to pending, clearing the lease. Used on
	// worker shutdown so in-flight steps are not left running until their
	// leases expire. Failing is not an error path the caller can act on:
	// a step not held by owner is either reclaimed or already finished.
	ReleaseStep(ctx context.Context, stepID, owner string) error
}

// ReconcileStore handles the two phase-7 paths the ordinary claim loop must
// never touch: the resolution of `in_doubt` steps against the real world,
// and the retry/counterfactual bookkeeping around a failed step.
type ReconcileStore interface {
	// ClaimNextInDoubtStep claims the oldest `in_doubt` step for owner,
	// taking the reconciliation lease on the row WITHOUT changing its
	// state: the row stays `in_doubt` until an outcome is recorded, so a
	// claim that dies leaves the ambiguity durable and no lease expiry can
	// hand the step to an executor. `in_doubt` steps are invisible to
	// ClaimNextStep; this is the only claim path that touches them. It
	// returns an error wrapping ErrNoWork when none are left.
	ClaimNextInDoubtStep(ctx context.Context, owner string) (domain.Step, error)
	// ScheduleRetry returns a FAILED step to pending with its lease set to
	// now()+delay, which makes it unclaimable until the backoff elapses
	// (ClaimNextStep only claims a pending row whose lease is empty or
	// expired). The delay is the durable backoff marker, not process state.
	ScheduleRetry(ctx context.Context, stepID string, delay time.Duration) error
	// MarkEnvironmentInDoubt moves the environment to in_doubt when one of
	// its steps resolved to unknown. Illegal edges (the state machine has
	// no in_doubt successor for the current state) are silently ignored:
	// the step's own in_doubt state still records the ambiguity.
	MarkEnvironmentInDoubt(ctx context.Context, environmentID string) error
	// ResumeFromInDoubt moves an in_doubt environment back to provisioning
	// (step index < verify) or verifying (the verify step itself) once its
	// ambiguous step has been reconciled. Non-in_doubt environments pass
	// through unchanged.
	ResumeFromInDoubt(ctx context.Context, environmentID string, stepIndex int) error
	// RecordReconcileOutcome writes the reconciler's verdict for a step the
	// reconciler still holds on its in_doubt (or borrowed expired) row:
	// done/failed/pending, or in_doubt again for an unobservable world. It
	// fails wrapping ErrNotFound when the lease was lost. This — not
	// CompleteStep — is the reconciler's record path, so an ambiguity is
	// never resolved by whoever happens to claim a `running` row.
	RecordReconcileOutcome(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error
	// ReapExpiredReconciliations clears the leases of abandoned
	// reconciliation claims (in_doubt rows whose reconciler died or whose
	// lease expired mid-look), returning them claimable again while the
	// rows stay in_doubt. It returns the count reaped.
	ReapExpiredReconciliations(ctx context.Context) (int64, error)
	// ListCleanupEnvironments returns the ids of every environment on a
	// cleanup path (cancelling, deleting, failed) that carries cleanup-plan
	// rows — the resume sweep's in_doubt-style worklist.
	ListCleanupEnvironments(ctx context.Context) ([]string, error)
	// ListFailedSteps returns every failed step with a cleared lease, for
	// the resume sweep to re-schedule under the row's attempt cap.
	ListFailedSteps(ctx context.Context) ([]domain.Step, error)
	// FinalizeCleanup inspects a deleting/cancelling/failed environment's
	// cleanup steps and closes the workflow: every step done → terminal
	// (deleted or cancelled); any step failed → failed. It is safe to call
	// repeatedly and is a no-op for environments not on a cleanup path.
	FinalizeCleanup(ctx context.Context, environmentID string) error
}

// StateStore moves environments along their state machine. Every move is a
// compare-and-set on the persisted state validated by the domain transition
// table first, so concurrent writers can never walk the machine past an
// illegal edge, and any `ready` write is confined to the two transactions
// that earned it (see CompleteVerification and CompleteRotation).
type StateStore interface {
	// BeginProvisioning moves pending -> provisioning; a no-op when the
	// environment is already provisioning or beyond it.
	BeginProvisioning(ctx context.Context, environmentID string) error
	// BeginVerification walks pending -> provisioning -> verifying. This is
	// the only path that carries an environment into `verifying`, the sole
	// ordinary state from which `ready` is reachable.
	BeginVerification(ctx context.Context, environmentID string) error
	// TransitionEnvironment CAS-moves the environment to `target`. `ready`
	// is deliberately rejected here — the two legitimate producers of
	// `ready` are the verification and rotation completion transactions.
	TransitionEnvironment(ctx context.Context, environmentID string, target domain.EnvState) error
	// WriteConnectionMeta merges the non-secret connection facts (database,
	// username, credentialRef) into the environment's metadata. Called only
	// after a successful verification probe; retries converge.
	WriteConnectionMeta(ctx context.Context, environmentID string, meta []byte) error
	// CompleteVerification marks the running verification step done for
	// owner and flips the environment verifying -> ready in ONE transaction.
	// A lost lease or a mismatched prior state errors and nothing is written.
	CompleteVerification(ctx context.Context, stepID, owner, environmentID string) error
	// CompleteRotation records one rotation on the verify step's metadata
	// and flips rotating -> ready in ONE transaction.
	CompleteRotation(ctx context.Context, environmentID string, entry []byte) error
}
