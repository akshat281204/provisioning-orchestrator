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
)

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
