// Package store provides durable persistence for environments and workflow
// steps. The schema is deliberately lean: two tables (environments,
// workflow_steps) enforced by constraints — there is no events table, no
// attempts table (attempts live on the step row), and no resources table.
package store

import (
	"context"
	"errors"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// Sentinel errors callers can branch on.
var (
	// ErrNotFound means the requested row does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicate means a uniqueness constraint was hit — for environments
	// that is (tenant_id, idempotency_key).
	ErrDuplicate = errors.New("store: duplicate")
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
