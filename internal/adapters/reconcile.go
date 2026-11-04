// reconcile.go — the phase-7 resolver. When a step's execution context
// expires mid-flight its outcome is unknown: the executor may have done
// partial external work, so the answer is not "retry" but "look". For every
// step kind the real world can answer one of exactly three questions:
//
//	exists-and-matches → done  (the side effect landed; record it)
//	absent             → pending (nothing happened; let the plan re-run it)
//	exists-but-differs → failed (a verdict cannot be recorded; look at it)
//
// Resource names derive from the environment id alone (naming.go), so the
// reconciler re-derives the same database, role, and secret path the
// executor would have touched and the mapping stays exact.
package adapters

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// ReconcileOutcome is what the world says about an in_doubt step. It maps
// one-to-one onto the step states the reconciler may record.
type ReconcileOutcome string

// The three observable outcomes.
const (
	// OutcomeDone: the step's side effect exists and matches the plan; the
	// step can be recorded done.
	OutcomeDone ReconcileOutcome = "done"
	// OutcomePending: the resource the step would create is absent, so the
	// attempt provably did not finish; the step returns to pending.
	OutcomePending ReconcileOutcome = "pending"
	// OutcomeFailed: the resource exists but disagrees with the plan (a
	// secret with a broken payload). It is a verdict-shaped failure, not an
	// ambiguity, and must be surfaced as step failed.
	OutcomeFailed ReconcileOutcome = "failed"
)

// The read-only probes the reconciler needs. They are deliberately tighter
// than the write interfaces (Capacity, Databases, …) so reconciliation can
// never mutate the world: looking must stay side-effect free.
type (
	// CapacityProbe answers whether an environment holds a reservation.
	CapacityProbe interface {
		Held(ctx context.Context, envID string) (bool, error)
	}
	// DatabasesProbe answers whether a database exists.
	DatabasesProbe interface {
		Exists(ctx context.Context, name string) (bool, error)
	}
	// RolesProbe answers whether a login role exists.
	RolesProbe interface {
		Exists(ctx context.Context, name string) (bool, error)
	}
	// SecretsProbe answers what a stored payload contains (an absent path
	// returns an error wrapping ErrSecretNotFound).
	SecretsProbe interface {
		Get(ctx context.Context, envID string) (map[string]string, error)
	}
)

// Reconciler decides the outcome of an in_doubt step by inspecting real
// resources. All probe fields are required; a nil probe fails the step
// loudly rather than guessing.
type Reconciler struct {
	Capacity  CapacityProbe
	Databases DatabasesProbe
	Roles     RolesProbe
	Secrets   SecretsProbe
	// Log is optional; a discard logger is used when nil.
	Log *slog.Logger
}

// Reconcile inspects the world and returns the outcome for one step kind.
// A returned error means "could not observe", never a verdict: the caller
// leaves the step in_doubt and a later pass asks again.
func (r *Reconciler) Reconcile(ctx context.Context, step domain.Step) (ReconcileOutcome, error) {
	switch step.Kind {
	case domain.KindReserveCapacity, domain.KindReleaseCapacity:
		if r.Capacity == nil {
			return "", fmt.Errorf("reconcile %s: no capacity probe", step.ID)
		}
		held, err := r.Capacity.Held(ctx, step.EnvironmentID)
		if err != nil {
			return "", fmt.Errorf("reconcile %s (%s): %w", step.Kind, step.ID, err)
		}
		if step.Kind == domain.KindReserveCapacity {
			// Exists-and-matches is trivially true: the reservation is keyed
			// by the environment id and nothing else is observable.
			return heldOutcome(held), nil
		}
		// release_capacity: a held reservation means the drop has not happened.
		if held {
			return OutcomePending, nil
		}
		return OutcomeDone, nil

	case domain.KindCreateDatabase, domain.KindDropDatabase:
		if r.Databases == nil {
			return "", fmt.Errorf("reconcile %s: no database probe", step.ID)
		}
		exists, err := r.Databases.Exists(ctx, DatabaseName(step.EnvironmentID))
		if err != nil {
			return "", fmt.Errorf("reconcile %s (%s): %w", step.Kind, step.ID, err)
		}
		if step.Kind == domain.KindCreateDatabase {
			return heldOutcome(exists), nil
		}
		// drop_database: a still-present database means cleanup has not run.
		if exists {
			return OutcomePending, nil
		}
		return OutcomeDone, nil

	case domain.KindStoreCreds:
		if r.Secrets == nil {
			return "", fmt.Errorf("reconcile store_credentials: no secret probe (%s)", step.ID)
		}
		payload, err := r.Secrets.Get(ctx, step.EnvironmentID)
		switch {
		case err == nil:
			// Exists-and-matches: the payload must name exactly this
			// environment's database and role. Anything else is a verdict
			// to fail on — it is not the credential this environment plans.
			return payloadOutcome(payload, step.EnvironmentID)
		case errors.Is(err, ErrSecretNotFound):
			return OutcomePending, nil
		case errors.Is(err, ErrSecretMalformed):
			return OutcomeFailed, nil
		default:
			return "", fmt.Errorf("reconcile store_credentials (%s): %w", step.ID, err)
		}

	case domain.KindDropRole:
		if r.Roles == nil {
			return "", fmt.Errorf("reconcile drop_role: no role probe (%s)", step.ID)
		}
		exists, err := r.Roles.Exists(ctx, RoleName(step.EnvironmentID))
		if err != nil {
			return "", fmt.Errorf("reconcile drop_role (%s): %w", step.ID, err)
		}
		// Symmetric with drop_database: a still-present role means the drop
		// has not run (cleanup alone is redoable → pending); a role that is
		// gone proved the compensation landed (→ done).
		if exists {
			return OutcomePending, nil
		}
		return OutcomeDone, nil

	case domain.KindCreateRole, domain.KindGrantPrivs:
		if r.Roles == nil {
			return "", fmt.Errorf("reconcile %s: no role probe", step.Kind)
		}
		// The role's name is derived from the environment id, and its
		// password is never observable from outside — existence is the
		// whole observable surface, so exists-is-matching. Grant privileges
		// ride the same probe: the step is idempotent and re-runnable, and
		// an absent role means its grants provably never happened.
		exists, err := r.Roles.Exists(ctx, RoleName(step.EnvironmentID))
		if err != nil {
			return "", fmt.Errorf("reconcile %s (%s): %w", step.Kind, step.ID, err)
		}
		if step.Kind == domain.KindGrantPrivs && exists {
			// Grant surfaces exist (the role does), but a grant's effect
			// cannot be observed without connecting as the role; re-running
			// the step is safe by idempotence, so pending is chosen over a
			// guessed done.
			return OutcomePending, nil
		}
		return heldOutcome(exists), nil

	case domain.KindDeleteSecret:
		if r.Secrets == nil {
			return "", fmt.Errorf("reconcile delete_secret: no secret probe (%s)", step.ID)
		}
		_, err := r.Secrets.Get(ctx, step.EnvironmentID)
		switch {
		case errors.Is(err, ErrSecretNotFound):
			// The secret is gone: the drop has provably happened (or was
			// never needed) and the cleanup step is done.
			return OutcomeDone, nil
		case errors.Is(err, ErrSecretMalformed):
			return OutcomeFailed, nil
		case err != nil:
			return "", fmt.Errorf("reconcile delete_secret (%s): %w", step.ID, err)
		default:
			// The secret still exists: the cleanup has not run yet.
			return OutcomePending, nil
		}

	case domain.KindVerifyUsable:
		// The probe is exactly the step's own work; there is no cheaper
		// side-effect to check, and re-running verification is safe (read
		// only). An ambiguous verify step is therefore re-executed fresh.
		return OutcomePending, nil

	default:
		return "", fmt.Errorf("reconcile (%s): unknown step kind %q", step.ID, step.Kind)
	}
}

// heldOutcome maps "resource present" onto a create-concern answer:
// present-and-matching → done, absent → pending.
func heldOutcome(exists bool) ReconcileOutcome {
	if exists {
		return OutcomeDone
	}
	return OutcomePending
}

// payloadOutcome decides whether a stored secret payload matches the plan
// for its environment: every key the workflow later reads must be present.
// A stored document that cannot serve the plan is exists-but-differs — the
// one outcome that is a verdict-shaped failure rather than an ambiguity.
func payloadOutcome(payload map[string]string, environmentID string) (ReconcileOutcome, error) {
	for _, key := range []string{"database", "username", "password"} {
		if payload[key] == "" {
			return OutcomeFailed, nil
		}
	}
	if payload["database"] != DatabaseName(environmentID) ||
		payload["username"] != RoleName(environmentID) {
		return OutcomeFailed, nil
	}
	return OutcomeDone, nil
}
