// Package domain holds the environment and step state machines. These types
// are pure Go with no I/O: they encode the legal state transitions from
// docs/design.md so persistence layers and workers cannot invent illegal moves.
package domain

import (
	"fmt"
)

// EnvState is the state of an environment. Values are persisted strings, part
// of the API contract in docs/design.md.
type EnvState string

// All environment states from the design contract.
const (
	EnvPending      EnvState = "pending"
	EnvProvisioning EnvState = "provisioning"
	EnvVerifying    EnvState = "verifying"
	EnvReady        EnvState = "ready"
	EnvRotating     EnvState = "rotating"
	EnvRollingBack  EnvState = "rolling_back"
	EnvRolledBack   EnvState = "rolled_back"
	EnvCancelling   EnvState = "cancelling"
	EnvCancelled    EnvState = "cancelled"
	EnvDeleting     EnvState = "deleting"
	EnvDeleted      EnvState = "deleted"
	EnvFailed       EnvState = "failed"
	EnvInDoubt      EnvState = "in_doubt"
)

// envTransitions is the complete legal environment transition set from
// docs/design.md. Terminal states have no entry at all.
var envTransitions = map[EnvState][]EnvState{EnvPending: {EnvProvisioning, EnvCancelling, EnvDeleting},
	EnvProvisioning: {EnvVerifying, EnvInDoubt, EnvRollingBack, EnvCancelling, EnvDeleting},
	EnvVerifying:    {EnvReady, EnvInDoubt, EnvRollingBack, EnvCancelling, EnvDeleting},
	EnvReady:        {EnvRotating, EnvDeleting},
	EnvRotating:     {EnvReady, EnvInDoubt, EnvFailed, EnvDeleting},
	EnvInDoubt:      {EnvProvisioning, EnvVerifying, EnvRotating, EnvReady, EnvRollingBack, EnvCancelling, EnvDeleting, EnvFailed},
	EnvRollingBack:  {EnvInDoubt, EnvRolledBack, EnvFailed},
	EnvCancelling:   {EnvInDoubt, EnvCancelled, EnvFailed},
	EnvDeleting:     {EnvInDoubt, EnvDeleted, EnvFailed},
	EnvFailed:       {EnvRollingBack, EnvCancelling, EnvDeleting, EnvRotating},
	// rolled_back, cancelled, deleted are terminal.
}

// envTerminalStates have no outgoing transitions but are valid states.
var envTerminalStates = []EnvState{EnvRolledBack, EnvCancelled, EnvDeleted}

// allEnvStates is every valid persisted environment state, terminal ones included.
var allEnvStates = func() map[EnvState]bool {
	states := make(map[EnvState]bool, len(envTransitions)+len(envTerminalStates))
	for s := range envTransitions {
		states[s] = true
	}
	for _, s := range envTerminalStates {
		states[s] = true
	}
	return states
}()

// CanTransitionTo reports whether moving from `s` to `to` is legal.
func (s EnvState) CanTransitionTo(to EnvState) bool {
	for _, next := range envTransitions[s] {
		if next == to {
			return true
		}
	}
	return false
}

// Valid reports whether s is a known environment state, terminal included.
func (s EnvState) Valid() bool { return allEnvStates[s] }

// StepState is the state of a single workflow step. Values are persisted
// strings from docs/design.md.
type StepState string

// All step states from the design contract.
const (
	StepPending     StepState = "pending"
	StepRunning     StepState = "running"
	StepInDoubt     StepState = "in_doubt"
	StepDone        StepState = "done"
	StepFailed      StepState = "failed"
	StepCompensated StepState = "compensated"
)

// stepTransitions is the complete legal step transition set. `failed ->
// pending` is legal only for a transient failure with retry budget remaining;
// callers in later phases enforce the budget, the transition itself stays legal.
// `done -> compensated` is legal only for forward rows; cleanup rows' `done`
// is terminal — callers must not compensate cleanup rows.
var stepTransitions = map[StepState][]StepState{
	StepPending: {StepRunning},
	StepRunning: {StepDone, StepFailed, StepInDoubt},
	StepInDoubt: {StepDone, StepPending, StepFailed},
	StepFailed:  {StepPending},
	StepDone:    {StepCompensated},
	// compensated is terminal.
}

// stepTerminalStates have no outgoing transitions but are valid states.
var stepTerminalStates = []StepState{StepCompensated}

// allStepStates is every valid persisted step state, terminal included.
var allStepStates = func() map[StepState]bool {
	states := make(map[StepState]bool, len(stepTransitions)+len(stepTerminalStates))
	for s := range stepTransitions {
		states[s] = true
	}
	for _, s := range stepTerminalStates {
		states[s] = true
	}
	return states
}()

// CanTransitionTo reports whether moving from `s` to `to` is legal.
func (s StepState) CanTransitionTo(to StepState) bool {
	for _, next := range stepTransitions[s] {
		if next == to {
			return true
		}
	}
	return false
}

// Valid reports whether s is a known step state, terminal included.
func (s StepState) Valid() bool { return allStepStates[s] }

// Transition moves an environment from `from` to `to`. It returns the new
// state unchanged when the move is legal, and an error naming both states
// when it is not; the caller then writes nothing and fires no side effect.
func Transition(from, to EnvState) (EnvState, error) {
	if !from.Valid() {
		return from, fmt.Errorf("environment transition: unknown source state %q", from)
	}
	if !to.Valid() {
		return from, fmt.Errorf("environment transition: unknown target state %q (from %q)", to, from)
	}
	if !from.CanTransitionTo(to) {
		return from, fmt.Errorf("illegal environment transition %q -> %q", from, to)
	}
	return to, nil
}

// StepTransition moves a step from `from` to `to`. It returns an error naming
// both states when the move is illegal.
func StepTransition(from, to StepState) (StepState, error) {
	if !from.Valid() {
		return from, fmt.Errorf("step transition: unknown source state %q", from)
	}
	if !to.Valid() {
		return from, fmt.Errorf("step transition: unknown target state %q (from %q)", to, from)
	}
	if !from.CanTransitionTo(to) {
		return from, fmt.Errorf("illegal step transition %q -> %q", from, to)
	}
	return to, nil
}

// CanTransition reports whether the environment edge from -> to is legal.
func CanTransition(from, to EnvState) bool { return from.CanTransitionTo(to) }

// CanStepTransition reports whether the step edge from -> to is legal.
func CanStepTransition(from, to StepState) bool { return from.CanTransitionTo(to) }

// StepKind names one unit of external work. Kinds are persisted strings from
// docs/design.md.
type StepKind string

// Forward plan step kinds, in workflow order 1..6.
const (
	KindReserveCapacity StepKind = "reserve_capacity"
	KindCreateDatabase  StepKind = "create_database"
	//nolint:gosec // G101 false positive: "store_credentials" is a workflow step kind, not a credential
	KindStoreCreds   StepKind = "store_credentials"
	KindCreateRole   StepKind = "create_role"
	KindGrantPrivs   StepKind = "grant_privileges"
	KindVerifyUsable StepKind = "verify_usable"
)

// Cleanup step kinds, in durable cleanup order (not the reverse of creation).
const (
	KindDropDatabase    StepKind = "drop_database"
	KindDropRole        StepKind = "drop_role"
	KindDeleteSecret    StepKind = "delete_secret"
	KindReleaseCapacity StepKind = "release_capacity"
)

// forwardPlan is the ordered forward workflow for a new environment.
var forwardPlan = []StepKind{
	KindReserveCapacity,
	KindCreateDatabase,
	KindStoreCreds,
	KindCreateRole,
	KindGrantPrivs,
	KindVerifyUsable,
}

// CleanupPlan is the durable cleanup order from docs/design.md: the database
// is dropped first so the role becomes droppable, the secret is retained until
// every consumer of it is gone, capacity is released last.
var CleanupPlan = []StepKind{
	KindDropDatabase,
	KindDropRole,
	KindDeleteSecret,
	KindReleaseCapacity,
}

// Plan returns the ordered 1-based forward step plan (index, kind) for a new
// environment. It returns a fresh slice; callers own it.
func Plan() []PlannedStep {
	plan := make([]PlannedStep, len(forwardPlan))
	for i, kind := range forwardPlan {
		plan[i] = PlannedStep{Index: i + 1, Kind: kind}
	}
	return plan
}

// PlannedStep is one entry of a plan: its immutable 1-based step_index and kind.
type PlannedStep struct {
	Index int
	Kind  StepKind
}
