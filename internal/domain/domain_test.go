package domain

import (
	"strings"
	"testing"
)

// every legal environment edge from the design contract must be present here
// exactly as written in docs/design.md.
var legalEnvEdges = []struct{ from, to EnvState }{
	{EnvPending, EnvProvisioning},
	{EnvPending, EnvCancelling},
	{EnvPending, EnvDeleting},
	{EnvProvisioning, EnvVerifying},
	{EnvProvisioning, EnvInDoubt},
	{EnvProvisioning, EnvRollingBack},
	{EnvProvisioning, EnvCancelling},
	{EnvProvisioning, EnvDeleting},
	{EnvVerifying, EnvReady},
	{EnvVerifying, EnvInDoubt},
	{EnvVerifying, EnvRollingBack},
	{EnvVerifying, EnvCancelling},
	{EnvVerifying, EnvDeleting},
	{EnvReady, EnvRotating},
	{EnvReady, EnvDeleting},
	{EnvRotating, EnvReady},
	{EnvRotating, EnvInDoubt},
	{EnvRotating, EnvFailed},
	{EnvRotating, EnvDeleting},
	{EnvInDoubt, EnvProvisioning},
	{EnvInDoubt, EnvVerifying},
	{EnvInDoubt, EnvRotating},
	{EnvInDoubt, EnvReady},
	{EnvInDoubt, EnvRollingBack},
	{EnvInDoubt, EnvCancelling},
	{EnvInDoubt, EnvDeleting},
	{EnvInDoubt, EnvFailed},
	{EnvRollingBack, EnvInDoubt},
	{EnvRollingBack, EnvRolledBack},
	{EnvRollingBack, EnvFailed},
	{EnvCancelling, EnvInDoubt},
	{EnvCancelling, EnvCancelled},
	{EnvCancelling, EnvFailed},
	{EnvDeleting, EnvInDoubt},
	{EnvDeleting, EnvDeleted},
	{EnvDeleting, EnvFailed},
	{EnvFailed, EnvRollingBack},
	{EnvFailed, EnvCancelling},
	{EnvFailed, EnvDeleting},
	{EnvFailed, EnvRotating},
}

// one illegal edge per source state, including the terminal states.
var illegalEnvEdges = []struct{ from, to EnvState }{
	{EnvPending, EnvReady},
	{EnvProvisioning, EnvReady},
	{EnvVerifying, EnvProvisioning},
	{EnvReady, EnvProvisioning},
	{EnvReady, EnvFailed}, // cancellation is not permitted once ready
	{EnvRotating, EnvRollingBack},
	{EnvInDoubt, EnvPending},
	{EnvRollingBack, EnvDeleted},
	{EnvCancelling, EnvRollingBack},
	{EnvDeleting, EnvRolledBack},
	{EnvFailed, EnvRolledBack},
	{EnvRolledBack, EnvReady}, // terminal
	{EnvCancelled, EnvDeleting},
	{EnvDeleted, EnvProvisioning}, // terminal
}

func TestEnvironmentLegalTransitions(t *testing.T) {
	for _, tc := range legalEnvEdges {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			if !CanTransition(tc.from, tc.to) {
				t.Fatalf("expected legal edge %q -> %q, got rejected", tc.from, tc.to)
			}
			got, err := Transition(tc.from, tc.to)
			if err != nil {
				t.Fatalf("Transition(%q,%q) legal edge errored: %v", tc.from, tc.to, err)
			}
			if got != tc.to {
				t.Fatalf("Transition returned %q, want %q", got, tc.to)
			}
		})
	}
}

func TestEnvironmentIllegalTransitions(t *testing.T) {
	for _, tc := range illegalEnvEdges {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			if CanTransition(tc.from, tc.to) {
				t.Fatalf("expected illegal edge %q -> %q, got allowed", tc.from, tc.to)
			}
			got, err := Transition(tc.from, tc.to)
			if err == nil {
				t.Fatalf("Transition(%q,%q) illegal edge did not error", tc.from, tc.to)
			}
			if got != tc.from {
				t.Fatalf("illegal Transition returned %q, want unchanged %q", got, tc.from)
			}
			// The error must name both states.
			if !strings.Contains(err.Error(), string(tc.from)) || !strings.Contains(err.Error(), string(tc.to)) {
				t.Fatalf("transition error %q must name both %q and %q", err, tc.from, tc.to)
			}
		})
	}
}

// AllTransitionsCovered checks that every state in envTransitions also appears
// in at least one test edge, so the test table cannot silently drift from the
// contract map.
func TestAllEnvStatesCovered(t *testing.T) {
	seen := map[EnvState]bool{}
	for _, e := range legalEnvEdges {
		seen[e.from], seen[e.to] = true, true
	}
	for _, e := range illegalEnvEdges {
		seen[e.from], seen[e.to] = true, true
	}
	for s := range envTransitions {
		if !seen[s] {
			t.Errorf("env state %q appears in the contract but in no test edge", s)
		}
	}
}

func TestEnvStateValid(t *testing.T) {
	for s := range envTransitions {
		if !s.Valid() {
			t.Errorf("known state %q reported invalid", s)
		}
	}
	if EnvState("swooning").Valid() {
		t.Error("unknown state reported valid")
	}
}

// legal step edges from the design contract.
var legalStepEdges = []struct{ from, to StepState }{
	{StepPending, StepRunning},
	{StepRunning, StepDone},
	{StepRunning, StepFailed},
	{StepRunning, StepInDoubt},
	{StepInDoubt, StepDone},
	{StepInDoubt, StepPending},
	{StepInDoubt, StepFailed},
	{StepFailed, StepPending},
	{StepDone, StepCompensated},
}

// one illegal edge per step source state, including compensated (terminal).
var illegalStepEdges = []struct{ from, to StepState }{
	{StepPending, StepDone},
	{StepRunning, StepRunning},
	{StepInDoubt, StepRunning},
	{StepFailed, StepDone},
	{StepDone, StepPending},
	{StepCompensated, StepPending}, // terminal
}

func TestStepLegalTransitions(t *testing.T) {
	for _, tc := range legalStepEdges {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			if !CanStepTransition(tc.from, tc.to) {
				t.Fatalf("expected legal edge %q -> %q, got rejected", tc.from, tc.to)
			}
			got, err := StepTransition(tc.from, tc.to)
			if err != nil {
				t.Fatalf("StepTransition(%q,%q) legal edge errored: %v", tc.from, tc.to, err)
			}
			if got != tc.to {
				t.Fatalf("StepTransition returned %q, want %q", got, tc.to)
			}
		})
	}
}

func TestStepIllegalTransitions(t *testing.T) {
	for _, tc := range illegalStepEdges {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			if CanStepTransition(tc.from, tc.to) {
				t.Fatalf("expected illegal edge %q -> %q, got allowed", tc.from, tc.to)
			}
			_, err := StepTransition(tc.from, tc.to)
			if err == nil {
				t.Fatalf("StepTransition(%q,%q) illegal edge did not error", tc.from, tc.to)
			}
			if !strings.Contains(err.Error(), string(tc.from)) || !strings.Contains(err.Error(), string(tc.to)) {
				t.Fatalf("step transition error %q must name both %q and %q", err, tc.from, tc.to)
			}
		})
	}
}

func TestStepTransitionUnknownStates(t *testing.T) {
	if _, err := StepTransition(StepState("musing"), StepRunning); err == nil {
		t.Error("expected error for unknown source state")
	}
	if _, err := StepTransition(StepRunning, "idling"); err == nil {
		t.Error("expected error for unknown target state")
	}
}

func TestPlan(t *testing.T) {
	plan := Plan()
	want := []StepKind{
		KindReserveCapacity,
		KindCreateDatabase,
		KindStoreCreds,
		KindCreateRole,
		KindGrantPrivs,
		KindVerifyUsable,
	}
	if len(plan) != len(want) {
		t.Fatalf("plan length %d, want %d", len(plan), len(want))
	}
	for i, p := range plan {
		if p.Index != i+1 {
			t.Errorf("plan[%d].Index = %d, want 1-based %d", i, p.Index, i+1)
		}
		if p.Kind != want[i] {
			t.Errorf("plan[%d].Kind = %q, want %q", i, p.Kind, want[i])
		}
	}
	first := Plan()
	first[0] = PlannedStep{Index: 0, Kind: "mutated"}
	if again := Plan(); again[0].Kind != KindReserveCapacity {
		t.Fatal("Plan does not return a fresh slice")
	}
}

func TestCleanupPlanOrder(t *testing.T) {
	want := []StepKind{KindDropDatabase, KindDropRole, KindDeleteSecret, KindReleaseCapacity}
	for i, k := range CleanupPlan {
		if k != want[i] {
			t.Errorf("CleanupPlan[%d] = %q, want %q", i, k, want[i])
		}
	}
}

func TestOperationKey(t *testing.T) {
	got := OperationKey("0e5a2b8c-0000-4000-8000-000000000001", 3, KindCreateDatabase)
	want := "env/0e5a2b8c-0000-4000-8000-000000000001/step/3/create_database"
	if got != want {
		t.Fatalf("OperationKey = %q, want %q", got, want)
	}
}

// readyOnlyThroughVerification pins the phase-6 invariant: the only ordinary
// edge into `ready` is verifying -> ready (and rotating -> ready, when a
// rotation completes). Any other direct transition must be rejected, so
// `ready` can never be claimed except through the state machinery of the
// verification (or rotation) step. The one other sanctioned edge,
// in_doubt -> ready, stays reserved for the ambiguity reconciliation in
// phase 7 (see docs/design.md); it is not reachable by the phase-6 code
// paths, all of which door through verification.
func TestReadyOnlyThroughVerification(t *testing.T) {
	for _, s := range []EnvState{EnvPending, EnvProvisioning, EnvRollingBack,
		EnvCancelling, EnvFailed, EnvCancelled, EnvDeleted, EnvRolledBack} {
		if s.CanTransitionTo(EnvReady) {
			t.Errorf("%q must not be able to transition directly to ready", s)
		}
	}
	// The two sanctioned producers of ready.
	if !EnvVerifying.CanTransitionTo(EnvReady) {
		t.Error("verifying -> ready must be legal: it is how provable readiness is recorded")
	}
	if !EnvRotating.CanTransitionTo(EnvReady) {
		t.Error("rotating -> ready must be legal: it is how a completed rotation is recorded")
	}
	// A transition helper must refuse an illegal move and write nothing.
	for _, s := range []EnvState{EnvPending, EnvProvisioning, EnvCancelled} {
		if _, err := Transition(s, EnvReady); err == nil {
			t.Errorf("Transition(%q, ready) succeeded; the state machine must reject it", s)
		}
	}
}
