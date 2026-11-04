package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// scriptedReconciler answers every Reconcile with a canned outcome, or an
// error when wantErr is set. Seens records the steps it was asked about.
type scriptedReconciler struct {
	mu      sync.Mutex
	outcome adapters.ReconcileOutcome
	err     error
	seens   []string
}

func (r *scriptedReconciler) Reconcile(_ context.Context, step domain.Step) (adapters.ReconcileOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seens = append(r.seens, step.ID)
	return r.outcome, r.err
}

// asked counts how many steps reached the reconciler.
func (r *scriptedReconciler) asked() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seens)
}

// seensFirst returns the id the reconciler was first asked about.
func (r *scriptedReconciler) seensFirst(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seens) == 0 {
		t.Fatal("the reconciler was never asked")
	}
	return r.seens[0]
}

// inDoubtStep seeds one step already carrying the ambiguity.
func inDoubtStep(id string, idx int, kind domain.StepKind) domain.Step {
	return domain.Step{ID: id, StepIndex: idx, Kind: kind, State: domain.StepInDoubt}
}

// TestInDoubtIsInvisibleToOrdinaryClaim pins the structural exclusion: an
// in_doubt step is never handed to an executor — the ordinary claim query
// returns no work for the whole store while the only step there is
// in_doubt. Reconciliation is its only way back.
func TestInDoubtIsInvisibleToOrdinaryClaim(t *testing.T) {
	st := newFakeStore(nil, inDoubtStep("doubtful", 1, domain.KindCreateDatabase))
	if _, err := st.ClaimNextStep(context.Background(), "w1", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("an in_doubt step must be unclaimable by the ordinary query, got %v", err)
	}
	// And the reconciler's claim path is the only one that touches it.
	s, err := st.ClaimNextInDoubtStep(context.Background(), "reconciler")
	if err != nil || s.ID != "doubtful" {
		t.Fatalf("reconciliation claim failed: (%s, %v)", s.ID, err)
	}
	if s.State != domain.StepInDoubt || s.LeaseOwner != "reconciler" {
		t.Fatalf("reconciliation claim must hold the ambiguous row: %+v", s)
	}
}

// TestReconcilePassResolvesEveryOutcome drives the pool's reconciliation
// pass over three seeded in_doubt steps and pins each of the three
// observable outcomes: done → StepDone, pending → StepPending, failed →
// StepFailed with the verdict recorded. Startup reconciliation (the first
// pass runs at Run) is what exercises it — no polling waits needed.
func TestReconcilePassResolvesEveryOutcome(t *testing.T) {
	cases := []struct {
		name    string
		outcome adapters.ReconcileOutcome
		want    domain.StepState
	}{
		{"exists-and-matches", adapters.OutcomeDone, domain.StepDone},
		{"absent", adapters.OutcomePending, domain.StepPending},
		{"exists-but-differs", adapters.OutcomeFailed, domain.StepFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newFakeStore(nil, inDoubtStep("step-"+tc.name, 2, domain.KindCreateDatabase))
			rec := &scriptedReconciler{outcome: tc.outcome}
			pool := &worker.Pool{
				LeaseDuration: time.Minute,
				StepTimeout:   time.Minute,
				Store:         st,
				Executor:      worker.NoopExecutor{},
				Reconciler:    rec,
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := startPool(ctx, pool)

			// The verdict is observed in the store's completion LOG, not in
			// the live row: a pending verdict is immediately reclaimable and
			// the pool's own worker can claim and re-execute the step before
			// the test polls the live state, so the live row is no reliable
			// observation of the reconcile verdict.
			got := waitRecordedOutcome(t, st, "step-"+tc.name, tc.want)
			stopPool(t, cancel, done)

			switch tc.outcome {
			case adapters.OutcomeDone, adapters.OutcomeFailed:
				if got.LeaseOwner != "" {
					t.Fatalf("resolved step must carry no lease: %+v", got)
				}
			}
			if rec.asked() != 1 || rec.seensFirst(t) != "step-"+tc.name {
				t.Fatalf("reconciler asked about the wrong steps: %v", rec.seens)
			}
			if tc.outcome == adapters.OutcomeFailed && got.LastError == "" {
				t.Fatalf("a failed reconciliation verdict must record why: %+v", got)
			}
			// The environment resumes its workflow once the ambiguity got
			// a verdict (pending and failed included — the step will be
			// re-executed by the ordinary claim).
			st.mu.Lock()
			defer st.mu.Unlock()
			var resumed bool
			for _, e := range st.envEvents {
				if e.Event == "resume" {
					resumed = true
				}
			}
			if !resumed {
				t.Fatal("a resolved in_doubt step must resume its environment")
			}
		})
	}
}

// waitRecordedOutcome polls the fake store's completion log until step id has
// a recorded outcome carrying the wanted verdict. The completion log is the
// durable record of what the store was told; the live row may have moved on
// by the time the test looks (a pending verdict is reclaimed and re-executed
// by the pool's own worker within one poll interval), so the log — append-only
// and race-free to search — is what the assertion must read.
func waitRecordedOutcome(t *testing.T, st *fakeStore, id string, want domain.StepState) domain.Step {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st.mu.Lock()
		for _, s := range st.completions {
			if s.ID == id && s.State == want {
				st.mu.Unlock()
				return s
			}
		}
		st.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	t.Fatalf("no recorded outcome ever set step %s to %s; completions: %+v", id, want, st.completions)
	return domain.Step{}
}

// TestReconcileObservationErrorKeepsLeaseAndRunsResumeSweep pins both the
// no-guess rule and pass liveness: a failed probe stays leased in_doubt until
// expiry, while the same pass still schedules retries and finalizes cleanup.
func TestReconcileObservationErrorKeepsLeaseAndRunsResumeSweep(t *testing.T) {
	failedCleanup := step("failed-cleanup", 7)
	failedCleanup.Kind = domain.KindDropDatabase
	failedCleanup.EnvironmentID = "env-cleanup"
	failedCleanup.State = domain.StepFailed
	failedCleanup.Attempts = 1
	st := newFakeStore(nil,
		inDoubtStep("unobservable", 1, domain.KindCreateDatabase),
		failedCleanup,
	)
	st.cleanupEnvIDs = []string{"env-cleanup"}
	rec := &scriptedReconciler{err: errors.New("probe unreachable")}
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      worker.NoopExecutor{},
		Reconciler:    rec,
		RetryPolicy:   worker.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Hour},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)
	defer stopPool(t, cancel, done)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st.mu.Lock()
		hasRetry := len(st.retryBackoffs) > 0
		hasFinalize := false
		for _, event := range st.envEvents {
			if event.Event == "finalize" {
				hasFinalize = true
			}
		}
		st.mu.Unlock()
		if rec.asked() == 1 && hasRetry && hasFinalize {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rec.asked() != 1 {
		t.Fatalf("a live reconciliation lease must prevent a hot loop, asked %d time(s)", rec.asked())
	}
	st.mu.Lock()
	var unresolved domain.Step
	for _, s := range st.steps {
		if s.ID == "unobservable" {
			unresolved = s
		}
	}
	events := append([]fakeEnvEvent(nil), st.envEvents...)
	hasRetry := len(st.retryBackoffs) == 1
	st.mu.Unlock()
	if unresolved.State != domain.StepInDoubt || unresolved.LeaseOwner == "" || unresolved.LeaseExpiresAt == "" {
		t.Fatalf("an unobservable row must remain in_doubt under its live lease: %+v", unresolved)
	}
	if !hasRetry {
		t.Fatal("resume sweep did not schedule the retryable cleanup step")
	}
	retryAt, finalizeAt := -1, -1
	for i, event := range events {
		if event.Event == "schedule_retry" && retryAt < 0 {
			retryAt = i
		}
		if event.Event == "finalize" && finalizeAt < 0 {
			finalizeAt = i
		}
	}
	if retryAt < 0 || finalizeAt < 0 || retryAt > finalizeAt {
		t.Fatalf("resume sweep must schedule retries before finalization, events: %+v", events)
	}
}

// TestReconciledCleanupStepFinalizesCleanup ties reconciliation into the
// cleanup finalizer: when a cleanup step's ambiguity resolves to done, the
// pool asks the store to close the deleting workflow.
func TestReconciledCleanupStepFinalizesCleanup(t *testing.T) {
	st := newFakeStore(nil, inDoubtStep("cleanup-drop-db", 7, domain.KindDropDatabase))
	rec := &scriptedReconciler{outcome: adapters.OutcomeDone}
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      worker.NoopExecutor{},
		Reconciler:    rec,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	waitForStepState(t, st, "cleanup-drop-db", domain.StepDone)
	stopPool(t, cancel, done)

	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.envEvents {
		if e.Event == "finalize" {
			return
		}
	}
	t.Fatal("a resolved cleanup step must trigger the cleanup finalizer")
}

// TestFailedStepIsRetriedUnderCap pins the bounded retry: a failing
// executor gets the step re-scheduled until the attempts cap, with the
// row's attempts enforcing the budget; past the cap the step stays failed
// for an operator instead of looping forever.
func TestFailedStepIsRetriedUnderCap(t *testing.T) {
	st := newFakeStore(nil, step("always-fails", 1))
	const maxAttempts = 3
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      errorExecutor{err: errors.New("dependency down")},
		RetryPolicy: worker.RetryPolicy{
			MaxAttempts: maxAttempts,
			BaseBackoff: 5 * time.Millisecond,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	deadline := time.Now().Add(5 * time.Second)
	var last domain.Step
	for time.Now().Before(deadline) {
		st.mu.Lock()
		last = st.steps[0]
		scheduled := len(st.retryBackoffs)
		st.mu.Unlock()
		if last.Attempts >= maxAttempts && scheduled == maxAttempts-1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopPool(t, cancel, done)

	if last.State != domain.StepFailed {
		t.Fatalf("an exhausted step must rest failed, got %s (+%+v)", last.State, last)
	}
	if last.Attempts != maxAttempts {
		t.Fatalf("attempts = %d, want exactly the capped %d", last.Attempts, maxAttempts)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if n := len(st.retryBackoffs); n != maxAttempts-1 {
		t.Fatalf("retry schedulings = %d, want %d (one fewer than attempts)", n, maxAttempts-1)
	}
	for _, d := range st.retryBackoffs {
		if d <= 0 {
			t.Fatalf("a retry must carry a positive durable backoff, got %s", d)
		}
	}
}

// TestFailedStepAtCapStaysFailedWithoutScheduling pins the cap from the
// other side: pre-seed the row at the cap and a failing execution must
// never schedule a further retry.
func TestFailedStepAtCapStaysFailedWithoutScheduling(t *testing.T) {
	s := step("capped-out", 1)
	s.Attempts = 3
	st := newFakeStore(nil, s)
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      errorExecutor{err: errors.New("dependency down")},
		RetryPolicy: worker.RetryPolicy{
			MaxAttempts: 3,
			BaseBackoff: 5 * time.Millisecond,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)
	waitForStepState(t, st, "capped-out", domain.StepFailed)
	stopPool(t, cancel, done)

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.retryBackoffs) != 0 {
		t.Fatalf("a step at the cap must never be re-scheduled, scheduled %d times", len(st.retryBackoffs))
	}
}
