package worker_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// fakeStore is an in-memory LeaseStore that mirrors the SQL semantics the
// phase pins: claimable means pending or running-with-expired-lease; claims
// are mutex-atomic (the fake stand-in for FOR UPDATE SKIP LOCKED); RenewLease
// and CompleteStep carry the owner guard and map a violation to
// store.ErrNotFound.
type fakeStore struct {
	mu    sync.Mutex
	steps []domain.Step
	clock func() time.Time

	claimErr      error // injected transient claim failure
	claims        []fakeClaim
	renewals      []fakeRenewal
	completions   []domain.Step
	releases      []string
	errNoWorkSeen int
	// inDoubtClaims records reconciliation claims; retrySchedules and
	// envEvents record the phase-7 bookkeeping calls.
	inDoubtClaims []string
	retryBackoffs []time.Duration
	// cleanupEnvIDs seeds ListCleanupEnvironments for sweep tests.
	cleanupEnvIDs []string
	// envEvents records MarkEnvironmentInDoubt / FinalizeCleanup call
	// names per environment, call order preserved.
	envEvents []fakeEnvEvent
}

type fakeEnvEvent struct {
	EnvironmentID string
	Event         string
	StepIndex     int
}

// leaseLayout keeps millisecond precision on the wire: the time.RFC3339
// layout truncates to whole seconds, which would make a sub-second lease
// look expired almost immediately.
const leaseLayout = "2006-01-02T15:04:05.000Z07:00"

func formatLease(t time.Time) string { return t.Format(leaseLayout) }

func parseLease(s string) (time.Time, error) { return time.Parse(leaseLayout, s) }

type fakeClaim struct {
	Owner  string
	StepID string
}

type fakeRenewal struct {
	Owner  string
	StepID string
}

func newFakeStore(now func() time.Time, steps ...domain.Step) *fakeStore {
	if now == nil {
		now = time.Now
	}
	return &fakeStore{steps: steps, clock: now}
}

// claimableID finds the oldest claimable step, mirroring the SQL WHERE and
// ORDER BY … LIMIT 1.
func (f *fakeStore) claimableID() int {
	now := f.clock().UTC()
	for i, s := range f.steps {
		claimable := false
		switch {
		case s.State == domain.StepPending:
			claimable = true
		case s.State == domain.StepRunning && s.LeaseExpiresAt != "":
			exp, err := parseLease(s.LeaseExpiresAt)
			if err == nil && exp.Before(now) {
				claimable = true
			}
		}
		if !claimable {
			continue
		}
		// Forward rows for one environment are strictly sequential. A later
		// row cannot become work while any lower-indexed forward row is not
		// done, matching the Postgres claim predicate.
		if s.EnvironmentID != "" && !isCleanupStep(s.Kind) {
			blocked := false
			for _, prev := range f.steps {
				if prev.EnvironmentID == s.EnvironmentID && prev.StepIndex < s.StepIndex &&
					!isCleanupStep(prev.Kind) && prev.State != domain.StepDone {
					blocked = true
					break
				}
			}
			if blocked {
				continue
			}
		}
		return i
	}
	return -1
}

func isCleanupStep(kind domain.StepKind) bool {
	for _, cleanup := range domain.CleanupPlan {
		if cleanup == kind {
			return true
		}
	}
	return false
}

// ClaimNextStep claims the oldest claimable step exactly as the SQL does.
func (f *fakeStore) ClaimNextStep(_ context.Context, owner string, lease time.Duration) (domain.Step, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		err := f.claimErr
		f.claimErr = nil
		return domain.Step{}, fmt.Errorf("injected: %w", err)
	}
	idx := f.claimableID()
	if idx < 0 {
		f.errNoWorkSeen++
		return domain.Step{}, fmt.Errorf("fake store: %w", store.ErrNoWork)
	}
	s := f.steps[idx]
	s.Reclaimed = s.State == domain.StepRunning // pre-claim running ⇒ expired lease
	s.State = domain.StepRunning
	s.LeaseOwner = owner
	s.Attempts++
	s.LeaseExpiresAt = formatLease(f.clock().UTC().Add(lease))
	f.steps[idx] = s
	f.claims = append(f.claims, fakeClaim{Owner: owner, StepID: s.ID})
	return s, nil
}

// RenewLease extends the live lease; a step not running under owner is a
// lost lease, reported as wrapping store.ErrNotFound.
func (f *fakeStore) RenewLease(_ context.Context, stepID, owner string, lease time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.ID != stepID {
			continue
		}
		if s.State != domain.StepRunning || s.LeaseOwner != owner {
			return fmt.Errorf("fake store: renew lease %s for %s: %w", stepID, owner, store.ErrNotFound)
		}
		s.LeaseExpiresAt = formatLease(f.clock().UTC().Add(lease))
		f.steps[i] = s
		f.renewals = append(f.renewals, fakeRenewal{Owner: owner, StepID: stepID})
		return nil
	}
	return fmt.Errorf("fake store: renew lease %s: %w", stepID, store.ErrNotFound)
}

// CompleteStep records the outcome under the owner guard.
func (f *fakeStore) CompleteStep(_ context.Context, stepID, owner string, state domain.StepState, lastErr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.ID != stepID {
			continue
		}
		if s.State != domain.StepRunning || s.LeaseOwner != owner {
			return fmt.Errorf("fake store: complete step %s for %s: %w", stepID, owner, store.ErrNotFound)
		}
		s.State = state
		s.LastError = lastErr
		s.LeaseOwner = ""
		s.LeaseExpiresAt = ""
		f.steps[i] = s
		f.completions = append(f.completions, s)
		return nil
	}
	return fmt.Errorf("fake store: complete step %s: %w", stepID, store.ErrNotFound)
}

// CompleteVerification marks the verification step done under its lease.
// Tests that exercise the ready gate use the env-aware fake; this fake
// simply records the completion like CompleteStep does for a done step.
func (f *fakeStore) CompleteVerification(ctx context.Context, stepID, owner, _ string) error {
	return f.CompleteStep(ctx, stepID, owner, domain.StepDone, "")
}

// ClaimNextInDoubtStep claims the oldest in_doubt step for owner by taking
// the reconciliation lease WITHOUT changing the state — the row stays
// in_doubt until an outcome is recorded, exactly as the SQL does. A claim
// whose lease expired is reclaimable; a live claim is not. The ordinary
// claimable query never returns an in_doubt row, which the fake mirrors
// structurally.
func (f *fakeStore) ClaimNextInDoubtStep(_ context.Context, owner string) (domain.Step, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.State != domain.StepInDoubt {
			continue
		}
		if s.LeaseOwner != "" && s.LeaseExpiresAt != "" {
			exp, err := parseLease(s.LeaseExpiresAt)
			if err != nil || !exp.Before(f.clock().UTC()) {
				continue // held by a live (or unreadable) reconciliation claim
			}
		}
		s.LeaseOwner = owner
		s.LeaseExpiresAt = formatLease(f.clock().UTC().Add(inDoubtLease))
		f.steps[i] = s
		f.inDoubtClaims = append(f.inDoubtClaims, s.ID)
		return s, nil
	}
	f.errNoWorkSeen++
	return domain.Step{}, fmt.Errorf("fake store: %w", store.ErrNoWork)
}

// RecordReconcileOutcome records the reconciler's verdict on a held
// ambiguous row — the state is already recorded as `in_doubt`, so the owner
// guard is state in (in_doubt) AND lease_owner = owner, mirroring the SQL.
// For any verdict other than in_doubt the fake also books the environment
// resume, because the real store's verdict transaction carries it: there is
// no separate ResumeFromInDoubt call the worker could crash between.
func (f *fakeStore) RecordReconcileOutcome(_ context.Context, stepID, owner string, state domain.StepState, lastErr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.ID != stepID {
			continue
		}
		if s.State != domain.StepInDoubt || s.LeaseOwner != owner {
			return fmt.Errorf("fake store: record reconcile outcome %s for %s: %w", stepID, owner, store.ErrNotFound)
		}
		s.State = state
		s.LastError = lastErr
		if state != domain.StepInDoubt {
			s.LeaseOwner = ""
			s.LeaseExpiresAt = ""
			f.envEvents = append(f.envEvents, fakeEnvEvent{s.EnvironmentID, "resume", s.StepIndex})
		}
		f.steps[i] = s
		f.completions = append(f.completions, s)
		return nil
	}
	return fmt.Errorf("fake store: record reconcile outcome %s: %w", stepID, store.ErrNotFound)
}

// ReapExpiredReconciliations clears the leases of abandoned reconciliation
// claims, returning rows claimable by the next pass; the rows stay in_doubt.
func (f *fakeStore) ReapExpiredReconciliations(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	now := f.clock().UTC()
	for i, s := range f.steps {
		if s.State != domain.StepInDoubt || s.LeaseOwner == "" || s.LeaseExpiresAt == "" {
			continue
		}
		if exp, err := parseLease(s.LeaseExpiresAt); err == nil && exp.Before(now) {
			s.LeaseOwner = ""
			s.LeaseExpiresAt = ""
			f.steps[i] = s
			n++
		}
	}
	return n, nil
}

// ListCleanupEnvironments is backed by an optional environment-state map the
// tests seed; unsynced (polling tests not exercising the sweep) it finds
// nothing.
func (f *fakeStore) ListCleanupEnvironments(_ context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanupEnvIDs, nil
}

// ListFailedSteps returns the failed rows the sweep may re-schedule.
func (f *fakeStore) ListFailedSteps(context.Context) ([]domain.Step, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.Step
	for _, s := range f.steps {
		if s.State == domain.StepFailed && s.LeaseOwner == "" {
			out = append(out, s)
		}
	}
	return out, nil
}

// inDoubtLease mirrors the store's reconciliation lease.
const inDoubtLease = 60 * time.Second

// ScheduleRetry returns a failed step to pending with its backoff lease
// recorded; the fake keeps the delay only for assertions — its claim path
// is deliberately simple and re-claims pending rows immediately.
func (f *fakeStore) ScheduleRetry(_ context.Context, stepID string, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.ID != stepID {
			continue
		}
		if s.State != domain.StepFailed {
			return fmt.Errorf("fake store: schedule retry %s: state %s, want failed: %w",
				stepID, s.State, store.ErrNotFound)
		}
		s.State = domain.StepPending
		s.LeaseOwner = ""
		s.LeaseExpiresAt = ""
		f.steps[i] = s
		f.retryBackoffs = append(f.retryBackoffs, delay)
		f.envEvents = append(f.envEvents, fakeEnvEvent{s.EnvironmentID, "schedule_retry", 0})
		return nil
	}
	return fmt.Errorf("fake store: schedule retry %s: %w", stepID, store.ErrNotFound)
}

// MarkEnvironmentInDoubt and FinalizeCleanup are booked as events: the fake
// as events: the fake has no environment table, so tests assert on the
// calls the worker made.
func (f *fakeStore) MarkEnvironmentInDoubt(_ context.Context, environmentID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cleanupID := range f.cleanupEnvIDs {
		if cleanupID == environmentID {
			return nil // cleanup state owns this environment; do not hide it
		}
	}
	f.envEvents = append(f.envEvents, fakeEnvEvent{environmentID, "in_doubt", 0})
	return nil
}

func (f *fakeStore) FinalizeCleanup(_ context.Context, environmentID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envEvents = append(f.envEvents, fakeEnvEvent{environmentID, "finalize", 0})
	return nil
}

// ReleaseStep returns a held step to pending; not holding it is a no-op.
func (f *fakeStore) ReleaseStep(_ context.Context, stepID, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.steps {
		if s.ID != stepID {
			continue
		}
		if s.State == domain.StepRunning && s.LeaseOwner == owner {
			s.State = domain.StepPending
			s.LeaseOwner = ""
			s.LeaseExpiresAt = ""
			f.steps[i] = s
		}
		f.releases = append(f.releases, stepID)
		return nil
	}
	return fmt.Errorf("fake store: release step %s: %w", stepID, store.ErrNotFound)
}

// snapshot returns a copy of the steps for assertions.
func (f *fakeStore) snapshot() []domain.Step {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.Step, len(f.steps))
	copy(out, f.steps)
	return out
}

// missCount counts ErrNoWork claim attempts — the polling traffic of an idle
// pool.
func (f *fakeStore) missCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errNoWorkSeen
}

// seed steps helper: step IDs are simple strings in the fake; indexes control
// claim ordering.
func step(id string, idx int) domain.Step {
	return domain.Step{ID: id, StepIndex: idx, State: domain.StepPending}
}

// runningSeed marks a step running under owner with expiry offset applied to
// the fake's clock, expressing a live (positive) or expired (negative) lease.
func runningSeed(s domain.Step, owner string, expiryFromNow time.Duration) domain.Step {
	s.State = domain.StepRunning
	s.LeaseOwner = owner
	s.Attempts = 1
	s.LeaseExpiresAt = formatLease(time.Now().UTC().Add(expiryFromNow))
	return s
}

// errorExecutor fails every execution with a fixed error.
type errorExecutor struct{ err error }

func (e errorExecutor) Execute(context.Context, domain.Step) error { return e.err }

// blockingExecutor blocks until its execution context is cancelled, ignoring
// it otherwise — it never returns a verdict of its own.
type blockingExecutor struct {
	started chan<- struct{}
}

func (b blockingExecutor) Execute(ctx context.Context, _ domain.Step) error {
	b.started <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

// slowExecutor ignores the context and sleeps a fixed wall-clock duration.
type slowExecutor struct {
	sleep time.Duration
}

func (s slowExecutor) Execute(context.Context, domain.Step) error {
	time.Sleep(s.sleep)
	return nil
}
