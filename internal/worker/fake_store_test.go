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
		switch {
		case s.State == domain.StepPending:
			return i
		case s.State == domain.StepRunning && s.LeaseExpiresAt != "":
			exp, err := parseLease(s.LeaseExpiresAt)
			if err == nil && exp.Before(now) {
				return i
			}
		}
	}
	return -1
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
