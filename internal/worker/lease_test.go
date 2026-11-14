package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// startPool launches the pool and returns its finished channel.
func startPool(ctx context.Context, p *worker.Pool) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return done
}

// stopPool cancels the run context and requires the pool to exit cleanly
// before the test deadline.
func stopPool(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pool stop errored: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pool did not stop after context cancel")
	}
}

// waitForStepState polls the fake store until the step reaches want.
func waitForStepState(t *testing.T, st *fakeStore, id string, want domain.StepState) domain.Step {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range st.snapshot() {
			if s.ID == id && s.State == want {
				return s
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("step %s never reached %s; steps: %+v", id, want, st.snapshot())
	return domain.Step{}
}

// TestLiveLeaseIsNotClaimableExpiredLeaseIs pins the claim filter directly on
// the fake store: a step held under a live lease is invisible to every other
// claimer, the same step whose lease_expires_at is past is reclaimable, and
// pending steps are claimed in step_index order.
func TestLiveLeaseIsNotClaimableExpiredLeaseIs(t *testing.T) {
	st := newFakeStore(nil,
		runningSeed(step("step-expired", 1), "w1", -time.Minute),
		runningSeed(step("step-live", 2), "w1", time.Minute),
		step("step-pending", 3),
	)

	s1, err := st.ClaimNextStep(context.Background(), "w2", time.Minute)
	if err != nil {
		t.Fatalf("claim of an expired-lease step: %v", err)
	}
	if s1.ID != "step-expired" {
		t.Fatalf("claimed %s, want the expired-lease step first", s1.ID)
	}
	if s1.LeaseOwner != "w2" || s1.Attempts != 2 {
		t.Fatalf("claim did not advance ownership: %+v", s1)
	}

	// The live-lease step (index 2) must be skipped in favour of pending 3.
	s2, err := st.ClaimNextStep(context.Background(), "w2", time.Minute)
	if err != nil {
		t.Fatalf("claim after live-lease skip: %v", err)
	}
	if s2.ID != "step-pending" {
		t.Fatalf("claimed %s, want the pending step while a live lease is held", s2.ID)
	}

	// Only step-live remains, and it stays untouchable.
	if _, err := st.ClaimNextStep(context.Background(), "w3", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("expected ErrNoWork while a live lease exists, got %v", err)
	}
	if got := st.snapshot()[1]; got.LeaseOwner != "w1" || got.State != domain.StepRunning {
		t.Fatalf("live-lease step was touched: %+v", got)
	}

	// Once that lease expires, the same step is claimable again.
	st.mu.Lock()
	st.steps[1] = runningSeed(step("step-live", 2), "w1", -time.Minute)
	st.mu.Unlock()
	s3, err := st.ClaimNextStep(context.Background(), "w3", time.Minute)
	if err != nil || s3.ID != "step-live" {
		t.Fatalf("expired lease must be reclaimable: got (%s, %v)", s3.ID, err)
	}
}

func TestForwardStepsWaitForEarlierStepCompletion(t *testing.T) {
	first, second := step("env-step-1", 1), step("env-step-2", 2)
	first.EnvironmentID, second.EnvironmentID = "env-sequential", "env-sequential"
	st := newFakeStore(nil, first, second)

	claimed, err := st.ClaimNextStep(context.Background(), "worker-1", time.Minute)
	if err != nil || claimed.StepIndex != 1 {
		t.Fatalf("first claim = (%+v, %v), want step 1", claimed, err)
	}
	if _, err := st.ClaimNextStep(context.Background(), "worker-2", time.Minute); !errors.Is(err, store.ErrNoWork) {
		t.Fatalf("later forward work must wait for its predecessor, got %v", err)
	}
	if err := st.CompleteStep(context.Background(), claimed.ID, "worker-1", domain.StepDone, ""); err != nil {
		t.Fatalf("complete first step: %v", err)
	}
	claimed, err = st.ClaimNextStep(context.Background(), "worker-2", time.Minute)
	if err != nil || claimed.StepIndex != 2 {
		t.Fatalf("second claim = (%+v, %v), want step 2 after step 1 is done", claimed, err)
	}
}

// TestConcurrentClaimsAreExclusive races more concurrent claimers than there
// are claimable steps against the fake store's mutex (the stand-in for FOR
// UPDATE SKIP LOCKED): every successful claim hands out a distinct step —
// two live claims on one step can never happen.
func TestConcurrentClaimsAreExclusive(t *testing.T) {
	var steps []domain.Step
	for e := 0; e < 2; e++ {
		for i := 1; i <= 3; i++ {
			steps = append(steps, step(fmt.Sprintf("env%d-step%d", e, i), i))
		}
	}
	st := newFakeStore(nil, steps...)

	const claimers = 12
	type result struct {
		st  domain.Step
		err error
	}
	results := make(chan result, claimers)
	var wg sync.WaitGroup
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s, err := st.ClaimNextStep(context.Background(), fmt.Sprintf("w%d", n), time.Minute)
			results <- result{s, err}
		}(i)
	}
	wg.Wait()
	close(results)

	claimed := map[string]string{} // step id -> owner
	noWork := 0
	for r := range results {
		switch {
		case errors.Is(r.err, store.ErrNoWork):
			noWork++
		case r.err != nil:
			t.Fatalf("unexpected claim error: %v", r.err)
		default:
			if owner, dup := claimed[r.st.ID]; dup {
				t.Fatalf("two workers claimed %s: %s and %s", r.st.ID, owner, r.st.LeaseOwner)
			}
			claimed[r.st.ID] = r.st.LeaseOwner
		}
	}
	if len(claimed) != len(steps) || noWork != claimers-len(steps) {
		t.Fatalf("got %d step claims and %d ErrNoWork, want %d/%d",
			len(claimed), noWork, len(steps), claimers-len(steps))
	}
	for _, s := range st.snapshot() {
		if s.State != domain.StepRunning || s.LeaseOwner == "" {
			t.Fatalf("step %s did not end up live-claimed: %+v", s.ID, s)
		}
	}
}

// renewingExecutor sleeps longer than several lease durations while ignoring
// its context, signalling entry and exit so the thief is scoped to exactly
// the in-flight window.
type renewingExecutor struct {
	sleep   time.Duration
	entered chan struct{}
	left    chan struct{}
}

func (e renewingExecutor) Execute(context.Context, domain.Step) error {
	close(e.entered)
	time.Sleep(e.sleep)
	close(e.left)
	return nil
}

// TestRenewingStepIsNeverReclaimed pins the crash-recovery contract from the
// healthy side: while a step keeps renewing its lease it is never stolen,
// even past a full LeaseDuration. A thief claiming every 25ms across the
// execution window (≈6 leases) must never succeed, and the step must finish
// done with exactly one attempt.
func TestRenewingStepIsNeverReclaimed(t *testing.T) {
	lease := 300 * time.Millisecond
	st := newFakeStore(nil, step("renew-target", 1))
	entered, left := make(chan struct{}), make(chan struct{})
	pool := &worker.Pool{
		LeaseDuration: lease,
		StepTimeout:   5 * time.Second,
		Store:         st,
		Executor:      renewingExecutor{sleep: 1800 * time.Millisecond, entered: entered, left: left},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	<-entered // the step is claimed and being heartbeat-renewed now

	thiefCtx, thiefCancel := context.WithCancel(context.Background())
	thiefBase, thiefStop := context.WithCancel(context.Background())
	defer thiefStop()
	thiefFound := make(chan error, 1)
	go func() {
		// Drain renewals needed for repeat executor runs; a thief finding
		// the step reclaims it, so reporting stays correct.
		defer thiefCancel()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-thiefCtx.Done():
				return
			case <-ticker.C:
				_, err := st.ClaimNextStep(thiefBase, "thief", lease)
				if err == nil {
					thiefFound <- errors.New("thief reclaimed a renewing step")
					return
				}
				if !errors.Is(err, store.ErrNoWork) {
					thiefFound <- fmt.Errorf("thief claim: %w", err)
					return
				}
			}
		}
	}()

	<-left // execution window closed: ≈6 leases had elapsed
	select {
	case err := <-thiefFound:
		t.Fatalf("a renewing step was reclaimed past its lease: %v", err)
	case <-time.After(2 * time.Second):
		// The thief stayed silent for well over a lease: nothing was stolen.
	}

	s := waitForStepState(t, st, "renew-target", domain.StepDone)
	if s.Attempts != 1 {
		t.Fatalf("step attempts = %d, want exactly one claim", s.Attempts)
	}
	stopPool(t, cancel, done)
}

// TestExecutorOutcomeRecorded covers the outcome path: a nil executor return
// records done with an empty last_error and clears the lease; a failing
// executor records failed with the error string verbatim.
func TestExecutorOutcomeRecorded(t *testing.T) {
	okStore := newFakeStore(nil, step("ok-step", 1))
	badStore := newFakeStore(nil, step("bad-step", 2))
	mk := func(st *fakeStore, ex worker.Executor) *worker.Pool {
		return &worker.Pool{
			LeaseDuration: time.Minute,
			StepTimeout:   time.Minute,
			Store:         st,
			Executor:      ex,
		}
	}
	okCtx, okCancel := context.WithCancel(context.Background())
	badCtx, badCancel := context.WithCancel(context.Background())
	okDone := startPool(okCtx, mk(okStore, worker.NoopExecutor{}))
	badDone := startPool(badCtx, mk(badStore, errorExecutor{err: errors.New("adapter exploded")}))
	defer stopPool(t, okCancel, okDone)
	defer stopPool(t, badCancel, badDone)

	ok := waitForStepState(t, okStore, "ok-step", domain.StepDone)
	if ok.LastError != "" || ok.LeaseOwner != "" {
		t.Fatalf("done step must carry no error and no lease: %+v", ok)
	}
	bad := waitForStepState(t, badStore, "bad-step", domain.StepFailed)
	if bad.LastError != "adapter exploded" || bad.LeaseOwner != "" {
		t.Fatalf("failed step must store the error verbatim and clear the lease: %+v", bad)
	}
}

// TestShutdownReleasesInFlightStepToPending pins shutdown behaviour: a step
// in flight when the run context is cancelled is released back to pending
// (not left running until lease expiry), and the pool exits cleanly.
func TestShutdownReleasesInFlightStepToPending(t *testing.T) {
	st := newFakeStore(nil, step("inflight", 1))
	started := make(chan struct{})
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      blockingExecutor{started: started},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		stopPool(t, cancel, done)
		t.Fatal("executor never started")
	}

	stopPool(t, cancel, done)

	got := st.snapshot()[0]
	if got.State != domain.StepPending || got.LeaseOwner != "" || got.LeaseExpiresAt != "" {
		t.Fatalf("in-flight step must be released to pending on shutdown, got %+v", got)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.releases) != 1 || st.releases[0] != "inflight" {
		t.Fatalf("expected exactly one release of the in-flight step, got %v", st.releases)
	}
	if len(st.completions) != 0 {
		t.Fatalf("shutdown must not record a fake outcome, got %+v", st.completions)
	}
}

// TestIdlePoolPollsBoundedly pins the no-hot-loop guarantee: an idle pool
// with Concurrency workers claims at a jittered, low rate — many claims
// within the window would betray a spin loop.
func TestIdlePoolPollsBoundedly(t *testing.T) {
	st := newFakeStore(nil) // nothing ever claimable → ErrNoWork forever
	pool := &worker.Pool{
		Concurrency:   2,
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      worker.NoopExecutor{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	time.Sleep(600 * time.Millisecond)
	claims := st.missCount()
	stopPool(t, cancel, done)

	if claims < 1 {
		t.Fatal("an idle pool must still poll for work")
	}
	// Six-hundred-millisecond window, two workers, minimum poll interval
	// 200ms: more than a handful of claims would mean busy-spinning.
	if claims > 20 {
		t.Fatalf("idle pool claimed %d times in 600ms — a hot loop, not bounded polling", claims)
	}
}

// TestStepTimeoutRecordsInDoubt covers the ambiguous path: when the
// execution context times out (outcome unknown) the worker records the step
// as in_doubt — never failed, never pending — clears its lease, and marks
// the environment in_doubt so only the reconciler may resolve the step.
func TestStepTimeoutRecordsInDoubt(t *testing.T) {
	st := newFakeStore(nil, step("slow-step", 1))
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   150 * time.Millisecond,
		Store:         st,
		Executor:      slowExecutor{sleep: 600 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	time.Sleep(800 * time.Millisecond) // past the step timeout and the executor's sleep
	stopPool(t, cancel, done)

	got := st.snapshot()[0]
	if got.State != domain.StepInDoubt {
		t.Fatalf("a timed-out step must be recorded in_doubt, got %s (+%+v)", got.State, got)
	}
	if got.LeaseOwner != "" || got.LeaseExpiresAt != "" {
		t.Fatalf("an in_doubt step carries no lease, got %+v", got)
	}
	if got.LastError == "" || got.State == domain.StepFailed || got.State == domain.StepPending {
		t.Fatalf("in_doubt must not be a verdict: %+v", got)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.envEvents {
		if e.Event == "in_doubt" {
			return
		}
	}
	t.Fatal("the environment must have been marked in_doubt with its step")
}

func TestForwardTimeoutDuringCleanupDoesNotHideCleanupState(t *testing.T) {
	forward := step("slow-forward-during-delete", 2)
	forward.EnvironmentID = "env-cleanup"
	st := newFakeStore(nil, forward)
	st.cleanupEnvIDs = []string{"env-cleanup"}
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   100 * time.Millisecond,
		Store:         st,
		Executor:      slowExecutor{sleep: 300 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	got := waitForStepState(t, st, forward.ID, domain.StepInDoubt)
	if got.EnvironmentID != "env-cleanup" {
		t.Fatalf("test step lost its cleanup environment: %+v", got)
	}
	stopPool(t, cancel, done)

	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.envEvents {
		if e.EnvironmentID == "env-cleanup" && e.Event == "in_doubt" {
			t.Fatal("a forward timeout must not move a cleanup-path environment to in_doubt")
		}
	}
}

// lostRecordStore wraps the fake store to make CompleteStep fail the way a
// lost lease or a transient store error does: the in_doubt record never
// lands, so the row never becomes ambiguous.
type lostRecordStore struct {
	*fakeStore
	failComplete bool
}

func (s *lostRecordStore) CompleteStep(
	ctx context.Context, stepID, owner string, state domain.StepState, lastErr string,
) error {
	if s.failComplete {
		return fmt.Errorf("fake store: complete step %s as %s (owner %s): %w",
			stepID, state, owner, store.ErrNotFound)
	}
	return s.fakeStore.CompleteStep(ctx, stepID, owner, state, lastErr)
}

// TestLostInDoubtRecordDoesNotStrandEnvironment covers the resumability hole:
// only an in_doubt that actually got RECORDED earns an in_doubt environment.
// When the record failed (lease lost to a reclaiming worker, transient store
// error), an environment marked in_doubt with no in_doubt step would never be
// resumed — no verdict commits the resume — so the worker must not move it.
func TestLostInDoubtRecordDoesNotStrandEnvironment(t *testing.T) {
	t.Run("failed record leaves the environment alone", func(t *testing.T) {
		slow := step("slow-lost-record", 1)
		slow.EnvironmentID = "env-lost"
		st := &lostRecordStore{fakeStore: newFakeStore(nil, slow), failComplete: true}
		pool := &worker.Pool{
			LeaseDuration: time.Minute,
			StepTimeout:   100 * time.Millisecond,
			Store:         st,
			Executor:      slowExecutor{sleep: 300 * time.Millisecond},
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := startPool(ctx, pool)

		// Past the step timeout and the executor's sleep: the record
		// attempt happened, and failed.
		time.Sleep(500 * time.Millisecond)
		stopPool(t, cancel, done)

		st.mu.Lock()
		defer st.mu.Unlock()
		for _, e := range st.envEvents {
			if e.EnvironmentID == "env-lost" && e.Event == "in_doubt" {
				t.Fatalf("a failed in_doubt record must not move the environment: %+v", st.envEvents)
			}
		}
	})

	t.Run("recorded in_doubt still moves the environment", func(t *testing.T) {
		slow := step("slow-recorded-in-doubt", 1)
		slow.EnvironmentID = "env-kept"
		st := &lostRecordStore{fakeStore: newFakeStore(nil, slow)}
		pool := &worker.Pool{
			LeaseDuration: time.Minute,
			StepTimeout:   100 * time.Millisecond,
			Store:         st,
			Executor:      slowExecutor{sleep: 300 * time.Millisecond},
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := startPool(ctx, pool)

		deadline := time.Now().Add(5 * time.Second)
		found := false
		for time.Now().Before(deadline) && !found {
			st.mu.Lock()
			for _, e := range st.envEvents {
				if e.EnvironmentID == "env-kept" && e.Event == "in_doubt" {
					found = true
				}
			}
			st.mu.Unlock()
			if !found {
				time.Sleep(10 * time.Millisecond)
			}
		}
		stopPool(t, cancel, done)
		if !found {
			t.Fatal("a recorded in_doubt step must still move its environment to in_doubt")
		}
	})
}
