// Package worker implements the lease-based step-claiming pool. There is no
// central scheduler: every worker atomically claims the next claimable step
// (pending, or running with an expired lease) from the store, executes it
// under a step timeout while heartbeating its lease, and records the outcome.
// A worker that dies loses its claim only when its lease expires — the lease
// is the crash recovery mechanism.
package worker

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// Store is the coordination surface the pool needs. *store.PostgresStore
// satisfies it; tests use a fake.
type Store interface {
	ClaimNextStep(ctx context.Context, owner string, lease time.Duration) (domain.Step, error)
	RenewLease(ctx context.Context, stepID, owner string, lease time.Duration) error
	CompleteStep(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error
	// CompleteVerification marks the verification step done and flips the
	// environment to ready in one transaction. Because `ready` exists only
	// here, a verification that never succeeds can never lend its readiness.
	CompleteVerification(ctx context.Context, stepID, owner, environmentID string) error
	ReleaseStep(ctx context.Context, stepID, owner string) error
	// Reconcile and retry bookkeeping (phase 7): the ambiguous-outcome
	// claim path (the row keeps its in_doubt state under the reconciliation
	// lease), the reconciler's record path, the reaper for abandoned
	// claims, the resume sweep's worklists, the durable rescheduling of
	// failed steps, the environment's in_doubt hops, and the cleanup
	// finalizer. See store.ReconcileStore.
	ClaimNextInDoubtStep(ctx context.Context, owner string) (domain.Step, error)
	RecordReconcileOutcome(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error
	ReapExpiredReconciliations(ctx context.Context) (int64, error)
	ListCleanupEnvironments(ctx context.Context) ([]string, error)
	ListFailedSteps(ctx context.Context) ([]domain.Step, error)
	ScheduleRetry(ctx context.Context, stepID string, delay time.Duration) error
	MarkEnvironmentInDoubt(ctx context.Context, environmentID string) error
	FinalizeCleanup(ctx context.Context, environmentID string) error
}

// Reconciler decides the outcome of an in_doubt step by looking at the real
// world (internal/adapters.Reconciler). Reconciliation may never record a
// guess: an error from Reconcile means "unobservable" and the step stays
// in_doubt for a later pass.
type Reconciler interface {
	Reconcile(ctx context.Context, step domain.Step) (adapters.ReconcileOutcome, error)
}

// Executor performs one step's external work. Phase 5 swaps the real
// adapters in; this phase ships a no-op executor.
type Executor interface {
	// Execute runs the step's work. The ctx it receives is bounded by the
	// pool's run context and StepTimeout: when either fires, the executor
	// should return promptly and the outcome is treated as unknown — the
	// step is left to expire and be reclaimed.
	Execute(ctx context.Context, step domain.Step) error
}

// NoopExecutor is the phase-4 executor: it succeeds immediately, so the
// pool's coordination behaviour is fully exercisable without adapters.
type NoopExecutor struct{}

// Execute succeeds without side effects.
func (NoopExecutor) Execute(context.Context, domain.Step) error { return nil }

// Poll/backoff tuning for the idle path: an idle pool must do bounded
// polling, not a hot loop. The interval grows with consecutive misses and
// every sleep carries jitter so a fleet does not synchronize.
const (
	defaultPollInterval = 200 * time.Millisecond
	maxPollInterval     = 2 * time.Second
	maxPollJitter       = 0.5 // up to +50% of the current interval
)

// Pool runs Concurrency claim→execute→record goroutines until the run
// context is cancelled. All claims of one Run share one owner identity,
// generated per run — a restart is a new worker.
type Pool struct {
	// Concurrency is the number of worker goroutines. Zero means 1.
	Concurrency int
	// LeaseDuration is how long a claimed step stays owned before another
	// worker may reclaim it; the heartbeat renews it every LeaseDuration/3.
	LeaseDuration time.Duration
	// StepTimeout bounds a single execution. It must stay comfortably below
	// LeaseDuration so a healthy step is never stolen mid-flight.
	StepTimeout time.Duration
	// Store coordinates claims and leases.
	Store Store
	// Executor performs the work of a claimed step. nil means NoopExecutor.
	Executor Executor
	// Reconciler resolves in_doubt steps by inspecting the real world
	// (internal/adapters.Reconciler). nil disables the reconciliation pass
	// entirely; no other component claims an in_doubt step.
	Reconciler Reconciler
	// ReconcileInterval spaces the reconciliation passes over the in_doubt
	// queue. The first pass runs at startup (startup reconciliation), later
	// ones periodically. Zero means ReconcileIntervalDefault.
	ReconcileInterval time.Duration
	// RetryPolicy bounds how a failed step may be retried: exponential
	// backoff with jitter, capped attempts. Zero MaxAttempts disables
	// scheduling (the failure is recorded and stays failed).
	RetryPolicy RetryPolicy
	// Tele publishes the phase-8 metrics for the pool's events: attempts,
	// lease expirations, reconciliation verdicts, in_doubt counts, and the
	// provisioning/cleanup duration series. nil disables all of them.
	Tele *telemetry.Metrics
	// StepPause holds a claimed step inside its running window before the
	// executor runs, for the step kinds listed in StepPauseKinds (empty
	// means every kind). Zero disables it. It exists only so the
	// failure-recovery demo can intercept a running step deterministically
	// instead of racing the millisecond-scale workflow; real deployments
	// leave it zero. The heartbeat keeps the lease alive throughout, so the
	// paused step is exactly the recoverable artifact the demo kills.
	StepPause time.Duration
	// StepPauseKinds limits StepPause to these step kinds.
	StepPauseKinds []string
	// Log is optional; a discard logger is used when nil.
	Log *slog.Logger
}

// Compile-time guards for the interfaces the pool consumes.
var _ Reconciler = ReconcilerFunc(nil)

// RetryPolicy is the bounded-retry configuration for a failed step. The
// counter that enforces the cap is the step's persisted attempts column: a
// crash inherits the same budget, never a fresh one.
type RetryPolicy struct {
	// MaxAttempts is the total number of executions a step may receive. A
	// step that reached it on failure stays failed for an operator.
	MaxAttempts int
	// BaseBackoff is the delay after the first failure; it grows
	// exponentially (BaseBackoff << attempts) and is capped at MaxBackoff.
	BaseBackoff time.Duration
	// MaxBackoff caps the backoff; zero means RetryMaxBackoff.
	MaxBackoff time.Duration
}

// Pool defaults, only meaningful when the corresponding field is zero.
const (
	// ReconcileIntervalDefault spaces the periodic reconciliation passes.
	ReconcileIntervalDefault = 30 * time.Second
	// RetryMaxBackoff caps one retry delay.
	RetryMaxBackoff = 30 * time.Second
	// retryBackoffJitter spreads concurrent retries so a fleet does not
	// thunder on one dependency at the same instant.
	retryBackoffJitter = 0.25
)

// getInterval resolves the reconciliation interval.
func (p *Pool) getInterval() time.Duration {
	if p.ReconcileInterval > 0 {
		return p.ReconcileInterval
	}
	return ReconcileIntervalDefault
}

// getMaxAttempts resolves the retry cap; zero or less disables scheduling.
func (p *Pool) getMaxAttempts() int { return p.RetryPolicy.MaxAttempts }

// pauseFor resolves the pre-execution pause for one step kind. An empty
// kind list pauses every kind.
func (p *Pool) pauseFor(kind domain.StepKind) time.Duration {
	if p.StepPause <= 0 {
		return 0
	}
	if len(p.StepPauseKinds) == 0 {
		return p.StepPause
	}
	for _, k := range p.StepPauseKinds {
		if k == string(kind) {
			return p.StepPause
		}
	}
	return 0
}

// getBackoffs resolves the base and capped-maximum backoff.
func (p *Pool) getBackoffs() (base, max time.Duration) {
	base = p.RetryPolicy.BaseBackoff
	if base <= 0 {
		base = time.Second
	}
	max = p.RetryPolicy.MaxBackoff
	if max <= 0 {
		max = RetryMaxBackoff
	}
	if max < base {
		max = base
	}
	return base, max
}

// ReconcilerFunc adapts a function to the Reconciler interface.
type ReconcilerFunc func(context.Context, domain.Step) (adapters.ReconcileOutcome, error)

// Reconcile calls f.
func (f ReconcilerFunc) Reconcile(ctx context.Context, step domain.Step) (adapters.ReconcileOutcome, error) {
	return f(ctx, step)
}

// compile-time guard that the real store satisfies the worker's interfaces.
var (
	_ Store   = (*store.PostgresStore)(nil)
	_ EnvGate = (*store.PostgresStore)(nil)
)

// newOwnerID produces a fresh worker identity, used as lease_owner.
func newOwnerID() string {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// crypto/rand does not fail on the platforms we run on; a
		// timestamp fallback still yields a distinguishable id.
		return fmt.Sprintf("worker-t%d", time.Now().UnixNano())
	}
	return "worker-" + hex.EncodeToString(b[:])
}

// logger substitutes a discard handler so every log call is nil-safe.
func (p *Pool) logger() *slog.Logger {
	if p.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return p.Log
}

// Run starts the pool and blocks until ctx is cancelled. On any exit path
// in-flight steps are released back to pending, so a graceful shutdown does
// not leave a step running until its lease expires.
func (p *Pool) Run(ctx context.Context) error {
	if p.Store == nil {
		return fmt.Errorf("worker: pool has no store")
	}
	if p.LeaseDuration <= 0 {
		return fmt.Errorf("worker: lease duration must be positive")
	}
	if p.StepTimeout <= 0 {
		return fmt.Errorf("worker: step timeout must be positive")
	}
	if p.Executor == nil {
		p.Executor = NoopExecutor{}
	}
	if p.Concurrency < 1 {
		p.Concurrency = 1
	}

	owner := newOwnerID()
	log := p.logger().With("worker_id", owner)
	log.Info("worker pool started", "concurrency", p.Concurrency,
		"lease_duration", p.LeaseDuration, "step_timeout", p.StepTimeout)

	var wg sync.WaitGroup
	// The claim loops announce their first poll on firstPolls; the startup
	// reconciliation pass waits for every announcement so a step it
	// resolves back to pending is picked up by a live poll cycle instead
	// of racing a loop that has not polled yet (a pending verdict surfaced
	// in the same microsecond it is reclaimed is invisible to observers
	// and unobservable progress).
	firstPolls := make(chan struct{}, p.Concurrency)
	// Startup + periodic reconciliation of in_doubt steps. Launching inside
	// the wait group means shutdown waits for the pass to notice ctx and
	// exit, never leaking a goroutine past Run.
	if p.Reconciler != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.reconcileLoop(ctx, log, owner, firstPolls)
		}()
	}
	for i := 0; i < p.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.work(ctx, log, owner, firstPolls)
		}()
	}
	wg.Wait()
	log.Info("worker pool stopped")
	return nil
}

// work is one claim→execute→record loop. On ErrNoWork (and transient store
// errors) it sleeps a jittered backoff — bounded polling, never a hot spin.
// It returns when the run context is cancelled or the pool is stopped.
func (p *Pool) work(ctx context.Context, log *slog.Logger, owner string, firstPolls chan<- struct{}) {
	var misses int
	firstPollDone := false
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		step, err := p.Store.ClaimNextStep(ctx, owner, p.LeaseDuration)
		if !firstPollDone {
			// Announce this loop's first poll to the startup
			// reconciliation gate, whatever the claim returned.
			firstPollDone = true
			select {
			case firstPolls <- struct{}{}:
			default:
			}
		}
		switch {
		case isNoWork(err):
			p.sleep(ctx, p.delay(&misses))
			continue
		case err != nil:
			// Transient store failure: do not spin, do not crash the run.
			log.Warn("worker claim failed", "err", err)
			p.sleep(ctx, p.delay(&misses))
			continue
		}
		misses = 0
		if !p.execute(ctx, log, owner, step) {
			return
		}
	}
}

// sleep waits for d, returning early when ctx is cancelled.
func (p *Pool) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// delay computes the next poll delay: it grows linearly with consecutive
// misses up to maxPollInterval, plus jitter, so an idle fleet keeps polling
// at a bounded rate without a synchronized thundering herd.
func (p *Pool) delay(misses *int) time.Duration {
	*misses++
	base := defaultPollInterval * time.Duration(*misses)
	if base > maxPollInterval {
		base = maxPollInterval
	}
	jitter := time.Duration(rand.Float64() * maxPollJitter * float64(base)) //nolint:gosec // G404: spread on a poll interval, not a security token
	return base + jitter
}

// isNoWork reports whether err is (or wraps) the store's ErrNoWork sentinel.
func isNoWork(err error) bool {
	return errors.Is(err, store.ErrNoWork)
}

// execute runs one claimed step: the executor under a step-timeout context,
// a heartbeat keeping the lease alive, and the outcome recorded. It returns
// false only when the pool is shutting down and the worker should exit.
func (p *Pool) execute(ctx context.Context, log *slog.Logger, owner string, step domain.Step) bool {
	// Phase 8: a claimed step whose row was still `running` before the
	// claim was reclaimed off an owner whose lease expired — the
	// observable signature of a dead worker. The claim returns the
	// pre-claim state as the step's Reclaimed flag, so a routine pending
	// claim logs nothing there. The attempt span joins the trace carried
	// in the step's metadata, so a step resumed on another process lands
	// in the same trace the creation request opened.
	if step.Reclaimed {
		p.Tele.IncLeaseExpiration(ctx)
	}
	p.Tele.ObserveStepAttempt(ctx, string(step.Kind))
	spanCtx, span := telemetry.StartStepSpan(
		ctx, step.Metadata, string(step.Kind), step.StepIndex, step.Attempts)
	execCtx, cancel := context.WithTimeout(spanCtx, p.StepTimeout)
	defer cancel()
	outcome := "unknown"
	// The closure reads outcome at return time, not capture time, so each
	// early-exit branch's verdict reaches the span.
	defer func() { telemetry.EndStepSpan(span, outcome) }()

	stopHeartbeat := p.startHeartbeat(execCtx, owner, step.ID, log)
	defer stopHeartbeat()

	// The demo's deterministic pause: the step is claimed, heartbeating,
	// and running for at least the pause, so an external observer
	// (scripts/failure-recovery-demo.sh) reliably sees the live lease and
	// can kill the process with the step mid-flight. Zero in production.
	if pause := p.pauseFor(step.Kind); pause > 0 {
		select {
		case <-time.After(pause):
		case <-execCtx.Done():
		}
	}

	execErr := p.Executor.Execute(execCtx, step)
	// Heartbeat must stop before the outcome is recorded so the lease is
	// stable while CompleteStep/ReleaseStep evaluates owner guards.
	stopHeartbeat()

	// An executor error caused by coordination (the run context was
	// cancelled to shut down, or the step itself timed out) is not an
	// outcome: the executor may have done partial external work, so the
	// step must neither be done nor failed-then-blindly-retried.
	if outcomeUnknown(execErr, execCtx) {
		if ctx.Err() != nil {
			// Shutdown: release in-flight work back to pending instead of
			// leaving it running until the lease expires. The step is
			// re-executed by the next live claim, as the day the worker
			// had crashed.
			outcome = "released"
			relCtx := context.WithoutCancel(ctx)
			if err := p.Store.ReleaseStep(relCtx, step.ID, owner); err != nil {
				log.Warn("worker could not release step on shutdown",
					"step_id", step.ID, "err", err)
			} else {
				log.Info("worker released in-flight step on shutdown", "step_id", step.ID)
			}
			return false
		}
		// Step-by-own timeout: outcome unknown — the executor may have
		// done partial external work, so the step is never reset to
		// pending nor failed. It records in_doubt: the ambiguity is a
		// durable fact, and only the reconciler may resolve it, against
		// the real world, never by blind re-execution.
		outcome = string(domain.StepInDoubt)
		relCtx := context.WithoutCancel(ctx)
		// The environment's in_doubt hop is earned only when the ambiguity
		// itself was durably recorded: if the in_doubt row did not land
		// (the lease was lost to a reclaiming worker, or the store had a
		// transient error), an environment marked in_doubt over no
		// in_doubt step could never be resumed — no verdict would ever
		// commit the resume. The store's move is additionally gated on an
		// actually-ambiguous row (store.MarkEnvironmentInDoubt), so a
		// reconciler that resolves the step in this window makes the move
		// a no-op either way.
		recErr := p.recordOutcome(relCtx, log, owner, step, domain.StepInDoubt, "step timed out; outcome unknown")
		if recErr == nil && !isCleanupKind(step.Kind) {
			// Cleanup environments keep their honest deleting/cancelling
			// state: the cleanup finalizer owns them, and inventing an
			// in_doubt environment hop would hide the cleanup in progress.
			if err := p.Store.MarkEnvironmentInDoubt(relCtx, step.EnvironmentID); err != nil {
				log.Warn("worker could not mark the environment in_doubt",
					"environment_id", step.EnvironmentID, "err", err)
			}
		}
		log.Warn("worker step timed out, recorded as in_doubt for reconciliation",
			"step_id", step.ID, "step_timeout", p.StepTimeout)
		return true
	}

	if execErr != nil {
		recErr := p.recordOutcome(ctx, log, owner, step, domain.StepFailed, execErr.Error())
		if recErr == nil {
			// The executor's verdict landed durably — now (and only now)
			// the span carries it. While the record is missing, the
			// workflow's observable state is still "unknown", and the real
			// claimant's later record owns the terminal metric below.
			outcome = string(domain.StepFailed)
		}
		p.maybeScheduleRetry(ctx, log, step)
		// `final` is derived from the row's retry eligibility, not from
		// whether this ScheduleRetry call succeeded: a transient scheduling
		// error still leaves the resume sweep a later pass to schedule the
		// retry, so the step is not terminal until the budget is exhausted.
		final := recErr == nil && !p.retryEligible(step)
		p.observeStepOutcome(ctx, step, domain.StepFailed, final, outcomeFromExecute)
		p.maybeFinalizeCleanup(ctx, log, step)
		return true
	}
	if step.Kind == domain.KindVerifyUsable {
		// Ready only ever lands in the same transaction that marks the
		// verification step done: no side channel, no race, no claim of
		// readiness the workflow did not earn.
		err := p.Store.CompleteVerification(ctx, step.ID, owner, step.EnvironmentID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			log.Warn("worker lost the lease before recording verification",
				"step_id", step.ID, "err", err)
		case err != nil:
			log.Error("worker could not record the verification outcome",
				"step_id", step.ID, "err", err)
		default:
			log.Info("worker recorded verification; environment ready",
				"step_id", step.ID, "step_index", step.StepIndex, "environment_id", step.EnvironmentID)
			outcome = string(domain.StepDone)
			p.observeStepOutcome(ctx, step, domain.StepDone, true, outcomeFromExecute)
		}
		return true
	}
	recDone := p.recordOutcome(ctx, log, owner, step, domain.StepDone, "")
	if recDone != nil {
		// The outcome never landed durably, so no terminal metric fact and
		// no done-marked span: the span carries "unknown" — the workflow's
		// real observable state until another claimant records the truth.
		return true
	}
	outcome = string(domain.StepDone)
	p.observeStepOutcome(ctx, step, domain.StepDone, true, outcomeFromExecute)
	p.maybeFinalizeCleanup(ctx, log, step)
	return true
}

// outcomeUnknown reports whether execErr (or its absence alongside an
// already-expired execution context) comes from coordination rather than
// from the executor's verdict.
func outcomeUnknown(execErr error, execCtx context.Context) bool {
	if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
		return true
	}
	if execErr == nil && (execCtx.Err() == context.Canceled || execCtx.Err() == context.DeadlineExceeded) {
		return true
	}
	return false
}

// recordOutcome records the step's end state while still holding the lease.
// A lost lease (store.ErrNotFound) means another worker reclaimed the step
// mid-flight: that worker will record the outcome, ours is a no-op — log it.
func (p *Pool) recordOutcome(ctx context.Context, log *slog.Logger, owner string, step domain.Step, state domain.StepState, lastErr string) error {
	err := p.Store.CompleteStep(ctx, step.ID, owner, state, lastErr)
	switch {
	case errors.Is(err, store.ErrNotFound):
		log.Warn("worker lost the lease before recording the outcome",
			"step_id", step.ID, "state", state, "err", err)
	case err != nil:
		log.Error("worker could not record the step outcome",
			"step_id", step.ID, "state", state, "err", err)
	default:
		log.Info("worker step recorded",
			"step_id", step.ID, "step_index", step.StepIndex, "state", state)
	}
	return err
}
