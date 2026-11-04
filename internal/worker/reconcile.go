// reconcile.go — the phase-7 resolution of unknown outcomes. A step whose
// execution context died mid-flight is neither done nor failed: it is
// in_doubt, and the only honest resolution is to look at the real world.
// This file runs that lookup: claim an in_doubt step (the only claim path
// that ever touches one), ask the Reconciler what the world says, and record
// the answer. An observation error is never a verdict — the step simply
// stays in_doubt and the next pass asks again.
//
// The same file carries the bounded-retry scheduler: a step that returned an
// error is eligible for retry with exponential backoff and jitter, capped at
// the attempts the ROW has already burned — never at process-local state.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// isCleanupKind reports whether kind belongs to the durable cleanup plan
// (the compensation steps of a deletion or cancellation).
func isCleanupKind(kind domain.StepKind) bool {
	for _, k := range domain.CleanupPlan {
		if k == kind {
			return true
		}
	}
	return false
}

// reconcileLoop runs a reconciliation pass at startup and then every
// interval, until the run context is cancelled. The startup pass waits for
// the pool's claim loops to have polled the queue once: a reconciled step
// falling back to pending must be picked up by a live claim loop's normal
// poll, not raced against a loop that has not started polling yet.
func (p *Pool) reconcileLoop(ctx context.Context, log *slog.Logger, owner string, firstPolls <-chan struct{}) {
	for i := 0; i < p.Concurrency; i++ {
		select {
		case <-firstPolls:
		case <-ctx.Done():
			return
		}
	}
	interval := p.getInterval()
	for {
		p.reconcilePass(ctx, log, owner)
		p.sleep(ctx, interval)
		if ctx.Err() != nil {
			return
		}
	}
}

// reconcilePass runs one sweep: reaped abandoned claims, every in_doubt step
// that is currently visible, oldest first, then the resume sweep (the
// cleanup finalizer and the capped retry scheduler for rows a crash ate the
// follow-up on), until the store stops answering or the context ends.
func (p *Pool) reconcilePass(ctx context.Context, log *slog.Logger, owner string) {
	// The steps_in_doubt gauge is swept from the rows, not incremented
	// locally: a restart must publish the truth it inherits, not forget it.
	if counter, ok := p.Store.(interface {
		CountStepsInDoubt(context.Context) (int64, error)
	}); ok {
		if n, err := counter.CountStepsInDoubt(ctx); err != nil {
			log.Warn("worker reconciliation could not sweep the in_doubt gauge", "err", err)
		} else {
			p.Tele.SetStepsInDoubt(n)
		}
	}
	if _, err := p.Store.ReapExpiredReconciliations(ctx); err != nil {
		log.Warn("worker reconciliation could not reap abandoned claims", "err", err)
	}
	for ctx.Err() == nil {
		step, err := p.Store.ClaimNextInDoubtStep(ctx, owner)
		if err == nil {
			p.resolveInDoubt(ctx, log, step)
			continue
		}
		if !isNoWork(err) {
			log.Warn("worker reconciliation claim failed", "err", err)
		}
		break
	}
	p.resumeSweep(ctx, log)
}

// resumeSweep is the crash-recovery tail of the reconciliation pass: the
// outcome recording and the finalize/retry follow-up are two statements, so
// a crash between them is a real possibility the sweep re-arms from the rows
// themselves. Environments on a cleanup path (deleting / cancelling /
// failed, cleanup rows present) are finalized again, and failed steps under
// their row's attempt cap are re-scheduled with the same durable backoff.
// Every call is idempotent: FinalizeCleanup closes only when a verdict is
// derivable, and ScheduleRetry is a no-op on any row not resting failed.
func (p *Pool) resumeSweep(ctx context.Context, log *slog.Logger) {
	nctx := context.WithoutCancel(ctx)
	// Reschedule before finalizing: a failure-to-retry crash gap must not
	// be mistaken for exhausted cleanup and terminalized before its retry
	// budget is considered. A listing error aborts the sweep so a terminal
	// verdict is never derived from worklists this pass could not read: a
	// failed cleanup row whose retry could not be scheduled must stay
	// retryable on the next pass, never finalized as exhausted.
	steps, err := p.Store.ListFailedSteps(nctx)
	if err != nil {
		log.Warn("worker resume sweep could not list failed steps, deferring the pass", "err", err)
		return
	}
	for _, step := range steps {
		p.maybeScheduleRetry(nctx, log, step)
	}
	envIDs, err := p.Store.ListCleanupEnvironments(nctx)
	if err != nil {
		log.Warn("worker resume sweep could not list cleanup environments", "err", err)
		return
	}
	for _, envID := range envIDs {
		if err := p.Store.FinalizeCleanup(nctx, envID); err != nil {
			log.Error("worker resume sweep could not finalize the cleanup workflow",
				"environment_id", envID, "err", err)
		}
	}
}

// resolveInDoubt asks the reconciler for one claimed step's real-world
// outcome and records it. Reconcile errors leave the step in_doubt (a claim
// displays no verdict — it just makes the inspection atomic), and the lease
// the pass lost on a crash is no worse: the reaper returns the row.
//
// Ambiguity resolution is intentionally outside RetryPolicy.MaxAttempts: a
// resolved `pending` verdict does not consume the retry budget, and a step
// on an oscillating in_doubt → pending → in_doubt cycle is therefore not
// capped by it. MaxAttempts bounds the error-retry schedule the executor
// failure path uses; re-executing a step whose outcome is provably absent is
// one observation against the world, not a new blind attempt against a
// failing dependency. A persistently misbehaving step self-reveals as
// repeated reconciliation verdicts in its row history.
func (p *Pool) resolveInDoubt(ctx context.Context, log *slog.Logger, step domain.Step) {
	outcome, err := p.Reconciler.Reconcile(ctx, step)
	if err != nil {
		// Could not observe. No guess is recorded; the step returns to
		// in_doubt so a later pass asks the world again.
		log.Warn("worker reconciliation could not observe the step, leaving in_doubt",
			"step_id", step.ID, "kind", step.Kind, "err", err)
		p.recordReconcileOutcome(ctx, log, step, domain.StepInDoubt, "")
		return
	}

	switch outcome {
	case adapters.OutcomeDone:
		// Exists-and-matches: the side effect landed. Record it as if the
		// step had finished normally.
		log.Info("worker reconciliation resolved the step as done",
			"step_id", step.ID, "kind", step.Kind)
		p.recordReconcileOutcome(ctx, log, step, domain.StepDone, "")
	case adapters.OutcomePending:
		// Absent: the attempt provably never finished. Back to pending,
		// where the ordinary claim re-executes it from scratch.
		log.Info("worker reconciliation resolved the step as pending",
			"step_id", step.ID, "kind", step.Kind)
		p.recordReconcileOutcome(ctx, log, step, domain.StepPending, "")
	case adapters.OutcomeFailed:
		// Exists-but-differs: a verdict-shaped failure to surface, not to
		// retry blindly.
		log.Warn("worker reconciliation resolved the step as failed",
			"step_id", step.ID, "kind", step.Kind)
		msg := fmt.Sprintf("reconciliation: %s exists but does not match the plan", step.Kind)
		p.recordReconcileOutcome(ctx, log, step, domain.StepFailed, msg)
	default:
		// An unrecognised outcome is an unobservation, not a verdict.
		log.Error("worker reconciliation produced an unknown outcome",
			"step_id", step.ID, "outcome", string(outcome))
		p.recordReconcileOutcome(ctx, log, step, domain.StepInDoubt, "")
	}
}

// recordReconcileOutcome writes the reconciler's answer on the row the
// reconciler holds (RecordReconcileOutcome — never the executor's
// CompleteStep, so a record can only ever land on an ambiguous or borrowed
// row). The verdict and the environment's resume out of in_doubt are ONE
// store transaction — there is no separate resume call left to crash
// between. Done and failed cleanup steps also ask the finalizer to close
// the cleanup; the finalizer is recoverable from the resume sweep, so it
// stays a follow-up call.
func (p *Pool) recordReconcileOutcome(
	ctx context.Context, log *slog.Logger, step domain.Step, state domain.StepState, lastErr string,
) {
	nctx := context.WithoutCancel(ctx)
	err := p.Store.RecordReconcileOutcome(nctx, step.ID, step.LeaseOwner, state, lastErr)
	switch {
	case errors.Is(err, store.ErrNotFound):
		log.Warn("worker reconciliation lost the step's lease before recording",
			"step_id", step.ID, "state", state, "err", err)
	case err != nil:
		log.Error("worker reconciliation could not record the outcome",
			"step_id", step.ID, "state", state, "err", err)
	}
	if err != nil {
		return // only a recorded verdict is a metric fact
	}
	// Phase 8: one reconciliation verdict, labelled by the decision the
	// reconciler landed, and its latency-series consequences.
	p.Tele.ObserveReconciliation(ctx, reconcileDecisionFor(state))
	p.observeStepOutcome(ctx, step, state, true, outcomeFromReconcile)
	if state != domain.StepInDoubt && state != domain.StepFailed && isCleanupKind(step.Kind) {
		p.maybeFinalizeCleanup(nctx, log, step)
	}
}

// reconcileDecisionFor maps the recorded step state onto the bounded
// reconcile-decision label domain.
func reconcileDecisionFor(state domain.StepState) string {
	switch state {
	case domain.StepDone:
		return telemetry.DecisionDone
	case domain.StepPending:
		return telemetry.DecisionPending
	case domain.StepFailed:
		return telemetry.DecisionFailed
	default:
		return telemetry.DecisionUnobservable
	}
}

// maybeFinalizeCleanup asks the store to close the cleanup workflow for the
// step's environment. It is a no-op for forward steps and for environments
// whose cleanup plan is still mid-flight — progress keeps reporting, and
// only a full plan concludes to a terminal or failed state.
func (p *Pool) maybeFinalizeCleanup(ctx context.Context, log *slog.Logger, step domain.Step) {
	if !isCleanupKind(step.Kind) {
		return
	}
	if err := p.Store.FinalizeCleanup(context.WithoutCancel(ctx), step.EnvironmentID); err != nil {
		log.Error("worker could not finalize the cleanup workflow",
			"environment_id", step.EnvironmentID, "step_id", step.ID, "err", err)
	}
}

// maybeScheduleRetry reschedules a just-failed step for another attempt
// while its persisted attempts are under the cap. The cap is read from the
// claimed row (step.Attempts): a retry the worker died before scheduling is
// never remembered locally, so the budget is the ROW's, and a step that
// exhausted it stays failed for an operator instead of looping forever. It
// reports whether a retry was actually scheduled — so the caller can tell a
// transient failure (retry pending) from a final one (the failed terminal
// outcome must be emitted to metrics now, and nothing else will).
func (p *Pool) maybeScheduleRetry(ctx context.Context, log *slog.Logger, step domain.Step) bool {
	maxAttempts := p.getMaxAttempts()
	if maxAttempts <= 0 || step.Attempts >= maxAttempts {
		return false // retry budget exhausted or scheduling disabled: stays failed
	}
	delay := p.retryDelay(step.Attempts)
	if err := p.Store.ScheduleRetry(context.WithoutCancel(ctx), step.ID, delay); err != nil {
		// Not fatal: the failure itself is durable; a missed reschedule
		// only means the step stays failed until an operator intervenes,
		// which is the conservative direction.
		log.Warn("worker could not schedule the step retry",
			"step_id", step.ID, "delay", delay, "err", err)
		return false
	}
	log.Info("worker scheduled a step retry",
		"step_id", step.ID, "attempts", step.Attempts, "max_attempts", maxAttempts, "delay", delay)
	return true
}

// retryDelay shapes the durable backoff for the attempt that just failed:
// exponentially growing with the attempt number, capped, and jittered so a
// fleet of workers does not hammer one failing dependency in unison.
func (p *Pool) retryDelay(attempts int) time.Duration {
	base, max := p.getBackoffs()
	// attempts counts the finished executions; the first retry waits base.
	// The doubling stays in the bounded integer domain: after the cap any
	// further growth is irrelevant, so the loop is deliberately shallow.
	d := base
	for i := 1; i < attempts && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	jitter := time.Duration(rand.Float64() * retryBackoffJitter * float64(d)) //nolint:gosec // G404: spread on a backoff, not a security token
	return d + jitter
}
