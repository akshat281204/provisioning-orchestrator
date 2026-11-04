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
	envIDs, err := p.Store.ListCleanupEnvironments(nctx)
	switch {
	case err != nil:
		log.Warn("worker resume sweep could not list cleanup environments", "err", err)
	default:
		for _, envID := range envIDs {
			if err := p.Store.FinalizeCleanup(nctx, envID); err != nil {
				log.Error("worker resume sweep could not finalize the cleanup workflow",
					"environment_id", envID, "err", err)
			}
		}
	}
	steps, err := p.Store.ListFailedSteps(nctx)
	if err != nil {
		log.Warn("worker resume sweep could not list failed steps", "err", err)
		return
	}
	for _, step := range steps {
		p.maybeScheduleRetry(nctx, log, step)
	}
}

// resolveInDoubt asks the reconciler for one claimed step's real-world
// outcome and records it. Reconcile errors leave the step in_doubt (a claim
// displays no verdict — it just makes the inspection atomic), and the lease
// the pass lost on a crash is no worse: the reaper returns the row.
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
// row). Done and failed cleanup steps also ask the finalizer to close the
// cleanup; the environment resume is asked regardless of state.
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
	if err == nil && state != domain.StepInDoubt {
		p.resumeAfterReconcile(nctx, log, step)
	}
}

// resumeAfterReconcile moves the environment out of in_doubt once its
// ambiguous step has a verdict, and lets the cleanup finalizer look at the
// compensation plan when the resolved step was a cleanup step.
func (p *Pool) resumeAfterReconcile(ctx context.Context, log *slog.Logger, step domain.Step) {
	if err := p.Store.ResumeFromInDoubt(ctx, step.EnvironmentID, step.StepIndex); err != nil {
		log.Warn("worker reconciliation could not resume the environment",
			"environment_id", step.EnvironmentID, "err", err)
	}
	if isCleanupKind(step.Kind) {
		p.maybeFinalizeCleanup(ctx, log, step)
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
// exhausted it stays failed for an operator instead of looping forever.
func (p *Pool) maybeScheduleRetry(ctx context.Context, log *slog.Logger, step domain.Step) {
	maxAttempts := p.getMaxAttempts()
	if maxAttempts <= 0 || step.Attempts >= maxAttempts {
		return // retry budget exhausted or scheduling disabled: stays failed
	}
	delay := p.retryDelay(step.Attempts)
	if err := p.Store.ScheduleRetry(context.WithoutCancel(ctx), step.ID, delay); err != nil {
		// Not fatal: the failure itself is durable; a missed reschedule
		// only means the step stays failed until an operator intervenes,
		// which is the conservative direction.
		log.Warn("worker could not schedule the step retry",
			"step_id", step.ID, "delay", delay, "err", err)
		return
	}
	log.Info("worker scheduled a step retry",
		"step_id", step.ID, "attempts", step.Attempts, "max_attempts", maxAttempts, "delay", delay)
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
