// telemetry.go — the pool's phase-8 metric hooks. The pool is where facts
// happen (attempts, verdicts, terminal outcomes), so it is where the
// instrumentation lives: one increment per durable event, labelsets bounded
// by the domain enumerations telemetry.Metrics enforces.
package worker

import (
	"context"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// Where a terminal outcome was observed from.
type outcomeSource int

const (
	// outcomeFromExecute: the executor's verdict recorded by a live claim's
	// CompleteStep / CompleteVerification.
	outcomeFromExecute outcomeSource = iota
	// outcomeFromReconcile: the reconciler's verdict recorded by
	// RecordReconcileOutcome, which resumes the environment instead of
	// completing the verification transaction.
	outcomeFromReconcile
)

// observeStepOutcome publishes the metric consequences of one recorded step
// outcome. Only terminal outcomes are emitted on the latency/duration series;
// a scheduled retry means the story is not over yet.
//
// final marks that the outcome is the step's last: a failure whose retry
// budget is exhausted, or a completion. The duration an outcome series covers
// is step.CreatedAt → now — plan rows are all written in one store
// transaction at workflow start, so any step's CreatedAt is the workflow's
// start time.
func (p *Pool) observeStepOutcome(
	ctx context.Context, step domain.Step, state domain.StepState, final bool,
	source outcomeSource,
) {
	if p.Tele == nil {
		return
	}
	switch {
	case !isCleanupKind(step.Kind) &&
		step.Kind == domain.KindVerifyUsable &&
		state == domain.StepDone:
		// `ready` lands only inside the verification-completion
		// transaction; this is provisioning's happy terminal outcome.
		// A verify_usable resolved to done by reconciliation on a row the
		// store keeps ambiguous (RecordReconcileOutcome) only resumes the
		// environment to verifying, so the real terminal observation
		// belongs exclusively to CompleteVerification (outcomeFromExecute).
		if final && source == outcomeFromExecute {
			p.Tele.ObserveProvisioningDuration(ctx,
				telemetry.OutcomeSuccess, secondsSinceRFC3339(step.CreatedAt))
		}
	case !isCleanupKind(step.Kind) && state == domain.StepFailed && final:
		// A forward step that exhausted its retry budget: the provisioning
		// workflow is stuck failed for an operator.
		p.Tele.ObserveProvisioningDuration(ctx,
			telemetry.OutcomeFailed, secondsSinceRFC3339(step.CreatedAt))
	case isCleanupKind(step.Kind) && step.Kind == domain.KindReleaseCapacity &&
		state == domain.StepDone && final:
		// The cleanup plan's last step is done — every earlier cleanup step
		// necessarily concluded (the plan runs in durable order), so this is
		// the successful terminal outcome of cleanup.
		p.Tele.ObserveCleanupDuration(ctx,
			telemetry.OutcomeSuccess, secondsSinceRFC3339(step.CreatedAt))
	case isCleanupKind(step.Kind) && state == domain.StepFailed && final:
		// A failed cleanup step the resume sweep cannot retry: the cleanup
		// workflow concluded failed.
		p.Tele.ObserveCleanupDuration(ctx,
			telemetry.OutcomeFailed, secondsSinceRFC3339(step.CreatedAt))
	}
}

// secondsSinceRFC3339 converts a persisted RFC3339 timestamp to elapsed
// seconds, robustly: a row whose timestamp does not parse contributes no
// observation rather than a wild one.
func secondsSinceRFC3339(rfc3339At string) float64 {
	at, err := time.Parse(time.RFC3339, rfc3339At)
	if err != nil {
		return -1 // the recorder drops negative observations
	}
	return time.Since(at).Seconds()
}
