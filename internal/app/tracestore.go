// tracestore.go — the /v1 store wrapper that opens a provisioning span for
// every environment creation and persists the W3C trace context on the
// environment's step rows. That write is what makes one provisioning one
// trace: the spans themselves are opened by whichever worker executes the
// steps, on whichever process, at whatever later time — and they join this
// trace because the traceparent lives in the rows, not in a dying process.
package app

import (
	"context"
	"log/slog"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// traceStore delegates to the Postgres store and decorates environment
// creation with tracing. Everything else is inherited verbatim.
type traceStore struct {
	*store.PostgresStore
	log *slog.Logger
}

// newTraceStore wraps the store the /v1 API reads and writes through.
func newTraceStore(st *store.PostgresStore, log *slog.Logger) *traceStore {
	return &traceStore{PostgresStore: st, log: log}
}

// CreateEnvironmentWithPlan opens the provisioning span (a child of the HTTP
// request span, which the trace middleware put into the request context).
// The span stays open only for this transaction — the workflow's real life
// happens on the workers — and its traceparent is written onto every step row
// so worker-side child spans join the same trace.
func (s *traceStore) CreateEnvironmentWithPlan(
	ctx context.Context, env domain.Environment, steps []domain.PlannedStep,
) (domain.Environment, error) {
	ctx, span := telemetry.NewProvisioningSpan(ctx)
	created, err := s.PostgresStore.CreateEnvironmentWithPlan(ctx, env, steps)
	if err != nil {
		telemetry.EndProvisioningSpan(span, false)
		return domain.Environment{}, err
	}
	if tp := telemetry.TraceparentFromContext(ctx); tp != "" {
		if err := s.SetStepsTraceparent(ctx, created.ID, tp); err != nil {
			s.log.Warn("app: could not carry the trace context onto the step rows",
				"environment_id", created.ID, "err", err)
			// Tracing is observability, not coordination: the workflow
			// proceeds, the steps simply start a fresh trace.
		}
	}
	telemetry.EndProvisioningSpan(span, true)
	return created, nil
}
