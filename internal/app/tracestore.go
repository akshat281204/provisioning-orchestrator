// tracestore.go — the /v1 store wrapper that opens a provisioning span for
// every environment creation and persists the W3C trace context on the
// environment's step rows. That write is what makes one provisioning one
// trace: the spans themselves are opened by whichever worker executes the
// steps, on whichever process, at whatever later time — and they join this
// trace because the traceparent lives in the rows, not in a dying process.
package app

import (
	"context"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// traceStore delegates to the Postgres store and decorates environment
// creation with tracing. Everything else is inherited verbatim.
type traceStore struct {
	*store.PostgresStore
}

// newTraceStore wraps the store the /v1 API reads and writes through.
func newTraceStore(st *store.PostgresStore) *traceStore {
	return &traceStore{PostgresStore: st}
}

// CreateEnvironmentWithPlan opens the provisioning span (a child of the HTTP
// request span, which the trace middleware put into the request context) and
// passes its W3C traceparent into the store transaction, which stamps it on
// every step row atomically with the plan. That atomicity is what makes one
// provisioning one trace: the spans themselves are opened by whichever worker
// executes the steps, at whatever later time — and they join this trace
// because the traceparent lives in the rows, not in a dying process.
func (s *traceStore) CreateEnvironmentWithPlan(
	ctx context.Context, env domain.Environment, steps []domain.PlannedStep,
	traceparent string,
) (domain.Environment, error) {
	ctx, span := telemetry.NewProvisioningSpan(ctx)
	created, err := s.PostgresStore.CreateEnvironmentWithPlan(
		ctx, env, steps, telemetry.TraceparentFromContext(ctx))
	if err != nil {
		telemetry.EndProvisioningSpan(span, false)
		return domain.Environment{}, err
	}
	telemetry.EndProvisioningSpan(span, true)
	return created, nil
}
