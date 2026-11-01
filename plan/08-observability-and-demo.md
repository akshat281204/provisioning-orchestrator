---
phase: 8
slug: observability-and-demo
title: Tracing, metrics, and the failure-recovery demo
touches: internal/telemetry/, internal/app/, cmd/orchestrator/, scripts/, docs/, README.md, Makefile
verify: make verify
---

## Goal

The system can be operated and demonstrated: one trace shows a whole multi-step
provisioning, Prometheus answers whether anything is stuck, and a single command proves
crash recovery end to end.

## Changes

- `internal/telemetry`: OTLP trace and metric setup against `OTEL_EXPORTER_OTLP_ENDPOINT`,
  with graceful no-op behaviour when the collector is absent so `make run` still works.
- Spans: one per environment provisioning, parented by the HTTP request span; one child
  span per step attempt, carrying `step.kind`, `step.index`, `attempt`, and the outcome.
  Carve out the W3C traceparent through step rows where feasible.
- Metrics, registered on `/metrics`:
  - `provisioning_duration_seconds{outcome}`
  - `provisioning_steps_in_doubt` — the alert-worthy gauge; anything above zero means
    something timed out and is unresolved
  - `provisioning_lease_expirations_total`
  - `provisioning_step_attempts_total{kind}`
  - `provisioning_reconciliations_total{decision}`
- `scripts/failure-recovery-demo.sh`, plus `make demo`, which:
  1. brings up compose and waits for readiness,
  2. creates an environment and waits for `ready`,
  3. verifies it is usable from a one-off `psql` client,
  4. creates a second environment and kills the worker process mid-provisioning,
  5. restarts the service, waits for the lease to expire, and shows the environment
     reaching `ready` with exactly one database and one role,
  6. prints the trace and metric endpoints to inspect.
- README status checklist and `docs/architecture.md` updated to describe behaviour that now
  exists rather than behaviour that is planned.

## Acceptance

- [ ] `make verify` passes.
- [ ] `make demo` completes without manual intervention and exits zero.
- [ ] The demo's final assertion counts exactly one database and one role for the
      interrupted environment, proving the step did not run twice.
- [ ] `/metrics` exposes every metric named above.
- [ ] A provisioning run produces a single trace containing a span per step.
- [ ] With no collector running, the service starts and serves normally.

## Out of scope

- Kafka or any separate event-streaming layer.
