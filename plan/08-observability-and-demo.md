---
phase: 8
slug: observability-and-demo
title: Traces, metrics, dashboard, and the failure-recovery demo
touches: internal/telemetry/, internal/app/, cmd/orchestrator/, scripts/, deploy/, docs/, README.md, Makefile
verify: make verify
---

## Goal

An operator can see whether anything is stuck, and one command proves crash recovery end to
end. Telemetry contains no secrets and no unbounded high-cardinality labels.

## Changes

- `internal/telemetry`: OTLP trace + metric providers against
  `OTEL_EXPORTER_OTLP_ENDPOINT`, degrading to a no-op when no collector is configured so
  `make run` still works.
- Tracing: one span per environment provisioning (parented to the HTTP request span), one
  child per step attempt carrying `step.kind`, `step.index`, `attempt`, `outcome`. Propagate
  the trace context through step `metadata` so a step resumed by a different worker joins
  the same trace.
- Metrics on `/metrics`:
  - `provisioning_duration_seconds{outcome}`
  - `provisioning_steps_in_doubt` — the alert-worthy gauge
  - `provisioning_lease_expirations_total`
  - `provisioning_step_attempts_total{kind}`
  - `provisioning_reconciliations_total{decision}`
  - `cleanup_duration_seconds{outcome}`
  - Label cardinality is bounded: no environment IDs, no tenant IDs, no free-text errors.
- `deploy/dashboard.json` — a Grafana dashboard over those metrics, provisionable locally.
- `scripts/failure-recovery-demo.sh` + `make demo`:
  1. bring up compose, wait for readiness
  2. create an environment, wait for `ready`
  3. connect from a one-off `psql` as the provisioned role to prove usability
  4. create a second environment and kill the process mid-provisioning
  5. restart; show the lease expiring and the environment reaching `ready`
  6. assert exactly one database and one role exist for it
  7. print trace and metric endpoints to inspect
- `docs/runbook.md`: how to find stuck workflows, what `in_doubt` means operationally, how
  to force reconciliation, how to recover a failed cleanup.

## Acceptance

- [ ] `make verify` passes.
- [ ] `make demo` completes unattended and exits zero.
- [ ] The demo's final assertion counts exactly one database and one role for the
      interrupted environment.
- [ ] `/metrics` exposes every metric named above.
- [ ] One provisioning produces a single trace with a span per step.
- [ ] The service starts and serves normally with no collector running.
- [ ] A test asserts no span attribute or metric label contains a sentinel password or an
      environment ID.

## Out of scope

- Alerting rules wired to a real Alertmanager, hosted Grafana.
