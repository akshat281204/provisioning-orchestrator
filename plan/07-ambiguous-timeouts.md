---
phase: 7
slug: ambiguous-timeouts
title: Ambiguous timeouts and reconciliation
touches: internal/worker/, internal/domain/, internal/provisioner/, internal/store/, cmd/orchestrator/
verify: make verify
---

## Goal

A step that times out has an unknown outcome, and the system resolves that unknown against
reality instead of retrying blindly. Combined with lease expiry, this is what makes a
crashed worker recoverable.

## Changes

- Distinguish the two failure classes explicitly in the worker:
  - The step returned an error → `failed`, eligible for retry or compensation.
  - The step hit `StepTimeout` with no result → `in_doubt`, never retried as-is.
- `in_doubt` steps are not claimable by the normal claim query. A separate reconcile pass
  picks them up.
- `internal/provisioner/reconcile.go`: `Reconcile(ctx, step) (Decision, error)` inspecting
  the actual world per step kind:
  - resource exists and matches intent → `done`
  - resource absent → `pending` (safe to retry)
  - resource exists but differs from intent → `failed` (needs a human, not a retry)
- Reconciliation runs on startup for every `in_doubt` step and periodically thereafter.
- Duplicate events: `POST /environments` with the same idempotency key returns the existing
  environment; add a test proving concurrent duplicate requests still yield one environment
  and one step plan (unique constraint wins the race, the loser returns the row).

## Acceptance

- [ ] `make verify` passes.
- [ ] A step that times out lands in `in_doubt`, not `failed` and not `pending`.
- [ ] `in_doubt` steps are invisible to the ordinary claim query.
- [ ] Reconciliation maps all three observable outcomes correctly, with tests for each.
- [ ] Killing a worker mid-step and restarting the service results in the step completing
      exactly once, with one database and one role in the end state.
- [ ] Ten concurrent `POST`s with one idempotency key create exactly one environment.

## Out of scope

- Metrics and tracing (phase 8).
