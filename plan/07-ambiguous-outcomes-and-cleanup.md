---
phase: 7
slug: ambiguous-outcomes-and-cleanup
title: in_doubt reconciliation, retries, and durable cleanup
touches: internal/worker/, internal/domain/, internal/adapters/, internal/store/, internal/httpapi/, migrations/
verify: make verify
---

## Goal

A timed-out step is an unknown, not a failure. The system resolves the unknown against
reality instead of retrying blindly. Cancellation and deletion are themselves durable,
resumable workflows — an environment is never reported deleted while cleanup is pending.

## Changes

- Worker distinguishes the two failure classes explicitly:
  - step returned an error → `failed`, eligible for bounded retry or compensation
  - step hit the step timeout with no result → `in_doubt`, never retried as-is
- `in_doubt` steps are excluded from the ordinary claim query; a separate reconcile pass
  claims them.
- `internal/adapters/reconcile.go`: per step kind, inspect the real world and decide —
  exists-and-matches → `done`; absent → `pending`; exists-but-differs → `failed`.
- Reconcile runs on startup for every `in_doubt` step, and periodically thereafter.
- Bounded retries with exponential backoff and jitter, capped attempts per step, recorded
  in `attempts` and `last_error`.
- `POST /v1/environments/{id}/cancel` — stop scheduling new steps; in-flight steps are
  allowed to finish or expire.
- `DELETE /v1/environments/{id}` — begin durable cleanup: a `deleting` state with
  compensation steps recorded as rows, executed in reverse order (drop role, drop database,
  delete secret, release capacity). Returns `202`. The environment reports `deleted` only
  when every cleanup step is `done`; until then it reports `deleting` with progress.
- A cleanup step that fails stays visible as `failed` and is retried; cleanup never
  silently abandons a resource.
- `metadata` carries the compact audit timeline (state changes with timestamps), redacted.

## Acceptance

- [ ] `make verify` passes.
- [ ] A timed-out step lands in `in_doubt`, never `failed` and never `pending`.
- [ ] `in_doubt` steps are invisible to the ordinary claim query.
- [ ] Reconciliation maps all three observable outcomes correctly, each with a test.
- [ ] Killing a worker mid-step and restarting yields exactly one database and one role.
- [ ] Cancelling a provisioning environment results in `cancelled` with no orphaned
      resources.
- [ ] `DELETE` returns `202`, stays `deleting` while cleanup is incomplete, and reports
      `deleted` only when it is.
- [ ] A cleanup step that keeps failing marks the environment `failed` with the step
      visible, not `deleted`.
- [ ] Ten concurrent `POST`s with one idempotency key still create exactly one environment.

## Out of scope

- Metrics and tracing for these paths (phase 8).
