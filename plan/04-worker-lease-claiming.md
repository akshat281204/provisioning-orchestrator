---
phase: 4
slug: worker-lease-claiming
title: Worker pool with lease-based step claiming
touches: internal/worker/, internal/store/, internal/app/, cmd/orchestrator/
verify: make verify
---

## Goal

Steps progress without any central scheduler. Several workers run concurrently, each grabs
a distinct step, and a worker that dies loses its claim only when the lease expires.

## Changes

- `internal/store`: add `ClaimNextStep(ctx, owner string, lease time.Duration) (*Step, error)`
  implemented as a single statement using `FOR UPDATE SKIP LOCKED` over rows that are
  `pending` or `running` with an expired lease, ordered by `step_index`, `limit 1`.
  Returns `ErrNoWork` when nothing is claimable.
- `internal/store`: add `RenewLease(ctx, stepID, owner, lease)` and
  `ReleaseStep(ctx, stepID, owner, newState, lastErr)`.
- `internal/worker/worker.go`:
  - `Pool{Concurrency, LeaseDuration, StepTimeout, Store, Executor}`.
  - `Run(ctx)` starts `Concurrency` goroutines, each looping: claim → execute with
    `context.WithTimeout(ctx, StepTimeout)` → record the outcome → repeat.
  - `Executor` is an interface `Execute(ctx, step) error`, so this phase ships a no-op
    executor and phase 5 swaps in the real one.
  - Idle backoff: when `ErrNoWork`, wait a short jittered interval instead of spinning.
  - On `ctx` cancellation, in-flight steps are released back to `pending`, not left
    `running` with a live lease.
- `internal/worker/heartbeat.go`: renew the lease every `LeaseDuration/3` while a step
  runs, so a slow-but-healthy step is not stolen.
- Wire the pool into `internal/app` and start it from `main`.

## Acceptance

- [ ] `make verify` passes.
- [ ] A step whose lease has expired is claimable by another worker; a step with a live
      lease is not.
- [ ] Two workers cannot claim the same step — tested against the fake store and asserted
      on the SQL in an integration test.
- [ ] A long-running step that keeps renewing is never reclaimed, even past
      `LeaseDuration`.
- [ ] Shutdown releases in-flight steps to `pending`.
- [ ] Idle workers do not spin: an executor that is never called still leaves CPU idle.

## Out of scope

- Real provisioning (phase 5), timeout/ambiguity semantics (phase 7).
