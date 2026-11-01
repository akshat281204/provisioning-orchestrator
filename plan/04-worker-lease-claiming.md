---
phase: 4
slug: worker-lease-claiming
title: Worker pool and lease-based claiming
touches: internal/worker/, internal/store/, internal/app/, cmd/orchestrator/
verify: make verify
---

## Goal

Steps advance with no central scheduler. Several workers claim distinct steps, and a
worker that dies loses its claim only when the lease expires — which is the whole crash
recovery mechanism.

## Changes

- `internal/store`: `ClaimNextStep(ctx, owner, lease)` as one statement over rows that are
  `pending`, or `running` with `lease_expires_at < now()`, ordered by `step_index`, using
  `FOR UPDATE SKIP LOCKED … LIMIT 1`. Returns `ErrNoWork` when nothing is claimable.
- `internal/store`: `RenewLease(ctx, stepID, owner, lease)` and
  `CompleteStep(ctx, stepID, owner, state, lastErr)`.
- `internal/worker/worker.go`:
  - `Pool{Concurrency, LeaseDuration, StepTimeout, Store, Executor}`.
  - `Run(ctx)` — N goroutines, each claim → execute under a step timeout → record outcome.
  - `Executor` interface so this phase ships a no-op executor and phase 5 swaps in the real
    adapters.
  - Jittered backoff on `ErrNoWork`; no busy spin.
  - On shutdown, in-flight steps are released to `pending` rather than left `running`.
- `internal/worker/heartbeat.go`: renew every `LeaseDuration/3` while executing, so a slow
  but healthy step is never stolen.
- Wire the pool into `internal/app`; start from `main`.

## Acceptance

- [ ] `make verify` passes.
- [ ] An expired lease is claimable by another worker; a live lease is not.
- [ ] Two workers never hold the same live step — asserted against the fake store and, in
      an integration test, on the real SQL.
- [ ] A step that keeps renewing is never reclaimed, even past `LeaseDuration`.
- [ ] Shutdown releases in-flight steps to `pending`.
- [ ] An idle pool performs bounded polling, not a hot loop.

## Out of scope

- Real provisioning (phase 5), ambiguity semantics (phase 7).
