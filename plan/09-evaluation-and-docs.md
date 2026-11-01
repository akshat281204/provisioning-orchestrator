---
phase: 9
slug: evaluation-and-docs
title: Concurrency tests, recovery measurements, and the runbook
touches: docs/, test/, scripts/, README.md, Makefile
verify: make verify
---

## Goal

The claims in the README become measurements. Every guarantee is backed by a reproducible
command and an honest statement of its limits.

## Changes

- `test/faults/` — one scenario per failure case in the project plan, each asserting both
  persisted workflow state **and** resulting external resources, not HTTP codes:
  1. database created, response lost
  2. worker crashes after creating a role, before recording completion
  3. readiness event duplicated, and arriving after cancellation
  4. grants partially fail after database creation
  5. secrets service unavailable after database provisioning
  6. two requests race for the last capacity slot
  7. cleanup partially succeeds then fails
  8. credential rotation succeeds remotely but the response is lost
  9. lease expires while the original worker is delayed
  10. unauthorized caller requests a credential
- `test/load/` — concurrent creation at a documented rate, reporting p50/p95/p99
  time-to-ready, error rate, and duplicate-resource count (must be zero).
- `scripts/benchmark.sh` — runs the load profile and writes results with machine and
  configuration context (CPU, memory, Go version, Postgres settings, concurrency).
- `docs/evaluation.md` — the numbers, the method, and what they do *not* show.
- `docs/runbook.md` — extend with the operational procedures confirmed while writing the
  fault scenarios.
- README: replace the status checklist with what is actually true, link the evaluation, and
  state the limitations explicitly (single Postgres, local adapters, at-least-once not
  exactly-once, no multi-region).

## Acceptance

- [ ] `make verify` passes.
- [ ] All ten fault scenarios exist and pass, asserting external state.
- [ ] The load test reports time-to-ready percentiles and asserts zero duplicate resources.
- [ ] `make benchmark` is reproducible and records its environment.
- [ ] `docs/evaluation.md` states the methodology and the limitations.
- [ ] No README claim is unbacked by a command or a test.
- [ ] The README's status section matches reality — no checked item is unbuilt.

## Out of scope

- Kubernetes, multi-region, cloud adapters. Kafka remains out unless a measured bottleneck
  justifies it.
