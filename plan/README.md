# Plan

One file per phase: `NN-slug.md`, numbered from `00` with no gaps. Each becomes one
branch, one PR, one merge. The source document is [`PROJECT_PLAN.md`](../PROJECT_PLAN.md).

The frontmatter is the contract the builder agent reads:

```markdown
---
phase: 0
slug: design-contract
title: Write the design contract before any code
touches: docs/design.md
verify: make verify
---

## Goal
## Changes
## Acceptance
## Out of scope
```

## Rules that keep the stack mergeable

1. **Every phase leaves the repository green on its own.** If phase 3 only passes once
   phase 4 lands, the plan is mis-sliced and the first merges put broken code on `main`.
2. **Phase N may assume phases 0..N-1 are merged. Never the reverse.**
3. **Keep each phase under roughly 400 changed lines.** Split further rather than exceed it.
4. **`touches` should be accurate.** A later phase rewriting an earlier phase's file is the
   main source of stack drift if the earlier PR is revised.
5. **Tests needing Postgres, Vault, or Docker go behind `//go:build integration`** so
   `make verify` stays runnable without them.

## Current plan

| # | Phase | What it adds |
| --- | --- | --- |
| 00 | design-contract | States, transitions, authz, secret handling, retry policy, schema rationale |
| 01 | local-foundation | Dependency-aware readiness, request logging, lint in CI |
| 02 | domain-and-lean-persistence | Transition rules; `environments` + `workflow_steps`; migrations |
| 03 | http-api | `/v1` create/status/history, atomic idempotent creation |
| 04 | worker-lease-claiming | Concurrent workers, lease expiry as crash recovery |
| 05 | provisioning-adapters | Capacity, database, role/grant, secrets adapters |
| 06 | readiness-and-credential-safety | Verified readiness, authorized retrieval, rotation, redaction |
| 07 | ambiguous-outcomes-and-cleanup | `in_doubt`, reconciliation, cancel, durable delete |
| 08 | observability-and-demo | Traces, metrics, dashboard, `make demo`, runbook |
| 09 | evaluation-and-docs | Ten fault scenarios, load test, benchmarks, honest README |

Phases 06 and 07 are the largest. If either comes back from review without converging, the
fix is to split it — not to raise `maxReviewRounds`.

Validate with `stack validate` before running `stack run`.
