# Plan

One file per phase: `NN-slug.md`, numbered from `01` with no gaps. Each becomes one
branch, one PR, one merge.

The frontmatter is the contract the builder agent reads:

```markdown
---
phase: 1
slug: auth-foundation
title: Add user model and session table
touches: src/db/schema.ts, src/auth/session.ts
verify: npm test
---

## Goal
## Changes
## Acceptance
## Out of scope
```

## Rules that keep the stack mergeable

1. **Every phase leaves the repository green on its own.** If phase 3 only passes once
   phase 4 lands, the plan is mis-sliced and the first merges put broken code on `main`.
2. **Phase N may assume phases 1..N-1 are merged. Never the reverse.**
3. **Keep each phase under roughly 400 changed lines.** Split further rather than exceed it.
4. **`touches` should be accurate.** A later phase rewriting an earlier phase's file is the
   main source of stack drift if the earlier PR is revised.
5. **Tests that need external services go behind a build tag** so `make verify` stays
   runnable without them.

## Current plan

| # | Phase | What it adds |
| --- | --- | --- |
| 01 | domain-state-machine | Pure domain types and legal state transitions |
| 02 | persistence | Postgres schema, migrations, store interfaces |
| 03 | http-api | Create/read environments, idempotent creation |
| 04 | worker-lease-claiming | Concurrent workers, lease expiry as crash recovery |
| 05 | provisioning-steps | Real database/role/credential steps, compensation |
| 06 | readiness-gate | `ready` only after a verified connection as the role |
| 07 | ambiguous-timeouts | `in_doubt` handling and reconciliation |
| 08 | observability-and-demo | Traces, metrics, `make demo` |

Validate with `stack validate` before running `stack run`.
