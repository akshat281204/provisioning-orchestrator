---
phase: 0
slug: design-contract
title: Write the design contract before any code
touches: docs/design.md, docs/architecture.md
verify: make verify
---

## Goal

The rules the rest of the build depends on are written down once, precisely, before they
are implemented nine times by an agent that has never seen the others. This is a document
phase: it produces no Go.

## Changes

- `docs/design.md`, covering:
  - **State model** — every state an environment and a step can occupy, with defined entry
    behaviour, exit behaviour, and which transitions are legal. Include `in_doubt` and the
    cleanup states.
  - **Step semantics** — ordered steps, `operation_key` derivation, and the explicit
    statement that work is *at least once*, never exactly once, and therefore every step
    must be idempotent or reconcilable.
  - **Retry and reconciliation policy** — what is retried with backoff, what is not, when
    a timeout becomes `in_doubt` rather than `failed`, and how each step kind is
    reconciled against reality.
  - **Authorization model** — who may create, read, cancel, delete, and retrieve a
    credential. State that credentials are never returned on the status path.
  - **Secret handling** — where credentials live, what may appear in logs, traces, error
    messages, and persisted `metadata`. Redaction rules.
  - **Persistence rationale** — which fields are typed columns and which are JSONB, and why.
    Explicitly list the tables deliberately *not* created (step attempts, events,
    resources, compensation records) and what replaces them.
  - **API contract** — the `/v1` surface, request/response shapes, and status codes.
- Update `docs/architecture.md` so it does not contradict `docs/design.md`; it stays the
  narrative version and defers to the contract for rules.

## Acceptance

- [ ] Every state has a stated entry condition and exit condition.
- [ ] Every legal transition is listed; the document says what happens on an illegal one.
- [ ] Ambiguous outcomes are defined per step kind, with the reconciliation action.
- [ ] Cleanup is specified as durable and retryable, and the doc states an environment is
      never reported deleted while cleanup is pending.
- [ ] The JSONB-vs-column decision is justified per field, not asserted.
- [ ] No secret appears in any example payload.

## Out of scope

- Implementation. Any code change beyond doc links in the README.
