---
phase: 1
slug: domain-state-machine
title: Domain model and provisioning state machine
touches: internal/domain/
verify: make verify
---

## Goal

The vocabulary of the system exists as pure, testable Go: what an environment is, what
steps exist, and which state transitions are legal. No database, no network, no clock —
this phase makes the rules explicit so later phases can enforce them rather than restate
them.

## Changes

- `internal/domain/environment.go`:
  - `Environment{ID, Name, IdempotencyKey, State, CreatedAt, UpdatedAt}`.
  - `State` as a string type with constants `pending`, `provisioning`, `verifying`,
    `in_doubt`, `ready`, `rolling_back`, `rolled_back`, `failed`.
  - `Terminal() bool` — true for `ready`, `rolled_back`, `failed`.
  - `String()` on `State`.
- `internal/domain/step.go`:
  - `StepKind` constants: `create_database`, `create_role`, `grant_privileges`,
    `store_credentials`, `verify_connectivity`.
  - `StepState` constants: `pending`, `running`, `in_doubt`, `done`, `failed`,
    `compensated`.
  - `Step{ID, EnvironmentID, Index, Kind, State, Attempts, LeaseOwner, LeaseExpiresAt,
    LastError}`.
  - `Plan()` returning the ordered default step kinds for a new environment.
- `internal/domain/transition.go`:
  - `CanTransition(from, to State) bool` implementing exactly the machine in
    `docs/architecture.md`.
  - `Transition(from, to State) (State, error)` returning a wrapped error naming both
    states when the move is illegal.
- Table-driven tests in `internal/domain/*_test.go` covering every legal edge, a
  representative illegal edge for each state, `Terminal()`, and `Plan()` ordering and
  uniqueness.

## Acceptance

- [ ] `make verify` passes.
- [ ] Every state in the diagram has at least one test for a legal and an illegal
      transition.
- [ ] `domain` imports nothing outside the standard library.
- [ ] `Plan()` returns the five step kinds in the documented order, with no duplicates.

## Out of scope

- Persistence, IDs beyond plain strings, JSON tags for HTTP responses.
