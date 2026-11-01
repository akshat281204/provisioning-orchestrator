---
phase: 2
slug: domain-and-lean-persistence
title: Domain transitions and the lean schema
touches: internal/domain/, internal/store/, migrations/, cmd/migrate/, Makefile
verify: make verify
---

## Goal

Legal state transitions exist as pure Go, and the two-table schema from the design contract
is real, migratable, and enforced by constraints. Nothing else is persisted — no events
table, no attempts table, no resources table.

## Changes

- `internal/domain/`:
  - `Environment` and `Step` types with `State` string enums matching `docs/design.md`.
  - `CanTransition(from, to) bool` and `Transition(from, to) (State, error)` returning an
    error naming both states when illegal.
  - `Plan()` returning the ordered step kinds for a new environment.
  - Table-driven tests: every legal edge, plus one illegal edge per state.
- `migrations/0001_init.sql`:
  - `environments`: `id uuid pk`, `tenant_id text not null`, `name text not null`,
    `state text not null`, `idempotency_key text not null`, `spec jsonb not null default
    '{}'`, `metadata jsonb not null default '{}'`, `created_at`, `updated_at`,
    `expires_at timestamptz null`, and `unique (tenant_id, idempotency_key)`.
  - `workflow_steps`: `id uuid pk`, `environment_id uuid not null references
    environments(id) on delete cascade`, `step_index int not null`, `kind text not null`,
    `state text not null`, `attempts int not null default 0`, `lease_owner text`,
    `lease_expires_at timestamptz`, `operation_key text`, `metadata jsonb not null default
    '{}'`, `last_error text`, `created_at`, `updated_at`, and `unique (environment_id,
    step_index)`.
  - Index on `(state, lease_expires_at)` and on `environment_id`.
  - Check constraints on `state` so an invalid state cannot be written even by hand.
- `internal/store/migrate.go` + `cmd/migrate` + `make migrate`: apply embedded
  `migrations/*.sql` in order, idempotently, tracking versions in the migration tool's own
  table (not a domain table).
- `internal/store/store.go`: `EnvironmentStore` and `StepStore` interfaces.
- `internal/store/postgres.go`: pgxpool implementation; map `pgx.ErrNoRows` to
  `store.ErrNotFound`; expose a duplicate detection for the idempotency constraint.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] Every legal edge in the design contract has a test; every state has an illegal edge
      that is rejected.
- [ ] Migrations apply twice without error and the second run is a no-op.
- [ ] Integration tests (build tag) cover create, get, idempotency lookup, the unique
      constraint on `(environment_id, step_index)`, and the foreign key cascade.
- [ ] `Get` of a missing id returns `ErrNotFound`, never a raw pgx error.
- [ ] `\d workflow_steps` shows no table exists purely for step attempts, events, or
      resources.

## Out of scope

- The claim/lease query (phase 4), HTTP (phase 3).
