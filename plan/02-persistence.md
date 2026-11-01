---
phase: 2
slug: persistence
title: Postgres schema, migrations, and repositories
touches: migrations/, internal/store/, cmd/migrate/, Makefile
verify: make verify
---

## Goal

Every piece of state that must survive a crash lives in Postgres, behind interfaces the
rest of the system can be tested against without a database.

## Changes

- `migrations/0001_init.sql`:
  - `environments` table: `id uuid primary key`, `name text not null`,
    `idempotency_key text not null unique`, `state text not null`,
    `created_at`, `updated_at timestamptz not null default now()`.
  - `provisioning_steps` table: `id uuid`, `environment_id uuid not null references
    environments(id) on delete cascade`, `step_index int not null`, `kind text not null`,
    `state text not null`, `attempts int not null default 0`, `lease_owner text`,
    `lease_expires_at timestamptz`, `last_error text`, and
    `unique (environment_id, step_index)`.
  - Index on `(state, lease_expires_at)` for the claim query, and on `environment_id`.
  - A `schema_migrations` table recording applied versions.
- `internal/store/store.go`: interfaces `EnvironmentStore` and `StepStore` with the
  methods later phases need (`Create`, `Get`, `GetByIdempotencyKey`, `List`, and for steps
  `ListForEnvironment`, `UpdateState`).
- `internal/store/postgres.go`: `pgxpool`-backed implementation. `New(ctx, dsn)` pings and
  returns a wrapped error on failure; `Close()`.
- `internal/store/migrate.go`: applies embedded `migrations/*.sql` in order inside a
  transaction, idempotently, recording applied versions.
- `cmd/migrate/main.go` plus a `make migrate` target.
- Errors: define `ErrNotFound` in `internal/store` and map `pgx.ErrNoRows` to it.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] Migrations apply twice in a row without error, and the second run changes nothing.
- [ ] `Get` for a missing id returns `ErrNotFound`, not a raw pgx error.
- [ ] Integration tests behind `//go:build integration` cover create, get, idempotency-key
      lookup, and the unique constraint on `(environment_id, step_index)`.
- [ ] Inserting two environments with the same `idempotency_key` fails with a detectable
      duplicate error.

## Out of scope

- The claim/lease query (phase 4), HTTP handlers (phase 3).
