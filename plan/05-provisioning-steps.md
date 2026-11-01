---
phase: 5
slug: provisioning-steps
title: Real provisioning steps and compensation
touches: internal/provisioner/, internal/worker/, internal/domain/, migrations/, go.mod
verify: make verify
---

## Goal

Steps actually create a database, a least-privilege role, grants, and a Vault credential —
and every step is safe to run twice, because it will be.

## Changes

- `go.mod`: add `github.com/jackc/pgx/v5`, `github.com/hashicorp/vault/api`.
- `internal/provisioner/postgres.go`:
  - `EnsureDatabase(ctx, name)` — check `pg_database`, create if absent, and treat
    `42P04` (duplicate_database) as success rather than an error.
  - `EnsureRole(ctx, name, password)` — same pattern against `pg_roles`, treating `42710`
    as success.
  - `GrantPrivileges(ctx, db, role)` — grant connect and usage; safe to repeat.
  - `VerifyConnectivity(ctx, db, role, password)` — opens a **new** connection as the role
    and runs `select 1`. Implemented here, wired up in phase 6.
  - `DropDatabase`, `DropRole` for compensation, each tolerating "does not exist".
- `internal/provisioner/vault.go`: `PutCredential(ctx, envID, user, password)` writing to
  the deterministic path `secret/data/environments/<envID>` so a retry overwrites rather
  than duplicating; `ReadCredential` for verification; `DeleteCredential` for compensation.
- `internal/provisioner/executor.go`: an `Executor` implementing the worker interface,
  dispatching on `StepKind`, loading the environment, and recording the effect.
- Compensation: when a step fails permanently, walk completed steps in reverse and run
  the matching drop/delete. Compensations are themselves recorded as step rows so a crash
  mid-rollback resumes rather than leaving debris. Environment ends `rolled_back`.
- Resource naming derived from the environment ID, sanitized to a valid Postgres
  identifier and length-limited with a stable suffix so two environments can never collide.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] Running `EnsureDatabase` twice for the same name returns success both times and
      leaves exactly one database.
- [ ] Running `EnsureRole` twice leaves exactly one role and rotates the password only
      when asked to.
- [ ] A credential written twice for the same environment leaves exactly one Vault path.
- [ ] Compensation drops the database and role it created, and tolerates a resource that
      is already gone.
- [ ] Integration tests cover each step against the compose stack.
- [ ] Generated identifiers are distinct for two different environments and never exceed
      Postgres's 63-byte limit.

## Out of scope

- The readiness gate (phase 6), timeout handling (phase 7).
