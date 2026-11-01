---
phase: 5
slug: provisioning-adapters
title: Capacity, database, role, and secrets adapters
touches: internal/adapters/, internal/worker/, internal/domain/, migrations/, go.mod
verify: make verify
---

## Goal

Steps do real work through narrow, replaceable adapters. Every external operation is
idempotent or queryable, because it will be retried after crashes.

## Changes

- `internal/adapters/` with one small interface per concern, so a later phase can swap the
  local implementation without touching workflow semantics:
  - `Capacity` — `Reserve(ctx, envID) error`, `Release(ctx, envID) error`. Local
    implementation tracks reservations in the database with a bounded limit, so two
    concurrent requests cannot both take the last slot.
  - `Databases` — `Ensure(ctx, name)`, `Exists(ctx, name)`, `Drop(ctx, name)`.
  - `Roles` — `Ensure(ctx, name, password)`, `Exists`, `GrantConnect`, `GrantUsage`,
    `Drop`.
  - `Secrets` — `Put(ctx, envID, payload)`, `Get`, `Delete`. Local implementation on Vault;
    the interface must not leak Vault-specific types.
- Idempotency per operation: check-then-create, treating `42P04` (duplicate_database) and
  `42710` (duplicate_object) as success. Grants are safe to repeat.
- Deterministic naming from the environment ID: sanitized, lowercased, length-limited with
  a stable suffix, well under Postgres's 63-byte identifier limit, and collision-free
  between environments.
- `operation_key` recorded on each step so a reopened step re-derives the same external
  resource name rather than inventing a new one.
- Least privilege: the provisioned role gets connect and usage on its own database and
  nothing else. No superuser, no `CREATEROLE`, no access to other databases.
- Audit: log the action and identifiers, never the password.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] `Ensure` twice leaves exactly one database, and exactly one role, and returns success
      both times.
- [ ] `Secrets.Put` twice for one environment leaves exactly one path and one value.
- [ ] Generated names are distinct across environments and never exceed 63 bytes.
- [ ] The provisioned role cannot connect to a database other than its own.
- [ ] Two concurrent `Capacity.Reserve` calls at the limit: exactly one succeeds.
- [ ] No adapter logs or persists a password.
- [ ] Integration tests cover every adapter against the compose stack.

## Out of scope

- Verification gate and credential retrieval authorization (phase 6). Ambiguity handling
  (phase 7).
