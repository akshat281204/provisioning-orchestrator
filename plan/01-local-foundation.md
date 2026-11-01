---
phase: 1
slug: local-foundation
title: Dependency-aware readiness, logging, and lint in CI
touches: internal/health/, internal/app/, internal/config/, cmd/orchestrator/, .github/workflows/ci.yml, go.mod, Makefile
verify: make verify
---

## Goal

The service tells the truth about whether it can work. Today `/readyz` returns 200
unconditionally, which is a lie the moment Postgres is unreachable. After this phase a
clean checkout starts from documented commands, and readiness reflects real dependencies.

## Changes

- Add `github.com/jackc/pgx/v5` and `github.com/hashicorp/vault/api` to `go.mod`.
- `internal/health/health.go`:
  - `Checker` interface `Check(ctx) error`, each with a name.
  - `Registry` running all checkers concurrently with a per-check timeout, returning a
    per-dependency result map.
  - Postgres checker: `pool.Ping`.
  - Vault checker: `Sys().Health()` plus a token lookup.
- `internal/app`: construct the pgx pool and Vault client at startup, register their
  checkers, close both on shutdown. `/readyz` returns `503` naming the failing dependency
  when any check fails, and `200` only when all pass. `/healthz` stays a liveness check that
  never touches dependencies.
- `internal/config`: add `DEPENDENCY_CHECK_TIMEOUT` and `SHUTDOWN_TIMEOUT`, both validated.
- Request logging middleware: method, path, status, duration, request id. It must never log
  request bodies or headers — `Idempotency-Key` is fine, `Authorization` is not.
- CI: add a `golangci-lint` step using the existing `.golangci.yml`.
- `Makefile`: add a `lint` target and include it in `verify`.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] `/readyz` returns `503` and names `postgres` when the database is unreachable.
- [ ] `/readyz` returns `200` only when every registered dependency passes.
- [ ] `/healthz` returns `200` regardless of dependency state.
- [ ] The service starts and shuts down cleanly with no dependencies running; it does not
      crash-loop.
- [ ] CI runs lint and fails on a deliberate lint violation.
- [ ] Logs from a request carrying an `Authorization` header contain no credential.

## Out of scope

- The schema and migrations (phase 2). Readiness checks connectivity only, not schema
  version.
