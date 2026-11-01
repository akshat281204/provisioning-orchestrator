---
phase: 3
slug: http-api
title: HTTP API and idempotent environment creation
touches: internal/httpapi/, internal/app/, cmd/orchestrator/
verify: make verify
---

## Goal

A client can create an environment and read its status over HTTP. Creating twice with the
same `Idempotency-Key` returns the same environment instead of provisioning a second one.

## Changes

- `internal/httpapi/api.go`: a `Handler(stores)` constructor returning `http.Handler`,
  mounted by `internal/app` alongside the existing health endpoints.
- `POST /environments`:
  - Body `{"name": "..."}`; `Idempotency-Key` header required, `400` if missing or empty.
  - Creates the environment as `pending` and inserts its steps from `domain.Plan()` in a
    **single transaction** — an environment without steps is a corrupt row.
  - `201` with the created environment; on a duplicate idempotency key, `200` with the
    existing environment and no new rows.
- `GET /environments/{id}`: `200` with the environment and its steps, `404` on
  `store.ErrNotFound`.
- `GET /environments`: list, newest first, with a `limit` query param (default 50, max
  200).
- JSON shapes live in `internal/httpapi/dto.go` so domain types stay transport-agnostic.
- Handler tests using an in-memory fake implementing the store interfaces. No database.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] Two `POST`s with the same `Idempotency-Key` produce exactly one environment and
      one set of steps, and the second response is `200`, not `201`.
- [ ] A `POST` without the header returns `400` and creates nothing.
- [ ] A created environment has exactly the steps `domain.Plan()` specifies, in order.
- [ ] `GET` of an unknown id returns `404` with a JSON body, not a bare status.
- [ ] `limit` above 200 is clamped, not rejected.

## Out of scope

- Auth, pagination cursors, PATCH, deletion.
