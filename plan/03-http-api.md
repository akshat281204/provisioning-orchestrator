---
phase: 3
slug: http-api
title: /v1 API with atomic, idempotent creation
touches: internal/httpapi/, internal/app/, internal/domain/
verify: make verify
---

## Goal

A client can create an environment and watch it, over the `/v1` surface the design contract
defines. Creation is atomic: an environment can never exist without its initial workflow
state. Repeating a request with the same key returns the same environment.

## Changes

- `internal/httpapi/api.go`, mounted by `internal/app` under `/v1`.
- `POST /v1/environments`:
  - Body `{"tenantId": "...", "name": "...", "spec": {...}}`.
  - `Idempotency-Key` header required — `400` and no writes when missing or empty.
  - Inserts the environment and all `domain.Plan()` steps in **one transaction**.
  - `202 Accepted` with `{"environmentId", "status", "statusUrl"}`.
  - Duplicate key: `200` with the existing environment, no new rows.
- `GET /v1/environments/{id}`: `200` with status and **non-secret** connection metadata
  (`host`, `database`, `credentialRef` — never the secret itself). `404` on `ErrNotFound`.
- `GET /v1/environments/{id}/history`: redacted step timeline derived from `workflow_steps`
  (index, kind, state, attempts, timestamps, last_error). No secret material.
- `internal/httpapi/dto.go` for wire shapes so domain types stay transport-agnostic.
- Handler tests against an in-memory fake implementing the store interfaces. No database.
- Central error mapper producing `{"error": {"code", "message"}}`; `last_error` and
  internal errors must be scrubbed before they reach a response.

## Acceptance

- [ ] `make verify` passes with no database running.
- [ ] Two `POST`s with one `Idempotency-Key` create exactly one environment and one step
      plan; the second returns `200`, not `202`.
- [ ] Ten concurrent `POST`s with one key create exactly one environment.
- [ ] A `POST` with no `Idempotency-Key` returns `400` and writes nothing.
- [ ] No response body, at any status, contains a password, token, or Vault secret value.
- [ ] `GET` of an unknown id returns `404` with a JSON error body.
- [ ] The history endpoint never returns `metadata` verbatim — only the redacted fields.

## Out of scope

- `cancel`, `delete`, `rotate` — they need durable cleanup and land in phases 6 and 7.
