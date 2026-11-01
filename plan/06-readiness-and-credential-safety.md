---
phase: 6
slug: readiness-and-credential-safety
title: Readiness gate, credential retrieval, and rotation
touches: internal/adapters/, internal/httpapi/, internal/worker/, internal/domain/, internal/telemetry/
verify: make verify
---

## Goal

`ready` means proven usable, not "every call returned 200". And a credential is only ever
readable through a separately authorized path — never on status, history, logs, traces, or
error messages.

## Changes

- Verification step executes for real: read the credential through the secrets adapter,
  open a **fresh** connection to the provisioned database as the provisioned role, run
  `select 1` and a harmless catalog query to confirm grants are effective.
- Retry policy: transient connection errors retry with backoff to a bounded deadline; an
  authentication failure is permanent and fails immediately — retrying bad credentials
  never converges.
- The environment becomes `ready` only in the transaction that marks the verification step
  `done`. A direct transition into `ready` is rejected by the state machine, with a test
  that fails if it is ever allowed.
- `POST /v1/environments/{id}/credentials` — the authorized retrieval path. Returns the
  credential payload; requires an explicit authorization check that the status endpoints do
  not have. Every call is audited with the caller identity.
- `POST /v1/environments/{id}/rotate` — generate a new password, apply it via `Roles`, write
  it to the secrets adapter, and record rotation in step `metadata`. Rotation must be safe
  to retry: applying the same new password twice is a no-op.
- Redaction: a single `redact` helper applied to every error surfaced in a response, every
  log record, and every span attribute. Add a test that runs a provisioning with a known
  password sentinel and asserts it appears in no captured log, span, or response.

## Acceptance

- [ ] `make verify` passes.
- [ ] An environment whose role lacks grants never reaches `ready`.
- [ ] `ready` is unreachable except through a successful verification step.
- [ ] An authentication failure fails fast rather than exhausting the retry budget.
- [ ] The status and history endpoints return no secret, and a sentinel password appears in
      no log line, span attribute, or error body.
- [ ] Credential retrieval is denied for a caller lacking the permission, and audited when
      allowed.
- [ ] Rotation twice in a row leaves the environment usable and one stored credential.
- [ ] Integration test: provision end to end, then connect from outside the service using
      the returned credential and succeed.

## Out of scope

- Full authentication/authorization infrastructure — a minimal, explicit policy check is
  enough; document the limitation.
