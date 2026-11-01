---
phase: 6
slug: readiness-gate
title: Verification gate before an environment is ready
touches: internal/provisioner/, internal/worker/, internal/domain/
verify: make verify
---

## Goal

An environment is reported `ready` only after it has been proven usable by connecting to it
as the provisioned role with the provisioned credential, the same path a consumer would
take. No optimistic readiness.

## Changes

- `verify_connectivity` step executes for real: read the credential back from Vault, open a
  fresh connection to the provisioned database as the provisioned role, and run
  `select 1` plus a trivial read of the catalog to confirm grants are effective.
- Retry policy for verification: transient connection errors retry with backoff up to a
  bounded deadline; an authentication failure is permanent and fails immediately, because
  retrying bad credentials never converges.
- The environment transitions to `ready` only after the verification step is `done`.
  The worker sets it in the same transaction that marks the final step done.
- A failed verification moves the environment to `rolling_back`.
- Surface the verification result on `GET /environments/{id}`, so a client can tell
  "provisioned" from "proven usable".

## Acceptance

- [ ] `make verify` passes.
- [ ] An environment whose database is reachable but whose role lacks grants does not
      become `ready`.
- [ ] `ready` is unreachable except through a successful verification step — asserted by a
      test that drives the state machine directly and fails if the transition is allowed.
- [ ] An authentication failure fails fast rather than exhausting the retry budget.
- [ ] Integration test: provision end to end, then connect with the returned credentials
      from outside the service and succeed.

## Out of scope

- Timeout ambiguity semantics (phase 7).
