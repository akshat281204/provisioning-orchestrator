# Design contract

This is the normative contract for the service. `docs/architecture.md` is the narrative
overview; if it differs from this document, this document wins. State names and step kinds
are persisted API: changing them requires an explicit migration and compatibility decision.

## Workflow and execution guarantees

An environment has one ordered forward workflow:

1. `reserve_capacity`
2. `create_database`
3. `store_credentials`
4. `create_role`
5. `grant_privileges`
6. `verify_usable`

Each step is a durable `workflow_steps` row created atomically with its environment. Its
1-based `step_index` and `kind` never change. A step can be claimed only after all earlier
forward steps are `done`. `operation_key` is the stable string
`env/<environment UUID>/step/<step_index>/<kind>`; it is assigned when the plan is made,
never changes on retry, and is the input to deterministic external resource naming. It
contains no credential or client-supplied secret.

External effects and Postgres commits cannot be one atomic transaction. Work is therefore
**at least once, never exactly once**: a process may repeat an operation after a crash or
lost acknowledgement. Every operation must be idempotent or reconcilable before it is
eligible for execution. The database records durable intent and progress; it does not claim
that an external effect happened merely because a worker held it in memory. An expired
`running` lease is ambiguous and is reconciled, not blindly reset to `pending`.

## State model

All state changes are validated against the transition lists below and committed before
being acted upon. An illegal transition is rejected with an error naming the source and
target; it writes no state and triggers no external side effect. Leases are coordination
fields, not states. A step's `attempts` counts claims/executions and is retained on that
step rather than in a separate attempt record.

### Environment states

| State | Entry condition | Exit condition |
| --- | --- | --- |
| `pending` | Environment and complete forward plan were inserted atomically; no forward work has started. | First claim starts `provisioning`; an accepted cancellation or deletion starts its cleanup state. |
| `provisioning` | Forward work is eligible; a worker may claim the earliest unfinished step. | All forward steps complete (`verifying`); an uncertain outcome (`in_doubt`); definitive or retry-exhausted forward failure (`rolling_back`); cancellation (`cancelling`); deletion (`deleting`). Forward failure enters rollback so already-created resources are cleaned up; it does not transition directly to `failed`. |
| `in_doubt` | A forward or cleanup step timed out, lost its worker/lease, or otherwise has an unknown external outcome. The corresponding step is `in_doubt` and excluded from ordinary claims. | If cleanup intent exists, reconciliation resumes that path (`rolling_back`, `cancelling`, or `deleting`). Otherwise ordinary forward work returns to `provisioning`; a `verify_usable` step with a fresh successful check goes to `ready`, or a retryable/non-ready check goes to `verifying`. An unresolvable forward conflict first records rollback intent; conflicts or exhausted cleanup retries enter `failed` with the intended cleanup still durable. |
| `verifying` | All setup steps are done and the readiness check is the only remaining forward step. | Successful fresh connection and query (`ready`); uncertain result (`in_doubt`); definitive failure (`rolling_back`); accepted cancellation/deletion (`cancelling`/`deleting`). |
| `ready` | The verification step completed successfully and was committed. | Client deletion or expiry begins cleanup (`deleting`). Cancellation is not permitted once ready; a cancel request returns `409`, and the client must use deletion. It is never set optimistically. |
| `rolling_back` | Provisioning or verification failed permanently and compensation is running. | All compensation succeeds (`rolled_back`); ambiguous cleanup operation (`in_doubt`); exhausted cleanup retries (`failed`). |
| `rolled_back` | All compensation for a failed provisioning attempt is complete. | Terminal. |
| `cancelling` | Cancellation was accepted; no new forward steps may be claimed and in-flight work is allowed to finish or become uncertain before cleanup. | Cleanup completes (`cancelled`); ambiguous cleanup (`in_doubt`); exhausted cleanup retries (`failed`). |
| `cancelled` | Cancellation cleanup completed and no managed resource remains. | Terminal. |
| `deleting` | Deletion was accepted; durable cleanup steps are being executed or retried. | Every cleanup step is done (`deleted`); ambiguous cleanup (`in_doubt`); exhausted cleanup retries (`failed`). |
| `deleted` | Every required deletion step is durably `done`. | Terminal. An environment is never reported `deleted` while cleanup is pending, failed, or in doubt. |
| `failed` | A non-reconcilable mismatch or exhausted cleanup retry budget needs operator attention. Forward failures first enter `rolling_back`; if that cleanup itself cannot safely complete or exhausts retries, the environment enters `failed`. Any cleanup intent and incomplete cleanup rows remain durable and visible; this is not equivalent to deletion. | An operator resumes the recorded workflow into its intended cleanup state (`rolling_back`, `cancelling`, or `deleting`), after which normal cleanup rules apply. |

The complete legal environment transition set is:

```
pending      -> provisioning | cancelling | deleting
provisioning -> verifying | in_doubt | rolling_back | cancelling | deleting
in_doubt     -> provisioning | verifying | ready | rolling_back | cancelling | deleting | failed
verifying    -> ready | in_doubt | rolling_back | cancelling | deleting
ready        -> deleting
rolling_back -> in_doubt | rolled_back | failed
cancelling   -> in_doubt | cancelled | failed
deleting     -> in_doubt | deleted | failed
failed       -> rolling_back | cancelling | deleting
rolled_back, cancelled, deleted -> (no transitions)
```

The cleanup intent (rollback, cancellation, or deletion) is durable in the cleanup step
plan, so an `in_doubt` reconciliation or operator resume returns to the right path. A
cancel request is accepted only while an environment is `pending`, `provisioning`,
`in_doubt`, or `verifying`; it stops forward scheduling and does not claim that in-flight
external work was cancelled. A `ready` environment is delete-only: cancellation returns
`409`, and deletion starts the `deleting` cleanup path. Deleting a ready environment is not
the same as rolling back a failed provision.

### Step states

| State | Entry condition | Exit condition |
| --- | --- | --- |
| `pending` | Planned but not currently claimed, or reconciliation proved that retry is safe. | A worker claims it (`running`). |
| `running` | A worker claimed the step and acquired a lease. | External effect is confirmed (`done`); a definitive error is recorded (`failed`); timeout, process loss, or expired lease makes the outcome unknown (`in_doubt`). |
| `in_doubt` | An external call or worker ended without a trustworthy outcome. | Reconciliation confirms effect (`done`), proves it absent (`pending`), or finds a conflicting state (`failed`). |
| `done` | The effect was confirmed and its result committed. | On rollback, its corresponding cleanup succeeds (`compensated`); otherwise terminal for this workflow. |
| `failed` | A permanent error, conflict, or exhausted retry policy was recorded. | A retryable transient failure is scheduled (`pending`); a cleanup failure remains failed until retry/operator action. |
| `compensated` | The cleanup corresponding to a completed forward step succeeded. | Terminal. |

The complete legal step transition set is:

```
pending    -> running
running    -> done | failed | in_doubt
in_doubt   -> done | pending | failed
failed     -> pending
done       -> compensated
compensated -> (no transitions)
```

Cleanup runs in the durable order `drop_database`, `drop_role`, `delete_secret`,
`release_capacity`, skipping actions for effects that were never created. The database is
dropped before its owner role to avoid attempting to remove a role that still owns it; the
secret is retained until database and role cleanup are complete, and capacity is released
last so the slot is not advertised as free while managed resources remain. Each cleanup
action is itself a durable workflow step with its own `operation_key`, lease, attempt count,
and state. A cleanup-action row follows `pending`/`running`/`in_doubt`/`failed`/`done`;
`done` is terminal for that cleanup row. Cleanup rows do not erase forward history;
completed forward steps are marked `compensated` only after their corresponding effect has
actually been removed.

## Retry, timeout, and reconciliation policy

Only classified transient errors are retried, with exponential backoff plus jitter and a
bounded attempt/deadline policy. Backoff is durable: eligibility is derived from the
persisted `updated_at` and `attempts`; jitter is deterministic from the operation key and
attempt number, so no worker-local timer or extra retry table is needed. Permanent
validation, authorization, authentication, and resource-conflict errors are not retried. A
failed step may return to `pending` only when the error is classified transient, its retry
budget remains, and the backoff deadline has elapsed. Exhaustion leaves a durable `failed`
step/environment for operator attention; it never silently drops work.

A step timeout is not proof of failure. It moves the step to `in_doubt`, not `failed` or
`pending`; a worker crash with an expired lease has the same treatment. The reconciler
inspects reality before choosing `done`, `pending`, or `failed`. It processes existing
`in_doubt` rows at startup and periodically. Cleanup retries follow the same durable policy;
deletion is reported only when every cleanup step is `done`.

| Step kind | Reconciliation check and action |
| --- | --- |
| `reserve_capacity` | The reservation is represented by the environment's `reserve_capacity` step state, not a separate resource row. `done` means the capacity transaction reserved the slot; `pending` means absent; an inconsistent state is `failed`. `release_capacity` atomically marks the reservation released; an already released slot is cleanup `done`. |
| `create_database` | Inspect the deterministic database name and expected owner/attributes plus its non-secret environment ownership marker. Present and matching: `done`; absent: `pending`; present but conflicting: `failed` without overwriting it. A duplicate-database response is success only after the match check. |
| `store_credentials` | Read the deterministic, environment-owned Vault path. Present with the expected environment/operation marker: `done`; absent: `pending`; marker conflict: `failed`. Credential generation uses put-if-absent so a retry never replaces the intended value; compare values only in memory if needed and never log them. |
| `create_role` | Inspect the deterministic role, its non-secret environment ownership marker, and required safe attributes (login, not superuser, no role/database creation). Matching role and successful authentication with the Vault credential: `done`; absent: `pending`; owned role with old password: `pending` to apply the same desired credential; conflicting marker/attributes: `failed`. The desired credential comes from Vault, never step metadata. |
| `grant_privileges` | Inspect effective required grants. All expected grants and no forbidden access: `done`; required grants absent: `pending` for idempotent reapplication; unexpected privileges: `failed` for explicit correction. |
| `verify_usable` | Open a fresh connection with the Vault credential and execute the readiness query. Success: step `done`, environment `ready`; a definite transient connectivity error is retried within its bound, with the environment in `verifying`; authentication/authorization failure is permanent and enters rollback. An interrupted/timeout result leaves the step and environment `in_doubt`. Without a competing cleanup intent, reconciliation success resolves the step to `done` and environment directly to `ready`; a definite transient/non-ready result resolves the step to `pending` and environment to `verifying` for a fresh attempt; a definite permanent result resolves the step to `failed` and environment to `rolling_back`. A durable cancellation/deletion/rollback intent takes precedence over the check result and resumes its cleanup path. |
| `rotate_credentials` | Reuse the same durably staged candidate from Vault on every attempt. Confirm the role accepts it with a fresh connection, then confirm the active Vault version; verified: `done`; no candidate: `pending`; conflicting role/secret state: `failed`. Never generate a replacement candidate during reconciliation. |
| `drop_role` | Inspect the deterministic role. Absent: cleanup `done`; present and owned by this environment: retry drop; ownership conflict: `failed` without deleting another actor's role. |
| `drop_database` | Inspect the deterministic database. Absent: cleanup `done`; present and owned by this environment: retry drop; ownership conflict: `failed` without deleting it. |
| `delete_secret` | Read the deterministic Vault path. Absent: cleanup `done`; present: retry delete and confirm absence. |
| `release_capacity` | Check the original `reserve_capacity` row. Already `compensated`: cleanup `done`; still reserved: atomically mark it `compensated` and finish cleanup. |

All external names and secret paths are derived from the immutable environment ID and
operation key. Ensure/create operations treat a duplicate result as success only after
checking that the existing resource matches this environment's intent. Destructive cleanup
must verify ownership before acting.

## Authorization model

The API requires an authenticated caller identity and tenant context (the initial
implementation may use an explicit in-process policy stub, not an implicit allow-all
contract). A caller may create only in a tenant for which it has create permission; read
status/history only for an environment in an authorized tenant; cancel or delete only
where it has the corresponding environment-management permission. Credential retrieval
requires a separate explicit `credentials:read` permission for that environment and is
audited with caller identity on every successful or denied attempt. Status/history
authorization does not confer credential access. Rotation requires a separate credential
management permission. Unauthenticated or cross-tenant requests are denied without
revealing whether an environment exists.

Credentials are never returned on the status or history path, regardless of caller
permissions. Only the credential-retrieval endpoint may return credential material.

## Secret handling and redaction

Credential payloads live only in Vault under a deterministic per-environment path. A
database row may hold the non-secret `credentialRef`, never a password/token or a reversible
encoding of one. Do not put credentials in request/response logs, trace attributes, metric
labels, error text, panic messages, step `last_error`, `spec`, or persisted `metadata`.
`metadata` is limited to non-secret identifiers and compact redacted audit facts. Error
messages exposed to callers use safe classifications, not raw adapter/Vault/database text.
The initial credential is generated once and durably written with put-if-absent before it is
applied to the database role. Rotation likewise stages one candidate durably before changing
the role. A retry reads and reuses the staged value; a process-local password is never the
only copy of an intended external change.

Logs and spans use an allowlist of operation, environment ID, step index/kind, state,
attempt, and duration. They do not record request bodies, authorization headers, Vault
responses, connection strings, or credential-bearing errors. A single redaction boundary
must scrub externally supplied error strings before persistence, logging, tracing, or HTTP
responses; structured outputs are allowlisted rather than relying only on string matching.
Tests use a known secret sentinel to prove it is absent from all those surfaces. Credential
retrieval responses are the sole intentional exception and must not be logged or traced.

## Persistence contract

The domain schema deliberately has two tables: `environments` and `workflow_steps`.
Typed columns are used where values participate in identity, joins, constraints, ordering,
leases, retrying, filtering, or timestamps. JSONB is used only for intentionally variable
input or compact non-secret metadata.

| Table / field | Representation and reason |
| --- | --- |
| `environments.id` | UUID typed primary key: stable identity and foreign-key target. |
| `tenant_id`, `name` | Text columns: tenant scoping, validation, and direct lookup/filtering are part of the API contract. |
| `state` | Text column constrained to the environment state enum: indexed/queryable workflow control and database-enforced valid values. |
| `idempotency_key` | Text column with a unique constraint together with `tenant_id`: atomic duplicate-request protection. |
| `created_at`, `updated_at`, `expires_at` | Timestamp columns: sorting, lease/expiry scheduling, and time comparisons need typed semantics. `expires_at` is nullable. |
| `spec` | JSONB: caller-defined, evolving provisioning parameters are a structured document not used as workflow identity or a secret store. Validate its allowed keys and reject credentials. |
| `metadata` | JSONB: compact, redacted audit/context fields can evolve without a migration; never use it for secrets or authoritative state. |
| `workflow_steps.id`, `environment_id` | UUID columns with primary/foreign-key constraints: durable step identity and indexed environment join. |
| `step_index` | Integer with unique `(environment_id, step_index)`: ordered execution and uniqueness are relational invariants. |
| `kind`, `state` | Constrained text columns: dispatch and state filtering require direct indexed/queryable values. |
| `attempts` | Integer: retry count is incremented and compared atomically. |
| `lease_owner`, `lease_expires_at` | Nullable text/timestamp columns: workers claim and reclaim work by typed lease predicates. |
| `operation_key` | Text column: immutable deterministic external-operation identity, directly available to workers/reconciliation. |
| `metadata` | JSONB: compact per-step redacted details may evolve; execution state remains in typed columns. |
| `last_error` | Nullable text: one sanitized, bounded diagnostic for the current step, queryable without a separate attempt log; never raw secret-bearing adapter output. |
| step `created_at`, `updated_at` | Timestamp columns: work ordering and operational visibility. |

No domain table is created for:

* **step attempts** — `attempts`, current state, timestamps, and the sanitized current
  `last_error` live on the step row; old per-attempt diagnostics are intentionally not kept.
* **events** — compact redacted state-change audit facts live in environment `metadata`;
  the authoritative current state remains the typed state column.
* **resources** — names are deterministically derived from environment ID/operation key,
  and adapters inspect the external systems of record rather than duplicating their state.
* **compensation records** — cleanup is represented by ordinary durable workflow-step rows
  with cleanup kinds and order, not a separate compensation table.

Migration bookkeeping owned by the migration tool is not a domain table. Secrets are in
Vault, not in either domain table.

## `/v1` API contract

JSON request/response bodies use the field names below. All errors have the shape
`{"error":{"code":"...","message":"..."}}`; messages are safe and contain no raw
internal error or secret. All IDs in paths are environment UUIDs.

| Method and path | Request | Success | Other contract |
| --- | --- | --- | --- |
| `POST /v1/environments` | `Idempotency-Key` header required; body `{"tenantId":"…","name":"…","spec":{…}}`. | `202` with `{"environmentId":"…","status":"pending","statusUrl":"/v1/environments/…"}`. | Missing/empty key or invalid body: `400`; same tenant/key and same request duplicate: `200` with existing environment/status; same key with different request: `409`; unknown/unauthorized tenant: `403`. No duplicate plan is created. |
| `GET /v1/environments/{id}` | No body. | `200` with `{"environmentId":"…","tenantId":"…","name":"…","status":"…","host":"…","database":"…","credentialRef":"…","createdAt":"…","updatedAt":"…"}`. Not-yet-known connection fields may be omitted. | `404` if absent or not visible to caller. Never includes credential material. |
| `GET /v1/environments/{id}/history` | No body. | `200` with `{"environmentId":"…","steps":[{"stepIndex":1,"kind":"…","state":"…","attempts":0,"createdAt":"…","updatedAt":"…","lastError":null}]}`; only redacted fields are returned. | `404` if absent or not visible. Never returns raw `metadata` or credentials. |
| `POST /v1/environments/{id}/cancel` | No body. | `202` with `{"environmentId":"…","status":"cancelling","statusUrl":"/v1/environments/…"}` when accepted; repeated request is safe. | Accepted only for `pending`, `provisioning`, `in_doubt`, or `verifying`; `404` if absent/not visible; `200` with status `cancelled` if already cancelled; `409` if `ready`, otherwise terminal, or not cancellable. A ready environment must be deleted instead. |
| `DELETE /v1/environments/{id}` | No body. | `202` with `{"environmentId":"…","status":"deleting","statusUrl":"/v1/environments/…"}` when durable deletion cleanup starts; repeated delete is safe. | `404` if absent/not visible; `200` with `{"environmentId":"…","status":"deleted"}` if already deleted; `409` for an incompatible terminal state. Never returns `deleted` while cleanup is pending. |
| `POST /v1/environments/{id}/credentials` | No body. | `200` returns a `credential` object with `username`, `password`, `host`, `port`, and `database` fields to the explicitly authorized caller. This is a schema, not a sample containing a credential value. | `403` if caller lacks `credentials:read`; `404` if absent/not visible; `409` unless ready; `503` if Vault is unavailable. Every attempt is audited; responses are not logged. |
| `POST /v1/environments/{id}/rotate` | No body. | `202` with `{"environmentId":"…","status":"ready","statusUrl":"/v1/environments/…"}` while the durable `rotate_credentials` step completes. | `403` without credential-management permission; `404` if absent/not visible; `409` if not ready; transient dependency failure is retried under the workflow policy. |

All routes return `401` when the caller is unauthenticated. Cross-tenant and otherwise
invisible environment IDs use `404` rather than disclosing existence; `403` is used only
when the resource is visible but the requested action is not authorized.

The status representation is deliberately a non-secret projection; connection details may
be omitted until they exist. No example payload in this contract contains a secret value.
The operational `/healthz` and `/readyz` endpoints are not environment-management APIs.
