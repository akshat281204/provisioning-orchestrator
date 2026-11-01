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
1-based `step_index` and `kind` never change, and every row records an immutable
`direction` — `forward` or `cleanup` — assigned when the row is planned. Forward rows are
ordered against forward rows and cleanup rows against cleanup rows; the claiming rules
built on that split are specified in *Claiming and ordering* below. `operation_key` is the
stable string `env/<environment UUID>/step/<step_index>/<kind>`; it is assigned when the
plan is made, never changes on retry, and is the input to deterministic external resource
naming. It contains no credential or client-supplied secret.

### Ownership and privilege model

The adapters connect to the target Postgres and to Vault with a configured administrative
identity. Every provisioned database is created by that administrative role and remains
owned by it for the environment's whole lifetime — no step ever transfers database
ownership to the provisioned role. The provisioned role is created after the database
exists, holds only the configured least-privilege grants — connect on the environment's
database, and usage and create on its working schema — and is never a superuser and can
never create roles or databases. This is why `create_database` precedes `create_role`, why
`grant_privileges` is a separate step that grants the role's access explicitly, and why the
`create_database` reconciliation check below has a well-defined expected owner. Each
database and role carries a non-secret environment ownership marker derived from the
environment ID, so reconciliation and destructive cleanup can prove a resource belongs to
this environment before acting on it.

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
| `pending` | Environment and complete forward plan were inserted atomically; no forward work has started. | The first forward claim atomically starts `provisioning`; an accepted cancellation or deletion starts its cleanup state. |
| `provisioning` | Forward work is eligible; a worker may claim the earliest unfinished step. | All setup steps complete (`verifying`); an uncertain outcome (`in_doubt`); definite, conflicting, or retry-exhausted forward failure (`rolling_back`); cancellation (`cancelling`); deletion (`deleting`). Forward failure enters rollback so already-created resources are cleaned up; it does not transition directly to `failed`. |
| `in_doubt` | A forward or cleanup step — including a rotation step — timed out, lost its worker/lease, or otherwise has an unknown external outcome. The corresponding step is `in_doubt` and excluded from ordinary claims. | Reconciliation resolves the step first, and the environment follows by step kind. **A forward step with a recorded cleanup intent:** the intent takes precedence over the step's outcome; the environment resumes that path (`rolling_back`, `cancelling`, or `deleting`) and the cleanup plan removes whatever debris the step left. **A provisioning-plan step, no cleanup intent:** proven absent or safe to retry — `provisioning` (or `verifying` when the readiness check is the only remaining forward step); a fresh successful readiness check — `ready`; an unresolvable conflict or exhausted retry budget — `rolling_back`, never `failed` directly. **A rotation step, no cleanup intent:** a fresh successful check with the new credential — `ready`; safe to retry with the staged candidate — `rotating`; an unresolvable role/secret conflict, or a pair that cannot be restored and re-verified — `failed` directly, because rotation has no compensating cleanup. **A cleanup step:** resolved (present-and-matching or absent) — resume that cleanup state; an unresolvable conflict or exhausted cleanup budget — `failed`, because cleanup cannot safely complete. |
| `verifying` | All setup steps are `done` — the transition is committed in the transaction that completes the last setup step — and the readiness check is the only remaining forward step. | Successful fresh connection and query (`ready`); uncertain result (`in_doubt`); definite failure or unresolvable conflict (`rolling_back`); accepted cancellation/deletion (`cancelling`/`deleting`). |
| `ready` | The verification step completed successfully and was committed. | An accepted rotate request begins rotation (`rotating`); client deletion or expiry begins cleanup (`deleting`). Cancellation is not permitted once ready; a cancel request returns `409`, and the client must use deletion. It is never set optimistically. |
| `rotating` | A rotate request was accepted from `ready`, or an operator explicitly resumed rotation on a rotation-`failed` environment: the `rotating` transition and exactly one outstanding `rotate_credentials` row are committed atomically. | Rotation verified — a fresh connection with the new credential and a confirmed active secret value (`ready`); a failed rotation whose pre-rotation pair was restored and re-verified with a fresh connection (`ready`, with the failed step durably recorded); an uncertain outcome (`in_doubt`); a failure that cannot restore the pair (`failed`); accepted deletion (`deleting`). Retryable transient failures keep the environment in `rotating` while the step retries under the standard policy. |
| `rolling_back` | Provisioning or verification failed permanently — a definite failure, an unresolvable conflict, or an exhausted retry budget — and compensation is planned. | All compensation succeeds (`rolled_back`); ambiguous cleanup operation (`in_doubt`); exhausted cleanup retries (`failed`). |
| `rolled_back` | All compensation for a failed provisioning attempt is complete. | Terminal. |
| `cancelling` | Cancellation was accepted; no new forward steps may be claimed and in-flight work is allowed to finish or become uncertain before cleanup. | Cleanup completes (`cancelled`); ambiguous cleanup (`in_doubt`); exhausted cleanup retries (`failed`). |
| `cancelled` | Cancellation cleanup completed and no managed resource remains. | Terminal. |
| `deleting` | Deletion was accepted; durable cleanup steps are being executed or retried. | Every cleanup step is done (`deleted`); ambiguous cleanup (`in_doubt`); exhausted cleanup retries (`failed`). |
| `deleted` | Every required deletion step is durably `done`. | Terminal. An environment is never reported `deleted` while cleanup is pending, failed, or in doubt. |
| `failed` | Cleanup could not safely complete, or a failed rotation could not restore a consistent credential pair; either way the environment needs operator attention. Entered when a cleanup step hits an unresolvable conflict or exhausts its retry budget (from `rolling_back`, `cancelling`, `deleting`, or while `in_doubt`), or when rotation failure handling cannot restore and re-verify the pre-rotation pair (from `rotating` or while `in_doubt`). Provisioning failures never enter `failed` directly: they first enter `rolling_back`, and become `failed` only if that cleanup cannot safely complete. Any cleanup intent and incomplete cleanup rows remain durable and visible; this is not equivalent to deletion. | Deletion stays client-driven (`deleting`). Every other resume — continuing a recorded cleanup (`rolling_back` or `cancelling`) or re-running rotation (`rotating`) — is an explicit operator action; normal rules apply afterwards. |

The complete legal environment transition set is:

```
pending      -> provisioning | cancelling | deleting
provisioning -> verifying | in_doubt | rolling_back | cancelling | deleting
verifying    -> ready | in_doubt | rolling_back | cancelling | deleting
ready        -> rotating | deleting
rotating     -> ready | in_doubt | failed | deleting
in_doubt     -> provisioning | verifying | rotating | ready | rolling_back | cancelling | deleting | failed
rolling_back -> in_doubt | rolled_back | failed
cancelling   -> in_doubt | cancelled | failed
deleting     -> in_doubt | deleted | failed
failed       -> rolling_back | cancelling | deleting | rotating
rolled_back, cancelled, deleted -> (no transitions)
```

The cleanup intent (rollback, cancellation, or deletion) is durable in the cleanup step
plan, so an `in_doubt` reconciliation or operator resume returns to the right path;
rotation intent is likewise durable in the single outstanding rotation row. A cancel
request is accepted only while an environment is `pending`, `provisioning`, `in_doubt`,
or `verifying`; it stops forward scheduling and does not claim that in-flight external
work was cancelled. A `ready` environment supports rotation and deletion but not
cancellation: a cancel request returns `409`, and the client must use deletion. Deleting
a ready environment is not the same as rolling back a failed provision.

`expires_at` is set once at creation, from the request or the tenant default, and is
nullable (null never expires). A periodic maintenance pass — the same pass that
reconciles `in_doubt` rows and expired leases — moves a `ready` environment whose
`expires_at` has passed into `deleting` by recording deletion intent and its cleanup
plan, exactly as a client deletion would.

### Step states

| State | Entry condition | Exit condition |
| --- | --- | --- |
| `pending` | Planned but not currently claimed, or reconciliation proved that retry is safe. | A worker claims it (`running`). |
| `running` | A worker claimed the step and acquired a lease. | External effect is confirmed (`done`); a definitive error is recorded (`failed`); timeout, process loss, or expired lease makes the outcome unknown (`in_doubt`). |
| `in_doubt` | An external call or worker ended without a trustworthy outcome. | Reconciliation confirms effect (`done`), proves it absent (`pending`), or finds a conflicting state (`failed`). |
| `done` | The effect was confirmed and its result committed. | For a forward row: its corresponding cleanup succeeds on rollback, cancellation, or deletion (`compensated`); otherwise terminal for this workflow. For a cleanup row: terminal. |
| `failed` | A definitive error of any classification — transient or permanent — a conflict, or an exhausted retry policy was recorded. Only the subset that is classified transient with retry budget remaining exits to `pending`; permanent errors, conflicts, and exhaustion are `failed` terminally for that step (subject to cleanup/operator handling below). | A retryable transient failure with remaining budget is rescheduled (`pending`); every other failure remains `failed` until retry/operator action. |
| `compensated` | The cleanup corresponding to a completed forward step succeeded. | Terminal. |

The complete legal step transition set is:

```
pending    -> running
running    -> done | failed | in_doubt
in_doubt   -> done | pending | failed
failed     -> pending       # only a transient failure with retry budget remaining
done       -> compensated   # forward rows only; a cleanup row's done is terminal
compensated -> (no transitions)
```

Cleanup runs in the durable order `drop_database`, `drop_role`, `delete_secret`,
`release_capacity`, skipping actions for effects that were never created. The order
follows from the ownership model above, not from creation order — it is deliberately not
the reverse of the forward sequence. The provisioned role never owns the database, but it
holds grants on that database and, once the environment has been used, owns the objects
created inside it; Postgres refuses to drop a role that still owns objects or holds
privileges. Dropping the database first destroys those objects and grants in one act and
leaves the role droppable; dropping the role first would fail precisely when the
environment had served its purpose. The secret is retained until database and role
cleanup are complete: deleting it is irreversible, and until then an interrupted or
resumed cleanup — or an operator diagnosing a stuck one — can still read the
environment's credential. Capacity is released last so the slot is not advertised as
free while managed resources remain. Each cleanup action is itself a durable workflow
step with its own `operation_key`, lease, attempt count, and state. A cleanup-action row
follows `pending`/`running`/`in_doubt`/`failed`/`done`; `done` is terminal for that
cleanup row. Cleanup rows do not erase forward history; completed forward steps are
marked `compensated` only after their corresponding effect has actually been removed —
a `rotate_credentials` row, like `store_credentials`, is compensated by `delete_secret`.
How cleanup rows are ordered and claimed relative to forward rows is specified in
*Claiming and ordering* below.

### Claiming and ordering

A row is claimable only from `pending`; a claim sets `running`, takes a lease, and
increments `attempts`. `running` rows with unexpired leases and `in_doubt` rows are never
claimed as ordinary work — the reconciler owns them.

- **Forward claim.** A `forward` row is claimable only while its environment is in a
  forward state (`pending`, `provisioning`, `verifying`, `rotating`) and every
  lower-indexed forward row is `done`. `ready` is not a forward state: no row is
  claimable while `ready`, so a ready environment never has hidden outstanding work. In
  `rotating` the provisioning plan is complete and the only claimable forward row is the
  single outstanding `rotate_credentials` row, planned after the forward plan with the
  next `step_index`; rotation applies a new desired state, so it is forward work. At
  most one rotation row is outstanding at a time, because rotate is accepted only from
  `ready`.
- **Cleanup claim.** A `cleanup` row is claimable only while its environment is in a
  cleanup state (`rolling_back`, `cancelling`, `deleting`) and every lower-indexed cleanup
  row is `done`. The cleanup plan is inserted in the fixed order above, in one transaction
  with the cleanup intent, taking the next `step_index` values after all existing rows —
  so a cleanup row's order among cleanup rows is its `step_index` order. The full plan is
  recorded regardless of which effects exist; a row whose effect was never created resolves
  to `done` on its first execution without performing the external action. Recording
  cleanup intent is idempotent: a repeated or concurrent rollback, cancellation, or
  deletion reuses the existing rows — check, then create, and treat a uniqueness violation
  on that path as success, not as a duplicate plan.
- **Cleanup is independent of forward completion.** A forward row in `failed`, `pending`,
  `done`, or `compensated` never blocks a cleanup claim: a failed or never-run forward step
  is exactly the debris cleanup exists to remove. The only forward rows that hold cleanup
  back are ones with an unresolved outcome — a forward row that is `running` (its lease may
  still be live) or `in_doubt` blocks that environment's cleanup claims until it finishes
  or is reconciled to a definitive state, so destructive cleanup never races an external
  effect whose outcome is unknown. The block is temporary by design: the reconciler
  processes `in_doubt` rows and expired leases at startup and periodically.

A `ready` environment has no claimable rows at all. While an environment is `in_doubt`,
`failed`, or terminal, no row is ordinarily claimed; reconciliation or operator resume
re-enters the workflow first. The claim query in `docs/architecture.md` illustrates this
predicate; the rules in this section are normative.

## Retry, timeout, and reconciliation policy

Only classified transient errors are retried, with exponential backoff plus jitter and a
bounded attempt/deadline policy. Backoff is durable: eligibility is derived from the
persisted `updated_at` and `attempts`; jitter is deterministic from the operation key and
attempt number, so no worker-local timer or extra retry table is needed. Permanent
validation, authorization, authentication, and resource-conflict errors are not retried. A
failed step may return to `pending` only when the error is classified transient, its retry
budget remains, and the backoff deadline has elapsed. Exhaustion never silently drops work:
it leaves a durable `failed` step, and the environment then follows that step kind's
failure path — `rolling_back` for a provisioning-plan step, in-place restoration for a
rotation step — and reaches `failed` itself only when cleanup cannot safely complete or a
rotation cannot restore a consistent credential pair, as defined in *State model*.

A step timeout is not proof of failure. It moves the step to `in_doubt`, not `failed` or
`pending`; a worker crash with an expired lease has the same treatment. The reconciler
inspects reality before choosing `done`, `pending`, or `failed`. It processes existing
`in_doubt` rows at startup and periodically. Cleanup retries follow the same durable policy;
deletion is reported only when every cleanup step is `done`.

| Step kind | Reconciliation check and action |
| --- | --- |
| `reserve_capacity` | The reservation is represented by the environment's `reserve_capacity` step state, not a separate resource row. `done` means the capacity transaction reserved the slot; `pending` means absent; an inconsistent state is `failed`. `release_capacity` atomically marks the reservation released; an already released slot is cleanup `done`. |
| `create_database` | Inspect the deterministic database name, its expected owner (the administrative role) and attributes, and its non-secret environment ownership marker. Present and matching: `done`; absent: `pending`; present but conflicting: `failed` without overwriting it. A duplicate-database response is success only after the match check. |
| `store_credentials` | Read the deterministic, environment-owned Vault path. Present with the expected environment/operation marker: `done`; absent: `pending`; marker conflict: `failed`. Credential generation uses put-if-absent so a retry never replaces the intended value; compare values only in memory if needed and never log them. |
| `create_role` | Inspect the deterministic role, its non-secret environment ownership marker, and required safe attributes (login, not superuser, no role/database creation). Matching role and successful authentication with the Vault credential: `done`; absent: `pending`; owned role with old password: `pending` to apply the same desired credential; conflicting marker/attributes: `failed`. The desired credential comes from Vault, never step metadata. |
| `grant_privileges` | Inspect effective required grants. All expected grants and no forbidden access: `done`; required grants absent: `pending` for idempotent reapplication; unexpected privileges: `failed` for explicit correction. |
| `verify_usable` | Open a fresh connection with the Vault credential and execute the readiness query. Success: step `done`, environment `ready`; a definite transient connectivity error is retried within its bound, with the environment in `verifying`; authentication/authorization failure is permanent and enters rollback. An interrupted/timeout result leaves the step and environment `in_doubt`. Without a competing cleanup intent, reconciliation success resolves the step to `done` and environment directly to `ready`; a definite transient/non-ready result resolves the step to `pending` and environment to `verifying` for a fresh attempt; a definite permanent result resolves the step to `failed` and environment to `rolling_back`. A durable cancellation/deletion/rollback intent takes precedence over the check result and resumes its cleanup path. |
| `rotate_credentials` | Reuse the same durably staged candidate on every attempt: one candidate per rotation, staged at the environment's deterministic rotation path and marked with this rotation's `operation_key`, while the active credential stays at the `credential_ref` path. Confirm the role accepts the candidate with a fresh connection, then confirm the active value matches it. Verified: step `done`, environment `ready`. Role still on the pre-rotation credential with the pair otherwise consistent: retry under the standard policy (step `pending`, environment `rotating`). Conflicting role/secret state — the role accepts neither the candidate nor the active credential, or a staged candidate is marked for a different rotation: step `failed`; then attempt to restore the pre-rotation pair and re-verify it with a fresh connection — restored: environment `ready`; unrestorable: environment `failed`. Never generate a replacement candidate during reconciliation. |
| `drop_role` | Inspect the deterministic role. Absent: cleanup `done`; present and owned by this environment: retry drop; ownership conflict: `failed` without deleting another actor's role. |
| `drop_database` | Inspect the deterministic database. Absent: cleanup `done`; present and owned by this environment: retry drop; ownership conflict: `failed` without deleting it. |
| `delete_secret` | Read the deterministic Vault path. Absent: cleanup `done`; present: retry delete and confirm absence. |
| `release_capacity` | Check the original `reserve_capacity` row. Already `compensated`: cleanup `done`; still reserved: atomically mark it `compensated` and finish cleanup. |

All external names and secret paths are derived from the immutable environment ID and
operation key; per environment there is exactly one active secret path (`credential_ref`)
and one rotation-candidate path. Ensure/create operations treat a duplicate result as
success only after checking that the existing resource matches this environment's intent.
Destructive cleanup must verify the environment ownership marker before acting.

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

Credential payloads live only in Vault under a deterministic per-environment path. The only
credential-related value in the database is the non-secret locator persisted in
`environments.credential_ref` and returned as `credentialRef` — never a password, token, or
a reversible encoding of one. Do not put credentials in request/response logs, trace
attributes, metric labels, error text, panic messages, step `last_error`, `spec`, or
persisted `metadata`.
`metadata` is limited to non-secret identifiers and compact redacted audit facts. Error
messages exposed to callers use safe classifications, not raw adapter/Vault/database text.
The initial credential is generated once and durably written with put-if-absent before it is
applied to the database role. Rotation likewise stages exactly one candidate durably —
refusing to overwrite a candidate marked for a different rotation — before changing the
role, and promotes it to the active path only after a fresh connection with the candidate
succeeds, so the pre-rotation credential stays readable at the active path until the pair
is consistent on the new value. A retry reads and reuses the staged value; a process-local
password is never the only copy of an intended external change.

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
| `credential_ref` | Non-secret text column on `environments`: the deterministic Vault locator for the environment's credential, resolved once when the environment row is created and returned as `credentialRef` on the status path. A locator holds no secret material, so persisting it in a typed column is safe, keeps the read path from re-deriving the path on every request, and keeps the reference stable if the path scheme is ever versioned. |
| `created_at`, `updated_at`, `expires_at` | Timestamp columns: sorting, lease/expiry scheduling, and time comparisons need typed semantics. `expires_at` is nullable. |
| `spec` | JSONB: caller-defined, evolving provisioning parameters are a structured document not used as workflow identity or a secret store. Validate its allowed keys and reject credentials. |
| `metadata` | JSONB: compact, redacted audit/context fields can evolve without a migration; never use it for secrets or authoritative state. |
| `workflow_steps.id`, `environment_id` | UUID columns with primary/foreign-key constraints: durable step identity and indexed environment join. |
| `step_index` | Integer with unique `(environment_id, step_index)`: ordered execution and uniqueness are relational invariants. |
| `direction` | Constrained text column (`forward` or `cleanup`), immutable after planning: the claim predicate partitions rows by direction and orders each partition by `step_index`, so direction must be a directly filterable typed value rather than a kind list re-encoded in every claim query. |
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
  the authoritative current state remains the typed state column. These audit facts are
  operator-facing and read from the database: the `/v1` surface deliberately never returns
  `metadata`, and the API timeline is the redacted step history.
* **resources** — names are deterministically derived from environment ID/operation key,
  and adapters inspect the external systems of record rather than duplicating their state.
* **compensation records** — cleanup is represented by ordinary durable workflow-step rows
  with cleanup kinds and order, not a separate compensation table.

Migration bookkeeping owned by the migration tool is not a domain table. Secrets are in
Vault, not in either domain table; the closest any row comes to one is the non-secret
`credential_ref` locator.

## `/v1` API contract

JSON request/response bodies use the field names below. All errors have the shape
`{"error":{"code":"...","message":"..."}}`; messages are safe and contain no raw
internal error or secret. All IDs in paths are environment UUIDs.

| Method and path | Request | Success | Other contract |
| --- | --- | --- | --- |
| `POST /v1/environments` | `Idempotency-Key` header required; body `{"tenantId":"…","name":"…","spec":{…}}`. | `202` with `{"environmentId":"…","status":"pending","statusUrl":"/v1/environments/…"}`. | Missing/empty key or invalid body: `400`; same tenant/key and same request duplicate: `200` with existing environment/status; same key with different request: `409`; unknown/unauthorized tenant: `403`. No duplicate plan is created. |
| `GET /v1/environments/{id}` | No body. | `200` with `{"environmentId":"…","tenantId":"…","name":"…","status":"…","host":"…","database":"…","credentialRef":"…","createdAt":"…","updatedAt":"…"}`. Not-yet-known connection fields may be omitted. | `404` if absent or not visible to caller. Never includes credential material. |
| `GET /v1/environments/{id}/history` | No body. | `200` with `{"environmentId":"…","steps":[{"stepIndex":1,"kind":"…","state":"…","attempts":0,"createdAt":"…","updatedAt":"…","lastError":null}]}`; only redacted fields are returned. | `404` if absent or not visible. Never returns raw `metadata` or credentials. |
| `POST /v1/environments/{id}/cancel` | No body. | `202` with `{"environmentId":"…","status":"cancelling","statusUrl":"/v1/environments/…"}` when accepted; repeated request is safe. | Accepted only for `pending`, `provisioning`, `in_doubt`, or `verifying`; `404` if absent/not visible; `200` with status `cancelled` if already cancelled; `409` if `ready` or `rotating`, otherwise terminal, or not cancellable. A ready environment must be deleted instead. |
| `DELETE /v1/environments/{id}` | No body. | `202` with `{"environmentId":"…","status":"deleting","statusUrl":"/v1/environments/…"}` when durable deletion cleanup starts; repeated delete is safe. | `404` if absent/not visible; `200` with `{"environmentId":"…","status":"deleted"}` if already deleted; `409` from any state that cannot legally enter `deleting` (`rolling_back` or `cancelling` in progress, or a terminal state other than `deleted`). Never returns `deleted` while cleanup is pending. |
| `POST /v1/environments/{id}/credentials` | No body. | `200` returns a `credential` object with `username`, `password`, `host`, `port`, and `database` fields to the explicitly authorized caller. This is a schema, not a sample containing a credential value. | `403` if caller lacks `credentials:read`; `404` if absent/not visible; `409` unless `ready` — including while `rotating` or `in_doubt`, so a mid-rotation or unverified credential is never handed out; `503` if Vault is unavailable. Every attempt is audited; responses are not logged. |
| `POST /v1/environments/{id}/rotate` | No body. | `202` with `{"environmentId":"…","status":"rotating","statusUrl":"/v1/environments/…"}` while the durable `rotate_credentials` step runs; the environment returns to `ready` only after a fresh connection with the new credential succeeds. | `403` without credential-management permission; `404` if absent/not visible; `409` unless `ready` (there is at most one outstanding rotation); transient dependency failure is retried under the workflow policy. |

All routes return `401` when the caller is unauthenticated. Cross-tenant and otherwise
invisible environment IDs use `404` rather than disclosing existence; `403` is used only
when the resource is visible but the requested action is not authorized.

The status representation is deliberately a non-secret projection; connection details may
be omitted until they exist. No example payload in this contract contains a secret value.
The operational `/healthz` and `/readyz` endpoints are not environment-management APIs.
