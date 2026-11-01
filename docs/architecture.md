# Architecture and durability model

This document is the narrative overview. The normative rules — exact states and legal
transitions, retry/reconciliation, authorization, secret handling, persistence, and the
`/v1` API — are in [`design.md`](design.md). If a narrative example here differs from that
contract, the contract wins.

## The problem shape

Provisioning an environment is a multi-step, side-effecting workflow against stateful
external systems:

```
reserve_capacity → create_database → store_credentials → create_role → grant_privileges → verify_usable
```

Each step:
- takes real time (seconds, sometimes tens of seconds),
- has an ambiguous outcome when it times out (did the `CREATE DATABASE` land or not?),
- is not atomic with the orchestrator's own state,
- leaves debris if abandoned halfway.

A crash, a duplicate delivery, or a network partition at any point can leave the
orchestrator believing something different from reality. The design goal is that **the
database row and the outside world converge** automatically when reality can be reconciled,
without creating duplicate resources; irreconcilable conflicts remain visible for operator
attention.

## Durable execution: steps are rows, not stack frames

An in-process workflow (goroutines, channels, function calls) loses all progress when the
process dies. The environment and its ordered workflow steps are durable rows. A worker
commits intent/progress around external effects, but an external system and Postgres cannot
share a transaction. A crash can therefore leave the effect and row out of sync; the next
worker consults the contract's idempotency and reconciliation rules rather than assuming
exactly-once execution.

## Claiming: leases, not locks

A claim is gated by the candidate's `direction` and its environment's state: forward rows
are claimed only in forward states, cleanup rows only in cleanup states, and each is
ordered only against rows of its own direction. The normative rules are in
[`design.md`](design.md); the query looks like this:

```sql
update workflow_steps
   set state = 'running',
       lease_owner = $1,
       lease_expires_at = now() + $2::interval,
       attempts = attempts + 1
 where id = (
      select candidate.id
        from workflow_steps candidate
        join environments env on env.id = candidate.environment_id
       where candidate.state = 'pending'
         -- order within a direction: earlier same-direction rows must be done
         and not exists (
             select 1 from workflow_steps predecessor
              where predecessor.environment_id = candidate.environment_id
                and predecessor.direction = candidate.direction
                and predecessor.step_index < candidate.step_index
                and predecessor.state <> 'done'
         )
         -- cleanup never waits on forward completion; only an unresolved
         -- forward outcome (running / in_doubt) holds it back
         and (
             (candidate.direction = 'forward'
              and env.state in ('pending', 'provisioning', 'verifying', 'rotating'))
             or
             (candidate.direction = 'cleanup'
              and env.state in ('rolling_back', 'cancelling', 'deleting')
              and not exists (
                  select 1 from workflow_steps inflight
                   where inflight.environment_id = candidate.environment_id
                     and inflight.direction = 'forward'
                     and inflight.state in ('running', 'in_doubt')
              ))
         )
       order by candidate.created_at, candidate.step_index
       for update of candidate skip locked
       limit 1
 )
returning *;
```

Three properties matter:

- `FOR UPDATE SKIP LOCKED` lets N workers claim distinct steps concurrently with no
  coordination. No leader election, no central scheduler.
- The claim is **direction-aware**. A failed or never-run forward step never blocks
  cleanup — that is the case cleanup exists for. Cleanup rows wait only on earlier cleanup
  rows, plus any forward row whose outcome is still unresolved, which the reconciler
  settles. Forward rows are never claimed once cleanup intent exists, because the
  environment is no longer in a forward state.
- The **lease** is what makes crash recovery work. A crashed worker cannot release
  anything; its lease simply expires. An expired `running` row is sent to reconciliation,
  never claimed as ordinary pending work. `LEASE_DURATION` must comfortably exceed
  `STEP_TIMEOUT`; `config.Validate` rejects a configuration where that ordering does not
  hold.

## `in_doubt` is not `failed`

A step that returns a definite error is classified as transient or permanent; only
transient errors within the retry policy are retried.

A step that times out has an **unknown** outcome. It moves to `in_doubt`, and a
step-kind-specific reconciler inspects actual state before allowing a retry. An expired
lease is treated the same way. Reconciliation runs at startup and periodically; the exact
check and resulting transition for every setup and cleanup kind are specified in
[`design.md`](design.md).

## Idempotency

Work is at least once, never exactly once. Every step must be idempotent or reconcilable:

- **`CREATE DATABASE`** — check `pg_database` first; treat `42P04` (duplicate_database) as
  success rather than failure.
- **`CREATE ROLE`** — same pattern with `pg_roles` and `42710` (duplicate_object).
- **Grants** — naturally idempotent, safe to re-run unconditionally.
- **Credential storage** — use a deterministic Vault path derived from the environment ID,
  so a retry converges on one secret. Secret values never appear in the database or
  observability data.
- **Ownership** — every database is created by, and stays owned by, the orchestrator's
  administrative role; the provisioned role holds only least-privilege grants, never
  ownership. The contract derives the fixed cleanup order from this.

Duplicate requests are handled one level up: creation is keyed by a client-supplied
idempotency key with a unique constraint, so a retried `POST /environments` returns the
existing environment instead of provisioning a second one.

## Compensation and deletion

Provisioning rollback, client cancellation, and client deletion are distinct durable
cleanup workflows. Each cleanup action is a workflow step, retried and reconciled like
forward work. A crash resumes cleanup from its rows. Cleanup steps run in the contract's
fixed order — drop database, drop role, delete secret, release capacity — an order derived
from ownership (the role owns objects inside the database and holds grants on it) rather
than the reverse of creation order, and are claimed independently of forward-step
completion; only a forward step with
an unresolved outcome holds them back until the reconciler settles it. `rolled_back`,
`cancelled`, and `deleted` mean their respective cleanup completed; an environment remains
`deleting` (or reports a visible cleanup failure) until all deletion steps are done. It is
never reported deleted while cleanup is pending. See the contract for states and terminal
conditions.

## The readiness gate

The last step is the one that makes this service trustworthy. It does not check that the
API returned 200 for each prior step. It opens a **new connection as the provisioned role
with the provisioned credential**, and runs a query against the provisioned database.

Only then does the environment become `ready`. An environment that is marked ready has
been proven usable by the same path a consumer would use. Credentials are retrievable only
through the separately authorized credentials endpoint, never through status or history.

A `ready` environment is quiescent — no step is claimable in `ready`. A rotate request
moves it to `rotating`, a durable state with exactly one outstanding `rotate_credentials`
step, and back to `ready` only after a fresh connection with the new credential succeeds;
an uncertain rotation outcome is `in_doubt` and is reconciled like any other step.
Credential retrieval is refused unless the environment is `ready`, so a mid-rotation
credential is never handed out.

## Observability

Every step emits a span parented to the environment's provisioning trace, so one trace
shows the whole multi-step flow including retries and reconciliation. Prometheus carries
the operational questions:

- `provisioning_duration_seconds{outcome}` — how long a full provision takes, by result
- `provisioning_steps_in_doubt` — anything above zero means something timed out and is
  awaiting reconciliation; this is the metric to alert on
- `provisioning_lease_expirations_total` — worker health, effectively a crash counter
- `provisioning_step_attempts_total{kind}` — retry pressure per step kind
