# Architecture and durability model

## The problem shape

Provisioning an environment is a multi-step, side-effecting workflow against stateful
external systems:

```
create_database → create_role → grant_privileges → store_credentials → verify_usable
```

Each step:
- takes real time (seconds, sometimes tens of seconds),
- has an ambiguous outcome when it times out (did the `CREATE DATABASE` land or not?),
- is not atomic with the orchestrator's own state,
- leaves debris if abandoned halfway.

A crash, a duplicate delivery, or a network partition at any point can leave the
orchestrator believing something different from reality. The design goal is that **the
database row and the outside world converge**, without human intervention and without
creating duplicate resources.

## Durable execution: steps are rows, not stack frames

An in-process workflow (goroutines, channels, function calls) loses all progress when the
process dies. Instead, every step is a row:

```sql
create table provisioning_steps (
    id              uuid primary key,
    environment_id  uuid not null references environments(id),
    step_index      int  not null,
    kind            text not null,
    state           text not null,          -- pending | running | in_doubt | done | failed | compensated
    attempts        int  not null default 0,
    lease_owner     text,
    lease_expires_at timestamptz,
    last_error      text,
    unique (environment_id, step_index)
);
```

Progress is a state transition on a row, committed in the same transaction that records
the effect. A worker that dies between steps loses nothing: the row still says
`step 3 pending`.

## Claiming: leases, not locks

```sql
update provisioning_steps
   set state = 'running',
       lease_owner = $1,
       lease_expires_at = now() + $2::interval,
       attempts = attempts + 1
 where id = (
     select id from provisioning_steps
      where state = 'pending'
         or (state = 'running' and lease_expires_at < now())
      order by step_index
      for update skip locked
      limit 1
 )
returning *;
```

Two properties matter:

- `FOR UPDATE SKIP LOCKED` lets N workers claim distinct steps concurrently with no
  coordination. No leader election, no central scheduler.
- The **lease** is what makes crash recovery work. A crashed worker cannot release
  anything; its lease simply expires. `LEASE_DURATION` must comfortably exceed
  `STEP_TIMEOUT`, or a healthy worker's step gets reclaimed while it is still running —
  which is exactly the duplicate execution the lease exists to prevent. `config.Validate`
  rejects a configuration where that ordering does not hold.

## `in_doubt` is not `failed`

A step that returns an error has definitely failed; it can be retried or compensated.

A step that times out has an **unknown** outcome. Retrying `CREATE DATABASE foo` because
the first call timed out is how you get "database already exists" — or worse, a retry loop
that never converges. So a timeout transitions the step to `in_doubt`, and reconciliation
inspects the actual world before deciding:

| Observed | Transition |
| --- | --- |
| Resource exists and matches intent | `done` — the work landed, the ack was lost |
| Resource absent | `pending` — safe to retry |
| Resource exists but differs | `failed` — needs human attention, not a retry |

Reconciliation runs on startup for every `in_doubt` step, so a process that died mid-step
resolves as soon as any worker comes back.

## Idempotency

Every step is written so that running it twice is a no-op or converges:

- **`CREATE DATABASE`** — check `pg_database` first; treat `42P04` (duplicate_database) as
  success rather than failure.
- **`CREATE ROLE`** — same pattern with `pg_roles` and `42710` (duplicate_object).
- **Grants** — naturally idempotent, safe to re-run unconditionally.
- **Credential storage** — write to a deterministic Vault path derived from the
  environment ID, so a retry overwrites rather than creates a second secret.

Duplicate *events* are handled one level up: creation is keyed by a client-supplied
idempotency key with a unique constraint, so a retried `POST /environments` returns the
existing environment instead of provisioning a second one.

## Compensation

When a step fails permanently, the completed steps are compensated in reverse order:
drop the role, drop the database, delete the Vault path. Compensation is itself a sequence
of retryable steps, so a crash during rollback resumes rather than leaving debris.

The environment ends in `rolled_back`, which is a terminal success — the system correctly
returned to a clean state.

## The readiness gate

The last step is the one that makes this service trustworthy. It does not check that the
API returned 200 for each prior step. It opens a **new connection as the provisioned role
with the provisioned credential**, and runs a query against the provisioned database.

Only then does the environment become `ready`. An environment that is marked ready has
been proven usable by the same path a consumer would use.

## Observability

Every step emits a span parented to the environment's provisioning trace, so one trace
shows the whole multi-step flow including retries and reconciliation. Prometheus carries
the operational questions:

- `provisioning_duration_seconds{outcome}` — how long a full provision takes, by result
- `provisioning_steps_in_doubt` — anything above zero means something timed out and is
  awaiting reconciliation; this is the metric to alert on
- `provisioning_lease_expirations_total` — worker health, effectively a crash counter
- `provisioning_step_attempts_total{kind}` — retry pressure per step kind
