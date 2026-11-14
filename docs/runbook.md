# Runbook

Operational guidance for the provisioning orchestrator: spotting stuck
workflows, reading `in_doubt`, forcing reconciliation, and recovering a failed
cleanup. Everything here works against the local compose stack; replace
`docker compose exec -T postgres psql …` with your deployment's database
access in production.

Live indicators:

| Signal | Where |
| --- | --- |
| Every phase-8 metric | `curl http://localhost:8080/metrics`, or Prometheus on :9090 |
| Grafana dashboard | http://localhost:3000 (provisioned from `deploy/dashboard.json`) |
| Health / dependency readiness | http://localhost:8080/healthz, `/readyz` |
| State-machine contract | [`docs/design.md`](design.md), [`docs/architecture.md`](architecture.md) |

---

## Finding stuck workflows

An environment is stuck when it has been out of a terminal state for much
longer than its workflow should take, or when it is not `ready` yet holds no
claimable work. Three ways to find them:

1. **Environment states.** `provisioning`, `verifying`, `in_doubt`,
   `cancelling`, `deleting`, `rotating` are all transitional and each one
   should drain within a couple of lease durations.

   ```sql
   SELECT state, count(*), min(updated_at) AS oldest
   FROM environments
   GROUP BY state
   ORDER BY oldest;
   ```

   A `min(updated_at)` older than a few minutes in any transitional state is a
   stuck workflow candidate.

2. **Step-level truth.** Environments lie only when their steps lie; the step
   rows are the ground truth.

   ```sql
   SELECT e.id, e.state, ws.kind, ws.state AS step_state,
          ws.attempts, ws.lease_owner, ws.lease_expires_at, ws.last_error
   FROM environments e
   JOIN workflow_steps ws ON ws.environment_id = e.id
   WHERE e.state NOT IN ('ready', 'deleted', 'cancelled', 'rolled_back')
   ORDER BY e.updated_at, ws.step_index;
   ```

   What to look for:

   - `step_state = running` with `lease_expires_at` in the past: a claim whose
     owner is presumed dead. It is reclaimable as soon as any worker polls —
     if no worker is alive, this does not drain (see below).
   - `step_state = in_doubt`: ambiguous, waiting for reconciliation.
   - `step_state = failed`: the retry budget (`attempts`) was exhausted; the
     environment will not recover on its own.
   - `step_state = pending` with nothing claiming it: no live worker, or a
     retry backoff window (`lease_expires_at` far in the future is the
     backoff's "not before" time on a pending row).

3. **Metrics.** On the dashboard:
   - `provisioning_steps_in_doubt` above zero for more than a reconciliation
     interval means the reconciler is not resolving (it is down, or it cannot
     observe the real world).
   - `provisioning_reconciliations_total{decision="unobservable"}` growing
     means reconciliations are failing to see real state — check Vault/psql
     connectivity of the process.
   - `provisioning_lease_expirations_total` rising steadily means workers
     keep dying mid-step or leases keep being lost; correlate with logs.
   - `provisioning_duration_seconds{outcome="failed"}` increments are
     environments that reached a permanently failed step.
   - `provisioning_step_attempts_total{kind=…}` growing much faster for one
     kind than the others means that step is being retried or reclaimed
     repeatedly.

## What `in_doubt` means operationally

`in_doubt` is **not** an error state and must never be reset to `pending` by
hand. A step lands there only when its execution context expired before a
verdict could be recorded — the step timeout fired, or the run context died —
so the side effect may or may not have happened in the real world (the
database may or may not exist, the secret may or may not have been written).

Because of that ambiguity the workflow is paused: the environment moves to
`in_doubt` **only after the ambiguity is durably recorded**, and every
resolution path runs the reconciler, which looks at the actual world (does
the database exist? does Vault hold the secret?) and records a verdict —
never a guess. `ready` cannot be earned while a step is `in_doubt`.

Operationally:

- A nonzero `provisioning_steps_in_doubt` is an alert, not an incident by
  itself: those steps may have left external state behind, and each one must
  be resolved before its environment can proceed.
- If the count stays nonzero past one or two reconciliation intervals
  (default 30s), the reconciler is failing. The most common cause is that
  the reconciler cannot read the real world (target database or Vault
  unreachable), which shows up as `reconciliations_total{decision="unobservable"}`.
- Steps whose reconciliation returns `failed` move their environment toward
  rollback/cleanup; that is a decision, not a wedge.
- The step's `last_error` for an in_doubt row reads
  "step timed out; outcome unknown" — it states the ambiguity, not the cause.
  Find the cause in the process logs (one line per step attempt) around the
  step's `updated_at`.

## Forcing reconciliation

The reconciler is part of the worker pool: it runs at startup and every
`ReconcileInterval` (default 30s). Each pass claims every in_doubt step whose
reconciliation lease is free and resolves as many as the real world lets it.
There is no separate reconciler to restart; any restart of the orchestrator
forces the startup pass immediately.

To force one:

```bash
# Graceful restart of the orchestrator (SIGTERM drains cleanly);
# the startup reconciliation claims the in_doubt queue on boot.
make run          # or restart your deployment's orchestrator process
```

The environment leaves `in_doubt` only when a step's verdict commits it
(`done` → resume, `pending` → resume, `failed` → cleanup path). If a pass
resolves nothing and the workload is actually fine, the reconciler could not
observe the world — check that the orchestrator can reach the target
Postgres (`psql <admin conn string>`) and Vault
(`vault status` against `VAULT_ADDR`), then restart again.

Clearing all in_doubt rows of one step from the database by hand is another
way to say "fake the verdict", and it defeats the one guarantee this service
exists to provide: never guessing what the outside world holds. Don't.

Do note the one legitimate manual intervention, the opposite direction: if a
step was stuck `in_doubt` and the operator has verified externally that the
step's effect exists (or does not), restarting the orchestrator is still the
only supported action; the reconciler's verdict logic already encodes "look
at reality". Wait for it rather than editing rows.

## Recovering a failed cleanup

A cleanup step ends `failed` only when its retry budget was exhausted (or
retry scheduling is disabled). The environment sits in `failed` (from a
forward failure, already rolled back) or `cancelling`/`deleting` with a
failed compensation row; the external state may be partially torn down.

The supported recovery is **re-issuing the same cleanup request**: the
cleanup path is idempotent and re-arming.

```bash
# A DELETE on a deleting/failed environment re-arms the wedged cleanup rows;
# a repeat call answers 202 and resumes from where the rows actually are.
curl -X DELETE http://localhost:8080/v1/environments/<env-id>

# The same story for a cancel workflow.
curl -X POST http://localhost:8080/v1/environments/<env-id>/cancel
```

The store treats the repeat as re-arming: rows stuck in `failed` or lost
leases are brought back into the claimable cleanup plan and execution
continues in the durable cleanup order
(`drop_database` → `drop_role` → `delete_secret` → `release_capacity`).
All four cleanup steps retry safely:
dropping an absent database/role/secret is a no-op by design.

Fix the underlying cause before re-issuing — the cleanup failed for a reason,
usually one of:

- **Target Postgres unreachable**: check `docker compose ps postgres` and the
  connection string in the environment.
- **The database cannot be dropped because sessions are open**: terminate
  backends first (`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE datname = '<provisioned db>'`), then re-issue DELETE.
- **Vault (un)reachable / token invalid**: `vault status` from the
  orchestrator's perspective; the secret step cannot run without Vault.
- **The role cannot be dropped because it still owns objects**: the cleanup
  order makes that unlikely (the database is gone first, along with what the
  role owned inside it). If it happens anyway after manual surgery on the
  database, `DROP OWNED BY <role>;` as admin before re-issuing.

Verify recovery by watching the same SQL above: the cleanup rows should move
`failed → pending → done` and the environment should reach `deleted` (or
`cancelled`), at which point every external artifact for it is gone.
`cleanup_duration_seconds{outcome="success"}` observes each finishing
workflow; a final `GET /v1/environments/<id>` reporting `deleted` is the
authoritative close.

## Alert signatures worth wiring (phase-8 dashboard metric names)

```promql
# Steps stuck ambiguous — page when nonzero for > 2m
provisioning_steps_in_doubt > 0

# Workers dying or leases being lost — page on sustained growth
rate(provisioning_lease_expirations_total[10m]) > 0

# Reconciler cannot see the world — page while it lasts
rate(provisioning_reconciliations_total{decision="unobservable"}[10m]) > 0

# Provisionings ending permanently failed — investigate within minutes
rate(provisioning_duration_seconds_count{outcome="failed"}[10m]) > 0
```

(Alertmanager routing itself is out of scope for this phase.)

---

## Fault scenarios as operational drills (phase 9)

Every procedure above was exercised while building the fault scenarios in
[`test/faults/`](../test/faults) — one test per failure case, each asserting
the persisted workflow state **and** the resulting external resources. They
double as repeatable drills: run `make test-faults` and the service's
recovery claims are re-proven against the live stack after any change.

What the scenarios confirmed, and the operational procedure each one backs:

| Scenario (test) | Drill | Confirmed procedure |
| --- | --- | --- |
| 1 — database created, response lost (`TestFault1…`) | Worker dies after `CREATE DATABASE`, before recording | Do nothing but keep a worker alive: lease expiry + idempotent re-run converge; verify with `pg_database` count = 1 and `attempts = 2` on the row |
| 2 — crash after role creation (`TestFault2…`) | Same shape, one step later | Same procedure; `pg_roles` count = 1, then the credential must actually connect |
| 3 — late/duplicate readiness (`TestFault3…`) | Verification completes (or tries to) after cancel | A late `ready` cannot win over cancellation; if a cancelled env ever reports `ready`, that is a P1 bug, not an operational state. Cleanup still ends `cancelled` with zero resources |
| 4 — partial grant failure (`TestFault4…`) | GRANT lands, second GRANT does not | Bounded retry absorbs it; after recovery, prove the grant externally by connecting as the role, not by reading state |
| 5 — Vault outage mid-provisioning (`TestFault5…`) | Secrets service down after database creation | Wait for Vault and let the retry policy run; the database is not duplicated and the secret is stored only when Vault accepts it. `attempts` on the row records the outage |
| 6 — last capacity slot race (`TestFault6…`) | Two creations, one slot | Exactly one wins; the loser rests `failed` after its full persisted retry budget and an operator cancels it (`POST /cancel`); never raise the limit to "unstick" a loser — release slots by cleaning environments |
| 7 — partial cleanup then failure (`TestFault7…`) | Drop succeeds, next drop fails once | The partial success is durable and visible (`pg_database` is already 0 while `drop_role` retries); the workflow re-arms and finishes `deleted`. See "Recovering a failed cleanup" above |
| 8 — rotation commit lost (`TestFault8…`) | Rotation applied remotely, response lost | The env stays `rotating`; simply retry `POST /rotate`. Verify exactly one live Vault version (`StoredVersions`), the audit log on the verify step, and that only the published password connects |
| 9 — lease expiry with delayed worker (`TestFault9…`) | Slow worker vs. reclaim | The stale completion is fenced out by the lease owner guard; a duplicate resource here would be a P1. The row's `attempts = 2` is the audit trail |
| 10 — unauthorized credential access (`TestFault10…`) | Wrong caller asks for a credential | 403 (401 without identity), the stored secret is untouched, and the denial is audited by caller identity. Check `curl /metrics`/logs for the audit line; investigate the caller, not the store |

Two recurring operational lessons the drills forced us to encode:

- **State is claimed from rows, not from responses.** Every scenario that
  loses a response (1, 8, 9) resolves the same way: look at the persisted
  rows and the real resources, never trust the HTTP answer alone.
- **Terminal verdicts are earned, never forced.** `ready`, `deleted`, and
  `cancelled` each land only through their owning transaction; when a drill
  tries to shortcut one (3, 7), the store refuses and the honest state
  (`rotating`, `deleting` with progress) stays visible.

## Benchmarking procedure

`make benchmark` runs the load profile (`test/load`) and records the result
with the machine and configuration context — CPU, memory, Go version,
Postgres settings (`shared_buffers`, `max_connections`, `synchronous_commit`,
…), the load parameters, and the git commit — into `benchmark-results/`.
Compare runs only against runs with comparable context; the numbers are
machine-dependent (see [`evaluation.md`](evaluation.md) for what they do not
show). The load run itself asserts the two properties that matter under
concurrency: a 100% ready rate and zero duplicate external resources.
