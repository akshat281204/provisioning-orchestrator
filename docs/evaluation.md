# Evaluation: what the guarantees actually measure

Everything the README claims is backed by a command you can run. This document
states the method, the numbers from a recorded run, and — just as important —
what those numbers do **not** show.

## How to reproduce

```bash
make up            # postgres 16, vault (dev mode), otel-collector, prometheus
make test-faults   # the ten fault scenarios
make load          # the load profile alone
make benchmark     # the load profile + recorded environment context
```

All three talk to the compose stack (`docker-compose.yml`); nothing is mocked
at the adapter boundary. The fault scenarios inject failures by wrapping the
real adapters (`internal/adapters`) and failing a controlled number of
external calls — the same code paths production uses, with one dependency
misbehaving. The make targets guard on the stack being up, so a forgotten
`make up` fails loudly instead of silently skipping every scenario.

`make benchmark` records the machine and configuration context (CPU, memory,
Go version, Postgres settings, load profile, git commit) next to every result
in `benchmark-results/`, so a number can always be traced to the
circumstances that produced it.

## Fault scenarios

One test per failure case in the project plan, in
[`test/faults/`](../test/faults/). Each scenario asserts **both** the
persisted workflow state (the `environments` and `workflow_steps` rows,
including attempt counts) **and** the resulting external resources
(`pg_database`, `pg_roles`, the Vault secret, the `capacity_reservations`
row) — never just HTTP status codes.

| # | Failure case | Scenario | What must hold afterwards |
| --- | --- | --- | --- |
| 1 | Database created, response lost | `TestFault1DatabaseCreatedResponseLost` | Exactly one database; step `done` with 2 attempts; env `ready` |
| 2 | Worker crashes after creating a role | `TestFault2WorkerCrashesAfterCreateRole` | Exactly one role (idempotent Ensure, no duplicate); env `ready`; the credential connects |
| 3 | Readiness duplicated / arriving after cancellation | `TestFault3ReadinessDuplicatedAndAfterCancel` | Second `CompleteVerification` refused; `ready` recorded exactly once in the timeline; a late readiness event after cancel never grants `ready`; cleanup ends `cancelled` with zero resources |
| 4 | Grants partially fail | `TestFault4GrantsPartiallyFail` | Step `done` with ≥3 attempts; exactly one role; a real external connection proves the grants |
| 5 | Secrets service unavailable after database creation | `TestFault5SecretsUnavailableAfterDatabase` | One database survives the outage; step `done` with ≥3 attempts; stored payload matches the plan and connects |
| 6 | Two requests race for the last capacity slot | `TestFault6CapacityLastSlotRace` | Exactly one reservation between the two; the loser rests `failed` after its whole retry budget, holds nothing, and cancels cleanly; the winner is `ready` |
| 7 | Cleanup partially succeeds then fails | `TestFault7CleanupPartialSuccessThenFailure` | The partial success is durable (database already dropped); `drop_role` recovers after ≥2 attempts; verdict `deleted`, nothing left behind |
| 8 | Rotation succeeds remotely, response lost | `TestFault8RotationSucceedsResponseLost` | Env stays honestly `rotating`; exactly one live Vault version; a retried rotation converges to `ready`; only the published password connects; the rotation audit log names the caller |
| 9 | Lease expires while the original worker is delayed | `TestFault9LeaseExpiresWhileWorkerDelayed` | The delayed worker's late completion is fenced out; exactly one database; step `done` with exactly 2 attempts |
| 10 | Unauthorized caller requests a credential | `TestFault10UnauthorizedCredentialRequest` | 403 (401 without identity); the stored secret is byte-identical after the denial; the denial is audited by caller identity, and the password appears in no log line |

## Load profile

`test/load/` creates 16 environments at 4/s against 4 workers (all
overridable: `LOAD_ENVS`, `LOAD_RATE_PER_S`, `LOAD_WORKERS`) and measures the
wall-clock time from creation request to the persisted `ready` state, then
counts the external resources each environment left behind. Time-to-ready is
sampled by polling the environment state every 50 ms, so the percentiles
carry up to ~50 ms of sampling quantization — treat sub-50 ms differences
between runs as noise. The run assumes exclusive use of the stack: the make
targets (`-p 1`) serialize the integration packages, and running other
provisioning work against the same Postgres concurrently would show up in
the resource accounting below.

Recorded runs (2026-11-11, arm64 host, 10 CPUs, 16 GB, Go 1.26.3, Postgres
16.15 with `shared_buffers=128MB`, compose colocated). The full context file
is re-recorded next to every result by `make benchmark`; because
`benchmark-results/` is gitignored, the relevant context is reproduced here:

```text
git commit:      <the commit under evaluation — see the context file of the run>
load profile:    LOAD_ENVS=16 LOAD_RATE_PER_S=4 LOAD_WORKERS=4
postgres:        server_version 16.15, shared_buffers 128MB, fsync on,
                 synchronous_commit on, full_page_writes on
vault:           dev-mode container at http://127.0.0.1:8200
```

| Metric | Run 1 | Run 2 |
| --- | --- | --- |
| Environments created | 16 at 4.0/s, 4 workers | 16 at 4.0/s, 4 workers |
| Ready rate | 16/16 (error rate 0%) | 16/16 (error rate 0%) |
| Time-to-ready p50 | ~162 ms | ~162 ms |
| Time-to-ready p95 | ~318 ms | ~318 ms |
| Time-to-ready p99 | ~318 ms | ~318 ms |
| Created databases / roles | 16 / 16, 0 unattributed | 16 / 16, 0 unattributed |
| Duplicate external resources | **0** | **0** |

The duplicate count is the assertion that matters, and its method is worth
stating because a naive version cannot fail: database and role names are
deterministic per environment and UNIQUE in the catalogs, so counting by
name always returns 0 or 1 no matter what the system did. Instead the run
snapshots every provisioned-prefixed resource (`px_%` databases, `pu_%`
roles) before and after, requires the created delta to equal the number of
resources the run's environments hold, and counts per-environment counts
above one as duplicates. A resource created during the run that no run
environment holds — the classic crash-mid-create orphan — fails the run.
Every step is at-least-once executed and idempotent, so a duplicate or
orphan under concurrency must be impossible; the load run is where that
claim is exercised, not asserted from reading the code.

## What these numbers do NOT show

Honesty section, because every benchmark flatters the machine that runs it:

- **Single Postgres, everywhere.** The orchestrator's own database and the
  provisioned target databases are the same Postgres instance. There is no
  network partition, no cross-host latency, and no independent failure
  domain between the store and the provisioned resources. A real deployment
  splits these, and these numbers say nothing about that topology.
- **Local adapters, not cloud APIs.** `LocalCapacity`, `LocalDatabases`,
  `LocalRoles`, and `VaultSecrets` run against the same Postgres and the
  dev-mode Vault container. Real IAM/database APIs are slower, rate-limit,
  and fail differently (quota errors, partial-failure surfaces, eventual
  consistency). Time-to-ready here is a floor, not a forecast.
- **At-least-once, not exactly-once.** The system deliberately re-executes
  steps (scenario 1 shows `attempts = 2`); idempotence converges the external
  world, but duplicate *executions* are a fact of the design, not a bug this
  evaluation eliminates. Exactly-once delivery of work is not claimed and
  not measured.
- **Single worker process, one machine.** The load profile runs one worker
  pool. Lease handoff between *processes* is covered by scenario 9 (two
  claimants), but fleet-scale contention on the claim CTE under many
  processes is not measured.
- **No multi-region, no clock-failure chaos.** Lease expiry uses Postgres
  `now()`; skewed clocks between processes are not simulated.
- **Vault dev mode.** Sealed/unsealed transitions, replication, and audit
  device behaviour are out of scope.
- **Machine-dependent timings.** The percentiles above were recorded on one
  arm64 laptop with the compose stack colocated. `make benchmark` re-records
  the environment context with every run so comparisons stay meaningful.

## Limits the tests do enforce, so they are not unknown unknowns

- The retry budget is bounded and persisted (`attempts`), and the fault
  scenarios show exhausted budgets resting `failed`, never spinning.
- Cancellation always wins over late readiness (scenario 3).
- Capacity is a hard limit under concurrency (scenario 6), and exhaustion is
  an honest, terminal verdict — not an unbounded retry.

These are the properties the architecture guarantees by construction; the
scenarios exist so a future change that breaks them fails a test, not a
production incident.
