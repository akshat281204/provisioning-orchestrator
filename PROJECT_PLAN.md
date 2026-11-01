# Provisioning Orchestrator — Project Plan

## Problem statement

Teams need isolated environments for development, testing, and preview deployments. Creating one can involve several independent systems—database, access control, credentials, and connectivity. If these operations are handled by scripts or a request handler, failures can leave partial resources behind, retries can create duplicates, and credentials or permissions can be mishandled.

This project builds a durable service that provisions and removes team environments through recoverable workflows. It tracks progress across dependent operations, resumes safely after crashes and ambiguous timeouts, applies least-privilege access, stores credentials securely, and marks an environment ready only after verifying that it is usable.

The central engineering question is: **How can a service coordinate multi-step provisioning when workers and dependencies can fail at any point, without duplicating resources, exposing secrets, or leaving unmanaged infrastructure behind?**

## Initial scope

The first implementation is local-first. A requested environment consists of a dedicated PostgreSQL database, a dedicated least-privilege database role, credentials stored through a secrets adapter, and a verified connectivity check. Docker Compose provides local dependencies. Provisioning adapters should make it possible to replace local implementations later without changing workflow semantics.

Use Go, PostgreSQL, Docker Compose, OpenTelemetry, Prometheus, and Vault Community Edition or a local secrets adapter. Kafka is not required initially; add a broker only if a later design decision justifies it.

Out of scope for the initial release: multi-region failover, a Kubernetes operator, arbitrary cloud-provider integrations, and claims of exactly-once execution.

## Core workflow

```text
Client
  │ POST /v1/environments + Idempotency-Key
  ▼
API ── transaction ──► PostgreSQL (environment + workflow progress)
                           │
                           ▼
                      Worker pool
                           │
         ┌─────────────────┼───────────────────┐
         ▼                 ▼                   ▼
  Reserve capacity   Create database    Store credential
                           │                   │
                           ▼                   ▼
                   Create role/grants    Secrets service
                           │
                           ▼
                  Verify as provisioned role
                           │
                           ▼
                    Mark environment ready
```

An environment is not ready merely because provisioning calls returned success. The final readiness gate connects using the provisioned identity and performs a real, harmless query. The API returns a credential reference, never the credential itself.

## System guarantees and semantics

- Workflow progress is durable across API and worker restarts.
- Work is **at least once**: a step may run more than once after a crash or lease expiry.
- Steps must therefore be idempotent or reconcilable. Stable operation keys and resource lookups handle ambiguous outcomes before retrying.
- Duplicate and late events are harmless; state transitions validate the current workflow state.
- `ready` means the configured identity can connect and has the expected access.
- Secrets do not appear in API responses other than authorized secret retrieval, workflow metadata, logs, traces, or error messages.
- Cleanup is itself durable and retryable. An environment is not reported deleted while required cleanup remains pending.
- External systems cannot generally provide a single atomic transaction with this service. The design documents partial outcomes rather than promising exactly-once side effects.

## Lean persistence model

Keep the relational schema deliberately small. Do not create a table for every event, step subtype, or optional property. Normalize data that is queried, constrained, or updated independently; use JSONB for sparse, adapter-specific, or one-off metadata.

### `environments`

One row per requested environment. Suggested fields:

- `id` UUID primary key
- `tenant_id`, `name`
- `state`
- `idempotency_key` (unique within the appropriate tenant/request scope)
- `spec` JSONB for the requested, versioned environment configuration
- `metadata` JSONB for sparse provisioning details that do not warrant dedicated columns
- `created_at`, `updated_at`, optional `expires_at`

Keep frequently filtered or correctness-critical values—tenant, state, idempotency key, timestamps—as typed columns. Do not hide these in JSON. Add JSONB keys only when their access patterns justify an index.

### `workflow_steps`

One row per durable step execution in the current workflow. Suggested fields:

- `id` UUID primary key, `environment_id` foreign key, `step_index`, `kind`
- `state`, `attempts`, lease owner/expiry
- `operation_key` for idempotent external operations
- `metadata` JSONB for adapter inputs/results and reconciliation hints; never store secrets
- `last_error`, `created_at`, `updated_at`
- Unique constraint on `(environment_id, step_index)`

This table is justified because steps need independent leases, retries, and state transitions. Avoid separate tables for step attempts, workflow events, resources, or compensation records in the initial version. Keep a compact, redacted timeline in JSONB metadata or derive it from step state and timestamps. Revisit only if real query, retention, or concurrency needs require separate storage.

### Migration tracking

Use the migration mechanism’s existing version tracking if available; do not create a second migration-history table unnecessarily. If a dedicated table is required by the chosen migration tool, it is infrastructure metadata, not application-domain data.

## API outline

- `POST /v1/environments` — create; requires an `Idempotency-Key`; returns `202 Accepted` and an environment ID/status URL.
- `GET /v1/environments/{id}` — status and non-secret connection metadata.
- `GET /v1/environments/{id}/history` — redacted workflow timeline.
- `POST /v1/environments/{id}/cancel` — cancel provisioning where possible.
- `DELETE /v1/environments/{id}` — request durable cleanup.
- `POST /v1/environments/{id}/rotate` — rotate credentials.
- Secret retrieval is a separately authorized path or integration, not part of normal status responses.

Example creation response:

```json
{
  "environmentId": "env-123",
  "status": "provisioning",
  "statusUrl": "/v1/environments/env-123"
}
```

Example ready response:

```json
{
  "environmentId": "env-123",
  "status": "ready",
  "database": {
    "host": "db.internal",
    "name": "env_123",
    "credentialRef": "secrets/env-123/database"
  }
}
```

## Phases and acceptance criteria

### Phase 0 — Design contract

Document states, transitions, step semantics, authorization, secret handling, retry/reconciliation policy, and the persistence model above.

**Done when:** every state has defined entry/exit behavior; ambiguous outcomes and cleanup are specified; schema choices explain what is relational and what belongs in JSONB.

### Phase 1 — Local foundation

Set up the Go service, configuration, Docker Compose dependencies, health/readiness endpoints, migrations, and CI checks.

**Done when:** a clean checkout starts locally from documented commands; readiness fails when required dependencies are unavailable; formatting, lint/vet, tests, and build pass in CI.

### Phase 2 — Domain and lean persistence

Implement typed domain states and legal transitions. Add the minimal `environments` and `workflow_steps` schema, JSONB spec/metadata, constraints, migrations, and store interfaces.

**Done when:** invalid transitions are rejected; migrations are repeatable; core uniqueness and foreign-key constraints are tested; no table exists solely to hold optional one-off metadata.

### Phase 3 — HTTP API and idempotent requests

Implement environment creation and retrieval, status/history responses, request validation, and idempotency-key handling. Persist environment and initial steps atomically.

**Done when:** repeating a request with the same key returns the same environment; no environment can exist without its initial workflow state; responses never expose secrets.

### Phase 4 — Worker claiming and leases

Add workers that claim pending steps safely, using PostgreSQL transactions and lease expiry. Support concurrent workers and expired-lease recovery.

**Done when:** two workers cannot validly own the same live step; a crashed worker’s expired work can be reclaimed; concurrency tests cover claims.

### Phase 5 — Provisioning adapters and permissions

Implement local capacity, database, role/grant, and secrets adapters. Make each operation idempotent or queryable for reconciliation. Apply least privilege and audit actions without recording credentials.

**Done when:** the happy path creates a database and role, stores credentials through the secrets adapter, and retries do not create duplicate resources.

### Phase 6 — Readiness and credential safety

Verify database connectivity as the provisioned role and validate expected privileges. Keep secret retrieval separate and authorized; add redaction and rotation foundations.

**Done when:** readiness cannot be reached before the verification query succeeds; secrets are absent from API status, logs, traces, errors, and persisted workflow metadata.

### Phase 7 — Ambiguous outcomes, retry, and compensation

Add bounded retries/backoff, step timeouts, `in_doubt` handling, resource reconciliation, cancellation, and reverse-order cleanup for partial provisioning.

**Done when:** a simulated successful operation with a lost response is discovered rather than duplicated; cleanup failures remain visible and retryable; late/duplicate events do not regress state.

### Phase 8 — Observability and failure demo

Add structured logs, OpenTelemetry traces, Prometheus metrics, a local dashboard, and repeatable fault-injection scenarios.

**Done when:** an operator can identify stuck workflows and retry storms; the demo can crash a worker after an external action and show safe recovery; telemetry contains no secrets or unbounded high-cardinality labels.

### Phase 9 — Evaluation and documentation

Add reproducible concurrent-request/load tests, capacity-race tests, recovery measurements, architecture documentation, and an operational runbook.

**Done when:** results include machine/configuration context and reproducible commands; claims are measured; limitations and failure semantics are documented.

## Failure cases the test suite must cover

1. Database creation succeeds but its response is lost.
2. Worker crashes after creating a role but before recording step completion.
3. Readiness event is duplicated or arrives after cancellation.
4. Grants partially fail after database creation.
5. Secrets service is unavailable after database provisioning.
6. Two requests race for the last capacity slot.
7. Cleanup partially succeeds and then fails.
8. Credential rotation succeeds remotely but the response is lost.
9. A worker lease expires while the original worker is delayed.
10. An unauthorized caller tries to retrieve credentials.

Tests should assert both persisted workflow state and resulting external resources, not just HTTP response codes.

## Portfolio deliverables

- Public Git repository with clear setup and teardown instructions.
- Architecture and state-machine diagrams.
- Lean schema explanation, including why metadata is JSONB and what is intentionally not normalized yet.
- Local Docker Compose environment requiring no paid services.
- Automated unit, integration, concurrency, and fault-injection tests.
- Failure-recovery demo and operational dashboard.
- Benchmarks with reproducible methodology and honest limitations.

## Definition of done

The project is complete when a user can request an environment; the system can create and verify it; the user can retrieve its secret only through an authorized path; and the system can survive worker crashes, duplicate requests, ambiguous dependency outcomes, and partial cleanup without silently duplicating or abandoning resources.
