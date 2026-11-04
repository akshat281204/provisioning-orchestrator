package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"strings"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// PostgresStore is the pgxpool implementation of EnvironmentStore and
// StepStore. It enforces the lean two-table schema and maps driver errors to
// the package's sentinel errors.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// compile-time checks that PostgresStore satisfies both store interfaces.
var (
	_ EnvironmentStore = (*PostgresStore)(nil)
	_ StepStore        = (*PostgresStore)(nil)
	_ LeaseStore       = (*PostgresStore)(nil)
	_ StateStore       = (*PostgresStore)(nil)
	_ ReconcileStore   = (*PostgresStore)(nil)
	_ CleanupStore     = (*PostgresStore)(nil)
)

// NewPostgresStore builds a store on top of an existing pool. The caller
// owns the pool's lifecycle.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// mapError converts well-known driver errors (missing row, unique violation)
// into package sentinels, wrapping otherwise.
func mapError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		// 23505 unique_violation
		if pgErr.SQLState() == "23505" {
			return fmt.Errorf("%s: %w: %w", op, ErrDuplicate, err)
		}
	}
	return fmt.Errorf("%s: %w", op, err)
}

// createColumns is the column list shared by the environment select queries.
const envColumns = `id, tenant_id, name, state, idempotency_key, spec, metadata, created_at, updated_at, expires_at`

// scanEnvInto scans one environments row into a domain.Environment.
func scanEnvInto(row pgx.Row) (domain.Environment, error) {
	var (
		env    domain.Environment
		envID  string
		state  string
		asset  []byte
		mdata  []byte
		crAt   time.Time
		upAt   time.Time
		expAt  *time.Time
		expTmp string
	)
	if err := row.Scan(&envID, &env.TenantID, &env.Name, &state, &env.IdempotencyKey,
		&asset, &mdata, &crAt, &upAt, &expAt); err != nil {
		return domain.Environment{}, err
	}
	env.ID = envID
	env.State = domain.EnvState(state)
	env.Spec = asset
	env.Metadata = mdata
	env.CreatedAt = crAt.Format(time.RFC3339)
	env.UpdatedAt = upAt.Format(time.RFC3339)
	if expAt != nil {
		expTmp = expAt.Format(time.RFC3339)
	}
	env.ExpiresAt = expTmp
	return env, nil
}

// Create persists a new environment. A (tenant_id, idempotency_key) collision
// returns an error wrapping ErrDuplicate.
func (s *PostgresStore) Create(ctx context.Context, env domain.Environment) (domain.Environment, error) {
	params, err := envParams(env)
	if err != nil {
		return domain.Environment{}, fmt.Errorf("store: environment create: %w", err)
	}
	row := s.pool.QueryRow(ctx, `
INSERT INTO environments (id, tenant_id, name, state, idempotency_key, spec, metadata, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING `+envColumns, params...)
	created, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError("store: environment create", err)
	}
	return created, nil
}

// envParams normalises the environment insert values shared by every create
// path: defaulted state, non-nil JSONB columns, and a parsed expires_at.
func envParams(env domain.Environment) ([]any, error) {
	if env.ID == "" {
		return nil, fmt.Errorf("id is required")
	}
	if env.State == "" {
		env.State = domain.EnvPending
	}
	if env.Spec == nil {
		env.Spec = []byte(`{}`)
	}
	if env.Metadata == nil {
		env.Metadata = []byte(`{}`)
	}
	var expAt any
	if env.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, env.ExpiresAt)
		if err != nil {
			return nil, fmt.Errorf("parse expires_at %q: %w", env.ExpiresAt, err)
		}
		expAt = t
	}
	return []any{env.ID, env.TenantID, env.Name, string(env.State), env.IdempotencyKey,
		env.Spec, env.Metadata, expAt}, nil
}

// Get returns one environment by id; a missing id returns ErrNotFound.
func (s *PostgresStore) Get(ctx context.Context, id string) (domain.Environment, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+envColumns+` FROM environments WHERE id = $1`, id)
	env, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError(fmt.Sprintf("store: environment get %s", id), err)
	}
	return env, nil
}

// GetByIdempotencyKey looks an environment up by its (tenant, key) pair; a
// miss returns ErrNotFound. This is the idempotency lookup used on retries.
func (s *PostgresStore) GetByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (domain.Environment, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+envColumns+` FROM environments WHERE tenant_id = $1 AND idempotency_key = $2`,
		tenantID, idempotencyKey)
	env, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError(
			fmt.Sprintf("store: environment get by idempotency key tenant=%s key=%s", tenantID, idempotencyKey), err)
	}
	return env, nil
}

// stepColumns is the column list shared by every step select query, in
// scanStepInto order.
const stepColumns = `id, environment_id, step_index, kind, state, attempts,
       COALESCE(lease_owner, ''), lease_expires_at,
       COALESCE(operation_key, ''), metadata, COALESCE(last_error, ''),
       created_at, updated_at`

// scanStepInto scans one workflow_steps row into a domain.Step.
func scanStepInto(row pgx.Row) (domain.Step, error) {
	var (
		st       domain.Step
		idx      int
		kind     string
		state    string
		attempts int
		leaseExp *time.Time
		leaseExs string
		asset    []byte
		crAt     time.Time
		upAt     time.Time
	)
	if err := row.Scan(&st.ID, &st.EnvironmentID, &idx, &kind, &state, &attempts,
		&st.LeaseOwner, &leaseExp, &st.OperationKey, &asset,
		&st.LastError, &crAt, &upAt); err != nil {
		return domain.Step{}, err
	}
	st.StepIndex = idx
	st.Kind = domain.StepKind(kind)
	st.State = domain.StepState(state)
	st.Attempts = attempts
	if leaseExp != nil {
		leaseExs = leaseExp.Format(time.RFC3339)
	}
	st.LeaseExpiresAt = leaseExs
	st.Metadata = asset
	st.CreatedAt = crAt.Format(time.RFC3339)
	st.UpdatedAt = upAt.Format(time.RFC3339)
	return st, nil
}

// cleanupKindWhere is the SQL IN-list of cleanup step kinds, derived from
// domain.CleanupPlan at init so the claim query and the domain cannot drift.
var cleanupKindWhere = func() string {
	names := make([]string, len(domain.CleanupPlan))
	for i, k := range domain.CleanupPlan {
		names[i] = "'" + string(k) + "'"
	}
	return "(" + strings.Join(names, ",") + ")"
}()

// claimableWhere is the shared predicate of the ordinary claim path. A step
// is claimable when it is pending with its backoff lease elapsed, or running
// with an expired lease (crash recovery); what it must never be is
// `in_doubt` — an unknown outcome is resolved by reconciliation, never by
// blind re-execution. Cleanup kinds run only while their environment is on
// a cleanup path (cancelling, deleting, or carrying a cleanup failure), AND
// only after the environment's forward work has settled (a still-running or
// ambiguous forward step could land a create after its drop compensation)
// AND only when every earlier cleanup index is done, so the compensation
// plan runs in its recorded reverse order. Forward kinds are frozen the
// moment the environment carries cleanup rows — cancelling, deleting, or a
// failed cleanup — so no new provisioning work is ever scheduled once
// cleanup or cancellation began, while in-flight steps finish.
const claimableWhere = `((ws.state = 'pending'
            AND (ws.lease_expires_at IS NULL OR ws.lease_expires_at < now()))
        OR (ws.state = 'running' AND ws.lease_expires_at < now()))
    AND ((ws.kind NOT IN %[1]s
            AND NOT EXISTS (SELECT 1 FROM workflow_steps cw
                WHERE cw.environment_id = e.id AND cw.kind IN %[1]s))
        OR (ws.kind IN %[1]s AND e.state IN ('cancelling', 'deleting', 'failed')
            AND NOT EXISTS (SELECT 1 FROM workflow_steps fw
                WHERE fw.environment_id = e.id AND fw.kind NOT IN %[1]s
                      AND fw.state IN ('running', 'in_doubt'))
            AND NOT EXISTS (SELECT 1 FROM workflow_steps pc
                WHERE pc.environment_id = e.id AND pc.kind IN %[1]s
                      AND pc.step_index < ws.step_index
                      AND pc.state <> 'done')))`

// forwardInFlightFilterWhere counts forward (non-cleanup) rows that are not
// yet resolved: the work a cancel/delete must wait for before its cleanup
// plan may run or the workflow may close.
var forwardInFlightFilterWhere = "kind NOT IN " + cleanupKindWhere + `
    AND state IN ('running', 'in_doubt')`

// cleanupFilterWhere matches the cleanup-plan rows of an environment's
// workflow_steps table.
var cleanupFilterWhere = "kind IN " + cleanupKindWhere

// ClaimNextStep atomically claims the oldest claimable step (pending, or
// running with an expired lease) for owner in one statement: the CTE locks
// the first candidate row FOR UPDATE so concurrent claimers skip past it
// (SKIP LOCKED — no worker can claim the same live step), and the UPDATE
// advances it to running under owner before returning the row. `in_doubt`
// steps are structurally unclaimable here — a pending row with a floating
// backoff lease is also skipped until its delay elapses.
func (s *PostgresStore) ClaimNextStep(ctx context.Context, owner string, lease time.Duration) (domain.Step, error) {
	if lease <= 0 {
		return domain.Step{}, fmt.Errorf("store: claim next step for %s: lease must be positive", owner)
	}
	where := fmt.Sprintf(claimableWhere, cleanupKindWhere)
	row := s.pool.QueryRow(ctx, `
WITH claimable AS (
    SELECT ws.id
    FROM workflow_steps ws
    JOIN environments e ON e.id = ws.environment_id
    WHERE `+where+`
    ORDER BY ws.step_index
    LIMIT 1
    FOR UPDATE OF ws SKIP LOCKED
)
UPDATE workflow_steps ws
SET state            = 'running',
    lease_owner      = $1,
    lease_expires_at = now() + make_interval(secs => $2),
    attempts         = attempts + 1,
    updated_at       = now()
FROM claimable c
WHERE ws.id = c.id
RETURNING ws.id, ws.environment_id, ws.step_index, ws.kind, ws.state, ws.attempts,
       COALESCE(ws.lease_owner, ''), ws.lease_expires_at,
       COALESCE(ws.operation_key, ''), ws.metadata, COALESCE(ws.last_error, ''),
       ws.created_at, ws.updated_at`, owner, lease.Seconds())
	step, err := scanStepInto(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Step{}, fmt.Errorf("store: claim next step: %w", ErrNoWork)
		}
		return domain.Step{}, fmt.Errorf("store: claim next step for %s: %w", owner, err)
	}
	return step, nil
}

// RenewLease extends owner's lease on stepID; a failure to match (expired
// lease, stolen step, or finished step) wraps ErrNotFound.
func (s *PostgresStore) RenewLease(ctx context.Context, stepID, owner string, lease time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_steps
SET lease_expires_at = now() + make_interval(secs => $2),
    updated_at       = now()
WHERE id = $1 AND lease_owner = $3 AND state = 'running'`,
		stepID, lease.Seconds(), owner)
	if err != nil {
		return fmt.Errorf("store: renew lease %s for %s: %w", stepID, owner, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: renew lease %s for %s: %w", stepID, owner, ErrNotFound)
	}
	return nil
}

// CompleteStep records the outcome of a step the owner still holds. lastErr
// is stored verbatim ("" for done). Besides done and failed it accepts
// in_doubt: a step whose execution context expired mid-flight has an unknown
// outcome, and recording the ambiguity — not a verdict — is the point. The
// lease is cleared either way so the step is no longer claimable in the
// running branch; in_doubt rows are likewise invisible to the ordinary
// claim (only ClaimNextInDoubtStep touches them).
func (s *PostgresStore) CompleteStep(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error {
	if state != domain.StepDone && state != domain.StepFailed && state != domain.StepInDoubt {
		return fmt.Errorf("store: complete step %s: illegal outcome state %q", stepID, state)
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_steps
SET state            = $2,
    last_error       = $3,
    lease_owner      = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND lease_owner = $4 AND state = 'running'`,
		stepID, string(state), lastErr, owner)
	if err != nil {
		return fmt.Errorf("store: complete step %s as %s: %w", stepID, state, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: complete step %s as %s (owner %s): %w", stepID, state, owner, ErrNotFound)
	}
	return nil
}

// ReleaseStep returns a running step to pending, clearing its lease. Only
// the current owner may release; if the row no longer matches (lease lost,
// step finished), it is a no-op — the step is either reclaimed or done.
func (s *PostgresStore) ReleaseStep(ctx context.Context, stepID, owner string) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_steps
SET state            = 'pending',
    lease_owner      = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND lease_owner = $2 AND state = 'running'`,
		stepID, owner)
	if err != nil {
		return fmt.Errorf("store: release step %s: %w", stepID, err)
	}
	_ = tag.RowsAffected() // zero is fine: the claim was already lost or finished
	return nil
}

// CreatePlan inserts the full forward step plan for an environment atomically.
// Each step starts pending with its immutable index, kind, and operation_key.
// Any (environment_id, step_index) collision errors.
func (s *PostgresStore) CreatePlan(ctx context.Context, environmentID string, steps []domain.PlannedStep) error {
	if len(steps) == 0 {
		return fmt.Errorf("store: create plan for %s: empty plan", environmentID)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: create plan for %s: begin: %w", environmentID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := insertPlan(ctx, tx, environmentID, steps); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: create plan for %s: commit: %w", environmentID, err)
	}
	return nil
}

// insertPlan writes every planned step row on an open transaction. Any
// (environment_id, step_index) collision maps to ErrDuplicate.
func insertPlan(ctx context.Context, tx pgx.Tx, environmentID string, steps []domain.PlannedStep) error {
	for _, step := range steps {
		opKey := domain.OperationKey(environmentID, step.Index, step.Kind)
		if _, err := tx.Exec(ctx, `
INSERT INTO workflow_steps (id, environment_id, step_index, kind, state, operation_key)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)`,
			environmentID, step.Index, string(step.Kind), string(domain.StepPending), opKey); err != nil {
			return mapError(fmt.Sprintf("store: create plan for %s (step %d)", environmentID, step.Index), err)
		}
	}
	return nil
}

// CreateEnvironmentWithPlan inserts the environment and its entire step plan
// in one transaction, so an environment can never exist without a workflow
// and never exists with a half-written plan. A (tenant_id, idempotency_key)
// or (environment_id, step_index) collision returns an error wrapping
// ErrDuplicate and writes nothing.
func (s *PostgresStore) CreateEnvironmentWithPlan(
	ctx context.Context, env domain.Environment, steps []domain.PlannedStep,
) (domain.Environment, error) {
	params, err := envParams(env)
	if err != nil {
		return domain.Environment{}, fmt.Errorf("store: create environment %s with plan: %w", env.ID, err)
	}
	if len(steps) == 0 {
		return domain.Environment{}, fmt.Errorf("store: create environment %s with plan: empty plan", env.ID)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Environment{}, fmt.Errorf("store: create environment %s with plan: begin: %w", env.ID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
INSERT INTO environments (id, tenant_id, name, state, idempotency_key, spec, metadata, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING `+envColumns, params...)
	created, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError(fmt.Sprintf("store: create environment %s with plan", env.ID), err)
	}
	if err := insertPlan(ctx, tx, created.ID, steps); err != nil {
		return domain.Environment{}, fmt.Errorf("store: create environment %s with plan: %w", env.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Environment{}, fmt.Errorf("store: create environment %s with plan: commit: %w", env.ID, err)
	}
	return created, nil
}

// ListForEnvironment returns the steps of an environment ordered by
// step_index; a missing environment yields an empty slice, not NotFound.
func (s *PostgresStore) ListForEnvironment(ctx context.Context, environmentID string) ([]domain.Step, error) {
	rows, err := s.pool.Query(ctx, `
SELECT `+stepColumns+`
FROM workflow_steps
WHERE environment_id = $1
ORDER BY step_index`, environmentID)
	if err != nil {
		return nil, fmt.Errorf("store: list steps for %s: %w", environmentID, err)
	}
	defer rows.Close()

	var steps []domain.Step
	for rows.Next() {
		var (
			st        domain.Step
			idx       int
			kind      string
			state     string
			attempts  int
			leaseExp  *time.Time
			leaseExpS string
			asset     []byte
			crAt      time.Time
			upAt      time.Time
		)
		if err := rows.Scan(&st.ID, &st.EnvironmentID, &idx, &kind, &state, &attempts,
			&st.LeaseOwner, &leaseExp, &st.OperationKey, &asset,
			&st.LastError, &crAt, &upAt); err != nil {
			return nil, fmt.Errorf("store: scan step row for %s: %w", environmentID, err)
		}
		st.StepIndex = idx
		st.Kind = domain.StepKind(kind)
		st.State = domain.StepState(state)
		st.Attempts = attempts
		if leaseExp != nil {
			leaseExpS = leaseExp.Format(time.RFC3339)
		}
		st.LeaseExpiresAt = leaseExpS
		st.Metadata = asset
		st.CreatedAt = crAt.Format(time.RFC3339)
		st.UpdatedAt = upAt.Format(time.RFC3339)
		steps = append(steps, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate steps for %s: %w", environmentID, err)
	}
	return steps, nil
}

// ClaimNextInDoubtStep is the reconciliation claim path: it claims the oldest
// `in_doubt` step for owner — one whose reconciliation lease is absent or
// expired — by taking the lease on the row WITHOUT changing its state. The
// row stays `in_doubt` for as long as it is ambiguous: this is the only
// claim query that touches in_doubt rows, the ordinary claim is structurally
// blind to them by state, and keeping the state through the inspection means
// a reconciler that dies mid-look leaves the ambiguity durable, never a
// `running` row that a lease expiry could hand to an executor.
//
// It secondarily claims a forward step of an environment on a cleanup path
// whose `running` lease expired: that step's outcome is unknown no matter
// what (it was mid-flight when cleanup was requested), so it is resolved
// against the real world too — the ordinary claim can never reclaim it, and
// the cleanup finalizer waits for it.
func (s *PostgresStore) ClaimNextInDoubtStep(ctx context.Context, owner string) (domain.Step, error) {
	row := s.pool.QueryRow(ctx, `
WITH claimable AS (
    SELECT ws.id
    FROM workflow_steps ws
    JOIN environments e ON e.id = ws.environment_id
    WHERE ws.state = 'in_doubt'
        AND (ws.lease_owner IS NULL OR ws.lease_expires_at IS NULL
             OR ws.lease_expires_at < now())
       OR (ws.kind NOT IN `+cleanupKindWhere+`
           AND ws.state = 'running' AND ws.lease_expires_at < now()
           AND e.state IN ('cancelling', 'deleting', 'failed')
           AND EXISTS (SELECT 1 FROM workflow_steps cw
               WHERE cw.environment_id = e.id AND cw.kind IN `+cleanupKindWhere+`))
    ORDER BY ws.step_index
    LIMIT 1
    FOR UPDATE OF ws SKIP LOCKED
)
UPDATE workflow_steps ws
SET lease_owner      = $1,
    lease_expires_at = now() + make_interval(secs => $2),
    updated_at       = now()
FROM claimable c
WHERE ws.id = c.id
RETURNING ws.id, ws.environment_id, ws.step_index, ws.kind, ws.state, ws.attempts,
       COALESCE(ws.lease_owner, ''), ws.lease_expires_at,
       COALESCE(ws.operation_key, ''), ws.metadata, COALESCE(ws.last_error, ''),
       ws.created_at, ws.updated_at`, owner, inDoubtLease.Seconds())
	step, err := scanStepInto(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Step{}, fmt.Errorf("store: claim next in_doubt step: %w", ErrNoWork)
		}
		return domain.Step{}, fmt.Errorf("store: claim next in_doubt step for %s: %w", owner, err)
	}
	return step, nil
}

// inDoubtLease is the lease a reconciler holds while resolving an in_doubt
// step. It must be renewed or resolved promptly so a dead reconciler's claim
// is still visible; a periodic pass reclaims expired ones.
const inDoubtLease = 60 * time.Second

// RecordReconcileOutcome is the ONLY way a reconciliation claim is resolved:
// the owner that holds the in_doubt (or borrowed expired) row records one of
// done / failed / pending / in_doubt and clears the lease, in one statement.
// The state it is leaving (in_doubt or a borrowed running row) has an
// in_doubt→pending edge and a running→pending edge in the step machine, and
// running→in_doubt / in_doubt→in_doubt is the no-verdict rewrite. A row not
// held by owner wraps ErrNotFound — the claim was lost, so this owner's
// verdict is void.
//
// The statement is transactional with the environment's resume: for any
// verdict other than in_doubt, the environment's move out of its in_doubt
// state commits with the verdict, so a crash can never leave the
// environment stranded in_doubt over a step that already carries its final
// verdict — no follow-up statement, and thus no crash gap between them.
func (s *PostgresStore) RecordReconcileOutcome(
	ctx context.Context, stepID, owner string, state domain.StepState, lastErr string,
) error {
	switch state {
	case domain.StepDone, domain.StepFailed, domain.StepPending, domain.StepInDoubt:
	default:
		return fmt.Errorf("store: record reconcile outcome %s: illegal outcome state %q", stepID, state)
	}
	clearLease := state != domain.StepInDoubt
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: record reconcile outcome %s: begin: %w", stepID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The verdict lands first and reads back the fields that route the
	// environment resume — a lost lease (the row did not stay held) voids
	// the verdict before any environment move is even considered.
	var (
		envID     string
		stepIndex int
	)
	row := tx.QueryRow(ctx, `
UPDATE workflow_steps
SET state            = $2,
    last_error       = $3`+leaseClearSQL(clearLease)+`
WHERE id = $1 AND lease_owner = $4 AND (state = 'in_doubt' OR state = 'running')
RETURNING environment_id, step_index`, stepID, string(state), lastErr, owner)
	if err := row.Scan(&envID, &stepIndex); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("store: record reconcile outcome %s as %s (owner %s): %w",
				stepID, state, owner, ErrNotFound)
		}
		return fmt.Errorf("store: record reconcile outcome %s as %s: %w", stepID, state, err)
	}
	if clearLease {
		// The verdict and the environment's resume are ONE commit: dying
		// between them is no longer expressible, so the in_doubt state can
		// never outlive a step that already carries a final verdict.
		if err := resumeEnvironmentTx(ctx, tx, envID, stepIndex); err != nil {
			return fmt.Errorf("store: record reconcile outcome %s: resume %s: %w", stepID, envID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: record reconcile outcome %s as %s: commit: %w", stepID, state, err)
	}
	return nil
}

// leaseClearSQL is the lease-clearing fragment of the reconcile verdict: an
// unobservation (in_doubt) keeps the reconciliation lease — clearing it
// would let the same pass immediately reclaim and hot-loop against an
// unavailable dependency, and the expiry reaper makes the row eligible
// later; a real verdict clears it, the ambiguity is over.
func leaseClearSQL(clear bool) string {
	if clear {
		return `,
    lease_owner      = NULL,
    lease_expires_at = NULL,
    updated_at       = now()`
	}
	return `,
    updated_at       = now()`
}

// resumeEnvironmentTx moves an in_doubt environment back into its workflow
// once its ambiguous step was reconciled: steps before the verify index go
// to provisioning, the verify step itself to verifying. It runs inside the
// verdict's transaction; any other current state passes through unchanged,
// and an illegal edge (a concurrent move won) is a logged no-op upstream.
func resumeEnvironmentTx(ctx context.Context, tx pgx.Tx, environmentID string, stepIndex int) error {
	var state string
	if err := tx.QueryRow(ctx,
		`SELECT state FROM environments WHERE id = $1 FOR UPDATE`, environmentID).Scan(&state); err != nil {
		return fmt.Errorf("store: read state of %s: %w", environmentID,
			mapError(fmt.Sprintf("store: read state of %s", environmentID), err))
	}
	current := domain.EnvState(state)
	if current != domain.EnvInDoubt {
		return nil
	}
	// An environment with cleanup rows is owned by the cleanup finalizer,
	// not by the forward workflow: delete/cancel was requested, so resuming
	// provisioning here would resurrect scheduling the cancellation stopped
	// (claimableWhere freezes forward kinds exactly for that reason). The
	// finalizer closes the environment once the compensation plan concludes.
	var n int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM workflow_steps
WHERE environment_id = $1 AND `+cleanupFilterWhere, environmentID).Scan(&n); err != nil {
		return fmt.Errorf("store: read cleanup rows of %s: %w", environmentID, err)
	}
	if n > 0 {
		return nil
	}
	target := domain.EnvProvisioning
	if stepIndex >= verifyStepIndex {
		target = domain.EnvVerifying
	}
	if !domain.CanTransition(current, target) {
		return nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(),
    metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`, environmentID, string(target), string(current))
	if err != nil {
		return fmt.Errorf("store: transition environment %s to %s: %w", environmentID, target, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: transition environment %s to %s: state moved while transitioning",
			environmentID, target)
	}
	return nil
}

// ReapExpiredReconciliations hands abandoned reconciliation claims back
// without a verdict: an in_doubt row whose reconciler died (or whose
// inDoubtLease ran out mid-look) loses its lease and becomes claimable by
// the next reconciliation pass again. The row stays in_doubt — the ambiguity
// outlives every dead inspector. Returns the number of rows reaped.
func (s *PostgresStore) ReapExpiredReconciliations(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_steps
SET lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE state = 'in_doubt' AND lease_owner IS NOT NULL AND lease_expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("store: reap expired reconciliations: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ListCleanupEnvironments returns the ids of every environment on a cleanup
// path — cancelling, deleting, or failed — that carries cleanup-plan rows.
// The resume sweep uses it to re-run the cleanup finalizer after a crash, so
// an environment whose last cleanup step completed is never stranded
// deleting (or failed) with the terminal verdict unwritten.
func (s *PostgresStore) ListCleanupEnvironments(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
SELECT e.id FROM environments e
WHERE e.state IN ('cancelling', 'deleting', 'failed')
    AND EXISTS (SELECT 1 FROM workflow_steps cw
        WHERE cw.environment_id = e.id AND `+cleanupFilterWhere+`)`)
	if err != nil {
		return nil, fmt.Errorf("store: list cleanup environments: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan cleanup environment ids: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListFailedSteps returns every workflow step resting in `failed` with a
// cleared lease — the rows the resume sweep re-schedules under their cap
// after a crash ate the schedule-retry call. A failed row whose lease is
// still live (impossible state today) is skipped rather than raced.
func (s *PostgresStore) ListFailedSteps(ctx context.Context) ([]domain.Step, error) {
	rows, err := s.pool.Query(ctx, `
SELECT `+stepColumns+`
FROM workflow_steps
WHERE state = 'failed' AND lease_owner IS NULL
ORDER BY updated_at`)
	if err != nil {
		return nil, fmt.Errorf("store: list failed steps: %w", err)
	}
	defer rows.Close()
	var steps []domain.Step
	for rows.Next() {
		var (
			st       domain.Step
			idx      int
			kind     string
			state    string
			attempts int
			leaseExp *time.Time
			leaseExs string
			asset    []byte
			crAt     time.Time
			upAt     time.Time
		)
		if err := rows.Scan(&st.ID, &st.EnvironmentID, &idx, &kind, &state, &attempts,
			&st.LeaseOwner, &leaseExp, &st.OperationKey, &asset,
			&st.LastError, &crAt, &upAt); err != nil {
			return nil, fmt.Errorf("store: scan failed step row: %w", err)
		}
		st.StepIndex = idx
		st.Kind = domain.StepKind(kind)
		st.State = domain.StepState(state)
		st.Attempts = attempts
		if leaseExp != nil {
			leaseExs = leaseExp.Format(time.RFC3339)
		}
		st.LeaseExpiresAt = leaseExs
		st.Metadata = asset
		st.CreatedAt = crAt.Format(time.RFC3339)
		st.UpdatedAt = upAt.Format(time.RFC3339)
		steps = append(steps, st)
	}
	return steps, rows.Err()
}

// ScheduleRetry returns a failed step to pending with its backoff lease set
// to now()+delay: the row becomes claimable again only after the delay, and
// the delay lives in the ROW, so a crash between the failure and the retry
// inherits the same backoff rather than hammering the dependency. Only a
// `failed` row may be rescheduled — a running one is someone's live work; a
// done one is finished. A no-op volume (0 rows) means the step moved on
// elsewhere, which is fine.
func (s *PostgresStore) ScheduleRetry(ctx context.Context, stepID string, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("store: schedule retry %s: negative delay %s", stepID, delay)
	}
	_, err := s.pool.Exec(ctx, `
UPDATE workflow_steps
SET state = 'pending',
    lease_owner = NULL,
    lease_expires_at = now() + make_interval(secs => $2),
    updated_at = now()
WHERE id = $1 AND state = 'failed'`, stepID, delay.Seconds())
	if err != nil {
		return fmt.Errorf("store: schedule retry %s: %w", stepID, err)
	}
	return nil
}

// MarkEnvironmentInDoubt CAS-moves the environment to in_doubt after one of
// its steps recorded an unknown outcome. The move is GATED on an actually
// ambiguous row: the UPDATE only matches while the environment still carries
// a workflow_steps row in `in_doubt`, so the environment can never be moved
// to in_doubt over a step whose ambiguity already resolved (whose record
// failed to land, or whose lease was lost to a reclaiming worker) — such an
// in_doubt environment would never be resumed, because the environment
// resume commits only with a step verdict. Zero rows changed by this race is
// a silent success: the step was resolved, its own transaction already
// resumed (or honestly never moved) the environment.
//
// A state with no legal in_doubt edge is passed through silently: the step
// row already carries the ambiguity, and inventing an illegal environment
// move would corrupt the machine.
func (s *PostgresStore) MarkEnvironmentInDoubt(ctx context.Context, environmentID string) error {
	state, err := s.envStateFor(ctx, environmentID)
	if err != nil {
		return err
	}
	// Cleanup intent owns the environment once cancellation/deletion starts.
	// Keep that state visible so cleanup claims and finalization can proceed;
	// the ambiguous forward row still records the unknown outcome by itself.
	if state == domain.EnvCancelling || state == domain.EnvDeleting {
		return nil
	}
	onCleanup, err := s.hasCleanupRows(ctx, environmentID)
	if err != nil {
		return err
	}
	if onCleanup {
		return nil
	}
	if !domain.CanTransition(state, domain.EnvInDoubt) {
		return nil
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(),
    metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3
  AND EXISTS (SELECT 1 FROM workflow_steps ds
      WHERE ds.environment_id = environments.id AND ds.state = 'in_doubt')`,
		environmentID, string(domain.EnvInDoubt), string(state))
	if err != nil {
		return fmt.Errorf("store: transition environment %s to %s: %w",
			environmentID, domain.EnvInDoubt, err)
	}
	_ = tag.RowsAffected() // zero is fine: no ambiguous row to earn the move
	return nil
}

// hasCleanupRows reports whether the environment carries any cleanup-plan
// rows — the durable marker that a cancellation or deletion was requested.
func (s *PostgresStore) hasCleanupRows(ctx context.Context, environmentID string) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
SELECT count(*) FROM workflow_steps
WHERE environment_id = $1 AND `+cleanupFilterWhere, environmentID).Scan(&n); err != nil {
		return false, fmt.Errorf("store: read cleanup rows of %s: %w", environmentID, err)
	}
	return n > 0, nil
}

// verifyStepIndex is derived from the shared plan so reconciliation routing
// stays correct if setup steps are added or reordered.
var verifyStepIndex = func() int {
	for _, step := range domain.Plan() {
		if step.Kind == domain.KindVerifyUsable {
			return step.Index
		}
	}
	return len(domain.Plan()) + 1
}()

// BeginCleanup moves the environment to cancelling or deleting and makes
// sure the compensation steps exist, in one transaction: the plan rows land
// together with the state move, so a crash between them leaves either the
// old honest state or a fully armed cleanup — never a deleting environment
// with no cleanup plan. Plan rows are written per row with ON CONFLICT
// DO NOTHING, so repeating the call (a second DELETE, a cancelled DELETE
// retried, a worker restart) never errors and never writes a second plan;
// a cleanup step wedged in `failed` is re-armed to pending so the retry the
// caller asked for is real.
func (s *PostgresStore) BeginCleanup(
	ctx context.Context, environmentID string, target domain.EnvState,
) (domain.Environment, error) {
	if target != domain.EnvDeleting && target != domain.EnvCancelling {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: target must be deleting or cancelling, got %q",
			environmentID, target)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: begin: %w", environmentID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM environments WHERE id = $1 FOR UPDATE`, environmentID).Scan(&state)
	if err != nil {
		return domain.Environment{}, mapError(fmt.Sprintf("store: begin cleanup %s", environmentID), err)
	}
	current := domain.EnvState(state)
	if !current.Valid() {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: unknown persisted state %q", environmentID, state)
	}
	if current == target {
		// Already on this cleanup path: fall through to re-arming wedged
		// rows (below), which turns a repeated DELETE into a real retry.
	} else {
		if !domain.CanTransition(current, target) {
			return domain.Environment{}, fmt.Errorf(
				"store: begin cleanup %s: %w: illegal %q -> %q",
				environmentID, ErrIllegalTransition, current, target)
		}
		if _, err := tx.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(),
    metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`,
			environmentID, string(target), string(current)); err != nil {
			return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: %w", environmentID, err)
		}
	}

	// Re-arm failed cleanup steps (one wedged step must not block its
	// siblings' retry either), then insert the plan rows for environments
	// that have no cleanup rows yet. Both are idempotent.
	if _, err := tx.Exec(ctx, `
UPDATE workflow_steps
SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE environment_id = $1 AND state = 'failed' AND `+cleanupFilterWhere, environmentID); err != nil {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: re-arm failed steps: %w", environmentID, err)
	}
	// Cleanup rows are indexed after the forward plan. The insert is
	// conflict-first (ON CONFLICT DO NOTHING per row): a second BeginCleanup
	// finds every (environment_id, step_index) already present and writes
	// nothing, WITHOUT aborting the transaction — the post-condition of two
	// DELETEs is one plan and one answering 202, never a half-consumed tx.
	cleanupSteps := make([]domain.PlannedStep, len(domain.CleanupPlan))
	for i, kind := range domain.CleanupPlan {
		cleanupSteps[i] = domain.PlannedStep{Index: cleanupStepIndex(i), Kind: kind}
	}
	for _, step := range cleanupSteps {
		opKey := domain.OperationKey(environmentID, step.Index, step.Kind)
		if _, err := tx.Exec(ctx, `
INSERT INTO workflow_steps (id, environment_id, step_index, kind, state, operation_key)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
ON CONFLICT (environment_id, step_index) DO NOTHING`,
			environmentID, step.Index, string(step.Kind), string(domain.StepPending), opKey); err != nil {
			return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: %w", environmentID, err)
		}
	}

	env, err := readEnvTx(ctx, tx, environmentID)
	if err != nil {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: %w", environmentID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Environment{}, fmt.Errorf("store: begin cleanup %s: commit: %w", environmentID, err)
	}
	return env, nil
}

// forwardStepCount is the number of steps in the forward plan; cleanup steps
// are indexed immediately after it.
var forwardStepCount = len(domain.Plan())

// cleanupStepIndex gives cleanup plan step i (0-based) its immutable
// workflow_steps index, right after the forward plan's 1..6.
func cleanupStepIndex(i int) int { return forwardStepCount + i + 1 }

// FinalizeCleanup closes a cleanup workflow: while the environment is
// deleting, cancelling, or carrying a cleanup failure it inspects the
// cleanup rows and moves the environment when the whole compensation plan
// has a verdict — every row done → terminal (deleted / cancelled), any row
// failed → failed. Partial progress leaves the state alone (the env keeps
// reporting `deleting` with progress). The state CAS guards each hop.
func (s *PostgresStore) FinalizeCleanup(ctx context.Context, environmentID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: finalize cleanup %s: begin: %w", environmentID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM environments WHERE id = $1 FOR UPDATE`, environmentID).Scan(&state)
	if err != nil {
		return mapError(fmt.Sprintf("store: finalize cleanup %s", environmentID), err)
	}
	current := domain.EnvState(state)
	onCleanup := current == domain.EnvDeleting || current == domain.EnvCancelling || current == domain.EnvFailed
	if !onCleanup {
		// Nothing to finalize; not an error — callers invoke this after
		// every cleanup step without checking the global state first.
		_ = tx.Rollback(ctx)
		return nil
	}

	// The verdict is derived from exactly the cleanup kinds, never from a
	// forward step that outlived the request. But the workflow may not CLOSE
	// while such a forward step is in flight either: its side effect could
	// land after every compensation ran (a create_database after its drop),
	// leaving exactly the orphan the cleanup promises to prevent. Leaving
	// the environment deleting here lets the reconciliation pass resolve
	// the forward row and the resume sweep re-finalize afterwards.
	var inFlight int
	err = tx.QueryRow(ctx, `
SELECT count(*) FROM workflow_steps
WHERE environment_id = $1 AND `+forwardInFlightFilterWhere, environmentID).Scan(&inFlight)
	if err != nil {
		return fmt.Errorf("store: finalize cleanup %s: %w", environmentID, err)
	}
	if inFlight > 0 {
		_ = tx.Rollback(ctx)
		return nil
	}

	var doneCount, failedCount int
	err = tx.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE state = 'done'),
       count(*) FILTER (WHERE state = 'failed')
FROM workflow_steps
WHERE environment_id = $1 AND `+cleanupFilterWhere, environmentID).Scan(&doneCount, &failedCount)
	if err != nil {
		return fmt.Errorf("store: finalize cleanup %s: %w", environmentID, err)
	}
	// The failure verdict must NOT wait for the whole plan: a cleanup step
	// that rests failed (exhausted retries, or a failed-to-reschedule
	// failure) BLOCKS every later cleanup index — the row behind it can
	// never be claimed while its predecessor is not done — so waiting for a
	// full set of verdicts would strand the environment `deleting` forever.
	// Workers and the resume sweep only finalize after a failing step had
	// its chance to be re-scheduled, so a resting failure here is one past
	// its retry budget (or unrecoverable), which is exactly the failure the
	// criterion demands be surfaced. The terminal (all-done) verdict is the
	// only one that still needs every row accounted for.
	switch {
	case failedCount > 0:
		// A cleanup step that keeps failing stays visible as failed and the
		// environment reports failed — never deleted while cleanup failed.
		if current == domain.EnvFailed || !domain.CanTransition(current, domain.EnvFailed) {
			_ = tx.Rollback(ctx)
			return nil
		}
		if _, err := tx.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(), metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`,
			environmentID, string(domain.EnvFailed), string(current)); err != nil {
			return fmt.Errorf("store: finalize cleanup %s: mark failed: %w", environmentID, err)
		}
	case doneCount == len(domain.CleanupPlan):
		// There must be a full cleanup plan at verdicts-all-done; an orphan
		// without one has nothing to wait for and is finalized as if it
		// were done.
		var total int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM workflow_steps
WHERE environment_id = $1 AND `+cleanupFilterWhere, environmentID).Scan(&total); err != nil {
			return fmt.Errorf("store: finalize cleanup %s: %w", environmentID, err)
		}
		if total != len(domain.CleanupPlan) {
			_ = tx.Rollback(ctx)
			return nil
		}
		// The failed→deleted path walks the legal edge failed->deleting
		// first, so the machine is never asked for an illegal move.
		if current == domain.EnvFailed {
			if _, err := tx.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(), metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`,
				environmentID, string(domain.EnvDeleting), string(current)); err != nil {
				return fmt.Errorf("store: finalize cleanup %s: re-route failed to deleting: %w", environmentID, err)
			}
			current = domain.EnvDeleting
		}
		terminal := domain.EnvDeleted
		if current == domain.EnvCancelling {
			terminal = domain.EnvCancelled
		}
		if _, err := tx.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(), metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`,
			environmentID, string(terminal), string(current)); err != nil {
			return fmt.Errorf("store: finalize cleanup %s: close to %s: %w", environmentID, terminal, err)
		}
	default:
		_ = tx.Rollback(ctx)
		return nil
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: finalize cleanup %s: commit: %w", environmentID, err)
	}
	return nil
}

// timelineSQL is the metadata fragment that appends one compact,
// inherently-redacted audit entry ({state, at}) to the metadata jsonb. Only
// state names and timestamps are recorded — no request body, no error text,
// no credential material — so the timeline can never leak a secret.
const timelineSQL = `jsonb_set(metadata, '{timeline}',
    COALESCE(metadata->'timeline', '[]'::jsonb) || jsonb_build_array(
        jsonb_build_object('state', $2::text, 'at', now())))`

// readEnvTx reads one environment inside an open transaction.
func readEnvTx(ctx context.Context, tx pgx.Tx, environmentID string) (domain.Environment, error) {
	row := tx.QueryRow(ctx, `SELECT `+envColumns+` FROM environments WHERE id = $1`, environmentID)
	env, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError(fmt.Sprintf("store: read environment %s", environmentID), err)
	}
	return env, nil
}

// transitionEnv is the atomic hop shared by every state move: read the
// persisted state, validate the edge against the domain transition table,
// then CAS-update on the state just read so a concurrent winner makes this
// writer a no-op instead of a second illegal move.
func (s *PostgresStore) transitionEnv(
	ctx context.Context, environmentID string, current, target domain.EnvState,
) error {
	if !domain.CanTransition(current, target) {
		return fmt.Errorf("store: transition environment %s: illegal %q -> %q",
			environmentID, current, target)
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE environments
SET state = $2, updated_at = now(),
    metadata = `+timelineSQL+`
WHERE id = $1 AND state = $3`, environmentID, string(target), string(current))
	if err != nil {
		return fmt.Errorf("store: transition environment %s to %s: %w", environmentID, target, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: transition environment %s to %s: state moved while transitioning",
			environmentID, target)
	}
	return nil
}

// envStateFor reads one environment's state; unknown ids wrap ErrNotFound.
func (s *PostgresStore) envStateFor(ctx context.Context, environmentID string) (domain.EnvState, error) {
	var state string
	if err := s.pool.QueryRow(ctx,
		`SELECT state FROM environments WHERE id = $1`, environmentID).Scan(&state); err != nil {
		return "", mapError(fmt.Sprintf("store: read state of %s", environmentID), err)
	}
	st := domain.EnvState(state)
	if !st.Valid() {
		return "", fmt.Errorf("store: read state of %s: unknown persisted state %q", environmentID, state)
	}
	return st, nil
}

// BeginProvisioning moves pending -> provisioning. Already-provisioning
// environments pass through unchanged.
func (s *PostgresStore) BeginProvisioning(ctx context.Context, environmentID string) error {
	state, err := s.envStateFor(ctx, environmentID)
	if err != nil {
		return err
	}
	if state == domain.EnvProvisioning {
		return nil
	}
	if err := s.transitionEnv(ctx, environmentID, state, domain.EnvProvisioning); err != nil {
		return err
	}
	return nil
}

// BeginVerification walks pending -> provisioning -> verifying. Monitoring
// the environment as `verifying` while the probe runs gives every sibling
// (the HTTP surface, cleanup) a truthful answer about what is underway.
func (s *PostgresStore) BeginVerification(ctx context.Context, environmentID string) error {
	for {
		state, err := s.envStateFor(ctx, environmentID)
		if err != nil {
			return err
		}
		switch state {
		case domain.EnvVerifying:
			return nil
		case domain.EnvPending:
			if err := s.transitionEnv(ctx, environmentID, state, domain.EnvProvisioning); err != nil {
				return err
			}
		case domain.EnvProvisioning:
			if err := s.transitionEnv(ctx, environmentID, state, domain.EnvVerifying); err != nil {
				return err
			}
		default:
			return fmt.Errorf("store: begin verification %s: from %q the probe step may not run",
				environmentID, state)
		}
	}
}

// rotationLockWait bounds how long a caller may wait for the lock before
// being told the environment is busy; rotationTryInterval spaces the
// non-blocking attempts.
const (
	rotationLockWait    = 5 * time.Second
	rotationTryInterval = 50 * time.Millisecond
)

// tryAdvisoryLock / unlockAdvisoryLock use the non-blocking variant so the
// outcome is unambiguous: an attempt either returns a definite true/false,
// or the whole connection is not trustworthy and is destroyed. A cancellable
// blocking pg_advisory_lock cannot give the same guarantee — after
// pgx returns a context error, the server may have granted the lock on the
// (pooled, still-open) session, and a released-not-closed connection would
// leak the lock into the pool, bricking every later rotation with
// ErrRotationBusy.
const tryAdvisoryLockSQL = `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`
const unlockAdvisoryLockSQL = `SELECT pg_advisory_unlock(hashtextextended($1, 0))`

// destroyRotationConn removes a connection from the pool and closes the
// network session, dropping every session-level lock it holds. This is the
// only safe answer to "I do not know whether this session holds the lock".
func destroyRotationConn(ctx context.Context, conn *pgxpool.Conn) {
	if pgxConn := conn.Hijack(); pgxConn != nil {
		_ = pgxConn.Close(context.WithoutCancel(ctx))
	}
}

// acquireRotationSession takes a dedicated connection and waits on it with
// the non-blocking try-lock loop. It reports ErrRotationBusy only after the
// caller's own context (capped at rotationLockWait) expires while no attempt
// has ever come back ambiguous; any driver-level error destroys the
// connection and surfaces as a real error, never as "busy".
func (s *PostgresStore) acquireRotationSession(
	ctx context.Context, environmentID string,
) (*pgxpool.Conn, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: acquire rotation lock %s: %w", environmentID, err)
	}
	// A pooled connection returns to the pool unchanged unless destroyed
	// explicitly below; plain Release is safe on paths where the lock was
	// never (and provably never) granted.
	wctx, wcancel := context.WithTimeout(ctx, rotationLockWait)
	defer wcancel()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		var locked bool
		if err := conn.QueryRow(wctx, tryAdvisoryLockSQL, environmentID).Scan(&locked); err != nil {
			// The attempt's outcome is unknown: the server may hold the lock
			// on this session. Destroy, never release.
			destroyRotationConn(ctx, conn)
			return nil, fmt.Errorf("store: acquire rotation lock %s: %w", environmentID, err)
		}
		if locked {
			return conn, nil
		}
		timer.Reset(rotationTryInterval)
		select {
		case <-timer.C:
		case <-wctx.Done():
			// No attempt ever reported a lock, so nothing is held: the
			// connection is clean and the caller is free to retry later.
			conn.Release()
			return nil, fmt.Errorf("store: acquire rotation lock %s: %w", environmentID, ErrRotationBusy)
		}
	}
}

// AcquireRotationLock takes the per-environment rotation lock: a session-level
// Postgres advisory lock keyed to the environment id. Rotation mutates Vault
// and the role in several adapter calls that cannot be one transaction, so
// serialization happens here: while this lock is held, no other request
// (in this process or another) writes a new password for the environment, and
// the last writer is provably the stored one.
//
// It returns the release function, which must be called exactly once; its
// error MUST be surfaced, because a failed release means the connection
// holding the lock was destroyed to protect the pool. A caller who could not
// take the lock in time receives an error wrapping ErrRotationBusy — the
// client retry will resume the rotation under the lock rather than race it.
func (s *PostgresStore) AcquireRotationLock(
	ctx context.Context, environmentID string,
) (func() error, error) {
	conn, err := s.acquireRotationSession(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	return func() error {
		nctx, ncancel := context.WithTimeout(context.WithoutCancel(ctx), rotationLockWait)
		defer ncancel()
		// Defensively unlock before the connection returns to the pool and
		// check that the lock was ours: pg_advisory_unlock answers true only
		// when this session actually held it.
		var unlocked bool
		if err := conn.QueryRow(nctx, unlockAdvisoryLockSQL, environmentID).Scan(&unlocked); err != nil || !unlocked {
			// The outcome is again unknown or unowned: the lock may survive
			// on a pooled session, so the connection is destroyed, NOT
			// released — Release would hand the lock back into the pool.
			destroyRotationConn(ctx, conn)
			return fmt.Errorf("store: release rotation lock %s: unlocked=%t: %w",
				environmentID, unlocked, err)
		}
		conn.Release()
		return nil
	}, nil
}

// TransitionEnvironment CAS-moves an environment to `target`. It is the API
// surface for operator-driven moves (rotation start and failure); `ready` is
// rejected here because the only legitimate producers of ready are the
// CompleteVerification and CompleteRotation transactions.
func (s *PostgresStore) TransitionEnvironment(
	ctx context.Context, environmentID string, target domain.EnvState,
) error {
	if target == domain.EnvReady {
		return fmt.Errorf("store: transition environment %s: ready is only reachable through a completed verification",
			environmentID)
	}
	state, err := s.envStateFor(ctx, environmentID)
	if err != nil {
		return err
	}
	if state == target {
		return nil
	}
	return s.transitionEnv(ctx, environmentID, state, target)
}

// WriteConnectionMeta merges the non-secret connection facts into the
// environment's metadata. The jsonb merge keeps any other metadata keys
// intact; only identifiers land here, never credential material.
func (s *PostgresStore) WriteConnectionMeta(
	ctx context.Context, environmentID string, meta []byte,
) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE environments
SET metadata = metadata || $2::jsonb, updated_at = now()
WHERE id = $1`, environmentID, string(meta))
	if err != nil {
		return fmt.Errorf("store: write connection metadata for %s: %w", environmentID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: write connection metadata for %s: %w", environmentID, ErrNotFound)
	}
	return nil
}

// CompleteVerification is the readiness gate's final move: the verification
// step is recorded done under its lease and the environment's state becomes
// ready in the SAME transaction. There is no code path that writes `ready`
// outside this one, so a successful probe is provably what made the
// environment ready and nothing else ever can.
func (s *PostgresStore) CompleteVerification(
	ctx context.Context, stepID, owner, environmentID string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: complete verification %s: begin: %w", stepID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
UPDATE workflow_steps
SET state = 'done',
    last_error = '',
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = now()
WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, stepID, owner)
	if err != nil {
		return fmt.Errorf("store: complete verification step %s: %w", stepID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: complete verification step %s for %s: not held: %w", stepID, owner, ErrNotFound)
	}

	tag, err = tx.Exec(ctx, `
UPDATE environments
SET state = 'ready', updated_at = now(),
    metadata = jsonb_set(metadata, '{timeline}',
        COALESCE(metadata->'timeline', '[]'::jsonb) || jsonb_build_array(
            jsonb_build_object('state', 'ready', 'at', now())))
WHERE id = $1 AND state = 'verifying'`, environmentID)
	if err != nil {
		return fmt.Errorf("store: complete verification of environment %s: %w", environmentID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf(
			"store: complete verification of environment %s: environment is not verifying; ready cannot be granted",
			environmentID)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: complete verification %s: commit: %w", stepID, err)
	}
	return nil
}

// rotationEntry is one line in the verify step's rotation log.
type rotationEntry struct {
	At string `json:"at"`
	By string `json:"by"`
}

// CompleteRotation records one rotation (at/by entry) on the verify step's
// metadata and flips rotating -> ready in one transaction. The metadata is
// merged (jsonb_set), so any other keys already on the verify step's
// document survive; a verify step that does not exist fails the whole
// transaction — a rotation is never "recorded" against nothing.
func (s *PostgresStore) CompleteRotation(
	ctx context.Context, environmentID string, entry []byte,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: complete rotation for %s: begin: %w", environmentID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var cur []byte
	err = tx.QueryRow(ctx, `
SELECT metadata FROM workflow_steps
WHERE environment_id = $1 AND kind = $2
FOR UPDATE`, environmentID, string(domain.KindVerifyUsable)).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("store: read rotation metadata for %s: no verification step exists: %w",
			environmentID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("store: read rotation metadata for %s: %w", environmentID, err)
	}

	var record struct {
		Rotations []rotationEntry `json:"rotations"`
	}
	if len(cur) > 0 {
		if err := json.Unmarshal(cur, &record); err != nil {
			return fmt.Errorf("store: parse rotation metadata for %s: %w", environmentID, err)
		}
	}
	var e rotationEntry
	if err := json.Unmarshal(entry, &e); err != nil {
		return fmt.Errorf("store: parse rotation entry for %s: %w", environmentID, err)
	}
	record.Rotations = append(record.Rotations, e)
	rotations, err := json.Marshal(record.Rotations)
	if err != nil {
		return fmt.Errorf("store: marshal rotation metadata for %s: %w", environmentID, err)
	}

	if _, err := tx.Exec(ctx, `
UPDATE workflow_steps
SET metadata = jsonb_set(metadata, '{rotations}', $2::jsonb, true),
    updated_at = now()
WHERE environment_id = $1 AND kind = $3`,
		environmentID, string(rotations), string(domain.KindVerifyUsable)); err != nil {
		return fmt.Errorf("store: record rotation for %s: %w", environmentID, err)
	}

	tag, err := tx.Exec(ctx, `
UPDATE environments
SET state = 'ready', updated_at = now(),
    metadata = jsonb_set(metadata, '{timeline}',
        COALESCE(metadata->'timeline', '[]'::jsonb) || jsonb_build_array(
            jsonb_build_object('state', 'ready', 'at', now())))
WHERE id = $1 AND state = 'rotating'`, environmentID)
	if err != nil {
		return fmt.Errorf("store: complete rotation of environment %s: %w", environmentID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: complete rotation of environment %s: environment is not rotating", environmentID)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: complete rotation for %s: commit: %w", environmentID, err)
	}
	return nil
}
