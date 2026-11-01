package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

// ClaimNextStep atomically claims the oldest claimable step (pending, or
// running with an expired lease) for owner in one statement: the CTE locks
// the first candidate row FOR UPDATE so concurrent claimers skip past it
// (SKIP LOCKED — no worker can claim the same live step), and the UPDATE
// advances it to running under owner before returning the row.
func (s *PostgresStore) ClaimNextStep(ctx context.Context, owner string, lease time.Duration) (domain.Step, error) {
	if lease <= 0 {
		return domain.Step{}, fmt.Errorf("store: claim next step for %s: lease must be positive", owner)
	}
	row := s.pool.QueryRow(ctx, `
WITH claimable AS (
    SELECT id
    FROM workflow_steps
    WHERE state = 'pending'
       OR (state = 'running' AND lease_expires_at < now())
    ORDER BY step_index
    LIMIT 1
    FOR UPDATE SKIP LOCKED
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
// is stored verbatim ("" for done); the lease is cleared so the step is no
// longer claimable in the running branch, since done/failed rows never match.
func (s *PostgresStore) CompleteStep(ctx context.Context, stepID, owner string, state domain.StepState, lastErr string) error {
	if state != domain.StepDone && state != domain.StepFailed {
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

// ---- Environment state machine (phase 6) ----

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
SET state = $2, updated_at = now()
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
SET state = 'ready', updated_at = now()
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
UPDATE environments SET state = 'ready', updated_at = now()
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
