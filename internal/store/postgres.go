package store

import (
	"context"
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
	if env.ID == "" {
		return domain.Environment{}, fmt.Errorf("store: environment create: id is required")
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
			return domain.Environment{}, fmt.Errorf("store: environment create: parse expires_at %q: %w", env.ExpiresAt, err)
		}
		expAt = t
	}

	row := s.pool.QueryRow(ctx, `
INSERT INTO environments (id, tenant_id, name, state, idempotency_key, spec, metadata, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING `+envColumns,
		env.ID, env.TenantID, env.Name, string(env.State), env.IdempotencyKey,
		env.Spec, env.Metadata, expAt)

	created, err := scanEnvInto(row)
	if err != nil {
		return domain.Environment{}, mapError("store: environment create", err)
	}
	return created, nil
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

	for _, step := range steps {
		opKey := domain.OperationKey(environmentID, step.Index, step.Kind)
		if _, err := tx.Exec(ctx, `
INSERT INTO workflow_steps (id, environment_id, step_index, kind, state, operation_key)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)`,
			environmentID, step.Index, string(step.Kind), string(domain.StepPending), opKey); err != nil {
			return mapError(fmt.Sprintf("store: create plan for %s (step %d)", environmentID, step.Index), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: create plan for %s: commit: %w", environmentID, err)
	}
	return nil
}

// ListForEnvironment returns the steps of an environment ordered by
// step_index; a missing environment yields an empty slice, not NotFound.
func (s *PostgresStore) ListForEnvironment(ctx context.Context, environmentID string) ([]domain.Step, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, environment_id, step_index, kind, state, attempts,
       COALESCE(lease_owner, ''), lease_expires_at,
       COALESCE(operation_key, ''), metadata, COALESCE(last_error, ''),
       created_at, updated_at
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
