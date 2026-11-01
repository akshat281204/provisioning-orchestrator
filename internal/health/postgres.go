package health

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresChecker verifies the orchestrator's own database is reachable.
// It checks connectivity only; schema correctness is a later phase.
type PostgresChecker struct {
	pool *pgxpool.Pool
}

// NewPostgresChecker wraps a pgx pool. The pool is not pinged here; checks
// happen on every readiness request.
func NewPostgresChecker(pool *pgxpool.Pool) *PostgresChecker {
	return &PostgresChecker{pool: pool}
}

// Name implements Checker.
func (c *PostgresChecker) Name() string { return "postgres" }

// Check implements Checker.
func (c *PostgresChecker) Check(ctx context.Context) error {
	if err := c.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}
	return nil
}
