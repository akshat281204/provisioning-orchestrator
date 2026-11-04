package adapters

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultCapacityLimit is the bounded number of concurrent provisioned
// environments the local Capacity implementation admits when no limit is set.
const DefaultCapacityLimit = 128

// Capacity is the reservation concern: it bounds how many environments may
// hold provisioned resources at once. Reserve must be idempotent per
// environment (a retry after a crash re-reserves its own slot) and must block
// the last free slot for at most one caller under concurrency.
type Capacity interface {
	Reserve(ctx context.Context, envID string) error
	Release(ctx context.Context, envID string) error
}

// ErrCapacityExhausted is returned when every slot is taken by other
// environments and the caller holds no reservation of its own.
var ErrCapacityExhausted = errors.New("adapters: capacity exhausted")

// LocalCapacity is the database-backed Capacity implementation. Reservations
// are committed rows (capacity_reservations), not process state, so a crash
// leaves them visible and Release can always find them.
type LocalCapacity struct {
	pool  *pgxpool.Pool
	Limit int
	log   *slog.Logger
}

// NewLocalCapacity builds a Capacity on the orchestrator's own pool. A limit
// of zero or less falls back to DefaultCapacityLimit. The pool must have run
// migration 0002 already.
func NewLocalCapacity(pool *pgxpool.Pool, log *slog.Logger) *LocalCapacity {
	return &LocalCapacity{pool: pool, Limit: DefaultCapacityLimit, log: log}
}

// Reserve takes one capacity slot for envID. It is safe to call twice: an
// environment holding its own reservation succeeds immediately without
// consuming a second slot. Concurrency is handled by taking an EXCLUSIVE
// table lock around the count-and-insert, so two callers racing for the last
// slot cannot both pass the limit check — exactly one commits.
func (c *LocalCapacity) Reserve(ctx context.Context, envID string) error {
	limit := c.Limit
	if limit <= 0 {
		limit = DefaultCapacityLimit
	}
	log := c.logger()

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: begin: %w", envID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// EXCLUSIVE allows concurrent readers but not concurrent writers, which
	// serializes exactly the operations that decide whether a slot is free.
	if _, err := tx.Exec(ctx, `LOCK TABLE capacity_reservations IN EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: lock: %w", envID, err)
	}

	// Idempotency first: our own row means we already hold a slot.
	var mine bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM capacity_reservations WHERE environment_id = $1)`,
		envID).Scan(&mine); err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: check: %w", envID, err)
	}
	if mine {
		log.Info("capacity reservation already held", "env_id", envID, "action", "reserve")
		return nil
	}

	var held int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM capacity_reservations`).Scan(&held); err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: count: %w", envID, err)
	}
	if held >= limit {
		return fmt.Errorf("adapters: capacity reserve %s: %d of %d slots held: %w",
			envID, held, limit, ErrCapacityExhausted)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO capacity_reservations (environment_id) VALUES ($1)`, envID); err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: insert: %w", envID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("adapters: capacity reserve %s: commit: %w", envID, err)
	}
	log.Info("capacity reserved", "env_id", envID, "held", held+1, "limit", limit, "action", "reserve")
	return nil
}

// Release frees envID's slot. Deleting a row that is not there is success:
// cleanup must never fail because the reservation already vanished (e.g. the
// environment row was deleted, which cascades).
func (c *LocalCapacity) Release(ctx context.Context, envID string) error {
	if _, err := c.pool.Exec(ctx,
		`DELETE FROM capacity_reservations WHERE environment_id = $1`, envID); err != nil {
		return fmt.Errorf("adapters: capacity release %s: %w", envID, err)
	}
	c.logger().Info("capacity released", "env_id", envID, "action", "release")
	return nil
}

// Held reports whether envID currently holds a capacity reservation. It is
// the reconciler's probe for reserve_capacity: the step's outcome is unknown
// exactly when this answer decides it.
func (c *LocalCapacity) Held(ctx context.Context, envID string) (bool, error) {
	var held bool
	if err := c.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM capacity_reservations WHERE environment_id = $1)`,
		envID).Scan(&held); err != nil {
		return false, fmt.Errorf("adapters: check capacity reservation %s: %w", envID, err)
	}
	return held, nil
}

// logger substitutes a discard handler so log calls are nil-safe.
func (c *LocalCapacity) logger() *slog.Logger {
	if c.log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.log
}

// compile-time guard that LocalCapacity satisfies Capacity.
var _ Capacity = (*LocalCapacity)(nil)
