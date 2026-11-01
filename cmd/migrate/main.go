// Command migrate applies the embedded SQL migrations to the configured
// database. Running it repeatedly is safe: applied versions are tracked in
// the migration tool's own schema_migrations table and an already-applied
// run is a no-op.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akshat281204/provisioning-orchestrator/internal/config"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	applied, err := store.ApplyMigrations(ctx, pool, store.MigrateOptions{LockTimeout: 30 * time.Second})
	if err != nil {
		log.Error("apply migrations", "err", err)
		os.Exit(1)
	}
	if len(applied) == 0 {
		log.Info("migrations up to date, nothing to apply")
		return
	}
	for _, m := range applied {
		fmt.Printf("applied %04d %s\n", m.Version, m.Name)
	}
	log.Info("migrations applied", "count", len(applied))
}
