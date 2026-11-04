// Command orchestrator runs the provisioning service.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/akshat281204/provisioning-orchestrator/internal/app"
	"github.com/akshat281204/provisioning-orchestrator/internal/config"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

func main() {
	log := slog.New(telemetry.NewRedactingHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	// Telemetry is the last thing to leave: shutdown flushes any queued
	// spans/metric batches to the collector. With no collector configured
	// the providers are no-ops and this is a free call.
	shutdown, err := telemetry.Setup(cfg.OTLPEndpoint)
	if err != nil {
		log.Error("failed to start telemetry", "err", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeout)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			log.Warn("telemetry shutdown incomplete", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := app.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to start service", "err", err)
		os.Exit(1)
	}

	if err := app.Run(ctx); err != nil {
		log.Error("service stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
