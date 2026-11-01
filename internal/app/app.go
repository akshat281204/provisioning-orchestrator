// Package app wires configuration into a runnable service.
//
// The skeleton deliberately holds only health endpoints. Provisioning, the
// worker loop, and persistence are added phase by phase — see plan/.
package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/config"
)

// App owns the long-lived components of the service.
type App struct {
	cfg    config.Config
	log    *slog.Logger
	server *http.Server
}

// New builds the application without starting anything.
func New(cfg config.Config, log *slog.Logger) *App {
	a := &App{cfg: cfg, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /readyz", a.handleReady)

	a.server = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return a
}

// Run serves until ctx is cancelled, then shuts down gracefully. Workers and
// database pools join this lifecycle in later phases.
func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		a.log.Info("http server listening", "addr", a.cfg.HTTPAddr)
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	a.log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return a.server.Shutdown(shutdownCtx)
}

func (a *App) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady reports whether the service can do useful work. It starts
// pessimistic and must only report ready once dependencies are verified.
func (a *App) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
