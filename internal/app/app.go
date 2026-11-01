// Package app wires configuration into a runnable service.
//
// The skeleton deliberately holds only health endpoints. Provisioning, the
// worker loop, and persistence are added phase by phase — see plan/.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	vault "github.com/hashicorp/vault/api"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akshat281204/provisioning-orchestrator/internal/config"
	"github.com/akshat281204/provisioning-orchestrator/internal/health"
)

// App owns the long-lived components of the service.
type App struct {
	cfg      config.Config
	log      *slog.Logger
	server   *http.Server
	registry *health.Registry
	pool     *pgxpool.Pool
}

// New builds the application without starting anything. The Postgres pool
// and Vault client are constructed here so a malformed URL fails fast at
// startup instead of on the first request; neither constructor dials the
// network, so the service still starts cleanly with dependencies down.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("app: create postgres pool: %w", err)
	}

	vaultCfg := vault.DefaultConfig()
	if vaultCfg == nil {
		pool.Close()
		return nil, fmt.Errorf("app: create vault config for %s", cfg.VaultAddr)
	}
	vaultCfg.Address = cfg.VaultAddr
	vaultClient, err := vault.NewClient(vaultCfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("app: create vault client: %w", err)
	}
	vaultClient.SetToken(cfg.VaultToken)

	a := &App{
		cfg: cfg,
		log: log,
		registry: health.NewRegistry(cfg.DependencyCheckTimeout,
			health.NewPostgresChecker(pool),
			health.NewVaultChecker(vaultClient),
		),
		pool: pool,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /readyz", a.handleReady)

	a.server = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           a.requestLogger(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return a, nil
}

// Run serves until ctx is cancelled, then shuts down gracefully. The
// database pool is closed on every exit path from this function. The Vault
// client holds no resources that need closing.
func (a *App) Run(ctx context.Context) error {
	defer a.pool.Close()

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	//nolint:contextcheck // graceful shutdown needs a context detached from the cancelled run ctx
	if err := a.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("app: graceful shutdown: %w", err)
	}
	return nil
}

// requestLogger records method, path, status, duration, and request id.
// It deliberately never logs headers or bodies: headers carry credentials
// (Authorization, cookies) and bodies carry secrets being provisioned.
func (a *App) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		reqID := req.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, req)

		a.log.Info("http request",
			"method", req.Method,
			"path", req.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", reqID,
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on the platforms we run on; fall back to
		// a timestamp so requests still get distinguishable ids.
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func (a *App) handleHealth(w http.ResponseWriter, _ *http.Request) {
	// Liveness: the process is up and serving. It must never touch
	// dependencies, or a dependency outage would make the orchestrator look
	// dead to its supervisor.
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady reports whether the service can do useful work: 200 only when
// every registered dependency passes, 503 naming the failures otherwise.
func (a *App) handleReady(w http.ResponseWriter, r *http.Request) {
	results := a.registry.Check(r.Context())
	failed := health.Failed(results)
	for _, name := range failed {
		a.log.Warn("dependency check failed", "dependency", name, "err", results[name])
	}
	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unready",
			"failed": failed,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
