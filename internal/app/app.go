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

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/config"
	"github.com/akshat281204/provisioning-orchestrator/internal/health"
	"github.com/akshat281204/provisioning-orchestrator/internal/httpapi"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// maxRequestIDLen bounds how long a client-supplied X-Request-Id may be
// before it is replaced with a generated id.
const maxRequestIDLen = 64

// Retry and reconciliation bounds for the production pool: a failed step is
// retried up to maxStepAttempts leases burned (row-counted, so a restart
// inherits the same budget), with exponential backoff capped at
// retryMaxBackoff, and in_doubt steps are re-asked every reconcileInterval.
const (
	maxStepAttempts   = 5
	retryBaseBackoff  = 2 * time.Second
	retryMaxBackoff   = 30 * time.Second
	reconcileInterval = 15 * time.Second
)

// newWorkerPool builds the lease pool and its real executor. The Postgres
// adapters share the app's pool and drive their DDL with the same
// credentials; Vault holds the provisioned credentials under its default
// dev-mode mount.
func newWorkerPool(
	cfg config.Config, pool *pgxpool.Pool, vaultClient *vault.Client, log *slog.Logger,
	tele *telemetry.Metrics,
) *worker.Pool {
	capacity := adapters.NewLocalCapacity(pool, log)
	databases := adapters.NewLocalDatabases(pool, cfg.DatabaseURL, log)
	roles := adapters.NewLocalRoles(pool, cfg.DatabaseURL, log)
	secrets := adapters.NewVaultSecrets(vaultClient, "secret")
	secrets.Log = log
	// The orchestrator store is the state machine's window for the worker:
	// verification drives pending/provisioning/verifying forward, and only
	// the store's CompleteVerification transaction can write `ready`.
	st := store.NewPostgresStore(pool)
	return &worker.Pool{
		Concurrency:   cfg.WorkerConcurrency,
		LeaseDuration: cfg.LeaseDuration,
		StepTimeout:   cfg.StepTimeout,
		Store:         st,
		// Reconciliation of in_doubt steps: the read-only probes ask the
		// same adapters the executor drives, minus every mutating call,
		// so "looking" can never become "doing".
		Reconciler: &adapters.Reconciler{
			Capacity:  capacity,
			Databases: databases,
			Roles:     roles,
			Secrets:   secrets,
			Log:       log,
		},
		// Startup pass runs a fresh pass every interval; the default pick
		// keeps a dead worker's ambiguity visible for at most a minute or
		// so without churning the database.
		RetryPolicy: worker.RetryPolicy{
			MaxAttempts: maxStepAttempts,
			BaseBackoff: retryBaseBackoff,
			MaxBackoff:  retryMaxBackoff,
		},
		ReconcileInterval: reconcileInterval,
		Executor: &worker.AdapterExecutor{
			Capacity:  capacity,
			Databases: databases,
			Roles:     roles,
			Secrets:   secrets,
			Verifier:  adapters.NewLocalVerifier(cfg.DatabaseURL),
			Envs:      st,
		},
		Tele: tele,
		Log:  log,
	}
}

// App owns the long-lived components of the service.
type App struct {
	cfg      config.Config
	log      *slog.Logger
	server   *http.Server
	registry *health.Registry
	pool     *pgxpool.Pool
	workers  *worker.Pool
	metrics  *telemetry.Metrics
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
	vaultCfg.Address = cfg.VaultAddr
	vaultClient, err := vault.NewClient(vaultCfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("app: create vault client: %w", err)
	}
	vaultClient.SetToken(cfg.VaultToken)

	metrics := telemetry.NewMetrics()

	a := &App{
		cfg: cfg,
		log: log,
		registry: health.NewRegistry(cfg.DependencyCheckTimeout,
			health.NewPostgresChecker(pool),
			health.NewVaultChecker(vaultClient),
		),
		pool: pool,
		// The worker pool shares the API's database pool: steps advance with
		// no central scheduler, claiming rows from the same tables the HTTP
		// API writes. Steps run through the phase-5 adapters; every
		// operation is idempotent, so retries after crashes converge.
		workers: newWorkerPool(cfg, pool, vaultClient, log, metrics),
		metrics: metrics,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /readyz", a.handleReady)
	// /metrics serves the phase-8 Prometheus registry. It is plaintext by
	// design (operators scrape it) and carries only bounded label values —
	// the guarantee internal/telemetry's tests enforce.
	mux.Handle("GET /metrics", metrics.Handler())

	// The /v1 surface is served by its own package against the same pool a
	// worker (later phases) will claim work from.
	v1 := httpapi.New(newTraceStore(store.NewPostgresStore(pool), log))
	v1.AttachCredentials(
		adapters.NewVaultSecrets(vaultClient, "secret"),
		adapters.NewLocalRoles(pool, cfg.DatabaseURL, log),
		adapters.NewLocalVerifier(cfg.DatabaseURL),
		httpapi.NewCredentialPolicy(cfg.CredentialCallers...),
		log,
	)
	mux.Handle("/v1/", http.StripPrefix("/v1", v1.Handler()))

	a.server = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           telemetry.TraceRequests(a.requestLogger(mux)),
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

	// The worker pool runs for the lifetime of the app and exits when ctx
	// is cancelled, releasing any in-flight step back to pending.
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		if err := a.workers.Run(ctx); err != nil {
			a.log.Error("worker pool stopped with error", "err", err)
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
	// Draining requests first lets in-flight API calls finish; the pool then
	// releases its steps before the pgx pool closes (deferred above).
	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		return fmt.Errorf("app: worker pool did not stop in time")
	}
	return nil
}

// requestLogger records method, path, status, duration, and request id.
// It deliberately never logs headers or bodies: headers carry credentials
// (Authorization, cookies) and bodies carry secrets being provisioned.
func (a *App) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		// Bound client-supplied request ids so a caller cannot inject
		// unbounded content into logs and response headers.
		reqID := req.Header.Get("X-Request-Id")
		if reqID == "" || len(reqID) > maxRequestIDLen {
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
	status      int
	wroteHeader bool
}

// Unwrap lets http.ResponseController reach the real writer so handlers
// that need it (Flush/Hijack) keep working through this wrapper.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// WriteHeader records only the first status send; later calls are no-ops so
// the logged status always matches what actually went on the wire.
func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
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
	if len(failed) > 0 {
		// One aggregate warning per probe; per-dependency detail goes in
		// the log fields, not one line per name, so a long outage with a
		// short probe interval does not spam the log.
		failedErrs := make(map[string]string, len(failed))
		for _, name := range failed {
			failedErrs[name] = results[name].Error()
		}
		a.log.Warn("dependency checks failed", "failed", failedErrs)
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
