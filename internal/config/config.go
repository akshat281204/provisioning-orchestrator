package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the runtime configuration, assembled from the environment.
// Every field has a usable default so `make run` works against docker-compose.
type Config struct {
	// HTTPAddr is the listen address for the API.
	HTTPAddr string
	// DatabaseURL points at the orchestrator's own Postgres instance.
	DatabaseURL string
	// VaultAddr is the Vault endpoint used to store provisioned credentials.
	VaultAddr string
	// VaultToken authenticates to Vault. In dev this is the root token.
	VaultToken string
	// OTLPEndpoint receives OpenTelemetry traces and metrics. Empty means no
	// collector is configured: the trace and metric providers degrade to
	// no-ops, so the service runs (and provisions) at full speed with no
	// collector listening anywhere. Default: unset — run with the
	// docker-compose collector by exporting
	// OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317.
	OTLPEndpoint string
	// WorkerConcurrency is how many provisioning steps may run at once.
	WorkerConcurrency int
	// StepTimeout bounds a single provisioning step before it is considered
	// ambiguous and must be reconciled rather than retried blindly.
	StepTimeout time.Duration
	// LeaseDuration is how long a worker owns a step before another worker may
	// reclaim it. Must be comfortably larger than StepTimeout.
	LeaseDuration time.Duration
	// DependencyCheckTimeout bounds a single readiness probe of a dependency,
	// so one hung dependency cannot stall /readyz.
	DependencyCheckTimeout time.Duration
	// ShutdownTimeout bounds graceful shutdown: draining in-flight requests
	// and closing dependencies.
	ShutdownTimeout time.Duration
	// StepPause is a deterministic pause applied before executing a step
	// kind (zero disables it). It exists only so the failure-recovery demo
	// can hold a step inside its running window and intercept the process
	// reliably; real deployments leave it zero.
	StepPause time.Duration
	// StepPauseKinds limits StepPause to these step kinds. An empty list
	// pauses every kind.
	StepPauseKinds []string
	// CredentialCallers is the explicit caller-identity allowlist of the
	// credential retrieval and rotation endpoints. This is a minimal policy
	// (see phase 6 out-of-scope notes): any real deployment must front it
	// with a real identity layer.
	CredentialCallers []string
}

// Load reads configuration from the environment, applying defaults.
func Load() (Config, error) {
	c := Config{
		HTTPAddr:               env("HTTP_ADDR", ":8080"),
		DatabaseURL:            env("DATABASE_URL", "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"),
		VaultAddr:              env("VAULT_ADDR", "http://localhost:8200"),
		VaultToken:             env("VAULT_TOKEN", "root"),
		OTLPEndpoint:           env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		WorkerConcurrency:      4,
		StepTimeout:            30 * time.Second,
		LeaseDuration:          90 * time.Second,
		DependencyCheckTimeout: 2 * time.Second,
		ShutdownTimeout:        15 * time.Second,
		CredentialCallers:      []string{"operator"},
	}

	var err error
	if c.WorkerConcurrency, err = envInt("WORKER_CONCURRENCY", c.WorkerConcurrency); err != nil {
		return Config{}, err
	}
	if c.StepTimeout, err = envDuration("STEP_TIMEOUT", c.StepTimeout); err != nil {
		return Config{}, err
	}
	if c.LeaseDuration, err = envDuration("LEASE_DURATION", c.LeaseDuration); err != nil {
		return Config{}, err
	}
	if c.DependencyCheckTimeout, err = envDuration("DEPENDENCY_CHECK_TIMEOUT", c.DependencyCheckTimeout); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", c.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if c.StepPause, err = envDuration("STEP_PAUSE", 0); err != nil {
		return Config{}, err
	}
	if v, ok := envCSV("STEP_PAUSE_KINDS", nil); ok {
		c.StepPauseKinds = v
	}

	if v, ok := envCSV("CREDENTIAL_RETRIEVAL_CALLERS", c.CredentialCallers); ok {
		c.CredentialCallers = v
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate rejects configurations that cannot work. A lease shorter than the
// step timeout would let a healthy worker's step be stolen mid-flight, which is
// exactly the duplicate-execution failure this service exists to avoid.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("config: database url is required")
	}
	if c.WorkerConcurrency < 1 {
		return fmt.Errorf("config: worker concurrency must be at least 1, got %d", c.WorkerConcurrency)
	}
	if c.StepTimeout <= 0 {
		return fmt.Errorf("config: step timeout must be positive, got %s", c.StepTimeout)
	}
	if c.LeaseDuration <= c.StepTimeout {
		return fmt.Errorf(
			"config: lease duration (%s) must exceed step timeout (%s) or steps will be reclaimed while still running",
			c.LeaseDuration, c.StepTimeout,
		)
	}
	if c.DependencyCheckTimeout <= 0 {
		return fmt.Errorf("config: dependency check timeout must be positive, got %s", c.DependencyCheckTimeout)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("config: shutdown timeout must be positive, got %s", c.ShutdownTimeout)
	}
	// A pause consuming the whole step timeout would make every paused step
	// deterministically ambiguous, which is never the intent of the hook.
	if c.StepPause >= c.StepTimeout {
		return fmt.Errorf(
			"config: step pause (%s) must stay below the step timeout (%s) or every paused step would time out",
			c.StepPause, c.StepTimeout,
		)
	}
	return nil
}

// envCSV reads a comma-separated list env var; missing/empty keeps def.
func envCSV(key string, def []string) ([]string, bool) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, false
	}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out, true
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s must be an integer, got %q", key, raw)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s must be a duration like 30s, got %q", key, raw)
	}
	return d, nil
}
