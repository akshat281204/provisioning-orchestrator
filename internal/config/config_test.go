package config

import (
	"testing"
	"time"
)

func TestValidateRejectsLeaseShorterThanStepTimeout(t *testing.T) {
	c := Config{
		DatabaseURL:            "postgres://x",
		WorkerConcurrency:      1,
		StepTimeout:            30 * time.Second,
		LeaseDuration:          30 * time.Second,
		DependencyCheckTimeout: 2 * time.Second,
		ShutdownTimeout:        15 * time.Second,
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected an error when lease duration does not exceed step timeout")
	}
}

func TestValidateAcceptsSaneConfig(t *testing.T) {
	c := Config{
		DatabaseURL:            "postgres://x",
		WorkerConcurrency:      2,
		StepTimeout:            30 * time.Second,
		LeaseDuration:          90 * time.Second,
		DependencyCheckTimeout: 2 * time.Second,
		ShutdownTimeout:        15 * time.Second,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRejectsNonPositiveTimeouts(t *testing.T) {
	base := Config{
		DatabaseURL:            "postgres://x",
		WorkerConcurrency:      2,
		StepTimeout:            30 * time.Second,
		LeaseDuration:          90 * time.Second,
		DependencyCheckTimeout: 2 * time.Second,
		ShutdownTimeout:        15 * time.Second,
	}

	for _, tc := range []struct {
		name string
		mut  func(*Config)
	}{
		{"zero dependency check timeout", func(c *Config) { c.DependencyCheckTimeout = 0 }},
		{"negative dependency check timeout", func(c *Config) { c.DependencyCheckTimeout = -time.Second }},
		{"zero shutdown timeout", func(c *Config) { c.ShutdownTimeout = 0 }},
		{"negative shutdown timeout", func(c *Config) { c.ShutdownTimeout = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mut(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadReadsTimeoutSettings(t *testing.T) {
	t.Setenv("DEPENDENCY_CHECK_TIMEOUT", "5s")
	t.Setenv("SHUTDOWN_TIMEOUT", "2m")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.DependencyCheckTimeout != 5*time.Second {
		t.Fatalf("expected dependency check timeout 5s, got %s", c.DependencyCheckTimeout)
	}
	if c.ShutdownTimeout != 2*time.Minute {
		t.Fatalf("expected shutdown timeout 2m, got %s", c.ShutdownTimeout)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("WORKER_CONCURRENCY", "")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.HTTPAddr == "" {
		t.Fatal("expected a default HTTP address")
	}
	if c.WorkerConcurrency != 4 {
		t.Fatalf("expected default concurrency 4, got %d", c.WorkerConcurrency)
	}
}
