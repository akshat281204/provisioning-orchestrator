package config

import (
	"testing"
	"time"
)

func TestValidateRejectsLeaseShorterThanStepTimeout(t *testing.T) {
	c := Config{
		DatabaseURL:       "postgres://x",
		WorkerConcurrency: 1,
		StepTimeout:       30 * time.Second,
		LeaseDuration:     30 * time.Second,
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected an error when lease duration does not exceed step timeout")
	}
}

func TestValidateAcceptsSaneConfig(t *testing.T) {
	c := Config{
		DatabaseURL:       "postgres://x",
		WorkerConcurrency: 2,
		StepTimeout:       30 * time.Second,
		LeaseDuration:     90 * time.Second,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
