package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeChecker struct {
	name string
	err  error
	// delay is how long the check takes, so timeout behaviour is testable.
	delay time.Duration
}

func (f fakeChecker) Name() string { return f.name }

func (f fakeChecker) Check(ctx context.Context) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func TestRegistryAllPass(t *testing.T) {
	r := NewRegistry(time.Second,
		fakeChecker{name: "postgres"},
		fakeChecker{name: "vault"},
	)
	results := r.Check(context.Background())
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for name, err := range results {
		if err != nil {
			t.Fatalf("expected %s to pass, got %v", name, err)
		}
	}
	if got := Failed(results); len(got) != 0 {
		t.Fatalf("expected no failures, got %v", got)
	}
}

func TestRegistryReportsFailureByName(t *testing.T) {
	r := NewRegistry(time.Second,
		fakeChecker{name: "postgres"},
		fakeChecker{name: "vault", err: errors.New("unreachable")},
	)
	results := r.Check(context.Background())
	if err := results["vault"]; err == nil {
		t.Fatal("expected vault to fail")
	}
	if err := results["postgres"]; err != nil {
		t.Fatalf("expected postgres to pass, got %v", err)
	}
	failed := Failed(results)
	if len(failed) != 1 || failed[0] != "vault" {
		t.Fatalf("expected failed=[vault], got %v", failed)
	}
}

func TestRegistryTimeoutsSlowChecker(t *testing.T) {
	r := NewRegistry(5*time.Millisecond,
		fakeChecker{name: "slow", delay: 10 * time.Second},
	)
	results := r.Check(context.Background())
	if err := results["slow"]; err == nil {
		t.Fatal("expected slow check to be cut off by the per-check timeout")
	}
}

func TestRegistrySucceedsWhenSomeChecksAreSlow(t *testing.T) {
	// The per-check timeout must be independent: a slow dependency may not
	// prevent a fast one from being reported as healthy.
	r := NewRegistry(5*time.Millisecond,
		fakeChecker{name: "fast"},
		fakeChecker{name: "slow", delay: 10 * time.Second},
	)
	results := r.Check(context.Background())
	if err := results["fast"]; err != nil {
		t.Fatalf("expected fast to pass despite slow sibling, got %v", err)
	}
}
