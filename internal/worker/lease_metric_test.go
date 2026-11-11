package worker_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// leaseExpirationsCount scrapes the pool's /metrics exposition for the
// provisioning_lease_expirations_total counter value (absent means 0).
func leaseExpirationsCount(t *testing.T, tele *telemetry.Metrics) int {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	tele.Handler().ServeHTTP(rec, req)
	line := ""
	for _, l := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(l, "provisioning_lease_expirations_total") {
			line = l
			break
		}
	}
	if line == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(line, "provisioning_lease_expirations_total %d", &n); err != nil {
		t.Fatalf("unparsable lease_expirations exposition %q: %v", line, err)
	}
	return n
}

// TestNormalClaimDoesNotCountLeaseExpiration pins the gating on the metric:
// a pool running an ordinary pending step performs a claim, an attempt, and
// an outcome, but the counter that reports observed worker deaths must stay
// at zero.
func TestNormalClaimDoesNotCountLeaseExpiration(t *testing.T) {
	st := newFakeStore(nil, step("fresh-pending-step", 1))
	tele := telemetry.NewMetrics()
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      worker.NoopExecutor{},
		Tele:          tele,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	waitForStepState(t, st, "fresh-pending-step", domain.StepDone)
	stopPool(t, cancel, done)

	if n := leaseExpirationsCount(t, tele); n != 0 {
		t.Fatalf("a normal pending claim must not count a lease expiration, got %d", n)
	}
}

// TestReclaimedExpiredLeaseCountsLeaseExpiration covers the real signal: a
// step seeded `running` under an owner whose lease already expired is
// reclaimed, and exactly that claim increments
// provisioning_lease_expirations_total.
func TestReclaimedExpiredLeaseCountsLeaseExpiration(t *testing.T) {
	st := newFakeStore(nil, runningSeed(step("orphaned-step", 1), "dead-worker", -time.Minute))
	tele := telemetry.NewMetrics()
	pool := &worker.Pool{
		LeaseDuration: time.Minute,
		StepTimeout:   time.Minute,
		Store:         st,
		Executor:      worker.NoopExecutor{},
		Tele:          tele,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startPool(ctx, pool)

	waitForStepState(t, st, "orphaned-step", domain.StepDone)
	stopPool(t, cancel, done)

	if n := leaseExpirationsCount(t, tele); n != 1 {
		t.Fatalf("a reclaimed expired-lease claim must count exactly one lease expiration, got %d", n)
	}
}
