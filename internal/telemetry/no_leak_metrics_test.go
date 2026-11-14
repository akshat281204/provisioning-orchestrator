package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// collectMetrics returns the exposition text of the registry's HTTP handler.
func collectMetrics(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("/metrics returned %d", rec.Code)
	}
	return rec.Body.String()
}

// TestMetricLabelsExcludeSecretsAndIDs covers the metric side of the phase-8
// guarantee: every label-bearing helper is fed sentinel values first (a
// would-be password, an environment id) and must not let either reach the
// exposition — boundedLabel collapses them to the fallback bucket. The
// run-observers after that use only genuine enum values.
func TestMetricLabelsExcludeSecretsAndIDs(t *testing.T) {
	m := NewMetrics()
	// A sentinel plundered through every label-bearing path: step kind,
	// reconcile decision, and the two outcome labels.
	m.ObserveStepAttempt(t.Context(), sentinelEnvID)
	m.ObserveReconciliation(t.Context(), sentinelEnvID)
	m.ObserveProvisioningDuration(t.Context(), sentinelPassword, 0.1)
	m.ObserveCleanupDuration(t.Context(), sentinelPassword, 0.1)

	// Then one observation of each family with legitimate values, so every
	// family exists in the exposition of this test too.
	m.ObserveStepAttempt(t.Context(), "verify_usable")
	m.IncLeaseExpiration(t.Context())
	m.SetStepsInDoubt(3)
	m.ObserveReconciliation(t.Context(), DecisionDone)
	m.ObserveProvisioningDuration(t.Context(), OutcomeSuccess, 1.5)
	m.ObserveCleanupDuration(t.Context(), OutcomeFailed, 0.25)

	body := collectMetrics(t, m)
	for _, s := range []string{sentinelPassword, sentinelEnvID} {
		if strings.Contains(body, s) {
			t.Errorf("/metrics exposition leaks %q", s)
		}
	}
	// The sentinels must have landed in the fallback bucket, not vanished:
	// the families carry other-labelled series now.
	if n := strings.Count(body, "provisioning_step_attempts_total{kind=\"other\"}"); n == 0 {
		t.Error("sentinel step kind did not land in the fallback {kind=\"other\"} bucket")
	}
	if n := strings.Count(body, `provisioning_reconciliations_total{decision="other"}`); n == 0 {
		t.Error("sentinel decision did not land in the fallback {decision=\"other\"} bucket")
	}
	if n := strings.Count(body, `provisioning_duration_seconds_bucket{outcome="other",le="+Inf"`); n == 0 {
		t.Error("sentinel outcome did not land in the fallback {outcome=\"other\"} bucket")
	}
	if n := strings.Count(body, `cleanup_duration_seconds_bucket{outcome="other",le="+Inf"`); n == 0 {
		t.Error("sentinel cleanup outcome did not land in the fallback {outcome=\"other\"} bucket")
	}
}

// TestMetricsExposeEveryPhase8Name names the contract metrics on /metrics.
func TestMetricsExposeEveryPhase8Name(t *testing.T) {
	m := NewMetrics()
	m.ObserveStepAttempt(t.Context(), "create_database")
	m.IncLeaseExpiration(t.Context())
	m.SetStepsInDoubt(1)
	m.ObserveReconciliation(t.Context(), DecisionPending)
	m.ObserveProvisioningDuration(t.Context(), OutcomeSuccess, 2)
	m.ObserveCleanupDuration(t.Context(), OutcomeSuccess, 2)

	body := collectMetrics(t, m)
	for _, name := range []string{
		"provisioning_duration_seconds",
		"provisioning_steps_in_doubt",
		"provisioning_lease_expirations_total",
		"provisioning_step_attempts_total",
		"provisioning_reconciliations_total",
		"cleanup_duration_seconds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics exposition missing %q", name)
		}
	}
}
