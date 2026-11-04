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
// guarantee: every gathered family's label values may only contain bounded
// enumeration values, never the sentinel password, never an environment id.
func TestMetricLabelsExcludeSecretsAndIDs(t *testing.T) {
	m := NewMetrics()
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
