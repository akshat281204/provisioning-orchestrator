package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/config"
	"github.com/akshat281204/provisioning-orchestrator/internal/health"
)

func newTestApp(t *testing.T, log *slog.Logger) *App {
	t.Helper()
	cfg := config.Config{
		HTTPAddr:               ":0",
		DatabaseURL:            "postgres://test",
		VaultAddr:              "http://localhost:8200",
		VaultToken:             "test-token",
		WorkerConcurrency:      1,
		StepTimeout:            time.Second,
		LeaseDuration:          2 * time.Second,
		DependencyCheckTimeout: 500 * time.Millisecond,
		ShutdownTimeout:        time.Second,
	}
	a, err := New(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

// withCheckers replaces the real dependency checkers with fakes, so readiness
// behaviour is testable without any external service.
func withCheckers(a *App, checkers ...health.Checker) {
	a.registry = health.NewRegistry(500*time.Millisecond, checkers...)
}

type fakeChecker struct {
	name string
	err  error
}

func (f fakeChecker) Name() string { return f.name }

func (f fakeChecker) Check(_ context.Context) error { return f.err }

func TestHealthEndpointAlwaysSucceeds(t *testing.T) {
	a := newTestApp(t, slog.New(slog.DiscardHandler))
	// Liveness must not depend on dependency state.
	withCheckers(a, fakeChecker{name: "postgres", err: errors.New("unreachable")})

	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestReadyEndpointFailsWhenDependencyFails(t *testing.T) {
	a := newTestApp(t, slog.New(slog.DiscardHandler))
	withCheckers(a,
		fakeChecker{name: "postgres", err: errors.New("connection refused")},
		fakeChecker{name: "vault"},
	)

	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "postgres") {
		t.Fatalf("expected body to name the failing dependency postgres, got %q", body)
	}
}

func TestReadyEndpointSucceedsOnlyWhenAllDependenciesPass(t *testing.T) {
	a := newTestApp(t, slog.New(slog.DiscardHandler))
	withCheckers(a,
		fakeChecker{name: "postgres"},
		fakeChecker{name: "vault"},
	)

	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequestLoggerNeverLogsCredentials(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	a := newTestApp(t, log)
	withCheckers(a, fakeChecker{name: "postgres", err: errors.New("down")})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	req.Header.Set("Authorization", "Bearer super-secret-token")
	req.Header.Set("Idempotency-Key", "idem-123")
	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, req)

	out := buf.String()
	if strings.Contains(out, "super-secret-token") {
		t.Fatalf("log output must not contain the Authorization credential, got: %s", out)
	}
	for _, field := range []string{`"method":"GET"`, `"path":"/readyz"`, `"status":503`, `"duration_ms"`, `"request_id"`} {
		if !strings.Contains(out, field) {
			t.Fatalf("expected log output to contain %s, got: %s", field, out)
		}
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("expected the request id to be echoed back as a response header")
	}
}
