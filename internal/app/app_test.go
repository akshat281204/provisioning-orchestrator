package app

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akshat281204/provisioning-orchestrator/internal/config"
)

func newTestApp(t *testing.T) *App {
	t.Helper()
	cfg := config.Config{
		HTTPAddr:          ":0",
		DatabaseURL:       "postgres://test",
		WorkerConcurrency: 1,
		StepTimeout:       1,
		LeaseDuration:     2,
	}
	return New(cfg, slog.New(slog.DiscardHandler))
}

func TestHealthEndpoint(t *testing.T) {
	a := newTestApp(t)
	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestReadyEndpoint(t *testing.T) {
	a := newTestApp(t)
	rec := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
