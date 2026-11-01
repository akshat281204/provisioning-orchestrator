package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	vault "github.com/hashicorp/vault/api"
)

// The production checkers decide whether /readyz is truthful, so they are
// tested against real pgxpool/vault.Client instances — not fakes. None of
// these tests need running infrastructure: an unreachable address is a
// perfectly good dependent-down fixture.

// unreachablePort listens on nothing; 1 is a privileged port that does not
// accept connections, so every dial to it fails immediately.
const unreachableAddr = "127.0.0.1:1"

func TestPostgresCheckerName(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@"+unreachableAddr+"/db")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	c := NewPostgresChecker(pool)
	if c.Name() != "postgres" {
		t.Fatalf("checker name must be postgres, got %q", c.Name())
	}
}

func TestPostgresCheckerFailsWhenUnreachable(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@"+unreachableAddr+"/db")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := NewPostgresChecker(pool).Check(ctx); err == nil {
		t.Fatal("expected ping to an unreachable postgres to fail")
	}
}

func TestVaultCheckerName(t *testing.T) {
	client, err := vault.NewClient(vaultClientCfg(t, "http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("vault.NewClient: %v", err)
	}
	client.SetToken("test-token")

	if got := NewVaultChecker(client).Name(); got != "vault" {
		t.Fatalf("checker name must be vault, got %q", got)
	}
}

func TestVaultCheckerFailsWhenUnreachable(t *testing.T) {
	client, err := vault.NewClient(vaultClientCfg(t, "http://"+unreachableAddr))
	if err != nil {
		t.Fatalf("vault.NewClient: %v", err)
	}
	client.SetToken("test-token")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := NewVaultChecker(client).Check(ctx); err == nil {
		t.Fatal("expected health check against an unreachable vault to fail")
	}
}

// vaultTestSrv builds a fake Vault HTTP server and a configured token-client
// pointed at it.
func vaultTestSrv(t *testing.T, healthStatus int, healthBody string, lookupStatus int, lookupBody string) (*httptest.Server, *vault.Client) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sys/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(healthStatus)
		_, _ = w.Write([]byte(healthBody))
	})
	mux.HandleFunc("/v1/auth/token/lookup-self", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(lookupStatus)
		_, _ = w.Write([]byte(lookupBody))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client, err := vault.NewClient(vaultClientCfg(t, srv.URL))
	if err != nil {
		t.Fatalf("vault.NewClient: %v", err)
	}
	client.SetToken("test-token")
	return srv, client
}

func TestVaultCheckerPassesWhenHealthy(t *testing.T) {
	_, client := vaultTestSrv(t,
		http.StatusOK, `{"initialized":true,"sealed":false}`,
		http.StatusOK, `{"data":{"id":"test-token"}}`,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := NewVaultChecker(client).Check(ctx); err != nil {
		t.Fatalf("expected healthy vault and token to pass, got %v", err)
	}
}

func TestVaultCheckerReportsSealedExplicitly(t *testing.T) {
	// Vault's health endpoint reports a sealed vault with sealed=true; the
	// checker must catch that even when the response status is 2xx.
	_, client := vaultTestSrv(t,
		http.StatusOK, `{"initialized":true,"sealed":true}`,
		http.StatusOK, `{}`,
	)

	err := NewVaultChecker(client).Check(context.Background())
	if err == nil {
		t.Fatal("expected a sealed vault to fail readiness")
	}
	if !strings.Contains(err.Error(), "sealed") {
		t.Fatalf("expected the error to name the sealed state, got %v", err)
	}
}

func TestVaultCheckerReportsSealedVia503(t *testing.T) {
	_, client := vaultTestSrv(t,
		http.StatusServiceUnavailable, `{"initialized":true,"sealed":true}`,
		http.StatusOK, `{}`,
	)

	err := NewVaultChecker(client).Check(context.Background())
	if err == nil {
		t.Fatal("expected a 503 sealed vault to fail readiness")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error kind: %v", err)
	}
}

func TestVaultCheckerReportsUninitialized(t *testing.T) {
	_, client := vaultTestSrv(t,
		http.StatusOK, `{"initialized":false,"sealed":false}`,
		http.StatusOK, `{}`,
	)

	err := NewVaultChecker(client).Check(context.Background())
	if err == nil {
		t.Fatal("expected an uninitialized vault to fail readiness")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("expected the error to name the uninitialized state, got %v", err)
	}
}

func TestVaultCheckerReportsInvalidToken(t *testing.T) {
	// Reachable and unsealed, but the token is bad: the lookup-self call
	// must fail readiness.
	_, client := vaultTestSrv(t,
		http.StatusOK, `{"initialized":true,"sealed":false}`,
		http.StatusForbidden, `{"errors":["permission denied"]}`,
	)

	err := NewVaultChecker(client).Check(context.Background())
	if err == nil {
		t.Fatal("expected an invalid token to fail readiness")
	}
	if !strings.Contains(err.Error(), "token lookup") {
		t.Fatalf("expected the error to name the token lookup, got %v", err)
	}
}

func vaultClientCfg(t *testing.T, addr string) *vault.Config {
	t.Helper()
	cfg := vault.DefaultConfig()
	if cfg == nil {
		t.Fatal("vault.DefaultConfig returned nil")
	}
	cfg.Address = addr
	// No retries: these tests exercise the checker's verdict, not the
	// client's retry policy, and retries would add seconds of backoff.
	cfg.MaxRetries = 0
	return cfg
}
