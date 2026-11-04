//go:build integration

// Integration tests for the Vault-backed Secrets adapter against the compose
// dev-mode Vault (vault:8200, root token by default).
package adapters_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	vault "github.com/hashicorp/vault/api"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
)

func newVaultSecrets(t *testing.T) *adapters.VaultSecrets {
	t.Helper()
	cfg := vault.DefaultConfig()
	// vault.DefaultConfig falls back to https; the compose stack is dev-mode
	// http on localhost. Only TEST_VAULT_ADDR overrides the local default.
	cfg.Address = "http://127.0.0.1:8200"
	if addr := os.Getenv("TEST_VAULT_ADDR"); addr != "" {
		cfg.Address = addr
	}
	client, err := vault.NewClient(cfg)
	if err != nil {
		t.Skipf("vault client error, skipping: %v", err)
	}
	if token := os.Getenv("TEST_VAULT_TOKEN"); token != "" {
		client.SetToken(token)
	} else {
		client.SetToken("root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Sys().HealthWithContext(ctx); err != nil {
		t.Skipf("vault not reachable, skipping: %v", err)
	}
	return adapters.NewVaultSecrets(client, "secret")
}

func TestVaultSecretsIdempotentPutAndDelete(t *testing.T) {
	sec := newVaultSecrets(t)
	envID := uniqueEnvID(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// No secret yet: missing paths are queryable, not a mystery error.
	if _, err := sec.Get(ctx, envID); !errors.Is(err, adapters.ErrSecretNotFound) {
		t.Fatalf("get before put: got %v, want ErrSecretNotFound", err)
	}

	payload := map[string]string{
		"database": adapters.DatabaseName(envID),
		"username": adapters.RoleName(envID),
		"password": "cr3d-never-in-logs",
	}
	if err := sec.Put(ctx, envID, payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Two Puts for one environment: still one path, one current value.
	if err := sec.Put(ctx, envID, payload); err != nil {
		t.Fatalf("second put: %v", err)
	}

	got, err := sec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("get after puts: %v", err)
	}
	for _, k := range []string{"database", "username", "password"} {
		if got[k] != payload[k] {
			t.Errorf("key %q = %q, want %q", k, got[k], payload[k])
		}
	}
	if len(got) != len(payload) {
		t.Errorf("payload has %d keys, want %d", len(got), len(payload))
	}

	// Exactly one version survives the second Put (superseded ones are
	// destroyed by the adapter).
	live, err := sec.StoredVersions(ctx, envID)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(live) != 1 {
		t.Errorf("%d live versions after two Puts, want exactly 1", len(live))
	}

	if err := sec.Delete(ctx, envID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Deleting the same path twice is cleanup-safe.
	if err := sec.Delete(ctx, envID); err != nil {
		t.Fatalf("second delete must be idempotent: %v", err)
	}
	if _, err := sec.Get(ctx, envID); !errors.Is(err, adapters.ErrSecretNotFound) {
		t.Errorf("get after delete: got %v, want ErrSecretNotFound", err)
	}
}
