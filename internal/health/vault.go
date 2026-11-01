package health

import (
	"context"
	"fmt"

	vault "github.com/hashicorp/vault/api"
)

// VaultChecker verifies Vault is reachable, unsealed, and that the configured
// token is actually valid — a sealed Vault or a revoked token both mean the
// service cannot store or fetch provisioned credentials.
type VaultChecker struct {
	client *vault.Client
}

// NewVaultChecker wraps a Vault client. The client must already carry its
// token; the checker validates that token on every check.
func NewVaultChecker(client *vault.Client) *VaultChecker {
	return &VaultChecker{client: client}
}

// Name implements Checker.
func (c *VaultChecker) Name() string { return "vault" }

// Check implements Checker.
func (c *VaultChecker) Check(ctx context.Context) error {
	if _, err := c.client.Sys().HealthWithContext(ctx); err != nil {
		return fmt.Errorf("vault health: %w", err)
	}
	if _, err := c.client.Auth().Token().LookupSelfWithContext(ctx); err != nil {
		return fmt.Errorf("vault token lookup: %w", err)
	}
	return nil
}
