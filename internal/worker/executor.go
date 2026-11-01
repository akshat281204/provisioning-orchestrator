// executor.go — the phase-5 Executor: it dispatches each step kind to the
// real adapters from internal/adapters. Resource names are derived from the
// environment id only (adapters/naming.go), so a step reclaimed after a crash
// re-derives the same database, role, and secret path instead of inventing
// new ones; combined with idempotent adapter operations this makes every
// execution at-least-once safe.
package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// AdapterExecutor runs a claimed step through the four capacity/database/
// role/secrets adapters. It replaces NoopExecutor once the compose stack is
// up.
type AdapterExecutor struct {
	// Capacity bounds concurrent provisioned environments.
	Capacity adapters.Capacity
	// Databases provisions the environment's own database.
	Databases adapters.Databases
	// Roles provisions the environment's login role.
	Roles adapters.Roles
	// Secrets stores the environment's credentials in Vault.
	Secrets adapters.Secrets
}

// Execute dispatches one step by kind. Every adapter call is idempotent, so
// re-execution after a crash converges to the same external state.
func (e *AdapterExecutor) Execute(ctx context.Context, step domain.Step) error {
	switch step.Kind {
	case domain.KindReserveCapacity:
		return e.Capacity.Reserve(ctx, step.EnvironmentID)

	case domain.KindCreateDatabase:
		return e.Databases.Ensure(ctx, adapters.DatabaseName(step.EnvironmentID))

	case domain.KindStoreCreds:
		return e.storeCredentials(ctx, step.EnvironmentID)

	case domain.KindCreateRole:
		return e.createRole(ctx, step.EnvironmentID)

	case domain.KindGrantPrivs:
		return e.grantPrivileges(ctx, step.EnvironmentID)

	case domain.KindVerifyUsable:
		// Verification is phase 6 (readiness gate + credential retrieval);
		// failing loudly beats pretending the environment is ready.
		return fmt.Errorf("worker: step %s: verify_usable is implemented in phase 6", step.Kind)

	case domain.KindDropDatabase:
		return e.Databases.Drop(ctx, adapters.DatabaseName(step.EnvironmentID))

	case domain.KindDropRole:
		return e.Roles.Drop(ctx, adapters.RoleName(step.EnvironmentID))

	case domain.KindDeleteSecret:
		return e.Secrets.Delete(ctx, step.EnvironmentID)

	case domain.KindReleaseCapacity:
		return e.Capacity.Release(ctx, step.EnvironmentID)

	default:
		return fmt.Errorf("worker: missing executor for step kind %q", step.Kind)
	}
}

// Payload keys shared by store_credentials and the steps that read them.
const (
	payloadKeyDatabase  = "database"
	payloadKeyUsername  = "username"
	payloadKeyPassword  = "password"
	passwordCharsetName = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
)

// storeCredentials writes the environment's credentials to Vault exactly
// once: an existing secret is reused, so a retry keeps the first password
// instead of rotating credentials under a possibly-ready role. Nothing is
// logged or persisted by these steps but the identifiers.
func (e *AdapterExecutor) storeCredentials(ctx context.Context, envID string) error {
	_, err := e.Secrets.Get(ctx, envID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, adapters.ErrSecretNotFound) {
		return fmt.Errorf("worker: store credentials %s: check: %w", envID, err)
	}

	password, err := newPassword()
	if err != nil {
		return fmt.Errorf("worker: store credentials %s: %w", envID, err)
	}
	payload := map[string]string{
		payloadKeyDatabase: adapters.DatabaseName(envID),
		payloadKeyUsername: adapters.RoleName(envID),
		payloadKeyPassword: password,
	}
	if err := e.Secrets.Put(ctx, envID, payload); err != nil {
		return fmt.Errorf("worker: store credentials %s: %w", envID, err)
	}
	return nil
}

// createRole provisions the login role with the stored password. The
// credentials step ran earlier in the plan; a missing secret fails the step
// and the plan retries it from the top, never inventing a role-less grant.
func (e *AdapterExecutor) createRole(ctx context.Context, envID string) error {
	payload, err := e.Secrets.Get(ctx, envID)
	if err != nil {
		return fmt.Errorf("worker: create role %s: %w", envID, err)
	}
	if err := e.Roles.Ensure(ctx, payload[payloadKeyUsername], payload[payloadKeyPassword]); err != nil {
		return fmt.Errorf("worker: create role %s: %w", envID, err)
	}
	return nil
}

// grantPrivileges gives the environment's role connect and usage on its own
// database and nothing else. The database was hardened (PUBLIC access
// revoked) at creation, so repeat grants here are the role's entire access
// surface.
func (e *AdapterExecutor) grantPrivileges(ctx context.Context, envID string) error {
	payload, err := e.Secrets.Get(ctx, envID)
	if err != nil {
		return fmt.Errorf("worker: grant privileges %s: %w", envID, err)
	}
	db, user := payload[payloadKeyDatabase], payload[payloadKeyUsername]
	if db == "" || user == "" {
		return fmt.Errorf("worker: grant privileges %s: stored payload lacks database/username", envID)
	}
	if err := e.Roles.GrantConnect(ctx, user, db); err != nil {
		return fmt.Errorf("worker: grant privileges %s: %w", envID, err)
	}
	if err := e.Roles.GrantUsage(ctx, user, db); err != nil {
		return fmt.Errorf("worker: grant privileges %s: %w", envID, err)
	}
	return nil
}

// passwordLen bounds generated passwords: long enough to satisfy any
// reasonable policy, short enough for logs-never-write-you-anyway policies.
const passwordLen = 32

// newPassword produces a random alphanumeric password from crypto/rand.
// Only identifier-safe characters are used, so the Postgres DDL literal
// built around it needs no exotic escaping.
func newPassword() (string, error) {
	out := make([]byte, passwordLen)
	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(passwordCharsetName))))
		if err != nil {
			return "", fmt.Errorf("worker: generate password: %w", err)
		}
		out[i] = passwordCharsetName[n.Int64()]
	}
	return string(out), nil
}
