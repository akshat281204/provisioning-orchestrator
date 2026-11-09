// executor.go — the phase-5 Executor: it dispatches each step kind to the
// real adapters from internal/adapters. Resource names are derived from the
// environment id only (adapters/naming.go), so a step reclaimed after a crash
// re-derives the same database, role, and secret path instead of inventing
// new ones; combined with idempotent adapter operations this makes every
// execution at-least-once safe.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// AdapterExecutor runs a claimed step through the capacity/database/role/
// secrets adapters. The Envs gate (a store-backed view of the environment's
// state machine) keeps `ready` reachable only through the verification step;
// nil in tests that exercise adapters alone.
type AdapterExecutor struct {
	// Capacity bounds concurrent provisioned environments.
	Capacity adapters.Capacity
	// Databases provisions the environment's own database.
	Databases adapters.Databases
	// Roles provisions the environment's login role.
	Roles adapters.Roles
	// Secrets stores the environment's credentials in Vault.
	Secrets adapters.Secrets
	// Verifier proves the provisioned role can use its database; a nil
	// Verifier fails every verify_usable execution loudly.
	Verifier adapters.Verifier
	// Envs advances the environment's state machine alongside step
	// progress; nil means "not wired in this test" and skips the gate.
	Envs EnvGate
	// VerifyBudget overrides the transient-retry window of verify_usable;
	// zero means the default deadline. Tests shorten it so a retry-policy
	// test stays fast; production code never sets it.
	VerifyBudget time.Duration
}

// EnvGate is the executor's window on the environment state machine. Every
// method is idempotent and store-guarded: a state move that the domain
// rejects never reaches SQL, and a CAS that lost a race is retried inside
// the implementation.
type EnvGate interface {
	// BeginProvisioning moves pending -> provisioning when the plan starts;
	// no-op when the environment is already beyond pending.
	BeginProvisioning(ctx context.Context, environmentID string) error
	// BeginVerification moves the environment to verifying (via
	// provisioning for a pending environment). Only `verifying` environments
	// may later become ready, so verification must not start without it.
	BeginVerification(ctx context.Context, environmentID string) error
	// WriteConnectionMeta merges the non-secret connection facts (database,
	// username, credentialRef) into the environment's metadata. It runs
	// only after verification succeeded, and every retry converges on the
	// same values because the names derive from the environment id.
	WriteConnectionMeta(ctx context.Context, environmentID string, meta []byte) error
}

// Execute dispatches one step by kind. Every adapter call is idempotent, so
// re-execution after a crash converges to the same external state.
func (e *AdapterExecutor) Execute(ctx context.Context, step domain.Step) error {
	switch step.Kind {
	case domain.KindReserveCapacity:
		if err := e.beginProvisioning(ctx, step.EnvironmentID); err != nil {
			return err
		}
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
		return e.verifyUsable(ctx, step)

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
	payloadKeyDatabase = "database"
	payloadKeyUsername = "username"
	payloadKeyPassword = "password"
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

	password, err := adapters.NewPassword()
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

// beginProvisioning is the nil-safe gate for the first plan step: running
// real work flips the environment out of pending, so `verifying` can never
// be the first state a step observes.
func (e *AdapterExecutor) beginProvisioning(ctx context.Context, envID string) error {
	if e.Envs == nil {
		return nil
	}
	if err := e.Envs.BeginProvisioning(ctx, envID); err != nil {
		return fmt.Errorf("worker: begin provisioning %s: %w", envID, err)
	}
	return nil
}

// verifyDeadline bounds the transient-failure retry window of one
// verify_usable execution, and firstVerifyBackoff/maxVerifyBackoff shape the
// exponential sleep between attempts. The whole window must stay comfortably
// below the pool's StepTimeout so a hanged verifier is bounded by its own
// deadline before the step's lease is ever at risk.
const (
	verifyDeadline     = 10 * time.Second
	firstVerifyBackoff = 250 * time.Millisecond
	maxVerifyBackoff   = 1 * time.Second
)

// verifyBudget returns the executor's configured retry window.
func (e *AdapterExecutor) verifyBudget() time.Duration {
	if e.VerifyBudget > 0 {
		return e.VerifyBudget
	}
	return verifyDeadline
}

// verifyUsable is the readiness gate. Ready means proven usable: the stored
// credential is read back from Vault, a fresh connection is opened to the
// provisioned database as the provisioned role, and `select 1` plus a
// catalog check confirm the grants are effective. Only the transaction that
// records this step's `done` (the pool's CompleteVerification call) can then
// mark the environment ready. Transient errors retry with backoff to the
// bounded deadline; an authentication failure stops immediately because
// retrying bad credentials never converges.
func (e *AdapterExecutor) verifyUsable(ctx context.Context, step domain.Step) error {
	if e.Verifier == nil {
		return fmt.Errorf("worker: verify %s: no verifier configured", step.EnvironmentID)
	}
	payload, err := e.Secrets.Get(ctx, step.EnvironmentID)
	if err != nil {
		return fmt.Errorf("worker: verify %s: read credentials: %w", step.EnvironmentID, err)
	}
	db, user, pass := payload[payloadKeyDatabase], payload[payloadKeyUsername], payload[payloadKeyPassword]
	if db == "" || user == "" || pass == "" {
		return fmt.Errorf("worker: verify %s: stored payload lacks database/username/password", step.EnvironmentID)
	}

	if e.Envs != nil {
		if err := e.Envs.BeginVerification(ctx, step.EnvironmentID); err != nil {
			return fmt.Errorf("worker: verify %s: readiness gate: %w", step.EnvironmentID, err)
		}
	}

	if err := e.verifyWithRetry(ctx, step.EnvironmentID, db, user, pass); err != nil {
		return err
	}

	// The non-secret connection facts reach the environment row so status
	// responses can name database and credentialRef. Writing them before the
	// step completes is safe: values derive from the environment id, so
	// every retry writes the same document.
	meta, err := connectionMetaJSON(step.EnvironmentID, payload)
	if err != nil {
		return fmt.Errorf("worker: verify %s: %w", step.EnvironmentID, err)
	}
	if e.Envs != nil {
		if err := e.Envs.WriteConnectionMeta(ctx, step.EnvironmentID, meta); err != nil {
			return fmt.Errorf("worker: verify %s: record connection metadata: %w", step.EnvironmentID, err)
		}
	}
	return nil
}

// verifyWithRetry implements the retry policy: transient errors (network,
// server unavailable, timeouts) are retried with exponential backoff until
// the bounded deadline; ErrVerificationAuth and ErrVerificationGrants are
// permanent — the probe stops and the step fails instead of burning the
// retry budget against a password mismatch that can never converge.
func (e *AdapterExecutor) verifyWithRetry(ctx context.Context, envID, db, user, pass string) error {
	deadline := time.Now().Add(e.verifyBudget())
	backoff := firstVerifyBackoff
	var attempts int
	for {
		attempts++
		err := e.Verifier.VerifyUsable(ctx, db, user, pass)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, adapters.ErrVerificationAuth):
			return fmt.Errorf("worker: verify %s: after %d attempt(s), %w: not retried", envID, attempts, err)
		case errors.Is(err, adapters.ErrVerificationGrants):
			return fmt.Errorf("worker: verify %s: %w", envID, err)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("worker: verify %s: %w", envID, ctx.Err())
		}
		if time.Now().Add(backoff).After(deadline) {
			return fmt.Errorf("worker: verify %s: %d attempt(s) exhausted the retry deadline: %w", envID, attempts, err)
		}
		if !sleepCtx(ctx, backoff) {
			return fmt.Errorf("worker: verify %s: %w", envID, ctx.Err())
		}
		backoff *= 2
		if backoff > maxVerifyBackoff {
			backoff = maxVerifyBackoff
		}
	}
}

// sleepCtx waits d, returning false when ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// connectionMeta marshals the non-secret connection facts an environment
// row and its status endpoint carry. Password goes nowhere near it.
func connectionMetaJSON(envID string, payload map[string]string) ([]byte, error) {
	meta := map[string]string{
		"database":      payload[payloadKeyDatabase],
		"username":      payload[payloadKeyUsername],
		"credentialRef": adapters.SecretPath(envID),
	}
	out, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal connection metadata: %w", err)
	}
	return out, nil
}
