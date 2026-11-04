package adapters

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	vault "github.com/hashicorp/vault/api"
)

// Secrets is the credential-storage concern. The payload is a plain map of
// strings on purpose: no Vault type leaks into the interface, so a later
// phase can move the implementation to another backend without touching
// workflow semantics.
type Secrets interface {
	Put(ctx context.Context, envID string, payload map[string]string) error
	Get(ctx context.Context, envID string) (map[string]string, error)
	Delete(ctx context.Context, envID string) error
}

// ErrSecretNotFound and ErrSecretMalformed distinguish a missing path from a
// present path whose document cannot be used. Reconciliation must never call
// malformed data "absent" and skip its cleanup.
var (
	ErrSecretNotFound  = errors.New("adapters: secret not found")
	ErrSecretMalformed = errors.New("adapters: malformed secret document")
)

// VaultSecrets is the local Secrets implementation: one KV-v2 path per
// environment, values written as a JSON document under `payload`. The
// backends's versioning already makes repeated writes converge to one current
// value, and the executor only writes when credentials are missing, so two
// Puts for one environment leave one path and one value.
type VaultSecrets struct {
	client *vault.Client
	// Mount is the KV-v2 mount ("secret" matches the dev-mode stack default).
	Mount string
	// Log is optional; a discard logger is used when nil.
	Log *slog.Logger
}

// NewVaultSecrets builds a Secrets adapter on an authenticated Vault client.
func NewVaultSecrets(client *vault.Client, mount string) *VaultSecrets {
	if mount == "" {
		mount = "secret"
	}
	return &VaultSecrets{client: client, Mount: mount, Log: slog.New(slog.DiscardHandler)}
}

// putDataKey is the single top-level key of the stored document.
const putDataKey = "payload"

// Put writes the payload. It is safe to call twice for one environment: the
// same input produces the same stored document at the same path. Superseded
// versions are destroyed right after the write, so two Puts for one
// environment still leave exactly one path and one stored value — the crash
// mid-write cannot lose data (the destroy only ever removes old versions) or
// wedge the retry (destroying absent versions is a no-op).
func (s *VaultSecrets) Put(ctx context.Context, envID string, payload map[string]string) error {
	secret, err := s.client.KVv2(s.Mount).Put(ctx, SecretPath(envID), map[string]any{
		putDataKey: payload,
	})
	if err != nil {
		return fmt.Errorf("adapters: put secret for %s: %w", envID, err)
	}
	if secret != nil && secret.VersionMetadata != nil {
		s.destroySuperseded(ctx, envID, secret.VersionMetadata.Version)
	}
	// Audit: action and identifiers only. Payload keys are logged; values —
	// in particular the password — never are.
	s.audit("secret stored", envID, payload)
	return nil
}

// destroySuperseded prunes versions below the just-written one so repeated
// idempotent writes never accumulate divergent credential copies. It is
// best-effort on purpose: a failure here leaves extra history in Vault, but
// Get must keep working, so the error is logged, not propagated.
func (s *VaultSecrets) destroySuperseded(ctx context.Context, envID string, vid int) {
	if vid <= 1 {
		return
	}
	older := make([]int, 0, vid-1)
	for v := 1; v < vid; v++ {
		older = append(older, v)
	}
	if err := s.client.KVv2(s.Mount).Destroy(ctx, SecretPath(envID), older); err != nil {
		s.Log.Warn("secrets adapter: could not destroy superseded versions",
			"env_id", envID, "versions", older, "err", err)
	}
}

// Get reads the stored payload. A path that has never been written (or was
// fully deleted) returns an error wrapping ErrSecretNotFound.
func (s *VaultSecrets) Get(ctx context.Context, envID string) (map[string]string, error) {
	secret, err := s.client.KVv2(s.Mount).Get(ctx, SecretPath(envID))
	switch {
	case err != nil:
		// The KV backend reports missing paths as a 404 grouped under
		// ErrSecretNotFound by the API's response handler wrap; keep both
		// readable.
		if errors.Is(err, vault.ErrSecretNotFound) {
			return nil, fmt.Errorf("adapters: get secret for %s: %w", envID, ErrSecretNotFound)
		}
		return nil, fmt.Errorf("adapters: get secret for %s: %w", envID, err)
	case secret == nil:
		return nil, fmt.Errorf("adapters: get secret for %s: %w", envID, ErrSecretNotFound)
	case secret.Data == nil:
		return nil, fmt.Errorf("adapters: get secret for %s: %w", envID, ErrSecretMalformed)
	}
	raw, ok := secret.Data["payload"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("adapters: get secret for %s: unexpected document shape: %w", envID, ErrSecretMalformed)
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		str, isStr := v.(string)
		if !isStr {
			return nil, fmt.Errorf("adapters: get secret for %s: key %q is %T: %w", envID, k, v, ErrSecretMalformed)
		}
		out[k] = str
	}
	return out, nil
}

// Delete removes the secret's data. Deleting a path that is not there is
// success: cleanup retries after crashes and must not wedge on absent
// resources. (Metadata deletion is not used so an operator can still audit
// that a deletion happened; phase 7 reconciles residuals.)
func (s *VaultSecrets) Delete(ctx context.Context, envID string) error {
	if err := s.client.KVv2(s.Mount).Delete(ctx, SecretPath(envID)); err != nil {
		return fmt.Errorf("adapters: delete secret for %s: %w", envID, err)
	}
	s.audit("secret deleted", envID, nil)
	return nil
}

// audit is the audit line for secret operations. It logs the action, the
// environment id, and the payload's key names (which identify what was
// stored) — never any value, so a password or connection string cannot end
// up in a log line.
func (s *VaultSecrets) audit(action, envID string, payload map[string]string) {
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	s.Log.Info("secrets adapter",
		"action", action, "env_id", envID, "payload_keys", strings.Join(keys, ","), "mount", s.Mount)
}

// StoredVersions reports which versions of the environment's secret are
// still live in Vault. It is a reconciliation/verification aid, not part of
// the Secrets interface, so backend-specific shape stays local here.
func (s *VaultSecrets) StoredVersions(ctx context.Context, envID string) ([]int, error) {
	versions, err := s.client.KVv2(s.Mount).GetVersionsAsList(ctx, SecretPath(envID))
	if err != nil {
		if errors.Is(err, vault.ErrSecretNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("adapters: list versions for %s: %w", envID, err)
	}
	out := make([]int, 0, len(versions))
	for _, v := range versions {
		// A version is current-STATE live when not destroyed and not
		// deleted (DeletionTime at the zero time).
		if !v.Destroyed && v.DeletionTime.IsZero() {
			out = append(out, v.Version)
		}
	}
	return out, nil
}
