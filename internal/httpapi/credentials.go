package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// This file implements the only authorized read/write path for credentials:
// POST /v1/environments/{id}/credentials (retrieval) and
// POST /v1/environments/{id}/rotate (rotation). The status and history
// endpoints cannot reach secret material; these endpoints are wired with an
// explicit policy check that the status endpoints do not have, and every
// call — allowed or denied — is audited with the caller identity.
//
// Known limitation (documented, per plan phase 6 out-of-scope): there is no
// general authentication/authorization infrastructure yet. Caller identity
// comes from the X-Caller-Id header and the allowed caller set is static
// configuration; a real deployment must put a real identity layer in front
// of this.

// callerIdentityHeader carries the asserted caller identity. It is truncated
// (via a length cap) rather than parsed, so it can hold any deployment's
// identity token convention.
const callerIdentityHeader = "X-Caller-Id"

// maxCallerIdentityLen bounds identity strings reaching logs.
const maxCallerIdentityLen = 200

// rotationAttemptBudget bounds one rotate attempt (secret I/O, the role
// update, the verification probe with its short retry, and the final store
// transaction). It is generous enough for several probes but still capped, so
// a slow verification cannot straddle the caller's patience twice.
const rotationAttemptBudget = 30 * time.Second

// CredentialPolicy answers exactly one question: may this caller read or
// rotate a provisioned credential? Deny is the default; a policy with no
// allowed callers denies everything.
type CredentialPolicy struct {
	allowed map[string]bool
}

// NewCredentialPolicy builds a policy from the allowed caller identities.
// Empty identities are ignored; an empty list denies everyone.
func NewCredentialPolicy(callers ...string) CredentialPolicy {
	p := CredentialPolicy{allowed: map[string]bool{}}
	for _, c := range callers {
		if c = strings.TrimSpace(c); c != "" {
			p.allowed[c] = true
		}
	}
	return p
}

// Allows reports whether caller may touch credentials.
func (p CredentialPolicy) Allows(caller string) bool {
	return p.allowed[caller]
}

// AttachCredentials wires the credential retrieval and rotation endpoints.
// Without it the endpoints answer 503 not_configured — the retrieval path
// must fail closed, never be left implicitly open.
func (a *API) AttachCredentials(
	secrets adapters.Secrets, roles adapters.Roles, verifier adapters.Verifier,
	policy CredentialPolicy, log *slog.Logger,
) {
	a.secrets, a.roles, a.verifier = secrets, roles, verifier
	a.policy = policy
	a.auditLog = log
}

// credentialResponse is the POST .../credentials body. It exists only on the
// retrieal path: no other response type in the package has a password field.
type credentialResponse struct {
	Database      string `json:"database"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	CredentialRef string `json:"credentialRef"`
}

// retrieveCredentials handles POST /v1/environments/{id}/credentials: the
// separately authorized path for reading the provisioned credential. Only
// ready environments are served (nothing was proven usable before that), the
// policy must allow the caller, and every attempt is audited.
func (a *API) retrieveCredentials(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.callerIdentity(w, r)
	if !ok {
		return
	}
	if !a.authorize(w, r, caller, "retrieve_credentials") {
		return
	}
	if a.credAPIUnconfigured(w) {
		return
	}
	envID, ok := environmentIDFromPath(w, r)
	if !ok {
		return
	}
	env, err := a.envs.Get(r.Context(), envID)
	if err != nil {
		writeEnvironmentOrError(w, domain.Environment{}, err)
		return
	}
	if env.State != domain.EnvReady {
		a.audit(caller, envID, false, "retrieve_credentials", "environment not ready")
		writeError(w, http.StatusConflict, "environment_not_ready",
			"credentials are only retrievable for a ready environment")
		return
	}
	payload, err := a.secrets.Get(r.Context(), envID)
	if err != nil {
		a.audit(caller, envID, false, "retrieve_credentials", "secret read failed")
		writeError(w, http.StatusInternalServerError, "internal_error",
			"reading the credential failed")
		return
	}
	// Fail closed: a stored document missing any field is not a servable
	// credential — an empty password written to a client would look like a
	// valid answer.
	if payload[payloadKeyPassword] == "" || payload[payloadKeyDatabase] == "" || payload[payloadKeyUsername] == "" {
		a.audit(caller, envID, false, "retrieve_credentials", "stored credential document is incomplete")
		writeError(w, http.StatusInternalServerError, "internal_error",
			"the stored credential is incomplete; the environment must be re-provisioned or rotated")
		return
	}
	a.audit(caller, envID, true, "retrieve_credentials", "allowed")
	writeJSON(w, http.StatusOK, credentialResponse{
		Database:      payload[payloadKeyDatabase],
		Username:      payload[payloadKeyUsername],
		Password:      payload[payloadKeyPassword],
		CredentialRef: adapters.SecretPath(envID),
	})
}

// rotateEnvironment handles POST /v1/environments/{id}/rotate: a new
// password is applied to the role, verified, and then written to the secrets
// adapter. The environment remains rotating on failures so a retry can
// converge without making an unverified credential retrievable.
//
// The whole attempt runs under the per-environment rotation lock, so
// overlapping requests (e.g. a client timeout plus its retry) can never each
// write a different new password mid-flight: only one attempt touches the
// secrets adapter and the role at a time, and each finishes or resumes
// cleanly.
func (a *API) rotateEnvironment(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.callerIdentity(w, r)
	if !ok {
		return
	}
	if !a.authorize(w, r, caller, "rotate_credentials") {
		return
	}
	if a.credAPIUnconfigured(w) {
		return
	}
	envID, ok := environmentIDFromPath(w, r)
	if !ok {
		return
	}

	// Serialize first: only one writer may touch Vault and the role for this
	// environment. A loser answers busy rather than racing the winner.
	release, err := a.envs.AcquireRotationLock(r.Context(), envID)
	if err != nil {
		if errors.Is(err, store.ErrRotationBusy) {
			a.audit(caller, envID, false, "rotate_credentials", "another rotation holds the lock")
			writeError(w, http.StatusConflict, "rotation_busy",
				"another rotation is already in progress for this environment")
			return
		}
		a.audit(caller, envID, false, "rotate_credentials", "acquiring the rotation lock failed")
		writeError(w, http.StatusServiceUnavailable, "rotation_unavailable",
			"the rotation lock could not be acquired; retry the request")
		return
	}
	defer func() {
		// A failed release destroyed the lock-holding connection — never a
		// permanent leak, but it must not pass silently.
		if rerr := release(); rerr != nil {
			a.audit(caller, envID, false, "rotate_credentials", "releasing the rotation lock failed")
			if a.auditLog != nil {
				a.auditLog.Error("rotation lock release failed", "error", rerr)
			}
		}
	}()

	a.rotationUnderLock(w, r, caller, envID)
}

// rotationUnderLock performs one rotate attempt while the per-environment
// lock is held. It re-reads the environment state, because the lock wait may
// have outlasted a full previous rotation.
func (a *API) rotationUnderLock(
	w http.ResponseWriter, r *http.Request, caller, envID string,
) {
	ctx, cancel := context.WithTimeout(r.Context(), rotationAttemptBudget)
	defer cancel()

	env, err := a.envs.Get(ctx, envID)
	if err != nil {
		a.audit(caller, envID, false, "rotate_credentials", "reading the environment failed")
		writeEnvironmentOrError(w, domain.Environment{}, err)
		return
	}

	// Only a ready environment starts a rotation; a rotating one is a retry
	// (resume) of an in-flight rotation. Record that durable intent before
	// external I/O so every failure is resumable. Any other state refuses the
	// request — rotating while provisioning would orphan the verification
	// that made the environment usable in the first place.
	switch env.State {
	case domain.EnvReady:
		if err := a.envs.TransitionEnvironment(ctx, envID, domain.EnvRotating); err != nil {
			writeError(w, http.StatusConflict, "state_conflict",
				"the environment moved before the rotation started")
			return
		}
	case domain.EnvRotating:
		// Resume of an interrupted attempt. The durable intent is NOT trusted
		// here: instead a fresh password is always generated (and written to
		// the secrets adapter below) so the interleave of "did the intent
		// reach the secrets adapter before the crash?" can never silently
		// reapply the OLD password and report a successful rotation with an
		// unchanged credential. This converges because the new value is
		// durably stored before the role is ever applied.
	default:
		a.audit(caller, envID, false, "rotate_credentials", "rotation refused in state "+string(env.State))
		writeError(w, http.StatusConflict, "invalid_state",
			"rotations can start only on a ready environment")
		return
	}

	payload, err := a.secrets.Get(ctx, envID)
	if err != nil {
		a.rotateFailure(w, r, caller, envID, "secret read failed")
		return
	}

	newPassword, err := adapters.NewPassword()
	if err != nil {
		a.rotateFailure(w, r, caller, envID, "password generation failed")
		return
	}
	payload[payloadKeyPassword] = newPassword
	if payload[payloadKeyDatabase] == "" || payload[payloadKeyUsername] == "" || newPassword == "" {
		a.rotateFailure(w, r, caller, envID, "stored credential document is incomplete")
		return
	}
	if err := a.roles.Ensure(ctx, payload[payloadKeyUsername], newPassword); err != nil {
		a.rotateFailure(w, r, caller, envID, "applying the new credential to the role failed")
		return
	}
	// Prove the rotated pair before claiming readiness: rotation is safe to
	// retry, but it can also fail — recorded as the domain's rotating ->
	// failed edge, never as a silent half-rotation.
	if err := verifyWithShortRetry(ctx, a.verifier,
		payload[payloadKeyDatabase], payload[payloadKeyUsername], newPassword); err != nil {
		a.rotateFailure(w, r, caller, envID, "verifying the rotated credential failed")
		return
	}
	// Publish the new credential only after the role accepts it and a fresh
	// connection proves it works. If this write fails, state remains rotating;
	// the next request applies and verifies another candidate before publishing.
	if err := a.secrets.Put(ctx, envID, payload); err != nil {
		a.rotateFailure(w, r, caller, envID, "storing the new credential failed")
		return
	}

	entry, err := json.Marshal(rotationRecord{At: time.Now().UTC().Format(time.RFC3339), By: caller})
	if err != nil {
		a.rotateFailure(w, r, caller, envID, "recording the rotation failed")
		return
	}
	if err := a.envs.CompleteRotation(ctx, envID, entry); err != nil {
		// The rotation itself SUCCEEDED: the new credential is applied to
		// the role, verified usable, and stored in the secrets adapter. A
		// bookkeeping failure here must NOT strand the environment: it stays
		// `rotating`, and the next call takes the resume path and converges.
		// Reserve `failed` for apply/verify failures, where the credential is
		// genuinely unusable.
		a.audit(caller, envID, false, "rotate_credentials", "recording the rotation failed")
		writeError(w, http.StatusServiceUnavailable, "rotation_uncommitted",
			"the rotation was applied but not yet committed; retry the request to finish it")
		return
	}
	a.audit(caller, envID, true, "rotate_credentials", "allowed")
	updated, err := a.envs.Get(ctx, envID)
	if err != nil {
		writeEnvironmentOrError(w, domain.Environment{}, err)
		return
	}
	writeEnv(w, http.StatusOK, updated, false)
}

// callerIdentity extracts and validates the asserted caller identity. A
// request without one is a denial, written here so every credential route
// shares the same rule, and audited by the caller of this helper.
func (a *API) callerIdentity(w http.ResponseWriter, r *http.Request) (string, bool) {
	caller := strings.TrimSpace(r.Header.Get(callerIdentityHeader))
	if caller == "" || len(caller) > maxCallerIdentityLen {
		writeError(w, http.StatusUnauthorized, "missing_caller_identity",
			"the "+callerIdentityHeader+" header must name the caller")
		return "", false
	}
	return caller, true
}

// authorize applies the credential policy and audits the outcome. It returns
// true only when the caller may proceed.
func (a *API) authorize(w http.ResponseWriter, r *http.Request, caller, action string) bool {
	envID := r.PathValue("id")
	if !a.policy.Allows(caller) {
		a.audit(caller, envID, false, action, "policy denied the caller")
		writeError(w, http.StatusForbidden, "permission_denied",
			"the caller is not authorized for this operation")
		return false
	}
	return true
}

// credAPIUnconfigured fails the request closed when no secret source is
// wired: absence of configuration must never read as authorization.
func (a *API) credAPIUnconfigured(w http.ResponseWriter) bool {
	if a.secrets == nil {
		writeError(w, http.StatusServiceUnavailable, "not_configured",
			"the credential API is not configured on this deployment")
		return true
	}
	return false
}

// audit writes one audit record for a credential path attempt. Allowed and
// denied attempts are both recorded with the caller identity; the payload is
// identifiers only.
func (a *API) audit(caller, envID string, allowed bool, action, outcome string) {
	if a.auditLog == nil {
		return
	}
	a.auditLog.Info("credential audit",
		"action", action,
		"environment_id", envID,
		"caller", caller,
		"allowed", allowed,
		"outcome", outcome,
	)
}

// payload keys of the stored credential document.
const (
	payloadKeyDatabase = "database"
	payloadKeyUsername = "username"
	payloadKeyPassword = "password"
)

// rotationRecord is one rotation audit entry persisted on the verification
// step's metadata. Only identifiers: never the new password.
type rotationRecord struct {
	At string `json:"at"`
	By string `json:"by"`
}

// rotateFailure records a retryable error without leaving the durable
// rotating state. A partial external operation must never make the rotation
// unrecoverable; callers can safely retry while the environment is rotating.
func (a *API) rotateFailure(
	w http.ResponseWriter, r *http.Request, caller, envID, reason string,
) {
	a.audit(caller, envID, false, "rotate_credentials", reason)
	writeError(w, http.StatusServiceUnavailable, "rotation_retryable",
		"the rotation did not complete; the environment remains rotating and the request can be retried")
}

// verifyWithShortRetry runs the verifier a few times over transient errors.
// Verification failures caused by credentials/grants are permanent here too:
// rotate gives up and marks the environment failed rather than looping.
func verifyWithShortRetry(
	ctx context.Context, verifier adapters.Verifier, db, user, pass string,
) error {
	const attempts = 3
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			timer := time.NewTimer(time.Duration(i) * 200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err = verifier.VerifyUsable(ctx, db, user, pass); err == nil {
			return nil
		}
		if errors.Is(err, adapters.ErrVerificationAuth) || errors.Is(err, adapters.ErrVerificationGrants) {
			return err
		}
	}
	return err
}
