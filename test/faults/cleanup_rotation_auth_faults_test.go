//go:build integration

// cleanup_rotation_auth_faults_test.go — scenarios 7, 8, 10: faults in the
// cleanup plan, the rotation commit path, and the authorization gate.
package faults

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/httpapi"
	"github.com/akshat281204/provisioning-orchestrator/internal/worker"
)

// TestFault7CleanupPartialSuccessThenFailure drives failure case 7: cleanup
// partially succeeds (the database is dropped, its effect is durable and
// visible), then the next compensation step (drop_role) fails once. The
// retry policy must recover the failed step, and the final verdict must be
// `deleted` with every external resource gone.
func TestFault7CleanupPartialSuccessThenFailure(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault7-cleanup-partial")
	stopProvisioning := rig.startPool(t, poolConfig{})
	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the environment to become ready before cleanup starts")

	// External resources exist now; assert that so "partially succeeds" is
	// a statement about observed state, not an assumption.
	ctx := context30sCtx(t)
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 1 {
		t.Fatalf("no database before cleanup: %d", n)
	}
	if _, err := rig.vaultSec.Get(ctx, envID); err != nil {
		t.Fatalf("no secret before cleanup: %v", err)
	}

	// Delete (the only legal cleanup edge from `ready`): 202, then the
	// compensation plan runs. The provisioning pool is stopped and drained
	// *before* the delete so its pool cannot claim a cleanup step in the
	// window between the 202 and the shutdown; the faulted pool then takes
	// over so the role drop fails once.
	stopProvisioning()
	code, resp := rig.call(t, http.MethodDelete, "/environments/"+envID, "", "")
	if code != http.StatusAccepted {
		t.Fatalf("delete: %d %v", code, resp)
	}

	faulted := &flakyRoles{Roles: rig.roles}
	faulted.dropLeft.Store(1)
	rig.startPool(t, poolConfig{
		exec: &worker.AdapterExecutor{
			Capacity:  rig.capacity,
			Databases: rig.dbs,
			Roles:     faulted,
			Secrets:   rig.vaultSec,
			Verifier:  adapters.NewLocalVerifier(rig.database),
			Envs:      rig.st,
		},
	})

	waitFor(t, func() bool { return rig.envState(t, envID) == domain.EnvDeleted },
		"the cleanup to recover from the failed drop and conclude")
	if n := rig.countTimeline(t, envID, "deleted"); n != 1 {
		t.Fatalf("deleted recorded %d times, want 1", n)
	}

	// Persisted truth: drop_role failed at least once before it was recorded
	// done; drop_database succeeded on its first and only attempt — the
	// partial success is visible on the row.
	roleDrop := rig.stepBy(t, envID, domain.KindDropRole)
	if roleDrop.State != "done" {
		t.Fatalf("drop_role = %s, want done after recovery", roleDrop.State)
	}
	if roleDrop.Attempts < 2 {
		t.Fatalf("drop_role attempts = %d, want >= 2 (fail, then recover)", roleDrop.Attempts)
	}
	if got := rig.stepBy(t, envID, domain.KindDropDatabase); got.State != "done" || got.Attempts != 1 {
		t.Fatalf("drop_database = %s with %d attempts, want done/1", got.State, got.Attempts)
	}

	// External truth: nothing left behind.
	if n := rig.countDatabase(t, adapters.DatabaseName(envID)); n != 0 {
		t.Fatalf("cleanup left the database behind")
	}
	if n := rig.countRole(t, adapters.RoleName(envID)); n != 0 {
		t.Fatalf("cleanup left the role behind")
	}
	if _, err := rig.vaultSec.Get(ctx, envID); !errIsNotFoundSecret(err) {
		t.Fatalf("cleanup left the credential secret behind: %v", err)
	}
	if n := rig.countReservations(t, envID); n != 0 {
		t.Fatalf("cleanup left a capacity reservation held")
	}
	// Ownership: nothing provisioned anywhere on the stack stands outside an
	// environment — the compensation's zero-left-behind claim is global.
	rig.assertResourcesOwned(t)
}

// TestFault8RotationSucceedsResponseLost drives failure case 8: the rotation
// applies the new credential remotely (role + Vault) but the commit — and
// with it the client's response — is lost. The environment stays honestly
// `rotating`; a retried rotation takes the resume path and converges. The
// remote truth must hold at every step: exactly one live stored credential,
// and only the currently published password connects.
func TestFault8RotationSucceedsResponseLost(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault8-rotation-lost")
	rig.startPool(t, poolConfig{})
	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the environment to become ready before rotating")

	// The original credential works.
	_, before := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", faultCaller, "")
	oldPassword, _ := before["password"].(string)
	user, _ := before["username"].(string)
	database, _ := before["database"].(string)
	if oldPassword == "" {
		t.Fatalf("no retrievable credential before rotation: %v", before)
	}
	connectOutside(t, rig.database, database, user, oldPassword)

	// An API whose first rotation commit fails: the remote side succeeded
	// (the role carries the new password, Vault carries it too), the commit
	// — and with it the response — is lost.
	uncommitted := &uncommittedStore{PostgresStore: rig.st}
	uncommitted.failRotation.Store(true)
	lostAPI := httpapi.New(uncommitted)
	lostAPI.AttachCredentials(rig.vaultSec, rig.roles, adapters.NewLocalVerifier(rig.database),
		httpapi.NewCredentialPolicy(faultCaller), rig.log)

	code, resp := rotateRequest(t, lostAPI, envID)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("lost rotation: %d %v", code, resp)
	}
	// The environment stays `rotating` — not ready, not failed.
	if state := rig.envState(t, envID); state != domain.EnvRotating {
		t.Fatalf("environment = %s after the lost commit, want rotating", state)
	}
	// The new credential is already live in Vault (stored before the commit).
	ctx := context30sCtx(t)
	stored, err := rig.vaultSec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("read stored secret: %v", err)
	}
	if stored["password"] == oldPassword {
		t.Fatalf("the rotation's remote work never reached Vault")
	}
	versions, err := rig.vaultSec.StoredVersions(ctx, envID)
	if err != nil || len(versions) != 1 {
		t.Fatalf("live stored versions = %v (err %v), want exactly one", versions, err)
	}

	// The client retries against the real API; the resume path converges.
	code, after := rig.call(t, http.MethodPost, "/environments/"+envID+"/rotate", faultCaller, "")
	if code != http.StatusOK {
		t.Fatalf("rotation retry: %d %v", code, after)
	}
	if state := rig.envState(t, envID); state != domain.EnvReady {
		t.Fatalf("environment = %s after the retried rotation, want ready", state)
	}

	// The published credential connects; the pre-rotation one does not.
	_, creds := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", faultCaller, "")
	newPassword, _ := creds["password"].(string)
	if newPassword == "" || newPassword == oldPassword {
		t.Fatalf("credential after rotation: %v", creds)
	}
	connectOutside(t, rig.database, database, user, newPassword)
	if err := oldPasswordRefused(t, rig.database, database, user, oldPassword); err == nil {
		t.Fatalf("the pre-rotation password still connects after rotation")
	}

	// Persisted truth: the rotation audit log on the verify step carries the
	// attempts, keyed by the caller identity.
	rotations := rotationLog(t, rig, envID)
	if len(rotations) < 1 {
		t.Fatalf("no rotation audit entries recorded")
	}
	for _, entry := range rotations {
		if entry["by"] != faultCaller {
			t.Fatalf("rotation audit entry without the caller identity: %v", entry)
		}
	}
}

// rotateRequest issues a rotate call against one API instance.
func rotateRequest(t *testing.T, api *httpapi.API, envID string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/environments/"+envID+"/rotate", strings.NewReader(""))
	req.Header.Set("X-Caller-Id", faultCaller)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal([]byte(rec.Body.String()), &out)
	return rec.Code, out
}

// oldPasswordRefused connects with the superseded password and reports the
// (expected) authentication error. It distinguishes a real password refusal
// (Postgres SQLSTATE 28P01) from a transient connection or query failure, so
// the assertion cannot pass for the wrong reason.
func oldPasswordRefused(t *testing.T, dsn, database, user, password string) error {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.Database, cfg.User, cfg.Password = database, user, password
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "28P01" {
			return err
		}
		t.Fatalf("connecting with the old password failed in an unexpected way (not an auth refusal): %v", err)
	}
	defer conn.Close(context.Background())
	var one int
	err = conn.QueryRow(context.Background(), `SELECT 1`).Scan(&one)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "28P01" {
		return err
	}
	t.Fatalf("querying with the old password failed in an unexpected way (not an auth refusal): %v", err)
	return nil
}

// TestFault10UnauthorizedCredentialRequest drives failure case 10: an
// unauthorized caller requests a credential. The response is a denial and —
// the point of asserting external state — nothing changes: no credential is
// served, the stored secret is untouched, and the denial is audited.
func TestFault10UnauthorizedCredentialRequest(t *testing.T) {
	rig := newRig(t)
	envID := rig.createEnv(t, "fault10-unauthorized")
	rig.startPool(t, poolConfig{})
	waitFor(t, func() bool { return rig.envIsReady(t, envID) },
		"the environment to become ready")

	// Baseline external state.
	ctx := context30sCtx(t)
	secretBefore, err := rig.vaultSec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("read stored secret: %v", err)
	}

	// Unauthorized caller: 403, and no credential in the body.
	code, body := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", "mallory", "")
	if code != http.StatusForbidden {
		t.Fatalf("unauthorized retrieval: %d %v", code, body)
	}
	if _, hasPassword := body["password"]; hasPassword {
		t.Fatalf("the denial must not carry a credential: %v", body)
	}

	// Missing identity: 401.
	code, body = rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", "", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous retrieval: %d %v", code, body)
	}

	// External truth: the secret is byte-identical (the whole payload, not
	// just one field), and only the authorized caller can read it afterwards.
	secretAfter, err := rig.vaultSec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("read stored secret after denial: %v", err)
	}
	if !maps.Equal(secretAfter, secretBefore) {
		t.Fatalf("a denied request must not touch the stored credential: before %v, after %v",
			secretBefore, secretAfter)
	}
	code, allowed := rig.call(t, http.MethodPost, "/environments/"+envID+"/credentials", faultCaller, "")
	if code != http.StatusOK || allowed["password"] != secretBefore["password"] {
		t.Fatalf("authorized retrieval after denials: %d %v", code, allowed)
	}

	// Persisted truth: the environment is still ready; nothing moved.
	if rig.envState(t, envID) != domain.EnvReady {
		t.Fatalf("environment = %s after denied requests, want ready", rig.envState(t, envID))
	}
	// The audit log names the denied callers and never the password.
	if !strings.Contains(rig.logs.String(), "mallory") {
		t.Fatalf("the denial was not audited with the caller identity")
	}
	if strings.Contains(rig.logs.String(), secretBefore["password"]) {
		t.Fatalf("the audit path leaked the password")
	}
}

// rotationLog reads the rotation audit entries persisted on the verify
// step's metadata.
func rotationLog(t *testing.T, rig *rig, envID string) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var raw []byte
	if err := rig.pool.QueryRow(ctx, `
SELECT metadata FROM workflow_steps
WHERE environment_id = $1 AND kind = $2`, envID, string(domain.KindVerifyUsable)).Scan(&raw); err != nil {
		t.Fatalf("read verify step metadata: %v", err)
	}
	var doc struct {
		Rotations []map[string]any `json:"rotations"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode rotation log: %v", err)
	}
	return doc.Rotations
}
