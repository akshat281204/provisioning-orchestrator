package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// The credential surface, offline: a ready environment, a stored secret with
// a known plaintext sentinel, configured roles/verifier, a policy, and an
// audit log that goes through the same redacting wrapper as every real log.
const envReadyID = "11111111-1111-4111-8111-111111111111"

const (
	sentinelPassword = "SENTINEL-plaintext-hunter2"
	allowedCaller    = "operator"
	deniedCaller     = "nobody"
)

// fakeSecrets implements adapters.Secrets in memory for the HTTP tests. It
// keeps one document per environment, mirroring Vault's KV shape.
type fakeSecrets struct {
	payload map[string]map[string]string
	puts    int
}

func newFakeSecrets(payload map[string]string) *fakeSecrets {
	return &fakeSecrets{payload: map[string]map[string]string{}}
}

func (s *fakeSecrets) Put(_ context.Context, envID string, p map[string]string) error {
	s.puts++
	out := map[string]string{}
	for k, v := range p {
		out[k] = v
	}
	s.payload[envID] = out
	return nil
}

func (s *fakeSecrets) Get(_ context.Context, envID string) (map[string]string, error) {
	p, ok := s.payload[envID]
	if !ok {
		return nil, fmt.Errorf("missing: %w", adapters.ErrSecretNotFound)
	}
	out := map[string]string{}
	for k, v := range p {
		out[k] = v
	}
	return out, nil
}

func (s *fakeSecrets) Delete(_ context.Context, envID string) error {
	delete(s.payload, envID)
	return nil
}

// fakeRoles implements adapters.Roles in memory for the HTTP tests.
type fakeRoles struct {
	passwords map[string]string
	// ensureHook, when set, runs inside every Ensure: a test blocks a
	// rotation mid-flight through it to interleave another request.
	ensureHook func()
}

func (r *fakeRoles) Ensure(_ context.Context, name, password string) error {
	if r.ensureHook != nil {
		r.ensureHook()
	}
	r.passwords[name] = password
	return nil
}

func (r *fakeRoles) Exists(_ context.Context, name string) (bool, error) {
	_, ok := r.passwords[name]
	return ok, nil
}

func (r *fakeRoles) GrantConnect(context.Context, string, string) error { return nil }
func (r *fakeRoles) GrantUsage(context.Context, string, string) error   { return nil }
func (r *fakeRoles) Drop(_ context.Context, name string) error {
	delete(r.passwords, name)
	return nil
}

// fakeVerifier implements adapters.Verifier with a scripted verdict.
type fakeVerifier struct{ err error }

func (v *fakeVerifier) VerifyUsable(context.Context, string, string, string) error { return v.err }

// connectingVerifier implements adapters.Verifier against a fakeRoles as
// only a real database would: a credential connects when and only when the
// applied role password matches, so a rotation that leaves Vault and the
// role disagreeing is a verification failure.
type connectingVerifier struct{ roles *fakeRoles }

func (v *connectingVerifier) VerifyUsable(_ context.Context, _, user, pass string) error {
	if v.roles.passwords[user] == pass {
		return nil
	}
	return fmt.Errorf("verify: %w", adapters.ErrVerificationAuth)
}

// setVerifier swaps the verifier fakes after wiring; the API holds the same
// pointer set is updated together with the rig's so tests can vary the
// verdict mid-test.
func (rig *credRig) setVerifier(v adapters.Verifier) {
	rig.verifier = v
	rig.api.verifier = v
}

// stateOf reads one environment's state elimination lock-safe.
func (rig *credRig) stateOf(envID string) domain.EnvState {
	rig.store.mu.Lock()
	defer rig.store.mu.Unlock()
	return rig.store.envs[envID].State
}

// TestRotationResumeConverges reproduces the interrupt-then-call-again crash
// the endpoint exists to survive: the environment sits in rotating and the
// secrets adapter may or may not hold the interrupted attempt's whole intent.
// The resume always generates a FRESH password and stores it before applying
// the role, so it can never silently re-apply the old credential and report
// success. It must converge: Vault, the role, and the verifier agree on one
// new password.
func TestRotationResumeConverges(t *testing.T) {
	rig := newCredRig(t)
	rig.setVerifier(&connectingVerifier{roles: rig.roles})

	env := rig.store.envs[envReadyID]
	env.State = domain.EnvRotating
	rig.store.envs[envReadyID] = env

	const staleIntent = "intent-left-behind-by-the-interrupted-attempt"
	rig.secrets.payload[envReadyID][payloadKeyPassword] = staleIntent

	rec := rig.post("/environments/"+envReadyID+"/rotate", allowedCaller)
	if rec.Code != http.StatusOK {
		t.Fatalf("resume rotation: %d %s", rec.Code, rec.Body.String())
	}
	if got := rig.secrets.payload[envReadyID][payloadKeyPassword]; got == staleIntent || got == sentinelPassword {
		t.Errorf("the resume must rotate to a fresh untouched password, got %q", got)
	}
	if got := rig.roles.passwords["role-"+envReadyID]; got != rig.secrets.payload[envReadyID][payloadKeyPassword] {
		t.Errorf("role applies %q, secrets adapter holds %q", got, rig.secrets.payload[envReadyID][payloadKeyPassword])
	}
	if rig.stateOf(envReadyID) != domain.EnvReady {
		t.Errorf("environment after resume: %q", rig.stateOf(envReadyID))
	}
}

// TestBookkeepingFailureIsResumableNotFatal pins the post-verification
// contract: when CompleteRotation fails AFTER the new credential was applied
// and verified, the environment must stay `rotating` (not `failed`) and the
// failure must be reported as retryable — a retry then converges to ready
// with exactly one stored credential.
func TestBookkeepingFailureIsResumableNotFatal(t *testing.T) {
	rig := newCredRig(t)
	rig.setVerifier(&connectingVerifier{roles: rig.roles})
	// The fake store's completeRotation hook fails once.
	rig.store.completeRotationHook = func() error {
		return fmt.Errorf("transient store hiccup")
	}

	rec := rig.post("/environments/"+envReadyID+"/rotate", allowedCaller)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("bookkeeping failure: %d %s, want 503", rec.Code, rec.Body.String())
	}
	if rig.stateOf(envReadyID) != domain.EnvRotating {
		t.Fatalf("environment after bookkeeping failure: %q, want rotating", rig.stateOf(envReadyID))
	}
	// The applied credential is valid: comparing Vault and the role proves it.
	if rig.roles.passwords["role-"+envReadyID] != rig.secrets.payload[envReadyID][payloadKeyPassword] {
		t.Error("the uncommitted rotation's credential is not consistent")
	}
	// The retry takes the resume path and converges.
	rig.store.completeRotationHook = nil
	second := rig.post("/environments/"+envReadyID+"/rotate", allowedCaller)
	if second.Code != http.StatusOK {
		t.Fatalf("rotation retry: %d %s", second.Code, second.Body.String())
	}
	if rig.stateOf(envReadyID) != domain.EnvReady {
		t.Errorf("environment after retry: %q", rig.stateOf(envReadyID))
	}
	if len(rig.secrets.payload) != 1 {
		t.Errorf("stored credentials: %d, want exactly one", len(rig.secrets.payload))
	}
}

// TestRotationInterleaveSerializesWriters interleaves two rotate calls: the
// first blocks mid-flight (at the role update, exactly where a client
// timeout lands), the second must refuse as busy instead of racing it with
// a different password — and a retry after that converges to a stored
// credential that provably connects.
func TestRotationInterleaveSerializesWriters(t *testing.T) {
	rig := newCredRig(t)
	rig.setVerifier(&connectingVerifier{roles: rig.roles})
	route := "/environments/" + envReadyID + "/rotate"

	releaseFirst := make(chan struct{})
	var failCheck sync.Once // only the first Ensure blocks
	blocking := func() {
		failCheck.Do(func() { <-releaseFirst })
	}
	rig.roles.ensureHook = blocking

	doneA := make(chan int, 1)
	go func() {
		rec := rig.post(route, allowedCaller)
		doneA <- rec.Code
	}()

	// Wait until A has taken the lock and moved the environment to rotating,
	// then run B: it must be refused as busy rather than double-writing.
	waitForState(t, rig, domain.EnvRotating)
	doneB := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		if rec := rig.post(route, deniedCaller); rec.Code != http.StatusForbidden {
			t.Errorf("denied caller inside an in-flight rotation: %d, want 403", rec.Code)
			doneB <- rec
			close(releaseFirst)
			return
		}
		doneB <- rig.post(route, allowedCaller)
	}()
	var recB *httptest.ResponseRecorder
	deadline := time.Now().Add(10 * time.Second)
	for recB == nil && time.Now().Before(deadline) {
		select {
		case r := <-doneB:
			recB = r
		case <-time.After(100 * time.Millisecond):
		}
	}
	if recB != nil && recB.Code != http.StatusConflict {
		t.Errorf("concurrent rotation raced the lock: %d, want 409: %s", recB.Code, recB.Body.String())
	}

	// Let A finish; it must succeed with its own generated password, leave
	// the environment ready, and leave Vault, the role, and the verifier in
	// exact agreement.
	close(releaseFirst)
	codeA := <-doneA
	if codeA != http.StatusOK {
		t.Fatalf("first rotation after interleave: %d", codeA)
	}
	if rig.stateOf(envReadyID) != domain.EnvReady {
		t.Fatalf("environment after the serialized rotation: %q", rig.stateOf(envReadyID))
	}
	rec := rig.post("/environments/"+envReadyID+"/credentials", allowedCaller)
	if rec.Code != http.StatusOK {
		t.Fatalf("retrieve after interleave: %d", rec.Code)
	}
	var got credentialResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode credential body: %v", err)
	}
	if got.Password == sentinelPassword {
		t.Error("sentinel password survived rotation")
	}
	if rig.roles.passwords[got.Username] != got.Password {
		t.Errorf("stored credential %q does not match the applied role password %q",
			got.Password, rig.roles.passwords[got.Username])
	}
}

// waitForState polls the rig until the environment reaches `want` — used to
// pin the interleaving before the concurrent request is issued.
func waitForState(t *testing.T, rig *credRig, want domain.EnvState) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if rig.stateOf(envReadyID) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("environment never reached %q (now %q)", want, rig.stateOf(envReadyID))
}

// credRig is the wired credential surface.
type credRig struct {
	api      *API
	store    *fakeStore
	secrets  *fakeSecrets
	roles    *fakeRoles
	verifier adapters.Verifier
	auditLog *bytes.Buffer
}

func newCredRig(t *testing.T) *credRig {
	t.Helper()
	st := newFakeStore()
	st.envs[envReadyID] = domain.Environment{
		ID:        envReadyID,
		TenantID:  "t1",
		Name:      "creds",
		State:     domain.EnvReady,
		Spec:      []byte(`{}`),
		Metadata:  []byte(fmt.Sprintf(`{"database":"db-%s","credentialRef":"secret/provisioning"}`, envReadyID)),
		CreatedAt: "2026-01-01T00:00:00Z",
		UpdatedAt: "2026-01-01T00:00:00Z",
	}
	audit := &bytes.Buffer{}
	api := New(st)
	api.auditLog = slog.New(telemetry.NewRedactingHandler(slog.NewJSONHandler(audit, nil)))
	rig := &credRig{
		api:   api,
		store: st,
		secrets: func() *fakeSecrets {
			s := newFakeSecrets(nil)
			s.payload[envReadyID] = map[string]string{
				payloadKeyDatabase: "db-" + envReadyID,
				payloadKeyUsername: "role-" + envReadyID,
				payloadKeyPassword: sentinelPassword,
			}
			return s
		}(),
		roles:    &fakeRoles{passwords: map[string]string{}},
		verifier: &fakeVerifier{},
		auditLog: audit,
	}
	rig.attach(allowedCaller)
	return rig
}

// attach wires the rig's fakes into the API, with the explicit policy the
// credential path requires.
func (rig *credRig) attach(caller ...string) {
	rig.api.secrets = rig.secrets
	rig.api.roles = rig.roles
	rig.api.verifier = rig.verifier
	rig.api.policy = NewCredentialPolicy(caller...)
}

// post issues a credential-route request as caller.
func (rig *credRig) post(route, caller string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, route, nil)
	if caller != "" {
		req.Header.Set(callerIdentityHeader, caller)
	}
	rec := httptest.NewRecorder()
	rig.api.Handler().ServeHTTP(rec, req)
	return rec
}

func TestCredentialRetrievalRequiresExplicitAuthorization(t *testing.T) {
	rig := newCredRig(t)
	route := "/environments/" + envReadyID + "/credentials"

	// Missing identity is a denial before any store lookup happens.
	if rec := rig.post(route, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no caller identity: %d, want 401", rec.Code)
	}
	// A caller outside the policy is denied, audited with its identity.
	if rec := rig.post(route, deniedCaller); rec.Code != http.StatusForbidden {
		t.Fatalf("unauthorized caller: %d, want 403", rec.Code)
	}
	if !strings.Contains(rig.auditLog.String(), deniedCaller) {
		t.Error("denied attempt was not audited with the caller identity")
	}
	// The allowed caller reaches the payload.
	rec := rig.post(route, allowedCaller)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed caller: %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got credentialResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode credential body: %v", err)
	}
	if got.Password != sentinelPassword || got.Username == "" || got.Database == "" {
		t.Errorf("credential body incomplete: %+v", got)
	}
	if !strings.Contains(rig.auditLog.String(), allowedCaller) {
		t.Error("allowed retrieval was not audited with the caller identity")
	}
}

func TestCredentialRetrievalOnlyForReadyEnvironments(t *testing.T) {
	rig := newCredRig(t)
	env := rig.store.envs[envReadyID]
	env.State = domain.EnvProvisioning
	rig.store.envs[envReadyID] = env
	rec := rig.post("/environments/"+envReadyID+"/credentials", allowedCaller)
	if rec.Code != http.StatusConflict {
		t.Fatalf("not ready: %d, want 409", rec.Code)
	}
	if strings.Contains(rec.Body.String(), sentinelPassword) {
		t.Error("a not-ready environment leaked its credential")
	}
	if !strings.Contains(rig.auditLog.String(), "environment not ready") {
		t.Error("the non-ready denial was not audited")
	}
}

// The heart of the phase: status and history return no secret, ever, even
// when the caller asserts an allowlisted identity.
func TestStatusAndHistoryNeverCarryTheSentinel(t *testing.T) {
	rig := newCredRig(t)
	for _, route := range []string{
		"/environments/" + envReadyID,
		"/environments/" + envReadyID + "/history",
	} {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		req.Header.Set(callerIdentityHeader, allowedCaller)
		rec := httptest.NewRecorder()
		rig.api.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d", route, rec.Code)
		}
		if strings.Contains(rec.Body.String(), sentinelPassword) {
			t.Errorf("%s leaked the sentinel password: %s", route, rec.Body.String())
		}
	}
	// Contrast on the same rig: the authorized path returns it.
	if rec := rig.post("/environments/"+envReadyID+"/credentials", allowedCaller); rec.Code != http.StatusOK {
		t.Fatalf("authorized retrieval: %d", rec.Code)
	}
}

func TestRotationFlow(t *testing.T) {
	rig := newCredRig(t)
	route := "/environments/" + envReadyID + "/rotate"

	// Denied callers cannot move the environment at all.
	if rec := rig.post(route, deniedCaller); rec.Code != http.StatusForbidden {
		t.Fatalf("denied caller: %d, want 403", rec.Code)
	}
	if rig.store.envs[envReadyID].State != domain.EnvReady {
		t.Errorf("denied caller moved the environment: %q", rig.store.envs[envReadyID].State)
	}

	// The allowed caller rotates.
	if rec := rig.post(route, allowedCaller); rec.Code != http.StatusOK {
		t.Fatalf("rotation: %d %s", rec.Code, rec.Body.String())
	}
	first := rig.secrets.payload[envReadyID][payloadKeyPassword]
	if first == sentinelPassword {
		t.Error("rotation stored the old password")
	}
	if rig.roles.passwords["role-"+envReadyID] != first {
		t.Error("the applied role password differs from the stored one")
	}
	if rig.store.envs[envReadyID].State != domain.EnvReady {
		t.Errorf("environment left rotating: %q", rig.store.envs[envReadyID].State)
	}

	// Rotation twice in a row: still ready, exactly one stored credential,
	// usable (the verifier refused otherwise), and retrievable.
	if rec := rig.post(route, allowedCaller); rec.Code != http.StatusOK {
		t.Fatalf("second rotation: %d %s", rec.Code, rec.Body.String())
	}
	second := rig.secrets.payload[envReadyID][payloadKeyPassword]
	if second == first {
		t.Error("second rotation reused the first password")
	}
	if len(rig.secrets.payload) != 1 {
		t.Errorf("stored credentials: %d, want exactly one", len(rig.secrets.payload))
	}
	if rig.store.envs[envReadyID].State != domain.EnvReady {
		t.Errorf("environment after second rotation: %q", rig.store.envs[envReadyID].State)
	}
	got := rig.post("/environments/"+envReadyID+"/credentials", allowedCaller)
	if !strings.Contains(got.Body.String(), second) {
		t.Error("retrieval after rotation did not return the rotated password")
	}
	if !strings.Contains(rig.auditLog.String(), allowedCaller) {
		t.Error("rotation was not audited with the caller identity")
	}
}

func TestRotationFailureRemainsRetryable(t *testing.T) {
	rig := newCredRig(t)
	rig.setVerifier(&fakeVerifier{err: fmt.Errorf("verify: %w", adapters.ErrVerificationAuth)})
	rec := rig.post("/environments/"+envReadyID+"/rotate", allowedCaller)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed rotation: %d, want retryable 503", rec.Code)
	}
	if rig.store.envs[envReadyID].State != domain.EnvRotating {
		t.Errorf("environment after failed rotation: %q, want rotating", rig.store.envs[envReadyID].State)
	}
	if got := rig.secrets.payload[envReadyID][payloadKeyPassword]; got != sentinelPassword {
		t.Errorf("unverified candidate replaced the active secret: got %q, want original", got)
	}
	if got := rig.roles.passwords["role-"+envReadyID]; got == sentinelPassword {
		t.Error("test did not exercise a role change before the verification failure")
	}
	if strings.Contains(rec.Body.String(), sentinelPassword) {
		t.Error("the failed rotation leaked the secret")
	}
	if !strings.Contains(rig.auditLog.String(), "rotate_credentials") {
		t.Error("the failed rotation was not audited")
	}
	rig.setVerifier(&connectingVerifier{roles: rig.roles})
	retry := rig.post("/environments/"+envReadyID+"/rotate", allowedCaller)
	if retry.Code != http.StatusOK || rig.stateOf(envReadyID) != domain.EnvReady {
		t.Fatalf("retry did not converge: status=%d state=%q body=%s", retry.Code, rig.stateOf(envReadyID), retry.Body.String())
	}
}
