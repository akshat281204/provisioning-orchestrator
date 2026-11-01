package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// fakeStore is an in-memory Store implementing the DB's envelopes with plain
// maps and a mutex, so handler behaviour is testable with no database. It
// mimics the database's two uniqueness constraints: (tenant_id,
// idempotency_key) and (environment_id, step_index).
type fakeStore struct {
	mu    sync.Mutex
	envs  map[string]domain.Environment
	byKey map[string]string // tenant+"\x00"+idempotency_key -> environment id
	steps map[string][]domain.Step
	// rotation lock bookkeeping: one held/holder pair per environment.
	locks map[string]*sync.Mutex
	held  map[string]bool
	// completeRotationHook, when set, fails every CompleteRotation call.
	completeRotationHook func() error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		envs:  map[string]domain.Environment{},
		byKey: map[string]string{},
		steps: map[string][]domain.Step{},
		locks: map[string]*sync.Mutex{},
		held:  map[string]bool{},
	}
}

func keyOf(tenantID, idemKey string) string { return tenantID + "\x00" + idemKey }

func (f *fakeStore) CreateEnvironmentWithPlan(
	_ context.Context, env domain.Environment, plan []domain.PlannedStep,
) (domain.Environment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.byKey[keyOf(env.TenantID, env.IdempotencyKey)]; ok {
		return domain.Environment{}, fmt.Errorf("fake: create environment with plan: %w", store.ErrDuplicate)
	}
	if env.CreatedAt == "" {
		env.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		env.UpdatedAt = env.CreatedAt
	}
	f.envs[env.ID] = env
	f.byKey[keyOf(env.TenantID, env.IdempotencyKey)] = env.ID
	for _, st := range plan {
		f.steps[env.ID] = append(f.steps[env.ID], domain.Step{
			ID:            fmt.Sprintf("step-%d", st.Index),
			EnvironmentID: env.ID,
			StepIndex:     st.Index,
			Kind:          st.Kind,
			State:         domain.StepPending,
			OperationKey:  domain.OperationKey(env.ID, st.Index, st.Kind),
		})
	}
	return env, nil
}

func (f *fakeStore) GetByIdempotencyKey(_ context.Context, tenantID, idemKey string) (domain.Environment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byKey[keyOf(tenantID, idemKey)]
	if !ok {
		return domain.Environment{}, fmt.Errorf("fake: get by key: %w", store.ErrNotFound)
	}
	return f.envs[id], nil
}

func (f *fakeStore) Get(_ context.Context, id string) (domain.Environment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// The database answers a non-UUID environment_id with SQLSTATE 22P02
	// (invalid text representation), which is NOT an ErrNotFound. Mimic it so
	// a test only passes when the handler rejects malformed ids itself.
	if !uuidPattern.MatchString(strings.ToLower(id)) {
		return domain.Environment{}, fmt.Errorf("fake: get %s: %w", id, pathIDNotUUID)
	}
	env, ok := f.envs[id]
	if !ok {
		return domain.Environment{}, fmt.Errorf("fake: get %s: %w", id, store.ErrNotFound)
	}
	return env, nil
}

func (f *fakeStore) ListForEnvironment(_ context.Context, environmentID string) ([]domain.Step, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.Step(nil), f.steps[environmentID]...), nil
}

// TransitionEnvironment CAS-moves the environment's state in memory.
func (f *fakeStore) TransitionEnvironment(_ context.Context, environmentID string, target domain.EnvState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.envs[environmentID]
	if !ok {
		return fmt.Errorf("fake: transition %s: %w", environmentID, store.ErrNotFound)
	}
	if !env.State.CanTransitionTo(target) {
		return fmt.Errorf("fake: transition %s: illegal %q -> %q", environmentID, env.State, target)
	}
	env.State = target
	f.envs[environmentID] = env
	return nil
}

// CompleteRotation flips rotating -> ready in memory. completeRotationHook,
// when set, fails it — tests use it to exercise the resumable bookkeeping
// failure path.
func (f *fakeStore) CompleteRotation(_ context.Context, environmentID string, _ []byte) error {
	f.mu.Lock()
	env, ok := f.envs[environmentID]
	var hookErr bool
	if f.completeRotationHook != nil {
		hookErr = true
	}
	f.mu.Unlock()
	if !ok || env.State != domain.EnvRotating {
		return fmt.Errorf("fake: complete rotation %s: not rotating", environmentID)
	}
	if hookErr {
		return f.completeRotationHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	env.State = domain.EnvReady
	f.envs[environmentID] = env
	return nil
}

// AcquireRotationLock mirrors the database's advisory lock: one holder per
// environment; a loser is refused with store.ErrRotationBusy. The paired
// mutex additionally serializes genuinely overlapping in-process attempts.

func (f *fakeStore) AcquireRotationLock(_ context.Context, environmentID string) (func() error, error) {
	f.mu.Lock()
	if f.locks == nil {
		f.locks = map[string]*sync.Mutex{}
	}
	if f.held == nil {
		f.held = map[string]bool{}
	}
	m, ok := f.locks[environmentID]
	if !ok {
		m = &sync.Mutex{}
		f.locks[environmentID] = m
	}
	held := f.held[environmentID]
	if !held {
		f.held[environmentID] = true
	}
	f.mu.Unlock()
	if held {
		return nil, fmt.Errorf("fake: acquire rotation lock %s: %w", environmentID, store.ErrRotationBusy)
	}
	m.Lock() // serialize genuinely overlapping attempts in-process
	return func() error {
		m.Unlock()
		f.mu.Lock()
		f.held[environmentID] = false
		f.mu.Unlock()
		return nil
	}, nil
}

func (f *fakeStore) envCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.envs)
}

// postCreate posts one environment-creation request. The body is read from
// body every call, so a torn-read test needs distinct bodies.
func postCreate(t *testing.T, a *API, body, idemKey string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/environments", strings.NewReader(body))
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, req)
	return rec
}

const validBody = `{"tenantId":"t1","name":"orders-db","spec":{"size":"small"}}`

func TestCreateReturnsAcceptedWithPlan(t *testing.T) {
	a := New(newFakeStore())

	rec := postCreate(t, a, validBody, "key-1")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp environmentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EnvironmentID == "" || resp.Status != "pending" || resp.StatusURL != "/v1/environments/"+resp.EnvironmentID {
		t.Fatalf("unexpected response body: %+v", resp)
	}
}

func TestMissingIdempotencyKeyWritesNothing(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	rec := postCreate(t, a, validBody, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("expected the error envelope, got %s", rec.Body.String())
	}
	if fs.envCount() != 0 {
		t.Fatalf("a request with no Idempotency-Key must write nothing, wrote %d environments", fs.envCount())
	}
}

func TestReplayReturnsSameEnvironmentWith200(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	first := postCreate(t, a, validBody, "key-1")
	second := postCreate(t, a, validBody, "key-1")

	if first.Code != http.StatusAccepted || second.Code != http.StatusOK {
		t.Fatalf("expected 202 then 200, got %d and %d", first.Code, second.Code)
	}
	var firstEnv, secondEnv environmentResponse
	_ = json.Unmarshal(first.Body.Bytes(), &firstEnv)
	_ = json.Unmarshal(second.Body.Bytes(), &secondEnv)
	if firstEnv.EnvironmentID == "" || firstEnv.EnvironmentID != secondEnv.EnvironmentID ||
		firstEnv.Status != secondEnv.Status {
		t.Fatalf("replay must return the same environment: %+v vs %+v", firstEnv, secondEnv)
	}
	// One environment and exactly the planned number of step rows.
	if fs.envCount() != 1 {
		t.Fatalf("expected 1 environment, got %d", fs.envCount())
	}
	fs.mu.Lock()
	steps := fs.steps[firstEnv.EnvironmentID]
	fs.mu.Unlock()
	if len(steps) != len(domain.Plan()) {
		t.Fatalf("expected %d plan steps, got %d", len(domain.Plan()), len(steps))
	}
}

func TestReplayWithDifferentBodyConflicts(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	first := postCreate(t, a, validBody, "key-1")
	second := postCreate(t, a, `{"tenantId":"t1","name":"other","spec":{}}`, "key-1")
	if first.Code != http.StatusAccepted || second.Code != http.StatusConflict {
		t.Fatalf("expected 202 then 409, got %d and %d: %s", first.Code, second.Code, second.Body.String())
	}
	if fs.envCount() != 1 {
		t.Fatalf("a conflicting replay must not write, wrote %d environments", fs.envCount())
	}
}

func TestConcurrentSameKeyCreatesExactlyOneEnvironment(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	const n = 10
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = postCreate(t, a, validBody, "key-race").Code
		}(i)
	}
	wg.Wait()

	accepted, replayed := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusAccepted:
			accepted++
		case http.StatusOK:
			replayed++
		default:
			t.Fatalf("unexpected status %d among concurrent replays", c)
		}
	}
	if accepted != 1 || replayed != n-1 {
		t.Fatalf("expected exactly one 202 and %d 200s, got %d and %d", n-1, accepted, replayed)
	}
	if fs.envCount() != 1 {
		t.Fatalf("expected exactly 1 environment, got %d", fs.envCount())
	}
}

func TestGetUnknownEnvironmentReturns404WithJSONError(t *testing.T) {
	a := New(newFakeStore())

	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/environments/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	var e errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("body must be the JSON error envelope, got %s: %v", rec.Body.String(), err)
	}
	if e.Error.Code == "" || e.Error.Message == "" {
		t.Fatalf("error body needs code and message, got %+v", e)
	}
}

// validMissingUUID is a syntactically valid UUID that the store has never
// seen. It is the realistic miss: the handler validates it and forwards it,
// and the (real or fake) store answers ErrNotFound — which must map to 404,
// not 500.
const validMissingUUID = "00000000-0000-4000-8000-000000000abc"

// TestValidButUnknownUUIDReturns404OnBothReadEndpoints covers the acceptance
// criterion through the actual ErrNotFound mapping in writeEnvironmentOrError:
// a well-formed UUID absent from the store answers 404 with the JSON error
// envelope on both GET /environments/{id} and GET /environments/{id}/history.
func TestValidButUnknownUUIDReturns404OnBothReadEndpoints(t *testing.T) {
	a := New(newFakeStore())

	for _, path := range []string{
		"/environments/" + validMissingUUID,
		"/environments/" + validMissingUUID + "/history",
	} {
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: expected 404, got %d: %s", path, rec.Code, rec.Body.String())
			continue
		}
		var e errorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
			t.Errorf("GET %s: body must be the JSON error envelope, got %s", path, rec.Body.String())
			continue
		}
		if e.Error.Code != "not_found" || e.Error.Message == "" {
			t.Errorf("GET %s: want not_found code and message, got %+v", path, e.Error)
		}
	}
}

func TestHistoryReturnsRedactedTimelineOnly(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	env := domain.Environment{
		ID: "11111111-2222-4333-8444-555555555555", TenantID: "t1", Name: "n",
		State:          domain.EnvProvisioning,
		IdempotencyKey: "k", Spec: []byte(`{}`),
		// metadata contains no whitelisted connection key and a secret-shaped
		// one; neither may reach the response.
		Metadata: []byte(`{"internalNote":"secret-material-XYZ"}`),
	}
	if _, err := fs.CreateEnvironmentWithPlan(context.Background(), env, domain.Plan()); err != nil {
		t.Fatalf("seed env: %v", err)
	}
	// A failed step whose last_error is what a driver would emit with the
	// credential embedded.
	fs.mu.Lock()
	steps := fs.steps[env.ID]
	steps[2].State = domain.StepFailed
	steps[2].LastError = `pq: role creation failed connecting postgres://orch:hunter2@db:5432/app password=hunter2`
	steps[2].Metadata = []byte(`{"vaultPath":"secret/data/x","password":"hunter2"}`)
	fs.steps[env.ID] = steps
	fs.mu.Unlock()

	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/environments/"+env.ID+"/history", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	for _, secret := range []string{"hunter2", "secret-material-XYZ", `"metadata"`, "vaultPath"} {
		if strings.Contains(body, secret) {
			t.Fatalf("history response leaks %q: %s", secret, body)
		}
	}
	var resp historyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(resp.Steps) != len(domain.Plan()) {
		t.Fatalf("expected %d steps, got %d", len(domain.Plan()), len(resp.Steps))
	}
	var failed *stepView
	for i := range resp.Steps {
		if resp.Steps[i].StepIndex == 3 {
			failed = &resp.Steps[i]
		}
	}
	if failed == nil {
		t.Fatal("missing step 3 in history")
	}
	if failed.State != string(domain.StepFailed) || failed.LastError == nil {
		t.Fatalf("step 3 should carry state and a redacted last_error: %+v", failed)
	}
	if strings.Contains(*failed.LastError, "hunter2") {
		t.Fatalf("last_error must be scrubbed, got %q", *failed.LastError)
	}
	if !strings.Contains(*failed.LastError, "[REDACTED]") {
		t.Fatalf("last_error should show the redaction marker, got %q", *failed.LastError)
	}
	// Errored steps only, no empty-string noise from healthy steps.
	for i := range resp.Steps {
		if resp.Steps[i].StepIndex != 3 && resp.Steps[i].LastError != nil {
			t.Fatalf("healthy steps must omit last_error, step %d had %+v", resp.Steps[i].StepIndex, resp.Steps[i].LastError)
		}
	}
}

func TestGetEnvironmentExposesConnectionFieldsButNeverMetadata(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	env := domain.Environment{
		ID: "66666666-7777-4888-9999-000000000000", TenantID: "t1", Name: "n",
		State:          domain.EnvReady,
		IdempotencyKey: "k", Spec: []byte(`{}`),
		// ready environment metadata as the provisioning workflow would write
		// it, with a password-shaped extra key that must never surface.
		Metadata: []byte(`{"host":"db.internal","database":"app","credentialRef":"oracle/cred/env-m","password":"hunter2"}`),
	}
	if _, err := fs.CreateEnvironmentWithPlan(context.Background(), env, domain.Plan()); err != nil {
		t.Fatalf("seed env: %v", err)
	}

	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/environments/"+env.ID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"host":"db.internal"`) ||
		!strings.Contains(body, `"database":"app"`) ||
		!strings.Contains(body, `"credentialRef":"oracle/cred/env-m"`) {
		t.Fatalf("expected non-secret connection metadata, got %s", body)
	}
	if strings.Contains(body, "hunter2") || strings.Contains(body, `"password"`) {
		t.Fatalf("response must never contain secret material: %s", body)
	}
}

func TestGetEnvironmentWithNoConnectionFieldsOmitsThem(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)
	env := domain.Environment{
		ID: "aaaaaabb-cccc-4ddd-8eee-ffff00000011", TenantID: "t1", Name: "n",
		State:          domain.EnvPending,
		IdempotencyKey: "k", Spec: []byte(`{}`), Metadata: []byte(`{}`),
	}
	if _, err := fs.CreateEnvironmentWithPlan(context.Background(), env, domain.Plan()); err != nil {
		t.Fatalf("seed env: %v", err)
	}

	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/environments/"+env.ID, nil))
	body := rec.Body.String()
	for _, key := range []string{"host", "database", "credentialRef"} {
		if strings.Contains(body, `"`+key+`"`) {
			t.Fatalf("unknown connection field %q must be omitted, got %s", key, body)
		}
	}
}

// TestMalformedPathIDReturns404Not500 covers the real-database failure mode:
// a non-UUID id sent into the uuid column raises SQLSTATE 22P02, which is not
// ErrNotFound. The fake store mimics that error, so these tests fail with a
// 500 if a handler ever forwards a malformed path id to the store.
func TestMalformedPathIDReturns404Not500(t *testing.T) {
	a := New(newFakeStore())

	for _, path := range []string{
		"/environments/nope",
		"/environments/ghost/history",
		"/environments/not-a-uuid-at-all",
		"/environments/11111111-2222-4333-8444-5555555555zz",
	} {
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: expected 404, got %d: %s", path, rec.Code, rec.Body.String())
			continue
		}
		var e errorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil ||
			e.Error.Code != "not_found" || e.Error.Message == "" {
			t.Errorf("GET %s: body must be the not_found error envelope, got %s", path, rec.Body.String())
		}
	}
}

// TestReplayIgnoresJSONBKeyOrder is the production-shape regression test for
// the fingerprint: Postgres jsonb does not preserve object key order (keys
// come back sorted by length, then bytewise), so the stored spec of this
// multi-key spec differs byte-wise from the request that created it. A
// replay of that same request must still answer 200, never 409.
func TestReplayIgnoresJSONBKeyOrder(t *testing.T) {
	fs := newFakeStore()
	a := New(fs)

	body := `{"tenantId":"t1","name":"orders-db","spec":{"region":"us-east-1","size":"small","replicas":2}}`
	first := postCreate(t, a, body, "key-jsonb")
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", first.Code, first.Body.String())
	}

	// Rewrite the stored spec the way jsonb would normalise it: keys sorted
	// by length ("size"(4) "region"(6) "replicas"(8)), whitespace removed.
	var resp environmentResponse
	if err := json.Unmarshal(first.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	fs.mu.Lock()
	env := fs.envs[resp.EnvironmentID]
	env.Spec = []byte(`{"size":"small","region":"us-east-1","replicas":2}`)
	fs.envs[resp.EnvironmentID] = env
	fs.mu.Unlock()

	second := postCreate(t, a, body, "key-jsonb")
	if second.Code != http.StatusOK {
		t.Fatalf("replay of the same request must be 200 despite jsonb key reordering, got %d: %s",
			second.Code, second.Body.String())
	}
	var replayed environmentResponse
	_ = json.Unmarshal(second.Body.Bytes(), &replayed)
	if replayed.EnvironmentID != resp.EnvironmentID {
		t.Fatalf("replay returned a different environment: %s vs %s", replayed.EnvironmentID, resp.EnvironmentID)
	}
}

// TestReplayNumericAndNestedSpecEquality checks that the fingerprint stays
// value-based for nested objects and numbers, not just flat key order.
func TestReplayNumericAndNestedSpecEquality(t *testing.T) {
	a := New(newFakeStore())

	body := `{"tenantId":"t1","name":"db","spec":{"limits":{"cpu":2,"mem": 4096},"tags":["a","b"]}}`
	if rec := postCreate(t, a, body, "key-nested"); rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	second := postCreate(t, a, body, "key-nested")
	if second.Code != http.StatusOK {
		t.Fatalf("replay of a nested/numeric spec must be 200, got %d: %s", second.Code, second.Body.String())
	}
	if rec := postCreate(t, a, `{"tenantId":"t1","name":"db","spec":{"limits":{"cpu":4,"mem":4096},"tags":["a","b"]}}`, "key-nested"); rec.Code != http.StatusConflict {
		t.Fatalf("a genuinely different nested value must be 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRedactTextPatterns(t *testing.T) {
	cases := map[string]string{
		`password=hunter2 failed`:                       "password=[REDACTED] failed",
		`connect postgres://orch:hunter2@db:5432/app`:   "connect postgres://orch:[REDACTED]@db:5432/app",
		`auth via Bearer eyJhbGciOiJIUzI1NiJ9 rejected`: "auth via Bearer [REDACTED] rejected",
		// Compound credential keys the key/value rule must not miss.
		`access_token=abc123`: `access_token=[REDACTED]`,
		`client_secret=shh`:   `client_secret=[REDACTED]`,
		`db_password=foo`:     `db_password=[REDACTED]`,
		// JSON- and quote-shaped leaks: keys wrapped in quotes, values as
		// quoted strings (including how a Vault body carries them).
		`{"password":"hunter2"}`:        `{"password":[REDACTED]}`,
		`"token":"abc123"`:              `"token":[REDACTED]`,
		`pagerduty: {"token":"s3cr3t"}`: `pagerduty: {"token":[REDACTED]}`,
		`apikey: sk-1234`:               `apikey: [REDACTED]`,
	}
	for in, want := range cases {
		if got := redactText(in); got != want {
			t.Errorf("redactText(%q) = %q, want %q", in, got, want)
		}
	}
}
