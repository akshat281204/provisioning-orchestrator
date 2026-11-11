package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"log/slog"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// maxBodyBytes bounds request bodies so a client cannot make the server
// buffer unbounded input before validation.
const maxBodyBytes = 1 << 20

// Store is the persistence surface the /v1 handlers need. It is satisfied by
// *store.PostgresStore and by the in-memory fake in tests.
type Store interface {
	// CreateEnvironmentWithPlan inserts the environment and every planned
	// step in one transaction, stamping traceparent on every step row in the
	// same commit, or errors wrapping store.ErrDuplicate.
	CreateEnvironmentWithPlan(
		ctx context.Context, env domain.Environment, steps []domain.PlannedStep,
		traceparent string,
	) (domain.Environment, error)
	// GetByIdempotencyKey looks up by (tenant, key); a miss is ErrNotFound.
	GetByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (domain.Environment, error)
	// Get looks up by id; a miss is ErrNotFound.
	Get(ctx context.Context, id string) (domain.Environment, error)
	// ListForEnvironment returns the steps ordered by index.
	ListForEnvironment(ctx context.Context, environmentID string) ([]domain.Step, error)

	// BeginCleanup starts the durable cleanup workflow: it moves the
	// environment to `target` (cancelling or deleting) and writes the
	// compensation step rows in one transaction, re-arming wedged cleanup
	// rows on a repeat call. Illegal moves wrap store.ErrIllegalTransition;
	// missing ids wrap store.ErrNotFound.
	BeginCleanup(ctx context.Context, environmentID string, target domain.EnvState) (domain.Environment, error)

	// TransitionEnvironment CAS-moves the environment's state (the
	// credential/rotation path only; `ready` is rejected at the store).
	TransitionEnvironment(ctx context.Context, environmentID string, target domain.EnvState) error
	// CompleteRotation records the rotation on the verify step's metadata
	// and returns the environment to ready in one transaction.
	CompleteRotation(ctx context.Context, environmentID string, entry []byte) error
	// AcquireRotationLock serializes rotations for one environment across
	// every caller and process: a caller that could not take it in time
	// receives an error wrapping store.ErrRotationBusy. The release function
	// must be called exactly once, when the rotation attempt is finished.
	// Without the lock, two overlapping rotations could each write a
	// different new password to Vault and the role, leaving the stored
	// credential non-connecting.
	AcquireRotationLock(ctx context.Context, environmentID string) (func() error, error)
}

// API serves the /v1 routes.
type API struct {
	envs Store
	// secrets, roles and verifier back the credential retrieval and
	// rotation endpoints; nil disables the endpoints (fail closed).
	secrets  adapters.Secrets
	roles    adapters.Roles
	verifier adapters.Verifier
	policy   CredentialPolicy
	auditLog *slog.Logger
}

// compile-time check that the concrete store keeps satisfying the handler
// interface.
var _ Store = (*store.PostgresStore)(nil)

// New builds the /v1 API on top of a store.
func New(envs Store) *API { return &API{envs: envs} }

// Handler returns the /v1 routes without a version prefix; the caller maps
// it under /v1. Each route names its method, so wrong-method requests get
// the mux's 405 rather than a shadowed handler.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /environments", a.createEnvironment)
	mux.HandleFunc("GET /environments/{id}", a.getEnvironment)
	mux.HandleFunc("GET /environments/{id}/history", a.getHistory)
	mux.HandleFunc("POST /environments/{id}/cancel", a.cancelEnvironment)
	mux.HandleFunc("DELETE /environments/{id}", a.deleteEnvironment)
	mux.HandleFunc("POST /environments/{id}/credentials", a.retrieveCredentials)
	mux.HandleFunc("POST /environments/{id}/rotate", a.rotateEnvironment)
	return mux
}

// createEnvironment handles POST /v1/environments. Creation is atomic (the
// environment and all plan steps land in one transaction) and idempotent
// (repeating the same Idempotency-Key never writes a second row).
func (a *API) createEnvironment(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key",
			"the Idempotency-Key header is required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body",
			"could not read the request body")
		return
	}
	var req createRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json",
			"the request body is not valid JSON")
		return
	}
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.Name = strings.TrimSpace(req.Name)
	if req.TenantID == "" || req.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request",
			"tenantId and name are required")
		return
	}
	spec, err := compactJSON(req.Spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json",
			"the spec must be valid JSON")
		return
	}

	// A replayed request with the same (tenant, key) never writes.
	existing, err := a.envs.GetByIdempotencyKey(r.Context(), req.TenantID, key)
	switch {
	case err == nil:
		a.replay(w, existing, req, spec)
		return
	case !errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusInternalServerError, "internal_error",
			"looking up the idempotency key failed")
		return
	}

	env := domain.Environment{
		ID:             newEnvironmentID(),
		TenantID:       req.TenantID,
		Name:           req.Name,
		State:          domain.EnvPending,
		IdempotencyKey: key,
		Spec:           spec,
	}
	created, err := a.envs.CreateEnvironmentWithPlan(r.Context(), env, domain.Plan(), "")
	switch {
	case err == nil:
		writeEnv(w, http.StatusAccepted, created, true)
	case errors.Is(err, store.ErrDuplicate):
		// Another request with this key won the race. Fetch its result and
		// answer as a replay, or 409 when the request bodies differ.
		existing, err := a.envs.GetByIdempotencyKey(r.Context(), req.TenantID, key)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error",
				"looking up the idempotency key after a duplicate insert failed")
			return
		}
		a.replay(w, existing, req, spec)
	default:
		writeError(w, http.StatusInternalServerError, "internal_error",
			"creating the environment failed")
	}
}

// replay answers an idempotent re-request. Same request fingerprint: 200 with
// the existing environment. Different body: 409, per the design contract.
func (a *API) replay(w http.ResponseWriter, existing domain.Environment, req createRequest, spec []byte) {
	if existing.Name != req.Name || !equalJSONValues(spec, existing.Spec) {
		writeError(w, http.StatusConflict, "idempotency_key_conflict",
			"the Idempotency-Key was already used with a different request body")
		return
	}
	writeEnv(w, http.StatusOK, existing, true)
}

// getEnvironment handles GET /v1/environments/{id}: status, non-secret
// connection metadata, and — while a cleanup workflow is underway — its
// progress. Credential material is never a field of the response.
func (a *API) getEnvironment(w http.ResponseWriter, r *http.Request) {
	envID, ok := environmentIDFromPath(w, r)
	if !ok {
		return
	}
	env, err := a.envs.Get(r.Context(), envID)
	if err != nil {
		writeEnvironmentOrError(w, env, err)
		return
	}
	// Progress needs the step rows; they are fetched only for the states
	// where the compensation plan can exist, so a plain read of a pending
	// environment stays a single-row read.
	var steps []domain.Step
	if needsCleanupProgress(env.State) {
		steps, err = a.envs.ListForEnvironment(r.Context(), envID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error",
				"reading the workflow steps failed")
			return
		}
	}
	writeEnvironment(w, http.StatusOK, env, false, steps)
}

// writeEnvironmentOrError maps a store lookup result onto the response. It is
// shared by both read endpoints so "not found" always becomes a 404 JSON
// error, never a raw 500.
func writeEnvironmentOrError(w http.ResponseWriter, env domain.Environment, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found",
			"no such environment")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error",
			"reading the environment failed")
		return
	}
	writeEnv(w, http.StatusOK, env, false)
}

// pathIDNotUUID is returned by the fake store when it receives an id the
// database would reject; it deliberately does not wrap ErrNotFound so tests
// catch a handler that forwards malformed path ids to the store instead of
// rejecting them first (Postgres answers such a query with SQLSTATE 22P02,
// which is not a not-found condition).
var pathIDNotUUID = errors.New("fake: path id is not a uuid")

// getHistory handles GET /v1/environments/{id}/history: the redacted step
// timeline. It maps domain.Step through stepView, which has no metadata
// field, and scrubs last_error, so no raw step state can reach the wire.
func (a *API) getHistory(w http.ResponseWriter, r *http.Request) {
	envID, ok := environmentIDFromPath(w, r)
	if !ok {
		return
	}
	env, err := a.envs.Get(r.Context(), envID)
	if err != nil {
		writeEnvironmentOrError(w, domain.Environment{}, err)
		return
	}
	steps, err := a.envs.ListForEnvironment(r.Context(), envID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error",
			"reading the workflow steps failed")
		return
	}

	resp := historyResponse{EnvironmentID: env.ID, Steps: make([]stepView, 0, len(steps))}
	for _, st := range steps {
		view := stepView{
			StepIndex: st.StepIndex,
			Kind:      string(st.Kind),
			State:     string(st.State),
			Attempts:  st.Attempts,
			CreatedAt: st.CreatedAt,
			UpdatedAt: st.UpdatedAt,
		}
		// A step still in flight has no failure to report; nil encodes that
		// without shipping an empty string.
		if st.LastError != "" {
			redacted := redactText(st.LastError)
			view.LastError = &redacted
		}
		resp.Steps = append(resp.Steps, view)
	}
	writeJSON(w, http.StatusOK, resp)
}

// cancelEnvironment handles POST /v1/environments/{id}/cancel: stop
// scheduling new steps. It begins the durable cancelling workflow (202) —
// in-flight steps are allowed to finish or expire, cleanup rows are already
// recorded, and the finalizer closes the workflow once they conclude.
func (a *API) cancelEnvironment(w http.ResponseWriter, r *http.Request) {
	a.beginCleanup(w, r, domain.EnvCancelling)
}

// deleteEnvironment handles DELETE /v1/environments/{id}: begin the durable
// cleanup (a `deleting` state with compensation rows). It answers 202; the
// environment reports deleted only when every cleanup step is done — until
// then GET keeps reporting deleting with progress.
func (a *API) deleteEnvironment(w http.ResponseWriter, r *http.Request) {
	a.beginCleanup(w, r, domain.EnvDeleting)
}

// beginCleanup is the shared body of cancel and delete: it asks the store
// for the durable cleanup move and maps the failure modes. A repeated call
// (a retried DELETE) is the store's idempotency problem and re-answers 202.
func (a *API) beginCleanup(w http.ResponseWriter, r *http.Request, target domain.EnvState) {
	envID, ok := environmentIDFromPath(w, r)
	if !ok {
		return
	}
	env, err := a.envs.BeginCleanup(r.Context(), envID, target)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "no such environment")
	case errors.Is(err, store.ErrIllegalTransition):
		writeError(w, http.StatusConflict, "illegal_state",
			"the environment cannot be cleaned from its current state")
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal_error",
			"starting the cleanup workflow failed")
	default:
		writeEnv(w, http.StatusAccepted, env, false)
	}
}

// uuidPattern accepts the canonical 8-4-4-4-12 hexadecimal form that both
// newEnvironmentID produces and the environment_id uuid column accepts.
var uuidPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// environmentIDFromPath validates the {id} path segment as a UUID. Path
// ids are UUIDs by design; anything else is a miss, not a server error —
// forwarding a non-UUID into the uuid column makes Postgres raise SQLSTATE
// 22P02, which would surface to the client as a 500. It returns false and
// has already written the response when it fails.
func environmentIDFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(strings.ToLower(id)) {
		writeError(w, http.StatusNotFound, "not_found", "no such environment")
		return "", false
	}
	return id, true
}

// writeEnv renders an environment without step rows (create/replay paths).
func writeEnv(w http.ResponseWriter, status int, env domain.Environment, withStatusURL bool) {
	writeEnvironment(w, status, env, withStatusURL, nil)
}

// writeEnvironment renders an environment as its wire shape, deriving the
// redacted connection fields from persisted metadata, the {state, at} audit
// timeline from metadata, and the compensation plan's progress from the
// step rows (when supplied). Only the whitelisted metadata keys are read
// out; anything else in metadata (credential material above all) is never
// copied into a response.
func writeEnvironment(w http.ResponseWriter, status int, env domain.Environment, withStatusURL bool, steps []domain.Step) {
	resp := environmentResponse{
		EnvironmentID: env.ID,
		Status:        string(env.State),
		TenantID:      env.TenantID,
		Name:          env.Name,
		CreatedAt:     env.CreatedAt,
		UpdatedAt:     env.UpdatedAt,
	}
	if withStatusURL {
		resp.StatusURL = fmt.Sprintf("/v1/environments/%s", env.ID)
	}
	var meta struct {
		Host          string `json:"host"`
		Database      string `json:"database"`
		CredentialRef string `json:"credentialRef"`
	}
	if err := json.Unmarshal(env.Metadata, &meta); err == nil {
		resp.Host = meta.Host
		resp.Database = meta.Database
		resp.CredentialRef = meta.CredentialRef
	}
	resp.Timeline = timelineFromMetadata(env.Metadata)
	if needsCleanupProgress(env.State) {
		resp.Cleanup = cleanupProgressOf(env.State, steps)
	}
	writeJSON(w, status, resp)
}

// needsCleanupProgress reports whether the response should show the
// compensation plan's progress: only environments on a cleanup path (or
// carrying cleanup failure) hold cleanup rows.
func needsCleanupProgress(state domain.EnvState) bool {
	switch state {
	case domain.EnvDeleting, domain.EnvCancelling, domain.EnvFailed,
		domain.EnvDeleted, domain.EnvCancelled:
		return true
	}
	return false
}

// timelineFromMetadata extracts the store-written {state, at} audit entries
// from an environment's metadata. The entries are state names and
// timestamps by construction, but the extraction still filters every entry
// through the redaction rules so the response can never regress into
// shipping free text if the shape ever changes.
func timelineFromMetadata(metadata []byte) []timelineView {
	var doc struct {
		Timeline []struct {
			State string `json:"state"`
			At    string `json:"at"`
		} `json:"timeline"`
	}
	if err := json.Unmarshal(metadata, &doc); err != nil {
		return nil
	}
	if len(doc.Timeline) == 0 {
		return nil
	}
	out := make([]timelineView, 0, len(doc.Timeline))
	for _, e := range doc.Timeline {
		out = append(out, timelineView{State: redactText(e.State), At: redactText(e.At)})
	}
	return out
}

// cleanupProgressOf summarises an environment's compensation plan from its
// step rows. It is rendered only for environments on a cleanup path (or
// carrying its failure): everywhere else the field would claim meaning it
// does not have. A nil return omits the field.
func cleanupProgressOf(state domain.EnvState, steps []domain.Step) *cleanupProgress {
	switch state {
	case domain.EnvDeleting, domain.EnvCancelling, domain.EnvDeleted, domain.EnvCancelled,
		domain.EnvFailed:
	default:
		return nil
	}
	var done, failed, total int
	for _, st := range steps {
		switch {
		case isCleanupKind(st.Kind):
			total++
			switch st.State {
			case domain.StepDone:
				done++
			case domain.StepFailed:
				failed++
			}
		}
	}
	if total == 0 {
		return nil
	}
	return &cleanupProgress{Done: done, Total: total, Failed: failed}
}

// isCleanupKind reports whether the step kind belongs to the compensation
// plan (drop_database, drop_role, delete_secret, release_capacity).
func isCleanupKind(kind domain.StepKind) bool {
	for _, k := range domain.CleanupPlan {
		if k == kind {
			return true
		}
	}
	return false
}

// writeError is the central error mapper: every non-2xx body is the same
// {"error":{"code","message"}} shape, and messages are scrubbed so an
// internal error's text cannot leak secret material.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{
		Code:    code,
		Message: redactText(message),
	}})
}

// writeJSON writes one JSON document with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// compactJSON normalises a spec document for value comparison and for
// storage; nil becomes the empty object.
func compactJSON(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return []byte(`{}`), nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// equalJSONValues compares two JSON documents by value, so an idempotency
// fingerprint cannot depend on representation. This matters because Postgres
// jsonb does not preserve object key order (it sorts keys by length, then
// bytewise), so the stored spec can differ byte-wise from the request that
// produced it while holding the same value. Both sides are decoded into
// generic Go values and compared; messages with numbers are compared as
// decoded float64 values, which is exact for the magnitudes a spec carries.
// On the (impossible under the write path, which validates first) path where
// a document is not valid JSON the comparison falls back to byte equality so
// it stays conservative rather than claiming "same".
func equalJSONValues(a, b []byte) bool {
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(va, vb)
}

// newEnvironmentID generates a random UUIDv4 for the environment row.
func newEnvironmentID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is a process-level problem; there is nothing
		// safe to write with a predictable id, so fail the request.
		return ""
	}
	// Set the version-4 and variant bits per RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
