package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// fake adapters recording what the executor called, without any real
// dependency: the dispatch and payload decisions are unit-testable offline.

type fakeCapacity struct {
	mu      sync.Mutex
	reserve map[string]int
	release map[string]int
}

func newFakeCapacity() *fakeCapacity {
	return &fakeCapacity{reserve: map[string]int{}, release: map[string]int{}}
}

func (f *fakeCapacity) Reserve(_ context.Context, envID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserve[envID]++
	return nil
}

func (f *fakeCapacity) Release(_ context.Context, envID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.release[envID]++
	return nil
}

type fakeDatabases struct {
	mu      sync.Mutex
	ensures map[string]int
	dropped []string
}

func newFakeDatabases() *fakeDatabases {
	return &fakeDatabases{ensures: map[string]int{}}
}

func (f *fakeDatabases) Ensure(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensures[name]++
	return nil
}

func (f *fakeDatabases) Exists(_ context.Context, name string) (bool, error) {
	return f.ensures[name] > 0, nil
}

func (f *fakeDatabases) Drop(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped = append(f.dropped, name)
	return nil
}

type fakeRoles struct {
	mu      sync.Mutex
	ensured map[string]string // name -> last password
	granted []string
	dropped []string
}

func (f *fakeRoles) Ensure(_ context.Context, name, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured[name] = password
	return nil
}

func (f *fakeRoles) Exists(_ context.Context, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.ensured[name]
	return ok, nil
}

func (f *fakeRoles) GrantConnect(_ context.Context, role, database string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.granted = append(f.granted, "connect:"+role+"@"+database)
	return nil
}

func (f *fakeRoles) GrantUsage(_ context.Context, role, database string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.granted = append(f.granted, "usage:"+role+"@"+database)
	return nil
}

func (f *fakeRoles) Drop(_ context.Context, role string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped = append(f.dropped, role)
	return nil
}

type fakeSecrets struct {
	mu             sync.Mutex
	store          map[string]map[string]string
	getErr, putErr error
	gets, written  int
}

func newFakeSecrets() *fakeSecrets {
	return &fakeSecrets{store: map[string]map[string]string{}}
}

func (f *fakeSecrets) Put(_ context.Context, envID string, payload map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	f.written++
	f.store[envID] = payload
	return nil
}

// writes counts completed Put calls.
func (f *fakeSecrets) writes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.written
}

func (f *fakeSecrets) Get(_ context.Context, envID string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	p, ok := f.store[envID]
	if !ok {
		return nil, fmt.Errorf("missing: %w", adapters.ErrSecretNotFound)
	}
	out := make(map[string]string, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out, nil
}

func (f *fakeSecrets) Delete(_ context.Context, envID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.store, envID)
	return nil
}

const envID = "00000000-0000-4000-8000-000000000042"

func newAdapterExecutor() (
	*AdapterExecutor, *fakeCapacity, *fakeDatabases, *fakeRoles, *fakeSecrets) {
	cap := newFakeCapacity()
	dbs := newFakeDatabases()
	rol := &fakeRoles{ensured: map[string]string{}}
	sec := newFakeSecrets()
	return &AdapterExecutor{Capacity: cap, Databases: dbs, Roles: rol, Secrets: sec}, cap, dbs, rol, sec
}

func TestAdapterExecutorStoresCredentialsOnce(t *testing.T) {
	ex, _, _, _, sec := newAdapterExecutor()
	ctx := context.Background()
	step := domain.Step{EnvironmentID: envID, Kind: domain.KindStoreCreds, OperationKey: domain.OperationKey(envID, 3, domain.KindStoreCreds)}

	if err := ex.Execute(ctx, step); err != nil {
		t.Fatalf("first store_credentials: %v", err)
	}
	first, err := sec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Re-execution (crash + reclaim) must reuse the first password, not
	// rotate credentials under a possibly ready role.
	if err := ex.Execute(ctx, step); err != nil {
		t.Fatalf("second store_credentials: %v", err)
	}
	second, err := sec.Get(ctx, envID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if first[passwordKeyCheck] != second[passwordKeyCheck] {
		t.Error("store_credentials rotated the password on retry")
	}
	if sec.writes() != 1 {
		t.Errorf("store wrote %d times, want exactly 1", sec.writes())
	}
	if first[payloadKeyDatabase] != adapters.DatabaseName(envID) ||
		first[payloadKeyUsername] != adapters.RoleName(envID) {
		t.Errorf("payload names do not match the generated ones: %v", first)
	}
	if first[payloadKeyPassword] == "" {
		t.Error("payload carries an empty password")
	}
}

const passwordKeyCheck = payloadKeyPassword

func TestAdapterExecutorRoleThenGrantUsesStoredPayload(t *testing.T) {
	ex, _, dbs, rol, _ := newAdapterExecutor()
	ctx := context.Background()

	credStep := domain.Step{EnvironmentID: envID, Kind: domain.KindStoreCreds}
	roleStep := domain.Step{EnvironmentID: envID, Kind: domain.KindCreateRole}
	grantStep := domain.Step{EnvironmentID: envID, Kind: domain.KindGrantPrivs}
	dbStep := domain.Step{EnvironmentID: envID, Kind: domain.KindCreateDatabase}

	if err := ex.Execute(ctx, dbStep); err != nil {
		t.Fatalf("create_database: %v", err)
	}
	if err := ex.Execute(ctx, credStep); err != nil {
		t.Fatalf("store_credentials: %v", err)
	}
	if err := ex.Execute(ctx, roleStep); err != nil {
		t.Fatalf("create_role: %v", err)
	}
	if got := rol.ensured[adapters.RoleName(envID)]; got == "" {
		t.Errorf("role ensured with empty password")
	}
	if err := ex.Execute(ctx, grantStep); err != nil {
		t.Fatalf("grant_privileges: %v", err)
	}
	want := []string{
		"connect:" + adapters.RoleName(envID) + "@" + adapters.DatabaseName(envID),
		"usage:" + adapters.RoleName(envID) + "@" + adapters.DatabaseName(envID),
	}
	if len(rol.granted) != 2 {
		t.Fatalf("granted %v, want connect+usage only", rol.granted)
	}
	for i, w := range want {
		if rol.granted[i] != w {
			t.Errorf("grant %d = %q, want %q", i, rol.granted[i], w)
		}
	}
	if got := dbs.ensures[adapters.DatabaseName(envID)]; got != 1 {
		t.Errorf("database ensured %d times, want 1", got)
	}
}

func TestAdapterExecutorCleanupKinds(t *testing.T) {
	ex, cap, dbs, rol, sec := newAdapterExecutor()
	ctx := context.Background()

	steps := []domain.Step{
		{EnvironmentID: envID, Kind: domain.KindDropDatabase},
		{EnvironmentID: envID, Kind: domain.KindDropRole},
		{EnvironmentID: envID, Kind: domain.KindDeleteSecret},
		{EnvironmentID: envID, Kind: domain.KindReleaseCapacity},
	}
	for _, s := range steps {
		if err := ex.Execute(ctx, s); err != nil {
			t.Fatalf("%s: %v", s.Kind, err)
		}
	}
	if len(dbs.dropped) != 1 || dbs.dropped[0] != adapters.DatabaseName(envID) {
		t.Errorf("database drop calls: %v", dbs.dropped)
	}
	if len(rol.dropped) != 1 || rol.dropped[0] != adapters.RoleName(envID) {
		t.Errorf("role drop calls: %v", rol.dropped)
	}
	if _, ok := sec.store[envID]; ok {
		t.Error("secret was not deleted")
	}
	if cap.release[envID] != 1 {
		t.Error("capacity was not released")
	}
}

func TestAdapterExecutorReserveAndUnknownKind(t *testing.T) {
	ex, cap, _, _, _ := newAdapterExecutor()
	ctx := context.Background()
	if err := ex.Execute(ctx, domain.Step{EnvironmentID: envID, Kind: domain.KindReserveCapacity}); err != nil {
		t.Fatalf("reserve_capacity: %v", err)
	}
	if cap.reserve[envID] != 1 {
		t.Error("reserve not delegated to the capacity adapter")
	}
	if err := ex.Execute(ctx, domain.Step{EnvironmentID: envID, Kind: domain.KindVerifyUsable}); err == nil {
		t.Error("verify_usable should fail loudly until phase 6 implements it")
	}
	if err := ex.Execute(ctx, domain.Step{EnvironmentID: envID, Kind: "not_a_kind"}); err == nil {
		t.Error("unmapped kind must error, not succeed silently")
	}
}

func TestAdapterExecutorCreateRoleWithoutSecret(t *testing.T) {
	ex, _, _, _, _ := newAdapterExecutor()
	err := ex.Execute(context.Background(),
		domain.Step{EnvironmentID: envID, Kind: domain.KindCreateRole})
	if err == nil {
		t.Fatal("creating a role without stored credentials must fail")
	}
	if !strings.Contains(err.Error(), "secret not found") {
		t.Errorf("error should wrap the missing-secret cause: %v", err)
	}
}

func TestAdapterExecutorSecretPutFailure(t *testing.T) {
	ex, _, _, _, sec := newAdapterExecutor()
	sec.putErr = errors.New("vault down")
	err := ex.Execute(context.Background(),
		domain.Step{EnvironmentID: envID, Kind: domain.KindStoreCreds})
	if err == nil {
		t.Fatal("a Put failure must be surfaced")
	}
}
