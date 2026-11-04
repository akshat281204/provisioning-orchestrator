// reconcile_test.go — the three observable outcomes of phase 7, each pinned
// against the reconciler with in-memory probe fakes: an exists-and-matches
// world says done, an absent resource says pending, and an
// exists-but-differs world says failed. No verdict may come from an
// observation error.
package adapters

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// fakeCapacity answers "is the reservation held?".
type fakeCapacityProbe struct{ held bool }

func (f fakeCapacityProbe) Held(context.Context, string) (bool, error) { return f.held, nil }

// fakeDatabasesProbe answers "does the database exist?".
type fakeDatabasesProbe struct{ exists bool }

func (f fakeDatabasesProbe) Exists(context.Context, string) (bool, error) { return f.exists, nil }

// fakeRolesProbe answers "does the role exist?".
type fakeRolesProbe struct{ exists bool }

func (f fakeRolesProbe) Exists(context.Context, string) (bool, error) { return f.exists, nil }

// fakeSecretsProbe answers with a fixed payload or a fixed error.
type fakeSecretsProbe struct {
	payload map[string]string
	err     error
}

func (f fakeSecretsProbe) Get(context.Context, string) (map[string]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.payload, nil
}

// reconcilerWith builds a Reconciler over the given probes.
func reconcilerWith(c CapacityProbe, d DatabasesProbe, r RolesProbe, s SecretsProbe) *Reconciler {
	return &Reconciler{Capacity: c, Databases: d, Roles: r, Secrets: s}
}

// reconcileOne runs one step through the reconciler and returns what it
// decided (outcome, error).
func reconcileOne(t *testing.T, r *Reconciler, step domain.Step) (ReconcileOutcome, error) {
	t.Helper()
	return r.Reconcile(context.Background(), step)
}

// wantStep builds a step of the given kind for a well-formed environment id.
func wantStep(kind domain.StepKind) domain.Step {
	return domain.Step{ID: "step-1", EnvironmentID: "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", StepIndex: 1, Kind: kind}
}

// TestReconcileExistsAndMatchesRecordsDone: when the world holds exactly the
// resource the step was creating, the ambiguity resolves to done.
func TestReconcileExistsAndMatchesRecordsDone(t *testing.T) {
	cases := []struct {
		name string
		mk   func() *Reconciler
		kind domain.StepKind
	}{
		{"reserve_capacity", func() *Reconciler {
			return reconcilerWith(fakeCapacityProbe{held: true}, nil, nil, nil)
		}, domain.KindReserveCapacity},
		{"create_database", func() *Reconciler {
			return reconcilerWith(nil, fakeDatabasesProbe{exists: true}, nil, nil)
		}, domain.KindCreateDatabase},
		{"store_credentials", func() *Reconciler {
			envID := wantStep(domain.KindStoreCreds).EnvironmentID
			return reconcilerWith(nil, nil, nil, fakeSecretsProbe{
				payload: map[string]string{
					"database": DatabaseName(envID),
					"username": RoleName(envID),
					"password": "p",
				},
			})
		}, domain.KindStoreCreds},
		{"create_role", func() *Reconciler {
			return reconcilerWith(nil, nil, fakeRolesProbe{exists: true}, nil)
		}, domain.KindCreateRole},
		{"delete_secret (secret gone)", func() *Reconciler {
			return reconcilerWith(nil, nil, nil, fakeSecretsProbe{
				err: fmt.Errorf("probe: %w", ErrSecretNotFound),
			})
		}, domain.KindDeleteSecret},
		{"release_capacity (reservation released)", func() *Reconciler {
			return reconcilerWith(fakeCapacityProbe{}, nil, nil, nil)
		}, domain.KindReleaseCapacity},
		{"drop_database (database gone)", func() *Reconciler {
			return reconcilerWith(nil, fakeDatabasesProbe{}, nil, nil)
		}, domain.KindDropDatabase},
		{"drop_role (role gone)", func() *Reconciler {
			return reconcilerWith(nil, nil, fakeRolesProbe{}, nil)
		}, domain.KindDropRole},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := reconcileOne(t, tc.mk(), wantStep(tc.kind))
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got != OutcomeDone {
				t.Fatalf("outcome = %q, want done", got)
			}
		})
	}
}

// TestReconcileAbsentReturnsPending: when the resource the step was creating
// is provably absent (or a compensation target is provably still present),
// the outcome is pending — re-executed by the ordinary claim.
func TestReconcileAbsentReturnsPending(t *testing.T) {
	cases := []struct {
		name string
		mk   func() *Reconciler
		kind domain.StepKind
	}{
		{"reserve_capacity (nothing held)", func() *Reconciler {
			return reconcilerWith(fakeCapacityProbe{}, nil, nil, nil)
		}, domain.KindReserveCapacity},
		{"create_database (absent)", func() *Reconciler {
			return reconcilerWith(nil, fakeDatabasesProbe{}, nil, nil)
		}, domain.KindCreateDatabase},
		{"store_credentials (secret absent)", func() *Reconciler {
			return reconcilerWith(nil, nil, nil, fakeSecretsProbe{
				err: fmt.Errorf("probe: %w", ErrSecretNotFound),
			})
		}, domain.KindStoreCreds},
		{"create_role (absent)", func() *Reconciler {
			return reconcilerWith(nil, nil, fakeRolesProbe{}, nil)
		}, domain.KindCreateRole},
		{"drop_database (still present)", func() *Reconciler {
			return reconcilerWith(nil, fakeDatabasesProbe{exists: true}, nil, nil)
		}, domain.KindDropDatabase},
		{"drop_role (still present)", func() *Reconciler {
			return reconcilerWith(nil, nil, fakeRolesProbe{exists: true}, nil)
		}, domain.KindDropRole},
		{"release_capacity (still held)", func() *Reconciler {
			return reconcilerWith(fakeCapacityProbe{held: true}, nil, nil, nil)
		}, domain.KindReleaseCapacity},
		{"delete_secret (still present)", func() *Reconciler {
			return reconcilerWith(nil, nil, nil, fakeSecretsProbe{
				payload: map[string]string{"database": "d", "username": "u", "password": "p"},
			})
		}, domain.KindDeleteSecret},
		{"grant_privileges (role absent)", func() *Reconciler {
			return reconcilerWith(nil, nil, fakeRolesProbe{}, nil)
		}, domain.KindGrantPrivs},
		{"verify_usable", func() *Reconciler {
			return reconcilerWith(nil, nil, nil, nil)
		}, domain.KindVerifyUsable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := reconcileOne(t, tc.mk(), wantStep(tc.kind))
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got != OutcomePending {
				t.Fatalf("outcome = %q, want pending", got)
			}
		})
	}
}

// TestReconcileExistsButDiffersRecordsFailed: a resource that exists but
// disagrees with the plan is a verdict-shaped failure — surfaced as failed,
// never retried blindly.
func TestReconcileExistsButDiffersRecordsFailed(t *testing.T) {
	// The one observable mismatch: a stored credential document whose
	// payload cannot serve the plan (missing keys). exists-but-differs.
	r := reconcilerWith(nil, nil, nil, fakeSecretsProbe{
		payload: map[string]string{"database": "db-xyz"}, // username/password missing
	})
	got, err := reconcileOne(t, r, wantStep(domain.KindStoreCreds))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got != OutcomeFailed {
		t.Fatalf("outcome = %q, want failed", got)
	}
}

func TestReconcileSecretNamesMustMatchEnvironment(t *testing.T) {
	step := wantStep(domain.KindStoreCreds)
	r := reconcilerWith(nil, nil, nil, fakeSecretsProbe{
		payload: map[string]string{"database": "other-db", "username": "other-role", "password": "p"},
	})
	got, err := reconcileOne(t, r, step)
	if err != nil || got != OutcomeFailed {
		t.Fatalf("mismatched credential payload: outcome=%q err=%v, want failed", got, err)
	}
}

func TestReconcileMalformedSecretIsNotTreatedAsAbsent(t *testing.T) {
	for _, kind := range []domain.StepKind{domain.KindStoreCreds, domain.KindDeleteSecret} {
		t.Run(string(kind), func(t *testing.T) {
			r := reconcilerWith(nil, nil, nil, fakeSecretsProbe{
				err: fmt.Errorf("probe: %w", ErrSecretMalformed),
			})
			got, err := reconcileOne(t, r, wantStep(kind))
			if err != nil || got != OutcomeFailed {
				t.Fatalf("malformed secret: outcome=%q err=%v, want failed", got, err)
			}
		})
	}
}

// TestReconcileObservationErrorIsNeverAVerdict pins the no-guess rule: an
// unobservable world answers an error and nothing is decided.
func TestReconcileObservationErrorIsNeverAVerdict(t *testing.T) {
	r := reconcilerWith(nil, nil, nil, fakeSecretsProbe{err: errors.New("vault timeout")})
	for _, kind := range []domain.StepKind{domain.KindStoreCreds, domain.KindDeleteSecret} {
		if _, err := r.Reconcile(context.Background(), wantStep(kind)); err == nil {
			t.Fatalf("reconcile %s: an unobservable world must error, not decide", kind)
		}
	}
}
