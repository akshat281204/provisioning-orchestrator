package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/adapters"
	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
	"github.com/akshat281204/provisioning-orchestrator/internal/telemetry"
)

// fakeVerifier scripts one outcome per attempt (the last repeats forever);
// it is the retry-policy test rig.
type fakeVerifier struct {
	script []error
	calls  int
	user   []string
}

func (f *fakeVerifier) VerifyUsable(_ context.Context, _, user, _ string) error {
	i := f.calls
	if i >= len(f.script) {
		i = len(f.script) - 1
	}
	f.calls++
	f.user = append(f.user, user)
	return f.script[i]
}

// fakeGate records which state-machine moves the executor asked for.
type fakeGate struct {
	beginProvisioning, beginVerification, writeMeta int
	beginVerificationErr, writeMetaErr              error
}

func (g *fakeGate) BeginProvisioning(context.Context, string) error {
	g.beginProvisioning++
	return nil
}

func (g *fakeGate) BeginVerification(context.Context, string) error {
	g.beginVerification++
	return g.beginVerificationErr
}

func (g *fakeGate) WriteConnectionMeta(_ context.Context, _ string, meta []byte) error {
	g.writeMeta++
	return g.writeMetaErr
}

// newVerifyExecutor wires the full verify rig: credentials pre-stored in the
// fake secrets adapter, a scripted verifier, and a recording gate.
func newVerifyExecutor(script ...error) (*AdapterExecutor, *fakeSecrets, *fakeVerifier, *fakeGate) {
	ex, _, _, rol, sec := newAdapterExecutor()
	ver := &fakeVerifier{script: script}
	gate := &fakeGate{}
	ex.Verifier = ver
	ex.Envs = gate
	rol.ensured[adapters.RoleName(envID)] = "generated-earlier"
	sec.store[envID] = map[string]string{
		payloadKeyDatabase: adapters.DatabaseName(envID),
		payloadKeyUsername: adapters.RoleName(envID),
		payloadKeyPassword: "v",
	}
	// One short budget keeps the retry tests fast: two backoff slots fit
	// inside it (250ms each).
	ex.VerifyBudget = 600 * time.Millisecond
	return ex, sec, ver, gate
}

func verifyStep() domain.Step {
	return domain.Step{
		EnvironmentID: envID,
		Kind:          domain.KindVerifyUsable,
		OperationKey:  domain.OperationKey(envID, 6, domain.KindVerifyUsable),
	}
}

func TestVerifySuccessRunsThroughTheGate(t *testing.T) {
	ex, _, ver, gate := newVerifyExecutor(nil)
	_ = ex
	if err := ex.Execute(context.Background(), verifyStep()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if ver.calls != 1 {
		t.Errorf("probe ran %d times, want 1", ver.calls)
	}
	if ver.user[0] != adapters.RoleName(envID) {
		t.Errorf("probed as %q, want the provisioned role", ver.user[0])
	}
	if gate.beginVerification != 1 {
		t.Errorf("BeginVerification ran %d times, want 1", gate.beginVerification)
	}
	if gate.writeMeta != 1 {
		t.Errorf("WriteConnectionMeta ran %d times, want 1", gate.writeMeta)
	}
}

// An authentication failure must stop immediately: retrying bad credentials
// never converges. On this scripted verifier the transient error after the
// auth failure would never fire a second probe anyway — the assertion is the
// exact attempt count plus a permanent-shaped error.
func TestVerifyAuthFailureFailsFast(t *testing.T) {
	ex, _, ver, gate := newVerifyExecutor(
		fmt.Errorf("wrapped: %w", adapters.ErrVerificationAuth),
		fmt.Errorf("transient"),
		nil,
	)
	start := time.Now()
	err := ex.Execute(context.Background(), verifyStep())
	if err == nil {
		t.Fatal("verify with bad credentials succeeded")
	}
	if ver.calls != 1 {
		t.Errorf("probed %d times, want exactly 1 on auth failure", ver.calls)
	}
	if time.Since(start) > 30*time.Millisecond {
		t.Errorf("auth failure took %s to fail fast", time.Since(start))
	}
	// The error names the permanent cause so logs say why, and the gate ran.
	if !strings.Contains(err.Error(), "authentication for the provisioned role failed") {
		t.Errorf("error %q does not name the auth failure", err)
	}
	if gate.writeMeta != 0 {
		t.Error("a failed verification still wrote connection metadata")
	}
}

func TestVerifyGrantFailureIsPermanent(t *testing.T) {
	ex, _, ver, _ := newVerifyExecutor(
		fmt.Errorf("wrapped: %w", adapters.ErrVerificationGrants), nil)
	err := ex.Execute(context.Background(), verifyStep())
	if err == nil || ver.calls != 1 {
		t.Fatalf("grant failure outcome: %v after %d probes", err, ver.calls)
	}
}

func TestVerifyTransientRetriesThenSucceeds(t *testing.T) {
	ex, _, ver, gate := newVerifyExecutor(
		fmt.Errorf("connection refused"), nil)
	if err := ex.Execute(context.Background(), verifyStep()); err != nil {
		t.Fatalf("transient retry should converge: %v", err)
	}
	if ver.calls != 2 {
		t.Errorf("probed %d times, want 2 (one retry)", ver.calls)
	}
	if gate.writeMeta != 1 {
		t.Error("metadata written on a path that eventually succeeded")
	}
}

func TestVerifyBoundedDeadlineExhaustsRetries(t *testing.T) {
	ex, _, ver, _ := newVerifyExecutor(fmt.Errorf("connection refused"))
	err := ex.Execute(context.Background(), verifyStep())
	if err == nil {
		t.Fatal("a perpetually transient verifier cannot succeed")
	}
	if ver.calls < 2 {
		t.Errorf("probed %d times; the budget should back off and retry", ver.calls)
	}
}

func TestVerifyGateErrorFailsTheStepBeforeProbing(t *testing.T) {
	ex, _, ver, gate := newVerifyExecutor(nil)
	gate.beginVerificationErr = errors.New("store: illegal transition")
	err := ex.Execute(context.Background(), verifyStep())
	if err == nil || ver.calls != 0 {
		t.Fatalf("verify without the gate error: %v, probes=%d", err, ver.calls)
	}
}

// A redaction guarantee at the executor boundary: even a hostile verifier
// error naming a password sentinel carries it only inside the error value,
// and RedactText srips it wherever observability formats it. The log-pipeline
// composition test lives in the httpapi/app suites.
func TestVerifySentinelErrorTextIsScrubbable(t *testing.T) {
	sentinel := "SENTINEL-plaintext-777"
	ex, _, _, _ := newVerifyExecutor(nil)
	ex.Secrets.(*fakeSecrets).store[envID][payloadKeyPassword] = sentinel
	ex.Verifier = &fakeVerifier{
		script: []error{fmt.Errorf("role verification failed with password=%s", sentinel)},
	}
	ex.VerifyBudget = 20 * time.Millisecond

	err := ex.Execute(context.Background(), verifyStep())
	if err == nil {
		t.Fatal("hostile verifier error should fail the step")
	}
	text := telemetry.RedactText(err.Error())
	if strings.Contains(text, sentinel) {
		t.Fatalf("sentinel password survived redaction: %q", text)
	}
	if !strings.Contains(text, telemetry.RedactedMarker) {
		t.Errorf("redaction should mark the scrubbed value, got %q", text)
	}
}
