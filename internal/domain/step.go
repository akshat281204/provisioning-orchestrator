package domain

import "fmt"

// Environment is the domain representation of one tenant environment. It is
// the Go mirror of the `environments` row; persistence maps it to SQL.
type Environment struct {
	ID             string
	TenantID       string
	Name           string
	State          EnvState
	IdempotencyKey string
	// Spec and Metadata are opaque JSON documents carried with the
	// environment. []byte holding JSON so persistence can scan jsonb directly.
	Spec      []byte
	Metadata  []byte
	CreatedAt string
	UpdatedAt string
	// ExpiresAt is optional; empty means the environment never expires.
	ExpiresAt string
}

// Step is the domain representation of one workflow_step row. step_index,
// kind, and operation_key are immutable; everything else tracks progress.
type Step struct {
	ID            string
	EnvironmentID string
	StepIndex     int
	Kind          StepKind
	State         StepState
	Attempts      int
	// LeaseOwner and LeaseExpiresAt are coordination fields, not states;
	// empty/expired means no worker owns the step.
	LeaseOwner     string
	LeaseExpiresAt string
	// OperationKey is the stable external-operation identity
	// env/<environment UUID>/step/<step_index>/<kind>.
	OperationKey string
	Metadata     []byte
	LastError    string
	CreatedAt    string
	UpdatedAt    string
	// Reclaimed is a claim-path flag, not persisted state: it reports that
	// the pre-claim state was `running` on an owner whose lease had
	// expired — the observable signature of a dead worker. Ordinary
	// pending claims leave it false, so a counter may distinguish real
	// worker deaths from routine execution.
	Reclaimed bool
}

// OperationKey builds the stable operation-key string for a step. It contains
// no credential or client-supplied secret.
func OperationKey(environmentID string, stepIndex int, kind StepKind) string {
	return fmt.Sprintf("env/%s/step/%d/%s", environmentID, stepIndex, kind)
}
