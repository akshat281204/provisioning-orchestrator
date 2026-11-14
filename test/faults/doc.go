//go:build integration

// Package faults holds the phase-9 fault-injection scenarios: one test per
// failure case in the project plan, each run against the real compose stack
// (Postgres + Vault) and asserting both the persisted workflow state (the
// environments and workflow_steps rows) and the resulting external resources
// (databases, roles, Vault secrets, capacity reservations) — never just HTTP
// codes.
//
// The tests carry the `integration` build tag and run via
// `make test-faults` (requires `make up`).
package faults
