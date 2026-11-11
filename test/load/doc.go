//go:build integration

// Package load runs the phase-9 load profile: concurrent environment
// creation at a documented rate against the real compose stack, reporting
// time-to-ready percentiles, the error rate, and the duplicate-resource
// count (which must be zero — that assertion is the point).
//
// Profile knobs (env-overridable, all with defaults):
//
//	LOAD_ENVS       environments to create     (default 16)
//	LOAD_RATE_PER_S creation rate              (default 4/s)
//	LOAD_WORKERS    worker pool concurrency    (default 4)
//	LOAD_RESULTS_FILE  optional path for a machine-readable JSON report
//
// Run via `make load` or `make benchmark` (requires `make up`).
package load
