package httpapi

// The redaction rules live in internal/telemetry so the same helper scrubs
// response bodies, log records, and (once phase 8 lands) span attributes.
// This file keeps the package-local spelling used across the handlers.
import "github.com/akshat281204/provisioning-orchestrator/internal/telemetry"

// redactText scrubs secret-shaped content from an arbitrary string and caps
// its length. It is applied to last_error and to every error-body message
// before anything is serialised to a client.
func redactText(s string) string { return telemetry.RedactText(s) }
