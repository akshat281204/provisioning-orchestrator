// Package httpapi implements the /v1 HTTP surface from docs/design.md. Wire
// shapes live in dto.go so the domain types stay transport-agnostic; every
// response body passes through the redaction rules before it is written.
package httpapi

import "encoding/json"

// createRequest is the POST /v1/environments body. Spec stays raw so an
// unknown-but-valid JSON document round-trips into the persisted spec column.
type createRequest struct {
	TenantID string          `json:"tenantId"`
	Name     string          `json:"name"`
	Spec     json.RawMessage `json:"spec"`
}

// environmentResponse is the environment POST body (creating and replayed)
// and the GET /v1/environments/{id} body. Not-yet-known connection fields
// (host, database, credentialRef) are omitted until the workflow has written
// them; credential material is never a field of this type at all.
type environmentResponse struct {
	EnvironmentID string `json:"environmentId"`
	Status        string `json:"status"`
	StatusURL     string `json:"statusUrl,omitempty"`
	TenantID      string `json:"tenantId,omitempty"`
	Name          string `json:"name,omitempty"`
	Host          string `json:"host,omitempty"`
	Database      string `json:"database,omitempty"`
	CredentialRef string `json:"credentialRef,omitempty"`
	CreatedAt     string `json:"createdAt,omitempty"`
	UpdatedAt     string `json:"updatedAt,omitempty"`
	// Timeline is the compact audit trail the workflow appends on every
	// environment state move ({state, at} pairs, state names only). It is
	// written by the store JSON-side and carries no free text, so it is
	// redacted by construction; it is filtered again on the way out.
	Timeline []timelineView `json:"timeline,omitempty"`
	// Cleanup carries progress only while a cleanup workflow is underway
	// (deleting, cancelling, or carrying a failed cleanup step): deletion
	// is a workflow, and the response must show where it stands.
	Cleanup *cleanupProgress `json:"cleanup,omitempty"`
}

// timelineView is one entry of the audit timeline: a state name and the
// time it was entered. No other field exists on purpose.
type timelineView struct {
	State string `json:"state"`
	At    string `json:"at"`
}

// cleanupProgress summarises the compensation plan's progress.
type cleanupProgress struct {
	Done   int `json:"done"`
	Total  int `json:"total"`
	Failed int `json:"failed,omitempty"`
}

// stepView is one redacted history entry: progress fields plus a scrubbed
// last_error. It deliberately has no metadata field at all, so raw step
// metadata cannot leak into a response by construction.
type stepView struct {
	StepIndex int     `json:"stepIndex"`
	Kind      string  `json:"kind"`
	State     string  `json:"state"`
	Attempts  int     `json:"attempts"`
	CreatedAt string  `json:"createdAt"`
	UpdatedAt string  `json:"updatedAt"`
	LastError *string `json:"lastError"`
}

// historyResponse is the GET /v1/environments/{id}/history body.
type historyResponse struct {
	EnvironmentID string     `json:"environmentId"`
	Steps         []stepView `json:"steps"`
}

// errorResponse is the central error shape: {"error":{"code","message"}}.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
