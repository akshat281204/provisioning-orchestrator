// Package adapters holds the narrow interfaces the worker's steps exercise
// (Capacity, Databases, Roles, Secrets) and their local implementations.
// Every operation is idempotent or queryable, because every step will be
// retried after a crash: at-least-once execution is the design contract.
package adapters

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// maxIdentifierLen is the cap on generated SQL identifiers. It is well under
// Postgres's 63-byte identifier limit, leaving room for any quoting or
// prefixing Postgres itself adds without truncation.
const maxIdentifierLen = 40

// Deterministic naming. The environment ID is the only input, so a reopened
// step re-derives the same database / role / secret path instead of inventing
// a new resource: sanitize the id to identifier-safe text and bind a stable
// hash suffix derived from the full id, which keeps names collision-free
// across environments even when sanitization would fold two ids together.

// DatabaseName is the provisioned database identifier for envID.
func DatabaseName(envID string) string {
	return envResourceName("px", envID)
}

// RoleName is the provisioned login-role identifier for envID.
func RoleName(envID string) string {
	return envResourceName("pu", envID)
}

// maxSecretPathSegment caps the generated Vault path segment. Vault paths
// are not SQL identifiers, but the same bounded-length principle applies.
const maxSecretPathSegment = 50

// SecretPath is the Vault KV path (within the configured mount) that holds
// one environment's provisioned credentials. Only the characters a-z, 0-9,
// '-' and '_' survive, and the segment is length-capped, so the path is a
// stable, safe single Vault path per environment.
func SecretPath(envID string) string {
	suffix := stableSuffix(envID)
	base := sanitizeIdentifier(envID)
	// Same truncation rule as identifiers: the suffix carries uniqueness,
	// the middle only readability.
	budget := maxSecretPathSegment - len(suffix) - 1
	if budget < 1 {
		budget = 1
	}
	if len(base) > budget {
		base = base[:budget]
	}
	return "provisioning/env/" + base + "-" + suffix
}

// envResourceName builds "prefix_sanitized_hash" capped at maxIdentifierLen.
// The prefix keeps names identifier-safe (they start with a letter) and
// separates provisioning resources from operator-created ones by sight.
func envResourceName(prefix, envID string) string {
	suffix := stableSuffix(envID)
	base := sanitizeIdentifier(envID)
	// Truncate the sanitized middle, never the suffix: the suffix carries
	// uniqueness, the middle only readability.
	budget := maxIdentifierLen - len(prefix) - len(suffix) - 2
	if budget < 1 {
		budget = 1
	}
	if len(base) > budget {
		base = base[:budget]
	}
	return prefix + "_" + base + "_" + suffix
}

// sanitizeIdentifier lowercases envID and maps every character outside
// a-z/0-9/'-'/'_' to '_' so the result is a legal unquoted identifier piece
// and a legal Vault path segment.
func sanitizeIdentifier(envID string) string {
	return strings.Map(func(r rune) rune {
		r = unicode.ToLower(r)
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, envID)
}

// stableSuffix is 8 hex characters of the SHA-256 of the full environment id.
// It is what makes names collision-free: two environments whose sanitized
// ids truncate or fold to the same readable middle still differ in suffix.
func stableSuffix(envID string) string {
	sum := sha256.Sum256([]byte(envID))
	return hex.EncodeToString(sum[:4])
}
