package httpapi

import (
	"regexp"
	"strings"
)

// Secret-looking content is scrubbed from every response body. These rules
// exist so a step error (or the message built while handling one) can never
// carry the very credential the workflow just created, even by accident: the
// former owner of this text may have embedded a connection string, a
// key=value assignment, or a bearer token.

// keyValSecret matches `password=hunter2`, `token: abc`, `pwd="hunter2"`,
// `access_token=abc123`, `{"password":"hunter2"}` and friends — the common
// shapes of credential leakage in driver errors and API error bodies.
//
// Three things make it catch more than a bare keyword match:
//   - `(?:[\w-]*)` before the keyword lets compound names through
//     (access_token, db_password, client_secret, apikey);
//   - the two optional quotes around the keyword let JSON-style keys match
//     (`"password":"hunter2"`), and the replacement keeps them so the
//     surrounding JSON stays readable; they are matched as independent
//     optional groups because RE2 has no backreferences;
//   - the quoted-value alternative spans escaped and empty strings, so a
//     Vault-style body `{"password":""}` still has its value replaced.
//
// The value is always replaced wholesale with the marker, regardless of
// whether it was quoted; keeping the quotes is not worth per-format
// replacement rules, and dropping them is safe.
var keyValSecret = regexp.MustCompile(
	`(?i)(["']?)((?:[\w-]*)(?:password|passwd|pwd|secret|token|api[_-]?key)[\w-]*)(["']?)` +
		`([\s]*[:=][\s]*)` +
		`(?:"(?:\\.|[^"\\])*"|[^\s;,"&)]+)`)

// urlPassword matches the password component of URLs like
// postgres://user:hunter2@host/db, keeping the username and host visible.
var urlPassword = regexp.MustCompile(`(\w+://[^:/\s@]+:)([^/\s@]+)(@)`)

// bearerSecret matches Authorization header values embedded verbatim into a
// message, e.g. Bearer eyJhbGci...
var bearerSecret = regexp.MustCompile(`(?i)\b(bearer)[\s]+[A-Za-z0-9._~+/=-]{8,}`)

// maxRedactedText bounds how much of an error string reaches a response. A
// truncated message loses context but never loses a secret it was truncated
// away from... no rule can promise that, so redaction runs first and the
// cap exists to keep huge payloads off the wire. Truncation only ever
// shortens a string; it cannot reveal a secret that surviving redaction
// already failed to catch.
const maxRedactedText = 512

const redactedMarker = "[REDACTED]"

// redactText scrubs secret-shaped content from an arbitrary string and caps
// its length. It is applied to last_error and to every error-body message
// before anything is serialised to a client.
func redactText(s string) string {
	if s == "" {
		return s
	}
	out := keyValSecret.ReplaceAllString(s, "$1$2$3$4"+redactedMarker)
	out = urlPassword.ReplaceAllString(out, "$1"+redactedMarker+"$3")
	out = bearerSecret.ReplaceAllString(out, "$1 "+redactedMarker)
	if len(out) > maxRedactedText {
		// The cap only shortens; a byte-boundary slice can split a rune, so
		// repair the tail instead of shipping invalid UTF-8.
		out = strings.ToValidUTF8(out[:maxRedactedText], "")
	}
	return out
}
