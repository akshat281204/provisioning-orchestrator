// Package telemetry centralises the credential-safety rules every observable
// surface must go through: response bodies, log records, and span attributes.
// A secret may exist in memory, inside the DDL statement Postgres parses,
// or inside Vault — but it must never survive into anything a human or a
// client can read back. The single RedactText helper is the only sanctioned
// scrubber; httpapi, the slog handler wrapper, and (once phase 8 lands) the
// OTel span processor all funnel through it.
//
// There is no actual tracing pipeline yet (phase 8 adds OTel). When it
// arrives, span attributes must be created only through helper functions
// that call RedactText first; until then this package owns the policy.
package telemetry

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
)

// Secret-looking content is scrubbed from every observable surface. These
// rules exist so a step error (or the message built while handling one) can
// never carry the very credential the workflow just created, even by
// accident: the former owner of this text may have embedded a connection
// string, a key=value assignment, or a bearer token.

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

// maxRedactedText bounds how much of an error string reaches any surface. A
// truncated message loses context but never loses a secret it was truncated
// away from... no rule can promise that, so redaction runs first and the
// cap exists to keep huge payloads off the wire. Truncation only ever
// shortens a string; it cannot reveal a secret that surviving redaction
// already failed to catch.
const maxRedactedText = 512

// RedactedMarker is the fixed replacement for a scrubbed value.
const RedactedMarker = "[REDACTED]"

// RedactText scrubs secret-shaped content from an arbitrary string and caps
// its length. It is applied to every log record, every span attribute, and
// every error-body message before anything is serialised to a client.
func RedactText(s string) string {
	if s == "" {
		return s
	}
	out := keyValSecret.ReplaceAllString(s, "$1$2$3$4"+RedactedMarker)
	out = urlPassword.ReplaceAllString(out, "$1"+RedactedMarker+"$3")
	out = bearerSecret.ReplaceAllString(out, "$1 "+RedactedMarker)
	if len(out) > maxRedactedText {
		// The cap only shortens; a byte-boundary slice can split a rune, so
		// repair the tail instead of shipping invalid UTF-8.
		out = strings.ToValidUTF8(out[:maxRedactedText], "")
	}
	return out
}

// redactAttr scrubs one log attribute. String values (and error values) are
// scrubbed wholesale; group attributes recurse into their children so
// logger.WithGroup-structured records are covered too. Other kinds (int,
// bool, time) cannot carry a credential we produced.
func redactAttr(a slog.Attr) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindString:
		return slog.String(a.Key, RedactText(a.Value.String()))
	case slog.KindAny:
		if e, ok := a.Value.Any().(error); ok {
			return slog.String(a.Key, RedactText(e.Error()))
		}
	case slog.KindGroup:
		sub := a.Value.Group()
		for i, s := range sub {
			sub[i] = redactAttr(s)
		}
		return slog.Any(a.Key, sub)
	}
	return a
}

// RedactingHandler wraps a slog.Handler so every record's message and every
// attribute value passes through RedactText before the wrapped handler sees
// it. WithAttrs/WithGroup are chained with pre-scrubbed values so both
// logger.With(...) attributes and per-call attributes are covered.
type RedactingHandler struct {
	next slog.Handler
}

// NewRedactingHandler wraps h.
func NewRedactingHandler(h slog.Handler) slog.Handler { return RedactingHandler{next: h} }

// Enabled delegates to the wrapped handler.
func (h RedactingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

// Handle drops the original record and re-adds every attribute after
// scrubbing, so per-call attributes (not just With(...) ones) are covered.
func (h RedactingHandler) Handle(ctx context.Context, rec slog.Record) error {
	clean := slog.Record{}
	clean.Time = rec.Time
	clean.Level = rec.Level
	clean.Message = RedactText(rec.Message)
	clean.PC = rec.PC
	rec.Attrs(func(a slog.Attr) bool {
		clean.AddAttrs(redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, clean)
}

// WithAttrs pre-scrubs each attribute before chaining.
func (h RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		clean[i] = redactAttr(a)
	}
	return RedactingHandler{next: h.next.WithAttrs(clean)}
}

// WithGroup chains unchanged; scrubbing happens at the attribute level.
func (h RedactingHandler) WithGroup(name string) slog.Handler {
	return RedactingHandler{next: h.next.WithGroup(name)}
}

var _ slog.Handler = RedactingHandler{}
