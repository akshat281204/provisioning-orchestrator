package telemetry

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactTextScrubbing(t *testing.T) {
	cases := map[string]string{
		"plain text":                          "plain text",
		"password=hunter2":                    "password=[REDACTED]",
		`{"password":"hunter2"}`:              `{"password":[REDACTED]}`,
		"conn: postgres://u:hunter2@h/db":     "conn: postgres://u:[REDACTED]@h/db",
		"Authorization: Bearer eyJhbGciOi...": "Authorization: Bearer [REDACTED]",
		"access_token=abc123":                 "access_token=[REDACTED]",
		`pwd="s3cr3t"`:                        `pwd=[REDACTED]`,
	}
	for in, want := range cases {
		if got := RedactText(in); got != want {
			t.Errorf("RedactText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactTextCapsLength(t *testing.T) {
	long := strings.Repeat("x", maxRedactedText+64)
	got := RedactText(long)
	if len(got) != maxRedactedText {
		t.Errorf("redaction cap: %d bytes, want %d", len(got), maxRedactedText)
	}
}

// The handler must scrub the record message, per-call attributes,
// With(...) attributes, error-typed values, and values nested in groups.
func TestRedactingHandlerCoversEveryRecordField(t *testing.T) {
	buf := &bytes.Buffer{}
	log := slog.New(NewRedactingHandler(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	log.Info("connected with password=hunter2", "role", "u", "error", errors.New("password=oops"))
	if strings.Contains(buf.String(), "hunter2") || strings.Contains(buf.String(), "oops") {
		t.Fatalf("message or attribute leaked: %s", buf.String())
	}
	if !strings.Contains(buf.String(), RedactedMarker) {
		t.Errorf("scrubbed values are unmarked: %s", buf.String())
	}

	buf.Reset()
	log = slog.New(NewRedactingHandler(slog.NewJSONHandler(buf, nil))).
		With("pwd", "hunter3", slog.Group("env", slog.String("password", "hunter4")))
	log.Debug("no direct attrs here")
	text := buf.String()
	if strings.Contains(text, "hunter3") || strings.Contains(text, "hunter4") {
		t.Fatalf("With() attributes leaked: %s", text)
	}
}
