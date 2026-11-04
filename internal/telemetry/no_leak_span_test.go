package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The acceptance guarantee for phase 8: no span attribute and no metric label
// contains credential material or a scoped identifier. The sentinel values
// stand in for (a) a generated password and (b) an environment UUID — the two
// things most at risk of leaking into telemetry surfaces.

const (
	sentinelPassword = "sentinel-hunter2-password"
	sentinelEnvID    = "0d3ddffa-e989-43d8-8892-d3ddffae989e"
)

// withRecorder installs a recording tracer provider and the W3C propagator,
// and returns (recorder, undo). The propagator install makes the traceparent
// join deterministic rather than dependent on the global default.
func withRecorder(t *testing.T) (*sdktracetest.SpanRecorder, func()) {
	t.Helper()
	rec := sdktracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return rec, func() {
		otel.SetTextMapPropagator(prevProp)
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	}
}

// assertNoSentinels fails the test if any span name, attribute or event
// carries a sentinel value.
func assertNoSentinels(t *testing.T, label string, rec *sdktracetest.SpanRecorder) {
	t.Helper()
	for _, sp := range rec.Ended() {
		payload := sp.Name() + "\n"
		for _, attr := range sp.Attributes() {
			payload += fmt.Sprintf("%s=%s\n", attr.Key, attr.Value.Emit())
		}
		for _, ev := range sp.Events() {
			payload += ev.Name + "\n"
			for _, attr := range ev.Attributes {
				payload += fmt.Sprintf("%s=%s\n", attr.Key, attr.Value.Emit())
			}
		}
		for _, s := range []string{sentinelPassword, sentinelEnvID} {
			if strings.Contains(payload, s) {
				t.Errorf("%s: span %q leaks %q:\n%s", label, sp.Name(), s, payload)
			}
		}
	}
}

// TestSpansExcludeSecretsAndIDs exercises the span helpers with a step whose
// metadata and error text are laced with sentinels; the emitted attribute set
// may only carry the bounded step fields (kind/index/attempt/outcome), never
// the secret, never the environment id.
func TestSpansExcludeSecretsAndIDs(t *testing.T) {
	rec, undo := withRecorder(t)
	defer undo()

	ctx := context.Background()

	// A valid traceparent plus a sentinel password in the rest of the
	// document: a cross-process resume must join the trace without importing
	// whatever else lives on the row.
	mt := []byte(fmt.Sprintf(
		`{"traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01","password":%q,"environment_id":%q}`,
		sentinelPassword, sentinelEnvID))

	sctx, span := StartStepSpanTracer(ctx, otelTracer(tracerName), mt, "create_database", 2, 3)
	joined := trace.SpanContextFromContext(sctx)
	if !joined.IsValid() {
		t.Fatal("expected the step span to join the trace carried in metadata")
	}
	if want, _ := trace.TraceIDFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); joined.TraceID() != want {
		t.Fatalf("step span started trace %s, want the traceparent's trace aaaa…", joined.TraceID())
	}
	span.SetAttributes(attribute.String("note", "plain text"))
	EndStepSpan(span, "done")

	// A failed attempt: error text is exactly where a generated password
	// could leak — FailSpan must scrub it before anything is recorded.
	_, fspan := StartStepSpan(ctx, mt, "verify_usable", 6, 1)
	FailSpan(fspan, "authentication failed: password="+sentinelPassword+" env="+sentinelEnvID)
	EndStepSpan(fspan, "failed")
	assertNoSentinels(t, "step spans", rec)

	// A request span: the path interpolates client-supplied environment ids
	// by construction, so the middleware must redact it.
	rec.Reset()
	TraceRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("POST", "/v1/environments/"+sentinelEnvID, nil))
	assertNoSentinels(t, "request span", rec)

	// A two-segment path (GET /v1/{id}) is exactly the boundary the route
	// allowlist exists for: the id-bearing segment must not reach the
	// http.route attribute — neither verbatim, nor as anything else the
	// client could have influenced. All recorded routes are fixed strings.
	rec.Reset()
	TraceRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/v1/"+sentinelEnvID, nil))
	gotRoute := ""
	for _, sp := range rec.Ended() {
		for _, attr := range sp.Attributes() {
			if attr.Key == "http.route" {
				gotRoute = attr.Value.Emit()
			}
		}
	}
	if gotRoute != "/v1" {
		t.Fatalf("two-segment id path recorded as route %q, want fixed /v1", gotRoute)
	}
	// An arbitrary non-API shape collapses to the opaque bucket rather than
	// carrying client text.
	rec.Reset()
	TraceRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/"+sentinelEnvID+"-arbitrary/free-text", nil))
	assertNoSentinels(t, "fallback request span", rec)
	for _, sp := range rec.Ended() {
		for _, attr := range sp.Attributes() {
			if attr.Key == "http.route" && attr.Value.Emit() != "/other" {
				t.Fatalf("unknown path recorded as route %q, want /other", attr.Value.Emit())
			}
		}
	}
}

// TestStepSpanPreservesParentCancellation pins the shutdown-safety property
// the traceparent join depends on: extracting the remote span context must
// derive from the caller's context, so a cancelled worker run context still
// cancels the step's execution context even when its metadata carries a valid
// traceparent.
func TestStepSpanPreservesParentCancellation(t *testing.T) {
	_, undo := withRecorder(t)
	defer undo()

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	mt := []byte(`{"traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}`)
	sctx, span := StartStepSpanTracer(
		parent, otelTracer(tracerName), mt, "create_database", 0, 1)
	span.End()
	cancel()

	if err := parent.Err(); err == nil {
		t.Fatal("parent context lost its cancellation")
	}
	select {
	case <-sctx.Done():
	default:
		t.Fatal("step context derived from a cancelled parent is not cancelled")
	}
}

// TestTraceparentRoundTrip covers the step-metadata round trip.
func TestTraceparentRoundTrip(t *testing.T) {
	tp := "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"
	mt, err := WriteStepTraceparent([]byte(`{"role":"db_owner"}`), tp)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	got, ok := ReadStepTraceparent(mt)
	if !ok || got != tp {
		t.Fatalf("round trip: got %q ok=%v", got, ok)
	}
	// Unrelated keys survive the merge.
	if !strings.Contains(string(mt), "db_owner") {
		t.Fatal("merge dropped pre-existing metadata keys")
	}
	if _, ok := ReadStepTraceparent([]byte(`{}`)); ok {
		t.Fatal("empty metadata must not report a traceparent")
	}
}

// testTracer returns a tracer over a (recorded) provider so spans are valid
// without touching the global state.
func testTracer() trace.Tracer {
	return testTracerProvider().Tracer("test")
}

func testTracerProvider() trace.TracerProvider {
	return sdktrace.NewTracerProvider()
}

// TestTraceparentFromContext verifies the serialised W3C traceparent form.
func TestTraceparentFromContext(t *testing.T) {
	tracer := testTracer()
	ctx, span := tracer.Start(context.Background(), "x")
	defer span.End()
	if s := TraceparentFromContext(ctx); !strings.HasPrefix(s, "00-") {
		t.Fatalf("expected W3C traceparent, got %q", s)
	}
	if s := TraceparentFromContext(context.Background()); s != "" {
		t.Fatalf("no span must serialize to empty, got %q", s)
	}
}
