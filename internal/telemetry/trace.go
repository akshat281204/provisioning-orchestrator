// trace.go — the tracing helpers: the HTTP request middleware, the step
// metadata round-trip that carries a trace context across worker processes,
// and the bounds-kept span construction every surface funnels through.
// Span attributes are created only here, so the redaction rules in redact.go
// apply to everything that reaches a collector.
package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the tracer scope every span in this service is built against.
const tracerName = "provisioning-orchestrator"

// stepSpanName builds the span name for one step attempt: `step <kind>`.
func stepSpanName(kind string) string {
	return "step " + RedactText(kind)
}

// provisioningSpanName is the create-handler span that parents the whole
// provisioning workflow.
const provisioningSpanName = "provisioning environment"

// NewProvisioningSpan opens the span that carries one environment's whole
// provisioning workflow, parented to the HTTP request span (which the
// middleware left in ctx; absent a valid parent — an untraced client or a
// no-op provider — the span is simply invalid, and the workflow starts no
// trace). It returns the context carrying the span — thread it to the store
// write — and the span itself.
func NewProvisioningSpan(ctx context.Context) (context.Context, trace.Span) {
	return otel.GetTracerProvider().Tracer(tracerName).Start(
		ctx, provisioningSpanName, trace.WithSpanKind(trace.SpanKindInternal))
}

// EndProvisioningSpan closes the provisioning span with success/error.
func EndProvisioningSpan(span trace.Span, ok bool) {
	if span == nil {
		return
	}
	if !ok {
		span.SetStatus(codes.Error, "create failed")
	}
	span.End()
}

// routePrefix maps a request path onto a fixed route allowlist — every route
// this service actually serves. Paths outside the allowlist never reach
// http.route as their own label: a one-segment path under /v1 carries an
// environment id at its second segment by construction, and any unrelated
// shape is client-supplied text, so both collapse. UUID-shaped text is
// stripped even inside the allowed prefixes, and every kept segment is
// scrubbed and bounded, so http.route can never carry an environment id or
// unbounded client text.
func routePrefix(path string) string {
	if strings.Trim(path, "/") == "" {
		return "/"
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(segments) >= 2 && segments[0] == "v1" && segments[1] == "environments":
		// /v1/environments/{id}[/{sub-resource}] — the third segment on is
		// an identifier by construction in every route of this shape; the
		// route itself ends at the prefix.
		segments = segments[:2]
	case segments[0] == "healthz" || segments[0] == "readyz" || segments[0] == "metrics":
		// Fixed-name unary routes.
		segments = segments[:1]
	case segments[0] == "v1":
		// An unknown route under the versioned prefix: record the version
		// only, never the client-supplied segment under it.
		segments = segments[:1]
	default:
		// Anything else is not a route this service serves, so not even
		// its first segment is trusted; record an opaque bounded bucket.
		segments = []string{"other"}
	}
	out := make([]string, 0, len(segments))
	for _, s := range segments {
		out = append(out, redactSpanText(s))
	}
	return "/" + strings.Join(out, "/")
}

// TraceRequests wraps a handler tracing middleware semantics: one span per
// HTTP request, parented to whatever trace context the caller supplied in the
// headers, so the client can join its view of the workflow to ours.
func TraceRequests(next http.Handler) http.Handler {
	return traceRequests(otel.GetTracerProvider().Tracer(tracerName), next)
}

func traceRequests(tracer trace.Tracer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		// The path interpolates client-supplied identifiers (a /v1/environments
		// path carries an environment id by design), so it is scoped to the
		// route prefix — sane pattern, no IDs. Method is bounded by HTTP.
		attrs := []attribute.KeyValue{
			attribute.String("http.method", boundedLabel(r.Method)),
			attribute.String("http.route", routePrefix(r.URL.Path)),
		}
		ctx, span := tracer.Start(ctx, "http "+boundedLabel(r.Method),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...))
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// traceparentMetadataKey is the step-metadata key carrying the W3C trace
// context. It is the only key in this package's namespace of step metadata.
const traceparentMetadataKey = "traceparent"

// TraceparentFromContext returns the W3C traceparent string for the span in
// ctx, or "" when there is no valid span. It is written once, at environment
// creation, so every step — executed by any worker, any process — joins the
// same trace.
func TraceparentFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return traceparentString(sc)
}

// traceparentString serialises a span context in W3C traceparent form. The
// propagator package can only parse, so the serialisation is spelled here;
// the format is fixed by the W3C trace context spec.
func traceparentString(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%02x",
		sc.TraceID(), sc.SpanID(), uint32(sc.TraceFlags()&trace.FlagsSampled))
}

// ReadStepTraceparent extracts the trace context wire string from a step's
// metadata document; ok is false when the metadata carries none. The document
// is decoded as map[string]any (as WriteStepTraceparent draws it): step
// metadata carries non-string values too (e.g. the rotation entry written by
// CompleteRotation), and a string-only decode would fail to parse and
// silently detach that step's span from its trace.
func ReadStepTraceparent(metadata []byte) (traceparent string, ok bool) {
	if len(metadata) == 0 {
		return "", false
	}
	var doc map[string]any
	if json.Unmarshal(metadata, &doc) != nil {
		return "", false
	}
	tp, _ := doc[traceparentMetadataKey].(string)
	if tp == "" {
		return "", false
	}
	return tp, true
}

// WriteStepTraceparent merges the trace context into a step metadata document
// without disturbing any other keys the store wrote.
func WriteStepTraceparent(metadata []byte, traceparent string) ([]byte, error) {
	if metadata == nil {
		metadata = []byte(`{}`)
	}
	var doc map[string]any
	if err := json.Unmarshal(metadata, &doc); err != nil {
		return nil, fmt.Errorf("telemetry: merge traceparent into step metadata: %w", err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	doc[traceparentMetadataKey] = traceparent
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("telemetry: merge traceparent into step metadata: %w", err)
	}
	return out, nil
}

// stepTraceContext re-joins a claimed step's attempt to its trace: valid
// traceparent wins (that is the resumed-by-a-different-worker case), otherwise
// the caller's context flows through untouched.
func stepTraceContext(ctx context.Context, metadata []byte) context.Context {
	tp, ok := ReadStepTraceparent(metadata)
	if !ok {
		return ctx
	}
	carrier := propagation.MapCarrier(map[string]string{traceparentMetadataKey: tp})
	// Extract derives from the caller's ctx, so a remote span context is
	// layered on top of it — cancellation, deadlines and the worker's run
	// values all survive the join. Extracting into context.Background()
	// would detach the step from the run and break graceful shutdown.
	joined := otel.GetTextMapPropagator().Extract(ctx, carrier)
	if trace.SpanContextFromContext(joined).IsValid() {
		return joined
	}
	return ctx
}

// StartStepSpan opens the span for one step execution attempt: one child per
// attempt under the environment's provisioning span, carrying the bounded
// attribute set (kind, index, attempt) — never a secret, never an error text.
// An invalid trace (no collector, no traceparent) costs one no-op span.
func StartStepSpan(
	ctx context.Context, metadata []byte, kind string, index, attempt int,
) (context.Context, trace.Span) {
	return StartStepSpanTracer(ctx, otelTracer(tracerName), metadata, kind, index, attempt)
}

// StartStepSpanTracer is StartStepSpan for tests with a specific provider.
func StartStepSpanTracer(
	ctx context.Context, tracer trace.Tracer, metadata []byte, kind string, index, attempt int,
) (context.Context, trace.Span) {
	ctx = stepTraceContext(ctx, metadata)
	return tracer.Start(ctx, stepSpanName(kind), trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("step.kind", RedactText(kind)),
			attribute.Int("step.index", index),
			attribute.Int("attempt", attempt),
		))
}

// EndStepSpan records the attempt's verdict and closes the span. The outcome
// is one of the bounded step-state strings persisted on the row when the
// attempt's record lands.
func EndStepSpan(span trace.Span, outcome string) {
	if span == nil {
		return
	}
	span.SetAttributes(attribute.String("outcome", RedactText(outcome)))
	span.End()
}

// spanScopedID matches a UUID-shaped identifier — environment ids, step ids,
// credential references all take this shape in this service — so span text
// can strip them even when surrounding text is not secret-shaped. Logs keep
// identifiers (they are the debugging currency there); spans must not carry
// them.
var spanScopedID = regexp.MustCompile(
	`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// redactSpanText is the span-side scrubber: secret-shaped content first
// (shared policy), then scoped identifiers, then the length cap.
func redactSpanText(s string) string {
	return RedactText(spanScopedID.ReplaceAllString(s, "[SCOPED_ID]"))
}

// FailSpan marks a span with the given error at error severity. The message
// is scrubbed (RedactText) and truncated before it is recorded, and recorded
// as a bounded attribute rather than an exception event, so no exception.*
// payload can bypass the scrubbing.
func FailSpan(span trace.Span, msg string) {
	if span == nil {
		return
	}
	span.SetAttributes(attribute.String("error.message", redactSpanText(msg)))
	span.SetStatus(codes.Error, redactSpanText(msg))
}
