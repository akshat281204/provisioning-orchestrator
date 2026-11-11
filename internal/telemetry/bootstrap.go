package telemetry

import (
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

// Timing constants for the OTLP exporters.
const (
	exportTimeout  = 10 * time.Second // one export attempt
	batchTimeout   = 2 * time.Second  // span batching
	metricInterval = 10 * time.Second // periodic metric reader
	setUpTimeout   = 5 * time.Second  // provider construction/shutdown
)

// ShutdownTimeout bounds the final export flush at process exit.
const ShutdownTimeout = 10 * time.Second

// noopTracerProvider is the no-config tracer provider: spans are recorded
// into nothing.
func noopTracerProvider() trace.TracerProvider { return nooptrace.NewTracerProvider() }

// noopMeterProvider is the no-config meter provider: records are free.
func noopMeterProvider() metric.MeterProvider { return noop.NewMeterProvider() }

// setProviders installs the global tracer and meter providers. One seam so
// Setup and InitNoop share it.
func setProviders(tp trace.TracerProvider, mp metric.MeterProvider) {
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
}

// setGlobalPropagator installs the global context propagator.
func setGlobalPropagator(p propagation.TextMapPropagator) {
	otel.SetTextMapPropagator(p)
}

// otelMeter is the process metric handle instrument recording goes through.
func otelMeter() metric.Meter {
	return otel.GetMeterProvider().Meter("provisioning-orchestrator")
}

// otelTracer is the process span handle span construction goes through.
func otelTracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}

// Compile-time guards that both no-op providers satisfy the otel interfaces.
var (
	_ trace.TracerProvider = nooptrace.NewTracerProvider()
	_ metric.MeterProvider = noop.NewMeterProvider()
)
