package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Shutdown flushes and releases the providers installed by Setup.
type Shutdown func(context.Context) error

// No-collector path: nothing is configured, so the global no-op providers go
// in and every span/metric record is free. See bootstrap.go.
func installNoopGlobals() {
	setProviders(noopTracerProvider(), noopMeterProvider())
}

// Setup installs the global OpenTelemetry providers: an OTLP/gRPC exporter
// per signal against endpoint, degrading every signal to the no-op provider
// when no collector is configured (empty endpoint) so `make run` — and any
// test — works with nothing listening. The returned Shutdown flushes both
// providers on process exit.
func Setup(endpoint string) (Shutdown, error) {
	setGlobalPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	if endpoint == "" {
		installNoopGlobals()
		return func(context.Context) error { return nil }, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), setUpTimeout)
	defer cancel()

	// The exporters are lazy: they dial nothing until the first export, so a
	// collector that disappears after startup only delays exports, never the
	// service. The local collector (deploy/otel-collector.yaml) is plaintext
	// OTLP/gRPC; a TLS collector is selected through the standard
	// OTEL_EXPORTER_OTLP_CERTIFICATE-style environment instead of here.
	traceExp, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(exportTimeout))
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp trace exporter: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithBatchTimeout(batchTimeout)))

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTimeout(exportTimeout))
	if err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), setUpTimeout)
		defer closeCancel()
		_ = tracerProvider.Shutdown(closeCtx)
		return nil, fmt.Errorf("telemetry: otlp metric exporter: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(metricInterval))))

	setProviders(tracerProvider, meterProvider)

	return func(ctx context.Context) error {
		var err error
		if e := tracerProvider.Shutdown(ctx); e != nil {
			err = fmt.Errorf("telemetry: trace provider shutdown: %w", e)
		}
		if e := meterProvider.Shutdown(ctx); e != nil {
			if err != nil {
				err = fmt.Errorf("%w; metric provider shutdown: %w", err, e)
			} else {
				err = fmt.Errorf("telemetry: metric provider shutdown: %w", e)
			}
		}
		return err
	}, nil
}
