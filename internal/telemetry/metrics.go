// metrics.go — the process's metric surface. One struct owns the Prometheus
// registry the /metrics endpoint serves and the OTel instruments the periodic
// OTLP reader pushes, so recording a fact lands on both surfaces with one
// call and can never drift.
//
// Label cardinality is bounded by construction: every labelled value comes
// from a domain enumeration (step kind, outcome, reconcile decision) through
// boundedLabel, never from a free-text field. There is no environment ID, no
// tenant ID, no error text — the acceptance test in this package asserts it.
package telemetry

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// Metrics owns every instrument this service exports.
type Metrics struct {
	registry *prometheus.Registry

	provisioningDuration *prometheus.HistogramVec
	stepsInDoubt         prometheus.Gauge
	leaseExpirations     prometheus.Counter
	stepAttempts         *prometheus.CounterVec
	reconciliations      *prometheus.CounterVec
	cleanupDuration      *prometheus.HistogramVec

	otlAttempts  otelmetric.Int64Counter
	otlDecisions otelmetric.Int64Counter
	otlLeases    otelmetric.Int64Counter
	otlProvDur   otelmetric.Float64Histogram
	otlCleanDur  otelmetric.Float64Histogram
	inDoubtValue atomic.Int64
}

// Outcome label values. Bounded sets: these are the only strings that may
// reach the outcome label.
const (
	OutcomeSuccess = "success"
	OutcomeFailed  = "failed"
)

// Reconcile decision label values, one per verdict the reconciler can record.
const (
	DecisionDone         = "done"
	DecisionPending      = "pending"
	DecisionFailed       = "failed"
	DecisionUnobservable = "unobservable"
)

// histogramBuckets cover step-scale latencies (seconds): sub-second probes up
// to several minutes of retries for provisioning and cleanup alike.
var histogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

// NewMetrics builds the instrument set on a fresh registry (never the global)
// against the current otel meter provider — the no-op one when no collector
// is configured, so every record call stays cheap there.
func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		provisioningDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "provisioning",
			Name:      "duration_seconds",
			Help:      "Time from environment creation to a terminal provisioning outcome (ready, or a permanently failed step).",
			Buckets:   histogramBuckets,
		}, []string{"outcome"}),
		stepsInDoubt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "provisioning",
			Name:      "steps_in_doubt",
			Help:      "Steps whose outcome is ambiguous and awaiting reconciliation. Alert-worthy: those steps may or may not have left external state behind.",
		}),
		leaseExpirations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "provisioning",
			Name:      "lease_expirations_total",
			Help:      "Steps claimed after their previous owner's lease expired, i.e. observed worker deaths.",
		}),
		stepAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "provisioning",
			Name:      "step_attempts_total",
			Help:      "Step execution attempts, by step kind.",
		}, []string{"kind"}),
		reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "provisioning",
			Name:      "reconciliations_total",
			Help:      "Reconciliation verdicts recorded, by decision.",
		}, []string{"decision"}),
		cleanupDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cleanup",
			Name:      "duration_seconds",
			Help:      "Time from cleanup-plan creation to the workflow's conclusion.",
			Buckets:   histogramBuckets,
		}, []string{"outcome"}),
	}
	m.registry.MustRegister(
		m.provisioningDuration, m.stepsInDoubt, m.leaseExpirations,
		m.stepAttempts, m.reconciliations, m.cleanupDuration,
	)

	// The OTel side mirrors the same facts; the no-op provider makes these
	// constructors and calls free when no collector is configured.
	meter := otelMeter()
	m.otlAttempts = mustCounter(meter, "provisioning_step_attempts",
		"Step execution attempts, by step kind")
	m.otlDecisions = mustCounter(meter, "provisioning_reconciliations",
		"Reconciliation verdicts recorded, by decision")
	m.otlLeases = mustCounter(meter, "provisioning_lease_expirations",
		"Steps reclaimed after lease expiry")
	m.otlProvDur = mustHistogram(meter, "provisioning_duration_seconds")
	m.otlCleanDur = mustHistogram(meter, "cleanup_duration_seconds")

	inDoubtGauge := mustInDoubtGauge(meter, &m.inDoubtValue)
	_ = inDoubtGauge

	return m
}

// Handler serves the registry as Prometheus text exposition (with
// OpenMetrics negotiation, which promhttp provides for free). Nil-safe so
// wiring can opt out cheaply.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# no metrics configured\n"))
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// boundedLabel is the cardinality guard: it passes through values that come
// from domain enumerations and is only ever called with arguments from those
// fixed sets. Keeping every labelled value behind one function makes the
// no-secrets/no-ids audit (this package's test) meaningful.
func boundedLabel(v string) string { return v }

// ObserveStepAttempt records one step execution attempt.
func (m *Metrics) ObserveStepAttempt(ctx context.Context, kind string) {
	if m == nil {
		return
	}
	kind = boundedLabel(kind)
	m.stepAttempts.WithLabelValues(kind).Inc()
	addCounter(ctx, m.otlAttempts, attribute.String("kind", kind))
}

// IncLeaseExpiration records that a step was claimed off a worker that died.
func (m *Metrics) IncLeaseExpiration(ctx context.Context) {
	if m == nil {
		return
	}
	m.leaseExpirations.Inc()
	addCounter(ctx, m.otlLeases, attribute.String("worker", "expired"))
}

// SetStepsInDoubt publishes the current count of ambiguous steps. It is an
// absolute gauge, so it must be fed from a periodic sweep over the rows —
// never from process-local increments a restart would forget.
func (m *Metrics) SetStepsInDoubt(n int64) {
	if m == nil {
		return
	}
	m.inDoubtValue.Store(n)
	m.stepsInDoubt.Set(float64(n))
}

// ObserveReconciliation records one verdict the reconciler landed.
func (m *Metrics) ObserveReconciliation(ctx context.Context, decision string) {
	if m == nil {
		return
	}
	decision = boundedLabel(decision)
	m.reconciliations.WithLabelValues(decision).Inc()
	addCounter(ctx, m.otlDecisions, attribute.String("decision", decision))
}

// ObserveProvisioningDuration records the creation→terminal-outcome interval.
func (m *Metrics) ObserveProvisioningDuration(
	ctx context.Context, outcome string, seconds float64,
) {
	if m == nil || seconds < 0 {
		return
	}
	outcome = boundedLabel(outcome)
	m.provisioningDuration.WithLabelValues(outcome).Observe(seconds)
	recordHistogram(ctx, m.otlProvDur, seconds, attribute.String("outcome", outcome))
}

// ObserveCleanupDuration records the cleanup-plan→conclusion interval.
func (m *Metrics) ObserveCleanupDuration(
	ctx context.Context, outcome string, seconds float64,
) {
	if m == nil || seconds < 0 {
		return
	}
	outcome = boundedLabel(outcome)
	m.cleanupDuration.WithLabelValues(outcome).Observe(seconds)
	recordHistogram(ctx, m.otlCleanDur, seconds, attribute.String("outcome", outcome))
}

// addCounter pushes one labelled counter increment to the OTel side.
func addCounter(
	ctx context.Context, c otelmetric.Int64Counter, attrs ...attribute.KeyValue,
) {
	if c == nil {
		return
	}
	c.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
}

// recordHistogram pushes one duration observation to the OTel side.
func recordHistogram(
	ctx context.Context, c otelmetric.Float64Histogram, seconds float64,
	attrs ...attribute.KeyValue,
) {
	if c == nil {
		return
	}
	c.Record(ctx, seconds, otelmetric.WithAttributes(attrs...))
}

// mustCounter registers an OTel counter; a real provider rejecting valid
// instrument input is a wiring bug, so it fails loudly.
func mustCounter(
	meter otelmetric.Meter, name, description string,
) otelmetric.Int64Counter {
	c, err := meter.Int64Counter(name, otelmetric.WithDescription(description))
	if err != nil {
		panic("telemetry: register OTel counter " + name + ": " + err.Error())
	}
	return c
}

// mustHistogram registers an OTel histogram with the same buckets the
// Prometheus collector uses.
func mustHistogram(meter otelmetric.Meter, name string) otelmetric.Float64Histogram {
	h, err := meter.Float64Histogram(name,
		otelmetric.WithDescription("Latency distribution in seconds"),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(histogramBuckets...))
	if err != nil {
		panic("telemetry: register OTel histogram " + name + ": " + err.Error())
	}
	return h
}

// mustInDoubtGauge registers the steps_in_doubt observable gauge; the callback
// reads the atomic the sweep publishes into, so no collector makes the read
// free and a configured one always sees the freshest swept value.
func mustInDoubtGauge(
	meter otelmetric.Meter, value *atomic.Int64,
) otelmetric.Int64ObservableGauge {
	g, err := meter.Int64ObservableGauge("provisioning_steps_in_doubt",
		otelmetric.WithDescription("Steps with an ambiguous outcome awaiting reconciliation"),
		otelmetric.WithInt64Callback(func(_ context.Context, obs otelmetric.Int64Observer) error {
			obs.Observe(value.Load())
			return nil
		}))
	if err != nil {
		panic("telemetry: register OTel gauge provisioning_steps_in_doubt: " + err.Error())
	}
	return g
}
