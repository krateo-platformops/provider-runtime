package telemetry

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	defaultServiceName    = "provider-runtime"
	defaultExportInterval = 30 * time.Second
)

// Config controls OpenTelemetry metrics export.
type Config struct {
	Enabled        bool
	ServiceName    string
	ExportInterval time.Duration
}

// Metrics exposes a small set of low-cardinality counters and histograms for
// provider-runtime consumers.
type Metrics struct {
	startupSuccess metric.Int64Counter
	startupFailure metric.Int64Counter

	getDuration metric.Float64Histogram
	getFailure  metric.Int64Counter

	reconcileDuration         metric.Float64Histogram
	queueDepth                metric.Int64UpDownCounter
	queueWaitDuration         metric.Float64Histogram
	queueOldestItemAge        metric.Float64Histogram
	queueWorkDuration         metric.Float64Histogram
	queueRequeueTotal         metric.Int64Counter
	reconcileSuccess          metric.Int64Counter
	reconcileFailure          metric.Int64Counter
	reconcileRequeueAfter     metric.Int64Counter
	reconcileRequeueImmediate metric.Int64Counter
	reconcileErrorRequeue     metric.Int64Counter
	reconcileInFlight         metric.Int64ObservableGauge
	reconcileInFlightCount    atomic.Int64

	connectDuration metric.Float64Histogram
	connectFailure  metric.Int64Counter

	observeDuration metric.Float64Histogram
	observeFailure  metric.Int64Counter

	createDuration metric.Float64Histogram
	createFailure  metric.Int64Counter

	updateDuration metric.Float64Histogram
	updateFailure  metric.Int64Counter

	deleteDuration metric.Float64Histogram
	deleteFailure  metric.Int64Counter

	finalizerAddDuration    metric.Float64Histogram
	finalizerAddFailure     metric.Int64Counter
	finalizerRemoveDuration metric.Float64Histogram
	finalizerRemoveFailure  metric.Int64Counter

	statusUpdateDuration metric.Float64Histogram
	statusUpdateFailure  metric.Int64Counter

	// Rate-limit governor (pkg/ratelimit).
	rateLimitDeferrals         metric.Int64Counter
	rateLimitSignals           metric.Int64Counter
	rateLimitCapacity          metric.Int64ObservableGauge
	rateLimitSecondsToReset    metric.Float64ObservableGauge
	rateLimitLocalCapacity     metric.Int64ObservableGauge
	rateLimitSaturationInput   metric.Float64ObservableGauge
	rateLimitSaturationEnabled metric.Int64ObservableGauge
	// rateLimitSnapshot is the governor's state, set by ObserveRateLimit.
	rateLimitSnapshot atomic.Pointer[func() ratelimit.Snapshot]
}

var _ ratelimit.MetricsRecorder = (*Metrics)(nil)

// Setup creates and configures an OTLP metrics pipeline.
//
// When metrics are disabled, Setup returns a nil Metrics handle and a no-op
// shutdown function.
func Setup(ctx context.Context, log logging.Logger, cfg Config) (*Metrics, func(context.Context) error, error) {
	if log == nil {
		log = logging.NewNopLogger()
	}

	if !cfg.Enabled {
		return nil, func(context.Context) error { return nil }, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	exportInterval := cfg.ExportInterval
	if exportInterval <= 0 {
		exportInterval = defaultExportInterval
	}

	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless())
	if err != nil {
		return nil, nil, err
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(exportInterval))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	meter := provider.Meter("github.com/krateo-platformops/provider-runtime")
	metrics, err := newMetrics(meter)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, err
	}

	otel.SetMeterProvider(provider)
	log.Info("OpenTelemetry metrics initialized")

	return metrics, provider.Shutdown, nil
}

func newMetrics(meter metric.Meter) (*Metrics, error) {
	var err error
	m := &Metrics{}

	if m.startupSuccess, err = meter.Int64Counter("provider_runtime.startup.success"); err != nil {
		return nil, err
	}
	if m.startupFailure, err = meter.Int64Counter("provider_runtime.startup.failure"); err != nil {
		return nil, err
	}
	if m.getDuration, err = meter.Float64Histogram("provider_runtime.reconcile.get.duration_seconds"); err != nil {
		return nil, err
	}
	if m.getFailure, err = meter.Int64Counter("provider_runtime.reconcile.get.failure"); err != nil {
		return nil, err
	}
	if m.reconcileDuration, err = meter.Float64Histogram("provider_runtime.reconcile.duration_seconds"); err != nil {
		return nil, err
	}
	if m.queueDepth, err = meter.Int64UpDownCounter("provider_runtime.reconcile.queue.depth"); err != nil {
		return nil, err
	}
	if m.queueWaitDuration, err = meter.Float64Histogram("provider_runtime.reconcile.queue.wait.duration_seconds"); err != nil {
		return nil, err
	}
	if m.queueOldestItemAge, err = meter.Float64Histogram("provider_runtime.reconcile.queue.oldest_item_age_seconds"); err != nil {
		return nil, err
	}
	if m.queueWorkDuration, err = meter.Float64Histogram("provider_runtime.reconcile.queue.work.duration_seconds"); err != nil {
		return nil, err
	}
	if m.queueRequeueTotal, err = meter.Int64Counter("provider_runtime.reconcile.queue.requeues"); err != nil {
		return nil, err
	}
	if m.reconcileSuccess, err = meter.Int64Counter("provider_runtime.reconcile.success"); err != nil {
		return nil, err
	}
	if m.reconcileFailure, err = meter.Int64Counter("provider_runtime.reconcile.failure"); err != nil {
		return nil, err
	}
	if m.reconcileRequeueAfter, err = meter.Int64Counter("provider_runtime.reconcile.requeue.after"); err != nil {
		return nil, err
	}
	if m.reconcileRequeueImmediate, err = meter.Int64Counter("provider_runtime.reconcile.requeue.immediate"); err != nil {
		return nil, err
	}
	if m.reconcileErrorRequeue, err = meter.Int64Counter("provider_runtime.reconcile.requeue.error"); err != nil {
		return nil, err
	}
	if m.reconcileInFlight, err = meter.Int64ObservableGauge("provider_runtime.reconcile.in_flight"); err != nil {
		return nil, err
	}
	if m.connectDuration, err = meter.Float64Histogram("provider_runtime.external.connect.duration_seconds"); err != nil {
		return nil, err
	}
	if m.connectFailure, err = meter.Int64Counter("provider_runtime.external.connect.failure"); err != nil {
		return nil, err
	}
	if m.observeDuration, err = meter.Float64Histogram("provider_runtime.external.observe.duration_seconds"); err != nil {
		return nil, err
	}
	if m.observeFailure, err = meter.Int64Counter("provider_runtime.external.observe.failure"); err != nil {
		return nil, err
	}
	if m.createDuration, err = meter.Float64Histogram("provider_runtime.external.create.duration_seconds"); err != nil {
		return nil, err
	}
	if m.createFailure, err = meter.Int64Counter("provider_runtime.external.create.failure"); err != nil {
		return nil, err
	}
	if m.updateDuration, err = meter.Float64Histogram("provider_runtime.external.update.duration_seconds"); err != nil {
		return nil, err
	}
	if m.updateFailure, err = meter.Int64Counter("provider_runtime.external.update.failure"); err != nil {
		return nil, err
	}
	if m.deleteDuration, err = meter.Float64Histogram("provider_runtime.external.delete.duration_seconds"); err != nil {
		return nil, err
	}
	if m.deleteFailure, err = meter.Int64Counter("provider_runtime.external.delete.failure"); err != nil {
		return nil, err
	}
	if m.finalizerAddDuration, err = meter.Float64Histogram("provider_runtime.finalizer.add.duration_seconds"); err != nil {
		return nil, err
	}
	if m.finalizerAddFailure, err = meter.Int64Counter("provider_runtime.finalizer.add.failure"); err != nil {
		return nil, err
	}
	if m.finalizerRemoveDuration, err = meter.Float64Histogram("provider_runtime.finalizer.remove.duration_seconds"); err != nil {
		return nil, err
	}
	if m.finalizerRemoveFailure, err = meter.Int64Counter("provider_runtime.finalizer.remove.failure"); err != nil {
		return nil, err
	}
	if m.statusUpdateDuration, err = meter.Float64Histogram("provider_runtime.status.update.duration_seconds"); err != nil {
		return nil, err
	}
	if m.statusUpdateFailure, err = meter.Int64Counter("provider_runtime.status.update.failure"); err != nil {
		return nil, err
	}

	if m.rateLimitDeferrals, err = meter.Int64Counter("provider_runtime.ratelimit.deferrals",
		metric.WithDescription("External calls the rate-limit governor deferred, by key and cause (throttled, capacity, saturated).")); err != nil {
		return nil, err
	}
	if m.rateLimitSignals, err = meter.Int64Counter("provider_runtime.ratelimit.signals",
		metric.WithDescription("Rate-limit signals providers reported, by key and kind (throttled, advisory).")); err != nil {
		return nil, err
	}
	if m.rateLimitCapacity, err = meter.Int64ObservableGauge("provider_runtime.ratelimit.capacity",
		metric.WithDescription("Current concurrency the governor allows per key: the number operators used to tune by hand.")); err != nil {
		return nil, err
	}
	if m.rateLimitSecondsToReset, err = meter.Float64ObservableGauge("provider_runtime.ratelimit.seconds_to_reset",
		metric.WithDescription("Seconds until a throttled key's hold lifts, 0 when it is not held."), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.rateLimitLocalCapacity, err = meter.Int64ObservableGauge("provider_runtime.ratelimit.local.capacity",
		metric.WithDescription("Current process-wide capacity the controller's own saturation allows.")); err != nil {
		return nil, err
	}
	if m.rateLimitSaturationInput, err = meter.Float64ObservableGauge("provider_runtime.ratelimit.saturation.input",
		metric.WithDescription("Each enabled saturation input's last value: PSI avg10 for cpu_pressure and memory_pressure, the share of memory.max for memory_working_set. A disabled input has no value."),
		metric.WithUnit("%")); err != nil {
		return nil, err
	}
	if m.rateLimitSaturationEnabled, err = meter.Int64ObservableGauge("provider_runtime.ratelimit.saturation.input.enabled",
		metric.WithDescription("1 when a saturation input is read, 0 when it is disabled (no cgroup v2, no PSI, no memory limit, or saturation off).")); err != nil {
		return nil, err
	}

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(m.reconcileInFlight, m.reconcileInFlightCount.Load())
		m.observeRateLimit(observer)
		return nil
	}, m.reconcileInFlight, m.rateLimitCapacity, m.rateLimitSecondsToReset, m.rateLimitLocalCapacity,
		m.rateLimitSaturationInput, m.rateLimitSaturationEnabled)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) IncStartupSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupSuccess.Add(ctx, 1)
}

func (m *Metrics) IncStartupFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupFailure.Add(ctx, 1)
}

func (m *Metrics) RecordGetDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.getDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncGetFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.getFailure.Add(ctx, 1)
}

func (m *Metrics) RecordReconcileDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.reconcileDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) RecordQueueWaitDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.queueWaitDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) AddQueueDepth(ctx context.Context, delta int64) {
	if m == nil || delta == 0 {
		return
	}
	m.queueDepth.Add(ctx, delta)
}

func (m *Metrics) RecordQueueOldestItemAge(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.queueOldestItemAge.Record(ctx, d.Seconds())
}

func (m *Metrics) RecordQueueWorkDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.queueWorkDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) recordRequeue(ctx context.Context, reason string, counter metric.Int64Counter) {
	if m == nil {
		return
	}
	counter.Add(ctx, 1)
	m.queueRequeueTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *Metrics) IncReconcileSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconcileSuccess.Add(ctx, 1)
}

func (m *Metrics) IncReconcileFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconcileFailure.Add(ctx, 1)
}

func (m *Metrics) IncReconcileRequeueAfter(ctx context.Context) {
	if m == nil {
		return
	}
	m.recordRequeue(ctx, "after", m.reconcileRequeueAfter)
}

func (m *Metrics) IncReconcileRequeueImmediate(ctx context.Context) {
	if m == nil {
		return
	}
	m.recordRequeue(ctx, "immediate", m.reconcileRequeueImmediate)
}

func (m *Metrics) IncReconcileErrorRequeue(ctx context.Context) {
	if m == nil {
		return
	}
	m.recordRequeue(ctx, "error", m.reconcileErrorRequeue)
}

func (m *Metrics) AddReconcileInFlight(delta int64) {
	if m == nil || delta == 0 {
		return
	}
	m.reconcileInFlightCount.Add(delta)
}

func (m *Metrics) RecordConnectDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.connectDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncConnectFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.connectFailure.Add(ctx, 1)
}

func (m *Metrics) RecordObserveDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.observeDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncObserveFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.observeFailure.Add(ctx, 1)
}

func (m *Metrics) RecordCreateDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.createDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncCreateFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.createFailure.Add(ctx, 1)
}

func (m *Metrics) RecordUpdateDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.updateDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncUpdateFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.updateFailure.Add(ctx, 1)
}

func (m *Metrics) RecordDeleteDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.deleteDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncDeleteFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.deleteFailure.Add(ctx, 1)
}

func (m *Metrics) RecordFinalizerAddDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.finalizerAddDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncFinalizerAddFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.finalizerAddFailure.Add(ctx, 1)
}

func (m *Metrics) RecordFinalizerRemoveDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.finalizerRemoveDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncFinalizerRemoveFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.finalizerRemoveFailure.Add(ctx, 1)
}

func (m *Metrics) RecordStatusUpdateDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.statusUpdateDuration.Record(ctx, d.Seconds())
}

func (m *Metrics) IncStatusUpdateFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.statusUpdateFailure.Add(ctx, 1)
}

// IncRateLimitDeferral counts one deferred external call for key.
func (m *Metrics) IncRateLimitDeferral(ctx context.Context, key string, reason ratelimit.DeferReason) {
	if m == nil {
		return
	}
	m.rateLimitDeferrals.Add(ctx, 1, metric.WithAttributes(
		attribute.String("key", key), attribute.String("cause", reason.Cause())))
}

// IncRateLimitSignal counts one throttled or advisory signal for key.
func (m *Metrics) IncRateLimitSignal(ctx context.Context, key string, kind ratelimit.SignalKind) {
	if m == nil {
		return
	}
	m.rateLimitSignals.Add(ctx, 1, metric.WithAttributes(
		attribute.String("key", key), attribute.String("kind", string(kind))))
}

// ObserveRateLimit makes snapshot the source of the rate-limit gauges.
// ratelimit.New calls it when its Options.Metrics is set.
func (m *Metrics) ObserveRateLimit(snapshot func() ratelimit.Snapshot) {
	if m == nil || snapshot == nil {
		return
	}
	m.rateLimitSnapshot.Store(&snapshot)
}

func (m *Metrics) observeRateLimit(observer metric.Observer) {
	fn := m.rateLimitSnapshot.Load()
	if fn == nil {
		return
	}
	snap := (*fn)()
	for _, k := range snap.Keys {
		attrs := metric.WithAttributes(attribute.String("key", k.Key))
		observer.ObserveInt64(m.rateLimitCapacity, int64(k.Capacity), attrs)
		observer.ObserveFloat64(m.rateLimitSecondsToReset, k.SecondsToReset, attrs)
	}
	observer.ObserveInt64(m.rateLimitLocalCapacity, int64(snap.LocalCapacity))
	for _, in := range snap.Inputs {
		attrs := metric.WithAttributes(attribute.String("input", in.Input))
		enabled := int64(0)
		if in.Enabled {
			enabled = 1
			// A disabled input reports no value: zero would read as healthy.
			observer.ObserveFloat64(m.rateLimitSaturationInput, in.Percent, attrs)
		}
		observer.ObserveInt64(m.rateLimitSaturationEnabled, enabled, attrs)
	}
}
