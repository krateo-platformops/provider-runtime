package telemetry

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// points returns the data points of the named metric, keyed by the value of
// attribute attr, as float64.
func points(t *testing.T, rm metricdata.ResourceMetrics, name, attr string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			add := func(set attribute.Set, v float64) {
				val, _ := set.Value(attribute.Key(attr))
				out[val.AsString()] += v
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					add(p.Attributes, float64(p.Value))
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					add(p.Attributes, float64(p.Value))
				}
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					add(p.Attributes, p.Value)
				}
			}
		}
	}
	return out
}

func TestRateLimitMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := newMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}

	g, err := ratelimit.New(ratelimit.Options{
		MaxConcurrentReconciles: 4,
		Metrics:                 m,
		Saturation: ratelimit.SaturationOptions{
			Enabled:                      true,
			ExperimentalMemoryWorkingSet: true,
			CgroupDir:                    filepath.Join("..", "ratelimit", "testdata", "no-memory-limit"),
			SamplePeriod:                 time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Start(ctx) }()
	// A deferral is Ramping only once the sampler runs: wait for its first sample.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		sampled := false
		for _, in := range g.Snapshot().Inputs {
			sampled = sampled || in.Enabled
		}
		if sampled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sampler never took its first sample")
		}
	}

	g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: time.Minute})
	g.Observe(ratelimit.Signal{Key: "dev.azure.com", HasQuota: true, Limit: 200, Remaining: 0})
	if _, d := g.Acquire(ctx, "api.github.com"); d == nil {
		t.Fatal("expected a throttled deferral")
	}
	var rm metricdata.ResourceMetrics
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		rm = metricdata.ResourceMetrics{}
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatal(err)
		}
		if points(t, rm, "provider_runtime.ratelimit.saturation.input.enabled", "input")["cpu_pressure"] == 1 || time.Now().After(deadline) {
			break
		}
	}

	if got := points(t, rm, "provider_runtime.ratelimit.deferrals", "cause"); got["throttled"] != 1 || len(got) != 1 {
		t.Errorf("deferrals by cause = %v, want throttled: 1 and nothing else: local capacity defers nothing", got)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.signals", "kind"); got["throttled"] != 1 || got["advisory"] != 1 {
		t.Errorf("signals by kind = %v, want one throttled and one advisory", got)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.capacity", "key"); got["api.github.com"] != 1 || got["dev.azure.com"] != 2 {
		t.Errorf("capacity by key = %v, want api.github.com 1 and dev.azure.com 2", got)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.seconds_to_reset", "key"); got["api.github.com"] < 59 || got["dev.azure.com"] != 0 {
		t.Errorf("seconds_to_reset by key = %v, want ~60 for the throttled key and 0 for the other", got)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.local.capacity", "none"); got[""] != 1 {
		t.Errorf("local capacity = %v, want 1: it starts at 1 and one sample only starts the ramp's clock", got)
	}

	// No memory limit: the working-set input is disabled, and says so, rather
	// than reporting a value of 0 that reads as healthy.
	enabled := points(t, rm, "provider_runtime.ratelimit.saturation.input.enabled", "input")
	if enabled["cpu_pressure"] != 1 || enabled["memory_working_set"] != 0 || len(enabled) != 2 {
		t.Errorf("input enabled = %v, want cpu_pressure 1 and memory_working_set 0, and no other input", enabled)
	}
	values := points(t, rm, "provider_runtime.ratelimit.saturation.input", "input")
	if _, ok := values["memory_working_set"]; ok {
		t.Errorf("the disabled memory_working_set input reported a value: %v", values)
	}
	if _, ok := values["cpu_pressure"]; !ok {
		t.Errorf("the enabled cpu_pressure input reported no value: %v", values)
	}
}

// With saturation off nothing is gated on the local capacity, so its gauge
// reports nothing, as core-provider's does: a value would read as a limit.
// The per-key gauges still report.
func TestLocalCapacityReportsNothingWithSaturationOff(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	m, err := newMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	g, err := ratelimit.New(ratelimit.Options{MaxConcurrentReconciles: 4, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: time.Minute})
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.local.capacity", "none"); len(got) != 0 {
		t.Errorf("local capacity = %v with saturation off, want no point", got)
	}
	if got := points(t, rm, "provider_runtime.ratelimit.capacity", "key"); got["api.github.com"] != 1 {
		t.Errorf("capacity by key = %v, want api.github.com 1: the key gauges report with saturation off", got)
	}
}

func TestRateLimitMetricsAreNilSafe(t *testing.T) {
	var m *Metrics
	m.IncRateLimitDeferral(context.Background(), "k", ratelimit.DeferThrottled)
	m.IncRateLimitSignal(context.Background(), "k", ratelimit.SignalAdvisory)
	m.ObserveRateLimit(func() ratelimit.Snapshot { return ratelimit.Snapshot{} })
}
