package telemetry

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// The instruments of docs/design-capacity-model.md §7, from a governor with a
// gated queue: what each controller holds, is guaranteed and is waiting for,
// its arrivals, the cuts by input, the CPU limit, and the model's capacity.
func TestLocalCapacityPerControllerMetrics(t *testing.T) {
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
			Enabled:       true,
			CapacityModel: true,
			CgroupDir:     filepath.Join("..", "ratelimit", "testdata", "cpu-limited"),
			SamplePeriod:  time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Start(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); g.Snapshot().CPU.Limit == nil; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the sampler never took its first sample")
		}
	}

	// Local capacity is 1: one of the two items runs, the other waits.
	q := ratelimit.NewGatedQueue[string](g, "widgets", nil)
	defer q.ShutDown()
	q.Add("a")
	q.Add("a")
	q.Add("b")
	running, shutdown := q.Get()
	if shutdown {
		t.Fatal("the queue shut down")
	}
	defer q.Done(running)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"provider_runtime.ratelimit.local.in_flight":  1,
		"provider_runtime.ratelimit.local.guaranteed": 1,
		"provider_runtime.ratelimit.local.waiting":    1,
		"provider_runtime.ratelimit.local.arrivals":   2,
	} {
		if got := points(t, rm, name, "controller"); got["widgets"] != want || len(got) != 1 {
			t.Errorf("%s by controller = %v, want widgets: %v and nothing else", name, got, want)
		}
	}
	for name, want := range map[string]float64{
		"provider_runtime.ratelimit.local.capacity":       1,
		"provider_runtime.ratelimit.cpu.limit":            0.5,
		"provider_runtime.ratelimit.local.model_capacity": 4,
	} {
		if got := points(t, rm, name, "none"); got[""] != want || len(got) != 1 {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if got := points(t, rm, "provider_runtime.ratelimit.local.cuts", "input"); len(got) != 2 || got["cpu_pressure"] != 0 || got["memory_working_set"] != 0 {
		t.Errorf("cuts by input = %v, want cpu_pressure 0 and memory_working_set 0", got)
	}
	// Nothing was measured yet: a value would read as a measurement.
	for _, name := range []string{
		"provider_runtime.ratelimit.cpu.used",
		"provider_runtime.ratelimit.cpu.background",
		"provider_runtime.ratelimit.cpu.per_reconcile",
		"provider_runtime.ratelimit.cpu.required",
	} {
		if got := points(t, rm, name, "none"); len(got) != 0 {
			t.Errorf("%s = %v before an interval was measured, want no point", name, got)
		}
	}
}

// The model's measurements are reported when the governor has them, and the
// cuts as it counts them.
func TestCapacityModelMetricsFromASnapshot(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	m, err := newMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	value := func(v float64) *float64 { return &v }
	m.ObserveRateLimit(func() ratelimit.Snapshot {
		return ratelimit.Snapshot{
			SaturationEnabled: true,
			LocalCapacity:     14,
			Controllers: []ratelimit.ControllerSnapshot{
				{Controller: "widgets", InFlight: 9, Guaranteed: 7, Waiting: 120, Arrivals: 5000},
				{Controller: "gadgets", InFlight: 5, Guaranteed: 7, Waiting: 0, Arrivals: 40},
			},
			Cuts: []ratelimit.CutSnapshot{{Input: "cpu_pressure", Cuts: 3}, {Input: "memory_working_set", Cuts: 0}},
			CPU: ratelimit.CPUSnapshot{
				Limit: value(0.5), ModelCapacity: value(14.3), Used: value(0.38), Background: value(0.02),
				PerReconcile: value(0.018), Required: value(0.62),
			},
		}
	})
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"provider_runtime.ratelimit.local.model_capacity": 14.3,
		"provider_runtime.ratelimit.cpu.limit":            0.5,
		"provider_runtime.ratelimit.cpu.used":             0.38,
		"provider_runtime.ratelimit.cpu.background":       0.02,
		"provider_runtime.ratelimit.cpu.per_reconcile":    0.018,
		"provider_runtime.ratelimit.cpu.required":         0.62,
	} {
		if got := points(t, rm, name, "none"); got[""] != want || len(got) != 1 {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if got := points(t, rm, "provider_runtime.ratelimit.local.cuts", "input"); got["cpu_pressure"] != 3 || got["memory_working_set"] != 0 || len(got) != 2 {
		t.Errorf("cuts by input = %v, want cpu_pressure 3 and memory_working_set 0", got)
	}
	for name, want := range map[string][2]float64{
		"provider_runtime.ratelimit.local.in_flight":  {9, 5},
		"provider_runtime.ratelimit.local.guaranteed": {7, 7},
		"provider_runtime.ratelimit.local.waiting":    {120, 0},
		"provider_runtime.ratelimit.local.arrivals":   {5000, 40},
	} {
		if got := points(t, rm, name, "controller"); got["widgets"] != want[0] || got["gadgets"] != want[1] || len(got) != 2 {
			t.Errorf("%s by controller = %v, want widgets %v and gadgets %v", name, got, want[0], want[1])
		}
	}
}

// With saturation off nothing is gated, so none of the per-controller or CPU
// instruments reports anything.
func TestCapacityInstrumentsReportNothingWithSaturationOff(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	m, err := newMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ratelimit.New(ratelimit.Options{MaxConcurrentReconciles: 4, Metrics: m}); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for name, attr := range map[string]string{
		"provider_runtime.ratelimit.local.model_capacity": "none",
		"provider_runtime.ratelimit.local.in_flight":      "controller",
		"provider_runtime.ratelimit.local.guaranteed":     "controller",
		"provider_runtime.ratelimit.local.waiting":        "controller",
		"provider_runtime.ratelimit.local.arrivals":       "controller",
		"provider_runtime.ratelimit.local.cuts":           "input",
		"provider_runtime.ratelimit.cpu.limit":            "none",
		"provider_runtime.ratelimit.cpu.used":             "none",
		"provider_runtime.ratelimit.cpu.background":       "none",
		"provider_runtime.ratelimit.cpu.per_reconcile":    "none",
		"provider_runtime.ratelimit.cpu.required":         "none",
	} {
		if got := points(t, rm, name, attr); len(got) != 0 {
			t.Errorf("%s = %v with saturation off, want no point", name, got)
		}
	}
}
