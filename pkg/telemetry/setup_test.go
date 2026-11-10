package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// resetGlobals puts back what Setup changes, so tests don't leak into each
// other.
func resetGlobals(t *testing.T) {
	t.Helper()
	prop := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		tracing.SetTracerProvider(nil)
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(prop)
	})
}

// clearOTelEnv unsets the variables these tests read, for the test's duration.
func clearOTelEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
		envProtocol, envTracesProtocol,
		EnvEnabled, EnvTracingEnabled, EnvExportInterval, EnvDeploymentName, EnvServiceVersion,
	} {
		t.Setenv(k, "")
	}
}

// The resource table of docs/design-tracing.md §5: every key from Config, and
// the environment wins for each one (design §10, test 24).
func TestNewResourceAttributes(t *testing.T) {
	cases := map[string]struct {
		envAttrs string
		want     map[string]string
	}{
		"FromConfig": {
			want: map[string]string{
				"service.name":        "git-provider",
				"service.version":     "1.2.3",
				"k8s.deployment.name": "git-provider-deploy",
				"service.instance.id": "git-provider-deploy",
			},
		},
		"TheEnvironmentWinsForEachKey": {
			envAttrs: "service.version=9.9.9,k8s.deployment.name=from-env,service.instance.id=pod-1",
			want: map[string]string{
				"service.name":        "git-provider",
				"service.version":     "9.9.9",
				"k8s.deployment.name": "from-env",
				"service.instance.id": "pod-1",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clearOTelEnv(t)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tc.envAttrs)
			res, err := newResource(context.Background(), Config{
				ServiceName: "git-provider", ServiceVersion: "1.2.3", DeploymentName: "git-provider-deploy",
			})
			if err != nil {
				t.Fatal(err)
			}
			for k, want := range tc.want {
				v, ok := res.Set().Value(attrKey(k))
				if !ok || v.AsString() != want {
					t.Errorf("%s = %q (set %v), want %q", k, v.AsString(), ok, want)
				}
			}
		})
	}
}

// Empty Config fields add nothing: no empty attribute is exported.
func TestNewResourceSkipsEmptyFields(t *testing.T) {
	clearOTelEnv(t)
	res, err := newResource(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"service.version", "k8s.deployment.name", "service.instance.id"} {
		if _, ok := res.Set().Value(attrKey(k)); ok {
			t.Errorf("%s is set from an empty Config", k)
		}
	}
}

// ConfigFromEnv reads Krateo's convention, OTEL_TRACING_ENABLED independent of
// OTEL_ENABLED and off when unset (decision D1), and refuses unparsable values.
func TestConfigFromEnv(t *testing.T) {
	t.Run("Unset", func(t *testing.T) {
		clearOTelEnv(t)
		cfg, err := ConfigFromEnv()
		if err != nil || cfg != (Config{}) {
			t.Errorf("ConfigFromEnv() = %+v, %v; want the zero Config", cfg, err)
		}
	})
	t.Run("MetricsOnTracingUnsetIsOff", func(t *testing.T) {
		clearOTelEnv(t)
		t.Setenv(EnvEnabled, "true")
		cfg, err := ConfigFromEnv()
		if err != nil || !cfg.Enabled || cfg.TracingEnabled {
			t.Errorf("ConfigFromEnv() = %+v, %v; want metrics on and tracing off", cfg, err)
		}
	})
	t.Run("All", func(t *testing.T) {
		clearOTelEnv(t)
		t.Setenv(EnvEnabled, "true")
		t.Setenv(EnvTracingEnabled, "true")
		t.Setenv(EnvExportInterval, "15s")
		t.Setenv(EnvDeploymentName, "git-provider")
		t.Setenv(EnvServiceVersion, "1.2.3")
		cfg, err := ConfigFromEnv()
		want := Config{Enabled: true, TracingEnabled: true, ExportInterval: 15 * time.Second, DeploymentName: "git-provider", ServiceVersion: "1.2.3"}
		if err != nil || cfg != want {
			t.Errorf("ConfigFromEnv() = %+v, %v; want %+v", cfg, err, want)
		}
	})
	for _, bad := range []struct{ key, value string }{
		{EnvEnabled, "yes please"},
		{EnvTracingEnabled, "maybe"},
		{EnvExportInterval, "soon"},
	} {
		t.Run("Bad"+bad.key, func(t *testing.T) {
			clearOTelEnv(t)
			t.Setenv(bad.key, bad.value)
			if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), bad.key) {
				t.Errorf("ConfigFromEnv() error = %v, want one naming %s", err, bad.key)
			}
		})
	}
}

// With both signals off, Setup returns a working no-op shutdown, sets no
// TracerProvider, and still sets the global propagator (design §3.1).
func TestSetupOff(t *testing.T) {
	clearOTelEnv(t)
	resetGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	metrics, shutdown, err := Setup(context.Background(), logging.NewNopLogger(), Config{})
	if err != nil || metrics != nil || shutdown == nil {
		t.Fatalf("Setup() = %v, %v, %v; want nil metrics, a shutdown, no error", metrics, shutdown == nil, err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown: %v", err)
	}
	if tracing.TracerProvider() != nil {
		t.Error("Setup with tracing off set a TracerProvider")
	}
	if !hasW3CPropagator() {
		t.Error("Setup with tracing off didn't set the W3C propagator")
	}
}

// With tracing on, Setup sets the provider for the reconcilers and the OTel
// global without dialling anything, and the protocol check applies only then
// (design §10, tests 21 and 24).
func TestSetupTracingAndProtocolCheck(t *testing.T) {
	t.Run("TracingOn", func(t *testing.T) {
		clearOTelEnv(t)
		resetGlobals(t)
		// Nothing listens here: otlptracehttp.New must not connect.
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
		metrics, shutdown, err := Setup(context.Background(), nil, Config{TracingEnabled: true})
		if err != nil {
			t.Fatalf("Setup: %v", err)
		}
		if metrics != nil {
			t.Error("metrics should be nil with only tracing on")
		}
		if tracing.TracerProvider() == nil {
			t.Error("Setup with tracing on didn't set tracing.TracerProvider")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})
	t.Run("GRPCWithOnlyMetricsIsFine", func(t *testing.T) {
		clearOTelEnv(t)
		resetGlobals(t)
		t.Setenv(envProtocol, "grpc")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
		_, shutdown, err := Setup(context.Background(), nil, Config{Enabled: true})
		if err != nil {
			t.Fatalf("Setup with only metrics and %s=grpc: %v", envProtocol, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})
	for _, key := range []string{envProtocol, envTracesProtocol} {
		t.Run("GRPCWithTracingFails/"+key, func(t *testing.T) {
			clearOTelEnv(t)
			resetGlobals(t)
			t.Setenv(key, "grpc")
			_, _, err := Setup(context.Background(), nil, Config{TracingEnabled: true})
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Errorf("Setup error = %v, want one naming %s", err, key)
			}
			if tracing.TracerProvider() != nil {
				t.Error("a refused Setup must set no TracerProvider")
			}
		})
	}
	t.Run("HTTPProtobufIsFine", func(t *testing.T) {
		clearOTelEnv(t)
		resetGlobals(t)
		t.Setenv(envProtocol, "http/protobuf")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
		_, shutdown, err := Setup(context.Background(), nil, Config{TracingEnabled: true})
		if err != nil {
			t.Fatalf("Setup with http/protobuf: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})
}

// The drain test: spans ended before shutdown reach the collector, because
// shutdown flushes the batcher. The collector here is an httptest server
// standing in for the OTLP/HTTP endpoint, reached through the base
// OTEL_EXPORTER_OTLP_ENDPOINT, to which the exporter appends /v1/traces.
func TestSetupShutdownFlushesSpans(t *testing.T) {
	clearOTelEnv(t)
	resetGlobals(t)

	var mu sync.Mutex
	var paths []string
	var bytes int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bytes += len(b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", srv.URL)

	_, shutdown, err := Setup(context.Background(), nil, Config{TracingEnabled: true, ServiceName: "drain-test"})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	_, span := tracing.TracerProvider().Tracer(tracing.ScopeName).Start(context.Background(), "reconcile")
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 || bytes == 0 {
		t.Fatal("shutdown exported nothing: the batcher wasn't flushed")
	}
	for _, p := range paths {
		if p != "/v1/traces" {
			t.Errorf("exported to %s, want /v1/traces", p)
		}
	}
}

// Metrics and traces share one resource. The test reads what Setup gives
// each provider from what its exporter sends: an httptest server stands in for
// the OTLP/HTTP collector and decodes both requests, and both carry the same
// resource with every key from Config.
func TestSetupSharesTheResource(t *testing.T) {
	clearOTelEnv(t)
	resetGlobals(t)

	var mu sync.Mutex
	got := map[string]map[string]string{} // path -> resource attributes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		attrs := map[string]string{}
		switch r.URL.Path {
		case "/v1/traces":
			req := &coltracepb.ExportTraceServiceRequest{}
			if err := proto.Unmarshal(body, req); err != nil {
				t.Errorf("decode traces: %v", err)
			}
			for _, rs := range req.GetResourceSpans() {
				for _, kv := range rs.GetResource().GetAttributes() {
					attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
				}
			}
		case "/v1/metrics":
			req := &colmetricspb.ExportMetricsServiceRequest{}
			if err := proto.Unmarshal(body, req); err != nil {
				t.Errorf("decode metrics: %v", err)
			}
			for _, rm := range req.GetResourceMetrics() {
				for _, kv := range rm.GetResource().GetAttributes() {
					attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
				}
			}
		}
		mu.Lock()
		if len(attrs) > 0 {
			got[r.URL.Path] = attrs
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", srv.URL)

	metrics, shutdown, err := Setup(context.Background(), nil, Config{
		Enabled: true, TracingEnabled: true,
		ServiceName: "shared", ServiceVersion: "1.2.3", DeploymentName: "shared-deploy",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	metrics.IncStartupSuccess(context.Background())
	_, span := tracing.TracerProvider().Tracer(tracing.ScopeName).Start(context.Background(), "reconcile")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	traces, metricsRes := got["/v1/traces"], got["/v1/metrics"]
	if traces == nil || metricsRes == nil {
		t.Fatalf("exported resources: traces %v, metrics %v; want both", traces, metricsRes)
	}
	for k, want := range map[string]string{
		"service.name":        "shared",
		"service.version":     "1.2.3",
		"k8s.deployment.name": "shared-deploy",
		"service.instance.id": "shared-deploy",
	} {
		if traces[k] != want || metricsRes[k] != want {
			t.Errorf("%s: traces %q, metrics %q; want %q on both", k, traces[k], metricsRes[k], want)
		}
	}
	if len(traces) != len(metricsRes) {
		t.Errorf("the two resources differ:\ntraces  %v\nmetrics %v", traces, metricsRes)
	}
}

// The protocol check follows OTel's precedence: the traces variable decides
// when it is set, and the general one only otherwise.
func TestSetupProtocolPrecedence(t *testing.T) {
	cases := map[string]struct {
		traces, general string
		ok              bool
	}{
		"TracesHTTPOverridesGeneralGRPC": {traces: "http/protobuf", general: "grpc", ok: true},
		"TracesGRPCFailsWhateverGeneral": {traces: "grpc", general: "http/protobuf", ok: false},
		"GeneralGRPCAloneFails":          {general: "grpc", ok: false},
		"GeneralHTTPAloneIsFine":         {general: "http/protobuf", ok: true},
		"NeitherIsFine":                  {ok: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clearOTelEnv(t)
			resetGlobals(t)
			t.Setenv(envTracesProtocol, tc.traces)
			t.Setenv(envProtocol, tc.general)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
			_, shutdown, err := Setup(context.Background(), nil, Config{TracingEnabled: true})
			if (err == nil) != tc.ok {
				t.Fatalf("Setup error = %v, want ok=%v", err, tc.ok)
			}
			if shutdown != nil {
				sctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = shutdown(sctx)
			}
		})
	}
}

func hasW3CPropagator() bool {
	fields := otel.GetTextMapPropagator().Fields()
	have := map[string]bool{}
	for _, f := range fields {
		have[f] = true
	}
	return have["traceparent"] && have["tracestate"] && have["baggage"]
}

func attrKey(k string) attribute.Key { return attribute.Key(k) }
