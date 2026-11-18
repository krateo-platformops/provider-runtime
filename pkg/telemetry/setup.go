package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

const defaultExportInterval = 30 * time.Second

// The resource attributes provider-runtime sets from Config.
const (
	serviceNameKey       = attribute.Key("service.name")
	serviceVersionKey    = attribute.Key("service.version")
	deploymentNameKey    = attribute.Key("k8s.deployment.name")
	serviceInstanceIDKey = attribute.Key("service.instance.id")
)

// The environment ConfigFromEnv reads: Krateo's OTel convention.
const (
	EnvEnabled        = "OTEL_ENABLED"
	EnvTracingEnabled = "OTEL_TRACING_ENABLED"
	EnvExportInterval = "OTEL_EXPORT_INTERVAL"
	EnvDeploymentName = "DEPLOYMENT_NAME"
	EnvServiceVersion = "SERVICE_VERSION"
)

// The OTLP protocol variables. otlptracehttp ignores them, so Setup checks
// them itself when tracing is on.
const (
	envProtocol       = "OTEL_EXPORTER_OTLP_PROTOCOL"
	envTracesProtocol = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	httpProtobuf      = "http/protobuf"
)

// Config controls OpenTelemetry metrics and trace export.
type Config struct {
	// Enabled turns on OTLP metrics export.
	Enabled bool
	// TracingEnabled turns on OTLP trace export (docs/design-tracing.md §3.1).
	// It is independent of Enabled.
	TracingEnabled bool
	// ServiceName sets the exported service.name, unless OTEL_SERVICE_NAME or
	// OTEL_RESOURCE_ATTRIBUTES already name one. Empty leaves it to those, or
	// to the SDK's default. There is no library-wide default: every provider
	// would then export the same service.name.
	ServiceName string
	// ServiceVersion sets service.version, unless OTEL_RESOURCE_ATTRIBUTES
	// names one. Main sets it from its build stamp, falling back to
	// SERVICE_VERSION.
	ServiceVersion string
	// DeploymentName sets k8s.deployment.name and service.instance.id, each
	// unless OTEL_RESOURCE_ATTRIBUTES names it. The chart passes the
	// Deployment's own name as DEPLOYMENT_NAME.
	DeploymentName string
	// ExportInterval is the metrics export interval; 0 means 30s.
	ExportInterval time.Duration
}

// ConfigFromEnv reads Krateo's OTel convention into a Config:
//
//   - OTEL_ENABLED: metrics, default false.
//   - OTEL_TRACING_ENABLED: traces, default false, whatever OTEL_ENABLED says.
//   - OTEL_EXPORT_INTERVAL: the metrics export interval, as a Go duration.
//   - DEPLOYMENT_NAME: k8s.deployment.name and service.instance.id.
//   - SERVICE_VERSION: service.version.
//
// An unparsable boolean or duration is an error. ServiceName is left for the
// caller to set. The library reads no environment by itself; main calls this.
func ConfigFromEnv() (Config, error) {
	var cfg Config
	var err error
	if cfg.Enabled, err = envBool(EnvEnabled); err != nil {
		return Config{}, err
	}
	if cfg.TracingEnabled, err = envBool(EnvTracingEnabled); err != nil {
		return Config{}, err
	}
	if v := os.Getenv(EnvExportInterval); v != "" {
		if cfg.ExportInterval, err = time.ParseDuration(v); err != nil {
			return Config{}, fmt.Errorf("%s: %w", EnvExportInterval, err)
		}
	}
	cfg.DeploymentName = os.Getenv(EnvDeploymentName)
	cfg.ServiceVersion = os.Getenv(EnvServiceVersion)
	return cfg, nil
}

func envBool(key string) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

// Setup creates and configures the OTLP metrics and trace pipelines.
//
//   - It always sets the global propagator to W3C TraceContext and Baggage,
//     before it checks either gate, so a provider's own otelhttp honours an
//     inbound context even with export off.
//   - Enabled gates metrics. When it's false, Setup returns a nil *Metrics.
//   - TracingEnabled gates traces. When it's true, Setup builds an
//     otlptracehttp exporter from the standard OTEL_* environment, with a
//     batcher, and installs the TracerProvider as the OTel global and as
//     tracing.SetTracerProvider. It passes no sampler and no span limits, so
//     the SDK's OTEL_TRACES_SAMPLER and OTEL_SPAN_* handling apply.
//   - With tracing on, the traces protocol must be http/protobuf, following
//     OTel's precedence: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL when it is set,
//     otherwise OTEL_EXPORTER_OTLP_PROTOCOL; unset is fine.
//   - Metrics and traces share one resource.
//
// The returned function shuts down the TracerProvider, which flushes its
// batcher, then the MeterProvider, and joins their errors. It is never nil.
func Setup(ctx context.Context, log logging.Logger, cfg Config) (*Metrics, func(context.Context) error, error) {
	if log == nil {
		log = logging.NewNopLogger()
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	noop := func(context.Context) error { return nil }
	if !cfg.Enabled && !cfg.TracingEnabled {
		return nil, noop, nil
	}
	if cfg.TracingEnabled {
		if err := checkProtocol(); err != nil {
			return nil, nil, err
		}
	}

	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	var metrics *Metrics
	meterShutdown := noop
	if cfg.Enabled {
		var mp *sdkmetric.MeterProvider
		metrics, mp, err = setupMetrics(ctx, res, cfg.ExportInterval)
		if err != nil {
			return nil, nil, err
		}
		otel.SetMeterProvider(mp)
		meterShutdown = mp.Shutdown
		log.Info("OpenTelemetry metrics initialized")
	}

	tracerShutdown := noop
	if cfg.TracingEnabled {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			_ = meterShutdown(ctx)
			return nil, nil, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		tracing.SetTracerProvider(tp)
		tracerShutdown = tp.Shutdown
		log.Info("OpenTelemetry tracing initialized")
	}

	return metrics, func(ctx context.Context) error {
		return errors.Join(tracerShutdown(ctx), meterShutdown(ctx))
	}, nil
}

// setupMetrics builds the OTLP metrics pipeline over res.
func setupMetrics(ctx context.Context, res *resource.Resource, interval time.Duration) (*Metrics, *sdkmetric.MeterProvider, error) {
	if interval <= 0 {
		interval = defaultExportInterval
	}
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	metrics, err := newMetrics(provider.Meter(tracing.ScopeName))
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, err
	}
	return metrics, provider, nil
}

// checkProtocol refuses a traces protocol other than http/protobuf: the trace
// exporter speaks only OTLP/HTTP, and it ignores these variables, so without
// this check protobuf would go to a gRPC port with nothing said. It follows
// OTel's precedence: the signal-specific OTEL_EXPORTER_OTLP_TRACES_PROTOCOL
// decides when it is set, and OTEL_EXPORTER_OTLP_PROTOCOL only otherwise, so a
// general grpc meant for another signal doesn't refuse traces set to
// http/protobuf.
func checkProtocol() error {
	key := envTracesProtocol
	v := os.Getenv(key)
	if v == "" {
		key = envProtocol
		v = os.Getenv(key)
	}
	if v != "" && v != httpProtobuf {
		return fmt.Errorf("%s=%q: provider-runtime exports traces over OTLP/HTTP only; set it to %s or unset it", key, v, httpProtobuf)
	}
	return nil
}

// newResource builds the resource metrics and traces share. The environment
// (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES) wins over cfg for every key, so
// an operator's setting keeps working.
func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	// A malformed OTEL_RESOURCE_ATTRIBUTES yields a partial resource, which
	// resource.Default tolerates too.
	env, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, err
	}
	var add []attribute.KeyValue
	for _, kv := range []attribute.KeyValue{
		serviceNameKey.String(cfg.ServiceName),
		serviceVersionKey.String(cfg.ServiceVersion),
		deploymentNameKey.String(cfg.DeploymentName),
		serviceInstanceIDKey.String(cfg.DeploymentName),
	} {
		if kv.Value.AsString() == "" {
			continue
		}
		if _, set := env.Set().Value(kv.Key); !set {
			add = append(add, kv)
		}
	}
	if len(add) > 0 {
		if env, err = resource.Merge(env, resource.NewSchemaless(add...)); err != nil {
			return nil, err
		}
	}
	return resource.Merge(resource.Default(), env)
}
