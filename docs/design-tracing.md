---
type: Architecture
title: provider-runtime — trace every reconcile
description: Design, not built, for OpenTelemetry tracing in provider-runtime. telemetry.Setup gains a TracerProvider, the reconciler opens a reconcile root span with connect, observe, create, update and delete children, and the krateo.io/traceparent contract gets a parent-or-link rule for long-lived objects. Trace context is propagated on outbound HTTP calls and onto Kubernetes objects that other Krateo controllers reconcile.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, tracing, opentelemetry, reconciler, propagation]
timestamp: 2026-11-09T00:00:00Z
---

# Trace every reconcile

**Status:** design, for review. Nothing is built. It builds on
[#58](https://github.com/krateo-platformops/provider-runtime/pull/58)
(`logging.ContextLogger`, `logging.WithContext`), which is open at `cda449d`
and has to merge first. The user decided its first five open questions on
2026-11-09, and §13 records them (D1 to D5). The review on #60 raised a sixth,
Q6, which is open: see §11.3, step 5.

**The goal** (the user's decision). Every provider built on provider-runtime
(git-provider, incident-provider, alert-provider and the others) gets
reconcile traces by bumping provider-runtime, calling `telemetry.Setup` in
main, and setting env. No provider writes span code.

**References.** Code is provider-runtime at `c9cc298` (main). `UR` is
`krateo-platformops/unstructured-runtime` at tag `v1.12.0` (`5b4dbff`), read with
`gh api`. `CR/` is `sigs.k8s.io/controller-runtime@v0.25.2`, and `SDK/` is
`go.opentelemetry.io/otel/sdk@v1.45.0`. `API/` is `go.opentelemetry.io/otel@v1.45.0`
and `go.opentelemetry.io/otel/trace@v1.45.0`, and `HTTP/` is
`go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.45.0`. These
are the versions in `go.mod`, read in the module cache. The consumers were read
on their `main` branches: core-provider `bfc5903`, alert-provider `b7deac4`,
incident-provider `395c94d`, git-provider `8bc9e3c` and observability `56e7ea4`.
§14 lists what was checked and what wasn't.

## 1. The problem

provider-runtime has no `TracerProvider` and starts no span.
`pkg/telemetry` exports metrics only (`metrics.go:120`). `pkg/logging/otel.go:79`
stamps `trace_id` and `span_id` when the record's context holds a span, and
#58 binds the reconciler's Logger to its `ctx`. But nothing ever puts a span
there: `CR/pkg/internal/controller/controller.go:464-481` (`reconcileHandler`)
passes `Reconcile` only a context carrying a logger and a reconcile ID, and
controller-runtime imports no OpenTelemetry package outside its tests.

So the consumers built tracing themselves, and each one did it differently:

- **core-provider** copied UR's setup into
  `go/core-provider/internal/tools/telemetry/tracing.go`, because
  "provider-runtime has no tracing" (its package comment). It sets the resource
  by writing `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` from main
  (`main.go:96-115`).
- **alert-provider** wraps the provider-runtime reconciler in its own
  `alert.reconcile` span (`internal/controllers/alert/tracing.go:30-43`) and
  opens `alert.observe`, `alert.create` and the others inside its
  ExternalClient.
- **git-provider** and **incident-provider** have no tracing, and they don't
  call `telemetry.Setup` either.

observability#127's audit-to-reconcile join finds reconciles by
`SpanName = 'reconcile' AND SpanKind = 'Server'`
(`ops/audit-reconcile-join/query.sql:62-63`). Today it sees no provider
reconcile at all.

## 2. The contract, and where provider-runtime differs

The source of truth is UR v1.12.0. What provider-runtime matches:

| | UR v1.12.0 | provider-runtime |
|---|---|---|
| Root span | `StartReconcileSpan` (`pkg/telemetry/tracing.go:67-85`): `reconcile`, `SpanKindServer` | the same |
| Started | `pkg/controller/worker.go:461`, after the fetch, ended by a deferred `RecordSpanError` and `End` (`:462-465`) | after the Get, ended by defer (§4.1) |
| Attributes | `k8s.object.kind`, `k8s.object.name`, `k8s.object.namespace` (`tracing.go:79-83`) | exactly these three on the root |
| Errors | `RecordError(err)` then `SetStatus(codes.Error, err.Error())` (`tracing.go:88-93`) | the same (§7) |
| Annotations | `krateo.io/traceparent`, `krateo.io/tracestate` (`pkg/meta/meta.go:19,22`) | the same keys, in `pkg/meta` |
| Propagator | `TraceContext` and `Baggage`, set globally before the "disabled" return (`tracing.go:41`) | the same global propagator (§3.1) |
| Exporter | `otlptracehttp.New(ctx)`, `WithBatcher` (`tracing.go:47,56`) | the same |
| Resource | `buildResource` (`pkg/telemetry/metrics.go:148-172`): `service.name`, `service.version`, and `k8s.deployment.name` and `service.instance.id`, both set to `DeploymentName` | the same keys (§5) |

Where provider-runtime differs on purpose:

1. **Resource precedence.** UR merges its Config attributes over
   `resource.Default()`, so the Config wins over `OTEL_SERVICE_NAME`
   (`metrics.go:171`, and `SDK/resource/resource.go:174-175`: "the value from b
   will overwrite the value from a"). provider-runtime has let the environment
   win since v1.9.1 (`pkg/telemetry/metrics.go:97-116`, `docs/log.md:162`). It
   keeps that rule and applies it to every key it adds.
2. **A long-lived object doesn't grow one trace forever.** UR extracts the
   annotation on every reconcile and makes it the parent each time
   (`tracing.go:68-76`). provider-runtime makes it the parent only on the pass
   that first sees it, and links to it after that (§6.2).
3. **A deferral is not an error.** UR's `deferPass` returns a `DeferredError`
   (`worker.go:843,863`), which `RecordSpanError` marks as Error. provider-runtime's
   deferral returns a nil error (`pkg/reconciler/ratelimit.go:180-199`), and its
   span stays Unset, with an event (§7).
4. **With tracing off, logs aren't stamped with someone else's trace.** UR's
   no-op tracer puts the remote span context in `ctx` as the active span
   (`API/internal/global/trace.go:185`), so every log of a disabled controller
   carries the upstream `trace_id` and `span_id`. provider-runtime carries the
   inbound context for propagation only (§6.4).
5. **provider-runtime's own carriers never send baggage.** The annotations and
   the HTTP transport carry `traceparent` and `tracestate` only. Platform
   baggage carries a portal session (observability's
   `ops/audit-reconcile-join/README.md`, the `session_id` column), and it
   shouldn't reach a third-party API.
6. **The root never takes an in-process parent.** If something wrapped the
   reconciler in its own span, the `reconcile` span would still be a root, or a
   child of the annotation's remote parent. That keeps the parent-or-link rule
   exact (§6.2) and the audit join's assumption true (§11.3).

## 3. The API

### 3.1 `telemetry.Setup` gets a TracerProvider

`telemetry.Setup` doesn't get a separate `SetupTracing`; it gains a
TracerProvider. The signature doesn't change:

```go
type Config struct {
	Enabled        bool          // metrics, as today
	TracingEnabled bool          // new: spans
	ServiceName    string        // as today (v1.9.1): env wins
	ServiceVersion string        // new: service.version; env wins
	DeploymentName string        // new: k8s.deployment.name and service.instance.id; env wins
	ExportInterval time.Duration // metrics, as today
}

func Setup(ctx context.Context, log logging.Logger, cfg Config) (*Metrics, func(context.Context) error, error)

// ConfigFromEnv reads Krateo's OTel convention: OTEL_ENABLED, OTEL_TRACING_ENABLED,
// OTEL_EXPORT_INTERVAL, DEPLOYMENT_NAME and SERVICE_VERSION. An unparsable value is an error.
func ConfigFromEnv() (Config, error)
```

- **One resource for both signals.** Setup builds the resource once
  (`newResource`, extended in §5) and gives the same value to the
  MeterProvider and the TracerProvider. The audit join reads
  `ResourceAttributes['k8s.deployment.name']` from spans, and the metrics
  should name the same Deployment. With two setup functions, two resources
  could drift.
- **The signals are gated independently.** `Enabled` gates metrics, as
  today, and `TracingEnabled` gates spans. With both off, Setup returns a nil
  `*Metrics` and a no-op shutdown, as it does today.
- **With tracing on,** Setup builds `otlptracehttp.New(ctx)` and
  `sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))`.
  It passes no sampler and no span limits, so the SDK's env handling stays in
  charge (§8). It then calls `otel.SetTracerProvider(tp)`, so the provider's own
  spans and an `otelhttp` client go to the same pipeline, and
  `tracing.SetTracerProvider(tp)` (§3.2). It logs "OpenTelemetry tracing
  initialized".
- **The global propagator is always set.** Setup calls
  `otel.SetTextMapPropagator(TraceContext, Baggage)` before it checks either
  gate, as UR does (`tracing.go:41`), so an `otelhttp` handler or client in the
  provider honours an inbound context even with export off.
  provider-runtime's own extraction and injection use `propagation.TraceContext{}`
  directly, so they work even when Setup is never called.
- **Protocol.** OTLP/HTTP only. `otlptracehttp` ignores
  `OTEL_EXPORTER_OTLP_PROTOCOL`: there is no reference to it in `HTTP/`. So
  Setup returns an error when `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` or
  `OTEL_EXPORTER_OTLP_PROTOCOL` is set to anything other than `http/protobuf`.
  Without that check, protobuf would go to a gRPC port with nothing said.
- **Shutdown flushes.** The returned function shuts down the TracerProvider
  first, then the MeterProvider, and joins their errors.
  `TracerProvider.Shutdown` shuts down each span processor
  (`SDK/trace/provider.go:298-320`), and the batcher exports what it holds
  when it shuts down. Main calls it with a bounded context after
  `mgr.Start` returns, as core-provider already does for its own shutdowns.
- **`ConfigFromEnv` is a helper main calls.** The library still reads no
  environment variables by itself (`docs/configuration.md:25`), apart from the
  standard `OTEL_*` variables the SDK reads and the protocol check above.
  Reading `OTEL_TRACING_ENABLED` inside Setup was rejected: core-provider reads
  it already and runs its own pipeline, so bumping provider-runtime would
  silently give it two TracerProviders (§12).
- **Unset means off.** `ConfigFromEnv` reads `OTEL_TRACING_ENABLED` on its
  own, and an unset variable means off, whatever `OTEL_ENABLED` says. That's
  the rule core-provider and the composition-dynamic-controller follow
  (decision D1, §13). `OTEL_ENABLED` gates metrics only.

### 3.2 `pkg/tracing`: the API-only half

The new package depends on the OTel API (`otel`, `otel/trace`,
`otel/propagation`) and on apimachinery's `metav1.Object`. It never depends on
the SDK or an exporter, so `pkg/reconciler` can import it without linking an
OTLP exporter into every provider. That's the same rule `pkg/logging`
follows (`otel.go:12-13`).

```go
package tracing

const ScopeName = "github.com/krateo-platformops/provider-runtime"

// SetTracerProvider sets the provider the reconcilers use. telemetry.Setup calls it.
// A provider that builds its own SDK TracerProvider calls it too (§11.3).
// nil turns tracing off.
func SetTracerProvider(tp trace.TracerProvider)
func TracerProvider() trace.TracerProvider // nil when off

// Decide applies §6.2's rule to an object's annotations. It is pure.
func Decide(annotations map[string]string) Decision

type Mode int // NoParent, Parent, Link
type Decision struct {
	Mode   Mode
	Remote trace.SpanContext // the annotation's context, valid in Parent and Link
	Marker string            // the krateo.io/traceparent-continued value to write, or ""
}

// StartReconcile opens the reconcile root span for obj, starting at start (§4.1).
// With tp nil it starts nothing: it returns ctx carrying the inbound context for
// Inject and the transport (§6.4), the no-op span, and a Decision with no Marker.
func StartReconcile(ctx context.Context, tp trace.TracerProvider, obj Object, start time.Time) (context.Context, trace.Span, Decision)

type Object struct {
	Kind, Name, Namespace string
	Annotations           map[string]string
}

// Inject and InjectAnnotations write ctx's trace context onto an object another
// Krateo controller reconciles (§6.5).
func Inject(ctx context.Context, obj metav1.Object) bool
func InjectAnnotations(ctx context.Context, annotations map[string]string) bool

// InjectHeader sets traceparent and tracestate on h (§6.6).
func InjectHeader(ctx context.Context, h http.Header) bool
```

`Decide` is exported so the rule can be tested on its own, and so
unstructured-runtime could adopt the same rule without copying it. Changing UR is
for UR's own session. `pkg/meta` gains `AnnotationKeyTraceparent`,
`AnnotationKeyTracestate` (the same strings as UR's `meta.go:19,22`) and
`AnnotationKeyTraceparentContinued` (§6.2).

### 3.3 How the reconciler gets a tracer

The reconciler gets a tracer through a new `reconciler.WithTracerProvider(tp)`
option. When that isn't given, it uses `tracing.TracerProvider()`, read with
one atomic load on each pass, so a Setup called after the reconcilers were
built still takes effect. The option exists for tests, so each test can use
its own in-memory provider without global state, and for a reconciler that
should use a different provider. The tracer is
`tp.Tracer(tracing.ScopeName)`, the same scope name as the meter
(`metrics.go:150`).

The OTel global was rejected as the reconciler's source. Through
`otel.GetTracerProvider()` the reconciler can't tell "off" from "on": the
global delegate's `Start` allocates a span on every call
(`API/internal/global/trace.go:185-187`), and that cost is exactly what off must
not have.

### 3.4 What tracing off costs

With `TracerProvider()` nil and no option set, a pass does one atomic load and
one annotation map lookup. When the object has no `krateo.io/traceparent` (the
fleet today), it allocates nothing, starts no span, and writes nothing.
`trace.SpanFromContext` with no span returns a package-level no-op
(`trace/context.go:37-45`). When the object does carry the annotation, the pass
parses it and adds one context value (§6.4). A test pins zero allocations for
the first case (§10).

## 4. The root span

### 4.1 Where it starts

controller-runtime starts no span (§1), so `Reconciler.Reconcile` starts it.
The span needs the object: §6.2 decides between parent and link from the
object's annotations. So it starts after the Get, but its start
time is the top of `Reconcile`, so it still covers the Get:

```go
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	started := time.Now()
	// the in-flight and duration metrics, and the Get's metric and log, stay as today
	deadline := started.Add(r.timeout + reconcileGracePeriod) // the deadlines of today's two ctxs
	getCtx, cancelGet := context.WithDeadline(ctx, deadline)
	managed := r.newManaged()
	getErr := r.client.Get(getCtx, req.NamespacedName, managed)
	cancelGet()
	if kerrors.IsNotFound(getErr) {
		return reconcile.Result{}, nil // the object is gone: no span, as in UR (worker.go:451-456)
	}
	spanCtx, span, d := tracing.StartReconcile(ctx, r.tracerProvider(), objectOf(r.objectKind, req, managed, getErr), started)
	panicked := true
	defer func() { endReconcileSpan(span, panicked, ...) }()
	log := logging.WithContext(r.log.WithValues("request", req), spanCtx) // #58, now with a span
	log.Debug("Reconciling")
	// ctx and externalCtx are derived from spanCtx with today's deadlines, so every
	// call below, and every provider method, gets the span.
	...
	res, err := r.reconcile(spanCtx, ...) // the rest of today's body
	panicked = false
	return res, err
}
```

- **The object's kind.** `k8s.object.kind` is the Kind alone, such as `Repo`,
  as UR's `GetKind()` gives it. The reconciler keeps
  `schema.GroupVersionKind(of).Kind` from `NewReconciler`, because its `kind`
  field is `Kind.group` (`reconciler.go:716`), and a typed object read through
  the client doesn't reliably carry its TypeMeta (`reconciler.go:394-396`).
- **A failed Get that isn't NotFound** still gets a span. Its attributes
  come from `req` and the error is recorded. UR returns before its span here
  (`worker.go:451-456`), so this error never reaches a trace there.
- **Panics.** The span ends with status Error and the description `panic`,
  through the `panicked` flag. There's no `recover`, so controller-runtime's
  own panic handling (`CR/pkg/internal/controller/controller.go:196-210`) sees
  exactly what it sees today.
- **Order with the metrics defer.** That doesn't matter: both run at return.

### 4.2 Logs correlate

The span starts before the per-reconcile Logger is bound, and the Logger is
bound to `spanCtx`. That's the incoming context plus the span, with no
deadline, which is the same reason #58 binds the incoming context and not the
timeout one. So "Reconciling" and every reconciler log after it carry the root
span's `trace_id` and `span_id`. With #58, a provider that binds its own Logger
in an ExternalClient method (`logging.WithContext(e.log, ctx)`) gets that
child span's ids (§4.4). This needs a slog-backed Logger: a logr-backed Logger
can't carry a context (#58's `docs/api.md` text), which matters for
git-provider (§11.1).

### 4.3 The gated queue and the slot wait

The slot wait isn't part of the span, and in this design it isn't a span event
either.

- **Not part of the span.** With saturation on, the gated queue takes a slot
  before the pop and gives it back in `Done` (`pkg/ratelimit/queue.go:19-25`).
  The root span opens inside that slot, so it measures the reconcile's work,
  never the wait for capacity. Backdating the span to when the item became
  ready would make `reconcile` duration mean "wait plus work". It would also
  move the audit join's reconcile timestamp (`query.sql:49-53`,
  `first_reconcile_after_ms`) to a time when nothing ran.
- **No event yet,** because nothing carries the number to it. controller-runtime
  calls `Reconcile(ctx, req)` with a context that holds a logger and a reconcile
  ID (`CR/.../controller.go:422-481`), and nothing from the queue. Handing the
  wait from the queue to the reconcile would need a per-item table shared by a
  queue and a reconciler. Today they're built separately and know no common
  name: the queue knows its controller name, and the reconciler doesn't.
- **What already shows it.** In Parent mode, the gap between the upstream
  span's end and the `reconcile` span's start is the watch latency plus the
  queue wait plus the slot wait. It's visible in the waterfall as it stands.
  `provider_runtime.reconcile.queue.wait.duration_seconds` attributes it,
  because the wait-instrumented queue measures from enqueue to pop
  (`pkg/controller/queue_wait.go:166-181`). A `queue.wait` event can follow
  if a reader needs it per trace. It's a separate design.

The per-key governor is different. `gate` runs inside `Reconcile`
(`pkg/reconciler/ratelimit.go:140-162`), so a deferral is an event on the root
(§4.5).

### 4.4 Child spans

Five children, each a direct child of `reconcile`, each `SpanKindInternal`,
named in lower case:

| Span | Around | Line today |
|---|---|---|
| `connect` | `r.external.Connect(externalCtx, managed)` | `reconciler.go:885` |
| `observe` | `external.Observe(externalCtx, managed)` | `:926` |
| `create` | `external.Create(externalCtx, managed)` | `:1201` |
| `update` | `external.Update(externalCtx, managed)` | `:1350` |
| `delete` | `external.Delete(externalCtx, managed)` | `:1089` |

- **Around the call itself.** Each child is started after its `gate` returns
  without a deferral and ended when the call returns. A deferred call makes no
  span. The provider's method gets the child's context, so the HTTP calls it
  makes and the spans it opens are children of `observe`, `create` and so on.
- **No attributes.** They inherit the root's object by being its children.
  A provider can add its own with `trace.SpanFromContext(ctx).SetAttributes(...)`
  in its method (§11.3).
- **Errors.** A child is marked Error (`RecordError`, `SetStatus`) exactly
  when its call returns a non-nil error, a conflict included. What that
  means for the pass is decided on the root (§7).
- **Not spanned:** the Get, status updates, finalizer writes, critical
  annotation updates and Disconnect. They're Kubernetes API calls, which
  alert-provider also leaves unspanned (`internal/tracing/tracing.go:15-18`).
  When one fails, the root records it.

### 4.5 Events on the root

Event names are fixed, and their attributes are under `provider_runtime.`:

| Event | When | Attributes |
|---|---|---|
| `ratelimit.deferred` | a `gate` returns a Deferral (`ratelimit.go:140-162`) | `provider_runtime.ratelimit.key`, `.cause` (`throttled`, `capacity`, from `DeferReason.Cause()`), `.requeue_after_s`, and `.until` (RFC 3339) for throttled only |
| `reconcile.paused` | the pause short-circuit (`reconciler.go:806`) | none |
| `k8s.conflict` | a path that requeues on `kerrors.IsConflict` | `provider_runtime.step` (`connect`, `observe`, `create`, `update`, `delete`, `finalizer`) |
| `trace.marker_failed` | the §6.2 marker write failed | `exception.message` |
| `credentialuse.acquire`, `credentialuse.release` | reserved: credential-use isn't wired into the reconciler yet (`pkg/credentialuse/doc.go`: "nothing in provider-runtime uses this package yet"; the plan's PR 7 wires it) | named when PR 7 is designed: the number of refs and the outcome class, never a Secret's data |

A deferral is at most one event per pass, because the pass ends on it.

## 5. Resource attributes

| Key | From, first match wins | Notes |
|---|---|---|
| `service.name` | `OTEL_SERVICE_NAME`, then `OTEL_RESOURCE_ATTRIBUTES`, then `Config.ServiceName`, then the SDK default `unknown_service:<binary>` | unchanged from v1.9.1 (`metrics.go:100-116`, `resource_test.go`); now shared by both signals |
| `service.version` | `OTEL_RESOURCE_ATTRIBUTES`, then `Config.ServiceVersion` | main sets it from its build stamp, falling back to `SERVICE_VERSION`, as core-provider does (`main.go:97-110`) |
| `k8s.deployment.name` | `OTEL_RESOURCE_ATTRIBUTES`, then `Config.DeploymentName` | the audit join's key (`query.sql:55`) |
| `service.instance.id` | `OTEL_RESOURCE_ATTRIBUTES`, then `Config.DeploymentName` | the same value, as UR sets it (`metrics.go:165-169`) |
| `telemetry.sdk.*` | `resource.Default()` | as today |

**Where the Deployment name comes from.** The chart sets `DEPLOYMENT_NAME`
to the same template expression it uses for the Deployment's
`metadata.name`, and `ConfigFromEnv` reads it. For git-provider that's
`{{ include "git-provider.fullname" . }}` (`helm/git-provider/templates/deployment.yaml:4`),
in the ConfigMap the Deployment already loads with `envFrom` (`:35-37`). This
is the mechanism core-provider uses for every composition controller
(`helm/core-provider/assets/cdc/configmap.yaml:22`), and the composition
controller reads it with `env.String("DEPLOYMENT_NAME", "")`
(`go/composition-dynamic-controller/main.go:160`). The downward API can't give
it: a `fieldRef` selects pod fields only, which are `metadata.name`,
`metadata.namespace`, labels, annotations, `spec.nodeName`,
`spec.serviceAccountName`, `status.hostIP` and `status.podIP(s)`
(`k8s.io/api@v0.37.2/core/v1/types.go:2671-2672`). The pod name is
`<deployment>-<replicaset hash>-<suffix>`, and stripping two suffixes from it
breaks for any Deployment name that has dashes in it.

**`service.instance.id` isn't unique per replica.** OTel's semantic
conventions want it unique per instance. With two replicas, both export
the same value. The Krateo contract sets it to the Deployment name anyway, and
providers run one active replica under leader election. Matching UR keeps the
join working.

**Effect on metrics.** The metrics resource gains `service.version`,
`k8s.deployment.name` and `service.instance.id` where the Config sets them. A
backend that turns resource attributes into series labels sees new labels.
ClickHouse's `ResourceAttributes` map does not create new series this way.

## 6. Propagation

### 6.1 Inbound: `krateo.io/traceparent`

The reconciler reads the managed object's `krateo.io/traceparent` and
`krateo.io/tracestate` into a `propagation.MapCarrier` under the W3C keys,
then calls `propagation.TraceContext{}.Extract`, as UR does
(`tracing.go:68-76`). `Extract` returns a remote span context, or leaves `ctx`
unchanged when the value is invalid (`API/propagation/trace_context.go:72-78`).
An invalid value counts as absent.

### 6.2 Parent or link

**The marker.** provider-runtime keeps one annotation of its own on the
object it reconciles:

```
krateo.io/traceparent-continued: <traceparent>
```

The value is the canonical form of the span context that was continued:
`00-<32 hex>-<16 hex>-<2 hex>`, as `TraceContext.Inject` writes it, in lower
case (`trace_context.go:39-64`). A value that doesn't parse counts as absent.

**The rule.** `Decide(annotations)` sets R to the annotation's remote span
context (§6.1) and M to the parsed marker:

1. R isn't valid: **NoParent.** The root is a new root with no link
   (`trace.WithNewRoot()`). No marker.
2. R is valid, and M is absent, or M's trace ID or span ID differs from R's:
   **Parent.** The root's parent is R (`trace.ContextWithRemoteSpanContext`),
   and `Marker` is `canonical(R)`.
3. Otherwise: **Link.** The root is a new root (`trace.WithNewRoot()`) with
   one link, `trace.Link{SpanContext: R}`. No marker.

Only a new traceparent value makes a Parent pass (decision D3, §13). The
marker is keyed on the value, not on whether an annotation is present. So when
a writer overwrites the annotation with a fresh context, the next pass that
sees it is a Parent pass, even though the object carried a traceparent before.
alert-provider's `Incident` is the case: the traceparent stamped at create, in
Analyzing, is continued once. A fresh one, stamped from `incident.analyze` when
the Incident goes to Open, is new and parents again (§11.3, step 5). Only the
trace ID and span ID count as new: a change to the flags or to `tracestate`
alone isn't a new causal event. A generation change with the same value is a
Link pass. That covers a spec edit by a writer that doesn't restamp the
annotation, and the deletion (§6.2, "Deletion").

**The generation isn't needed, and it isn't in the marker.** The plan agreed
with marketing first made a generation change a Parent pass too, and only
that needed the generation recorded. Nothing else does:
- Retries are bounded by when the marker is written, before the external
  calls (below).
- A restart reads the marker from the object.
- A writer that is in the trace restamps the annotation, and its new value
  makes the Parent pass.

**The write.** In Parent mode with tracing on, the reconciler writes the
marker right after the span starts. That's before the pause and orphan
short-circuits and before the governor's `pristine` copy
(`reconciler.go:870`). The write is a JSON merge patch of only that annotation,
with no resourceVersion precondition. It can't overwrite another field, and it
can't conflict. `client.Patch` decodes the response into `managed`, so the
updates later in the pass carry the new resourceVersion. Writing before the
external calls is what bounds the trace: if the pass fails and is retried with
backoff, the retries are linked and not parented. With the marker written at
the end, a permanently failing object would parent every retry onto the
upstream trace, forever.

**When the write fails** (RBAC, a webhook, the object gone): the pass goes
on, records `trace.marker_failed`, and logs once per object. The reconciler then
remembers the marker in memory, keyed by UID and dropped on deletion, so that
within this process the next passes link. Without that, an object whose marker
can never be written would parent every pass. After a restart, such an object
parents once more.

**Why the marker survives restarts.** The marker lives on the object, so a
restart, a leader failover or a new replica decides exactly as the old process
would have. Nothing else on the object can stand in for it:

- Annotation-only updates don't change `metadata.generation`, because the
  CRD strategy bumps it only for changes outside `metadata`
  (`apiextensions-apiserver@v0.37.2/pkg/registry/customresource/strategy.go:178-186`).
  So generation alone can't tell that a new traceparent arrived.
- The status subresource can't carry it: a status update copies the old object
  and keeps only `status` (`status_strategy.go:67-91`). A status field would
  need a CRD change in every provider, and the `ConditionedStatus` in
  `apis/common/v1` has no `observedGeneration`.
- The alternatives that need no write are rejected in §12.

**Deletion.** Setting `deletionTimestamp` bumps the generation
(`apiserver@v0.37.2/pkg/registry/rest/delete.go:166-172`), but the deleter
doesn't restamp the annotation. A `helm uninstall` can't. So the deletion pass
is a Link pass, linked to the last stamper's trace, and not attributed to it.
In Parent mode, a marker patch on an object being deleted is allowed. If the
object is gone, the patch returns NotFound, which counts as success.

### 6.3 What the marker costs

- **Writes.** One metadata patch per new traceparent value, only
  on objects that carry `krateo.io/traceparent`, and only with tracing on. The
  composition-dynamic-controller stamps a child only when its release really
  changes: the per-reconcile traceparent is stripped before the release digest
  (`go/composition-dynamic-controller/internal/tools/processor/processor.go:39-44,92-117`).
  So stamps arrive as often as upstream changes do.
- **One extra reconcile per write.** The patch changes the resourceVersion,
  and a watch event follows. The extra pass sees the marker match and runs in
  Link mode. That's one more Observe against the external API. A provider whose
  predicates already drop annotation-only updates doesn't pay it. alert-provider's
  `changed` lets annotation changes through (`internal/controllers/alert/alert.go:120-139`).
- **The platform's "nothing stamped" rule.** The audit join was built so that
  nothing is stamped on the user's object (observability's
  `ops/audit-reconcile-join/README.md`, first paragraph). This marker is such a
  stamp, triggered by an edge. The user accepted it (decision D2, §13): it's
  the only option that survives restarts.

### 6.4 With tracing off

With no TracerProvider, `StartReconcile` starts no span and writes no marker.
When R is valid, it puts R in `ctx` under a `pkg/tracing` key that only
`Inject`, `InjectAnnotations` and `InjectHeader` read. It's not the active
span. So a disabled provider still passes an upstream context on to the
objects it writes, and to its HTTP calls (UR's reason, in
`go/composition-dynamic-controller/internal/composition/composition.go:495-507`).
But its logs aren't stamped with a trace it took no part in, which is
difference 4 in §2. Because no marker is written while off, turning tracing
on parents the first pass of each annotated object onto the annotation's
trace once.

### 6.5 Onto Kubernetes objects: `Inject`

`tracing.Inject(ctx, obj)` writes `ctx`'s trace context onto `obj`. That's the
active span, or with tracing off, §6.4's inbound context.
- **It overwrites.** An existing `krateo.io/traceparent` is replaced with the
  current context. An existing `krateo.io/tracestate` is replaced when the
  context has a tracestate, and **removed** when it has none, so a stale
  tracestate never travels with a new traceparent.
- **Without a valid context,** it returns false and changes nothing, existing
  annotations included.
- **`InjectAnnotations`** does the same on a map, like UR's `InjectTraceparent`
  (`tracing.go:98-107`).

For annotations, overwriting is the point: a writer that does new work on the
object stamps that work's context. `InjectHeader` (§6.6) is the opposite: it
never overwrites an existing HTTP header, because there a header already set
comes from a more specific span in the same process.

The rules on the caller, in its doc comment:

- **Only onto an object another Krateo controller reconciles.** That means a
  composition, or another provider's managed resource: alert-provider's
  `Incident`, which incident-provider reconciles, is the case in the plan
  (§11.3). Never onto a Pod, ConfigMap or Secret, and never onto the managed
  resource being reconciled, whose annotation is input.
- **Only on a write the caller makes anyway:** a create, or an update that
  changes the object. A write made only to refresh the annotation makes every
  pass a new "first sight" downstream, plus a watch event. The
  composition-dynamic-controller avoids exactly this by excluding the annotation
  from its digest (§6.3).
- **Never in a status write.** A status-subresource write keeps only `status`
  and drops metadata (`status_strategy.go:67-91`). When the change that matters
  is in status, the annotation has to go in a separate metadata write. §11.3,
  step 5, shows what that does to the ordering.
- **Never copy `krateo.io/traceparent-continued`.** It belongs to the object
  it's written on.

The reconciler never injects anything by itself, because providers mostly
don't write such objects.

### 6.6 Outbound HTTP

`httpsignal.Transport.RoundTrip` sets `traceparent`, and `tracestate` when
there is one, from `req.Context()`, through `tracing.InjectHeader`. It does
nothing when:

- the context has no valid trace context, which is the case for every
  request today, so nothing changes;
- the request already has a `traceparent`, for example from an `otelhttp`
  transport wrapped outside it. That one is the more specific parent;
- `Transport.NoTracePropagation` is true, for a provider whose external
  host must not receive a trace header.

It clones the request before setting a header, because a RoundTripper must not
change the caller's request. It never sends baggage (difference 5).
The propagation is in `RoundTrip` and not in `NewTransport`, so a
`&httpsignal.Transport{Base: b}` literal gets it too.

**Why httpsignal, and not `otelhttp`.** Providers already wrap their clients
with httpsignal: git-provider's go-git client is
`httpsignal.NewTransport(nil)` (`internal/clients/git/transport.go:58-59`),
and its remote operations use the reconcile's context (`:39-42`). They get
propagation by bumping provider-runtime. `otelhttp` would add a contrib
dependency, and a client span for every request. One go-git fetch is several
requests, so the volume of §9 would grow. A provider that wants client spans
wraps `otelhttp.NewTransport` outside httpsignal, as alert-provider does in its
own transport (`internal/tracing/transport.go:19-30`). The rule above keeps
otelhttp's header. httpsignal stays a package a provider can choose not to use
(its package comment: "this package is optional").

## 7. Errors and status

The root's status follows how the pass ended:

| How the pass ended | Synced | Root status | On the root |
|---|---|---|---|
| `Reconcile` returns a non-nil error | `ReconcileError` on most paths | Error, with `err.Error()` as description | `RecordError(err)` |
| The unresolved create handshake, inside its recovery window (`reconciler.go:1009-1013`): `ReconcileError` written, nil returned | False, `ReconcileError` | Error, with the condition's message | `RecordError` |
| A governor deferral (`ratelimit.go:180-199`) | False, `ReconcileDeferred` | Unset | `ratelimit.deferred` |
| A conflict requeue (`rateLimitedRequeue()` on `IsConflict`) | unchanged | Unset | `k8s.conflict` |
| Paused (`reconciler.go:806-815`) | False, `ReconcilePaused` | Unset | `reconcile.paused` |
| Success, including `RequeueAfter` | True, `ReconcileSuccess` | Unset, never Ok | none |
| A panic | none written | Error, `panic` | none |
| Get NotFound | none written | no span | none |

- **Error iff the pass failed.** A deferral and a conflict requeue are not
  failures. provider-runtime already treats them that way: they requeue with
  a nil error, so no failure backoff is used. The tail sampler's
  `error-status` policy (observability's
  `charts/otel-collector-tailsampling/values.yaml`, under `policies`) keeps
  every Error trace, so a deferral storm marked Error would keep every trace of
  it.
- **The handshake path.** It's the only path that writes `ReconcileError` and
  returns nil, which `grep` of `ReconcileError(` against its returns in
  `reconciler.go` shows. It marks the root itself. A test (§10) fails if a
  new such path appears without doing the same.
- **Never Ok.** OTel's API leaves Ok to the application. A runtime that set it
  would hide a provider's own Error.
- **A long error.** Error text often embeds an HTTP body, and the SDK doesn't
  bound it: `SetStatus` keeps the whole description
  (`SDK/trace/span.go:205-225`), and an event attribute is cut by count, not by
  length (`span.go:517-531`). provider-runtime cuts the status description and
  `exception.message` to 4 KiB, on a rune boundary. The same text is already in
  the Synced message and the logs, so a trace exposes nothing new.

## 8. Export and sampling

- **Exporter.** `otlptracehttp.New(ctx)` with no options, so the standard env
  configures it. `OTEL_EXPORTER_OTLP_ENDPOINT` is a base URL, and the exporter
  appends `/v1/traces` (`HTTP/internal/otlpconfig/envconfig.go:50-61`).
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is used as it is (`:64-79`).
  alert-provider's workaround for an empty path (`internal/tracing/tracing.go:115-128`)
  is needed only because it passes the URL through `WithEndpointURL`.
  provider-runtime doesn't.
- **Batcher, never sync.** The SDK defaults are a 5 s schedule, a 2048-span
  queue and 512-span batches (`SDK/trace/batch_span_processor.go:21-28`), and
  `OTEL_BSP_*` env overrides them (`:91-101`). A full queue drops spans. It
  blocks only with `BlockOnQueueFull` (`:38,58-62`), which provider-runtime
  doesn't set. A slow or dead collector never blocks a reconcile.
- **Sampler.** None in code. The SDK reads `OTEL_TRACES_SAMPLER` and
  `OTEL_TRACES_SAMPLER_ARG` (`SDK/trace/sampler_env.go`) before it applies
  options (`provider.go:106-117,495-512`), so a `WithSampler` in code would
  override the env. With nothing set, it defaults to `ParentBased(AlwaysSample())`
  (`provider.go:518-521`). That's `parentbased_always_on`: a Parent pass follows
  the upstream's decision, and a NoParent or Link pass is a sampled root.
- **core-provider** sets no sampler in code (`tracing.go:42-45`). No
  `OTEL_TRACES_SAMPLER` appears anywhere in its repo outside tests, charts
  included. So it runs the SDK default, which is `parentbased_always_on`.
  alert-provider sets `OTEL_TRACES_SAMPLER=parentbased_always_on` explicitly
  (`helm/alert-provider/values.yaml:65-67`). Provider charts should set it
  explicitly too, so the value is visible.
- **The endpoint:**
  `http://otel-collector-daemonset-local.krateo-system.svc:4318`.
  - **Verified on krateo-057 by observability** (its review on #60). The
    release is `otel-collector-daemonset`, and the Service is a ClusterIP
    with `internalTrafficPolicy: Local` and 4 of 4 node endpoints: installer
    checked the EndpointSlice. The 70 composition controllers export there.
    The template is observability's
    `charts/otel-collector-daemonset/templates/service-local.yaml`
    (`<release>-local`).
  - **Each provider chart sets it explicitly,** and doesn't inherit a
    fallback. core-provider's chart value `otel.endpoint` defaults to `""`,
    and its configmap then falls back to the headless
    `otel-collector-daemonset-opentelemetry-collector` Service
    (`helm/core-provider/templates/configmap.yaml:13`), which isn't node-local
    (`service-local.yaml` explains why). installer sets the `-local` URL
    explicitly on 057 for that reason (observability's review).
  - alert-provider uses `http://$(HOST_IP):4318`
    (`templates/deployment.yaml:227-232`), which is also node-local.

## 9. Cardinality and cost

- **Span names** are a fixed set of six: `reconcile`, `connect`, `observe`,
  `create`, `update` and `delete`. No name includes an object, a key or a
  host. Event names are fixed (§4.5).
- **Attributes.** The root has exactly three. Children have none of their
  own. Event attributes are bounded per event. The values are per object, which
  is right for traces, and none of them becomes a metric label. Resource
  attributes are constant for the process.
- **Limits.** provider-runtime doesn't change the SDK's span limits
  (`SDK/trace/span_limits.go:9-31`: 128 attributes, events and links), and
  `OTEL_SPAN_*_LIMIT` still applies (`:94-115`). A root has at most one link
  and a few events. The 4 KiB cut on error text is §7's.
- **Volume** is arithmetic, not a measurement. One pass makes 3 or 4 spans:
  the root, `connect`, `observe`, and one of `create`, `update` or `delete` when
  the pass acts. An object polled every P seconds makes about
  `3.2 × 86400 / P` spans a day. With a 3-minute poll that's about 1,500 a day
  per object, so 200 objects make about 300K a day. alert-provider's routine
  loop was 316K spans a day on krateo-057, 2.3 times the rest of the cluster,
  and it was put under the tail sampler's 5% rule for that (observability's
  `charts/otel-collector-tailsampling/values.yaml`, the
  `outside-sampled-services` comment). A provider not listed in that rule is kept
  at 100%. So each provider that turns tracing on is added to the list
  (decision D4, §11.5). There's still no sampler in code (§8).
- **Off** costs what §3.4 says.

## 10. Tests

All with `SDK/trace/tracetest`. For deterministic assertions, use a
`SpanRecorder` registered with `WithSpanProcessor`, or an `InMemoryExporter`
behind `WithSyncer`. Those are test-only: production never syncs. Each test
gets its own provider through `WithTracerProvider`, so no test touches the
globals.

1. **Shape.** A pass that observes and updates makes exactly `reconcile`
   (Server), `observe` and `update` (Internal). The children's parent is the
   root, and the root has exactly the three attributes, with the Kind alone.
   A cluster-scoped object gets `k8s.object.namespace = ""`. The root's start
   is no later than the Get (§4.1).
2. **Each mode of §6.2**, on a fake client that records patches:
   - NoParent: no annotation, so no parent, no link and no patch.
   - Parent: a valid annotation and no marker give a remote parent equal to R,
     and a patch of exactly the marker.
   - Link: run the same object again with the patched marker. The pass is a new
     root with one link to R, and makes no patch.
   - New value: a new span ID gives Parent again.
   - Generation only: bump the generation with the same traceparent. That's
     Link, with no patch (D3). A deletion with the same traceparent is also Link.
   - An invalid traceparent, and an invalid marker.
   - Flags or `tracestate` changed alone: Link.
   - An in-process parent span in the incoming context is not the root's
     parent (difference 6).
3. **Restart.** A second `Reconciler`, built from scratch, on the object the
   first one marked: Link. This is what "doesn't break across restarts"
   means, tested.
4. **A failed marker write.** The patch is forbidden: the pass completes and
   records `trace.marker_failed`, the next pass in the same process links,
   and a new process parents once.
5. **A failed pass after Parent.** Observe fails: the marker was already
   written, so the retry links.
6. **Status mapping**, one test per row of §7. That includes the deferral's
   event and its attributes, the handshake path, `k8s.conflict` with its
   step, a panic (Error, and the panic still reaches controller-runtime), and
   Get NotFound (no span). A source test lists every `SetConditions(...ReconcileError(...))`
   in `reconciler.go`, and fails when one is followed by a nil-error return
   without marking the root.
7. **Log correlation.** A `NewSlogLogger` over `NewOTelJSONHandler` into a
   buffer: the "Reconciling" record and a later reconciler record carry the
   root's `trace_id` and `span_id`. A provider Observe that logs through
   `logging.WithContext(log, ctx)` carries `observe`'s span ID. With tracing
   off, no record carries a `trace_id`, even with an annotation (§6.4).
8. **Off is a no-op.** No provider and no option set: nothing is recorded by
   a recorder registered on the global, and the fake client sees no patch.
   `testing.AllocsPerRun` on `StartReconcile` with a nil provider and an object
   without the annotation is 0. With the annotation, `Inject` and `InjectHeader`
   propagate R.
9. **Inject.** It sets both keys, removes a stale `tracestate`, does nothing
   without a context, and never writes the marker key.
10. **Transport.** Against an `httptest.Server`: `traceparent` matches the
    context's span, and there is no header without one. An existing header is
    kept, and `NoTracePropagation` sends none. There's never a baggage header.
    The caller's request is not changed. The literal form gets propagation too.
11. **Setup.** Tracing off returns a working no-op shutdown and sets no
    provider. On, it sets both providers without dialling, because
    `otlptracehttp.New` doesn't connect. `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` is
    an error. The resource table of §5, including env precedence, extends
    `resource_test.go`. Metrics and traces get an equal resource. Shutdown
    exports spans ended before it, through a real batcher over an in-memory
    exporter. That's the drain test.
12. **Concurrency** (as required for concurrent code). N workers reconcile
    distinct objects at once under `-race`. Every span's parent and link
    belong to its own object, and the marker fallback map stays consistent
    while objects are deleted.

## 11. Adoption

What each consumer sets. The changes belong to each consumer's own session.

### 11.1 git-provider

- **Bump** from v1.11.0.
- **Main** calls `telemetry.ConfigFromEnv()`, sets `ServiceName`, calls
  `telemetry.Setup`, and shuts it down after `mgr.Start` returns. It calls
  neither today.
- **Logger.** Main builds its Logger with `logging.NewLogrLogger(logr.FromSlogHandler(...))`
  (`main.go:63-66`), and a logr Logger can't carry a context (#58). For logs
  to correlate it switches to `logging.NewSlogLogger` over `logging.NewOTelHandler`.
- **Chart ConfigMap:**
  - `OTEL_TRACING_ENABLED`
  - `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector-daemonset-local.krateo-system.svc:4318`, set explicitly (§8)
  - `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
  - `OTEL_TRACES_SAMPLER=parentbased_always_on`
  - `OTEL_SERVICE_NAME=git-provider`, set explicitly (§11.5)
  - `SERVICE_VERSION`
  - `DEPLOYMENT_NAME: {{ include "git-provider.fullname" . }}`
  - `OTEL_ENABLED`, only if it also wants metrics.
- **Before `OTEL_TRACING_ENABLED=true`:** observability lists its `service.name`
  in the tail sampler (§11.5).
- **RBAC.** It already has `patch` on its kinds
  (`templates/clusterrole.yaml:13-23`), which the marker needs.
- **Outbound.** Propagation comes with the bump, through httpsignal (§6.6).

### 11.2 incident-provider

- **Bump** from v1.8.2. That's several minors: the governor, the gated queue
  and the Ready contract came in between (`docs/log.md`).
- **Main and chart** as git-provider, with `OTEL_SERVICE_NAME=incident-provider` (§11.5). Its Logger is already slog over
  `NewOTelJSONHandler` (`main.go:63-64`), so logs correlate after #58.
- **Inject** nothing. It writes Pods and ConfigMaps
  (`internal/controllers/incident/incident.go:360-368`, `apply.go:384-395`),
  which no Krateo controller reconciles.
- **Outbound.** It makes no HTTP calls to propagate on.
- **Its place in the plan.** alert-provider creates the `Incident` objects
  that incident-provider reconciles (§11.3). With alert-provider's `Inject`,
  incident-provider's first pass on a new `Incident` is a child of
  alert-provider's `incident.fire`, and later passes link to it. That's the
  alert → RCA → remediation trace.

### 11.3 alert-provider: migrating without double spans

alert-provider keeps its own SDK pipeline. It's the only one with a redacting
exporter, which strips URL queries before export (`internal/tracing/redact.go`).
It hands that pipeline to provider-runtime and deletes the spans that
provider-runtime now makes. All in one PR, with the bump:

1. In `tracing.Setup`, after `otel.SetTracerProvider(tp)`, it calls
   `prtracing.SetTracerProvider(tp)`. The reconciler's spans then go through
   the redacting exporter, with alert-provider's resource.
2. It adds `k8s.deployment.name` and `service.instance.id` to that resource
   (`tracing.go:88-91` sets only `service.name` and `service.version`), from
   `DEPLOYMENT_NAME`, which its chart gains.
3. It removes the `traced` wrapper (`internal/controllers/alert/tracing.go:30-43`,
   and the `traced{r}` in `alert.go:117`). If it stayed, `alert.reconcile`
   would be a second trace with one span: provider-runtime's root never takes
   an in-process parent (difference 6).
4. It replaces `action` (`tracing.go:47-68`) with attributes on
   provider-runtime's child span: `trace.SpanFromContext(ctx).SetAttributes(alert.hyperdx_id, alert.state, ...)`
   in Observe, Create, Update and Delete. `alert.observe` inside `observe`
   would be a second span for the same work.
   `incident.fire` still links to the span its context holds
   (`tracing.Detach`, `internal/tracing/tracing.go:146-152`), which is now
   provider-runtime's `observe`.
5. **It injects at three points of an Incident's life.** This is acmp's flow,
   from its review on #60.
   - **The lifecycle.** `Fire` creates the Incident in Analyzing, inside
     `incident.fire` (`internal/incident/writer.go:457-463`). The RCA runs in
     `incident.analyze` (`writer.go:605-631`). `finish` then writes the analysis
     and its howToFix scripts and sets Open (`writer.go:468-494`).
   - **Who remediates.** From Open on, incident-provider runs the scripts
     (`writer.go:1-8`). Its Observe marks a check due only when there's a
     current script (`internal/controllers/incident/machine.go:175-177`), and
     its Update starts the check Pod (`incident.go:288-315`).

   The three injections:
   - **(a) At create:** `prtracing.InjectAnnotations` in `kube.Create`
     (`internal/incident/kube.go:100-113`). incident-provider's first pass, on
     the Analyzing Incident, is a child of `incident.fire`.
   - **(b) At Open:** a **fresh** traceparent from the current
     `incident.analyze` span, which overwrites (a)'s (§6.5). Because the marker
     is keyed on the value (§6.2), it's new, and the next pass parents on it.
   - **(c) At reanalysis:** the same as (b), from the `incident.analyze` that
     `incident.reanalyze` starts (`reanalysis.go:37-40`), when it writes the
     new scripts. That's acmp's decision, recorded in its session.

   **The Open write can't carry the annotation.**
   - `finish` writes through `patchStatus` and then `kube.PatchStatus`, which
     is `client.Status().Patch` (`kube.go:115-120`).
   - The Incident CRD has the status subresource
     (incident-provider's `observability.krateo.io_incidents.yaml:495-496`,
     `apis/incident/v1alpha1/types.go:405`).
   - A status write drops metadata (§6.5), and a main-resource write drops
     status (`customresource/strategy.go:166-176`).

   So (b) and (c) have to be a separate metadata patch next to the status
   write (`kube.Patch`, `kube.go:123-128`). That's two watch events, and
   neither order guarantees that the pass that starts the check Pod is the
   Parent pass under §6.2:
   - **Annotation first, then Open.** The annotation's pass usually runs while
     the Incident is still Analyzing. It's the Parent pass, and it writes the
     marker. The Open pass, which starts the Pod, then links. The two events
     coalesce into one Parent pass only if both land before a worker takes the
     key.
   - **Open first, then the annotation.** The Open pass sees (a)'s value,
     already continued, so it links, and it starts the Pod. The annotation's
     pass parents later, but only observes.

   Getting the remediation into the RCA's trace is open question Q6 (§13).
6. Its tests (`tracing_test.go:58-78`) and its configuration docs' span table
   (`docs/configuration.md:51-55`) move to `reconcile` and the lower-case
   children. On observability's side, the tail sampler comment and its test
   that name `alert.reconcile` (`tests/tailsampling-docker.py`) follow. The
   rules themselves key on `service.name` and `^incident\.`, so they still
   match.
7. **It keeps the `incident.*` span names exactly:** `incident.fire`,
   `incident.compare`, `incident.analyze` and `incident.reanalyze`. The tail
   sampler keeps any trace with an `incident.*` span whole (the
   `alert-provider-incident` rule, `^incident\.`). observability says
   alert-provider pinned those names with a test in 0.2.67. Only the
   reconcile-side spans (`alert.reconcile` and the `alert.*` actions) move to
   provider-runtime's names.

alert-provider's `OTEL_TRACING_ENABLED` defaults to `OTEL_ENABLED`
(`internal/tracing/tracing.go:63-67`). It keeps its own Setup, so it keeps that
rule. `ConfigFromEnv`'s "unset means off" (D1) is for the providers that use it.

### 11.4 core-provider (not in the brief, but affected)

core-provider already calls `telemetry.Setup` and has its own
`SetupTracing`. To adopt, it sets `TracingEnabled` from its existing flag and
deletes `coretelemetry.SetupTracing`. Otherwise two TracerProviders race for
the global. Its `compositiondefinition.observe` span becomes a child of
provider-runtime's `observe`.

### 11.5 observability: one step for each provider that turns tracing on

When a provider's chart turns tracing on, observability adds that provider's
`service.name` (its `OTEL_SERVICE_NAME`) to the
`outside-sampled-services` condition of the tail sampler, in
`charts/otel-collector-tailsampling/values.yaml` (decision D4).
- **Effect.** The routine loop then falls to the `sample-5pct` rule. The
  `error-status` rule still keeps every failed reconcile, and a Parent pass
  inside a kept trace stays whole with it.
- **No sampler in code.** Providers set no sampler in code, and their charts
  keep `OTEL_TRACES_SAMPLER=parentbased_always_on` (§8). Without this step,
  every trace from a provider not on the list is kept at 100% (§9).
- **Order.** The sampler entry ships first. The provider's chart change that
  sets `OTEL_TRACING_ENABLED=true` ships after it.
- **What the audit join then finds.** With a provider under `sample-5pct`,
  observability#127's join finds a provider object's reconcile after a write
  about 1 time in 20 when that reconcile succeeded, and every time when it
  failed, because `error-status` keeps it. A reconcile that is a Parent pass
  under a stored portal trace is kept with that trace. So **a successful
  provider reconcile missing from the join doesn't mean it never ran.**
  Composition controllers are different: their reconcile traces are kept whole
  on purpose, because the reconcile widgets read them (observability's review
  on #60).

**`service.name` per provider.** The sampler matches
`resource.attributes["service.name"]` exactly, and a mismatch silently keeps a
provider at 100%. So each provider's chart sets `OTEL_SERVICE_NAME` explicitly,
to a fixed string and never one derived from the release name. Because the
environment wins (§5), that string is the exported `service.name`.
observability puts the same string in `outside-sampled-services`.

| Provider | `OTEL_SERVICE_NAME` | Source |
|---|---|---|
| git-provider | `git-provider` | proposed: git-provider exports no service name today. Its chart has no `OTEL_SERVICE_NAME`, and its log handler names none (`main.go:59-66`) |
| incident-provider | `incident-provider` | the name its logs already carry (`main.go:34,63`) |
| alert-provider | `krateo-alert-provider` | its chart's `otel.serviceName` (`helm/alert-provider/values.yaml:68-69`), already in `outside-sampled-services` |
| core-provider, if it adopts (§11.4) | `core-provider` | its `--otel-service-name` default, `<kebab(providerName)>-provider` with `providerName = "Core"` (`main.go:38,72`) |

A provider not in this table picks its value when it adopts, and the same
order holds: the sampler entry ships first, the chart change after.

For the audit join (decision D5), observability maps each provider's API group
to its Deployment name to join provider objects. `<resource>-<version>-controller`
names only composition controllers (§13, D5).

## 12. Alternatives rejected

- **A separate `telemetry.SetupTracing`, as UR has.** Two resources that can
  drift, two shutdowns to order, and a second call to forget.
- **Wrapping the reconciler in a `reconcile.Reconciler` that starts the span,
  as alert-provider does.** The wrapper hasn't read the object, so it can't
  apply §6.2 without a second Get. And it can't open the children.
- **The OTel global as the reconciler's tracer.** It can't tell off from on,
  and it allocates on every pass (§3.3).
- **Parent on every pass (UR today).** One trace per object, growing for the
  object's whole life. The tail sampler keeps traces whole, so it would keep
  that one too.
- **Link on every pass.** It loses the one causal edge worth keeping: the
  first reconcile after an upstream change.
- **First sight from an in-memory memo.** No writes, but a restart or a
  failover forgets it. Linking an unknown context would demote the real first
  sight after every restart, and parenting it would grow the trace on every
  restart. The memo is kept only as the fallback when the marker can't be
  written (§6.2).
- **The marker in status.** A status field needs a CRD change in every
  provider. A dedicated condition type would put bookkeeping in a user-facing
  API that readiness checks read.
- **First sight from `managedFields` times.** Caches commonly strip
  `managedFields`, and a manager's time changes for any field it owns, not
  just the annotation.
- **Writing the marker at the end of the pass.** A failing pass would parent
  every retry (§6.2).
- **Backdating the root to enqueue time, or a queue span.** These change what
  `reconcile` duration means and what the audit join's timestamp means, and
  nothing carries the per-item number to the reconcile (§4.3).
- **`otelhttp` inside provider-runtime.** A contrib dependency, and a span for
  every request (§6.6).
- **Baggage on provider-runtime's carriers.** It would carry a portal session
  to third parties (§2).
- **Reading `OTEL_TRACING_ENABLED` inside Setup.** That would silently double
  core-provider's pipeline (§11.4), and the library reads no env by itself.
- **Outcome attributes on the root,** such as a deferral key or a requeue
  reason. They would break "exactly three", so they're events.
- **A sync exporter.** It blocks reconciles on the collector.

## 13. Decided by the user, 2026-11-09

The questions this design left open, and what was decided. Every premise was
checked in source, except where it's marked as a cluster fact.

**D1. `OTEL_TRACING_ENABLED` unset means off, on its own.** This was option (a).
`ConfigFromEnv` reads it on its own, defaulting to false, and `OTEL_ENABLED`
gates only metrics (§3.1). That's what core-provider and the
composition-dynamic-controller do (core-provider `main.go:69-70`, CDC
`main.go:155-156`). alert-provider defaults it to `OTEL_ENABLED`
(`internal/tracing/tracing.go:63-67`), and says snowplow and access-review do
the same. It keeps that rule in its own Setup (§11.3). Turning on metrics never
starts a span stream.

**D2. The marker annotation is accepted.** This was option (a), and it's the
only option that survives restarts (§6.2). Its cost is one metadata patch, plus
one extra reconcile, per new traceparent value, only on annotated objects and
only with tracing on (§6.3). Rejected:
- (b) An in-memory memo, which loses the first sight after a restart or a
  failover.
- (c) Parenting every pass, as UR does, which grows traces without bound.

**D3. Parent only on a new traceparent value.** This was option (b). A
generation change with the same value is a Link pass, and so is a deletion
(§6.2). That changes the plan first agreed with marketing, which counted a
generation change too. The reasons:
- A generation-only change comes from a writer that doesn't restamp. The
  portal doesn't stamp: the audit join is out of band because "nothing is
  stamped on the user's object" (observability's README). Nothing in
  incident-provider's code injects a traceparent either: its apply Pod runs a
  script with a kubeconfig (`apply.go:384-395`).
- So parenting such a change would pin it to the last stamper's trace.
- A writer that is in the trace restamps, and its new value is caught.

So the marker is the traceparent alone, with no generation (§6.2).

**D4. Each provider's `service.name` goes on the tail sampler's
sampled-services list when it turns tracing on.** This was option (a). It's a
step for observability, in §11.5. There's no sampler in code (§8). At §9's
rate, 200 objects on a 3-minute poll make about 300K spans a day. That's
arithmetic: the object counts on krateo-057 weren't checked. Rejected:
- (b) Head sampling, which loses whole Link-mode traces at random, errors
  included.
- (c) Turning tracing on only while investigating.

**D5. Exactly three attributes now; `k8s.object.group` follows in both
runtimes together.**
- **The problem.** The audit join builds `<resource>-<version>-controller` and
  compares it with `k8s.deployment.name` (`query.sql:8-9`). That's
  core-provider's naming for composition controllers. A provider runs one
  Deployment for several Kinds: git-provider's ClusterRole covers `repoes` and
  `localresources` (`clusterrole.yaml:13-23`). And the audit record has no Kind.
- **For now,** observability maps each provider's API group to its Deployment
  name (§11.5).
- **The follow-up.** Adding `k8s.object.group` to the `reconcile` span is a
  coordinated contract change, made in unstructured-runtime and
  provider-runtime together, with observability's join. It's recorded here and
  not designed here. With the Kind alone, two providers that each have a Kind
  of the same name look the same. Each runtime's own session makes its change.

**Q6 (open). How does an Incident's remediation join the RCA's trace?**
§11.3, step 5, shows the problem. Under §6.2 the first pass to see a new value
is its child. But alert-provider's Open is a status write, so the fresh value
arrives in a separate write, and the pass that starts the check Pod isn't
reliably that first pass. Under D4, incident-provider's own roots are kept 5% of
the time when healthy, so a remediation that only links to the RCA is usually
not stored at all.
- **(a) Continue on the first pass that acts, not the first that sees**
  (provider-runtime; it refines D3's mechanics, not its rule). Only a new value
  makes a Parent pass, as decided.
  - **The change.** In Parent mode, the marker is written just before the
    pass's first Create, Update or Delete call, instead of at first sight. A
    pass that only connects and observes stays Parent and writes nothing.
  - **The bound.** If no pass acts within W of this process first seeing the
    value, the next pass writes the marker and links. W is proposed at
    10 minutes, and the first-seen time is kept in memory by UID, so a restart
    reopens W once.
  - **For the Incident,** with the annotation written first: the Analyzing
    pass doesn't act, because no current script means no check is due
    (`machine.go:175-177`), so it stays Parent. The Open pass calls Update,
    which starts the Pod, and writes the marker just before. So the
    remediation is a child of `incident.analyze`.
  - **What it costs.** The observe-only passes inside W parent onto the trace,
    and so do retries that fail before acting. Both are bounded by W.
  - **What it requires** is a writer that stamps the annotation **before** the
    change that triggers work. That's a rule to add to §6.5.
- **(b) Put the trigger and the context in one write** (alert- and
  incident-provider). What starts remediation would move out of status, into
  a main-resource field or annotation that incident-provider reads, so that one
  `kube.Patch` carries both. It's an API change in two other repos, and
  provider-runtime is unchanged.
- **(c) Accept a link.** Write the annotation, then Open. The remediation pass
  is a new root linked to `incident.analyze`. Nothing changes anywhere, but the
  remediation isn't in the RCA's trace, and usually isn't stored.
- **Recommendation: (a).** "The first pass that acts on a new upstream context
  is its child" says what a trace should say. It fixes the case without an API
  change elsewhere, and it keeps the growth bounded. The cost is a second piece
  of state (the in-memory first-seen time) and a rule change for writers
  (stamp first), so it's yours to decide. If (a) is chosen, §6.2, §6.3, §6.5
  and §10's tests change with it.

## 14. Checked, and not

Checked in source:

- **The UR contract** (§2), read at the v1.12.0 tag.
- **controller-runtime v0.25.2** imports no OTel package outside its tests,
  and passes no queue data to `Reconcile`.
- **SDK behaviour:** the default sampler, the order of env and options, the
  batcher's defaults and its non-blocking drop, `Shutdown`, span limits, and
  that event attribute values and status descriptions aren't truncated.
- **API behaviour:** the global tracer's remote-context behaviour,
  `TraceContext` inject and extract, and `WithNewRoot`, `WithLinks` and
  `WithTimestamp` as start options (`trace/config.go:256-311`).
- **otlptracehttp** appends `/v1/traces` to the base endpoint and ignores
  `OTEL_EXPORTER_OTLP_PROTOCOL`.
- **Kubernetes:** a CRD's generation rule, the status strategy, the
  generation bump on graceful deletion, and the downward API's field list.
- **The consumers:** every consumer file cited in §11, observability's join,
  Service and tail-sampler files, and #58's diff.

Not checked:

- **What krateo-057 runs**, checked by me: the env values, the object counts,
  and which endpoint each Deployment uses. I don't touch clusters. The brief
  reports the env convention as verified on 057. observability verified §8's
  endpoint, the release name `otel-collector-daemonset` and the -local
  Service's 4 of 4 node endpoints on 057, in its review on #60. alert-provider's
  `incident.*` name test in 0.2.67 is as observability reports it; I didn't
  read that test.
- **Whether every provider's predicates let annotation-only updates through**
  (§6.3). Only alert-provider's was read.
- **How long it takes to build:** not estimated.
