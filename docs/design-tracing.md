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
(`logging.ContextLogger`, `logging.WithContext`). That's merged, and it
shipped in v1.13.0 (`3ab2917`). The user decided its six open questions on
2026-11-09, and §13 records them (D1 to D6). The sixth came from the review on
#60.

**The goal** (the user's decision). Every provider built on provider-runtime
(git-provider, incident-provider, alert-provider and the others) gets
reconcile traces by bumping provider-runtime, calling `telemetry.Setup` in
main, and setting env. No provider writes span code.

**References.** Code is provider-runtime at `3ab2917` (main, v1.13.0). `UR` is
`krateo-platformops/unstructured-runtime` at tag `v1.12.0` (`5b4dbff`), read with
`gh api`. `CR/` is `sigs.k8s.io/controller-runtime@v0.25.2`, and `SDK/` is
`go.opentelemetry.io/otel/sdk@v1.45.0`. `API/` is `go.opentelemetry.io/otel@v1.45.0`
and `go.opentelemetry.io/otel/trace@v1.45.0`, and `HTTP/` is
`go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.45.0`. All
were read in the module cache. The OTel versions are the ones in `go.mod`;
otlptracehttp isn't in `go.mod` yet. It's a **new dependency**, at v1.45.0 to
match `otel` v1.45.0 (§3.1). The consumers were read
on their `main` branches: core-provider `bfc5903`, alert-provider `b7deac4`,
incident-provider `395c94d`, git-provider `8bc9e3c` and observability `56e7ea4`.
§14 lists what was checked and what wasn't.

## 1. The problem

provider-runtime has no `TracerProvider` and starts no span.
`pkg/telemetry` exports metrics only (`metrics.go:120`). `pkg/logging/otel.go:79`
stamps `trace_id` and `span_id` when the record's context holds a span. Since
v1.13.0 (#58) the reconciler binds its Logger to its `ctx`
(`reconciler.go:747`). But nothing ever puts a span
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
   (`tracing.go:68-76`). provider-runtime makes a new value the parent until the
   first pass that acts on it, or until a window closes, and links to it after
   that (§6.2, D6).
3. **A deferral is not an error.** UR's `deferPass` returns a `DeferredError`
   (`worker.go:843,863`), which `RecordSpanError` marks as Error. provider-runtime's
   deferral returns a nil error (`pkg/reconciler/ratelimit.go:180-199`), and its
   span stays Unset, with an event (§7).
4. **With tracing off, nothing is stamped with someone else's trace.** UR's
   no-op tracer puts the remote span context in `ctx` as the active span
   (`API/internal/global/trace.go:185`). So every log of a disabled controller
   carries the upstream `trace_id` and `span_id`, and so does every outbound
   call its instrumentation makes. provider-runtime carries the inbound context
   only onto the Kubernetes objects it writes, through `Inject`. It doesn't
   stamp its logs with it, and doesn't send it over HTTP (§6.4).
5. **provider-runtime's own carriers never send baggage.** The annotations and
   the HTTP transport carry `traceparent` and `tracestate` only. Platform
   baggage carries a portal session (observability's
   `ops/audit-reconcile-join/README.md`, the `session_id` column), and it
   shouldn't reach a third-party API.
6. **The root never takes an in-process parent.** If something wrapped the
   reconciler in its own span, the `reconcile` span would still be a root, or a
   child of the annotation's remote parent. That keeps the parent-or-link rule
   exact (§6.2) and the audit join's assumption true (§11.3).

**Changes to code that exists today.** Besides the new code, three things
behave differently. Each goes in `docs/log.md` when it ships; the entry is
already there, under this design.
- **`RetryingCriticalAnnotationUpdater` writes only the runtime's own keys**
  (§6.2, "The critical write").
  - **Today** it captures the whole annotation map, re-Gets the object, adds
    the captured map back, and Updates (`pkg/reconciler/api.go:41-51`). So it
    writes back a stale `krateo.io/traceparent` and `tracestate` over a fresh
    one written in between, and it reverts any other writer's annotations the
    same way.
  - **After,** it carries only `krateo.io/external-name`, the
    `krateo.io/external-create-*` markers and `krateo.io/traceparent-continued`.
    A provider that sets another annotation in memory and relies on this write
    to persist it would lose it. None of the consumers read for §11 does: git-,
    incident- and alert-provider set no annotation on their managed resources,
    and core-provider's annotation writes are its own (`getters.go:279-299`).
- **`telemetry.Setup` sets the global propagator on every call** (§3.1).
  OTel's default global propagator is a no-op
  (`API/internal/global/propagator.go:29,55`). So a provider's own `otelhttp`
  starts extracting and injecting W3C context and baggage once it calls Setup,
  tracing on or off. core-provider already sets the same propagator itself, so
  it sees no change.
- **httpsignal sets `traceparent` on outbound requests when tracing is on**
  (§6.6). With tracing off, it sends nothing.

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
  provider honours an inbound context even with export off. That's what the
  brief asks for ("installed even when export is off"). It changes behaviour
  for existing Setup callers, so it's in §2's list and in `docs/log.md`.
  - **Why not set it only with tracing on:** a provider would then honour an
    inbound context depending on its export switch. core-provider already sets
    it unconditionally (`tracing.go:32`).
  - **provider-runtime's own carriers ignore the global.** Its extraction and
    `Inject` use `propagation.TraceContext{}` directly, so they behave the same
    even when Setup is never called.
- **Protocol, checked only with tracing on.** OTLP/HTTP only. `otlptracehttp`
  ignores `OTEL_EXPORTER_OTLP_PROTOCOL`: there is no reference to it in
  `HTTP/`. So when `TracingEnabled` is true, Setup returns an error if
  `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` or `OTEL_EXPORTER_OTLP_PROTOCOL` is set
  to anything other than `http/protobuf`. Without that check, protobuf would go
  to a gRPC port with nothing said. With tracing off there's no trace exporter
  to misconfigure, so a metrics-only provider with a gRPC protocol variable set
  for another tool doesn't start failing.
- **A new dependency.** `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp`
  v1.45.0, which pulls in `otlptrace` v1.45.0. Neither is in `go.mod` today.
  Only `pkg/telemetry` imports it.
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

// Decide applies §6.2's rule. It is pure.
//   - remembered is the value this process already marked, or failed to mark,
//     on this object (§6.2, "The memo"), or "".
//   - firstSeen is when this process first saw the annotation's current value.
//     The zero time means first sight, with the window open.
//   - window == 0 means Parent with WriteMarker = Now: the first-sight rule.
func Decide(annotations map[string]string, remembered string, firstSeen, now time.Time, window time.Duration) Decision

type Mode int  // NoParent, Parent, Link
type When int  // Never, BeforeFirstAction, Now
type Decision struct {
	Mode        Mode
	Remote      trace.SpanContext // the annotation's context, valid in Parent and Link
	Marker      string            // the krateo.io/traceparent-continued value, or ""
	WriteMarker When              // when the reconciler writes Marker
}

// StartReconcile opens the reconcile root span for obj as d says, starting at
// start (§4.1). The reconciler computes d, from its memo and Decide, before it
// calls this. With tp nil it starts nothing. It returns ctx carrying d.Remote,
// when that's valid, for Inject only (§6.4), and the no-op span.
func StartReconcile(ctx context.Context, tp trace.TracerProvider, obj Object, d Decision, start time.Time) (context.Context, trace.Span)

type Object struct {
	Kind, Name, Namespace string
	Annotations           map[string]string
}

// Inject and InjectAnnotations write ctx's trace context onto an object another
// Krateo controller reconciles (§6.5).
func Inject(ctx context.Context, obj metav1.Object) bool
func InjectAnnotations(ctx context.Context, annotations map[string]string) bool

// InjectHeader sets traceparent and tracestate on h from ctx's active span
// only, never from the inbound context, so it sends nothing with tracing off (§6.6).
func InjectHeader(ctx context.Context, h http.Header) bool
```

`Decide` is exported so the rule can be tested on its own, and so
unstructured-runtime could adopt the same rule without copying it. Changing UR is
for UR's own session. `pkg/meta` gains `AnnotationKeyTraceparent`,
`AnnotationKeyTracestate` (the same strings as UR's `meta.go:19,22`) and
`AnnotationKeyTraceparentContinued` (§6.2).

The reconciler gains two options: `WithTracerProvider` (§3.3) and
`WithTraceContinueWindow` (§6.2).

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
		r.traces.forget(req.NamespacedName) // §6.2, "The memo"
		return reconcile.Result{}, nil       // the object is gone: no span, as in UR (worker.go:451-456)
	}
	tp := r.tracerProvider()
	d := r.traces.decide(tp, req.NamespacedName, managed, time.Now()) // the memo, then tracing.Decide; no entry when tp is nil
	spanCtx, span := tracing.StartReconcile(ctx, tp, objectOf(r.objectKind, req, managed, getErr), d, started)
	panicked := true
	defer func() { endReconcileSpan(span, panicked, ...) }()
	log := logging.WithContext(r.log.WithValues("request", req), spanCtx) // v1.13.0 binds ctx here (reconciler.go:747); now it holds a span
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
- **Today's binding moves.** v1.13.0 binds the Logger at the top of
  `Reconcile`, before the Get (`reconciler.go:747`). The binding moves to after
  the span starts, and "Reconciling" moves with it.
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
deadline, which is the same reason v1.13.0 binds the incoming context and not
the timeout one. So "Reconciling" and every reconciler log after it carry the
root span's `trace_id` and `span_id`. Since v1.13.0, a provider that binds its
own Logger
in an ExternalClient method (`logging.WithContext(e.log, ctx)`) gets that
child span's ids (§4.4). This needs a slog-backed Logger: a logr-backed Logger
can't carry a context (`docs/api.md`, since v1.13.0), which matters for
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
| `connect` | `r.external.Connect(externalCtx, managed)` | `reconciler.go:888` |
| `observe` | `external.Observe(externalCtx, managed)` | `:929` |
| `create` | `external.Create(externalCtx, managed)` | `:1204` |
| `update` | `external.Update(externalCtx, managed)` | `:1353` |
| `delete` | `external.Delete(externalCtx, managed)` | `:1092` |

- **Around the call itself.** Each child is started after its `gate` returns
  without a deferral and ended when the call returns. A deferred call makes no
  span. The provider's method gets the child's context, so the HTTP calls it
  makes and the spans it opens are children of `observe`, `create` and so on.
- **Each method's own `ctx`.** A provider must use the `ctx` each method is
  given. A client built in Connect that keeps Connect's `ctx` and uses it
  later would parent its Observe-time calls onto the `connect` span, which
  has already ended. The doc comments of `ExternalConnecter` and
  `ExternalClient` say so.
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
| `reconcile.paused` | the pause short-circuit (`reconciler.go:809`) | none |
| `k8s.conflict` | a path that requeues because `kerrors.IsConflict(err)` | `provider_runtime.step`: `connect` (`reconciler.go:898`), `observe` (`:940`), `handshake` (the marker-removing Update, `:1047`), `finalizer` (`:836`, `:1136`, `:1162`), `create_pending` (`:1192`), `create` (`:1241`), `late_init` (`:1303`), or `marker` (§6.2) |
| `trace.marker_failed` | the §6.2 marker write failed with anything but a conflict | `exception.message` |
| `credentialuse.acquire`, `credentialuse.release` | reserved: credential-use isn't wired into the reconciler yet (`pkg/credentialuse/doc.go`: "nothing in provider-runtime uses this package yet"; the plan's PR 7 wires it) | named when PR 7 is designed: the number of refs and the outcome class, never a Secret's data |

A deferral is at most one event per pass, because the pass ends on it. The
`step` set is every `IsConflict` site in `reconciler.go` at `3ab2917`. A source
test (§10) fails if one is added without a step. The event keys on
`IsConflict` only: a conflict that a path wraps or turns into a
`ReconcileError` is not this event.

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

**The rule.** `Decide(annotations, remembered, firstSeen, now, window)` sets R
to the annotation's remote span context (§6.1) and M to the parsed marker. A
value is *continued* when M, or `remembered`, has R's trace ID and span ID.

1. R isn't valid: **NoParent.** The root is a new root with no link
   (`trace.WithNewRoot()`). `WriteMarker` is `Never`.
2. R is continued: **Link.** The root is a new root (`trace.WithNewRoot()`)
   with one link, `trace.Link{SpanContext: R}`. `WriteMarker` is `Never`.
3. R isn't continued and `window` is 0: **Parent**, with `WriteMarker` set to
   `Now`. That's the earlier first-sight rule, kept as an option.
4. R isn't continued, and `firstSeen` is zero or `now - firstSeen < window`:
   **Parent.** The root's parent is R (`trace.ContextWithRemoteSpanContext`),
   `Marker` is `canonical(R)`, and `WriteMarker` is `BeforeFirstAction`.
5. R isn't continued, and `now - firstSeen >= window`: **Link,** with
   `WriteMarker` set to `Now` (decision D6, §13).

The reconciler computes the Decision before it starts the span, from its memo
and `Decide`, and passes it to `StartReconcile` (§3.2). So the mode a test
asserts is the mode the span gets.

**Which pass is the child.** The pass that **first acts** on a new value is
its child, not the pass that first sees it (D6).
- **What "acts" means.** A pass acts when the runtime calls the provider's
  `Create`, `Update` or `Delete`. That's the runtime's calls only: it can't see
  what a provider does inside Observe. alert-provider starts incident firings
  from Observe (`internal/controllers/alert/alert.go:442-473`), and those don't
  count. Observe-time work like that joins the upstream trace for as long as
  the window keeps passes Parent, and no longer.
- **A pass that only connects and observes** stays a child of R and writes
  nothing, so the next pass is still a Parent pass. That's what lets a change
  that arrives in two writes, the context first and the trigger after, put the
  work in the context's trace (§11.3, step 5).

Only a new traceparent value makes a Parent pass (decision D3, §13). The
marker is keyed on the value, not on whether an annotation is present. So when
a writer overwrites the annotation with a fresh context, the next pass that
sees it is a Parent pass, even though the object carried a traceparent before.
- **alert-provider's `Incident` is the case.** The traceparent stamped at
  create, in Analyzing, is continued once. A fresh one, stamped from
  `incident.analyze` before the Incident goes to Open, is new and parents
  again (§11.3, step 5).
- **What counts as new.** Only the trace ID and span ID: a change to the flags
  or to `tracestate` alone isn't a new causal event.
- **What doesn't.** A generation change with the same value is a Link pass.
  That covers a spec edit by a writer that doesn't restamp the annotation, and
  the deletion ("Deletion", below).

**The generation isn't needed, and it isn't in the marker.** The plan agreed
with marketing first made a generation change a Parent pass too, and only
that needed the generation recorded. Nothing else does:
- Retries are bounded by the marker, written before the acting call, and by
  the window.
- A restart reads the marker from the object.
- A writer that is in the trace restamps the annotation, and its new value
  makes the Parent pass.

**The write, with an optimistic lock.** Every marker write is a JSON merge
patch of only that annotation, built with
`client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})`
(`CR/pkg/client/patch.go:68,83-86,158`). The patch then carries the
resourceVersion the pass read, and the API server refuses it with a conflict
if the object has changed since.
- **Sent on a copy.** The patch goes on a deep copy of `managed`, never on
  `managed` itself. `managed` holds what Observe computed in memory, and
  decoding the response into it would throw that away.
- **What's copied back.** When the patch succeeds, the reconciler copies only
  the new resourceVersion and the marker onto `managed`. The lock guarantees
  the stored object is the one the pass read plus the marker, so `managed` with
  the new version is still exactly that object plus the pass's own changes.
  Later writes in the pass stay version-checked, including the status Update
  after each acting call (`reconciler.go:1380`, `:1121`), and the provider's
  own writes. A status Update with a stale version is refused
  (`status_strategy.go:79-81`).
- **Why the lock.** Without it, an unconditioned patch would succeed against
  whatever the object had become, and copying its version onto `managed`
  would switch optimistic concurrency off for the rest of the pass. Say
  alert-provider's reanalysis `Status().Patch`es new scripts between
  incident-provider's Get and its marker patch. incident-provider's
  `Status().Update(managed)` would then silently overwrite them. A pass that
  read a stale cache would also parent and act a second time.
- **On a conflict:** `rateLimitedRequeue()`, before the pass acts, with
  `k8s.conflict` and step `marker`. The memo entry stays as it was, so the next
  pass reads the object afresh and, if the value is still new, is a Parent pass
  again. D6 holds.

**When each write happens.**
- **`BeforeFirstAction`:** just before the first `Create`, `Update` or
  `Delete` call, after that call's `gate` lets it through, and before its child
  span starts. A pass the governor defers doesn't act and doesn't write.
  - **On the create path,** it's a separate locked patch, just before the
    `krateo.io/external-create-pending` Update (`reconciler.go:1189-1200`).
    That's one more write per Parent create. Folding the marker into the
    create-pending Update would save the write. But a create-pending write
    rejected only because of the marker would then block the create, and a
    conflict there would mean something else. One failure rule for every
    marker write is worth the extra write.
- **`Now`** (rules 3 and 5): after the pause check (`reconciler.go:809`) and
  the Orphan-deletion path (`:824-851`), and before the governor's `pristine`
  copy (`:873`). A paused object, or one released under the Orphan policy,
  gets no marker write: nothing on it acts, and writing to a paused object
  would be the runtime acting on it.

Writing before the acting call bounds the trace. If the call fails and the
pass is retried with backoff, the retries link and don't parent. With the
marker written at the end, a permanently failing object would parent every
retry onto the upstream trace, forever.

**A crash between the marker write and the act** loses the parent. The next
pass sees the marker, links, and does the work in a linked root. That's the
accepted cost of writing first, and it only happens when the process dies
inside that gap.

**The critical write carries only the runtime's own keys.** The reconciler's
default `CriticalAnnotationUpdater` today captures the whole annotation map,
re-Gets the object, adds the map back and Updates (`pkg/reconciler/api.go:41-51`).
It runs after `Create` (`reconciler.go:1228`, `:1268`) and when an unresolved
handshake adopts the resource (`:989`). So a fresh traceparent B, written by
another controller while `Create` ran, would be reverted to the A the pass had
read, and B would be lost. The updater changes to capture only
`krateo.io/external-name`, the `krateo.io/external-create-pending`,
`-succeeded` and `-failed` markers, and `krateo.io/traceparent-continued`. That
also stops it reverting any other writer's annotations. It's a change to
existing code, listed in §2.

**The memo: what the reconciler keeps in memory.** One entry per object, in a
map keyed by `types.NamespacedName` and holding the object's UID:

| Field | Meaning |
|---|---|
| `uid` | the object the entry is about |
| `value` | the canonical traceparent last seen |
| `firstSeen` | when this process first saw `value`, monotonic |
| `state` | `seen` (not continued yet), `marked` (this process wrote the marker), `failed` (the write failed with something other than a conflict) |

- **No entry when tracing is off.** With no TracerProvider, the reconciler
  calls `Decide` with an empty `remembered` and a zero `firstSeen`, and keeps
  nothing.
- **Created** on the first Parent decision for a value, as `seen`.
- **Replaced** when the value changes, or when the UID differs. A deleted and
  recreated object with the same name starts fresh.
- **`marked`** after a successful marker write. It stays until a Get shows the
  marker on the object, then it's dropped. Until then, `remembered` makes the
  pass Link, so a cache that lags behind the write doesn't produce a second
  Parent pass.
- **`failed`** after a write that failed with anything other than a conflict
  (RBAC, a webhook). The pass records `trace.marker_failed` and logs once per
  object, and later passes in this process link through `remembered`.
  Without it, an object whose marker can never be written would parent every
  pass. After a restart, such an object parents again until its next acting
  pass or the end of the window.
- **Dropped** on NotFound, when the finalizer is removed, when the annotation
  goes away or stops parsing, and as above.
- **Locking.** One `sync.Mutex` per Reconciler guards the map. It's held only
  to read or update an entry, never across an API call. controller-runtime
  never runs two passes of one key at once, so two passes never race on one
  entry; the mutex protects the map across keys.
- **Growth.** At most one entry per object the controller has reconciled that
  carries a traceparent whose marker isn't confirmed yet. `seen` entries end
  within a window and a pass. `marked` entries end at the next read. `failed`
  entries last while the object lives. NotFound and finalizer removal drop
  them, so the memo never outgrows the set of live annotated objects.

**The window: when a new value stops parenting without an action.** A value
that never leads to an action would otherwise make every pass a Parent pass,
forever.
- **How it's measured.** From the memo's `firstSeen`, this process's first
  sight of the value, by the monotonic clock. It closes when
  `now - firstSeen >= window`. The next pass then links and writes the marker
  (rule 5).
- **The setting.** `window` is a Reconciler option,
  `reconciler.WithTraceContinueWindow(d)`, defaulting to 10 minutes. 0 means
  "continue on the first pass" (rule 3).

Why 10 minutes:
- **Every default poll fits at least three times.** provider-runtime's
  default poll is 1 minute (`reconciler.go:33`). incident-provider and
  alert-provider poll every minute (incident-provider `main.go:43`,
  alert-provider `main.go:124`, `values.yaml:47`). git-provider and
  core-provider poll every 3 minutes (git-provider `main.go:37`, core-provider
  `main.go:66`).
- **Failure backoff saturates well inside it.** The controller's failure
  backoff starts at 1 s and caps at 60 s (`pkg/controller/options.go:159`).
- **A two-write change lands in milliseconds.** The context and the trigger
  are written back to back, so the window matters only for a value that never
  leads to an action.

It's an option, not a constant, because a provider polling much more slowly
(say every 30 minutes) needs a window longer than its poll. Only then does a
change that waits for the next poll still parent.

**How many passes join one upstream trace,** for each new value, before the
window closes. These are upper bounds, from arithmetic:

| What the passes are | How many in 10 minutes | Why |
|---|---|---|
| Observe-only polls | about 10 | at a 1-minute poll |
| Failing retries | about 15 | backoff 1, 2, 4 … 32 s, then 60 s each |
| At-capacity deferrals | about 40 | each requeues after a random 1 s to `RecoveryInterval` (default 30 s), with no backoff (`pkg/ratelimit/governor.go:21,72-75,554`) |

Each deferred pass is a Parent root with a `ratelimit.deferred` event and no
children. The deferral row is the bound that matters.

**Across a restart before the first action.** The first-sight time is in
memory, so a restart, a failover or a new replica starts the window again from
its own first sight. At worst, a pass after the restart still parents. That's
bounded by one more window per restart, and nothing grows without bound. The
marker is the durable part: once a pass has acted, every process links.

**What the rule guarantees, and what it doesn't.**
- **What it doesn't guarantee.** It raises the chance that the pass that does
  the work is the child of the context that caused it. It doesn't make it
  certain:
  - Another pass may act first, for an unrelated reason.
  - The window may close first.
  - A throttled hold lasts until the server's deadline, cut to `MaxHold`
    (default 1 hour, `governor.go:27`). That can outlast the window, so the
    acting pass after a long hold links.
  - A crash between the write and the act loses the parent (above).

  A pass that misses still links to the context, so a reader can get from it
  to the upstream trace.
- **Bounded growth.** For each new value, the trace gains at most the passes
  in the table, plus one acting pass, plus one more window per restart. It's
  never the open-ended growth of parenting every pass (UR today).
- **D3 still holds.** Only a new traceparent value opens a window and makes
  Parent passes. A generation change, a spec edit or a deletion with the same
  value is a Link pass, as decided. D6 changes only which pass consumes a new
  value, not what counts as new.

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
A marker patch on an object being deleted is allowed. If the object is gone,
the patch returns NotFound, which counts as success, and the memo entry is
dropped.

### 6.3 What the marker costs

- **Writes.** At most one marker write per new traceparent value, only on
  objects that carry `krateo.io/traceparent`, and only with tracing on. A
  value that never leads to an action costs its write when the window closes.
  The composition-dynamic-controller stamps a child only when its release
  really changes: the per-reconcile traceparent is stripped before the release
  digest
  (`go/composition-dynamic-controller/internal/tools/processor/processor.go:39-44,92-117`).
  So stamps arrive as often as upstream changes do.
- **The passes inside the window.** Up to the bounds in §6.2's table join the
  upstream trace for each new value. The at-capacity deferrals are the
  largest. For an object that acts on its first pass, the usual case for a
  composition's child, it's none.
- **One extra reconcile per write.** The patch changes the resourceVersion,
  and a watch event follows. The extra pass sees the marker match and runs in
  Link mode. That's one more Observe against the external API. A provider whose
  predicates already drop annotation-only updates doesn't pay it. alert-provider's
  `changed` lets annotation changes through (`internal/controllers/alert/alert.go:120-139`).
- **Turning tracing on is a burst.** No marker is written while tracing is
  off. So when it turns on, every annotated object's existing value is new at
  once. For each object:
  - up to a window of Parent passes, onto an upstream trace that may be days
    old, such as the composition controller's last apply;
  - then one marker write and one extra reconcile.

  All of it lands across the fleet within the first window after the rollout.
  The backend shows those spans as late additions to old traces. The
  adoption steps (§11.5) say to expect it.
- **The platform's "nothing stamped" rule.** The audit join was built so that
  nothing is stamped on the user's object (observability's
  `ops/audit-reconcile-join/README.md`, first paragraph). This marker is such a
  stamp, triggered by an edge. The user accepted it (decision D2, §13): it's
  the only option that survives restarts.

### 6.4 With tracing off

With no TracerProvider, `StartReconcile` starts no span, the reconciler
writes no marker and keeps no memo entry.
- **Inbound context, for `Inject` only.** When R is valid, it's put in `ctx`
  under a `pkg/tracing` key that only `Inject` and `InjectAnnotations` read.
  It isn't the active span. So a disabled provider still passes an upstream
  context on to the Krateo objects it writes, which is UR's reason
  (`go/composition-dynamic-controller/internal/composition/composition.go:495-507`).
- **Not on HTTP.** `InjectHeader` reads only the active span, so a disabled
  provider sends no `traceparent` on its HTTP calls.
- **Not in logs.** Its logs aren't stamped with a trace it took no part in,
  which is difference 4 in §2.

Turning tracing on later makes every annotated object's value new once (§6.3,
"Turning tracing on is a burst").

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
  pass a new value downstream, plus a watch event. The
  composition-dynamic-controller avoids exactly this by excluding the annotation
  from its digest (§6.3).
- **Never in a status write.** A status-subresource write keeps only `status`
  and drops metadata (`status_strategy.go:67-91`). When the change that matters
  is in status, the annotation has to go in a separate metadata write.
- **Stamp before the change that triggers the work.** When the context and
  the change that makes the downstream controller act are two writes, write
  the annotation **first**. The downstream pass that sees only the annotation
  doesn't act, so it stays a child and writes no marker (§6.2). The pass that
  sees the change acts and is a child too. In the other order, the acting pass
  sees the old value, which may already be continued, so it links. alert-provider's
  Open transition is the case (§11.3, step 5).
- **Never copy `krateo.io/traceparent-continued`.** It belongs to the object
  it's written on.

Where these rules are documented:
- the doc comments of `Inject` and `InjectAnnotations`;
- `docs/usage.md`, in a section on propagating to another controller, which
  lands with the implementation;
- `docs/api.md`, with the functions.

The reconciler never injects anything by itself, because providers mostly
don't write such objects.

### 6.6 Outbound HTTP

`httpsignal.Transport.RoundTrip` sets `traceparent`, and `tracestate` when
there is one, from the **active span** in `req.Context()`, through
`tracing.InjectHeader`. It never uses §6.4's inbound context. It does nothing
when:

- **There's no active span.** That's always the case with tracing off, so
  bumping provider-runtime alone changes no request. With tracing on, every
  request a reconcile makes through httpsignal carries the `traceparent` of
  `connect`, `observe`, `create`, `update` or `delete`. That's the behaviour
  change in §2's list.
- **The request already has a `traceparent`,** for example from an `otelhttp`
  transport wrapped outside it. That one is the more specific parent.
- **`Transport.NoTracePropagation` is true,** for a provider whose external
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
| The unresolved create handshake, inside its recovery window (`reconciler.go:1012-1016`): `ReconcileError` written, nil returned | False, `ReconcileError` | Error, with the condition's message | `RecordError` |
| A governor deferral (`ratelimit.go:180-199`) | False, `ReconcileDeferred` | Unset | `ratelimit.deferred` |
| A conflict requeue (`rateLimitedRequeue()` on `IsConflict`), the marker's included | unchanged | Unset | `k8s.conflict`, with its step (§4.5) |
| Paused (`reconciler.go:809-818`) | False, `ReconcilePaused` | Unset | `reconcile.paused` |
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
globals. Marker tests run against a fake client that records patches and
enforces the resourceVersion precondition. The lock tests (12, 13 and 21) run
on envtest, so the API server's own conflict handling is what's tested.

1. **Shape.** A pass that observes and updates makes exactly `reconcile`
   (Server), `observe` and `update` (Internal). The children's parent is the
   root, and the root has exactly the three attributes, with the Kind alone.
   A cluster-scoped object gets `k8s.object.namespace = ""`. The root's start
   is no later than the Get (§4.1).
2. **Each rule of §6.2.** Each case asserts the span's **mode**: its parent,
   whether it's a new root, and its links. Each also asserts the Decision's
   `WriteMarker`, and the writes the fake client saw.
   - **Rule 1:** no annotation is NoParent, with no link and no patch.
   - **Rule 4:** a valid annotation and no marker give a remote parent equal
     to R.
   - **Observe-only:** a Parent pass whose Observe reports up to date writes no
     marker. The next pass is a Parent pass again.
   - **The first action:** a Parent pass that updates writes exactly the
     marker, locked, and the fake client records it **before** `Update` is
     called. The child span `update` starts after the write, and `managed`
     keeps Observe's in-memory changes, with the new resourceVersion. On the
     create path, the locked patch comes just before the create-pending Update.
   - **A deferred action:** the governor defers the `Update`, so no marker is
     written.
   - **Rule 2:** run the same object again with the marker in place. It's a new
     root with one link to R, and makes no patch.
   - **A new value:** a new span ID gives Parent again.
   - **Generation only:** bump the generation with the same traceparent. It's
     Link, with no patch (D3). A deletion with the same traceparent is also Link.
   - **Invalid input:** an invalid traceparent, and an invalid marker.
   - **Flags or `tracestate` changed alone:** Link.
   - **An in-process parent span** in the incoming context is not the root's
     parent (difference 6).
3. **Window 0, and a zero `firstSeen`** (reviewer's 4).
   - `Decide` with `window = 0` returns Parent with `WriteMarker = Now`.
   - A zero `firstSeen` with a positive window returns Parent with
     `BeforeFirstAction`.
   - The span's mode matches in both. `WithTraceContinueWindow(0)` writes
     the marker on the first pass, after the pause check.
4. **The window closes.** With a fake clock, a value first seen `window` ago
   gives Link with `WriteMarker = Now`, and the marker is written after the
   pause check and before the gates.
5. **Restart.**
   - A second `Reconciler`, built from scratch, on the object the first one
     marked: Link. This is what "doesn't break across restarts" means, tested.
   - A second `Reconciler` on an object whose value was seen but not acted on:
     its own window starts, so it parents until it acts or its window closes.
6. **acmp's Analyzing → Open sequence,** on an Incident-shaped fake provider
   whose Observe acts only when a script is set, as incident-provider's
   `checkDue` does:
   1. Create with traceparent A, in Analyzing: an acting pass, which writes
      marker A.
   2. Write a fresh traceparent B (a metadata patch): the pass is a child of
      B and doesn't act, so there's no marker.
   3. Set Open with a script (a status write): the pass is a child of B, acts,
      and writes marker B before `Update`.
   4. The next poll links to B.

   The same sequence with the status write first: the Open pass links (to A)
   and acts. The test pins that the order in §6.5 is what makes the
   difference.
7. **A failed marker write.** The patch is forbidden: the pass completes,
   records `trace.marker_failed`, and the entry becomes `failed`, so the next
   pass in the same process links. A new process parents again until its next
   acting pass or the end of the window.
8. **A failed pass after acting.** Update fails after the marker is written,
   so the retry links. Observe failing before any action leaves the retry a
   Parent pass, until the window closes.
9. **Status mapping**, one test per row of §7.
   - It includes the deferral's event and its attributes, the handshake path,
     a panic (Error, and the panic still reaches controller-runtime), and Get
     NotFound (no span).
   - `k8s.conflict` is tested at every step in §4.5's list.
   - A source test lists every `SetConditions(...ReconcileError(...))` in
     `reconciler.go`, and fails when one is followed by a nil-error return
     without marking the root.
   - A second source test lists every `kerrors.IsConflict` in `reconciler.go`,
     and fails when one has no step.
10. **Log correlation.** A `NewSlogLogger` over `NewOTelJSONHandler` into a
    buffer: the "Reconciling" record and a later reconciler record carry the
    root's `trace_id` and `span_id`. A provider Observe that logs through
    `logging.WithContext(log, ctx)` carries `observe`'s span ID. With tracing
    off, no record carries a `trace_id`, even with an annotation (§6.4).
11. **Off is a no-op.**
    - No provider and no option set: a recorder registered on the global
      records nothing, and the fake client sees no patch.
    - `testing.AllocsPerRun` on `StartReconcile` with a nil provider and an
      object without the annotation is 0.
    - With the annotation, `Inject` propagates R, and `InjectHeader` and the
      transport send **no** header.
12. **A concurrent writer between the Get and the marker patch** (reviewer's
    1). Another client `Status().Patch`es the object after the pass's Get and
    before its marker patch. The locked patch conflicts, and the pass requeues
    before acting, with `k8s.conflict` and step `marker`. The other writer's
    status survives, and the next pass is Parent.
13. **A stale-cache second pass** (reviewer's 2).
    - Pass 1 marks. Pass 2 runs on a cached object that doesn't show the
      marker yet. The `marked` memo entry makes pass 2 Link, and it doesn't act
      as a child.
    - The same with the memo cleared, as after a restart, against an object
      whose resourceVersion has moved on: the locked patch conflicts, and
      nothing acts twice.
14. **A fresh B survives the post-Create critical write** (reviewer's 3).
    - Another client writes traceparent B while `Create` runs. After the
      critical write (`reconciler.go:1228`, `:1268`), the object still carries
      B, not the A the pass read.
    - A third annotation written meanwhile survives too.
    - The same through the handshake adoption (`:989`).
15. **Tracing off, past the window** (reviewer's 5): an annotated object
    reconciled for longer than the window with tracing off. No patch, no memo
    entry, no header.
16. **NotFound, and delete and recreate** (reviewer's 6).
    - NotFound drops the entry.
    - Finalizer removal drops it.
    - An object recreated under the same name, with a new UID, starts with no
      entry and is decided afresh.
17. **A deferral storm** (reviewer's 7). With the governor at capacity, the
    deferred passes inside the window are Parent roots with
    `ratelimit.deferred` and no marker. Their number is within §6.2's bound,
    and the first pass after the window links and writes the marker.
18. **A paused object past the window** (reviewer's 8): no marker write while
    it's paused, and the pass after it's unpaused writes it.
19. **A marker conflict** (reviewer's 9): rules 3 and 5's `Now` write and the
    `BeforeFirstAction` write each conflict, each requeues before acting with
    step `marker`, and each leaves the entry `seen`.
20. **A create-pending write rejected only because of the marker**
    (reviewer's 10). The locked marker patch conflicts, so the pass requeues
    before create-pending is written. No unresolved create handshake is left
    behind, and the next pass creates.
21. **The protocol check with tracing off** (reviewer's 11). With
    `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`, Setup succeeds when only metrics are
    on, and fails when tracing is on.
22. **Inject.** It overwrites both keys, removes a stale `tracestate`, does
    nothing without a context, and never writes the marker key.
23. **Transport.** Against an `httptest.Server`:
    - `traceparent` matches the active span, and there's no header without
      one, including with only an inbound context.
    - An existing header is kept, and `NoTracePropagation` sends none.
    - There's never a baggage header.
    - The caller's request is not changed.
    - The literal form gets propagation too.
24. **Setup.** Tracing off returns a working no-op shutdown and sets no
    provider. On, it sets both providers without dialling, because
    `otlptracehttp.New` doesn't connect. The resource table of §5, including
    env precedence, extends `resource_test.go`. Metrics and traces get an
    equal resource. Shutdown exports spans ended before it, through a real
    batcher over an in-memory exporter. That's the drain test.
25. **Concurrency** (as required for concurrent code). N workers reconcile
    distinct objects at once under `-race`. Every span's parent and link
    belong to its own object. The memo stays consistent while objects are
    deleted, recreated and restamped.

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
- **Expect a burst when it turns on** (§6.3). For every git-provider object
  that carries a `krateo.io/traceparent`, there's up to a window of Parent
  passes onto the last stamper's trace, then a marker write and an extra
  reconcile, all within the first 10 minutes.
- **RBAC.** It already has `patch` on its kinds
  (`templates/clusterrole.yaml:13-23`), which the marker needs.
- **Outbound.** Propagation comes with the bump, through httpsignal (§6.6).

### 11.2 incident-provider

- **Bump** from v1.8.2. That's several minors: the governor, the gated queue
  and the Ready contract came in between (`docs/log.md`).
- **Main and chart** as git-provider, with `OTEL_SERVICE_NAME=incident-provider` (§11.5). Its Logger is already slog over
  `NewOTelJSONHandler` (`main.go:63-64`), so logs correlate since v1.13.0 (#58).
- **Inject** nothing. It writes Pods and ConfigMaps
  (`internal/controllers/incident/incident.go:360-368`, `apply.go:384-395`),
  which no Krateo controller reconciles.
- **Outbound.** It makes no HTTP calls to propagate on.
- **Its place in the plan.** alert-provider creates the `Incident` objects
  that incident-provider reconciles (§11.3). With alert-provider's `Inject`,
  incident-provider's first acting pass on a new `Incident` is a child of
  alert-provider's `incident.fire`. The fresh traceparent stamped before Open
  makes the remediation pass a child of `incident.analyze` (§11.3, step 5).
  Later passes link. That's the alert → RCA → remediation trace.

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

   **(b) and (c) are a metadata patch, written before the Open status write.**
   - **Why a separate write.** `finish` writes through `patchStatus` and then
     `kube.PatchStatus`, which is `client.Status().Patch` (`kube.go:115-120`).
     The Incident CRD has the status subresource (incident-provider's
     `observability.krateo.io_incidents.yaml:495-496`,
     `apis/incident/v1alpha1/types.go:405`). A status write drops metadata
     (§6.5), and a main-resource write drops status
     (`customresource/strategy.go:166-176`).
   - **The patch.** It's `kube.Patch` (`kube.go:123-128`) with
     `prtracing.InjectAnnotations` from the `incident.analyze` span, written
     **before** the status write that sets Open and the scripts (§6.5's
     writer rule).
   - **What incident-provider does with it, under D6:**
     - **The patch's pass** sees the fresh value, B, while the Incident is still Analyzing. It
       has no current script, so no check is due (`machine.go:175-177`). It
       doesn't act, it's a child of B, and it writes no marker.
     - **The Open pass** sees B still not continued. It marks a check due,
       writes the marker, then calls `Update`, which starts the check Pod. So
       the remediation is a child of `incident.analyze`, in the trace that the
       `^incident\.` rule keeps whole.
     - **If both events land before a worker takes the key,** one pass sees
       both and is the same Parent pass.
   - **The window.** It doesn't come into play: the patch and the status write
     are milliseconds apart. If a pass acts on the Analyzing Incident for
     another reason first, for example a human decision in spec that Observe
     applies, B is continued there and the Open pass links. That's the case
     §6.2 doesn't guarantee, and the link still leads back to B.

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
- **The burst at rollout** (§6.3). When the chart turns tracing on, every
  annotated object of that provider has a new value. observability sees:
  - Parent passes onto possibly days-old upstream traces, for up to a window;
  - then one marker write and one extra reconcile per object.

  They're spread over the first window. Under `sample-5pct` most of those
  passes aren't stored, and the ones that fail are.
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
- **Continuing on the first pass that sees a value** (this design before D6).
  When the context and the trigger arrive in two writes, as alert-provider's
  status-carried Open does, the pass that does the work is not reliably the
  child (§11.3, step 5).
- **A window without a marker.** It would close only in memory, so every
  restart would reopen it for every annotated object.
- **An unconditioned marker patch.** Copying its resourceVersion onto
  `managed` switches optimistic concurrency off for the rest of the pass
  (§6.2, "The write, with an optimistic lock").
- **The marker folded into the create-pending Update.** It saves a write, but
  a rejection caused only by the marker blocks the create (§6.2).
- **Leaving the critical write as it is.** It reverts a fresh traceparent
  written during `Create`, and every other writer's annotations (§6.2).
- **Sending the inbound context over HTTP with tracing off.** A disabled
  provider would forward a trace header it took no part in (§6.4).
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
only with tracing on (§6.3). The review on #60 added two things:
- **A burst at rollout.** Because no marker is written while off, turning
  tracing on makes every annotated object's value new at once. That's a
  fleet-wide burst of marker writes and extra reconciles, plus Parent passes
  onto possibly days-old traces, within the first window (§6.3).
- **The patch is locked.** Every marker write uses an optimistic lock, and a
  conflict requeues before acting (§6.2). Rejected:
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

**D6. A new value is continued by the first pass that acts, within a window.**
This was option (a) of the question the review on #60 raised: how an
Incident's remediation joins the RCA's trace.
- **The problem.** alert-provider's Open is a status write, so a fresh
  traceparent has to arrive in a separate metadata write (§11.3, step 5).
  Under the first-sight rule, the pass that starts the check Pod wasn't
  reliably the child.
- **The decision.**
  - The marker is written just before a Parent pass's first Create, Update or
    Delete call.
  - Observe-only passes stay children and write nothing.
  - If no pass acts within the window, measured in memory from this process's
    first sight, default 10 minutes, the marker is written anyway and later
    passes link.
  - Writers stamp before the change that triggers the work.

  §6.2, §6.3, §6.5, §10 and §11.3 specify it. After the independent review
  on #60, §6.2 also specifies:
  - the optimistic lock on every marker write;
  - the memo's keys, states, locking and growth;
  - the deferral bound;
  - what "acts" covers;
  - the crash gap.
- **D3 still holds:** only a new value parents (§6.2).
- **Rejected:**
  - (b) Moving incident-provider's trigger out of status, so one write
    carries both. That's an API change in two other repos.
  - (c) Accepting a link. The remediation's own root is kept only 5% of the
    time when healthy (D4), so it would usually not be stored.

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
- **For the independent review's fixes:**
  - controller-runtime v0.25.2's `MergeFromWithOptimisticLock`;
  - provider-runtime's code at `3ab2917`: the critical updater's
    capture-everything write (`pkg/reconciler/api.go:41-51`), every
    `IsConflict` site and acting call in `reconciler.go`, and the governor's
    at-capacity jitter and `MaxHold` default (`governor.go:21,27,72-75,554`);
  - that OTel's default global propagator is a no-op
    (`API/internal/global/propagator.go:29,55`);
  - that `otlptracehttp` isn't in `go.mod`.
- **What alert-provider does in Observe:** it starts its firings there
  (`alert.go:442-473`).
- **The consumers:** every consumer file cited in §11, observability's join,
  Service and tail-sampler files, and #58 as merged in v1.13.0.

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
