---
type: Architecture
title: provider-runtime — trace every reconcile
description: Design, built in v1.14.0 with D8 in v1.14.1, for OpenTelemetry tracing in provider-runtime. telemetry.Setup gains a TracerProvider, the reconciler opens a reconcile root span with connect, observe, create, update and delete children, and the krateo.io/traceparent contract gets a parent-or-link rule for long-lived objects. Trace context is propagated on outbound HTTP calls and onto Kubernetes objects that other Krateo controllers reconcile.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, tracing, opentelemetry, reconciler, propagation]
timestamp: 2026-11-10T00:00:00Z
---

# Trace every reconcile

**Status:** built in v1.14.0 (#64, #65, #66); D8 changes it in v1.14.1. It
builds on [#58](https://github.com/krateo-platformops/provider-runtime/pull/58)
(`logging.ContextLogger`, `logging.WithContext`). That's merged, and it
shipped in v1.13.0 (`3ab2917`). The user decided its open questions on
2026-11-09, and §13 records the decisions (D1 to D8). D6 came from the review
on #60, and D7, the OTLP endpoint, from core-provider's review of it. D8, on
2026-11-10, came from krateo-057's first day under v1.14.0: every reconcile is
its own trace, and only the first act for a new value joins the upstream.

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
   (`tracing.go:68-76`). In provider-runtime every reconcile is its own trace,
   linked to the annotation's value. Only the first act step for a new value,
   the `create`, `update` or `delete` span, is its child (§6.2, D6 and D8).
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
6. **The root never takes a parent.** If something wrapped the reconciler in
   its own span, the `reconcile` span would still be a root. It's never a child
   of the annotation's remote span either (D8). That keeps the parent-or-link
   rule exact (§6.2) and the audit join's assumption true (§11.3).

**Changes to code that exists today.** Besides the new code, four things
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
    to persist it would lose it. **No consumer does.** I scanned every
    non-archived go.mod in krateo-platformops, root and nested, that requires
    provider-runtime under either module path. All of them use the new path,
    `github.com/krateo-platformops/provider-runtime`; none uses the old
    `krateoplatformops` path. The repos still on the old module path live in
    the old `krateoplatformops` org. That org's `provider-runtime` stops at
    v1.2.1, still under the old module path, so they can't take this change
    without rewriting the module path. For each consumer in
    krateo-platformops, I checked whether it sets annotations on a managed
    resource:

    | Consumer (go.mod) | provider-runtime | Uses the reconciler | Annotations it writes |
    |---|---|---|---|
    | alert-provider | v1.13.0 | yes | none on its managed resources. `Alert.SetAnnotations` only keeps `krateo.io/paused` through an accessor (`apis/alert/v1alpha1/types.go:256-267`) |
    | git-provider | v1.11.0 | yes | none |
    | incident-provider | v1.8.2 | yes | none |
    | access-review | v1.8.2 | yes | none |
    | identity-lifecycle | v1.8.2 | yes | none |
    | oasgen-provider (`go/oasgen-provider`) | v1.8.2 | yes | only on CRDs, Deployment pod templates and policies, never on its managed resource, and through its own writes (`internal/tools/crd/apply.go:228-230`, `deployment/deployment.go:32-34`, `policy/policy.go:244-245`) |
    | core-provider (`go/core-provider`) | v1.7.2 | yes | only on compositions, through its own dynamic-client writes (`helpers/getters/getters.go:273-299,323-345`) |
    | core-provider (`go/testprovider`) | v1.8.2 | yes | none |
    | core-provider (`go/chart-inspector`) | v1.7.2 | no: `apis/common/v1` only | none |
    | cmp-usage-collector | v1.7.0 | no: `pkg/logging` only | only on Secrets, through its own clientset writes (`internal/child/child.go:139-145`) |

    The reviewer found the same for access-review and identity-lifecycle.
- **`telemetry.Setup` sets the global propagator on every call** (§3.1).
  OTel's default global propagator is a no-op
  (`API/internal/global/propagator.go:29,55`). So a provider's own `otelhttp`
  starts extracting and injecting W3C context and baggage once it calls Setup,
  tracing on or off. core-provider and oasgen-provider already set the same
  propagator themselves, tracing on or off (core-provider `tracing.go:32`,
  oasgen-provider `go/oasgen-provider/internal/tools/telemetry/tracing.go:31`),
  so they see no change. The re-review on #60 found the change has no
  observable effect on today's consumers.
- **httpsignal sets `traceparent` on outbound requests when tracing is on**
  (§6.6). With tracing off, it sends nothing.
- **With tracing on, a concurrent write requeues before acting** (§6.2, "The
  write, with an optimistic lock").
  - **Today,** if another writer changes the object between the pass's Get and
    its status Update, the pass still acts on the external system, and then
    its status Update fails on the resourceVersion.
  - **With tracing on,** a Parent pass on the update or delete path catches the
    same write at the locked marker patch, and requeues **before** acting. It
    backs off at 1 s, 2 s, 4 s and so on, up to 60 s
    (`pkg/controller/options.go:159`).
  - **For an object written more often than one Connect and Observe last,**
    the act can slip until the window closes. Then rule 5's write, right after
    the Get, has almost no gap to conflict in, and the act runs in a Link
    pass. It's bounded by the window, and it matches the create path, which
    already conflicts on the create-pending Update before `Create`
    (`reconciler.go:1190-1194`).

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
  `HTTP/`. So when `TracingEnabled` is true, Setup returns an error if the
  traces protocol is set to anything other than `http/protobuf`. It follows
  OTel's precedence: `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` when it is set,
  otherwise `OTEL_EXPORTER_OTLP_PROTOCOL` (corrected in implementation review). Without that check, protobuf would go
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
- **The exception: the joined act (D8).** The first act step for a new value
  isn't the root's child: its parent is the upstream span (§6.2). Its root is
  in another trace, and under `sample-5pct` usually isn't stored. So it carries
  the root's three attributes itself: `k8s.object.kind`, `k8s.object.name` and
  `k8s.object.namespace`. The root's own contract, exactly those three, is
  unchanged.
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
   **Parent.** The root is a new root with one link to R, as in Link mode.
   What differs is that the pass's first act step is R's child, and the root
   links to it too (below).
   `Marker` is `canonical(R)`, and `WriteMarker` is `BeforeFirstAction`.
5. R isn't continued, and `now - firstSeen >= window`: **Link,** with
   `WriteMarker` set to `Now` (decision D6, §13).

The reconciler computes the Decision before it starts the span, from its memo
and `Decide`, and passes it to `StartReconcile` (§3.2). So the mode a test
asserts is the mode the span gets.

**Every reconcile is its own trace (D8).** In every mode, `StartReconcile`
starts the root with `trace.WithNewRoot()`. When R is valid (Parent and Link),
the root carries one link, `trace.Link{SpanContext: R}`, plus one to its act on
the first acting pass; with NoParent it has none. The root is never R's child.
In a Parent pass, the first act step is started by `tracing.StartActStep`:
- its parent is R (`trace.ContextWithRemoteSpanContext`), so it's in R's
  trace;
- it carries one link back to its `reconcile` root;
- it carries the root's three `k8s.object.*` attributes (§4.4);
- the root adds a link to it (`Span.AddLink`), so the link goes both ways. The
  root adds it only when the act is sampled: an act that isn't is never
  exported (§8);
- it's `SpanKindInternal`, named `create`, `update` or `delete`, as before.

What the provider does under it with the method's `ctx` (its own spans,
httpsignal's `traceparent`) is in R's trace too. Every other span of the pass,
`connect`, `observe` and any later act step, is a child of the root. So a
pass's spans are in two traces only when the pass is the first to act on a new
value:

```
new trace                                 R's trace (krateo.io/traceparent)
reconcile  Server                         R  (the stamper's span, remote)
  links -> R, -> update                   `-- update  Internal, link -> reconcile
|-- connect  Internal                         k8s.object.kind/name/namespace
`-- observe  Internal                         `-- what the provider's Update does with ctx
```

Every other pass is one trace: the root, with exactly one link to R when R is
valid, and all its children under it.

**Which span is the child.** The first act step of the pass that **first
acts** on a new value is R's child: not the pass that first sees the value
(D6), and not the pass's root (D8).
- **What "acts" means.** A pass acts when the runtime calls the provider's
  `Create`, `Update` or `Delete`. That's the runtime's calls only: it can't see
  what a provider does inside Observe. alert-provider starts incident firings
  from Observe (`internal/controllers/alert/alert.go:442-473`), and those don't
  count. Under D8, Observe-time work like that never joins the upstream
  trace: it's in the pass's own root, which links to R.
- **A pass that only connects and observes** is its own trace, linked to R,
  and writes nothing, so the next pass is still a Parent pass. That's what lets
  a change that arrives in two writes, the context first and the trigger after,
  put the work in the context's trace (§11.3, step 5).

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
  the new resourceVersion and the marker onto `managed`.
  - **Why that's safe.** The lock guarantees that the stored object is the one
    the pass read plus the marker. So `managed` with the new version is still
    exactly that object, plus the pass's own changes.
  - **The exception is a mutating admission webhook,** which can change other
    fields of the object as it admits the patch. `managed` doesn't see those
    changes. That's harmless: every later write in the pass goes through
    admission again, so the webhook applies its change again.
  - **Later writes stay version-checked,** including the status Update after
    each acting call (`reconciler.go:1380`, `:1121`), and the provider's own
    writes. The generic store refuses an update whose resourceVersion isn't
    the stored one
    (`apiserver@v0.37.2/pkg/registry/generic/registry/store.go:807-808`).
    The status subresource goes through the same store, after copying in the
    old object (`status_strategy.go:79-81`).
- **Why the lock.** Without it, an unconditioned patch would succeed against
  whatever the object had become, and copying its version onto `managed`
  would switch optimistic concurrency off for the rest of the pass. Say
  alert-provider's reanalysis `Status().Patch`es new scripts between
  incident-provider's Get and its marker patch. incident-provider's
  `Status().Update(managed)` would then silently overwrite them. A pass that
  read a stale cache would also act a second time, with its act in R's trace.
- **On a conflict:** `rateLimitedRequeue()`, before the pass acts, with
  `k8s.conflict` and step `marker`. The memo entry stays as it was, so the next
  pass reads the object afresh and, if the value is still new, is a Parent pass
  again. D6 holds.

**When each write happens.**
- **`BeforeFirstAction`:** just before the first `Create`, `Update` or
  `Delete` call, after that call's `gate` lets it through, and before its act
  step, R's child, starts. A pass the governor defers doesn't act and doesn't
  write.
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
pass is retried with backoff, the retries link, and their acts are children of
their own roots. With the marker written at the end, a permanently failing
object would put every retry's act into the upstream trace, forever.

**A crash between the marker write and the act** loses the join. The next
pass sees the marker, links, and does the work in its own trace. That's the
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
  (RBAC, an admission webhook's Forbidden or Invalid). **The pass goes on:**
  it records `trace.marker_failed`, logs once per object, and acts without
  the marker. On the create path, it writes create-pending without the marker
  and creates. Later passes in this process link through `remembered`.
  Without it, an object whose marker can never be written would put every
  pass's act into the upstream trace. After a restart, such an object's next
  act joins again, once, unless the window closes first.
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

**The window: the fallback that writes the marker when no pass acts.** Under
D8 it parents nothing: the passes inside it are their own traces. It exists so
that a value that never leads to an action is continued anyway. Without it,
every pass would stay a Parent pass, forever, and an act much later, for an
unrelated reason, would join a stale trace.
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
change that waits for the next poll still join.

**What joins one upstream trace,** for each new value: one act step, the
first, and what runs under it. The passes inside the window don't (D8). In
v1.14.0, under D6 as first built, every pass inside the window was R's child.
These were the upper bounds then, from arithmetic, and they're what D8
removes:

| What the passes are | How many in 10 minutes | Why |
|---|---|---|
| Observe-only polls | about 10 | at a 1-minute poll |
| Failing retries | about 15 | backoff 1, 2, 4 … 32 s, then 60 s each |
| At-capacity deferrals | about 40 | each requeues after a random 1 s to `RecoveryInterval` (default 30 s), with no backoff (`pkg/ratelimit/governor.go:21,72-75,554`) |

Under D8, each deferred pass is its own trace: a root with one link to R, a
`ratelimit.deferred` event and no children.

**Across a restart before the first action.** The first-sight time is in
memory, so a restart, a failover or a new replica starts the window again from
its own first sight. At worst, an act after the restart still joins: one act
step per restart, and nothing grows without bound. The marker is the durable
part: once a pass has acted, every process links.

**What the rule guarantees, and what it doesn't.**
- **What it doesn't guarantee.** It raises the chance that the act that does
  the work is the child of the context that caused it. It doesn't make it
  certain:
  - Another pass may act first, for an unrelated reason.
  - The window may close first.
  - A throttled hold lasts until the server's deadline, cut to `MaxHold`
    (default 1 hour, `governor.go:27`). That can outlast the window, so the
    act after a long hold isn't R's child.
  - A crash between the write and the act loses the join (above).

  Every pass's root links to the context, so a reader can get from it to the
  upstream trace.
- **Bounded growth.** For each new value, the upstream trace gains one act
  step and what runs under it, plus one more per restart while the marker
  isn't on the object. Polls, retries and deferrals add nothing to it. It's
  never the open-ended growth of parenting every pass (UR today).
- **D3 still holds.** Only a new traceparent value opens a window and makes
  Parent passes. A generation change, a spec edit or a deletion with the same
  value is a Link pass, as decided. D6 and D8 change only which span consumes
  a new value, not what counts as new.

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
  - **How often the composition-dynamic-controller stamps.** It strips the
    per-reconcile traceparent from the manifest before computing the release
    digest, so a composition reconcile with no content change doesn't upgrade
    the release
    (`go/composition-dynamic-controller/internal/tools/processor/processor.go:39-44,92-117`).
    The objects themselves keep it, though: each apply stamps every object in
    the release with that reconcile's traceparent, whether the object changed
    or not. On krateo-057, observability saw every object's value come from
    the 07:36:11Z roll (a cluster fact, as observability reports it).
  - **So every chart upgrade gives every child a new value.** Each child then
    costs one marker write and one extra reconcile, within the window.
- **What joins the upstream trace.** One act step per new value, and only if a
  pass acts on it (D8). Observe-only polls, failing retries and deferrals
  inside the window don't. Under D6 as v1.14.0 built it, they did, and on
  krateo-057 that was the cost that mattered: each chart upgrade put about 260
  observe-only reconciles into the upgrade's trace, which the tail sampler
  kept whole (§13, D8).
- **One extra reconcile per write.** The patch changes the resourceVersion,
  and a watch event follows. The extra pass sees the marker match and runs in
  Link mode. That's one more Observe against the external API. A provider whose
  predicates already drop annotation-only updates doesn't pay it. alert-provider's
  `changed` lets annotation changes through (`internal/controllers/alert/alert.go:120-139`).
- **Turning tracing on is a burst.** No marker is written while tracing is
  off. So when it turns on, every annotated object's existing value is new at
  once. For each object:
  - if a pass acts within the window, its act step joins an upstream trace
    that may be days old, such as the composition controller's last apply;
  - one marker write and one extra reconcile.

  All of it lands across the fleet within the first window after the rollout.
  The backend shows those act steps as late additions to old traces. The
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
  doesn't act, so it writes no marker and adds nothing to the upstream trace
  (§6.2). The pass that sees the change acts, and its act step is the child.
  In the other order, the acting pass sees the old value, which may already be
  continued, so it only links. alert-provider's
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
  (`provider.go:518-521`), and `ParentBased` drops a span whose remote parent
  isn't sampled (`sampling.go:182,200`).
  - **Under `parentbased_always_on`** every reconcile root is sampled, and the
    joined act span follows the stamp's sampled flag. A `00` stamp drops the
    act, what the provider does under it, and its link back to the root, and
    the root's own trace has no act either.
  - **Krateo's own stamps are sampled** (`01`): observability checked them on
    krateo-057.
  - **A ratio sampler** would drop roots at random, and with them any act
    under a stamp it dropped upstream.
- **core-provider** sets no sampler in code (`tracing.go:42-45`). No
  `OTEL_TRACES_SAMPLER` appears anywhere in its repo outside tests, charts
  included. So it runs the SDK default, which is `parentbased_always_on`.
  alert-provider sets `OTEL_TRACES_SAMPLER=parentbased_always_on` explicitly
  (`helm/alert-provider/values.yaml:65-67`).
- **The rule for charts.** Charts set `OTEL_TRACES_SAMPLER` to
  `parentbased_always_on`, or leave it unset, which is the same. `always_on` is
  allowed and also records a first act under an unsampled stamp. Never a ratio
  sampler. Setting it explicitly keeps the value visible.
- **The endpoint: `http://$(HOST_IP):4318` by default** (decision D7, §13).
  `HOST_IP` comes from the downward API (`fieldRef: status.hostIP`,
  `k8s.io/api@v0.37.2/core/v1/types.go:2671-2672`), in the provider
  Deployment's `env`. That's the same standard as core-provider's cdc, which
  the user also decided, and what alert-provider already uses
  (`templates/deployment.yaml:225-232`).
  - **It reaches the node's agent on every daemonset version in use.** The
    daemonset exposes hostPort 4318: it's on by default in the upstream
    opentelemetry-collector chart 0.170.0 (`values.yaml:425-430`, which I
    read; `_config.tpl:481-482`, per observability's check). observability's
    wrapper uses that chart version (`charts/otel-collector-daemonset/Chart.yaml:132-134`)
    and doesn't override the ports. observability checked from chart source
    that this holds for the versions live today, 0.1.19 and 0.1.23.
  - **The Service needs no name.** The default needs no Service name, so it
    doesn't depend on the collector's release name.
  - **An explicit `otel.endpoint` wins.** The chart renders the default only
    when its explicit `otel.endpoint` value is empty, so a `-local` URL, or any
    other, can still be set.

  **Why `-local` is only an override.**
  - **It isn't on every cluster.** The node-local Service
    `<release>-local.<namespace>.svc:4318` exists only from
    otel-collector-daemonset 0.1.21 (observability `e9fe3ea`, #123: version
    0.1.21 in its `Chart.yaml`, against 0.1.20 in the commit before it).
    observability reports demo runs 0.1.19, which I can't check.
  - **Its name depends on the release.** It derives from the collector's Helm
    release name (`templates/service-local.yaml`).
  - **It's for senders that can't use the downward API,** such as the
    composition-dynamic-controller. The cdc reads its endpoint from a
    ConfigMap through `envFrom`, which can't carry a `fieldRef`. That's why
    observability added the Service at all (`service-local.yaml`, its header
    comment). A provider Deployment can use `env`.
  - **On krateo-057,** observability verified the `-local` Service, release
    `otel-collector-daemonset` with 4 of 4 node endpoints, in its review on
    #60. installer sets it explicitly there for the composition controllers.
    core-provider's chart falls back to the headless
    `otel-collector-daemonset-opentelemetry-collector` Service when
    `otel.endpoint` is empty (`helm/core-provider/templates/configmap.yaml:13`),
    and that Service isn't node-local.

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
  `OTEL_SPAN_*_LIMIT` still applies (`:94-115`). A root has at most two
  links, to R and to its joined act (D8), and a few events. The 4 KiB cut on error text is §7's.
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
2. **Each rule of §6.2.** Each case asserts the span's **mode**: the root
   has no parent in any mode (D8), and it has one link to R in Parent and Link
   mode (plus one to its act on the first acting pass) and none with
   NoParent. Each also asserts the Decision's
   `WriteMarker`, and the writes the fake client saw.
   - **Rule 1:** no annotation is NoParent, with no link and no patch.
   - **Rule 4:** a valid annotation and no marker give a Parent pass: a new
     root linked to R.
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
   2. Write a fresh traceparent B (a metadata patch): the pass is a Parent
      pass, its own trace linked to B, and doesn't act, so there's no marker
      and nothing in B's trace.
   3. Set Open with a script (a status write): the pass is a Parent pass,
      writes marker B, then acts. Its `update` span is B's child (D8).
   4. The next poll links to B, and nothing more joins B's trace.

   The same sequence with the status write first: the Open pass links (to A)
   and acts. The test pins that the order in §6.5 is what makes the
   difference.
7. **A failed marker write.** The patch is forbidden: the pass completes,
   records `trace.marker_failed`, and the entry becomes `failed`, so the next
   pass in the same process links. In a new process, the next act joins
   again, once, unless the window closes first.
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
    deferred passes inside the window are Parent passes, each its own trace,
    with `ratelimit.deferred` and no marker. Their number is within §6.2's bound,
    and the first pass after the window links and writes the marker.
18. **A paused object past the window** (reviewer's 8): no marker write while
    it's paused, and the pass after it's unpaused writes it.
19. **A marker conflict** (reviewer's 9): rules 3 and 5's `Now` write and the
    `BeforeFirstAction` write each conflict, each requeues before acting with
    step `marker`, and each leaves the entry `seen`.
20. **Admission rejects the marker patch** (reviewer's 10, as re-reviewed).
    - A validating webhook, or a fake client, refuses the marker patch with
      Forbidden, and in a second case with Invalid.
    - The pass records `trace.marker_failed`, and the entry becomes `failed`.
    - create-pending is written without the marker, the create proceeds, and no
      unresolved create handshake is left behind.
    - The next pass links through `remembered`, and writes no marker.
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
26. **D8: one reconcile, one trace** (`tracing_d8_test.go`).
    - In every mode in which the pass doesn't act (NoParent, a new value
      observed only, Link, window 0, paused, deferred, a marker conflict),
      the root has no parent, every span of the pass is in the root's trace,
      and nothing is in R's.
    - The first act for a new value, on the update, create and delete paths,
      is R's child with one link to its root, after the marker. It carries the
      root's three `k8s.object.*` attributes, and the root links to it. `connect`
      and `observe` stay in the root's trace. Every other pass's root has
      exactly one link.
    - An unsampled stamp (`00`) under the default sampler: the root is
      recorded and sampled with its link to R, and the act isn't recorded, so
      there's no link either way and nothing in R's trace. Under `AlwaysSample`
      the act is recorded, with both links.
    - A second act, after the marker, is a child of its root. So is the act
      after the window's fallback write. Within one pass, only the first act
      step joins.
    - Observe-only passes inside the window put nothing in R's trace, and each
      is its own trace.
    - Window 0 writes the marker before Connect, and the pass's act still
      joins. A failed (Forbidden) marker write still lets the first act join,
      and the next act doesn't.
    - Test 6's sequence: the Open pass's `update` is B's child, and the
      Analyzing pass and the poll after Open add nothing to B's trace.
    - **The real path,** on envtest: a client that reads through an informer
      cache, the default finalizer and the default critical updater. The
      marker reaches the API server, once written before the first act and
      once by the window's fallback. That's regression coverage for the
      krateo-057 report, which wasn't a marker bug (§13, D8).

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
  - `OTEL_EXPORTER_OTLP_ENDPOINT`, only when the chart's explicit
    `otel.endpoint` is set. Otherwise the Deployment's `env` renders the
    default (below).
  - `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
  - `OTEL_TRACES_SAMPLER=parentbased_always_on`
  - `OTEL_SERVICE_NAME=git-provider`, set explicitly (§11.5)
  - `SERVICE_VERSION`
  - `DEPLOYMENT_NAME: {{ include "git-provider.fullname" . }}`
  - `OTEL_ENABLED`, only if it also wants metrics.
- **The endpoint, in the Deployment's `env`** (D7, §8). git-provider loads
  its ConfigMap with `envFrom` (`templates/deployment.yaml:35-37`). `env` wins
  over `envFrom` for the same key (`k8s.io/api@v0.37.2/core/v1/types.go:3176-3180`),
  so the default is rendered only when `otel.endpoint` is empty. That keeps an
  explicit value in the ConfigMap from being silently overridden:

  ```yaml
  env:
    {{- if and .Values.otel.tracing (not .Values.otel.endpoint) }}
    # HOST_IP first: Kubernetes expands $(HOST_IP) only from a variable declared before it.
    - name: HOST_IP
      valueFrom: { fieldRef: { fieldPath: status.hostIP } }
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: "http://$(HOST_IP):4318"
    {{- end }}
  ```
- **Egress.** If the chart adds an egress NetworkPolicy, the policy must
  allow node IPs on TCP 4318, and the CNI must support `hostPort`. GKE does.
  git-provider's chart ships no NetworkPolicy today: its templates are
  `clusterrole`, `clusterrolebinding`, `configmap`, `deployment`, `hpa` and
  `serviceaccount`.
- **Before `OTEL_TRACING_ENABLED=true`:** observability lists its `service.name`
  in the tail sampler (§11.5).
- **Expect a burst when it turns on** (§6.3). For every git-provider object
  that carries a `krateo.io/traceparent`, there's a marker write and an extra
  reconcile within the first 10 minutes, and, if a pass acts in that time, one
  act step in the last stamper's trace.
- **RBAC.** It already has `patch` on its kinds
  (`templates/clusterrole.yaml:13-23`), which the marker needs.
- **Outbound.** Propagation comes with the bump, through httpsignal (§6.6).

### 11.2 incident-provider

- **Bump** from v1.8.2. That's several minors: the governor, the gated queue
  and the Ready contract came in between (`docs/log.md`).
- **Main and chart** as git-provider, with `OTEL_SERVICE_NAME=incident-provider`
  (§11.5). That includes the `HOST_IP` endpoint in the Deployment's `env`,
  the `otel.endpoint` override rule and the egress caveat.
  - **Its one NetworkPolicy** (`templates/checks.yaml`, from line 90) selects
    only the check Pods, `app.kubernetes.io/component: incident-check`. So the
    controller's egress to 4318 isn't restricted today.
  - **If a policy ever selects the controller,** it must allow node IPs on
    4318. Its Logger is already slog over
  `NewOTelJSONHandler` (`main.go:63-64`), so logs correlate since v1.13.0 (#58).
- **Inject** nothing. It writes Pods and ConfigMaps
  (`internal/controllers/incident/incident.go:360-368`, `apply.go:384-395`),
  which no Krateo controller reconciles.
- **Outbound.** It makes no HTTP calls to propagate on.
- **Its place in the plan.** alert-provider creates the `Incident` objects
  that incident-provider reconciles (§11.3). With alert-provider's `Inject`,
  the first act on a new `Incident` is a child of alert-provider's
  `incident.fire`. The fresh traceparent stamped before Open makes the
  remediation's act, the `update` that starts the check Pod, a child of
  `incident.analyze` (§11.3, step 5). Every pass is its own trace, linked to
  the Incident's current value (D8). That's the alert → RCA → remediation
  trace.

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
     (`internal/incident/kube.go:100-113`). The act of incident-provider's
     first acting pass on the Analyzing Incident is a child of
     `incident.fire`.
   - **(b) At Open:** a **fresh** traceparent from the current
     `incident.analyze` span, which overwrites (a)'s (§6.5). Because the marker
     is keyed on the value (§6.2), it's new, and the next act joins it.
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
   - **What incident-provider does with it, under D6 and D8:**
     - **The patch's pass** sees the fresh value, B, while the Incident is still Analyzing. It
       has no current script, so no check is due (`machine.go:175-177`). It
       doesn't act, so it's its own trace with one link to B, it adds nothing
       to B's trace, and it writes no marker.
     - **The Open pass** sees B still not continued. It marks a check due,
       writes the marker, then calls `Update`, which starts the check Pod. Its
       `update` span is the first act on B, so it's a child of
       `incident.analyze`, in the trace that the `^incident\.` rule keeps
       whole, with a link back to the pass's root.
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

**Its endpoint already matches D7.** Its chart sets `HOST_IP` from
`status.hostIP` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://$(HOST_IP):4318`
(`templates/deployment.yaml:225-232`). Adopting adds only the `otel.endpoint`
override, rendering the default when it's empty, and the egress caveat in §11.1.

alert-provider's `OTEL_TRACING_ENABLED` defaults to `OTEL_ENABLED`
(`internal/tracing/tracing.go:63-67`). It keeps its own Setup, so it keeps that
rule. `ConfigFromEnv`'s "unset means off" (D1) is for the providers that use it.

### 11.4 core-provider and oasgen-provider (not in the brief, but affected)

Both already call `telemetry.Setup`, and both have their own `SetupTracing`.
To adopt, each sets `TracingEnabled` from its existing flag and deletes its own
`SetupTracing`. Otherwise two TracerProviders race for the global.
- **core-provider** (`go/core-provider`, provider-runtime v1.7.2). Its
  `compositiondefinition.observe` span (`compositiondefinitions.go:817`) comes
  from `coretelemetry.Tracer()`, which is `otel.Tracer(tracerName)` on the
  global provider (`internal/tools/telemetry/tracing.go:54`). Since
  `telemetry.Setup` sets that global (§3.1), the span keeps working, and it
  becomes a child of provider-runtime's `observe`. Adoption is two steps
  (core-provider's review on #60):
  1. Set `TracingEnabled` from its existing `--otel-tracing-enabled` flag
     (`main.go:70`).
  2. Delete `coretelemetry.SetupTracing` and its call and shutdown hook in main
     (`main.go:177-186`).

  core-provider applies the HOST_IP endpoint (D7) to its own Deployment and to
  the cdc. That's the user's decision for the cdc, and it's core-provider's
  change to make.
- **oasgen-provider** (`go/oasgen-provider`, provider-runtime v1.8.2) has its
  own TracerProvider and propagator in
  `internal/tools/telemetry/tracing.go:29-49` (the propagator at `:31`). Its
  main calls `telemetry.Setup` (`main.go:130`) and then its own
  `SetupTracing` (`main.go:148`), gated by `OTEL_TRACING_ENABLED` (`main.go:50`).

### 11.5 observability: one step for each provider that turns tracing on

When a provider's chart turns tracing on, observability adds that provider's
`service.name` (its `OTEL_SERVICE_NAME`) to the
`outside-sampled-services` condition of the tail sampler, in
`charts/otel-collector-tailsampling/values.yaml` (decision D4).
- **Effect.** The routine loop then falls to the `sample-5pct` rule. The
  `error-status` rule still keeps every failed reconcile, and a first act
  inside a kept upstream trace stays with it (D8).
- **No sampler in code.** Providers set no sampler in code. Charts set
  `OTEL_TRACES_SAMPLER` to `parentbased_always_on`, or leave it unset, which is
  the same. `always_on` is allowed and also records a first act under an
  unsampled stamp. Never a ratio sampler (§8). Without this step, every trace
  from a provider not on the list is kept at 100% (§9).
- **Order.** The sampler entry ships first. The provider's chart change that
  sets `OTEL_TRACING_ENABLED=true` ships after it.
- **The burst at rollout** (§6.3). When the chart turns tracing on, every
  annotated object of that provider has a new value. observability sees:
  - one act step, for each object that acts within the window, onto a
    possibly days-old upstream trace (D8);
  - one marker write and one extra reconcile per object.

  They're spread over the first window. Under `sample-5pct` most of the
  reconciles' own traces aren't stored, and the ones that fail are.
- **What the audit join then finds.** With a provider under `sample-5pct`,
  observability#127's join finds a provider object's reconcile after a write
  about 1 time in 20 when that reconcile succeeded, and every time when it
  failed, because `error-status` keeps it. A first act that joins a stored
  upstream trace is kept with that trace, but its root, a trace of its own
  (D8), isn't kept by it. So **a successful
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
- **Link on every pass, act steps included.** It loses the one causal edge
  worth keeping: the first act after an upstream change.
- **Parenting every pass inside the window (D6 as v1.14.0 built it).** A
  creator that stamps every object it writes, and restamps on every release
  apply, pulls every observe-only poll of every object into its trace for the
  window. D8 replaces it (§13).
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
checked in source, except where it's marked as a cluster fact. D8 was decided
on 2026-11-10, after v1.14.0 shipped.

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
  fleet-wide burst of marker writes and extra reconciles, plus, under D8,
  one act step per acting object onto possibly days-old traces, within the
  first window (§6.3).
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
- **Changed by D8.** D6's window made every pass inside it a child of the
  value. D8 keeps D6's rule for which pass consumes a value, the first that
  acts, but makes only that pass's act step the child.

**D7. The OTLP endpoint is `http://$(HOST_IP):4318` by default.** Decided by
the user, the same standard as core-provider's cdc.
- **How it's set.** `HOST_IP` comes from the downward API
  (`fieldRef: status.hostIP`), in the provider Deployment's `env`. It's
  rendered only when the chart's explicit `otel.endpoint` is empty; an explicit
  value, such as the `-local` URL, wins.
- **Why it works everywhere.** The daemonset exposes hostPort 4318 in every
  version in use (§8). It needs no Service name.
- **The caveat.** An egress NetworkPolicy must allow node IPs on 4318, and the
  CNI must support `hostPort` (§11.1).
- **Rejected:**
  - (b) The `-local` Service as the default. It exists only from
    otel-collector-daemonset 0.1.21 (demo runs 0.1.19, as observability
    reports), and its name depends on the release. It stays as an override for
    senders that can't use the downward API, such as the cdc's ConfigMap
    `envFrom`.
  - (c) No default, with every chart setting the endpoint itself. A chart
    that forgot it would export to the SDK's default `localhost:4318`, which
    is wrong in a pod. That's otlptracehttp's default when no endpoint is set
    (`HTTP/internal/otlpconfig/options.go:84`).

**D8. Every reconcile is its own trace; only the first act for a new value
joins it** (2026-11-10, for v1.14.1). Decided by the user after krateo-057's
first day under v1.14.0, and re-confirmed once the facts below were
corrected.
- **What D6 got wrong.** D6 made every pass inside the window a child of a new
  value, so that the pass that acts would be. That's right for a value stamped
  just before the change that triggers the work, like an Incident's B. It's
  wrong for a value that its creator stamps on every object it writes, whether
  or not the object has anything to do. Then every observe-only poll of every
  such object joins the creator's trace for up to 10 minutes.
- **The evidence on krateo-057.** These are cluster facts, as acmp and
  observability report them; I don't touch clusters.
  - The composition-dynamic-controller stamps `krateo.io/traceparent` on every
    object it creates. With alert-provider 0.2.71, the reconciles of all 28
    Alerts joined their creating composition's trace: at least 3,204 spans.
    The tail sampler kept that trace at 100%, because it holds a cdc span.
    That was about 13 times the volume.
  - The cdc also re-stamps. Each release apply stamps every object in the
    release, changed or not (§6.3; the traceparent is stripped only from the
    digest, `processor.go:39-44,92-117`). Today's values came from the
    07:36:11Z roll. So under v1.14.0, every chart upgrade reopens a 10-minute
    window for every object, and about 260 observe-only reconciles merge into
    the upgrade's trace each time, kept at 100% for the same reason.
  - **It wasn't a marker bug.** 28 of 28 markers were written by 08:11Z, with
    no write errors. The creator trace stopped growing after 08:10, and since
    about 08:09Z every reconcile has been a new root with one link. v1.14.0
    did what D6 specified: a 10-minute window, not a trace that never ends.
    D8 changes the design, not a defect. The real-path envtest test (§10,
    test 26) keeps the marker write covered.
- **The decision.**
  - Every reconcile is its own trace: a `reconcile` root (Server, the same
    three attributes) with one link to the object's `krateo.io/traceparent`
    when it's present and valid, plus one to its act on the first acting
    pass. It's never that value's child.
  - Only the first act step for a new value, the `create`, `update` or
    `delete` span, is started with the upstream traceparent as its parent,
    plus one link back to its `reconcile` root. It carries the root's three
    `k8s.object.*` attributes, because it isn't the root's child.
  - **Link both ways** (the user's decision on the review of #70): when that
    act starts, the root adds a link to it (`Span.AddLink`), if the act is
    sampled.
  - The marker is written before that step, as in D6: with the optimistic
    lock, and with a requeue before acting on a conflict.
  - Later passes, and observe-only, deferred, paused and conflict passes, get
    the root and the link only.
  - The window stays only as the fallback that writes the marker when no pass
    acts within it. It no longer parents anything.
- **D6's remediation still joins.** The Open pass's `Update`, which starts
  the check Pod, is the first act on B. So its `update` span is a child of the
  traceparent stamped from `incident.analyze`, in the trace the `^incident\.`
  rule keeps whole (§11.3, step 5). The Analyzing pass that only observes B
  is its own trace.
- **D3 still holds:** only a new value opens a window (§6.2).
- **What it costs.**
  - The upstream trace shows the act, not the reconcile around it: no
    `connect` or `observe` before it. The act's link leads to its root.
  - Work a provider does in Observe without acting, like alert-provider's
    incident firings (§6.2), never joins the upstream trace. It's in the
    pass's own root, which links to it.
  - **An unsampled stamp.** Under `parentbased_always_on` every reconcile
    root is sampled, and the joined act span follows the stamp's sampled flag.
    A `00` stamp drops the act, what the provider does under it, and its link
    back to the root, and the root's own trace has no act either. Krateo's
    stamps are `01` (observability checked krateo-057).
  - **The rule for charts.** Charts set `OTEL_TRACES_SAMPLER` to
    `parentbased_always_on`, or leave it unset, which is the same. `always_on`
    is allowed and also records a first act under an unsampled stamp. Never a
    ratio sampler (§8, §11.5).
- §2, §4.4, §6.2, §6.3, §8, §10 (test 26), §11.2, §11.3 and §11.5 specify it.

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
- **The consumer scan for the critical-write change** (§2). Every non-archived
  go.mod in krateo-platformops was read: the root ones through the GitHub
  GraphQL API, and the nested ones through each repo's tree. That's
  core-provider's four, oasgen-provider's two, snowplow's, authn's and
  otel-collector's three. Each was matched against both module paths, and each
  consumer's code was grepped for annotation writes. Archived repos weren't
  scanned. The old `krateoplatformops` org wasn't scanned either. Its repos are
  on the old module path, and its `provider-runtime` is still on that path at
  v1.2.1 (its `go.mod` and tags, read through the API). So they can't take the
  change without rewriting the module path.
- **oasgen-provider's tracing setup**, and that `apiserver`'s generic store
  checks the resourceVersion (`store.go:807-808`).
- **The consumers:** every consumer file cited in §11, observability's join,
  Service and tail-sampler files, and #58 as merged in v1.13.0.
- **For D8:** OTel SDK v1.45.0's `ParentBased` defaults
  (`SDK/trace/sampling.go:182,200`) and `AddLink`, which a non-recording span
  ignores (`SDK/trace/span.go:678-690`). The cdc's `processor.go` on
  core-provider's `main`, read through the API on 2026-11-10. The digest is computed from the manifest with the
  traceparent lines stripped, and the objects are decoded from the original
  manifest, so they keep it.

Not checked:

- **What krateo-057 runs**, checked by me: the env values, the object counts,
  and which endpoint each Deployment uses. I don't touch clusters. D8's
  numbers (28 Alerts, at least 3,204 spans, about 13 times the volume, about 260
  reconciles per upgrade, 28 of 28 markers by 08:11Z, Krateo's stamps
  sampled `01`) are acmp's and observability's. The brief
  reports the env convention as verified on 057. observability verified, in
  its reviews on #60: the `-local` Service on 057 (release
  `otel-collector-daemonset`, 4 of 4 node endpoints); hostPort 4318 on 0.1.19
  and 0.1.23, from chart source; and that demo runs 0.1.19. I read the
  upstream `values.yaml:425-430` and observability's `e9fe3ea` myself. I didn't
  read `_config.tpl:481-482`. alert-provider's
  `incident.*` name test in 0.2.67 is as observability reports it; I didn't
  read that test.
- **Whether every provider's predicates let annotation-only updates through**
  (§6.3). Only alert-provider's was read.
- **How long it takes to build:** not estimated.
