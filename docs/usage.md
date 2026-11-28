---
type: Usage
title: provider-runtime — usage
description: How to consume provider-runtime as a Go module and the minimal code to build a provider on it.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-get, usage]
timestamp: 2026-11-13T00:00:00Z
---

# Usage

provider-runtime is consumed as a plain Go module. It requires Go ≥ 1.26
(see `go.mod`; since v1.7.0) and pins `sigs.k8s.io/controller-runtime` v0.25.x / `k8s.io/*`
v0.37.x (v0.23.x / v0.35.x up to v1.11.x) — your provider's
controller-runtime version must be compatible. Go's minimal version selection
raises a provider that requires older versions to these when it upgrades
provider-runtime; bump them in your own `go.mod` too, and build against
controller-runtime v0.25's API (its release notes list the breaking changes).

Build with **Go ≥ 1.26.10**. The `go` line is only the language floor; the
standard library comes from your toolchain. Older 1.26 patches carry
`net/http` and `crypto/x509` vulnerabilities that `govulncheck` reports
against code using this module. You don't need to pin a patch version for
this: the floating `golang:1.26` images (for example `golang:1.26-trixie`)
track the latest 1.26 patch, which was 1.26.10 on 2026-10-28.

This repo's own `go.mod` carries `toolchain go1.26.10`, so its builds,
tests and `govulncheck` scans use a patched standard library. Go ignores a
dependency's `toolchain` line, so it does not raise your floor. Set your
own if you scan locally with an older Go: under `GOTOOLCHAIN=auto` the scan
runs on the version your `go.mod` selects, and at `go 1.26.0` that reports
standard-library findings your patched build doesn't have.

```sh
go get github.com/krateo-platformops/provider-runtime@v1.10.0
```

If your provider also uses `github.com/krateo-platformops/audit` (v0.3.0+),
use provider-runtime ≥ v1.7.1. v1.7.0's `pkg/logging` doesn't compile
against the `otel/log` version audit selects.

## Minimal provider

Implement an `ExternalClient` for your external system, then let the library
drive the loop:

```go
import (
    ctrl "sigs.k8s.io/controller-runtime"

    "github.com/krateo-platformops/provider-runtime/pkg/controller"
    "github.com/krateo-platformops/provider-runtime/pkg/logging"
    "github.com/krateo-platformops/provider-runtime/pkg/reconciler"
    "github.com/krateo-platformops/provider-runtime/pkg/resource"
)

func Setup(mgr ctrl.Manager, o controller.Options) error {
    r := reconciler.NewReconciler(mgr,
        resource.ManagedKind(v1alpha1.MyResourceGroupVersionKind),
        reconciler.WithExternalConnecter(&connector{kube: mgr.GetClient()}),
        reconciler.WithLogger(o.Logger.WithValues("controller", "myresource")),
        reconciler.WithPollInterval(o.PollInterval),
    )

    return ctrl.NewControllerManagedBy(mgr).
        Named("myresource").
        For(&v1alpha1.MyResource{}).
        WithOptions(o.ForControllerRuntime()).
        Complete(r)
}
```

Your CRD type satisfies `resource.Managed` by embedding
`metav1.TypeMeta`/`metav1.ObjectMeta` and exposing
`GetCondition`/`SetConditions` over an embedded
`apis/common/v1.ConditionedStatus`. The connector implements
`Connect(ctx, mg) (reconciler.ExternalClient, error)`; the client implements
`Observe`/`Create`/`Update`/`Delete`. A complete, compilable version of this —
including scheme registration, the OTel-model logger and a runnable CRD — is
[examples/greeting-controller](../examples/greeting-controller/README.md).

## Migrating to v1.8.0

Use v1.8.2 or later. v1.8.1 fixed a false `ReadyNotSetAfterObserve` report
on resources being deleted; v1.8.2 makes the runtime set `Ready=Deleting`
itself when `Connect` or `Observe` fails on a resource being deleted. v1.8.x
requires Go 1.26 (since v1.7.0).

**Rollout order, per cluster:** core-provider **2.13.56** or later (its
rollup maps `NotObserved` as unevaluated) must be live on a cluster before
any provider built on v1.8.x is deployed there. Otherwise the rollup reads
`NotObserved` as "converging", and a parent composition goes
`Ready=False/Creating` during a provider's outage.

"Live" means the code is running, not that a chart was pinned: core-provider
runs a separate composition controller per CompositionDefinition version
(`<resource>-<version>-controller`), so check the image on every one of them.
A leftover controller that nothing drives keeps its old image; it matters
only if it serves the parent of a v1.8.x provider's kind.

**Verifying the rollup during a provider rollout.** The rollup's handling of
`NotObserved` can only be seen once a v1.8.x provider is deployed and one of
its `Observe` calls fails. A healthy rollout fails nothing, so it cannot
exercise this: "never caught" is the expected result of every clean rollout,
and a series of them is not evidence. The rollup is verified by an end-to-end
test in core-provider's CI, against a real API server (envtest), with a
separate provider process built on this runtime whose `Observe` is made to
fail per object: `internal/composition/notobserved_envtest_test.go` in the
composition-dynamic-controller, run by the `envtest-cdc` job on every PR
(`go test -tags envtest -run NotObservedEnvtest`). Its cases follow the rule
below, including the excluded mixed parent, and a CI step removes the
rollup's `NotObserved` mapping to prove the test then fails (core-provider#317).
What a green run does *not* cover: envtest has no kubelet, scheduler or helm,
so the test says nothing about the composition controller as a deployed image,
its RBAC (a child it isn't allowed to read is a separate path, core-provider#318),
its Deployment wiring, or helm actually rendering the children. It proves the
rollup's mapping, not the installed controller around it.
Watching a real rollout is optional and only
meaningful when something actually breaks. When you do judge it, judge it per
parent composition:

- *Excluded parent:* any other child is *not* healthy (`Ready=True`) or
  unevaluated (`Unknown/ZeroEvidence` or `Unknown/NotObserved`). That covers
  `Ready=False` for any reason, and `Ready=Unknown` with any other reason,
  including one added later: its reason belongs to that child, and a new
  reason excludes the parent until someone deliberately adds it to the
  unevaluated list.
- *Eligible parent:* at least one child is `Ready=Unknown/NotObserved`, and
  the parent is not excluded.
- *Pass:* every eligible parent reads `Ready=True` (`Available` or
  `AvailablePartialCoverage`), or `Unknown/ZeroEvidence` when every child is
  unevaluated.
- *Fail:* an eligible parent reads `Ready=False`, whatever the reason.
- *No result*, reported as such: "never caught" when no child reached
  `NotObserved`, and "inconclusive" when `NotObserved` children appeared but
  every parent was excluded. Give the counts: eligible, excluded, and total
  parents with a `NotObserved` child.

v1.8.0 makes the Ready contract explicit (see
[overview](./overview.md#the-ready-contract-since-v180)). When `Connect` or
`Observe` fails, the runtime now sets `Ready` to `Unknown/NotObserved`. The
next successful `Observe` must set `Ready` again, because the runtime won't.
A provider adopting v1.8.0 must:

1. **Set `Ready` on every successful `Observe` path**, including
   `ResourceExists: false`, drift and early "nothing to do" returns. A path
   that doesn't set it leaves `NotObserved` in place after a failure, and the
   runtime reports it as a `ReadyNotSetAfterObserve` Warning event
   (`reconciler.ReasonReadyNotSetAfterObserve`) and an Error log. Paths taken
   while the resource is being deleted are exempt: the runtime sets
   `Ready=Deleting` there itself (v1.8.1; v1.8.0 reported them by mistake).
   Setting `Ready` there anyway is allowed, so the helper can still check such
   a path if your `Observe` sets it.
2. **Wrap with `reconciler.ReadyAssessed` the errors returned after setting
   `Ready`.** For example, when `Observe` reads the resource, sets `Ready`,
   and then a write fails:

   ```go
   mg.SetConditions(prv1.Available())
   if err := writeStatus(ctx, mg); err != nil {
       return reconciler.ExternalObservation{}, reconciler.ReadyAssessed(err)
   }
   ```

   Without the wrap, the runtime replaces that `Ready` with `NotObserved`.
   Don't wrap an error that means "I could not tell".
3. **Add `reconcilertest.AssertObserveSetsReady` to your tests**, once per
   success path, so the contract can't silently rot:

   ```go
   reconcilertest.AssertObserveSetsReady(t, ctx, client, mg)
   ```

Make the bump in the same PR as these fixes. A bump alone turns every path
that misses `Ready` into a stuck `NotObserved` with a Warning. Nothing
changes for a provider until it bumps. Anything that rolls `Ready` up must
know the `NotObserved` reason before a v1.8.0 provider reaches a cluster.
For Krateo, that's core-provider#310.

## The pieces you opt into

- **Reconciler options** — `WithTimeout`, `WithPollInterval` /
  `WithPollIntervalHook` / `WithPollJitterHook`, `WithCreationGracePeriod`,
  `WithFinalizer`, `WithRecorder` / `WithThrottledRecorder` (Kubernetes
  events), `WithMetrics` (a `MetricsRecorder`, e.g. `*telemetry.Metrics`),
  `WithCriticalAnnotationUpdater`. See [configuration](./configuration.md)
  for every default.
- **Logging** — build a `logging.Logger` from logr (`NewLogrLogger`) or slog
  (`NewSlogLogger`); use `logging.NewOTelHandler(level, w, serviceName)` as
  the binary's slog handler to get OTel-model JSON on stderr plus OTLP export
  once a LoggerProvider is installed.
  - *Log-to-trace correlation* (unreleased, v1.13.0). Bind a Logger to a
    context with `logging.WithContext(log, ctx)`, and every record it logs
    carries that context to the slog handler. It works when the Logger is
    slog-backed (`NewSlogLogger`), `ctx` holds a span, and the handler reads
    it. `NewOTelJSONHandler` and `NewOTelHandler`'s JSON stream add `trace_id`
    and `span_id`. `NewOTelHandler`'s otelslog bridge gives the context to
    the LoggerProvider your main installs with `global.SetLoggerProvider`,
    which sets the OTLP record's trace context. A logr-backed Logger can't
    carry a context to its sink, so `WithContext` returns it unchanged.
    `Reconcile` binds its per-reconcile Logger to the `ctx` it is given.
    With tracing on (unreleased), it starts the reconcile span itself and binds
    the Logger to it, and each provider method gets its child span's `ctx`.
    With tracing off, that `ctx` holds a span only if you wrap the
    `Reconciler` in a reconciler that starts it. Your `ExternalClient` methods get a `ctx`
    (the reconcile's, with its timeout) but not that Logger, so bind your
    own where you log:

    ```go
    func (e *external) Create(ctx context.Context, mg resource.Managed) error {
        log := logging.WithContext(e.log, ctx)
        // ... call the API with ctx, so its client spans are children ...
        log.Info("created external resource", "externalName", meta.GetExternalName(mg))
        return nil
    }
    ```

    `WithContext` on a Logger that doesn't implement `logging.ContextLogger`
    returns it unchanged, so this is safe with any Logger.
- **Metrics** — `telemetry.Setup(ctx, log, telemetry.Config{Enabled: true})`
  returns `(*Metrics, shutdown, error)`; pass the metrics handle to
  `reconciler.WithMetrics` and `controller.Options.QueueWaitRecorder`.
- **Tracing** (unreleased; [the design](./design-tracing.md)). Read Krateo's
  OTel convention in main and pass it to `Setup`, with your service name. Shut
  it down after the manager returns, with a bounded context, so the batcher
  flushes:

  ```go
  cfg, err := telemetry.ConfigFromEnv() // OTEL_ENABLED, OTEL_TRACING_ENABLED, DEPLOYMENT_NAME, SERVICE_VERSION, ...
  if err != nil { /* ... */ }
  cfg.ServiceName = "git-provider"
  metrics, shutdown, err := telemetry.Setup(ctx, log, cfg)
  if err != nil { /* ... */ }
  defer func() {
      ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
      defer cancel()
      _ = shutdown(ctx)
  }()
  ```

  `OTEL_TRACING_ENABLED` unset means off, whatever `OTEL_ENABLED` says. See
  [configuration](./configuration.md) for the environment, and the design's
  §11 for the chart side: the `HOST_IP` endpoint, `OTEL_SERVICE_NAME` and
  `DEPLOYMENT_NAME`.
  - *Outbound HTTP.* A client wrapped with `httpsignal.NewTransport` sends the
    active span's `traceparent` on every request made with the method's `ctx`.
    With tracing off it sends nothing.
  - *Onto another controller's object.* When you create or update an object
    another Krateo controller reconciles, call `tracing.Inject(ctx, obj)` on
    it first, so that controller continues your trace. Follow the rules:
    - only onto such an object, never a Pod, ConfigMap, Secret or your own
      managed resource;
    - only on a write you make anyway;
    - never in a status write (the status subresource drops metadata);
    - stamp before the change that triggers the work, when they're two writes;
    - never copy `krateo.io/traceparent-continued`.
  - *Each method's own `ctx`.* Use the `ctx` each method is given. A client
    that keeps the `ctx` from `Connect` would parent later calls onto a span
    that has ended.
  - *What the reconciler does with it.* With a TracerProvider, every
    `Reconcile` is a `reconcile` span with `connect`, `observe`, `create`,
    `update` and `delete` children, in a trace of its own that links to your
    managed resource's `krateo.io/traceparent`. For a new value, the first
    `create`, `update` or `delete` span is the value's child, so your
    `Create`, `Update` or `Delete`, and what it does with its `ctx`, joins the
    upstream trace. Later acts don't. If no pass acts within the window
    (`reconciler.WithTraceContinueWindow`, default 10 minutes; raise it if you
    poll much more slowly), the value is continued without joining. The
    reconciler records that in `krateo.io/traceparent-continued`, which needs
    `patch` on your kind. Work your Observe does by itself never joins. Add your
    own attributes to the child spans with
    `trace.SpanFromContext(ctx).SetAttributes(...)`.
- **Rate limiting** — `ratelimiter.NewController()` for per-controller
  defaults, `ratelimiter.LimitRESTConfig(cfg, rps)` to bound client-side QPS,
  `workqueue.NewExponentialTimedFailureRateLimiter` for time-expiring
  per-item backoff.
- **Rate-limit governor** (off by default) — slows reconciles down by itself
  when an external API signals a rate limit, per endpoint key, or when the
  controller's container is CPU-saturated. It replaces hand-tuning
  `--max-reconcile-rate` and `--poll`. See
  [configuration](./configuration.md#rate-limit-governor-controlleroptionsratelimit)
  for every setting and
  [the design](./design-rate-limit-governor.md) for why it works this way.
  - *Wiring.* Set `o.RateLimit.Enabled` (and `o.RateLimit.Saturation.Enabled`
    for the saturation input) from one chart flag each, call
    `o.SetupRateLimitGovernor(mgr)` once, before the controllers, pass
    `reconciler.WithRateLimitGovernor(o.RateLimitGovernor)` to every
    reconciler, and build every controller with
    `WithOptions(o.ForControllerRuntime())`. A `nil` governor is a no-op, so
    this is safe when the chart leaves the flag unset.
  - *Saturation.* With `o.RateLimit.Saturation.Enabled`, each controller's
    queue takes a slot in the process-wide `local` capacity before every
    reconcile and gives it back when the reconcile returns, whether or not the
    provider implements `RateLimitKeyer`. Low capacity then behaves like fewer
    workers: the queue's order is the order work runs in, and nothing is popped
    that can't run. The slot covers the whole reconcile, so expensive work is
    bounded wherever the provider does it. It reads `cpu.pressure`
    (`some total`) once a second, and measures the share of the last 10s in
    which some task waited for CPU. `local` starts at 1 and rises by 1 every
    clean 2s, so 16 workers take about 30s. Pressure over the threshold for a
    whole 10s halves it, never below 1 and at most once per 10s; it then
    rises by 1 per clean 2s again. Controllers sharing the
    governor share `local`; an idle controller holds none of it.
  - *Nothing is written on the object for it.* The controller's own saturation
    isn't a state of the object: the item waits in the queue, with no
    `Synced=False`, no event and no status write. It shows in
    `provider_runtime.ratelimit.local.capacity` and the input gauges (the
    local capacity gauge reports nothing with saturation off). Key
    deferrals still write `Synced=False/ReconcileDeferred`. Before this, a full
    `local` deferred popped items and the startup ramp was a quiet special
    case; both are gone ([design](./design-local-slot-before-pop.md)).
  - *Two things the runtime checks for you.* The governor's sampler must run:
    `SetupRateLimitGovernor` adds it to the manager, but if you call
    `ratelimit.New` yourself, add it. Without it `local` stays at 1 and every
    saturation input reports as disabled. And every controller must be built
    with `ForControllerRuntime`: the governor logs an error at start if more
    reconcilers use it than controllers had their queue gated.
  - *Memory is not protected.* The saturation flag reads CPU only. The working
    set is a separate input,
    `o.RateLimit.Saturation.ExperimentalMemoryWorkingSet`, off by default and
    experimental. Don't turn it on expecting OOM protection: it did not
    prevent OOMKills in 0 of 19 runs on an overcommitted workload, or a crash
    loop after one (see
    [configuration](./configuration.md#re-measurement-of-the-memory-input)).
    Keep workers times the largest call well under the memory limit. A sample
    period below 1s only matters to this input.
  - *Keying.* A keyer is needed only for per-endpoint API rate limits.
    Implement `reconciler.RateLimitKeyer` on the connecter, the client, or
    both. Only calls of a connecter or client that implements it are gated
    per endpoint; each key recovers by 1 per `RecoveryInterval` (30s). Return
    the endpoint host (`httpsignal.HostKey` of the base
    URL), or `host|credential-reference` when the quota is per identity. Key
    on the credential's reference (for example the Secret's namespace and
    name), never its value: credentials rotate, and a key that changes on
    rotation splits one quota into several. `RateLimitKey` must not do I/O,
    and must return the same string the provider's signals use.
  - *Objects on two endpoints.* When one object's calls reach several
    rate-limited scopes, such as a sync from one server to another, implement
    `reconciler.RateLimitKeysProvider` instead: `RateLimitKeys` returns every
    scope the object's calls reach. A call then needs a slot in each key, all
    or nothing, and is deferred if any of them is throttled or full, with the
    deferral naming the key that stopped it. One key can't do this honestly: a
    throttled scope the call isn't gated on keeps being called, and sharing one
    key slows the healthy scope too. The rules for keys above hold for each
    key. Return one key when the object's endpoints are the same.
  - *Reporting.* Wrap the HTTP client's transport with
    `httpsignal.NewTransport(base)` and pass the context the runtime handed
    to `Connect` or the `ExternalClient` method into each request
    (`http.NewRequestWithContext`). Or call `ratelimit.Report(ctx, Signal)`
    yourself. Report on successful responses too: Azure DevOps announces a
    delay on a 200 before it starts refusing.
  - *Testing.* Call `reconcilertest.AssertReportsRateLimits` in the
    provider's tests. It feeds the provider's call a recorded GitHub 429, a
    recorded Azure DevOps advisory 200 and a healthy GitHub 200, and fails
    unless the first is reported as throttled with a deadline, the second as
    advisory, and the third as neither. The runtime can't check this from
    outside: a provider that never reports looks like an endpoint that is
    never limited.
  - *What a deferral looks like.* A pass whose next call is deferred makes no
    external call. It writes `Synced=False` with reason `ReconcileDeferred`
    and leaves `Ready` as it was. The message names the key and the server's
    deadline, for example `reconciliation deferred: api.github.com is
    rate-limited by the server; next attempt 2026-11-05T10:00:31Z`, or says
    it is waiting for capacity. A deferral by the local capacity names the
    cause and no key, for example `reconciliation deferred: controller
    saturated: CPU pressure 63% (some, over 10s); waiting for capacity (1
    call(s) in flight allowed); no server deadline was given`, or
    `reconciliation deferred: controller recovering its local concurrency
    limit (2 call(s) in flight allowed); waiting for capacity; no server
    deadline was given`.
    A Normal `ReconcileDeferred` event is recorded, through the throttled
    recorder, when the object enters a deferral or a throttle names a new
    deadline. The pass requeues after the deadline (or 1s to the
    RecoveryInterval) with a nil error, so it consumes no failure backoff.
    When the deferral would write the object back exactly as it was read
    (an at-capacity deferral repeats the same message every pass), the
    status update is skipped and the pass only requeues. A throttled
    deferral that names a new deadline still writes.
  - *The pass that gets the 429 is still an error.* The call failed, so that
    pass reports `ReconcileError` and consumes one backoff step, as today.
    The passes after it are deferrals until the server's deadline.

  **No requirement on where providers do their work.** With saturation on, the
  local slot covers the whole reconcile, so work before `Connect` is bounded as
  much as work inside it.

- **Scale to zero** (unreleased; for owners that manage controller
  Deployments, not for the controllers themselves) — `pkg/scaletozero`
  runs a per-Kind controller Deployment at 0 replicas while the Kind has no
  instances, and wakes it when one appears. See
  [the design](./design-scale-to-zero.md).
  - *Wiring.* Call it from the reconcile of the object that owns the
    controller, and persist `LastSeen` in that object's status:

    ```go
    // A live client: Exists fails on a cache-backed one.
    live, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme(), Mapper: mgr.GetRESTMapper()})
    // ...
    res, err := scaletozero.Reconcile(ctx, live, deployKey, gvk, sel,
        obj.Status.LastSeen.Time, 10*time.Minute, obj.Spec.ReplicaCount)
    if err != nil {
        return ctrl.Result{}, err // nothing was patched; render nothing new
    }
    obj.Status.LastSeen = metav1.NewTime(res.LastSeen)
    return ctrl.Result{RequeueAfter: res.RequeueAfter}, nil
    ```
  - *The helper is the mechanism, not the trigger.* It watches nothing. A
    controller at 0 gets no reconcile, so the owner must reconcile its owning
    object when the first instance of the Kind appears, for example with a
    per-Kind watch.
  - *The selector* must be exactly the one the controller reconciles by
    (`labels.Everything()` when it takes every instance). A narrower one
    scales a controller to zero while instances it serves still exist.
  - *A live reader.* `Exists` needs `mgr.GetAPIReader()`, and `Reconcile` a
    client from `client.New`, not `mgr.GetClient()`.
  - *Per call:* two reads, a GET of the scale subresource and a
    metadata-only LIST that stops at the first instance; a PATCH only on
    change. Safe on every poll. During the hold-down, `RequeueAfter` keeps
    the scale-down on time between polls.
  - *Precondition.* Write the Deployment's `spec.replicas` nowhere else. A
    full-object Update that omits `replicas` defaults it to 1 and fights the
    helper: render `replicas` from the last `Result.Replicas`, which is never
    0 on an error.
  - *RBAC.* list on the watched resource cluster-wide; get and patch on
    `deployments/scale`.
  - *A Kind that holds credentials* must not sit at 0: nothing releases its
    holds then. See [Credential use](#credential-use-and-scale-to-zero).
- **A name that comes from data**, as a `secretRef` in a spec. Check it, and the
  namespace, where the API's schema doesn't: with
  `rest.IsValidPathSegmentName`, or as the DNS names they are. A typed or a
  dynamic client refuses one that isn't a path segment; a metadata client
  (`client-go/metadata`, or `PartialObjectMetadata` through controller-runtime)
  doesn't, nor does a cached reader (`resource.GetSecret` and
  `GetConfigMapValue` read through the client you give them). It matters where
  you restrict the namespace and not the name. An object of a cluster-scoped kind
  is named without a namespace: a `credentialuse` ref that fills one in names no
  object, and is Missing.

## Credential use

`pkg/credentialuse` and the reconciler's adapter hold the Secrets, and the
objects that lead to them, that a managed resource reads in `Connect`, for as
long as losing them would be unrecoverable. A namespace deletion then can't
wedge on a managed resource whose credentials were deleted first. Each held
object carries a finalizer `credential-use.<lowercase kind>.<group>/<namespace>` of the
holder's kind, added before the holder connects and removed once no holder of
that namespace uses the object. The design is
[design-credential-use-finalizer](./design-credential-use-finalizer.md); section
numbers (§) below are its.

### The limit: direct refs only

**This release holds direct refs only** (decided 2026-11-24). Every ref that
`CredentialRefs` returns must have a nil `Next`: name each Secret, and each
other object `Connect` reads, as a ref of its own. A chain, a ref with a `Next`,
is **refused in this release**, and the whole list with it, direct refs
included (`credentialuse.ErrChainsNotSupported`, §9):

- a **new** object doesn't connect: `Synced=False/ReconcileError`, with the error
  naming the ref, and `Ready=NotObserved`;
- an **established** object keeps working, with `Synced=True/ReconcileSuccess`
  whose message is `credential-use: refs refused, not held: <the error, cut>
  (§9)` for as long as it is refused. It keeps what it already holds, except
  the holds whose target withdrew consent, which the pass still drops
  (`acquire.go:452-474`). Nothing new is held, its direct refs included. The pass that isn't refused
  takes the message off, a failed one included (decided 2026-11-25). Any other
  Synced (an `Observe` or `Connect` error, a policy report, a deferral, a paused
  holder) says its own outcome instead. The message is set in every status write
  the pass makes, so it costs no write of its own;
- the `CredentialUseDegraded` Warning event is recorded **once** per holder, with
  a log line, until a pass of it isn't degraded or it is forgotten
  (`tracker.go:542-545`), and a restart records it again. The event expires with
  the API server's `--event-ttl`, so the message is what lasts. The gauge is
  `credentialuse_refused_holders{holder_kind}` (decided 2026-11-25): the
  holders, new ones included (`tracker.go:540`), whose last pass in this
  process was refused. It drops one at a pass
  that isn't refused, at `Forget` and at the holder's deletion. The counter
  `credentialuse_degraded_total{reason="ChainsNotSupported"}` counts every
  pass.

A Secret whose name is only inside another object can't be named, so leave it
out: it isn't protected, as before. Read that other object in `Connect` as you
do today: `credentialuse.Resolved` returns nothing. A later release follows
`Next`. On an upgrade to credential-use an existing object has no record yet,
so it holds nothing until a pass writes one. Disown doesn't let a refused new
object connect.

### Adopting it

Implement `reconciler.CredentialRefsProvider` on the connecter, give the
reconciler a finalizer that implements `resource.NamedFinalizer`, and add two
lines to the controller's builder. The refs come from memory, never from an API
call, and are used to acquire, never to release:

```go
func (c *connector) CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error) {
    cr := mg.(*v1alpha1.MyResource)
    return []credentialuse.Ref{credentialuse.SecretRef(cr.Namespace, cr.Spec.SecretRef.Name)}, nil
}

r := reconciler.NewReconciler(mgr, kind,
    reconciler.WithExternalConnecter(c),
    reconciler.WithFinalizer(finalizer), // implements resource.NamedFinalizer
    reconciler.WithCredentialUse(credentialuse.KindOptions{ /* optional */ }),
)
return ctrl.NewControllerManagedBy(mgr).
    Named("myresource").
    For(&v1alpha1.MyResource{}, builder.WithPredicates(r.IgnoreCredentialUseRecord())).
    WatchesRawSource(r.CredentialUseSource()).
    WithOptions(o.ForControllerRuntime()).
    Complete(r)
```

- Without `WatchesRawSource(r.CredentialUseSource())` the manager fails: at the
  kind's first reconcile, or within 2 minutes of election, whichever comes first
  (`credentialuse.go:164-190`, `:653-682`). A connecter that doesn't implement the interface changes nothing: no
  informer, no annotation, no write.
- `IgnoreCredentialUseRecord` drops the update event that only the record's own
  write causes, so it costs no extra pass.
- `KindOptions` defaults to Secrets, `LabelScoped`. Name the protected kinds in
  `Protected`, and set `Watch: credentialuse.PerName` for `resourceNames` RBAC
  (it needs `SharedOptions.IndexNamespace`, given through
  `WithCredentialUseShared`). `PerNamePoll` isn't supported: `Track` refuses it.
  Don't set `Content`, which is for chains.
- Run `reconcilertest.AssertCredentialRefsCover` in your tests: it fails on a
  Secret that `Connect` reads and the refs don't name, on a `Next`, on a ref whose
  object isn't in the fixture, and on conflicting refs
  (`reconcilertest/credentialuse.go:115-126`). Build the connecter under test with
  `reconcilertest.NewRecordingReader`, and pass that reader to the assertion. It
  can't be used, in this release, for a fixture whose `Connect` reads a Secret
  through another object. In production, wrap the reader with
  `reconciler.CredentialUseCheckingReader`: a read of a protected kind the refs
  don't name is logged once and counted in `credentialuse_unrecorded_reads_total`.
- Before adopting, check the conversions of each custom resource the refs name,
  and of your own kind: a CRD that serves several versions must convert without
  losing a field, a label or an annotation, or a hold drops fields or is never
  released (§6.2). With the conversion strategy `None` that means equal schemas.
- **An outside `MetricsRecorder` must embed `credentialuse.NopMetrics` or
  `*telemetry.Metrics`, not both** (`credentialuse/metrics.go:7-10`). The
  interface grows with the metrics, so one that embeds neither stops compiling at
  the next method. See the [log](./log.md) for what the first release added.

### What the runtime writes where readers look

- **The `krateo.io/external-create-*` annotations are history, not state.**
  `pending`, `succeeded` and `failed` are the times of the last attempts of a
  create, kept for the create-tracking guard. Don't read one as the object's
  current condition: judge an object by its `Ready` and `Synced` conditions,
  which is what the first adopter asked for.
- **The holder's annotations.** `credential-use.krateo.io/refs.v1` is the
  record of what the holder names, and `credential-use.krateo.io/unheld` lists
  the refs that aren't held, with a reason and a time (`NotConsented`,
  `ConsentWithdrawn`). Both are the runtime's: don't edit them.
- **A failure that points to the list.** When a pass fails in `Connect` or
  `Observe` and the list isn't empty, the message ends
  `; credential-use: <n> ref(s) not held (<reason> since <time>), see annotation
  credential-use.krateo.io/unheld`. It is at most 160 characters and is never
  added to a successful pass.

### Grants

Bind a **ClusterRole with a ClusterRoleBinding**. Never a namespaced Role: it is
deleted with its namespace, and the objects there then can't finish deleting
(§13). For `LabelScoped`, the default:

| Resource | Verbs |
|---|---|
| `secrets`, and each other protected kind | `get`, `list`, `watch`, `patch` |
| `namespaces` | `get`, `list`, `watch` |
| your own kinds | `get`, `list`, `watch`, `patch`, `update` |
| `customresourcedefinitions`, by `resourceNames` (your kinds') | `get`, `list`, `watch`, `patch` |
| `events` | `create`, `patch` |
| the non-resource URL `/apis/*` | `get` (granted by `system:discovery`, unless your cluster removed its binding) |

`patch` on your own CRDs is new for providers that ship static CRDs: the writes
are rare, and nothing else writes them (§13, L6). `list` and `watch` on Secrets
cluster-wide expose every Secret to the provider: use `LabelScoped` only where
it already holds that grant.

For `PerName`, where the refs' names are known when the RBAC is generated, grant
the protected kinds `get` and `patch` (and `list`, `watch` with a `metadata.name`
field selector) by `resourceNames` instead, and for the index: `configmaps` by
`resourceNames` (`get`, `update`, `patch`, `watch`), and `create` on `configmaps`
in the index namespace, which can't be scoped by name. The namespace is
installer's, providers never create it, and an admission policy limits what is
written there (§10.6.1). If a write to the index fails with "the namespace's
admission policy has no parameters", check that the policy's parameter
ConfigMap `cu-policy-params` exists. Every holder kind of one `Shared` must agree
on a protected kind's watch mode.

### Across namespaces

A holder holds a Secret in **another** namespace only if that Secret, or its
Namespace, carries `credential-use.krateo.io/allow-holders-from` listing the
holder's namespace; `*` is accepted on the Secret only. Without it the
reference still works as before, isn't protected, and the holder gets a
`CredentialRefNotHeld` event. Removing a namespace from the annotation
releases those holds with a `CredentialConsentWithdrawn` event. That needs a pass
of each holder: **a paused holder keeps its holds** (below).

### If you delete a Secret your objects use

If you delete a Secret your objects use while its namespace is live, recreate it
before deleting those objects, or delete the objects first. If objects are already
stuck deleting on a missing Secret in a live namespace, recreate the Secret and
they finish (§5.2).

### Uninstalling a provider

Before uninstalling, delete its objects and wait for them to go. Or annotate each
holder CRD with `credential-use.krateo.io/disown=all` (or a comma-separated list
of protected kinds, such as `Secret,KubernetesTarget.core.krateo.io`), and wait
until **all three** hold (`credentialuse.DisownSettled` checks them):

1. `credential-use.krateo.io/disown` is set and not empty;
2. `credential-use.krateo.io/disown-settled` is the same string;
3. `credential-use.krateo.io/held-count` is `0`.

Then uninstall; take the provider's grants last. After an out-of-order
uninstall, remove the objects' own finalizers first; then, if the CRD is gone,
run `credentialuse.DisownPrefix` (`DryRun: true` reports what it would strip)
or remove by hand the `credential-use.<lowercase kind>.<group>/<namespace>` finalizers,
the annotation with the same key, the kind's `credential-use.krateo.io/k-<hash>`
label, and the `credential-use.krateo.io/held` label once no `credential-use.`
finalizer is left. If the CRDs are deleted while the provider runs and the
objects are gone, it releases what they held by itself: keep it, and its RBAC,
until nothing carries the kind's finalizer (§6.4, §11.5). Its metrics don't
tell: they count what the process caches.

If the count stays `unknown`, or the flag doesn't come, the CRD's Warning events
say why (§7.4):

- `CredentialUseDisownUnreadable`: an object the index or a record names can't be
  read. Grant `get` on each object the event names, or, no sooner than the
  provider's largest timeout plus 60s after the switch was last changed, confirm
  by hand that it carries no finalizer of the kind and add it to the index's
  `credential-use.krateo.io/vouched` annotation. Removing the line alone is
  undone by the tamper check.
- `CredentialUseIndexLost`: the index was lost while the switch is on. Follow the
  event's two steps: remove the kind's finalizers from the objects the switch
  covers, then set `credential-use.krateo.io/vouched-index` on the index
  ConfigMap to `<uid>/<kinds>`, with `kubectl annotate --overwrite`.
- `CredentialUseDisownIndexFull`: raise `SharedOptions.IndexMaxLines`, or correct
  the record of the holder the event names.
- `CredentialUseDisownRestarted`: something keeps writing the CRD, which starts
  Disown's run over. Find and stop the writer; the event names the last three.

**Vouches name the kinds** (decided 2026-11-25). An entry of `vouched` is
`<namespace>/<Kind[.group]>/<name>`, or `<namespace>/<Kind[.group]>/*` for every
object of a kind in a namespace, once none of its objects of that kind carries
the holder kind's marker label. A bare `<namespace>/*` is malformed: it is
ignored, and logged once per value. Write the kind as the index's lines do, `Kind`
or `Kind.group`, with no version. Remove the vouches again once Disown has
settled.

**A set `disown` switch that parses to nothing**, such as `,`, never settles: it
keeps a Kind's controller up (see
[scale to zero](#credential-use-and-scale-to-zero)) until it is removed (decided
2026-11-25).

### Stopping

A graceful stop drains the index's queued checks (PerName; decided 2026-11-25),
bounded: it ends 10s after the last moment the loops or a call begun before the
stop was busy, and at most 25s after the loops end (that 10s plus the 15s an
acquirer call is given). Set the manager's `GracefulShutdownTimeout` to **25s or
more**: under about 25s the drain is cut short (#146). A **kill stays the
accepted case** (accepted 2026-11-24, §10.6.3): a check the process owed is lost, and the object may
be held without its line until the holder's next pass adds it. The startup
recheck doesn't find it. What the drain leaves undone is logged with how many.

### Limits

Each limit that was decided carries its date. The others are of the design.

- **The Disown kill path.** In PerName mode, a process that is pruning an
  object's line as the switch is set, and is killed between the removal and the
  re-add, can leave a hold that Disown settles over (decided 2026-11-24, §7.4,
  §10.6.3). A hold that lands later than Disown's landing margin isn't covered
  either. Remove the finalizer by hand, `credential-use.<lowercase kind>.<group>/<namespace>`,
  or run `DisownPrefix` with `Names` once the CRD is gone.
- **A paused holder** keeps its holds, whatever the target's owner does, and so
  does a holder no controller reconciles (decided 2026-11-24, §11.3). The way out:
  unpause it (deleting it isn't enough while it is paused), remove the finalizer
  from the target by hand, or use the Disown switch.
- **A protected kind's CRD recreated with another scope** (decided 2026-11-24):
  restart the providers that track it. Until then a ref with a namespace of a
  kind a process still maps cluster-scoped is Missing without a call: a new
  holder connects without its hold, and in PerName a hold may be counted as gone.
- **Unmappable lines.** An index line whose kind doesn't map keeps the Kind's
  controller up, as a set switch that parses to nothing does (decided 2026-11-26).
  The index keeps the lines of a kind taken out of PerName or uninstalled. Once
  its holds are found another way or are gone with its CRD, remove its lines by
  hand:

  ```sh
  kubectl -n <IndexNamespace> edit configmap cu-index-<lowercase holder kind>.<holder group>
  # in data.entries, delete every line that starts with <group>/<Kind>/
  ```
- **A rollout that changes which refs `RefsFunc` names.** Two versions that name
  different refs aren't "safe per pass": the version that names fewer rewrites the
  record without what only the other names, and that object is released then,
  if its Secret or namespace is being deleted. It is lost if the Secret or its
  namespace is deleted before the other version's next pass (design §7.3). The rule: finish
  the rollout, so that no process of the version that names fewer refs still runs
  passes, before deleting Secrets or namespaces of the kind's holders. It is a
  documented limit of this release.
- **Conflicting refs.** When refs of one holder name one object more than once,
  the first wins, and a later one that asks for more (to hold an object whose
  first ref was Guaranteed) isn't done. It is logged once per holder, object
  and thing asked for, and counted in `credentialuse_refs_conflicting_total{asked}`
  (§9.1).
- **The standby cap.** A process that waits for leader election runs its
  informers but not its write loops, so nothing drains its release queue.
  While no write loops run, what waits is bounded: 65,536 names, 32 MiB of them,
  and 16,384 keys across all holder kinds. Past it the key whose names take the
  most is collapsed, and its namespace's list waits instead. When the loops
  start, whatever collapsed is made up for: every object of each `LabelScoped`
  protected kind is listed once (metadata only, 500 to a page), so a process with
  more than 65,536 held objects and recorded names lists at **each** start.
  Needs `list` on each protected kind. What it doesn't find: a name of a kind that
  left `Protected` and no record names any more, and a PerName hold without its
  index line (§6.2, "Without the write loops").
- **Release cost.** A release batch reads at most 64 objects a turn and the rest
  waits behind other keys: a teardown of 200 held Secrets makes 4 lists where it
  made one. It delays, and never loses, a release (§6.2).

### Credential use and scale to zero

A Kind that holds credentials must not sit at 0 replicas: nothing releases then.
`scaletozero.ReconcileWithOptions` and `ExistsWithOptions` take options, and
`Reconcile` and `Exists` keep their signatures. `HeldWithOptions` checks only the
holds and the switch, for an owner whose instances may rightly remain, as
core-provider's `Undeploy` on its `SkipCRD` path.

- `WithHeld(protected...)` counts a protected object that carries a finalizer of
  the Kind's, found by the Kind's marker label (for `LabelScoped` kinds), and a
  `disown` switch that hasn't settled.
- `WithHeldIndex(index)` reads the index ConfigMap and GETs the objects its
  lines name (for `PerName`). A missing or unreadable index is an error, never
  false.
- `WithHeldFilter(accept)` counts only the holds `accept` takes, and still stops
  at the first (decided 2026-11-26). It doesn't apply to the Disown switch.
- The two are independent and add up: a hold found by either counts (decided
  2026-11-25). A holder kind whose protected kinds are all `LabelScoped` passes
  `WithHeld` with them, one whose kinds are all `PerName` passes `WithHeldIndex`,
  and one that mixes the modes passes both: `WithHeld` with its `LabelScoped`
  kinds, and `WithHeldIndex` for the others. `WithHeld` with no kind is an error.
- `WithRESTMapper(m)`: `ExistsWithOptions` takes a `client.Reader`, which
  carries no mapper, so it needs this beside either option.
  `ReconcileWithOptions` takes its client's.

Every failed read is an error, never "nothing held", so the controller stays up.
Grants, beyond the plain helper's: `get` on the holder kind's CRD (by
`resourceNames`); with `WithHeld`, `list` on each protected kind, cluster-wide;
with `WithHeldIndex`, `get` on the index ConfigMap and `get` on each object its
lines name (a `resourceNames` rule can grant it).

### Telemetry

With tracing on, each pass that acquires records a `credentialuse.acquire` event
on the reconcile span (settled 2026-11-22; the
attributes, and the closed sets of `outcome` and reasons, are in
[the tracing design](./design-tracing.md#45-events-on-the-root)). Counts describe
**one pass**: read the latest event per resource. A kept ref that the pass dropped
is in `.unheld.ConsentWithdrawn`. The metrics are `credentialuse_*`, labelled
`holder_kind` (`pkg/telemetry` records them; [api](./api.md) lists the
instruments), and the events are `CredentialUseDegraded`, `CredentialRefNotHeld`,
`CredentialConsentWithdrawn`, `CredentialConsentWildcardIgnored`,
`CredentialReleaseRejected`, `WaitingForCredentialUsers`, `CredentialDeleting`,
`CredentialUseIndexFilling` and `CredentialUseIndexTampered`, besides Disown's
four.

## Developing this library

```sh
make test       # go test -v ./...
make generate   # regenerate deepcopy (controller-gen, build tag `generate`)
make lint       # golangci-lint run
```

`make kind-up` / `make kind-down` manage a local kind cluster for manual
testing of consumers; the library's own tests need no cluster. The
`pkg/scaletozero` tests need envtest's kube-apiserver and etcd binaries:
`export KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0)`, with
setup-envtest from controller-runtime's release-0.25, as CI pins it in
`.github/workflows/run-tests.yaml`. Without it they skip locally, and fail
under `CI`.
