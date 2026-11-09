---
type: Usage
title: provider-runtime — usage
description: How to consume provider-runtime as a Go module and the minimal code to build a provider on it.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-get, usage]
timestamp: 2026-11-09T00:00:00Z
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
    `Reconcile` already binds its per-reconcile Logger to the `ctx` it is
    given. Neither provider-runtime nor controller-runtime starts a span
    there, so that `ctx` holds one only if you wrap the `Reconciler` in a
    reconciler that starts it. Your `ExternalClient` methods get a `ctx`
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
    (`some avg10`) once a second. `local` starts at 1 and rises by 1 every 2s
    until its first cut, so 16 workers take about 30s. A saturated sample
    halves it, never below 1, and after any cut it rises by 1 per clean
    `RecoveryInterval` (30s), the same as API limits. Controllers sharing the
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
    saturated: CPU pressure 63% (some avg10); waiting for capacity (1
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
