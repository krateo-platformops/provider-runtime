---
type: Usage
title: provider-runtime — usage
description: How to consume provider-runtime as a Go module and the minimal code to build a provider on it.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-get, usage]
timestamp: 2026-11-04T00:00:00Z
---

# Usage

provider-runtime is consumed as a plain Go module. It requires Go ≥ 1.26
(see `go.mod`; since v1.7.0) and pins `sigs.k8s.io/controller-runtime` v0.23.x / `k8s.io/*`
v0.35.x — your provider's controller-runtime version must be compatible.

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
go get github.com/krateo-platformops/provider-runtime@v1.8.0
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

v1.8.0 makes the Ready contract explicit (see
[overview](./overview.md#the-ready-contract-since-v180)). When `Connect` or
`Observe` fails, the runtime now sets `Ready` to `Unknown/NotObserved`. The
next successful `Observe` must set `Ready` again, because the runtime won't.
A provider adopting v1.8.0 must:

1. **Set `Ready` on every successful `Observe` path**, including
   `ResourceExists: false`, drift and early "nothing to do" returns. A path
   that doesn't set it leaves `NotObserved` in place after a failure, and the
   runtime reports it as a `ReadyNotSetAfterObserve` Warning event and an
   Error log.
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
- **Metrics** — `telemetry.Setup(ctx, log, telemetry.Config{Enabled: true})`
  returns `(*Metrics, shutdown, error)`; pass the metrics handle to
  `reconciler.WithMetrics` and `controller.Options.QueueWaitRecorder`.
- **Rate limiting** — `ratelimiter.NewController()` for per-controller
  defaults, `ratelimiter.LimitRESTConfig(cfg, rps)` to bound client-side QPS,
  `workqueue.NewExponentialTimedFailureRateLimiter` for time-expiring
  per-item backoff.

## Developing this library

```sh
make test       # go test -v ./...
make generate   # regenerate deepcopy (controller-gen, build tag `generate`)
make lint       # golangci-lint run
```

`make kind-up` / `make kind-down` manage a local kind cluster for manual
testing of consumers; the library's own tests need no cluster.
