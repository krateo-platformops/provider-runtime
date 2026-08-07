---
type: Usage
title: provider-runtime — usage
description: How to consume provider-runtime as a Go module and the minimal code to build a provider on it.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-get, usage]
timestamp: 2026-08-07T00:00:00Z
---

# Usage

provider-runtime is consumed as a plain Go module. It requires Go ≥ 1.25 (see
`go.mod`) and pins `sigs.k8s.io/controller-runtime` v0.23.x / `k8s.io/*`
v0.35.x — your provider's controller-runtime version must be compatible.

```sh
go get github.com/krateo-platformops/provider-runtime@v1.3.0
```

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
