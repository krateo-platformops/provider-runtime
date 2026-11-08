---
type: Example
title: greeting-controller — a minimal provider built on provider-runtime
description: A compilable managed-resource controller (Greeting CRD + fake external system) showing the full provider-runtime wiring.
resource: github.com/krateo-platformops/provider-runtime
tags: [example, reconciler, managed-resource]
timestamp: 2026-11-05T00:00:00Z
---

# greeting-controller

A minimal but complete provider built on provider-runtime. It reconciles a
`Greeting` custom resource against a fake in-process "external system",
demonstrating everything a real Krateo provider wires up:

- a **Managed type** (`Greeting`) embedding `apis/common/v1.ConditionedStatus`
  and satisfying `resource.Managed`;
- an **`ExternalConnecter` / `ExternalClient`** pair implementing
  Observe / Create / Update / Delete against the external system;
- **`reconciler.NewReconciler`** with functional options
  (`WithExternalConnecter`, `WithLogger`);
- **`controller.DefaultOptions()`** → `ForControllerRuntime()` for queue and
  rate-limiter wiring;
- the **OTel-model JSON logging** handler from `pkg/logging`
  (`NewOTelJSONHandler` + `NewSlogLogger`);
- the **rate-limit governor**, off by default: the connector implements
  `reconciler.RateLimitKeyer` (every Greeting talks to one store, so one key),
  and two flags turn the inputs on —
  `--rate-limit-governor` / `GREETING_PROVIDER_RATE_LIMIT_GOVERNOR` and
  `--saturation-governor` / `GREETING_PROVIDER_SATURATION_GOVERNOR`. The
  saturation flag turns on the local input, CPU only, for all of the
  controller's calls, `Connect` and every `ExternalClient` method; it would do
  so even without the keyer, which only matters for per-endpoint limits. A
  third flag, `--saturation-experimental-memory` /
  `GREETING_PROVIDER_SATURATION_EXPERIMENTAL_MEMORY`, adds the experimental
  memory input, which is not OOM protection. `--rate-limit-max-hold` /
  `GREETING_PROVIDER_RATE_LIMIT_MAX_HOLD` caps how long a refusal holds the
  key (default 1h, 0 for no cap). Nothing else needs setting; see
  [configuration](../../docs/configuration.md#rate-limit-governor-controlleroptionsratelimit).

The external name lifecycle is real: on first reconcile `Observe` reports the
external resource missing, `Create` stamps the `krateo.io/external-name`
annotation, and subsequent reconciles report `Available` (the `Ready`
condition) or drift (`Diff`) when `spec.message` changes.

`Observe` sets `Ready` on every path that returns no error, as the
provider-runtime Ready contract requires (since v1.8.0; see
[overview](../../docs/overview.md#the-ready-contract-since-v180)).
`main_test.go` checks each of those paths with
`reconcilertest.AssertObserveSetsReady`; run it with `go test .`.

## Compile (no cluster needed)

```sh
go build .
```

## Run against a cluster

Preconditions: a reachable cluster in your kubeconfig (e.g. `kind create
cluster`) and permission to create CRDs.

```sh
kubectl apply -f ./crd.yaml
go run .
```

Then, in another terminal:

```sh
kubectl apply -f ./greeting.yaml
kubectl get greeting hello -n default -o yaml
```

The status gains `Ready=True` (reason `Available`) and `Synced=True` (reason
`ReconcileSuccess`) conditions, and the object gains the
`krateo.io/external-name` annotation. Edit `spec.message` and watch the
controller log the `Diff` and update the fake external system. Delete the
`Greeting` and the reconciler removes the finalizer after `Delete` succeeds.
