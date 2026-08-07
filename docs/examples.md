---
type: ExampleIndex
title: provider-runtime — examples
description: Index of the runnable examples shipped with provider-runtime.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, examples]
timestamp: 2026-08-07T00:00:00Z
---

# Examples

- [greeting-controller](../examples/greeting-controller/README.md) — a
  minimal, compilable provider: a `Greeting` CRD reconciled against a fake
  external system, wiring `reconciler.NewReconciler`, an
  `ExternalConnecter`/`ExternalClient`, `controller.DefaultOptions()` and the
  OTel-model logger. `go build .` needs no cluster; running it needs a
  kubeconfig plus `kubectl apply -f crd.yaml`.
