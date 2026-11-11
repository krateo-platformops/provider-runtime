---
type: Library
title: provider-runtime — index
description: The map of the provider-runtime documentation bundle, the Go library every Krateo provider controller is built on.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, controller-runtime, managed-resource, reconciler]
timestamp: 2026-11-10T00:00:00Z
---

# provider-runtime

provider-runtime is the Go library Krateo providers are built on: a
managed-resource reconciliation runtime layered over
`sigs.k8s.io/controller-runtime`. It supplies the generic reconcile loop
(Observe → Create/Update/Delete against an external system), the shared
condition/annotation vocabulary (`Ready`/`Synced`, `krateo.io/external-name`,
`krateo.io/management-policy`, …), logging in the OTel log model, OTLP metrics,
and rate-limiting / workqueue utilities. Consumers implement an
`ExternalClient` for their external system; the library drives everything else.
It is consumed as a plain Go module — no image, no chart, no CRDs shipped.

- [overview](./overview.md) — package design and the managed-resource reconcile flow
- [usage](./usage.md) — `go get` + the minimal code to build a provider
- [configuration](./configuration.md) — build tags, environment, and the option surface the library reads
- [api](./api.md) — the exported Go API surface, package by package
- [examples](./examples.md) — index of runnable examples
- [release](./release.md) — how a version ships (git tag, Go module proxy)
- [log](./log.md) — curated history
- [design: scale to zero](./design-scale-to-zero.md) — a helper owners call to run a per-Kind controller Deployment at 0 replicas while its Kind is empty (implemented, unreleased)
- [design: rate-limit governor](./design-rate-limit-governor.md) — automatic slow-down under server-side rate limits and controller CPU saturation (implemented, off by default; CPU threshold provisional; memory input experimental)
- [design: saturation cost model](./design-saturation-cost-model.md) — sizing local capacity from what reconciles cost against the container's CPU and memory limits, replacing the halve-and-recover step (decided, not built)
- [design: tracing](./design-tracing.md) — a reconcile root span with connect/observe/create/update/delete children, the `krateo.io/traceparent` parent-or-link rule, and W3C propagation on outbound HTTP, from `telemetry.Setup` (built in v1.14.0; D8, every reconcile its own trace, in v1.14.1)
- [design: status write conflicts](./design-status-write-conflict.md) — options for a handler that writes the managed object mid-reconcile, making the runtime's status write conflict (#80; options only, not decided)
