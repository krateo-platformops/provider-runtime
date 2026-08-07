# provider-runtime

The Go library every Krateo provider controller is built on: a managed-resource reconciliation runtime over controller-runtime.

[![Test and coverage](https://github.com/krateo-platformops/provider-runtime/actions/workflows/run-tests.yaml/badge.svg)](https://github.com/krateo-platformops/provider-runtime/actions/workflows/run-tests.yaml)

## What is this

provider-runtime supplies the generic reconcile loop for managed resources —
Observe → Create/Update/Delete against an external system — plus the shared
condition model (`Ready`/`Synced`), the `krateo.io/*` annotation vocabulary
(external-name, pause, management/deletion policy), OTel-model logging, OTLP
metrics, and rate-limiting/workqueue utilities. Providers implement an
`ExternalClient`; the library drives everything else. Consumed as a plain Go
module — no image, no chart, no CRDs shipped.
Full picture: [docs/index.md](docs/index.md).

## Install

```sh
go get github.com/krateo-platformops/provider-runtime@v1.3.0
```

## Configure

See [docs/configuration.md](docs/configuration.md). Most used:

| Setting | Default | Effect |
|---|---|---|
| `reconciler.WithPollInterval` | `1m` | Speculative re-observe interval per managed resource |
| `controller.Options.MaxConcurrentReconciles` | `1` | Per-controller worker count |
| `telemetry.Config.Enabled` | `false` | OTLP metrics pipeline (`provider_runtime.*` instruments) |

## Examples

- [examples/greeting-controller](examples/greeting-controller) — a minimal compilable provider: `Greeting` CRD reconciled against a fake external system.

## Docs

- [docs/index.md](docs/index.md) — the map
- [docs/overview.md](docs/overview.md) — package design and the reconcile flow
- [docs/usage.md](docs/usage.md) — `go get` + minimal provider code
- [docs/configuration.md](docs/configuration.md) — build tags, env, options, annotations
- [docs/api.md](docs/api.md) — the exported Go API surface
- [docs/examples.md](docs/examples.md) — examples index
- [docs/release.md](docs/release.md) — how a release ships (tag-only)
- [docs/log.md](docs/log.md) — curated history

## Develop & release

`make test` (unit tests, no cluster needed) · `make generate` (deepcopy) · `make lint` — release runbook: [docs/release.md](docs/release.md).
