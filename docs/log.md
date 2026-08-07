---
type: Log
title: provider-runtime — log
description: Curated chronological history of provider-runtime — notable changes and decisions, newest first.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, history]
timestamp: 2026-08-07T00:00:00Z
---

# Log

Curated history — notable changes and decisions, newest first. Full release
notes live in GitHub Releases; the mechanics are in [release](./release.md).

## 2026-08-07
- Adopted the Krateo Documentation Standard (this bundle) — the repo's first
  documentation, authored fresh from source.

## 2026-08-03 — v1.3.0
- Go module identity migrated to `github.com/krateo-platformops/provider-runtime`
  (full independence from the previous org); `plumbing` dependency moved to its
  new home in the same change.

## 2026-07-10 — v1.2.2
- `pkg/logging` became the shared home for the OTel-aligned log model
  (`NewOTelJSONHandler` / `NewOTelHandler` with OTLP export via the otelslog
  bridge) that consumers previously duplicated.

## 2026-04..07 — v1.2.x
- v1.2.0 added `pkg/telemetry`: OTLP metrics for the reconcile loop
  (`provider_runtime.*` instruments) plus queue-wait instrumentation in
  `pkg/controller`.
- v1.2.1 made all `Metrics` methods nil-safe (panic fix, #22).

## 2026-04-09 — v1.1.0
- Throttled event recorder (`WithThrottledRecorder`) to damp repeated
  reconcile-error events.

## 2026-03-09 — v1.0.0
- Event handling refactored onto the `plumbing` event recorder; the local
  event package removed. First 1.x line — the baseline the Krateo providers
  (core-provider et al.) build on.

## 2024-08-30 — v0.9.0
- Total refactoring (breaking): the reconciler/external-client split took its
  current shape (#9); later 0.x releases added the exponential timed-failure
  workqueue limiter, context logger utilities, and controller-runtime bumps.

## 2023-01-26 — origin
- First commit: generic managed-resource reconciler with condition types,
  credential selectors and deepcopy generation, descending from the
  Crossplane runtime pattern.
