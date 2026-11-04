---
type: Architecture
title: provider-runtime — automatic slow-down under server-side rate limits
description: Design (not implemented) for a per-endpoint governor that slows reconciles when an external API signals a rate limit, aligned with unstructured-runtime's design.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, reconciler, workqueue]
timestamp: 2026-11-05T00:00:00Z
---

# Automatic slow-down under server-side rate limits

**Status:** design, not implemented. **Scope:** provider-runtime. The same
feature for unstructured-runtime is designed by its owner in
[unstructured-runtime#50](https://github.com/krateo-platformops/unstructured-runtime/pull/50).
This document reuses that design's vocabulary on purpose and only spells out
where provider-runtime differs. §8 lists two points that apply to both and
are raised on #50.

## 1. The problem, measured

GitHub Enterprise and Azure DevOps enforce server-side rate limits. Today
operators absorb them by hand: they lower `--max-reconcile-rate`, raise
`--poll`, and re-tune when either side changes. The survey behind this design
(all three orgs, plus the `krateoplatformops` org where the Azure DevOps
provider lives) found:

- **No component slows down when an API says it's rate-limited.** A 429, or a
  GitHub 403 with `X-RateLimit-Remaining: 0`, becomes an ordinary reconcile
  error.
  - In provider-runtime that error goes to controller-runtime's per-item
    exponential backoff of 1s to 60s (`pkg/controller/options.go`).
  - The other workers keep calling the same endpoint at full rate.
- **The signal exists, but nothing uses it.** rest-dynamic-controller already
  reads `Retry-After` and `X-RateLimit-Reset` (`validators.go` `RetryAfter`),
  but only to decide whether a 403 is permanent; the duration it computes is
  discarded.
  - git-provider can't see the headers at all: go-git drops them on 403 and
    hides them on 429.
  - No component reads Azure DevOps's `X-RateLimit-*` headers.
- **The knobs are process-wide.**
  - `--max-reconcile-rate` (used as `MaxConcurrentReconciles`), `--poll`,
    `--sync`, and the min/max error retry intervals.
  - None can tell one endpoint from another: a GitHub Enterprise limit is
    tuned by slowing every resource the provider manages.
- **provider-runtime has no way to slow a provider down.**
  - `Options.GlobalRateLimiter` is set by default, but `ForControllerRuntime`
    never wires it into the queue.
  - The `ratelimiter.New` wrapper rate-limits every reconcile, not only
    throttled ones.
  - There's no requeue-after on the error path either: controller-runtime
    ignores `RequeueAfter` when an error is returned.

## 2. Shared with unstructured-runtime, and kept identical

These are adopted from #50 as they are, so that two controllers reconciling
against the same GitHub Enterprise report the same state in the same words:

- **The signal is reported by the provider, through a context sink, on success
  as well as failure.** Azure DevOps sends `X-RateLimit-Delay` on a
  successful response, before it starts refusing. An error-only channel can
  react only after throttling has begun. The runtime makes no outbound HTTP
  calls of its own (the provider does), so it never parses HTTP.
- **The same `Signal` shape:** `Key`, `RetryAfter`, `ResetAt`, `Remaining`,
  `Limit`, `HasQuota`, `Throttled`. `Report(ctx, Signal)` is a no-op when no
  governor is configured.
- **A governor keyed by endpoint, not global**, with two states.
  - *Throttled:* the gate for that key is closed until the server's own
    deadline, `max(RetryAfter, ResetAt)`. The runtime never guesses a
    shorter wait.
  - *Advisory:* capacity halves (minimum 1).
  - Recovery is additive: +1 per clean `RecoveryInterval` (default 30s), up to
    `MaxConcurrentReconciles`.
- **A deferred pass writes `Synced=False` with reason `ReconcileDeferred`**,
  and a message naming the key and the next attempt as an RFC 3339 time.
  `Ready` is left unchanged. It writes no assessment, so no `NotObserved`
  and no `ReconcileError`. It doesn't stay silent either, so no healthy state
  goes stale without anyone being able to tell.
- **A deferral is not a failure.** It doesn't touch the item's failure
  backoff.
- **Off by default.** With no governor configured, behaviour is identical to
  today's.
- **Configured in the controller, not in any CRD.** The governor's settings
  (`RecoveryInterval`, the ceiling, enabled or not) are controller options,
  with flags and env set by each provider's chart. Nothing goes into a
  managed-resource or RestDefinition schema. oasgen doesn't regenerate existing
  CRDs on upgrade (oasgen-provider#453), so a schema-borne setting would never
  reach kinds that are already deployed.

`ReconcileDeferred` doesn't collide with any provider-runtime reason today:
`Available`, `Unavailable`, `Creating`, `Deleting`, `NotObserved`,
`ReconcileSuccess`, `ReconcileError` and `ReconcilePaused`. It gets a
constant, `ReasonReconcileDeferred`, and a test that pins the string as a
consumer contract.

## 3. Where provider-runtime differs: controller-runtime owns the queue

provider-runtime's reconciler runs inside controller-runtime, so the
mechanics differ from unstructured-runtime's own priority queue:

| Concern | provider-runtime |
|---|---|
| Workers | `MaxConcurrentReconciles`, fixed when the controller starts. The governor limits how many reconciles are *inside a provider call* for a key, not the goroutine count. |
| Requeue on deferral | `return reconcile.Result{RequeueAfter: until - now}, nil`. A nil error means controller-runtime calls `Forget`, so the per-item failure backoff isn't consumed and the requeue happens exactly when the server said. |
| Status write | The deferral sets `Synced=ReconcileDeferred` and writes status once, through the same resourceVersion-guarded `Status().Update` as every other path. A conflict requeues, as today. |
| Where the gate sits | Around **`Connect` and every `ExternalClient` call** (`Observe`, `Create`, `Update`, `Delete`). `Connect` counts too, because some providers call the vendor there: alert-provider calls HyperDX, and git-provider loads credentials. |

## 4. The key must be known before the call: `RateLimitKeyer`

The governor has to decide whether to let a call through *before* making it,
so the key can't come only from a `Signal` the provider reports during the
call. provider-runtime adds one optional interface:

```go
// RateLimitKeyer names the rate-limited scope a managed resource's external
// calls belong to, typically the endpoint host, or host|credential when the
// quota is per identity. It must not do I/O: it is called before every gated
// call, including when the endpoint is throttled.
type RateLimitKeyer interface {
	RateLimitKey(mg resource.Managed) (string, error)
}
```

- A connector or external client that implements it is gated. One that
  doesn't is never gated, so the governor is opt-in per provider as well as
  per process.
- A `Signal` whose `Key` differs from the keyer's key is still applied to its
  own key. A reconcile can learn about an endpoint it didn't expect, for
  example after a redirect.
- An error from `RateLimitKey` is a provider defect. The reconcile proceeds
  ungated and the runtime logs once per object. Failing closed would turn a
  keying bug into an outage.
- **Key on the credential's reference, never its value.** Where the quota is
  per identity, use the reference (for example the Secret's namespace and
  name), not the token or anything derived from it. Credentials are rotated by
  secret managers outside Krateo, often. A key that changes on rotation would
  silently split one budget into several, each believing it has the full quota.
  (Raised by oasgen-provider, whose rest-dynamic-controller resolves auth per
  request.)
- **The host comes from configuration, not from a request.** For
  rest-dynamic-controller it's the RestDefinition's server, or
  `spec.serverURL`, already resolved by its host policy. Per-operation
  `servers` are refused there, so there's one host per RestDefinition.

## 5. The gate must never block a worker

`Acquire` doesn't wait. When the key is throttled, or at capacity, it returns
a deferral immediately:

- **Throttled:** defer until the server's deadline.
- **At capacity:** defer for a short, jittered interval, by default a random
  value between 1s and the current `RecoveryInterval`. It isn't a deadline,
  so the condition message says "waiting for capacity" rather than a time.
  The random value comes from `crypto/rand`, as in unstructured-runtime#53:
  `math/rand` trips gosec G404, and a scanner that passes needs no `#nosec`
  for every later reviewer to re-justify. If the read fails, the full
  `RecoveryInterval` is used, never a shorter delay.

A blocking gate would park a worker on a throttled key. With
`MaxConcurrentReconciles` workers and one throttled endpoint, every worker
can end up parked there, and resources on healthy endpoints stop reconciling
too. That's the global slow-down this design exists to avoid, brought back by
the gate.

## 6. Signalling helpers for providers

The runtime core never parses HTTP. But every provider that talks to GitHub
or Azure DevOps would otherwise rewrite the same header parsing. So there's an
optional, separate package, `pkg/ratelimit/httpsignal`: an `http.RoundTripper`
wrapper that a provider puts around its client, which reports a `Signal` from
the response and passes it through unchanged.

- **Headers it reads:** `Retry-After` (seconds and HTTP-date) and
  `X-RateLimit-Remaining`, `-Limit` and `-Reset` (GitHub and Azure DevOps).
  It also reads `X-RateLimit-Delay` and `X-RateLimit-Resource` (Azure DevOps,
  advisory) and `X-RateLimit-Used`.
- **When a response counts as throttled:** a 429, or a 403 with
  `Remaining: 0` or with `Retry-After`.
  - On a 403 or 429, `Retry-After` is authoritative on its own. GitHub's
    secondary limits arrive that way, sometimes *without*
    `X-RateLimit-Remaining: 0` (github-provider-kog's owner pointed this out),
    so a check keyed only on `Remaining` or `Reset` would miss them.
  - A secondary-limit 403 with neither `Retry-After` nor `Remaining: 0` is
    signalled only in the response body. Body parsing isn't in v1, so that
    case stays the provider's to classify.
- **How the key is chosen:** the request URL's host by default, or
  `host|credential` where the quota is per token. GitHub's is per token: all
  32 of github-provider-kog's RestDefinitions share one host and one budget
  per token. A provider can override the key per request through the request
  context.
- **Vendor headers can change.** The helper's header list is verified against
  each vendor's current documentation when it's implemented, not taken from
  this document. Its tests use recorded responses.

go-git doesn't expose response headers, so git-provider would plug this
helper in through go-git's HTTP transport. That's git-provider's change; this
design only makes it possible.

## 7. Observability and verification

- **Metrics:** the same four as #50, under the `provider_runtime.ratelimit.*`
  prefix: per-key capacity, deferrals, seconds to reset, and signals by
  kind.
- **An event:** a Normal `ReconcileDeferred` event on the first deferral of an
  object per throttle window. It goes through the throttled recorder, so a
  long window doesn't flood events.

Tests, each written so it fails without the behaviour it checks:

| Requirement | Test |
|---|---|
| A deferral writes no assessment | A deferred pass leaves `Ready` byte-identical, writes `Synced=False/ReconcileDeferred`, and calls no `ExternalClient` method. |
| A deferral is visible and dated | The message parses to the deadline. One status update is the denominator. |
| A deferral doesn't consume backoff | It returns `RequeueAfter` with a nil error. The item's `NumRequeues` stays 0 across two deferrals. |
| Keys are independent | With two keys and one throttled, the other key's calls go through at once. |
| No worker is parked | With `MaxConcurrentReconciles=N` and one throttled key, N reconciles of a second key still run concurrently. |
| The server's deadline is respected | `RetryAfter=30s` defers for at least 30s, never less. |
| Recovery is additive | Capacity returns to the ceiling in N steps of `RecoveryInterval`, not one. |
| Off by default | With no governor: no gate, no deferrals, and the existing suite passes unchanged. |
| Providers really report | A conformance helper in `pkg/reconciler/reconcilertest`, alongside `AssertObserveSetsReady`: feed the provider's client a recorded 429 and a recorded Azure DevOps advisory 200, and assert that a `Signal` was reported. |
| The reason string can't drift | A contract test pinning `"ReconcileDeferred"`, and naming its consumers. |

**What a clean rollout cannot verify** (the
[org convention](https://github.com/krateo-platformops/.github/blob/main/CONTRIBUTING.md)):

- **The path:** throttled and advisory deferral.
- **Why a rollout can't reach it:** a healthy endpoint sends no limit signal,
  so a rollout without real throttling exercises none of it.
- **The instrument:** the unit tests above, plus the conformance helper run
  against recorded vendor responses.
- **What a green run doesn't cover:** the vendor's live header behaviour, which
  can change without notice, and how throttling interacts with other clients
  that share the same quota outside Krateo.

## 8. Two points that apply to unstructured-runtime#50 too

1. **#50 never says how the key is known before the call.** It says "each
   worker calls `Acquire` immediately before `externalClient.Observe`", but
   the key is only supplied in the `Signal` the handler reports *during* the
   call. §4's `RateLimitKeyer`, which names the key from the object without
   I/O, closes that gap. Learning keys from past signals would leave every
   object's first call ungated, and would go wrong when an object changes
   endpoint.
2. **#50's `Acquire` blocks.** It "blocks until this key may make a call".
   With a fixed worker pool, every worker can block on one throttled key and
   starve the healthy ones. §5's non-blocking gate, which defers immediately,
   avoids that.

## 9. Open questions

1. **Where the shared types live: decided, duplicated.** core-provider, as
   plumbing's owner, chose to duplicate `Signal` and `"ReconcileDeferred"` in
   each runtime. Each repo gets a contract test that pins the literal and
   names the other runtime as a consumer, in the shape `ReasonZeroEvidence`
   already uses. The reasons:
   - a shared type would make every vocabulary change wait for a plumbing
     release and two bumps;
   - the compiler would only check that the field names match, not that both
     runtimes read `Throttled` or `ResetAt` the same way, which needs a test
     either way.

   If the two ever have to change in lockstep more than once, that's the
   evidence for a shared module. (provider-runtime already depends on plumbing,
   at v1.13.0, so the extra dependency isn't the cost; the release coupling
   is.)
2. **Should `httpsignal` (§6) be shared too?** It's useful to providers on
   both runtimes, for example rest-dynamic-controller's vendor calls. Same
   question as 1.
3. **Per-verb cost.** #50 defers weighting verbs by cost for v1. This design
   agrees, for the same reason: guessing weights would be worse than not
   weighting.
4. **Which callers are actually in scope.**
   - **GitHub through github-provider-kog: in scope, through
     unstructured-runtime.** That chart has no plugin. Its 32 RestDefinitions
     call `https://api.github.com` directly from rest-dynamic-controller, so
     GitHub's headers reach rest-dynamic-controller itself, whose
     `RetryAfter` already parses them. GitHub Enterprise arrives with
     oasgen-provider#374 (`spec.serverURL`, targeted at 0.52.0). No change to
     the RestDefinitions is needed for this design: the signalling and keying
     live in rest-dynamic-controller.
   - **Git over HTTP (GitHub Enterprise, Azure DevOps) through git-provider:
     in scope, on provider-runtime.** It needs the `httpsignal` transport
     plugged into go-git (§6), which is git-provider's change.
   - **Azure DevOps through the `azuredevops-provider` and
     `*-rest-dynamic-controller-plugin` repos: out of scope, with no known
     owner.** They live only in the old `krateoplatformops` org, which
     github-provider-kog's owner describes as superseded. The provider is on
     provider-runtime v0.7.0 under that org's old module path, and the
     plugins drop the vendor's response headers before rest-dynamic-controller
     sees them. Whoever takes on Azure DevOps REST would need to move it to
     v1.x and forward the headers.
