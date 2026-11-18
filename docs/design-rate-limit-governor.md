---
type: Architecture
title: provider-runtime — automatic slow-down under rate limits and controller saturation
description: Design, implemented in provider-runtime, for a governor that slows reconciles when an external API signals a rate limit or the controller's own CPU is saturated, with an experimental memory input, aligned with unstructured-runtime's design.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, reconciler, workqueue]
timestamp: 2026-11-13T00:00:00Z
---

# Automatic slow-down under rate limits and controller saturation

**Status:** implemented in provider-runtime (`pkg/ratelimit`,
`pkg/ratelimit/httpsignal`, `pkg/reconciler`, `pkg/controller`), off by
default. It ships rate-limit and CPU slow-down. **It does not ship memory
protection:** the memory input is in the code, off by default and experimental,
because it did not prevent OOMKills on the kind re-measurement. The saturation
settings were decided by the user on 2026-11-05 from the measurements recorded
in [configuration](./configuration.md#the-pressure-thresholds-are-provisional).
The CPU pressure threshold (50%) stays provisional until core-provider
validates it (open question 5). Where the implementation chose something this
design leaves open, §11 says so, and marks what the user decided. **Scope:** provider-runtime. The same
feature for unstructured-runtime is designed by its owner in
[unstructured-runtime#50](https://github.com/krateo-platformops/unstructured-runtime/pull/50).
This document reuses that design's vocabulary on purpose and only spells out
where provider-runtime differs. §9 lists two points that apply to both and
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
  goes stale without anyone being able to tell. **One exception, decided by the
  user:** a deferral on `local`'s startup ramp (§7) is quiet. The ramp lasts
  about 30s and isn't a state of the object, and reporting it turned every
  healthy object `Synced=False` on every restart. unstructured-runtime has no
  `local`, so this doesn't change what the two share.
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
| Status write | The deferral sets `Synced=ReconcileDeferred` and writes status once, through the same resourceVersion-guarded `Status().Update` as every other path. A conflict requeues, as today. A deferral on `local`'s startup ramp sets nothing (§7). When the object would be written back exactly as it was read (an at-capacity deferral repeats the same message every pass), the write is skipped and the pass only requeues. A throttled deferral that names a new deadline still writes. |
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

- A connector or external client that implements it is gated per endpoint.
  One that doesn't is not gated per endpoint, so the per-endpoint governor is
  opt-in per provider as well as per process. The keyer is needed only for
  per-endpoint API limits. With saturation enabled (§7), every reconcile
  still needs a `local` slot, keyer or not, taken by the queue before the pop.
- A `Signal` whose `Key` differs from the keyer's key is still applied to its
  own key. A reconcile can learn about an endpoint it didn't expect, for
  example after a redirect.
- An error from `RateLimitKey` is a provider defect. The reconcile proceeds
  without per-endpoint gating (still bounded by `local` when saturation is on)
  and the runtime logs once per object. Failing closed would turn a
  keying bug into an outage.
- **Key on the credential's reference, never its value.** Where the quota is
  per identity, use the reference (for example the Secret's namespace and
  name), not the token or anything derived from it. Credentials are rotated by
  secret managers outside Krateo, often. A key that changes on rotation would
  silently split one budget into several, each believing it has the full quota.
  (Raised by oasgen-provider, whose rest-dynamic-controller resolves auth per
  request.)
- **One object can reach several scopes: `RateLimitKeysProvider`.** A sync
  between two endpoints makes calls to both in every reconcile, and neither
  key alone describes it. Gated on one, it keeps calling the other through a
  429. Reporting both under one key slows the healthy endpoint and names the
  wrong one on the object. A key per pair splits a shared quota. So a provider
  can return all of an object's keys with `RateLimitKeys(mg) ([]string, error)`,
  and a call needs a slot in each, all or nothing (§5). Every rule above holds
  for each key. See §11, item 17.
- **The host comes from configuration, not from a request.** For
  rest-dynamic-controller it's the RestDefinition's server, or
  `spec.serverURL`, already resolved by its host policy. Per-operation
  `servers` are refused there, so there's one host per RestDefinition.

## 5. The gate must never block a worker

`Acquire` doesn't wait. When the key is throttled, or at capacity, it returns
a deferral immediately:

- **Throttled:** defer until the server's deadline, cut to `MaxHold` (1h by
  default, 0 for no cap) after the refusal when the server asks for longer.
  Added in v1.12.0: an unbounded hold let a broken or hostile server freeze
  every object on a key, deletions included
  ([configuration](./configuration.md#rate-limit-governor-controlleroptionsratelimit)).
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

## 7. Slowing down when the controller itself is saturated

A rate limit is the external API saying "too fast". The controller's own pod
can say it too: CPU starved, or memory close to its limit. CPU ships; memory
is an experimental input that is off by default (§11). Today that is also
tuned by hand, with the same `--max-reconcile-rate`. Too high, and a burst of
reconciles gets the pod OOMKilled or CPU-throttled until its probes fail.
Too low, and it idles on a quiet day. The same governor handles both, with a
second, process-wide input.

**What is measured: the container's cgroup, never the node.** The controller
can only shed its own work, so it reads the cgroup v2 files of its own
container:

| Input | File | Meaning |
|---|---|---|
| CPU pressure | `cpu.pressure`, the change of `some total` over 10s | Share of the last 10s in which a runnable task waited for CPU. It covers throttling by the CPU limit and contention on the node alike. The kernel's `some avg10` isn't used: it is an exponential average that still holds most of a stall 10s after it ended (§11 item 19). |
| Memory working set (**experimental, off by default**) | `memory.current` minus `inactive_file` from `memory.stat`, against `memory.max` | The same working set kubelet uses for eviction and the OOM killer acts on. Page cache the kernel can drop is excluded, so a large but reclaimable cache does not slow anything down. It did not prevent OOMKills when measured (§11). |

`memory.pressure` (`full avg10`) is not an input. It was in this design to
catch thrashing before the working set reaches the limit, but on the kind
measurement it read 0.00 up to every OOMKill. A Go controller's memory is
anonymous, and with no swap the kernel has nothing to reclaim, so nothing
stalls before the OOM killer acts. The user decided to drop it (§11).

**Where it hooks in: the queue, with a process-wide capacity.** The governor
already keeps a capacity per key. Saturation adds one more capacity, `local`,
which bounds the reconciles in flight across every controller sharing the
governor. It is taken before the pop, not inside the reconcile: low capacity
then behaves like fewer workers, the queue's order is the order work runs in,
and nothing is popped that can't run. The first implementation gated each call
inside the reconcile, so a popped item that found `local` full was deferred for
1s to 30s and slept through any slot that freed meanwhile, while whatever came
next in the queue took it: the inversion core-provider found in
unstructured-runtime#72. The design of the fix is
[design-local-slot-before-pop](./design-local-slot-before-pop.md):

- `ForControllerRuntime` builds each controller's queue as a gated queue
  (`ratelimit.NewGatedQueue`) when saturation is on. A worker asks for a slot
  only when its queue has an item to hand out, so an idle worker holds nothing,
  and one controller's idle workers can't starve another's backlog. The slot is
  given back in `Done`, which controller-runtime calls when the reconcile
  returns, on every path including a panic, so it covers the whole reconcile.
- When several queues have work, freed slots go round-robin across them.
- One sampler goroutine reads the files every second (default) and publishes
  the result atomically. Neither `Acquire` nor the queues do I/O. A faster
  period only matters to the experimental memory input. It doesn't make CPU
  pressure react faster: it is measured over a 10-second window.
- A call inside the reconcile needs a slot in its keys only. A call without a
  key isn't gated in the reconciler at all.
- *Slow start:* `local` starts at 1, not at the ceiling, and rises by 1 per
  clean `StartupRampInterval` (2s by default, not tied to the sample period),
  so 16 workers are all admitted after about 30s.
- *CPU-saturated* (§11 item 19): CPU pressure over its threshold for a whole
  10s window halves `local`, with a minimum of 1, at most once per window. The
  next cut needs a whole window measured after the last one, so no cut is made
  twice on the same stall. Pressure over the threshold that doesn't cut (in a
  governor's first 10s, or within 10s of the last cut) holds `local`: it isn't
  a clean sample. One reconcile always keeps going, so a saturated controller
  still finishes deletions and still makes progress. Reconciles already running
  keep their slots.
- *Recovery after a CPU cut:* +1 per clean `StartupRampInterval`, up to the
  ceiling. A rise is granted to a waiting worker at once.
- *Recovery after a cut by the experimental memory input:* +1 per clean
  `RecoveryInterval` (30s by default), the same as §2 for API limits.
- *Experimental memory input, soft ratio:* at 80% of `memory.max` by default
  (provisional), `local` halves, once per sample.
- *Experimental memory input, hard ratio:* at 90% of `memory.max` by default
  (provisional), `local` drops to 1 at once rather than halving. The one-reconcile
  floor still applies, and that reconcile can be the OOMKill (§11).

**No requirement on providers.** The slot covers the whole reconcile, so a
provider's expensive work is bounded wherever it happens. The first
implementation bounded only the work inside the gated calls and had to require
providers to keep it there.

**The controller's own saturation is not written on the object.** It isn't a
state of the object, so a full `local` defers nothing: the item waits in the
queue, with no `Synced=False/ReconcileDeferred`, no event and no status write.
It shows in `provider_runtime.ratelimit.local.capacity` and the input gauges.
Key deferrals, throttled and at capacity, keep the contract of §5.

**The trap that stays: a sampler that never started.** Only the sampler raises
`local`, so a Governor whose sampler never ran stays at 1 and runs one reconcile
at a time, now with no sign on the object. Until the first sample every input
reports as disabled, so `saturation.input.enabled` is 0.
`SetupRateLimitGovernor` adds the Governor to the manager; a provider calling
`ratelimit.New` must do that itself. `Start` logs an error when more
reconcilers registered than controllers had their queue gated, which catches a
controller built without `ForControllerRuntime`, and warns when a second
Governor with saturation runs in the same process.

**When the input isn't there, the input is off, not the controller.**

- With no cgroup v2, or no PSI files (cgroup v1, some kernels, local runs
  outside a container), that input is disabled. The runtime logs this once at
  Info and reports it as a metric. Failing closed would stop a controller
  because of how its node was built.
- With the experimental memory input on and no memory limit (`memory.max` is
  `max`), the working-set check is off and CPU pressure still applies.
- With no CPU limit, CPU pressure still measures contention on the node,
  which is the case where slowing down helps.

**Out of scope here:**

- `GOMEMLIMIT`, for now. Setting it from the container limit makes the Go GC
  work harder before the OOM killer acts. It's a deployment setting each
  provider's chart owns, and a runtime library shouldn't change it at import
  time. §11 item 14 proposes setting it when the memory input is on.
- The Kubernetes API server's own overload. client-go already honours API
  Priority and Fairness 429s with `Retry-After`.
- A vendor server that is saturated rather than rate-limited. That is a
  vendor's 503 with `Retry-After`. §6's helper should treat it like a 429,
  and the implementation adds that case to its header table.

**Configuration**, like the rest, is controller options only, with flags and
env set by each chart and nothing in any CRD:

- whether saturation is enabled at all, independently of the rate-limit
  input; with it on, `UsePriorityQueue=false` is an error, because the gated
  queue is a priority queue;
- the sample period, 1s by default;
- the startup ramp interval, 2s by default;
- the CPU pressure threshold, 50% by default (provisional);
- whether the experimental memory input is on, off by default;
- that input's working-set soft and hard ratios, 80% and 90% by default
  (provisional).

The defaults come from measurements on a kind cluster with a provider under
synthetic load, and were decided by the user from them (§11). The CPU pressure
threshold stays provisional until core-provider validates it. This is open
question 5.

## 8. Observability and verification

- **Metrics:** the same four as #50, under the `provider_runtime.ratelimit.*`
  prefix: per-key capacity, deferrals, seconds to reset, and signals by
  kind.
  Saturation (§7) adds `local` capacity, each input's current value, and
  deferrals by cause (`throttled`, `capacity`). A disabled
  input reports as disabled, not as zero, since zero reads as healthy.
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
| A multi-key call is all or nothing | With either of its keys throttled or full, no external call is made, the object names that key, and the keys with room hold no slot afterwards. It takes one local slot, and waits for the latest of several deadlines. |
| No worker is parked | With `MaxConcurrentReconciles=N` and one throttled key, N reconciles of a second key still run concurrently. |
| The server's deadline is respected | `RetryAfter=30s` defers for at least 30s, never less. |
| A hold is bounded | A deadline past `MaxHold` is cut to it, logged once per key and counted; 0 is no cap; a refusal after the cut hold holds the key again. |
| Recovery is additive | Capacity returns to the ceiling in N steps of `RecoveryInterval`, not one. |
| Off by default | With no governor: no gate, no deferrals, and the existing suite passes unchanged. |
| Providers really report | A conformance helper in `pkg/reconciler/reconcilertest`, alongside `AssertObserveSetsReady`: feed the provider's client a recorded 429 and a recorded Azure DevOps advisory 200, and assert that a `Signal` was reported. |
| Saturation halves, floors at 1, recovers additively | Feed cgroup fixtures (testdata copies of `cpu.pressure`, `memory.current`, `memory.stat`, `memory.max`). The working set over its soft ratio halves `local` once per sample, never below 1. CPU pressure is fed as a cumulative stall: over the threshold for a whole window it halves `local` once per window, never below 1, and `local` then rises by 1 per ramp interval. A fresh governor's `local` is 1 and reaches N after N-1 clean samples. |
| The working set excludes reclaimable cache | A fixture with `memory.current` at 98% of the limit, mostly `inactive_file`, does not slow down. The same total with no `inactive_file` does. |
| The hard memory ratio drops to 1 at once | At 90% working set, `local` goes from N to 1 in one sample. |
| A missing input is disabled, not saturated | With no PSI file, or `memory.max` set to `max`, the controller ramps to full capacity, and the metric says the input is disabled. |
| Saturation bounds calls without a keyer | A provider that implements no `RateLimitKeyer` is bounded by `local`. |
| An identical deferral writes nothing | Two consecutive at-capacity deferrals make one status update; a throttled deferral with a new deadline still writes. |
| The hot path does no I/O | `Acquire` makes no syscalls, checked with a sampler stub that fails the test if it is read outside its tick. |
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
- **Saturation (§7) has the same gap.** A healthy rollout never saturates.
  The instrument is the fixture tests plus the kind measurement from
  open question 5. A green run doesn't cover real kernels' PSI behaviour
  across node images.

## 9. Two points that apply to unstructured-runtime#50 too

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

## 10. Open questions

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
5. **Saturation thresholds (§7).** Measured, and decided by the user (§11): CPU
   ships, with its threshold provisional until core-provider validates it, and
   the memory input stays experimental. The defaults are measured,
   not chosen: a kind cluster, a provider built on this
   runtime, and synthetic load until the pod is throttled or close to its
   memory limit. The implementation PR records the thresholds where
   slowing down prevented the OOMKill or probe failure without idling the
   controller. The same question goes to unstructured-runtime's owner, so
   both runtimes slow down at the same point.

## 11. Implementation notes

Where the implementation made a choice this design leaves open:

1. **A throttled signal also drops the key's capacity to 1**, besides closing
   the gate until the deadline. This is unstructured-runtime#50 §3, which §2
   adopts. When the hold lifts, the key resumes at one call and recovers
   additively, rather than at the ceiling.
2. **A future `ResetAt` next to a healthy quota is not advisory.**
   `ratelimit.Classify` reads a non-throttled signal as advisory when the
   quota is a quarter or less of the limit, when it carries a `RetryAfter`,
   or when it carries a future `ResetAt` without quota counters. GitHub
   sends `X-RateLimit-Reset` on every response, so treating any future reset
   as advisory, as unstructured-runtime#53's governor does, would halve every
   GitHub key on every call.
3. **Only reducing signals restart the clean `RecoveryInterval`.** A healthy
   quota report changes nothing, so a key keeps recovering while GitHub's
   headers keep arriving.
4. **The ceiling is `MaxConcurrentReconciles` times the reconcilers sharing
   the governor.** Providers run several controllers, each with
   `--max-reconcile-rate` workers, against one endpoint. A ceiling of one
   controller's workers would cap a healthy endpoint below today's total.
   Each `reconciler.WithRateLimitGovernor` registers one reconciler.
5. **Decided (the user, 2026-11-05), then replaced by item 18: saturation bounds
   every gated call, keyer or not.** With saturation enabled, `Connect` and every
   `ExternalClient` call need a `local` slot even when the provider
   implements no `RateLimitKeyer`. The keyer is needed only for per-endpoint
   API limits. A local deferral's message names the cause and no key. The
   first implementation read §4 literally (no keyer, never gated), which left
   saturation protection to providers that had written a keyer for another
   reason.
6. **`X-RateLimit-Delay` on a served response becomes `RetryAfter`** when
   there is no `Retry-After`. The signal is then advisory. On a throttled
   response it is not used; only `Retry-After` and `X-RateLimit-Reset` name a
   deadline.
7. **The gate for `Create` is asked before the create-pending annotation is
   written.** A deferral after it would leave an unresolved create handshake
   for a create that was never attempted.
8. **The client is gated on its own key, or on the connecter's** when only
   the connecter implements `RateLimitKeyer`. `Connect` is gated on the
   connecter's key.
9. **The `ReconcileDeferred` event** is recorded when the object enters a
   deferral, or when a throttled deferral names a new deadline. It goes
   through the throttled recorder.
10. **Decided (the user, 2026-11-05): the thresholds.** §7 referred to "the
    CPU and memory pressure thresholds in this document" but gave none. The
    first implementation used 50% (`cpu.pressure` `some avg10`) and 10%
    (`memory.pressure` `full avg10`). CPU 50% stays, still provisional pending
    core-provider's validation, because the kind measurement found it removed
    the probe failures without idling. The memory pressure input is dropped,
    because the kind measurement read 0.00 up to every OOMKill.
11. **What the kind measurement (open question 5) found against this design,
    and what was decided.** It is recorded in
    [configuration](./configuration.md#the-pressure-thresholds-are-provisional).
    - **Decided (the user, 2026-11-05): `memory.pressure` is dropped as an
      input**, because the kind measurement read it at 0.00 up to every
      OOMKill. A Go controller's memory is anonymous, and with no swap there
      is nothing to reclaim and so nothing to stall on. The working-set
      ratios are the input that acts.
    - **Decided, then superseded by item 13: a 250ms sample period.** The
      first measurement OOMed in 3 of 5 runs at 1s, and a 1s sample can't stop
      calls admitted between two samples. `avg10` is a 10-second average, so
      the faster period could only help the working set.
    - **Decided (the user, 2026-11-05): working-set ratios of 0.80 (halve) and
      0.90 (straight to 1)**, because at about 5% of the limit per call 10% of
      headroom is less than two calls. 0.80 was clean in 2 runs then; the
      re-measurement did not repeat that. They stay as the experimental
      input's provisional values (item 13).
    - **Decided, then revised by item 13: slow start for `local`**, because
      the kind measurement showed a controller restarted after an OOMKill
      meeting its whole backlog at full concurrency and crash-looping. It first
      rose by 1 per clean sample, which the re-measurement showed was too fast.
12. **Decided (the user, 2026-11-05): a deferral that changes nothing writes
    nothing.** When the object would be written back exactly as it was read,
    the status update is skipped and the pass only requeues. At capacity the
    requeue is 1s to the `RecoveryInterval` and the message carries no time,
    so every pass would otherwise make the same write. A throttled deferral
    that names a new deadline still writes.
13. **Re-measured: the memory input did not prevent OOMKills. Decided (the
    user, 2026-11-05): this release ships rate-limit and CPU slow-down, and
    memory stays in the code, off by default and experimental.** The tables
    are in [configuration](./configuration.md#re-measurement-of-the-memory-input).
    On the synthetic workload (16 workers holding 16 MiB each, 100% of a
    256Mi limit), it was clean in 0 of 19 runs at 250ms, 0.80/0.90, with a slow
    start and +1 per clean sample. Alternating on one host, it restarted 3, 4,
    3 times and the code before it 5, 0, 5. Cold start still crash-looped (4
    restarts in 5 of 5 runs, against 5 with the governor off). From that:
    - **Decided: the memory input is off by default.** A provider that enables
      saturation gets CPU only. `ExperimentalMemoryWorkingSet` turns the input
      on, and 0.80/0.90 stay as its provisional values.
    - **Decided: the default sample period is back to 1s.** `avg10` gains
      nothing from a faster sample, and 250ms cost up to 1.8% of one core, 7%
      of a 250m limit. That was on a busy host; a later session on a quieter
      one measured 0.19%, so the cost varies about tenfold. A faster period
      only matters to the memory input.
    - **Decided: one recovery rule for `local`, whichever input cut.** After
      any cut, +1 per clean `RecoveryInterval` (30s), the same as API limits
      and what CPU 50 was first measured with. At +1 per clean sample `local`
      was back at the ceiling within 4 seconds, before the heap shrank; a
      scratch build with +1 per 30s after the first cut restarted 3, 1, 0
      times against 4, 4, 4, alternating on one host (three runs each, so a
      direction, not a proof).
    - **Decided: a startup ramp of +1 per 2s until the first cut**, a named
      default not tied to the sample period, so 16 workers take about 30s. +1
      per 250ms had reached 9 calls in 2 seconds, faster than the working set
      reacts.
    - **Decided: the one-call floor stays.** In 18 of 19 runs the last sample
      before the first death read `local=1`, at 96 to 100% of the limit: that
      call landed on Go's retained heap. Holding it back would trade the
      OOMKill for a stall, and CPU saturation needs the floor to keep finishing
      deletions.
    - **Decided: the CPU threshold stays 50**, still provisional until
      core-provider validates it. At the final code it ran 3 of 3 without a
      restart, at 40 to 47% pressure against 78% with the governor off, and at
      1.4 to 2.1 Observes/s against 2.6 (one control, on a busy host). A CPU
      limit below one core keeps `local` near 1 at this threshold, and
      configuration.md tells operators what to do about that.
    - **Decided: startup-ramp deferrals are quiet** (§7), because a restart
      otherwise turned every healthy object `Synced=False`: 200 objects, 200
      status updates, 200 at `ReconcileDeferred`, 0 provider calls, measured
      with a test in review. With the fix the same restart makes 0 status
      updates and 0 events, and each pass requeues with a nil error.

The rest of this section is the follow-up for the memory input. It is filed
here and isn't built. The memory input stays experimental until item 16's
gates are met.

14. **Follow-up: the runtime sets the Go memory limit.** When the memory input
    is on and `GOMEMLIMIT` isn't set, the runtime calls `debug.SetMemoryLimit`
    from the container limit, in the region of 0.8 of `memory.max` (about 200Mi
    for 256Mi). An explicit value always wins, whether from `GOMEMLIMIT` or an
    option. The reason: the working set sits on plateaus set by the GC's heap
    goal, so it doesn't track load, and with `GOMEMLIMIT=200MiB` throughput
    tripled and restarts fell to 2 or fewer (1 of 5 runs clean; no help with
    the governor off). This revisits §7, which left it to charts. It would need
    a measured answer to where it applies: only with the memory input on, as
    proposed here, because a library changing it otherwise is what §7 ruled out.
15. **Follow-up: GOMEMLIMIT-relative headroom as the input.** core-provider's
    suggestion: a working-set ratio is the wrong input when one unit of work is
    large relative to the limit. With a call at 6% of the limit, 90% is less
    than two calls from the end, and the working set says little about whether
    the next call fits. Test the headroom left under the Go memory limit
    instead of the working-set ratio. The runtime can read that without I/O
    (`runtime/metrics`), so it may also cost less to sample than `memory.stat`.
16. **Follow-up: re-test gates before the memory input can default on.** All of
    these, on the overcommitted workload and with one run at a time or
    alternating, on a host whose load is recorded:
    - clean in at least 18 of 20 runs, against 0 of 19 so far;
    - no crash loop on cold start, with the backlog present before the
      controller starts, in at least 9 of 10 runs;
    - a control of the code before the change alternating in the same block;
    - restarts split by cause, OOMKill or liveness kill;
    - throughput not below the old code's clean runs;
    - the sampler's cost reported at the chosen period, in a container;
    - a run on a real provider, not only the example.

    Decided by the user as the bar before the memory input can default on.
17. **One object, several scopes. Decided by the user on 2026-11-05, after
    git-provider's adoption found the gap.** A `Repo` has `fromRepo` and `toRepo`,
    each with its own URL and credential, and every external method touches
    both. The design assumed one host per object (§4), which holds for
    rest-dynamic-controller and not here. Decided: an optional
    `RateLimitKeysProvider`, additive, so `RateLimitKeyer` doesn't change.
    - **All or nothing, not blocking.** `AcquireAll` checks every key, then
      takes every slot, under one lock. A deferred call holds nothing, so calls
      that share keys can't wait on each other, and a full key costs its
      neighbour no capacity.
    - **The deferral names what stopped the call.** Among held keys, the latest
      deadline, because the call can't run sooner. A hold outranks a full key,
      as it does for one key. A full local capacity names no key when the call
      has several.
    - **One call takes one local slot**, however many keys it has: `local`
      bounds calls, not endpoints.
    - **The cost:** a call needs all its keys free at once, so under contention
      a multi-key object can be deferred more often than a single-key one. It
      requeues like any deferral. This has not been measured.
    - Not done: per-call keys, gating `Observe` on one endpoint and `Create` on
      another. The keyer is asked per object, and a `Repo` reaches both
      endpoints from every method.
    - unstructured-runtime doesn't need this unless one of its objects spans
      two scopes. `Signal`, `Classify` and the deferral wording are unchanged.
18. **Built: the local slot is taken before the pop. Decided by the user on
    2026-11-07,** after core-provider found the inversion in
    unstructured-runtime#72 and the same shape here by reading. The design,
    its options and the decisions are in
    [design-local-slot-before-pop](./design-local-slot-before-pop.md). It
    replaces items 5 and 13's local deferrals and the quiet startup ramp:
    `DeferSaturated`, `DeferRamping`, `Deferral.Local` and `Deferral.Cause` are
    gone, a keyless call isn't gated, and nothing is written on the object for
    the controller's own saturation. It changes user-visible behaviour of an
    off-by-default feature, so it is a minor release. Both measurement blocks of
    item 16 measured the old gate and are repeated on the combined code.
19. **Built: CPU pressure over a 10s window, one cut per window. Decided by the
    user on 2026-11-13.** Measured on GKE on 2026-11-08 (250m, 16 workers), the
    step of item 16 took `local` from 16 to 1 on one stall and settled at about
    70% of the CPU limit, with 31% less throughput. The cause: it halved on
    every 1s sample of `some avg10`, an exponential average with a 10s time
    constant, so it kept cutting on a stall its first cut had already answered,
    and then recovered at +1 per 30s.
    - **The signal is the change of `cpu.pressure`'s cumulative `some total`
      over the last 10s** (`CPUPressureWindow`), not `avg10`. A window that
      starts after a cut holds nothing from before it.
    - **A cut needs a whole window of evidence, and the next cut a whole window
      measured after the last one.** Pressure over the threshold that doesn't
      cut holds `local`.
    - **After a CPU cut, +1 per clean `StartupRampInterval` (2s).** The memory
      input keeps item 16's rules: the soft ratio halves once per sample, the
      hard ratio drops to 1 at once, and recovery is +1 per `RecoveryInterval`.
    - **The bars for the re-measurement** (the user, 2026-11-08): at least 90%
      of the CPU limit used, pressure about 50, throughput at least 90% of the
      unthrottled run, no restarts. **Met on 2026-11-13** on GKE at 250m: 93.4%
      of the limit, pressure 41.7, 91.0% of the throughput, no restarts, where
      the step before it read 72.2%, 17.0 and 69.5%
      ([configuration](./configuration.md#cpu-step-re-measured-2026-11-13)).
    - **This replaces the cost model**
      ([design-saturation-cost-model](./design-saturation-cost-model.md)),
      which was stopped after its review: at CPU limits of 100m to 125m its own
      calibration lowered the target to about 78% of the limit, and the design
      had grown far past this step
      ([the review](https://github.com/krateo-platformops/provider-runtime/pull/57#issuecomment-6856212229)).
    - `CPUPressureThreshold` keeps its name and its default, 50, still
      provisional. Its meaning moves from `avg10` to the window's share, and
      the `saturation.input` metric reports the same value. It changes
      user-visible behaviour of an off-by-default feature, so it is a minor
      release.
