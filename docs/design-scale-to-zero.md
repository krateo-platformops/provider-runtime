---
type: Architecture
title: provider-runtime — scale idle controllers to zero
description: Design, implemented in pkg/scaletozero, for a helper that owners call to run a per-Kind controller Deployment at 0 replicas while its Kind has no instances and wake it when one appears.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, scale-to-zero, deployment, controller]
timestamp: 2026-11-07T00:00:00Z
---

# Scale idle controllers to zero

**Status:** implemented in `pkg/scaletozero`, unreleased. The API was settled
with both owners, core-provider and oasgen-provider, in
[#39](https://github.com/krateo-platformops/provider-runtime/issues/39).
Three later decisions changed it and are recorded below: `Exists` instead of
`Count` (§4.1), what an error reports (§4.3), and the `active` count (§4.4).

## 1. The problem

core-provider runs one controller Deployment per composition version, and
oasgen-provider one per generated REST Kind. Each runs whether or not its
Kind has any instances. On krateo-057, 32 controllers with no instances used
236 MiB between them, for nothing.

While a Kind has no instances, its controller should run 0 replicas, and go
back up when an instance appears. The controller can't do this itself: at 0
replicas it isn't running to notice the first instance. The owner that
manages the Deployment has to decide.

## 2. The helper is the mechanism, not the trigger

Nothing inside the helper watches anything. The owner calls
`scaletozero.Reconcile` from its own reconcile of the object that owns the
controller (a CompositionDefinition, a RestDefinition). A controller at 0
with no instances gets no reconcile, and neither does its owner, unless the
owner arranges one. **The owner must reconcile its owning object when the
first instance of the Kind appears**, for example with a per-Kind watch, and
when the last one goes, or within its resync.

## 3. The API

```go
package scaletozero

func Exists(ctx context.Context, r client.Reader, gvk schema.GroupVersionKind, sel labels.Selector) (bool, error)
func Decide(exists bool, lastSeen, now time.Time, holdDown time.Duration) (replicas int32, requeueAfter time.Duration)
func Reconcile(ctx context.Context, c client.Client, deploy types.NamespacedName, gvk schema.GroupVersionKind,
	sel labels.Selector, lastSeen time.Time, holdDown time.Duration, active int32) (Result, error)
```

- `Exists` reports whether at least one instance matching `sel` exists, in
  any namespace, terminating ones included. It stops at the first match.
- `Decide` is pure. It returns 1 (running) or 0 (idle), and when to look
  again.
- `Reconcile` reads the scale, lists, decides, and patches the Deployment's
  scale subresource only on change. `Result` carries `Replicas`, `Reason`
  (`Active`, `IdleHoldDown`, `Idle`, or `Unknown` on an error), `LastSeen`,
  `RequeueAfter`, `Exists` and `Patched`. The four reasons are exported
  constants, and a test pins their literals.

The helper keeps no state. `lastSeen` comes in and the new value comes back
in `Result.LastSeen`. The owner persists it where nothing else overwrites it,
such as its own status: core-provider keeps it in the CompositionDefinition's
status. An annotation on the Deployment was rejected because an owner's
full-object apply can erase it.

**Per-call cost.** `Reconcile` is safe to call on every poll. Each call makes
two reads, a GET of the scale subresource and a metadata-only LIST that
stops at the first instance, and a PATCH only when the replicas change.
During the hold-down, `RequeueAfter` is the time left, so the scale-down
lands on time even when the owner's polls are further apart.

## 4. The rules

### 4.1 Listing

*Decision, 2026-11-07, from oasgen-provider:* the decision is binary, so the
helper asks whether an instance exists instead of counting them. Walking
every instance of a Kind was wasted reads, and each extra page was another
chance of a mid-walk error.

- **The selector is required.** There is no default. It must be exactly the
  one the controller uses to choose what it reconciles: core-provider passes
  its `krateo.io/composition-version` label, oasgen-provider passes
  `labels.Everything()`. A narrower selector misses instances, and a miss
  scales a controller to zero while instances it serves still exist. A nil
  selector is an error. controller-runtime would not reject it: it turns a
  nil selector into `labels.Nothing()`.
- **An error is never false.** A failed LIST on any page, a missing CRD or an
  unserved version (the RESTMapper's no-match error), or a CRD deleted after
  it was mapped (a 404) is returned as an error.
- **Terminating instances count.** Their finalizers still need the
  controller.
- **Metadata-only, one item per page.** `metav1.PartialObjectMetadataList`
  with `Limit: 1`.
- **An empty page with a continue token is not the end.** With a label
  selector the API server may return fewer items than the limit, even none,
  and still have more. `Exists` follows `Continue` until it sees an item or
  the token runs out. A server that hands back the token it was sent is an
  error, not a loop. envtest's API server filled the page in the test that
  looked for this, so the empty page is produced with an interceptor.
- **A live reader is required.** Pass `mgr.GetAPIReader()` to `Exists`, and
  to `Reconcile` a client built with `client.New`, never `mgr.GetClient()`.
  A cache-backed reader truncates a page at `Limit`, sets the continue token
  `continue-not-supported`, and refuses the next page. So `Exists` fails on
  it whenever the Kind is empty, rather than say false. It would also start
  a metadata informer per Kind, which is the memory this helper exists to
  save.

### 4.2 Deciding

- An instance means running, and `lastSeen` becomes now.
- An empty Kind with a zero `lastSeen` starts the hold-down now. A Kind seen
  empty for the first time is never scaled down at once.
- An empty Kind within the hold-down stays running, and `RequeueAfter` is the
  time left.
- An empty Kind once `holdDown` has passed since `lastSeen` (inclusive) goes
  idle.
- A `lastSeen` in the future (a clock gone backwards, or one written by a
  clock ahead of this one) stays running and looks again within one
  `holdDown`. It is kept, not reset to now: resetting it would restart the
  hold-down on a clock that is behind, and could scale down sooner than
  `holdDown` after the last real sighting. The cost is that a `lastSeen` far
  in the future keeps the controller up until the clock passes it.
- `RequeueAfter` is set only during the hold-down, when the decision changes
  with time alone. When the Kind is active or idle it is 0, and the owner's
  own trigger (§2) brings the next call.
- `holdDown` must be greater than 0. `Decide` has no error return, so a
  `holdDown <= 0` returns running; `Reconcile` returns it as an error.

### 4.3 An error changes nothing, and never reads as 0

On any error `Reconcile` patches nothing and passes `lastSeen` back
unchanged. A failed LIST never scales a controller up from 0, nor down.

*Decision, 2026-11-07:* no result of an error may read as 0, because an owner
that renders `Result.Replicas` without checking the error would scale its
controller down. On an error `Reason` is `Unknown` and `Replicas` is:

- the Deployment's current replicas, when they were read and are non-zero;
- otherwise `active` (or 1, when `active` itself is the invalid argument).

`Reconcile` reads the scale before it lists, so a failed LIST reports the
current replicas. Rendering `active` after an error wakes a controller that
is at 0, so owners should still check the error.

### 4.4 Only between idle and running: the `active` count

*Decision, 2026-11-07:* the helper never patches a running controller down
to 1. `Reconcile` takes `active` (>= 1, an error otherwise), the replicas to
run while instances exist; core-provider passes `cdc.replicaCount`.

| Decided | Deployment now | Patched to |
|---|---|---|
| running | 0 | `active` |
| running | any non-zero count, `active` or not | left alone |
| idle | any non-zero count | 0 |
| idle | 0 | left alone |

Once a controller is up, the owner or an HPA owns its count. `Decide` still
returns only 1 or 0; `Reconcile` maps 1 to `active` when it wakes a
controller.

### 4.5 The scale subresource only

`Reconcile` reads the current replicas from `deployments/scale`
(`autoscaling/v1` Scale), not from the Deployment's spec, and patches only
`spec.replicas` there, with a raw merge patch that says the number. A patch
computed from the Scale would send `"replicas": null` for 0, because
`ScaleSpec.Replicas` is `omitempty`. The API server reads that null as 0:
the revert check's mutant for it survived. So the raw patch is clearer, not a
fix. The owner needs list on the watched resource across all
namespaces, and get and patch on `deployments/scale`; no access to the
Deployment object itself.

### 4.6 The caller's precondition

An owner that calls `Reconcile` must not write the controller Deployment's
`spec.replicas` anywhere else, including by re-applying it from a desired
spec. A full-object `Update` that omits `replicas` doesn't satisfy this
either: the API server defaults a nil to 1, so the owner would scale a
zeroed controller back up and fight the helper. core-provider's `Deploy`
does a full-object Update, so it renders `replicas` from the last
`Result.Replicas`.

## 5. What stays with the owners

- **Ready at 0 replicas.** Both owners' readiness reads
  `spec.replicas == status.readyReplicas`, so an idle controller reads as
  ready (0 == 0). Nothing tells "idle because the Kind is empty" from "at 0
  for another reason" except the `Idle` reason the owner can surface from
  `Result`.
- **Per-Kind watch growth.** The trigger in §2 is a watch per Kind in the
  owner. That costs memory in the owner as the number of Kinds grows.
- **Wake-up latency.** The first instance of an idle Kind waits for the
  owner's reconcile, the patch, a pod to schedule and start, and the
  controller's cache to sync before it is reconciled.

## 6. Tests

`Decide` has table tests. `Exists` and `Reconcile` run against envtest's
kube-apiserver and etcd, because controller-runtime v0.23.3's fake client
can't stand in: its List ignores `Limit` and `Continue`, it lists a missing
CRD as empty instead of failing, and its scale Patch patches the Scale body
as an object of its own. In CI the envtest tests fail, rather than skip, when
`KUBEBUILDER_ASSETS` is unset. Each rule was broken in turn to check that a
test notices: [`hack/revert-check-scale-to-zero.txt`](../hack/revert-check-scale-to-zero.txt).
