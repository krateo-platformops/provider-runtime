---
type: Architecture
title: provider-runtime — take the local slot before the pop
description: Design, implemented, for taking the saturation gate's local slot before the workqueue pop, so low local capacity behaves like fewer workers, without letting an idle worker hold a slot when several controllers share one governor.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, workqueue, reconciler]
timestamp: 2026-11-07T00:00:00Z
---

# Take the local slot before the pop

**Status:** built, as design A (§4), with the decisions in §9 and what the build found in §10. Not yet
measured on a cluster. It changes §7 of
[design-rate-limit-governor](./design-rate-limit-governor.md) (§11 item 18
there) and follows core-provider's design on
[unstructured-runtime#72](https://github.com/krateo-platformops/unstructured-runtime/issues/72).
Code references in §1 to §3 are to provider-runtime at `d0632f1`, before the
change, and controller-runtime v0.23.3 (`CR/` is
`sigs.k8s.io/controller-runtime@v0.23.3`).

## 1. The problem

The local gate runs after the pop. A worker pops an item
(`CR/pkg/internal/controller/controller.go:421`), the reconciler asks the governor
for a local slot inside the reconcile (`pkg/reconciler/ratelimit.go:138-160`,
`pkg/ratelimit/governor.go:486-503`), and when local capacity is full it returns
`RequeueAfter` of a random 1s to 30s (`governor.go:492`). The popped item sleeps
through any slot that frees meanwhile, and whatever is next in the queue takes
it, whatever its priority: the inversion core-provider found in #72. Local
capacity also bounds only the gated calls, not the reconcile, which is why §7 has
to require providers to do expensive work inside those calls.

**Goal.** `local` behaves like fewer workers. Queue order is the order work runs
in. A popped item never sleeps through a freed slot. The slot covers a whole
reconcile. Per-key gating stays after the pop, because a key needs the object.

## 2. What we have to work with

- **controller-runtime owns the worker loop.** Each worker runs
  `for c.processNextWorkItem(ctx) {}` (`CR/pkg/internal/controller/controller.go:308-315`).
  `processNextWorkItem` calls `c.Queue.GetWithPriority()` (`:421`), defers
  `c.Queue.Done(obj)` (`:433`), then runs `reconcileHandler` (`:444`). Nothing can
  go before the pop or around the reconcile except through the queue.
- **We own only the queue constructor.** `controller.Options.NewQueue` (default at
  `CR/pkg/controller/controller.go:260-272`) is called once at start
  (`controller.go:338`). A result implementing `priorityqueue.PriorityQueue` is
  used as is; anything else is wrapped and loses priority (`:339-343`, wrapper
  `:551-566`). Ours is built by `ForControllerRuntime` (`pkg/controller/options.go:132-150`),
  which sets `NewQueue` only when a `QueueWaitRecorder` is given, via
  `newControllerQueue` (`pkg/controller/queue_wait.go:35-46`).
- **Shutdown goes through the queue.** On `ctx.Done()` the controller calls
  `c.Queue.ShutDown()` (`controller.go:344-347`) and waits for its workers
  (`:325-327`).
- **`Done` runs on every exit path.** It is deferred in `processNextWorkItem`: on
  return, on error, and when a panic unwinds. With `RecoverPanic` on (default) the
  panic becomes an error (`controller.go:195-212`); with it off it is re-raised
  after the deferred calls.
- **The priority queue exposes no peek and no readiness signal.** Its API is
  `TypedRateLimitingInterface` plus `AddWithOpts` and `GetWithPriority`
  (`CR/pkg/controller/priorityqueue/priorityqueue.go:39-43`). Delayed and
  rate-limited items turn ready in its own goroutine (`handleWaitingItems`, `:309`).
  `Len` counts ready items including those it can't hand out because their key is
  processing (`:486-497`; `handleReadyItems` skips them, `:390-393`). Its metrics
  go through an internal package (`priorityqueue.go:15`, `CR/pkg/internal/metrics`),
  so a queue of our own can't emit the same `workqueue_*` series.
- **Several controllers share one governor.** `SetupRateLimitGovernor` builds one
  per `Options` (`options.go:103-129`), and every controller set up from that
  `Options` uses it. git-provider does this: `repo` and `localresource` both use
  `o.Controller.ForControllerRuntime()` and the same governor. A process can also
  hold several governors: `ratelimit.New` is public, and each `Options` gets one.

## 3. Finding: no design meets the goal on the queues we have

To take a slot before the pop without holding it idle, a worker must know an item
is available before popping. The queues don't say so, and polling `Len` is ruled
out (and wrong for the priority queue). So:

**No design that wraps controller-runtime's queues as they are meets all of the
goal.** Either the slot is taken before the pop and held while the worker waits
for an item (core-provider's choice, which starves other controllers here), or
the item is popped first and held while it waits for a slot. Meeting the goal
exactly needs the queue to know its own readiness, which means owning more of it.

| | Design | Meets the goal | Several controllers | Cost |
|---|---|---|---|---|
| A | **Gated queue over the priority queue** (recommended, §4): ours owns delays and rate limiting and keeps an exact count of items available to hand out; the priority queue keeps ordering and dedupe | yes, within a controller | no idle hold; bounded across controllers, §5 | ~300 lines plus a model test. The count must be exact. `workqueue_retries_total` stays 0 for gated controllers, since delays no longer reach the inner queue. |
| B | **Our own queue type** with readiness built in | yes, and across controllers by priority | no idle hold | ~700 lines against upstream's 741. Loses every `workqueue_*` metric for gated controllers (internal provider). Drifts from upstream fixes. |
| C | **Pop, then wait in the queue for a slot**, woken by release, highest-priority popped item first | no: an item is popped that can't run, and a higher-priority arrival can run after one already-popped item of its controller | no idle hold | ~100 lines on the existing queue; simplest. Bound: one reconcile per controller per higher-priority arrival. |
| D | **Per-controller capacity shares** with core-provider's slot-before-pop | yes, within a controller | idle workers hold only their own share | A busy controller can't use an idle one's share, so capacity is wasted exactly when one has the backlog. Shares can't follow demand without polling. |
| E | **Upstream hook**: a readiness callback on the priority queue, or a hook around `processNextWorkItem` | yes | depends on the hook | Months of lead time, ours to carry meanwhile. Worth proposing anyway. |

**Recommendation: A.** It meets the goal within each controller, holds no slot
idle, and keeps upstream's ordering, dedupe and most workqueue metrics. C is the
fallback if A's exact count is judged too fragile. B only if cross-controller
priority order matters more than losing the workqueue metrics.

## 4. Design A: the gated queue

Used only when the governor has saturation on. Otherwise `ForControllerRuntime`
builds the same queue as today, so the default path doesn't change. With
saturation on, `ForControllerRuntime` sets `NewQueue` whether or not a
`QueueWaitRecorder` is set.

**State**, per queue, under its lock:

- `inner`, a controller-runtime priority queue that only receives immediate adds,
  so every change to its contents is caused by a call of ours. **Every call into
  `inner` is made holding the queue's lock, in the same critical section that
  updates the record below.** The first version didn't, and had two races (§10);
- `ready` (keys ready in `inner`) and `processing` (keys a worker holds).
  Available = `ready` minus `processing`, which is exactly what `inner` can hand
  out (`priorityqueue.go:390-393`);
- `waiting`, delayed keys with ready time and priority, each with a timer; a
  timer that fires makes every due key ready at once (§10). Upstream's rules apply: the earlier time and higher priority win
  (`:228-246`), and an immediate add cancels the wait (`:254-257`);
- `idle`, workers blocked in `GetWithPriority`, and `granted`, slots handed to the
  queue whose worker hasn't popped yet.

Per governor, under its lock: capacity, slots in use, and the queues with an idle
worker and an available item.

**Adds.** An immediate add goes to `inner` and `ready`. If the key wasn't
available before, the count rises and the governor is told. A delayed or
rate-limited add computes its time (`rateLimiter.When`, as upstream at `:205-210`)
and goes to `waiting`. When its timer fires it becomes an immediate add with its
stored priority. `Forget` and `NumRequeues` go to the rate limiter.

**Get.** A worker counts itself idle and waits on the queue's condition. It never
asks for a slot while nothing is available, so an idle worker holds nothing. The
governor grants a slot only to a queue with an idle worker and an available item.
Under its lock and then the queue's, it takes a slot, counts one more grant
against the available items, moves one worker from idle to granted, and signals.
The woken worker, still holding the queue's lock, calls `inner.GetWithPriority()`
and records the key it gets as `processing` in the same critical section. No add,
pop or `Done` can change `inner` in between.

**Done.** `Done(key)` takes the queue's lock, then calls `inner.Done(key)`,
removes the key from `processing` and, if the key was re-added meanwhile, counts
it available again, all in one critical section. It then gives the slot back to
the governor, which grants it on. controller-runtime defers `Done`, so the slot is
released on return, error and panic, and covers the whole reconcile. A `Done` for
a key no worker holds does nothing.

**Capacity changes.** A rise from the sampler grants new slots the same way. A
cut lets running reconciles keep their slots and grants none until in-use falls
below the new capacity.

**Shutdown.** `ShutDown` marks the queue shut, removes it from the governor's
list, wakes every waiting worker (idle or granted) and shuts `inner` down. A
waiting worker returns `shutdown=true`, so `processNextWorkItem` returns false and
it exits (`controller.go:422-425`). A slot granted to a worker that finds the
queue shut is released before returning. Running reconciles finish; their `Done`
releases their slots.

**Never lost, never held.** An item is popped only after its slot is taken, in the
same critical section that counted it, so nothing popped waits for a slot. A ready,
unprocessed item stays in `inner` until popped, so nothing is lost while waiting.
A slot is taken only with an available item and a worker, so none is held idle.

**Lock order:** governor, then queue, then `inner`'s own locks and the clock's. A
queue never takes the governor's lock while holding its own: an add or `Done`
updates the queue, unlocks, then tells the governor. `inner` never calls back into
the queue. A timer callback only starts a goroutine, which then takes the queue's
lock, so the queue's lock is never awaited under the clock's.

**Why the record stays exact.** `inner` changes only through calls the queue makes
under its lock, and each one updates the record in the same critical section. So
the record can't drift by interleaving: an add, a pop and a `Done` are each one
step. It could only be wrong through a mistake in how it models `inner`, which the
model test (test 9) and the concurrent test exist to rule out. A pop that finds
the record disagreeing with what `inner` handed out, or a slot released with none
in flight, is counted and logged as an error, and the package's tests fail on it.

**Why the pop can't block forever while holding the queue's lock.** The reviewer of
the first version warned against holding it across `inner.GetWithPriority()`,
because a pop with nothing to hand out would wait inside `inner` and keep every
other caller out. A pop is made only after a grant, and a grant only when the
record shows an item that is ready and not processing. With every change to
`inner` made under the same lock, nothing changes between the grant's count and
the pop. What remains is whether `inner` hands that item out without needing
anything the pop is holding. It does (references to `CR/pkg/controller/priorityqueue/priorityqueue.go`):

- **Buffered adds.** `AddWithOpts` only appends to `addBuffer` and signals
  (`:153-170`). The buffer is flushed by `inner`'s own goroutines: `handleAddBuffer`
  (`:172-184`), and `handleReadyItems`, which flushes before it hands anything out
  (`:376`). `GetWithPriority` counts itself a waiter and signals
  `handleReadyItems` (`:424-436`), which takes only `inner`'s locks, never ours.
  So an item added before the pop is in the ready tree by the time it is handed
  out.
- **A key re-added while it is processing.** `inner` keeps it in the ready tree
  but skips it while the key is locked (`:390-393`), until `Done` unlocks it and
  signals (`:467-473`). The record counts such a key as not free until the queue's
  own `Done`, which calls `inner.Done` in the same critical section first. So the
  record never counts a locked key as available.
- **Shutdown while a pop waits inside `inner`.** The queue's `ShutDown` needs the
  lock, so it waits for the pop. Nothing it does is needed to wake the pop:
  `handleReadyItems` stops only when `inner`'s `done` channel closes (`:362-364`),
  and only `inner.ShutDown` (`:475-479`) closes it, which the queue calls under
  the same lock. So `inner`'s dispatcher is running for as long as the pop waits,
  hands the counted item over the `get` channel (`:400`), and the pop returns. Then
  `ShutDown` runs. A test pins this: `TestShutDownDuringAPopInsideInnerReturns`.

The one way it could block is a record that counts an item `inner` can't hand
out. That is a defect, not a state the queue reaches by timing, and it has no
timeout: the tests above are what rule it out.

**The `QueueWaitRecorder` wrapper.** At `d0632f1` it lacked `AddWithOpts` and
`GetWithPriority`, so controller-runtime wrapped it and every item ran at
priority 0 (`controller.go:339-343`). That was fixed separately in v1.10.1
(`queueWaitInstrumentedPriority`). With a recorder, `ForControllerRuntime` wraps
the gated queue in that variant, so priorities hold under it.

## 5. Several controllers, several governors

- **No starvation across controllers.** Idle workers hold nothing, and a slot goes
  only to a queue with work.
- **Order left across controllers.** When several queues have work, the governor
  picks round-robin. Within a queue the order is the queue's. Across queues a
  high-priority item can wait for the other queues' turns: at most one grant per
  other queue with work, so at most Q-1 reconciles for Q controllers on the
  governor (one for git-provider's two). Picking by priority across queues would
  remove that, at the cost of mirroring the priority queue's priority rules too,
  which is most of B. Not done here.
- **Several governors.** Each governor's slots are its own, so two with saturation
  in one process would each allow full capacity on one cgroup. That is true today
  too. `ratelimit.New` will log a warning when a second saturation governor starts
  in the process. An error would break every test that builds several governors.
- **A controller built without `ForControllerRuntime`** gets no local bound, where
  today the reconciler's gate still bounded it. Every consumer checked
  (git-provider, patch-provider, azuredevops-provider, the example) uses
  `ForControllerRuntime`. The governor counts reconcilers (`RegisterReconciler`)
  and gated controllers (`RegisterGatedController`, at setup, §10). When the
  sampler starts, if more reconcilers registered than controllers were gated, it
  logs an error once with the counts.

## 6. What goes away, and what shows instead

Removed:

- the reconciler's local slot: `gate` asks for keys only, so a provider without a
  keyer isn't gated in the reconciler at all;
- `Deferral.Local`, and the local use of `Deferral.Capacity` and `Deferral.Cause`;
- `DeferSaturated`, `DeferRamping`, and the local use of `DeferAtCapacity`;
- the quiet-ramp branch (`pkg/reconciler/ratelimit.go:187-196`), `rampLeftLocked`
  (`governor.go:447-453`), and the written `causeSamplerNotStarted`
  (`governor.go:455-457`, `:499-501`);
- §7's requirement on providers to do expensive work inside the gated calls, since
  the slot now covers the whole reconcile.

The controller's own saturation isn't a state of the object, so it stops being
written on it: no `Synced=False/ReconcileDeferred` for local capacity, no event,
no status write. Key deferrals, throttled and at capacity, don't change. What
shows instead:

- `provider_runtime.ratelimit.local.capacity` and the saturation input gauges,
  names unchanged;
- the `deferrals` counter keeps its name and its `throttled` and `capacity` causes
  for keys; `saturated` and `ramping` stop appearing;
- the existing log line when capacity is cut.

No new metric is needed. A gauge of workers waiting for a local slot would be the
direct signal; this design proposes it only if unstructured-runtime adds the same
name.

**The trap that stays.** A governor whose sampler never started keeps capacity at
1, and that now throttles whole reconciles silently, where today it showed as a
deferral cause on the object. Until the first sample every input reports disabled
(`saturation.go:308-324`), so `saturation.input.enabled` is 0;
`SetupRateLimitGovernor` adds the governor to the manager. Test 8 pins it.

**Release.** This removes user-visible behaviour from a feature that's off by
default, which fits a minor release. The release notes say so.

## 7. Tests

Where possible each runs a real controller-runtime controller
(`controller.NewTypedUnmanaged` with a channel source) over the queue
`ForControllerRuntime` builds, so the real worker loop is exercised.

| # | Test | On today's code |
|---|---|---|
| 1 | Capacity 1, slot busy, low-priority items queued, then a high-priority item: when the slot frees, the high-priority item runs next. | **fails**: a popped high-priority item is deferred 1s to 30s and a low one takes the slot |
| 2 | When a slot frees, the next reconcile starts within 100ms; a capacity rise starts a waiting one within 100ms. | **fails**: the deferred item sleeps 1s or more |
| 3 | The slot covers the whole reconcile: capacity 2, a reconciler that burns time before `Connect`, never more than 2 in that section. | **fails**: work outside the gated calls isn't bounded |
| 4 | No reconcile returns `RequeueAfter` for local capacity and nothing writes `ReconcileDeferred` for it. | **fails** |
| 5 | Two controllers, one governor, capacity 1: A has 4 idle workers and an empty queue; B's item runs at once. | passes today; **fails on core-provider's slot-before-pop**, kept as a guard and checked by mutant |
| 6 | Release on every exit path: return, error, requeue, panic with `RecoverPanic` on and off; slots in use return to 0. | passes today vacuously; guards the new code |
| 7 | Shutdown with workers waiting, idle and granted: `Start` returns and every worker exits. | passes today (no waiters); guards the new code |
| 8 | Sampler never started: inputs report disabled and local capacity reads 1, mirroring core-provider's `TestUnstartedSamplerShowsAsADisabledInput`. | passes today; pins the trap |
| 9 | Model test: random immediate, delayed, rate-limited and prioritised adds, pops, `Done`s and a shutdown, against a reference model of the priority queue's rules; the available count never differs from what `inner` can hand out. | new code |
| 10 | With a `QueueWaitRecorder`, a high-priority item still runs ahead of low ones. | **fails**: the wrapper drops priority |

Each new behaviour gets a revert check, counted as before.

## 8. Re-measurement

The memory branch then rebases onto this and is re-measured on the combined code.
Both blocks measured the old gate, so both repeat: memory (throughput, and
possibly clean counts) and the CPU threshold.

## 9. Open points, decided by the user on 2026-11-07

1. **Design A,** the gated queue.
2. **Round-robin across controllers,** with the Q-1 bound of §5.
3. **Saturation with `UsePriorityQueue=false` is an error** from
   `SetupRateLimitGovernor`, naming the conflict. No silent fallback.
4. **No waiting-workers gauge** for now.
5. **No upstream proposal** for now.
6. **Several saturation Governors in one process: a warning,** and **a
   reconciler with no gated queue: an error, logged once,** as in §5.

## 10. What the build found

- **Delayed items that come due together are handed out together.** A timer
  makes every key that is due ready at once, as the priority queue's
  `handleWaitingItems` does, so items due at the same instant are handed out by
  priority, not in the order their timers ran. It uses the fired entry's time as
  now, because a test clock runs timer callbacks under its own lock. This is for
  parity with upstream. It matters only in the instant between two timers due
  together, which no test can hit from outside, so a white-box test calls the
  timer's callback directly. An earlier model-test failure that looked like this
  bug was the test's own error: it treated keys due at different times within
  one clock step as due together, when handing out the earlier one first is
  right. The model now steps the clock to one deadline at a time.
- **The model test had a deadlock of its own.** It read the governor's count
  while holding the queue's lock, the reverse of the code's lock order
  (governor, then queue), and hung about once in a few hundred runs. The code's
  lock order was right.
- **One difference from the priority queue stays:** among items of one priority,
  a delayed item is ordered by when it became ready, not by when it was added.
- **The registration count is taken at setup, not when the queue is built.**
  `ForControllerRuntime` registers the controller with the Governor when it runs,
  at setup. Counting queues as controller-runtime builds them would be wrong on
  a standby replica, where controllers wait for leadership and never build their
  queues while the sampler runs.
- **Tests, against the code before the change:** of §7's end-to-end tests, 1 to
  4 fail on `d0632f1` (no reconcile within 1s after the slot freed, twice; four
  reconciles in flight with capacity 1; three popped items requeued for local
  capacity) and 5 to 8 pass there. Test 10 belongs to the v1.10.1 fix. The model
  test (9) and the queue's own tests are new code.
- **The model test,** 120 steps a run: 2,000 runs without the race detector and
  5,000 under `-race` (65s), with no failure and no data race. The 5,000 runs
  made 154,516 pops, 73,636 of them with a lower-priority key also available,
  49,006 delayed items coming due, 35,060 re-adds while processing and 71,789
  capacity changes.
- **Revert check** (`hack/revert-check-local-slot.py`): 30 of 32 mutants killed
  by assertion, none by a compile failure, panic or timeout. The two survivors
  are equivalent: a keyless call reaching `AcquireAll`, and `AcquireAll` without
  keys reaching `acquire`, both take nothing and return a no-op release.
- **Local smoke** (`hack/smoke-local-slot.sh`): the example controller with
  `--saturation-governor` against envtest, no cluster: 12 of 12 Greetings Ready,
  none written `ReconcileDeferred`, no error or warning. macOS has no cgroup v2,
  so this checks the wiring, not saturation.
