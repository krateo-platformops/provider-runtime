---
type: Architecture
title: provider-runtime — credential-use model, a sound state-space reduction
description: Spec, under soundness review and not implemented, for a partial-order reduction in pkg/credentialuse/internal/apimodel's scheduler, so that the two-process scenario of the credential-use model can be searched exhaustively in CI.
resource: github.com/krateo-platformops/provider-runtime
tags: [spec, model-checking, credential-use, tests]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use model: a sound state-space reduction

**Status: spec, under soundness review, not implemented.** It is item 2 of
the user's decision on #59: make TwoProcessesAcquireOneHolder exhaustive in
CI with no cut in scope. The implementation waits for an independent
soundness review, and for the user's decision on the base-semantics changes
in §4: C-clock, C-crash and C-clock+. A throwaway measurement prototype ran
in a scratch copy, outside the repository (§8). It is how two of the
obligations below were found. It sits under PR 2 of
[the implementation plan](./plan-credential-use-implementation.md), and the
scheduler it changes is `pkg/credentialuse/internal/apimodel/sched.go`.

## 1. The scheduler today

- A schedule is a sequence of **steps**. A step picks one enabled actor and
  resumes it. If the actor was parked at a `Call`, the step first runs the
  call's op atomically, or injects a fault (error, lost response, crash).
  Then the actor runs its own Go code until its next park.
- An actor parks in one of three places:
  - at a `Call`, which is always enabled, and faultable if the actor is
    `Faulty`;
  - at a `Wait(cond, notBefore)`, which is enabled when cond holds and the
    clock has reached notBefore, and is never faultable;
  - at "start", before its first line runs, which is always enabled.
- The logical clock ticks 1ms on every step, and jumps to the earliest
  notBefore when nothing is enabled.
- A **preemption** is a step that picks an actor other than the last one
  while the last one is still enabled. A schedule has at most P preemptions
  and F faults. `Explore` is a DFS over every choice within (P, F).
- **A crash** is FaultCrash on a call of an actor in process Π. It marks
  every actor of Π crashed, and replaces Π's memory (`*process`) with a fresh
  one. A crashed actor's calls return ErrCrashed until it calls Restart (each
  holder pass and each batch starts with one) or Wait (which clears the mark
  before and after parking).

A step that resumes an actor from a Wait or from start is a **resume step**.
The code it runs, up to the next park, is its **segment**.

## 2. Where the schedules go

In TwoProcessesAcquireOneHolder (2,625,480 schedules with one fault), many
choice points are resume steps whose segment does nothing visible:
- an actor starting, then parking at its first Wait or call;
- a holder's or releaser's Wait being satisfied, then the actor building its
  protocol and parking at its first call.

Each is its own step, so the DFS orders it against every other step. Those
orders differ in nothing observable.

## 3. The reduction

**Quiet segments.** A segment is **quiet** if it:
- (a) reads only actor-local variables, immutable values, and the process
  pointer (§4, C-crash);
- (b) writes only actor-local variables: its own crashed flag via Restart,
  and the protocol value it builds;
- (c) ends at a park (a Call, a Wait, or the actor's end) without making a
  call.

The harness **declares** quietness at the park that precedes the segment:
`WaitQuiet(...)` instead of `Wait(...)`, and a `QuietStart` flag on `Go`. It
declares it only where (a) to (c) hold. §6's guard checks (b) at run time,
and §6's differential test checks the whole claim.

**Rule R.** Before computing each step's options, the scheduler resumes, at
once, every actor parked at a quiet park whose Wait is ready, in the order the
actors were added, repeating until none is left. This is a **silent
transition**: no choice point, no clock tick, no preemption, no change to
`last`, no trace entry. The segment runs to the actor's next park. If that
park is a Call, the Call is an ordinary step later, with all its fault
options.

**Unchanged:** every Call stays a step, with its faults, and so does every
resume from a park that isn't quiet. That includes the informer's watch and
the relister's relist, whose segments deliver events and relist, which are
visible.

## 4. Why it is sound

**Claim.** For every schedule σ of the unreduced search within (P, F), there
is a schedule σ' of the reduced search within (P, F) that has the same calls
in the same order, with the same faults and the same results. It has the
same informer deliveries at the same points between calls, the same
terminal state, and the same checked observations, so the same violations.
Conversely, a reduced schedule is an unreduced schedule in which every quiet
resume happens as early as possible. So the reduced search adds no behaviour
the unreduced one lacks, at bounds no larger.

**The mapping.** σ' is σ with each quiet resume step removed, and its segment
run silently at the earliest point R runs it: right after the step that made
its Wait ready, or at the start.

**The conditions** (partial-order reduction terms: a singleton ample set made
of an invisible transition independent of all others, with no cycle to
ignore):

- **C-invisible.** By (b), a quiet segment changes nothing that any check
  (the safety invariant, checkHeld, the grace check, the deletion-phase
  check, convergence, the vacuity counters), any other actor's step, or any
  Wait condition reads. *Harness obligations that follow:*
  - The counters scenario Waits read (`started`, `batchesStarted`) move out
    of segments, into the op of the unit's first call: a "began" hook on
    modelServer, so that "a pass has begun" means "its first call has run".
    *The prototype broke exactly this.* A quiet segment that incremented
    `batchesStarted` enabled B's wait early. B then sat enabled at its call
    as the last actor, so a switch away from it cost a preemption, and the
    one-fault search lost every schedule in H1's state (0 against 41,328).
  - A releaser with caches is never quiet, at start or after a Wait: its
    segment can run a whole batch against the cache (R0) without a call.
    *The prototype also broke this:* the Teardown releaser ran three batches
    silently at time 0, and the search lost every release.
- **C-independent.** By (a), no other step changes what a quiet segment
  does, with two exceptions, the clock and crashes, each fixed by a change
  of the base semantics. Both searches get each change, so they compare like
  with like.
  - **C-clock.** Unreduced, a resume step ticks the clock; R doesn't. So σ'
    would run every later step 1ms earlier per removed step, and grace
    decisions, deletionTimestamps and delivery times would differ.
    *Base change:* a quiet resume doesn't tick the clock. **This changes
    what the base model can reach.** Today some grace-relative states are
    reachable only because no-op steps tick the clock. In the prototype,
    Teardown interleaving A at one preemption lost all 8 of its schedules
    that release over a live user after the grace: the grace couldn't run
    out within the steps left. Real time isn't counted in steps. So the
    honest fix is to make the passage of time an explicit choice of the
    environment:
    - **C-clock+.** Whenever some actor waits on a future notBefore, "advance
      the clock to the earliest such notBefore" is an option at every step,
      not only when nothing else is enabled.
    - It isn't counted as a preemption, since no actor is switched away
      from, and it's capped by a new bound T (time advances per schedule),
      default 1 in CI.
    - Both searches get it. It makes both strictly more complete than
      today's model, which could reach those states only through the
      accident of tick counts. Its cost goes into §8's measurement.
  - **C-crash.** A segment reads the process pointer and builds its protocol
    from `(*p).ver` and `(*p).seen`, and Restart clears its crashed flag. If
    a crash of the same process falls between the segment and its first call
    (unreduced: resume, crash, call), today the pass is lost, and its
    protocol holds the dead process's memory. σ' can't put the crash there.
    *Base changes:*
    - (i) **A restart resets process memory in place.** The same
      `*verified`, `*firstSight` and cache objects are cleared and relisted,
      not replaced, so a segment that read the pointers before the crash
      sees the new process's memory, as one that read them after it does.
    - (ii) **A crash kills only work that has made a call.** Each actor
      records the process generation at the first call of its unit (a
      holder pass, a release batch, a deleter call), and a call returns
      ErrCrashed iff the process restarted since then. Restart and Wait end
      the unit. A unit with no call yet is unaffected by a crash: it is the
      new process's work, as in a real restart, where the reconcile runs
      again in the new process.

    With (i) and (ii), resume→crash→call and crash→resume→call reach the same
    state.
- **C-enabledness.** A quiet segment enables or disables nothing, by
  C-invisible. A ready quiet park can't be disabled before R runs it: R runs
  it before the next step.
- **C-stable.** A quiet Wait's condition must stay true once true, or σ could
  end with the actor never resumed (a deadlock, or a skipped unit), which R
  can't reproduce. *Obligation:* every quiet condition is either:
  - a monotone counter threshold (`started`, `batchesStarted`, `livePasses`,
    `batches` ≥ k);
  - or a model fact no later step undoes in that scenario: "the Secret is
    deleting or gone", "the holder is deleting or gone", "the Secret's UID
    changed". The rotation actor's "gone" wait is undone only by that
    actor's own later recreate, so it is stable while the actor waits on
    it.

  Any other Wait stays non-quiet.
- **C-progress.** Every quiet segment reaches a park, and no cycle of quiet
  parks runs without a call. So R terminates.

**C-bounds: the mapping keeps σ' within (P, F).**
- *Faults:* every call of σ is in σ', with the same fault, so F is
  unchanged.
- *Preemptions:* removing a resume step s_X of actor X between steps of Y
  and Z can only remove switches. The switch into s_X and the switch out of
  it each cost at most one preemption in σ, against at most one for Y→Z in
  σ'. The switch into X's later call has the same predecessor in both, and
  X is enabled in both: in σ at its call after its resume, in σ' at the same
  call after its silent segment. So σ' has no more preemptions than σ.
- *Time advances (C-clock+):* σ' takes the same advances as σ, at the same
  points between calls.

**Faults still apply at every original point.** Faults apply only at calls,
and every call is still a step. A crash that, unreduced, fell between a
segment and its call is reached in σ' with the crash before the silent
segment, which (i) and (ii) make equivalent.

## 5. What it does not do

- It doesn't reduce calls: every call, every fault, and every order of calls
  between actors is still enumerated.
- It doesn't reduce informer deliveries, which are visible. Their timing
  against calls is searched in full, and they are likely the largest factor
  left. Reducing them needs a dependence relation (DPOR), which is out of
  scope.

## 6. Tests of the reduction

- **A quiet-segment guard (C-invisible as a run-time check).** In tests, a
  quiet segment that moves the model's sequence number, a version counter on
  the process memory (verified, firstSight, cache), or the world's
  instrumentation counters panics.
- **The differential test.** For scenarios small enough to search both ways
  (Teardown ×3, AcquireAgainstRelease, AwayAndBack with 3 and 4 live passes,
  TwoReleasers, DeletionsAroundTheGrace, Recreate, and a shrunk two-process
  scenario), compare the reduced and unreduced searches, with the base
  changes in both:
  - with bounds high enough that neither search is cut by them: the sets of
    terminal states must be equal, and so must the sets of violation
    messages. A terminal state is hashed canonically from the model's
    objects, the process memories, and the world's recorded observations
    (reads, releases, holds, deletion phases);
  - at CI's bounds, (1,0), (0,1) and (1,1): the reduced sets must contain
    the unreduced ones (§4's mapping), and any difference is reported.
- **Mutants.** The same comparison on revert-check mutants the model kills:
  - H1's skip without the exact version;
  - M4's late clock;
  - a deletion pass that may skip;
  - R3 without the array test.

  Both searches must find violations, with the same set of messages.
- **Then:** the two-process scenario at (0,1), (1,0) and (1,1), and the
  revert check with it included.

## 7. What changes for the existing results

C-clock, C-clock+ and C-crash change the base model, so every published
schedule count changes, and the reach assertions (U5, L6, H1) are re-checked
under the new semantics. C-clock+ should restore interleaving A's U5
releases. If a reach assertion fails anyway, it is reported, not weakened.

## 8. Measurement (the throwaway prototype)

The prototype, in a scratch copy outside the repository, has R and quiet declarations, but none of
the base changes, and it still breaks C-invisible as described in §4. Its
counts, with one fault:

| Scenario | Today | Prototype |
|---|---|---|
| Teardown | 1,314 | 567 |
| AwayAndBack, 3 live passes | 19,555 | 7,270 |
| TwoReleasers | 20,060 | 7,438 |
| DeletionsAroundTheGrace | 36,135 | 4,113 |
| Recreate | 43,788 | 7,260 |
| TwoProcessesAcquireOneHolder | 2,625,480 | 46,284 |

So R cuts the searches by about 3 to 57 times. But these counts **aren't
evidence of soundness**: the prototype lost behaviours (U5 in interleaving A,
releases in Teardown, H1's state in the two-process scenario), each traced
to an obligation above. The real numbers come after the base changes, with
the differential test passing.
