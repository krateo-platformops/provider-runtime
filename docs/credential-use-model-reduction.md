---
type: Architecture
title: provider-runtime — credential-use model, a sound state-space reduction
description: Spec, under soundness review and not implemented, for a partial-order reduction in pkg/credentialuse/internal/apimodel's scheduler, so that the two-process scenario of the credential-use model can be searched exhaustively in CI.
resource: github.com/krateo-platformops/provider-runtime
tags: [spec, model-checking, credential-use, tests]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use model: a sound state-space reduction

**Status: spec, under soundness review, not implemented** (revision 2). It
is item 2 of the user's decision on #59: make TwoProcessesAcquireOneHolder
exhaustive in CI, with no cut in scope.
- The user has decided the base semantics (§3): event-driven time with up to
  two jumps to deadlines, and crashes that lose only units that have made a
  call.
- The new base multiplies every search by 70 to 110, so the user decided on
  a stateful search with a time-abstract state hash (§10). §10 is under
  review, before any code.
- The implementation goes in this order:
  1. the new base and the stateful search;
  2. the gate (§3.3, adjusted as §10.4 proposes);
  3. the quiet-segment reduction, if it's still needed (§4);
  4. the two-process scenario, exhaustive in CI.

A throwaway measurement prototype ran in a scratch copy, outside the
repository (§9). It is how two of the obligations below were found. The
spec sits under PR 2 of
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
  and F faults. `Explore` is a DFS over every choice within (P, F). (On the
  new base, only visible steps count, §5.)
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

## 3. The new base semantics (decided by the user)

Both searches, reduced and unreduced, run on this base. It replaces §1's
clock and crash rules. Each change is needed to make §5's reduction exact.
The user decided the choices below.

### 3.1 Event-driven time

- **Only calls move time.** Each call step ticks the clock, by at most 1s
  (§10.2), and no other step does: not resumes, not informer deliveries, not relists. A delivery
  carries the time of the last call before it. Equal times can only stop a
  skip, since `canSkip` needs `t_obs < t_issue` strictly.
- **Up to T = 2 jumps per schedule.** Any pending deadline later than the
  clock can be chosen at any step, as an explicit step of the environment.
  The clock jumps to it, and no actor runs. The pending deadlines are:
  - every actor's notBefore;
  - every grace end, for each P being deleted and each process: the later of
    P's deletionTimestamp and that process's first sight, plus the grace;
  - the thresholds each scenario declares (its "into the grace" moments).

  A jump isn't a preemption, since no actor is switched away from. T counts
  jumps.
- **When nothing is enabled**, the clock still jumps to the earliest
  notBefore, as today. That jump is forced, and doesn't count against T.

**Ticks must be negligible next to every deadline** (the reviewer's
Medium). The argument below fails if call ticks can cross a deadline. With
today's 5ms test grace and 1ms per call, any five calls end the grace with
no jump, so "many calls, still inside the grace" can't be represented, and
time would move through uncounted ticks as well as through the jumps the
user decided. So:
- **The test grace is 1000h**, against at most 1s per call (§10.2 sets the
  time unit to 1h). A schedule makes at most a few hundred calls, so ticks
  never come near a deadline.
- **Every "into the grace" moment a scenario needs is a declared
  deadline**, reached by a jump, not by counting calls.
- **A call's tick never reaches a pending deadline.** Deadlines built from
  event times lie only a few ticks apart: a deletion and its first sight, or
  two processes' first sights. So after a jump to one, a fixed tick would
  cross the next within a few calls. The tick is therefore at most 1/1024
  of the time left to the next pending deadline, and at most 1s, which
  leaves room for hundreds of calls (§10.2's capacity condition). **A run-time check panics if a tick can't fit**, that
  is, if a call would have to cross a deadline. Only jumps (and the forced
  jump when nothing is enabled) cross deadlines.

Decoupled time reaches everything today's base reaches: each place where a
tick crossed a deadline becomes a jump at the same point. The reach
counters then show whether T = 2 is enough. The case to watch is Teardown
interleaving A's 8 U5 schedules at one preemption. If T = 2 loses any of
them, the work stops and is reported; T is not raised to make it pass.

**Why deadlines are enough.** Every time-dependent decision compares the
clock with a deadline:
- GraceOver is "now ≥ grace end";
- a requeue or a timer is "now ≥ notBefore";
- a scenario threshold.

The remaining time comparisons (t_obs, t_issue, delivery times,
deletionTimestamps) compare step times with each other, and only their order
matters, which calls already fix. So any assignment of real times to a
schedule's steps is equivalent, for every check, to one where time moves a
tick per call and jumps only to deadlines. Each jump crosses one deadline the
schedule needs crossed early. T = 2 covers the scenarios' needs, one grace
and one timer. A scenario that needs more raises T, and the vacuity counters
show which.

### 3.2 Crashes lose only work that has made a call

- **(i) A restart resets process memory in place.** The same `*verified`,
  `*firstSight` and cache objects are cleared and relisted, not replaced.
  Code that read the process pointer before the crash sees the new process's
  memory, just as code that read it after the crash does.
- **(ii) A unit loses its work only if it has made a call.** Each actor
  records the process generation at the first call of its unit of work: a
  holder pass, a release batch, a deleter call. A call returns ErrCrashed if
  and only if the process has restarted since then. Restart and Wait end the
  unit. A unit with no call yet is unaffected: it becomes the new process's
  work, as in a real restart, where the reconcile runs again.
- **The obligation (the reviewer's): a unit whose generation is stale
  doesn't touch process memory, neither mutating nor reading it.** Reads
  matter too: a stale unit's acquire could take the skip path, which makes
  no call, read the new process's memory, and report Held.
  - In tests, `verified` and `firstSight` get an unexported access hook,
    called on every read and every mutation, and nil in production. The
    harness's cache gets the same hook.
  - The harness sets it to panic if the running actor's unit generation is
    stale.
  - Today's protocol code reads and mutates process memory only in the same
    step as a successful call, or before the unit's first call. A stale
    unit's calls all fail, and acquire and release return at the first
    failure. So the hook should never fire, and it makes that a checked
    property.
- **The pass budget changes.** Under the old base, a crash between a pass's
  segment and its first call lost the pass, and the lost pass counted
  against the holder's live or deletion passes. Under (ii), the pass goes on
  in the new process. So in those schedules a holder has one more pass that
  does work than under the old base. The gate in §3.3 compares the two bases
  with this difference in place.
- **A stale R0 view is legitimate.** A releaser with caches whose unit began
  before a crash, and has made no call yet, may decide R0 on the old cache's
  view, read before the in-place reset. Caches may lag by any amount, and R0
  can only keep. So this is a stale cache, which the design already allows
  (§6.4, "What the proof assumes").

### 3.3 The gate before the reduction goes in (M-b, the user's)

In this order:
1. **The old base, first.** Rerun the revert check on the old base to
   establish its result: 45 killed and 1 equivalent, with mutant 2
   replaced. The run also produces a **per-mutant matrix**: for each mutant,
   every test and model scenario it is killed in, found without `-failfast`
   so that every scenario runs. Its output replaces the stale one from
   6f22bd6.
2. **Then the new base, unreduced, with the same two runs.** The results go
   side by side:
   - **the revert check must kill the same mutants**, and the matrix shows,
     per mutant, the scenarios that kill it on each base;
   - **the reach counters may not drop:** for each scenario and each
     exhaustive profile, the number of schedules reaching U5, L6, H1's state
     and a deleting read must be no lower than the old base's.

If any count is lower, the work stops and is reported. It is not adjusted
to pass.

**Superseded by the user's decision (§10.4).** With the stateful search,
counts from the two bases aren't comparable. The gate is inclusion of
terminal states, S_old ⊆ S_new, together with the per-mutant matrix and
equal violation messages under each killed mutant.

**What the gate does not show.**
- Schedule counts aren't a behaviour-inclusion check: the two bases'
  schedules aren't comparable one to one.
- The reach counters cover only U5, L6, H1's state and deleting reads.
  Other states are compared only through the mutants each base kills.

## 4. The reduction

**Quiet segments.** A segment is **quiet** if it meets three conditions:
- (a) it reads only actor-local variables, immutable values, and the process
  pointer;
- (b) it writes only actor-local variables: its own crash state, through
  Restart, and the protocol value it builds;
- (c) it ends at a park (a Call, a Wait, or the actor's end) without making
  a call.

The harness **declares** quietness at the park that precedes the segment:
`WaitQuiet(...)` instead of `Wait(...)`, and a `QuietStart` flag on `Go`.

**Rule R.** Before computing each step's options, the scheduler resumes, at
once, every actor parked at a quiet park whose Wait is ready. It does so in
the order the actors were added, and repeats until none is left. This is a
**silent transition**: no choice point, no clock change, no preemption, no
change to `last`, and no trace entry. With rule R off, each quiet resume is
a step of its own, and it is still never a preemption and never `last`
(§5: only visible steps count). The segment runs to the actor's next
park. If that park is a Call, the Call is an ordinary step later, with all
its fault options.

**Unchanged:** every Call stays a step, with its faults, and so does every
time jump. So does every resume from a park that isn't quiet, including the
informer's watch and the relister's relist, whose segments are visible.

**(a) is checked, not trusted (M-a).** In tests, the scheduler marks the
running actor as inside a quiet segment, and these panic there:
- the clock's `Now()` (passing the clock along, as `newProtocol` does, is
  allowed);
- every accessor of the model and of the harness's caches;
- the world's counters and records.

The guard also panics on (b)'s side: any mutation of the model, the process
memory or the world inside a quiet segment.

## 5. Why it is sound

**Claim, forward.** For every schedule σ of the unreduced search within
(P, F, T), there is a schedule σ' of the reduced search within (P, F, T),
with:
- the same calls in the same order, with the same faults and results;
- the same jumps and the same informer deliveries, at the same points
  between calls;
- the same terminal state and the same checked observations, and so the
  same violations.

**The forward mapping.** σ' is σ with each quiet resume step removed. Its
segment runs silently at the point R runs it: right after the step that made
its Wait ready, or at the start.

**Claim, converse.** For every schedule ρ of the reduced search within
(P, F, T), there is an unreduced schedule within (P, F, T) with the same
calls, jumps, deliveries, terminal state and observations. So the reduced
search adds no behaviour.

**The converse mapping.** Put each quiet resume into the unreduced
schedule as a step at the point where R ran it in ρ: right after the step
that made its Wait ready (a call, a visible resume, or a jump, chosen or
forced), or at the start, in the order R ran them. The unreduced search
offers each of them there: a ready quiet park is enabled, and the
preemption bound never filters out a quiet resume (below). The two
schedules then pass through the same state at every visible step: every
actor at the same park, with the same memory.

**Preemptions count visible steps only (the user's decision).** As in
CHESS, a preemption is a **visible step** (a call, or a resume from a park
that isn't quiet) whose actor isn't the last visible step's, while that
actor is still enabled. A quiet resume is never a preemption and never
becomes the last actor, in every search: with rule R, where it isn't a step
at all, and without it, where it is a step of its own.

**The conditions** (partial-order reduction terms: a singleton ample set made
of an invisible transition, independent of every other, with no cycle to
ignore):

- **C-invisible.** By (b), checked by M-a's guard, a quiet segment changes
  nothing that any of these read:
  - any check: the safety invariant, checkHeld, the grace check, the
    deletion-phase check, convergence, the vacuity counters;
  - any other actor's step;
  - any Wait condition.

  *Harness obligations:*
  - The counters scenario Waits read (`started`, `batchesStarted`) move into
    the op of the unit's first call (a "began" hook on modelServer). The
    measurement prototype (§9) left them in segments, and lost every
    schedule in H1's state.
  - A releaser with caches is never quiet: its segment can run a whole R0
    batch against the cache without a call. The prototype marked one quiet,
    and lost every release in Teardown.
- **C-independent.** By (a), checked by M-a's guard, no other step changes
  what a quiet segment does, with two exceptions, both closed by §3:
  - **the clock:** resumes never move time on the new base, so removing or
    moving one shifts no time;
  - **crashes:** with §3.2's (i) and (ii), resume→crash→call and
    crash→resume→call reach the same state.
- **C-enabledness.** A quiet segment enables or disables nothing, by
  C-invisible. A ready quiet park can't be disabled before R runs it, since R
  runs it before the next step.
- **C-stable.** A quiet Wait's condition must stay true once it is true.
  *Obligation:* every quiet condition is either:
  - a monotone counter threshold (`started`, `batchesStarted`,
    `livePasses`, `batches` ≥ k);
  - or a model fact no later step undoes in that scenario: "the Secret is
    deleting or gone", "the holder is deleting or gone", "the Secret's UID
    changed".

  The rotation actor's "gone" wait is undone only by that actor's own later
  recreate, so it holds while the actor waits on it. Any other Wait stays
  non-quiet.
- **C-progress.** Every quiet segment reaches a park, and no cycle of quiet
  parks runs without a call. So R terminates.

**C-bounds.** Faults and jumps are the same steps in both schedules:
- *Faults:* σ' has every call of σ, with the same fault.
- *Jumps:* σ' has the same jumps, at the same points between calls. Each is
  offered in σ' too:
  - an actor's notBefore can differ between the two only while σ still has
    it at a ready quiet park, whose notBefore has passed, so every deadline
    pending in σ is pending in σ';
  - a jump is offered only while some non-daemon actor has more to do than
    a quiet segment that is due: an actor at a ready quiet park doesn't
    count, in either search. Without that rule, σ could jump while the last
    live actor sits at a ready quiet park whose segment ends it. σ' can't
    make that jump: R has run the segment, and only daemons are left. With
    the rule, every actor that counts in σ counts in σ'. Nothing is lost:
    by C-independent, moving a quiet resume ahead of a jump reaches the
    same state.

A visible step's cost depends on three things: its own actor, the last
visible actor L, and whether L is enabled.
- *Converse:* at every visible step the two schedules are in the same
  state, so the same actors are enabled, and every step costs the same.
  Quiet resumes cost nothing. The bounds hold exactly.
- *Forward:* σ' is σ without its quiet resumes. The visible steps are the
  same, in the same order, so each has the same actor and the same L. And
  every actor enabled at a visible step of σ' is enabled at the same step
  of σ: either it is at the same park in both, or σ hasn't yet taken a
  quiet resume that R has taken in σ', and the ready quiet park it still
  sits at in σ is enabled. So no visible step costs more in σ' than in σ.

**Why the old counting failed.** Under §1's counting, a quiet resume taken
as a step was a switch like any other: it cost a preemption if the last
actor was enabled, and it became the last actor. So the cost of switching
to X was paid at X's resume, and it depended on the time then. Take Y's
call, after which Y waits for a deadline; then X's quiet resume, free,
since Y is disabled; then a jump to Y's deadline, which wakes Y; then X's
call, free, since X is the last actor. Reduced, R runs X's segment
silently, X's call comes after the jump with Y last and enabled, and it
costs a preemption. Revision 2's argument said the switch into X's call
"has the same predecessor in both schedules, and X is enabled in both".
That held for X, but not for the last actor: a jump between X's quiet
resume and X's call can wake the last actor, so the switch's cost depends
on time. The differential test found it. On Teardown interleaving A at
{1,0}, the old counting gave 240 terminal states reduced against 246
unreduced; visible-only counting gives 240 = 240.

Visible-only counting is the new base's, so it changes what a bound p
reaches against the old base: an old schedule within p can cost more than
p on the new base. The gate maps that difference explicitly (§10.4,
preemption reclassification).

**Faults still apply at every original point.** Faults apply only at calls,
and every call is still a step. A crash that, unreduced, fell between a
segment and its call is reached in σ' with the crash before the silent
segment. §3.2 makes the two equivalent.

## 6. What it does not do

- It doesn't reduce calls or jumps: every call, every fault, every jump,
  and every order of calls between actors is still enumerated.
- It doesn't reduce informer deliveries, which are visible. Their timing
  against calls is searched in full, and they are likely the largest factor
  left. Reducing them needs a dependence relation (DPOR), which is out of
  scope.

## 7. Tests

- **The guards.** These run in every model test, both searches:
  - M-a's quiet-segment guard;
  - §3.2's stale-generation guard.
- **The gate** (§3.3), on the new base, before the reduction.
- **The differential test.** Compare the reduced and unreduced searches on
  the new base, for scenarios small enough to search both ways: Teardown ×3,
  AcquireAgainstRelease, AwayAndBack with 3 and 4 live passes,
  TwoReleasers, DeletionsAroundTheGrace, Recreate, and a shrunk two-process
  scenario. Two comparisons:
  - with bounds high enough that neither search is cut by them: the sets of
    terminal states must be equal, and so must the sets of violation
    messages. A terminal state is hashed canonically from the model's
    objects, the process memories, and the world's recorded observations
    (reads, releases, holds, deletion phases);
  - at CI's bounds, (1,0), (0,1) and (1,1) with T = 2: the two sets must be
    equal too, by §5's two mappings, and any difference is reported.
- **Mutants.** The same comparison on revert-check mutants the model kills:
  - H1's skip without the exact version;
  - M4's late clock;
  - a deletion pass that may skip;
  - R3 without the array test.

  Both searches must find violations, with the same set of messages.
- **Then:** the two-process scenario exhaustive at (0,1), (1,0) and (1,1)
  in CI, and the revert check with it included.

## 8. What changes for the existing results

The new base changes every published schedule count. The gate (§3.3) is
what keeps the change honest. The mutants killed and the reach counters are
compared with the old base, side by side, and any drop stops the work.

## 9. Measurement (the throwaway prototype)

The prototype, in a scratch copy outside the repository, has R and quiet
declarations, but none of §3's base changes, and it still breaks
C-invisible as described in §5. Its counts, with one fault:

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

## 10. Stateful search with a time-abstract state hash (the user's decision, under review)

*Revision 3: answers the soundness reviews of 87f333b and 5bb4446, and records
the user's decision on the gate. Revision 4 records the user's two decisions
after the reduction's first build: preemptions count visible steps only
(§5, and §10.4's third mapping), and the time unit is 1h (§10.2's capacity
condition).*

**Why.** The new base (§3) is what the user decided, but T = 2 jumps multiply
every search by 70 to 110. AwayAndBack with 3 live passes goes from 19,555
to 1,538,761 schedules with one fault, and from 29,833 to 3,307,693 with one
preemption. Its search with one preemption and one fault would be about 100
million. Each step offers a jump to each pending deadline, and two jumps can
sit at any two positions, most of them equivalent. The user chose the
timed-model-checking answer: region equivalence (Alur and Dill; UPPAAL's
zones) and a stateful search that remembers visited states (SPIN, TLC). Two
paths that reach the same abstract state are searched once.

### 10.1 What a state is

An abstract state has seven parts. Each is hashed canonically.

1. **Every actor's control point and local memory.** A Go coroutine's stack
   can't be inspected. So an actor's state is made of three things:
   - **Its declared persistent locals:** the variables it keeps across units
     of work, registered explicitly. Examples: a holder's pass number, a
     releaser's batch number, a deleter's next key. **Names captured in a
     closure count as locals.** Recreate's wait condition captures the
     first Secret's UID, so that UID is declared and renamed like any other
     name (point 2).
   - **A digest of its inputs in the current unit:** every call's label,
     fault and result, and every value its own code reads from shared state
     through a hooked accessor (views, the verified set, first sights, the
     clock, the world's counters). The digest resets when a unit ends
     (Restart, Wait).
   - **The time values it holds, as live time terms** (§10.2), not frozen
     ranks. R2's now is held across the Namespace GET and the list. An A2's
     issue time is held across the call. Their relation to the clock and to
     deadlines changes while they are held, so they take part in the time
     abstraction like any stored time. The digest refers to them by term.

   An actor is deterministic given its code and its inputs. So two actors
   with equal locals, equal digests and equal held terms have stacks that
   behave alike, and merging them is sound. The representation is finer
   than the stack, so it can miss merges, which costs search time and never
   soundness. *Obligation:* every read of shared state goes through a hooked
   accessor. The merge audit (§10.5) is the check on this.
2. **The API model:** every object's metadata, and the event log after the
   oldest cache's position, since the older part can no longer be delivered.
   **Names are renamed canonically:** UIDs, resourceVersions and epochs are
   compared only for equality, so each is replaced by its index of first
   occurrence in a canonical traversal of the state.
   - The traversal orders objects by key (kind, namespace, name, which the
     scenario fixes), and actors by registration. It never sorts by raw
     UIDs, resourceVersions or epochs.
   - Every place a name lives is renamed together: objects, caches, the
     verified set, monitors, and actors' locals and held values.
   - The model's counters only need to allocate fresh names, which renaming
     keeps.
   - Log positions (cache sequence numbers) are stored relative to the
     retained suffix.
3. **The caches:** their objects, and where each stands in the retained log.
4. **Process memory:** the verified sets and first sights, with their times
   as terms (§10.2), and, for each actor, whether its unit is stale or not
   begun. That is relative to the process's generation, not the counter.
5. **The scheduler:** each actor's park (its label and whether it's done, its
   notBefore as a term; a Wait's condition is a function of the state), and
   the last actor, because whether a switch costs a preemption depends on
   it.
6. **The checks, as monitors with small state.** Checking over the whole
   history of reads and releases at the end would make almost every state
   distinct. So each check runs **on the fly, at the event it is about**, and
   records a violation then, with the path that reached it.
   - **The hold monitor** updates on every model event of each holder, made
     by any actor, through the model's event hook. A version whose record
     drops P ends that holder's hold on P. It keeps the earliest unbroken
     hold per holder and P, as a term. It no longer replays the log from an
     old position (today's `namedSince`).
   - **The read monitor** keeps the latest allowed U5 release's
     `NamespaceReadAt`, `GraceEnd` and `at`, as terms, and the namespace
     deletion's time. With them it checks each deleting holder's read as it
     happens.
   - **The other parts:** the deletion-phase flags, the scenario's counters,
     and **flags for event-type reach**. A U5 release, an L6 pass, an H1
     pass and a deleting read are events, so each sets a monitor flag
     that becomes part of the state.
   - Convergence is checked at terminal states, as now.
7. **Time,** as §10.2 says.

### 10.2 Time: region equivalence for a discrete clock

**Order alone is not enough.** Code compares a term built as t + c (a grace
end, a requeue) with another term u, which amounts to comparing u − t with c.
Order keeps neither the integer parts nor the differences. Today's code has
four such cases:
- (a) R2's WaitForGrace requeue becomes a wait until `at + RequeueAfter` in
  the harness. That is the grace end plus the ticks between R2's now and
  `at`, not the grace end itself.
- (b) `requeueAt` falls back to 1ms when the grace end is already past.
- (c) a restored epoch's requeue at now + `restoredEpochRequeue`, which can
  fall either side of a pending grace end;
- (d) the late deletions' threshold, dt + grace/2, created after dt.

**The representation: values relative to the earliest live term, with tick
parts reduced to their order.**
- **Every constant the code adds to a time is an integer multiple of a unit
  U = 1h:** the model's grace is 1000h, grace/2 is 500h, and the model's
  restored-epoch requeue is 1h.
  - `restoredEpochRequeue` is a protocol field for this. Production leaves
    it unset and keeps 1s, the constant and its doc unchanged; a test pins
    that. The model sets one U.
  - U was 1s until the capacity condition below showed that the clock's 1ns
    resolution can't hold two nested jumps at that size.
- **A call's tick is at most B = 1s**, and less near deadlines and live
  terms' fine parts (the invariant below). A schedule makes at most K = 512
  calls between two jumps (below), so its level-0 ticks add up to at most
  512s, below U/2 = 30min.
- Every time value is then a jump-built coarse part plus a fine part: an
  earlier term plus constants, from jumps to deadlines, plus a sum of ticks.
  Written relative to the earliest live term, a value is n·U + f with
  |f| < U/2.
- **A term is represented by the pair (n, the rank of f among all live
  terms' fine parts).** The ranking is global, across whole levels, and the
  terms include every registered deadline, passed or pending (grace ends,
  notBefores, thresholds). Then t + c against u is decided
  lexicographically: n_t + c/U against n_u, then f_t against f_u. Both are
  kept, so every comparison the code can make has the same answer in two
  states with equal representations.
- **The invariant the representation relies on: a tick never moves now's
  fine part past any live term's fine part.** The tick is
  min(B, a D-th of the gap to the next pending deadline, a D-th of the gap
  from now's fine part to the next larger fine part of any live term), with
  D = 1024, and at least the clock's 1ns resolution. A tick that can't fit
  panics. Then:
  - fixed terms keep a static global order of fine parts, and that order
    changes only when a term is added;
  - now's place in it doesn't depend on tick sizes;
  - cross-level facts are kept. A deadline built later from an old base can
    land at the same level as one built from a recent base, and their order
    is then f_old against f_recent, which the global order holds.

  The only thing the invariant gives up is a tick passing an unregistered
  t + c, which the design excludes: the code compares now only with
  registered deadlines.
- **The capacity condition, and why U = 1h and B = 1s.** Fine parts of
  events after a jump sit between fine parts already there, so each jump
  shrinks the room ticks have, by about e·D. The condition that 1ns still
  holds the deepest level's ticks, for T nested jumps and at most K calls
  between any two jumps (chosen or forced):
  - K ≤ D calls shrink a gap by at most a factor (1 − 1/D)^K ≥ 1/e, so
    every tick of a level is at least the level's starting gap over e·D;
  - a jump lands on a term, and the next larger fine part, made at the
    level before, is at least one of that level's ticks away: so a level's
    gap is at least the level before's smallest tick;
  - level 0's gap is U/2 (fine parts start at 0, at a whole U), and its
    ticks are also at most B, so its smallest tick is at least
    min(B, U/2/(e·D));
  - so the deepest level's smallest tick is at least
    min(B, U/2/(e·D))/(e·D)^T, and that must be at least 1ns:

  **room = min(e·D·B, U/2) ≥ (e·D)^(T+1) ns.**

  With U = 1h, B = 1s, D = 1024 and T = 2: room = min(2,784s, 1,800s) =
  1,800s, against (e·D)^3 ≈ 21.6s, a margin of about 83. With revision 3's
  U = 1s and B = 1ms, room = min(2.78s, 0.5s) = 0.5s, 43 times too small:
  DeletionsAroundTheGrace's small variant at {1,0} panicked with "a call's
  tick would pass a live term's fine part". A third nested jump doesn't fit
  either way: (e·D)^4 ≈ 16.7h.
  - The scheduler checks the condition at the start of every search, for
    the search's T (`CheckCapacity`), and K is declared,
    `MaxCallsBetweenJumps` = 512: a schedule that makes more calls between
    two jumps panics, since the condition no longer covers it.
  - A test runs the tick's own arithmetic in the worst case the condition
    is derived for (K calls per level, each jump landing one smallest tick
    below the next fine part). The deepest level's smallest tick is 351ns.
  - The tick's run-time panic stays as it was, the backstop for anything
    the derivation doesn't model.
- The shift to the earliest live term is sound because every comparison is
  a difference.
- **Why not zones (DBMs).** Zones represent symbolic clocks and constraints
  between them. Here the clock is concrete and discrete, the only
  constraints are u − t ≷ c with c a multiple of U, and fine parts are
  bounded below U/2. The integer part with fine-part order is the region
  construction, and it is exact for this case. Zones would be needed only
  if ticks could add up to U, which the assertion below rules out.

**The assertion, a run-time check.** It panics if any of these happens:
- a tick crosses a pending deadline, or brings now from outside U of a
  deadline to inside it, which would mean the coarse part moved by ticks;
- a schedule's total ticks reach U/2;
- **(L1) a tick changes now's fine rank against a live term t with
  n_now = n_t + c/U, for a constant c in use.** That would change the
  answer to "now ≥ t + c" without a deadline for t + c being registered.
  Today every such t + c is registered as a deadline, so this can't happen,
  but the representation's exactness claim is general, so the check is
  too.

Only jumps move coarse parts, and only jumps cross deadlines.

**(L2) Further obligations, each checked:**
- **A deadline is registered with its base term, and the registration
  panics unless it differs from that term by a multiple of U.**
- **`Clock.Since` panics in the model.** Every duration the code needs is a
  constant, or a difference that the absolute requeues remove.
- **The schedule's start sits on a whole U** (16:00:00), so that coarse
  parts are whole multiples from the start.

**Two code changes make every requeue a term** (they land with the
implementation):
- **Requeues become absolute.** `ReleaseResult` returns `RequeueAt`, the
  grace end itself or now + `restoredEpochRequeue`, instead of a duration,
  and the harness waits until exactly that time. That removes (a).
- **The 1ms fallback becomes "now".** A grace end already past requeues at
  once. That removes (b).
- (c) and (d) are deadlines built as a term plus a constant, which the
  representation handles.

**Every reader of time, and what it does with the value:**

| Reader | Use | How it's handled |
|---|---|---|
| `acquire.go` A1, `at := clock.Now()` | stored as `observed` | a term, compared only |
| `acquire.go` A2, `issued := clock.Now()` (×2) | held across the call, then stored as `written` | a held term (§10.1), then a stored term |
| `release.go` R1, `now` | stored as a first sight; `GraceEnd` = max(dt, first sight) + grace; `now.Before(graceEnd)`; the requeue | the first sight is a term, the grace end a deadline; the requeue is absolute (`RequeueAt = graceEnd`) |
| `release.go` R1, a restored epoch | `RequeueAt = now + epochRequeue()` | case (c): a deadline, term plus one U in the model (1s in production) |
| `release.go` R2, `now` before the reads | held across the Namespace GET and the list; GraceOver is now ≥ grace end | a held term, compared with a deadline |
| `release.go` R2, `nsReadAt = clock.Now()` | stored in the decision, read by the read monitor | a term |
| `release.go` `requeueAt` | the earliest requeue | an absolute minimum of terms; a past grace end gives now (case (b), removed) |
| `decide.go` `GraceEnd` | max(dt, first sight) + grace, through a clamped `Sub` | a deadline, term plus constant |
| the model: Create and Delete | creation and deletion timestamps | terms; `Patch` keeps them from the stored object, so no truncation |
| the harness: deliveries (`holderEvent`, `listCompleted`) | `recordSince`, `lastList` | terms |
| the harness: holds, `nsDeleted`, a release's `at` | the monitors | terms |
| the harness: requeue waits | until `RequeueAt` | case (a), removed: exactly the grace end |
| the harness: late deletions | until dt + grace/2 | case (d): a deadline, term plus constant |
| the harness: `converge` | its clock at now + 3 × grace, after the search | outside the search |
| the scheduler: `tick` | at most 1s, and a 1024th of the gap to the next deadline or larger fine part | a fine part; only its order is kept, and the assertion bounds the total |

**What enforces "time is used only by comparison, storage, or adding a
constant":**
- **M-a's guard (§4):** a quiet segment can't read the clock at all.
- **The run-time assertion above.**
- **The merge audit (§10.5).** It catches what a perturbation check can't:
  behaviour that depends on differences in history between two merged
  states.
- **Review:** the table above is the list of readers. A new reader must be
  added to it, and the audit catches one that isn't.

### 10.3 The bounds stay exact under caching

A visited state is stored with the **budgets it was reached with**: the
preemptions, faults and jumps it had left. A state reached again is pruned
only if a stored budget dominates the new one, meaning at least as large in
all three. Every future the new path could take within its budget has then
been searched from that state already. Otherwise the state is searched
again, and the new budget joins the stored set, which keeps only budgets no
other dominates. The review confirmed this dominance rule as sound.

The last actor is part of the state, so the preemption accounting is the
same on both paths. The search stays a replay DFS: a pruned path ends at the
visited state, and the search backtracks as at a terminal state.

### 10.4 What is reported, and the gate (the user's decision)

**Reporting.**
- **Counts are of distinct abstract states, not schedules:** states visited,
  distinct terminal states, and, for each reach flag (U5, L6, H1's state, a
  deleting read), the distinct states in which it is set. Paths merge, so
  the number of schedules is no longer known, and none is reported.
- **A violation** is reported once per distinct violating state, with the
  first path found to it as its witness.
- **The vacuity checks** stay, on distinct-state counts.

**The gate (§3.3), as the user decided: inclusion of terminal states, through
a projection both bases share.**
- **Why a projection.** The old base's constants (a 5ms grace, 1ms ticks, a
  threshold at start + grace − 1ms) aren't multiples of U, and its ticks
  carry time. So an old state mapped through §10.2's relative representation
  would never land in S_new.
- **The projection.** Both bases' terminal states are compared on a coarser
  abstraction:
  - names renamed canonically;
  - monitors as §10.1 defines them;
  - every time term, deadlines included (grace ends per P and process,
    notBefores, thresholds), kept **only by its order, ties included**.
- **The requirement is proj(S_old) ⊆ proj(S_new),** per scenario and
  profile. The old base is enumerated statelessly, offline; the new base's
  terminal states come from the stateful search.
- **Why order alone is fine as a projection, but wasn't for merging (M1).**
  - Merging needs equal *futures*. A future can compare t + c with u, and
    order alone doesn't decide that.
  - A terminal state has no future. What the checks are about there are
    order facts: GraceOver (now against a grace end), the namespace deleted
    before or after the Namespace GET and the grace end, a hold before the
    holder's deletion. So the projection keeps what the gate compares, and
    drops only values no check reads at the end.
- **The three intended differences go through a declared mapping, or are
  reported for triage, never silently dropped:**
  - crash rule (ii)'s extra pass (§3.2);
  - the redefined thresholds: the late deletions' "into the grace" moves
    from start + grace − 1ms to dt + grace/2;
  - **preemption reclassification** (the user's decision, §5): the new base
    counts only visible steps as preemptions, so an old schedule within
    bound p can cost p′ ≠ p on the new base. For each old-base terminal
    state found at bound p, its labelled old schedule
    (`actor:label!fault`, with jumps where the old base's ticks crossed a
    deadline) is replayed on the new base with `ReplayLabels`, its
    visible-only preemption count p′ is computed, and the state must be in
    proj(S_new) at p′. Where p′ is above the gate's bound, the state is
    checked with a reduced new-base search at p′. On Teardown interleaving
    A at {1,0}, 6 terminal states need p′ = 2. They are checked with a
    reduced new-base search at {2,0}, and the gate's output lists them as
    mapped, each with its p′.

  All three are path-based, so they are implemented by **replaying the old
  schedule's labelled trace** (each step's actor, label and fault) on the
  new base, not its choice indices, which mean nothing on the other base.
  A trace that doesn't replay, or that replays to a terminal state outside
  proj(S_new), goes to triage.
- **The per-mutant matrix is equal or larger**: every scenario that kills a
  mutant on the old base kills it on the new.
- **Under each killed mutant, the old and new bases report equal sets of
  violation messages.**

### 10.5 Tests of the stateful search

- **The differential test.** On scenarios small enough to search both ways,
  stateful against stateless: the same set of terminal abstract states, and
  the same set of violation messages. The scenarios are Teardown ×3,
  AcquireAgainstRelease, the smaller AwayAndBack, and **the two-process
  scenario, shrunk** (one live pass for B, one deletion pass each), at
  (0,1) and (1,0) with T = 2. The same comparison runs on the revert-check
  mutants the model kills: H1's skip without the exact version, M4's late
  clock, a deletion pass that may skip, and R3 without the array test. Both
  searches must find violations, with the same messages.
- **The merge audit.** In a test mode, at every prune (or a sample of prunes,
  on the big scenarios), keep the pruned path, a concrete state as a replay
  prefix, and the stored witness path. Run both under the same sequences of
  choices. The abstract traces and the violations must be identical.
  - This is the check that the abstraction loses nothing the futures depend
    on: a read that bypasses a hook, a time used as a value, a name the
    renaming misses.
  - It runs on every prune in the differential test's scenarios.
  - **On the big scenarios (L3):**
    - audited pairs run to the end; k is only a cap, and a pair that hits it
      is reported;
    - several continuations are tried per audited prune: the first, and
      seeded random ones;
    - the sample is seeded, and the seed logged;
    - the sample favours prunes whose two concrete states differ the most,
      so it doesn't spend itself on identical ones.
  - **One full offline audit** checks every prune of the two-process
    scenario at {0,1}, about 2.6 million stateless schedules' worth.

### 10.6 Risks and traps, stated before the code

- **How much merges is an empirical question.** Swapping two steps that both
  store times can change the state, and often should: an A2's issue time
  against a delivery time is exactly what t_obs < t_issue compares. The
  gain is expected from jump positions and from steps that store no time. A
  measurement after the review will show it. If it isn't enough for the
  two-process scenario in CI, that will be reported, not worked around.
- **Traps the audit must catch, and the review named:**
  - **time:** a duration computed as a difference and used later (cases (a)
    to (d), removed or made terms); ticks adding up toward U;
  - **actor representation:**
    - a held time frozen as a rank instead of a live term;
    - a name in a closure that the renaming misses;
    - a monitor that reads from an old log position instead of updating on
      events;
    - a canonical traversal that sorts by raw names.
- **The per-actor digest is only as good as its hooks.** A missed read is a
  soundness bug, and the merge audit is the guard against it.

**What the merge audit found while the search was built**:
- **Fine parts ranked globally, with no tick invariant.** On Teardown at
  {0,1}, 71 of 2,907 audited prunes diverged, and the terminal states
  didn't match (537 stateful against 554 stateless). Tick sizes, which are
  values, decided the order of fine parts across levels.
- **Fine parts ranked per level, with deadlines as terms.** 0 of 2,670
  diverged, and terminal states matched (404 = 404). But the reviewer
  showed it isn't sound in general: two deadlines landing at one level from
  bases at different levels are ordered by a cross-level fact that per-level
  ranks drop.
- **Adopted: the reviewer's (A).** A global rank with the tick invariant
  above. One more harness fault came out first: a forced jump (nothing
  enabled) revisited the loop top with the same number of choices, so a
  choice prefix named two states. The audit's replays then started from the
  wrong one. The state is now checked once per choice, after any forced
  jump. Teardown at {1,0}: 0 of 2,577 audited prunes diverged, and terminal
  states matched (246 = 246). The results across every scenario and both
  bounds are reported with the code.

## 11. The stateless reference: measured, then split (the user's decision)

*Pre-registered before the measurement runs.*

**Why.** The differential test's first leg, the stateless unreduced search at
T = 2, is its whole cost. On Teardown it is 0.62 to 1.95 million schedules per
subtest, 100 to 200 times the stateful search's runs. On every other small
scenario it didn't finish: 16.9h on a c3-standard-22, 10h for
DeletionsAroundTheGrace on an n2-standard-32, one core per subtest
(hack/differential-credential-use-2/). The user chose to keep the check as it
is and make it parallel: measure each scenario's size first, then split the
depth-first search by choice prefix across cores.

### 11.1 The measurement, and how it is extrapolated

For each small scenario of §10.5 at {0,1} and {1,0}, with T = 2 (the
differential test's 18 subtests), `TestModelStatelessSize` (behind
`CREDENTIALUSE_MODEL_SIZE`) reports three things, streamed as it goes:

- **Knuth's estimate of the schedule count** (Knuth, "Estimating the
  efficiency of backtrack programs", 1975). A probe is one schedule with every
  choice drawn uniformly from the options the search would offer there, under
  the same bounds. The product of the option counts along it is an unbiased
  estimate of the number of schedules, which are the leaves of the choice
  tree. The estimate is the mean over N probes, with its relative standard
  error, computed in the log domain. N = 20,000 per subtest, from one logged
  seed.
- **A capped depth-first search**, the serial search itself, stopped at
  300,000 schedules: the count reached, the time, the rate (schedules per
  second on one core), and its position in the tree. Position is the fraction
  of leaves before the current one if every subtree at a choice were the same
  size, Σ_k (c_k/n_k)·∏_{j<k}(1/n_j), and runs/position is a second estimate
  of the total. A subtest that finishes under the cap reports its exact
  count.
- **The rate**, from the capped search.

**How it is read, fixed now:**
- **Calibration first.** The serial search finishes on the six Teardown
  subtests, so their counts are known (0.62 to 1.95 million). Both estimates
  are reported against them, as ratios. Knuth's estimator is unbiased but
  heavy-tailed: with too few probes it usually comes out low. If its Teardown
  ratios fall outside [1/3, 3], the other estimates are reported as
  unreliable, not adjusted.
- **The projected total** for a subtest is Knuth's mean. The DFS-position
  estimate is reported beside it and not mixed in.
- **The projected time** is total/rate on one core, and total/(rate·C) on C
  cores, for C = 22, 64 and 128. The split's load imbalance is unknown before
  §11.2 is built, so the parallel times are lower bounds, labelled as such.
- **A subtest is reported infeasible** if its projected time on 128 cores is
  above 24h. That is reported to the user. Nothing is cut or reshaped here.

The schedule count is the stateless search's leaf count exactly: a probe
draws from the options that `Run` computes, and it computes them the same way
for a probe as for the search.

### 11.2 The split: the depth-first search by choice prefix

`apimodel.ExploreParallel(b, workers, scenario)` runs the stateless search's
schedules across workers. Only the stateless search is split; the stateful
searches are 100 to 200 times smaller, and their visited set orders them.

- **A worker owns a subtree:** a choice prefix and a floor, the depth above
  which it doesn't backtrack. It searches the subtree depth first, exactly
  as `Explore` does: replay the prefix, run to a leaf, then take the deepest
  owned choice with an untried option.
- **Splitting.** After each schedule, if the queue of subtrees is shorter
  than the number of workers, the worker hands off every untried option at
  the shallowest depth it owns that has one, each as a subtree of its own,
  and raises its floor past that depth. The first subtree is the whole tree.
  No schedule runs only to learn the tree's shape.
- **Why it runs exactly the serial search's schedules.** A schedule is a
  leaf of the choice tree, named by its choices, and replaying a prefix
  gives the same options, since a scenario is deterministic (the scheduler
  panics when a replay offers fewer). A hand-off splits a subtree into the
  part the worker keeps (the subtree under its current choice at that depth)
  and the parts it gives away (the subtrees under the untried choices), which
  are disjoint and together the whole. So every leaf is in exactly one
  subtree, and is run once. The stateless search has no memory between
  schedules, so what it reports is a function of the set of leaves: the run
  count, the usage, the terminal states and the violations are the serial
  search's.
- **What differs:** the order of schedules; which ten failures are kept when
  there are more (the differential test turns violations into records, so
  only harness errors are failures); and `MaxRuns`, which stops the split
  search within a worker's run of the cap.
- **Callbacks run concurrently.** The differential test's records take a
  lock. Each schedule builds its own world, model and scheduler, and shares
  nothing with another; a short split search on Teardown runs under CI's
  race detector (`TestModelParallelSearchShares`).

**The tests.**
- `TestExploreParallelIsExplore`, in apimodel: on a scenario with calls, a
  wait, a deadline to jump to, faults and preemptions, the multiset of
  schedules, the run count and the usage equal `Explore`'s with 1, 2, 3 and
  8 workers, at three bounds (562, 228 and 4,200 schedules), under the race
  detector.
- `TestModelParallelStatelessIsSerial`, behind
  `CREDENTIALUSE_MODEL_PARALLEL_PROOF`: on every Teardown order at {0,1} and
  {1,0}, where the serial search finishes, the serial and split searches
  must report the same schedule count, the same terminal abstract states and
  the same violations. It takes hours, because its serial half is the
  serial search (0.62 to 1.95 million schedules per subtest).
- The differential test's stateless leg splits across
  `CREDENTIALUSE_MODEL_DIFF_WORKERS` workers; by default it is serial.

### 11.3 What runs, and the pass rules (the user's decision, pre-registered)

The measurement (hack/differential-credential-use-2/size-487543c/RESULTS.md)
found 8 of the 18 subtests infeasible at T = 2, even on 128 cores. The user
decided (2026-11-12): **the feasible set at T = 2, and a T = 1 reference for
the rest.** In this order, each step only if the one before passes:

1. **The split's proof on Teardown** (`TestModelParallelStatelessIsSerial`):
   on the six Teardown subtests, the serial and split stateless searches run
   side by side. **Pass:** for every subtest, both finish, with equal
   schedule counts, equal usage, equal sets of terminal abstract states,
   equal sets of violations, and no harness failure. Anything else stops the
   work, and the parallel runs don't start.
2. **The full reference at T = 2, split,** on the 10 feasible subtests:
   Teardown ×3, acquire against release and deletions around the grace
   (small), each at {0,1} and {1,0}. The differential test's three searches,
   with the stateless one split (`CREDENTIALUSE_MODEL_DIFF_WORKERS`).
   **Pass:** every search finishes; stateless against stateful and stateful
   against reduced have equal sets of terminal abstract states and of
   violations; every prune is audited, with 0 divergences; no harness
   failure.
3. **The four infeasible scenarios** (recreate, away and back, two
   releasers, two processes, each small or shrunk), **reduced coverage:**
   - **First, their size at T = 1,** measured as §11.1 does (Knuth's mean
     from 20,000 probes, a 300,000-schedule capped search for the rate),
     read by the same rules: calibrated on Teardown at T = 1, whose capped
     searches finish; above 24h on 128 cores is infeasible. **If any is
     infeasible at T = 1, it is reported before anything runs.**
   - **Then the three-way comparison at T = 1,** stateless split. **Pass:**
     as in step 2, at T = 1.
   - **And the stateful against reduced comparison at T = 2**, with no
     stateless leg (`CREDENTIALUSE_MODEL_DIFF_STATELESS=off`). **Pass:** both
     finish; equal sets of terminal abstract states and of violations; every
     prune audited, with 0 divergences; no harness failure.
   - **What reduced coverage leaves unchecked, said in the results:** at
     T = 2 these scenarios' stateful search is checked against a stateless
     reference only through the merge audit, not exhaustively; the stateless
     reference covers them at T = 1, so schedules that need a second jump
     are compared stateful against reduced, not against the stateless
     search.

A failed rule is reported with its witness, as the differential test logs
it. Nothing is rerun to pass.

## 12. Sleep sets under the bounds (the user's decision; spec revision 2, for re-review, not implemented)

*For away and back and two releasers, whose stateless reference at T = 1 is
1.25 to 3.2 times too large for §11.3's rule. The two-process scenario is out
of scope: it needs 150 to 570 times, and the user ruled out full DPOR.*

*Revision 2 answers the independent review of revision 1
([credential-use-model-reduction-review-12.md](./credential-use-model-reduction-review-12.md)).
That review found revision 1 unsound, with two counterexamples at P = 0, F = 0,
T = 0 (CE1 and CE2). Each fix below is checked against both, and against the
lemma.*

### 12.1 Why plain sleep sets are not sound here

Sleep sets (Godefroid) skip a transition t at a state once an earlier sibling
subtree has explored t, until a transition dependent with t runs. Under a
preemption bound, the schedule that would cover a pruned one can cost more.
In Coons, Musuvathi and McKinley's terms (Bounded Partial-Order Reduction,
OOPSLA 2013, Def. 4.3 and 4.4), the preemption bound is neither *stable*
(equivalent sequences can cost different preemptions) nor *extensible* (an
independent transition can change another's cost).

**A state lost by plain sleep sets at P = 0** (the review's D8):
- a has two steps: a1 writes U, then a2 writes V. b has two steps: b1 writes
  W, then b2 reads V.
- In b's subtree, a1 stays asleep, because it is independent of b1 and b2.
  So the state in which b reads V = 0 and a runs afterwards is never reached
  there.
- In a's subtree, putting b between a1 and a2 needs a preemption.

**What the paper proves, and what it doesn't:**
- BPOR is DPOR with conservative backtrack points. Besides the point before
  the most recent dependent transition, it adds one at the most recent
  context switch before that transition (Def. 5.2(2), Algorithm 3).
- It is proven to reach every *local* state reachable within the bound
  (Theorems 1, 4 and 8; Theorem 8 assumes an acyclic state space).
- Sleep sets are applied in every run it reports (§5.4), in a limited form:
  none are added at a conservative backtrack point. "Our proofs explicitly
  cover only the unoptimized algorithms" refers to DPOR's backtracking
  optimisations and the all-reachable-within-the-bound optimisation, not to
  sleep sets. Still, no proof in the paper or the dissertation covers sleep
  sets. So no sleep-set theorem is imported.
- The paper's preemption-bound persistent sets need two conditions that
  revision 1 split apart:
  - the segment run until it blocks, ext(s, t) (Def. 4.5);
  - its companion: independence from the *next transition of the last
    actor, even if that transition is disabled* (Def. 4.6, Lemma 2
    condition 4, §4.3.1).

  Revision 1 dropped the companion, which is CE1.
- Musuvathi and Qadeer, "Partial-Order Reduction for Context-Bounded State
  Exploration" (MSR-TR-2007-12), is prior work on the same combination. Its
  report page confirms it; its text hasn't been read. It is read before any
  code, and anything it changes here is recorded.
- We need more than local states: the differential test compares *global*
  terminal states and violations, so §12.3's lemma is about global states.

### 12.2 Transitions, footprints and dependence

- **A transition** is one scheduler step:
  - an actor's call, with its fault;
  - an actor's resume from a Wait or from start;
  - a quiet resume (rule R off, as in the reference);
  - a jump, chosen or forced.
- **A footprint** is the set of (object, mode) a transition reads and
  writes. Every item below must be recorded, by a hook, a diff or the
  scheduler; the review's D3 is folded in.
- **The model:**
  - each key, read or write;
  - every write also writes **the log**: it appends an event and takes the
    next resourceVersion and, on Create, the next UID;
  - the log is read by every informer's `Pending` and `Next`, and by
    `Snapshot`. `Snapshot` has no hook today and gets one: it reads every
    key and the log position;
  - `List(kind, namespace)` reads every key in that range; `Cache.List`, the
    relist, reads every key;
  - `FinalizeNamespace` gets the guard it lacks.
- **Each process:**
  - its cache, verified set and first sights, read or write, per process;
  - its **generation**: every call of an actor of the process reads it,
    through `Stale`;
  - **a crash** writes the generation, the memory, the caches and every
    actor's unit state in the process. It also *reads*: its restart runs with
    no running actor, reads the clock and relists. That restart is attributed
    to the crashing transition explicitly, not through `Sched.running`.
- **The clock:**
  - every reading is a read: `Clock.Now` from an actor's code, and
    `Sched.Now` from the model's own timestamps on Create and Delete, the
    read monitor and the deliveries. `Sched.Now` gets a recording hook;
  - a call's tick is a *tick*. Ticks commute with ticks and conflict with
    reads.
  - **A change of the live terms or of the pending deadlines is found by
    diffing**, not declared: `Terms()` and `pending()` before and after each
    transition. A transition that changes either conflicts with every tick.
    This covers `deadlines()`'s direct reads of every process's first sights
    and of Secrets' deletionTimestamps: a delivery, a relist, a crash's reset
    of first sights, and a Secret's Delete all change it.
- **The world's monitors and counters,** each field apart:
  - `nsDeleted`, `lastAllowed`, `deletingRead`, `u5Any`, `releasedAny`, `l6`,
    `h1`;
  - `holds` per holder, `deletionPhase` per actor, each counter per key;
  - `errs`, whose appends are ordered writes.

  They are plain fields today and get explicit hooks. The monitors' peeks
  (`peekCache`, `peekWritten`) are reads of their process's cache and
  verified set.
- **Guards:**
  - the scheduler evaluates a Wait's condition with no running actor, so it
    records the reads itself, per actor and state, by evaluating the
    condition with read-recording on;
  - **a guard must not read the clock.** A guard that calls `Clock.Now` or
    `Sched.Now` panics. That assertion is new, and F2 rests on it;
  - a guard may write only its actor's own variables, as the late deletions'
    `dt` does. The write detector (§12.4) checks that it writes nothing
    shared.
- **Dependence:** t and u are dependent if
  - they are the same actor's;
  - either is a jump;
  - their footprints conflict: the same object with at least one write, a
    tick against a clock read, or a tick against a term or deadline change;
  - or one writes what the other's guard reads.

### 12.3 The rule

- **A firm free point** is a state where the last actor is unset, or done,
  or blocked at a Wait *by its guard only*, its notBefore already passed.
  Every option there costs no preemption, and neither a jump nor a quiet
  resume can change that (F2).
- **a's segment from a state s** is ext(s, a) as in the paper: a's steps
  from s until a is no longer enabled, with whatever faults the schedule
  chooses on its calls. A quiet resume belongs to its actor's segment. One
  that ends at a Call continues through it. A jump ends every segment.
- **Entries.**
  - At a firm free point s, once a's subtree from s has been searched, a gets
    a *sleep entry* in each later sibling's subtree.
  - The entry carries the union footprint, reads and writes, of every
    variant of a's segment searched in that subtree.
  - **No entry is made** for a daemon, nor for an actor any of whose searched
    variants did not end with the actor blocked or done (D7).
- **Removal.** Along a later subtree, an entry is removed:
  - by every transition dependent with its union footprint (a jump removes
    every entry);
  - **(F1)** at every state of the branch, s' included, if the entry's writes
    meet the *current last actor's guard reads*. The scheduler evaluates that
    guard with read-recording even when the last actor's next transition is
    never taken. Only the last actor's guard is needed: the preemption cost
    reads only its enabledness, and the other actors' guards are covered by
    their own transitions.
- **Application, deferred (F3).** At a firm free point s' with a's entry
  present:
  - a's option is *still offered and taken*, and nothing is filtered, jumps
    included (D5);
  - if a then runs uninterrupted until it blocks or ends, with no other
    actor's step, no quiet resume and no jump in between, the run ends there
    as pruned. That's neither terminal nor a deadlock, and no forced jump
    follows;
  - if anything else comes first, the entry is dropped, and the search goes
    on as usual.

  So no state ever has all its options asleep. Every interrupted variant of
  a's segment is explored, and what is pruned is the continuation after a
  complete, uninterrupted A.
- **ExploreParallel (D6).**
  - A subtree handed off carries only the entries already on its path when
    it is handed off.
  - So the split reduced search explores a superset of the serial reduced
    one. For it, terminal states and violations are compared, not schedule
    counts.

**The lemma, revision 2.**

*Setting:*
- s is a firm free point where a's subtree was searched.
- π is a path from s to a firm free point s' that contains no jump. Every
  transition of π is independent of A's union footprint.
- By F1, A's writes miss the last actor's guard reads at every state of π,
  s' included.
- At s', a runs its complete segment A uninterrupted, then ρ follows.

*Claim:* A · π · ρ is a schedule of the unreduced search from s, explored in
a's subtree. It uses the same faults and jumps, costs no more preemptions
than π · A · ρ, and ends in the same global state, monitors included.

*Proof sketch* (the review's, checked here step by step):
1. **A from s' is a variant of a's segment from s.** By induction over π: π
   writes nothing that A reads, guards included.
   - A's ticks commute with π's.
   - A's term or deadline changes conflict with π's ticks, so if π made a
     call, the entry would be gone.
   - Its timed waits behave the same, because a tick never reaches a
     pending deadline (§3.1).
2. **π's costs are unchanged in A · π:**
   - **The first step is free:** a is not enabled after A.
   - **Each later step:** the last actor is the same, and its enabledness
     is unchanged, by F1 for its guard. For time: π reads no clock, or it
     would conflict with A's ticks. π's timed waits are ready, or not, as
     before: A's ticks reach no pending deadline.
   - **No forced jump appears:** π's next step is always enabled.
3. **After both orders the global states are equal,** the clock included
   (ticks commute), except the last actor: a after π · A, and L, π's last
   visible actor, after A · π.
4. **ρ costs no more in A · π · ρ:**
   - **L stays disabled:** it is unset, done, or blocked by its guard alone
     (s' is firm). ρ's invisible prefix can't wake it: a jump only moves the
     clock, which guards don't read, and a quiet resume writes only its
     actor's variables.
   - **So ρ's first visible step is free in A · π · ρ.** In π · A · ρ it is
     free, or costs one if a jump woke a.
   - **From then on the last actor coincides.** So cost(A · π · ρ) ≤
     cost(π · A · ρ), and the faults and jumps are the same.
5. **The variants F3 doesn't prune are explored.** The pruned set is
   exactly π · A · ρ for complete, uninterrupted A, and every one has an
   explored cover. By induction on the DFS order, a's subtree comes first.

*Checked against the counterexamples:*
- **CE1** (c's guard reads `started`, which a writes; the state is
  R = (1, 0)). After c0, c is the last actor and is blocked by that guard.
  F1 sees A's write to `started`, removes a's entry, and the branch
  c c0 a a1 b b1 is explored, reaching R = (1, 0). F2 alone wouldn't catch
  it: that point is firm.
- **CE2** (L time-blocked at T1 as the last actor; the state is Q = "L").
  After l1, L is blocked by its notBefore, so the point isn't firm. Nothing
  is applied there, and σ is explored. F1 alone wouldn't catch it: L's guard
  reads nothing.
- **§12.1's lost-state example:** a's segment is a1 a2. Its union writes U
  and V, and b2 reads V, so the entry is removed at b2.

*Assumptions the proof rests on, each checked in §12.4:*
- guards don't read the clock;
- the footprints are complete;
- jump options at s' are computed unfiltered;
- the run's tick invariant holds (§3.1).

### 12.4 Checks, before the rule is used on away and back

The review showed that revision 1's commutativity audit couldn't catch CE1
or CE2, which are cost and enabledness effects, not failures to commute, and
that it was only statistical for missing footprints (D9). Revision 2's checks:

1. **A deterministic write detector**, on every transition of every reduced
   run in the soundness differential:
   - before and after each transition, a deep snapshot of the model
     (objects, log, resourceVersion and UID counters), every process's
     memory and caches, the world's fields, and the scheduler's actor
     fields;
   - every object that changed must be in the transition's recorded write
     set. A change outside it fails the run.
2. **A read audit, stratified by kind:** crash, relist, delivery, Create and
   Delete, timed park, guard-only. On seeded samples of adjacent pairs that
   the relation calls independent, both orders are replayed from the same
   prefix, and the concrete states after both are compared, the last actor
   and the bound counters left out.
3. **CE1, CE2 and §12.1's example as exhaustive unit tests** in
   sched_test.go: the reduced search's terminal states must equal the
   unreduced one's.
4. **The soundness differential,** reduced against unreduced. Identical
   terminal states and violations are required, with 0 detector failures and
   0 read-audit mismatches:
   - the 10 feasible subtests at T = 2, against step 2's saved sets;
   - **recreate at T = 1**, against step 2's saved set. It has the patterns
     CE1 and CE2 are built from: a `before` guard on a model fact, a
     releaser's trigger, the rotation's quiet waits;
   - **a guards-and-timers scenario, new and small enough to search
     unreduced at T = 2,** built from the harness's patterns in away and
     back. Those are the holders' `before` guards on `started` and
     `batchesStarted` (model_test.go's two-process scenario) and a cacheless
     releaser's timed requeue as the last actor. It is sized before it's
     used;
   - the four mutants, each against an unreduced run of the same mutant.

   A failure stops the work.

### 12.5 The plan and its cost

**In order:**
1. Read MSR-TR-2007-12 and record what it changes.
2. Build the footprints of §12.2, the guard assertion and the write
   detector, and check the detector against the unreduced search first.
3. Build the rule of §12.3 in `Explore` and `ExploreParallel`. Entries live
   on the replay stack per depth, with each searched option's union
   footprint. Add the unit tests.
4. Re-review the code.
5. Measure the factor on the six Teardown subtests at T = 2.
6. Run the soundness differential of §12.4.
7. Only then, away and back and two releasers exhaustively at T = 1, sized
   first with a capped reduced run and the Teardown factor.

**Cost, re-estimated with the review's fixes:**

| work | days |
|---|---|
| reading MSR-TR-2007-12 | 0.25 |
| footprints with D3: generations, the crash's reads, `Snapshot`'s hook, `Sched.Now`'s hook, term and deadline diffing, the world's fields, guard read-recording, the guard assertion | 2 to 2.5 |
| the rule with F1 to F3, entries on the replay stack, the parallel hand-off | 1.5 |
| the write detector and the stratified read audit | 1.5 |
| the unit tests, the guards-and-timers scenario and its sizing | 1 |
| the code re-review and its fixes | 1 |
| **total** | **about 7 to 8** |

That is about twice revision 1's 3.5 days.

**VM time:**
- about 2h for the factor;
- the differential's reduced runs cost the unreduced runs divided by the
  factor: up to step 2's own 32 to 51h on 96 vCPU if the factor is near 1.

**Risks, stated before the code:**
- F1, F2 and F3 each give up reduction:
  - F1 drops an entry whenever the last actor's guard reads what it writes;
  - F2 excludes the free points where the last actor waits on a timer,
    which is common, since releasers wait on requeues;
  - F3 runs A itself before pruning.

  The factor on away and back and two releasers may stay below the 1.25 to
  3.2 times they need. If it does, that is reported, not worked around.
- Footprint completeness is the soundness hinge. The write detector checks
  the writes deterministically. Reads are checked only by the stratified
  audit and the differential.
