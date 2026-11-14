---
type: Review
title: provider-runtime — credential-use model, review of spec §12 (sleep sets) at d7fa08b
description: Independent review of docs/credential-use-model-reduction.md §12, revision 1, by a fresh reviewer: two counterexamples, the fixes, footprint omissions and the audit's limits. §12 revision 2 answers it.
resource: github.com/krateo-platformops/provider-runtime
tags: [review, model-checking, credential-use, sleep-sets]
timestamp: 2026-11-12T00:00:00Z
---

# Independent review: §12 "Sleep sets under the bounds" (d7fa08b)

Worktree: provider-runtime/.claude/worktrees/agent-a56ab3373faf6abc1, branch
feat/credential-use-2-protocol, head d7fa08b. Read only. No code was run apart
from text extraction of the two papers.

Sources: the OOPSLA 2013 paper and Coons's dissertation were both fetched as
PDFs and read as text. Claims about the paper below are from that text. The one
claim from memory (Musuvathi and Qadeer, MSR-TR-2007, cited as [13]) is marked.

## Verdict

**Unsound as written.** There are two independent counterexamples (CE1, CE2)
at P = 0, F = 0, T = 0. Each is a schedule within the bounds whose terminal
global state the §12.3 rule never reaches. Both break the simple case of the
lemma as well as the general one.

**Likely sound with three fixes (F1, F2, F3) and the footprint additions (D3).**
F1 and F2 are each checked against the counterexample it closes. I could not
build a counterexample once all three are in. That is a proof sketch, not a
proof, and it rests on two assumptions: guards never read the clock, and the
footprints are complete.

---

## D1 (critical): the guard of a parked last actor is not in the dependence relation (CE1)

§12.2 attributes a Wait's guard reads to "that actor's next transition". The
cost of a step depends on `s.last.enabled(now)` (sched.go:664, 742). The last
actor's next transition is usually *not in π*: the actor took a step in π and
then parked. So A can flip the enabledness of π's last actor while still being
"independent of every transition of π, guards included". The general case's
"the same enabled actors (A changes no guard)" does not follow from the premise.

**CE1.** P = 0, F = 0, T = 0. Actors registered a, c, b. No timed waits.
- a: `Call(a1: Get(K1); started++)`, then end. This is the harness's own
  pattern: `w.inc(w.started, …)` inside the first call's transition
  (model_test.go:477).
- c: `Call(c0: write W)`, then `Wait(started >= 1)`, then `Call(c1: write Z)`,
  then end. This is the away-and-back pattern of `before` guards on `started`
  and `batchesStarted` (model_test.go:1083-1099).
- b: `Call(b1: R := (W, Z); write R)`, then end.

Unreduced schedule σ = c:start c:c0 b:start b:b1 a:start a:a1 c:w c:c1. It costs
0 preemptions: after c0, c is disabled (started = 0), so b's switch is free.
σ ends with **R = (1, 0)**.

Reduced search:
- *a first:* a ends with started = 1. Then either c runs c0, after which c is
  enabled (started = 1) and b cannot preempt it, giving R = (1, 1). Or b runs
  first, giving R = (0, 0).
- *c first* (a holds an entry): c:start and c:c0 are independent of A, which
  touches K1, `started` and a tick. The state after c0 is free, because c is
  disabled. a is asleep there. b:start and b:b1 are independent of A too: A
  never touches the log. The state after b is free, a is still asleep, c is
  disabled, and the branch is abandoned. The other order, c c0 a a1 b b1
  (also cost 0), is pruned at the same free point.
- *b first:* entries for a and c. b reads W = 0, so this branch cannot reach
  R = (1, 0).

So R = (1, 0) is unreachable in the reduced search. The covering schedule
A·π·ρ = a a1 c c0 b b1 … needs one preemption, because c is enabled and last.
CE1 generalises to any P by prefixing P preemption-requiring steps.

**F1 (fix).** At every state along the pruned branch, the guard reads of the
*current last actor* are evaluated with read-recording, *even when its next
transition is never taken*. An entry whose write set meets those reads is
removed. This includes the state s' itself. It is exactly the condition that
§12.3 dropped from the paper: Lemma 2 condition 4,
β ↔ next(final(S.α1…αi), αi.tid), and §4.3.1, "independent with the next
transition by each thread …, even if that transition is disabled".

Checked on CE1: after c0, c is last and its guard reads `started`, which A
writes. So a's entry is removed, the branch c c0 a a1 b b1 is explored, and it
reaches R = (1, 0).

Only the last actor's guard is needed, not every parked actor's. The cost check
reads only last's enabledness, and π's own steps are covered by the existing
rule. This keeps the informer's `Pending()` guard (model_test.go:415) from
killing every entry, except at states right after an informer step.

## D2 (critical): `last` differs after the swap, and an invisible transition can wake one but not the other (CE2)

The simple case says "last included as far as costs see it: in both orders the
last actor is not enabled". After π·A the last actor is a. After A·π it is L,
π's last visible actor. Jumps, chosen or forced, and quiet resumes do not change
`last` (sched.go:696, 737, 748). So a jump at the start of ρ can wake L while a
stays done. Then ρ's first visible step is free in the pruned order and costs a
preemption in the explored one. The paper's model has no time and no invisible
transitions, so it has no counterpart to this. It is the same phenomenon as
§5's "why the old counting failed".

**CE2.** P = 0, F = 0, T = 0. `Deadlines()` = {T1}, a constant threshold as
`w.thresholds` allows (model_test.go:239, 343), so no park changes the pending
set. Rule R is off, as in the reference. Actors registered X, a, L.
- X (QuietStart): `Wait(T1)`, then `Call(x1: Q = "X")`, then end.
- a: `Call(a1: Get(Ka))`, then end.
- L: `Call(l1: Get(Kl))`, then `Wait(T1)`, then `Call(l2: Q = "L")`, then end.

Unreduced schedule σ = X:start(quiet) L:start L:l1 a:start a:a1
[forced jump to T1] X:xw X:x1 L:lw L:l2. Cost 0: after the jump, last = a is
done. σ ends with **Q = "L"**.

Reduced search: in every branch where a runs first, L is last at the forced jump
and enabled after it, so X cannot go first and Q = "X". In every branch where L's
segment precedes a, the free point after L:l1 has a asleep: A is independent,
with ticks commuting and no deadline change. That branch is abandoned. With
P = 0, a:start is never offered while L sits at its Call. So Q = "L" is
unreachable.

**F2 (fix).** Apply entries only at a *firm* free point. That is a free point
whose last actor is unset, done, or blocked at a Wait whose notBefore has
already passed (blocked by its guard only). It also needs an assertion that
guards never call `Clock.Now`; today nothing stops them. Under F2, L at s' can be
woken only by a visible step, and at that step `last` coincides in both orders.

Checked on CE2: L at s' is time-blocked, so s' is not firm, a is taken, and σ is
explored. CE1 is still caught only by F1, because c is guard-blocked and so
firm. Both fixes are needed.

The alternative is to make a jump reset `last`. That changes the base semantics
of what P reaches, and it is the user's decision, so I do not recommend it here.

## D3 (high): footprint omissions in §12.2

Each of these is an access the list does not name, or names only on one side.
- **Process generation reads.** Every Call reads `proc.gen` through `Stale`
  (sched.go:469, 473, 478-479, 511-513), but §12.2 lists only the crash's write.
  Swap: a's call with FaultCrash in process P, against b's call in P whose unit
  had already made a call. In π·A, b's op runs. In A·π, b gets ErrCrashed and
  its op never runs. Different states, but "independent" under §12.2.
- **The crash's reads.** Restart runs with `s.running = nil`
  (sched.go:502-505). It reads the clock (sched.go:504) and relists
  (harness_test.go:181-188 → Cache.List, cache.go:47 → Model.Snapshot,
  model.go:201). Snapshot has no Guard hook, and it reads every key and the log
  position. Any hook attributed through `s.running` loses all of this.
- **Model timestamps bypass the clock hook.** model_test.go:262 is
  `apimodel.New(s.Now)`, and Sched.Now (sched.go:405) does not go through
  Clock.Now. So Create and Delete read the clock unseen (model.go:158, 364),
  and so does the read monitor (`w.s.Now()`, model_test.go:540). These are
  caught only if term and deadline changes are found by *diffing*
  `Terms()`/`pending()` around each transition. The spec says "changes", but
  not how they are detected.
- **Deadlines from first sights.** `deadlines()` (model_test.go:343-359) reads
  every process's `seen.at` "directly, as instrumentation", plus Secret
  deletionTimestamps through `m.List`. So a delivery, a relist, a crash's reset
  of `seen` (which *removes* deadlines) and a Secret Delete all change the
  ticks.
- **Monitor reads bypass the access hooks.** `peekCache` → Cache.Objects
  (model_test.go:1224, cache.go:105) and `peekWritten` (model_test.go:1235) do
  this, inside checkHeld (model_test.go:704-719). World fields `nsDeleted`
  (written at 667 and 1149), `lastAllowed`, `deletingRead`, `u5Any`, `l6` and
  `holds` are plain struct fields that need manual hooks on both sides. `errs`
  is an ordered slice (model_test.go:362).
- **Smaller items.** Cache.List (the relist) reads the whole model. It is not
  `Model.List(kind, ns)`. FinalizeNamespace has no `m.guard()` (model.go:373),
  and no model-harness caller today. Create also reads the namespace key and
  `m.uid`; the log covers both.
- **Guard evaluation runs with `running == nil`.** It happens at sched.go:559,
  659, 664, 742 and 885. The scheduler must attribute guard reads per
  (actor, state) itself (needed for F1).

## D4 (medium): the rule prunes every continuation of a; the lemma covers only an uninterrupted, complete A

Rule 3 drops a's option at s', and with it π·A1·u·ρ, where A1 is a proper prefix
of a's segment.
- **u is a preemption.** A1·π·u·ρ has equal cost: the preemption moves from u to
  π's first visible step, and u is then free because L stays disabled (F1).
  This holds, but the spec should say so.
- **u is a quiet resume.** It commutes with everything (§5), so move it past A.
- **u is a chosen jump J.** A1·π·J costs one more preemption. The cover has to be
  π·J·A·ρ. That exists only if the jump options at s' are kept (see D5), and it
  matches only if A1's ticks being erased or kept by J is invisible. Concretely
  the clock differs, so this relies on §10.2's abstraction, which the spec does
  not invoke.

**F3 (fix, simplest sound option).** Deferred pruning. Take a's step at s'.
If a then runs uninterrupted to block, abandon the branch at that point. If any
other step or a jump comes first, drop the entry. This explores the
interrupted variants and still prunes every ρ subtree after a complete A.
Alternatively, give entries only to actors whose every variant is one step long.

## D5 (medium, implementation trap)

- If the sleep filter runs before sched.go:672, the jump options disappear
  whenever a was the only enabled actor. The subtree π·J… is then lost and
  nothing covers it.
- A state whose options are all asleep must end the run as "pruned". Otherwise
  sched.go:677-698 forces a jump, which produces a schedule the unreduced
  search does not have, or reports a deadlock (sched.go:693-694).

## D6 (medium): ExploreParallel

`split` (parallel.go:73-91) hands off untried siblings *before* the current
sibling's subtree is finished, so "after a's subtree has been searched" has no
moment there.
- A handed-off subtree may carry only the entries already on its path.
- The parallel search then explores a superset of the serial one, so §11.3
  step 1's "equal schedule counts" will no longer hold for the reduced search.
  Compare terminal sets instead.

## D7 (low)

- "Quiet resumes are length-one segments" holds only when the quiet segment
  ends at an unready Wait or at the actor's end. One that ends at a Call leaves
  the actor enabled, so its segment includes the Call.
- A daemon whose Wait is always ready (relist, model_test.go:431) never
  completes a segment, so it must never get an entry.
- The guard at model_test.go:1135 writes `dt`. Guards are not pure, and this is
  harmless only because `dt` is actor-local and the value it stores is stable.

## D8 (low): §12.1 and the citations

- **Accurate:** Defs 4.3 and 4.4 (stable, extensible), and "the preemption bound
  is neither stable nor extensible". Algorithm 3 is BPOR for the preemption
  bound. Theorems 1, 4 and 8 cover local-state reachability; Theorem 8 assumes
  an acyclic state space. Def 4.5 is ext(s, t). §5.4: "we never place
  transitions in the sleep set if they were conservatively added by BPOR due to
  the bound". The quote "our proofs explicitly cover only the unoptimized
  algorithms" is right.
- **Inaccurate gloss.** "That is, without sleep sets" is wrong. §5.4 says sleep
  sets are *always* applied, to both DPOR and BPOR. "Unoptimized" means without
  the three DPOR backtracking optimisations and without the
  all-reachable-within-the-bound optimisation. The conclusion still stands: no
  proof covers sleep sets.
- **Imprecise.** The conservative backtrack point is the most recent context
  switch *before the most recent dependent transition* (Def 5.2(2)), not just
  "the most recent point where the executing thread changed".
- **The omission that matters.** §12.1 cites ext(s, t) but not its companion
  condition, independence with last(α)'s next transition even if disabled. That
  omission is D1.
- **The example.** §12.1's example shows only that the covering argument fails,
  not that a state is lost. Here is a lost state for plain sleep sets at P = 0.
  a has two steps: a1 writes U, then a2 writes V. b has two steps: b1 writes W,
  then b2 reads V. In b's subtree, a1 stays asleep, and the state where b reads
  V = 0 and a runs afterwards is lost. In a's subtree, putting b between a1 and
  a2 needs a preemption.
- **From memory, unconfirmed.** [13], Musuvathi and Qadeer, "Partial-order
  reduction for context-bounded state exploration" (MSR-TR-2007-12), is prior
  work on exactly this combination. The spec should consult it.

## D9 (high, process): the commutativity audit cannot catch D1 or D2, and it can pass with a footprint missing

- **As written, it can fail on correct footprints.** It compares bound counters
  and the key. The key includes `last=` (sched.go:710). Swapping two
  independent steps of different actors changes `last`, and often the
  preemption count, because the bound is not stable.
- **Once relaxed, it checks only state commutation.** CE1 and CE2 commute
  exactly at the state level; they are cost and enabledness effects. Only the
  differential can see them. The 10 Teardown-family subtests may not exercise
  cross-actor counter guards or timed last actors. Away and back, the scenario
  the rule is meant for, does (model_test.go:1083-1099, 630-632).
- **It is probabilistic for D3.** Crash × stale call, restart × write, and
  deadline-changing writes are rare in a uniform sample of adjacent pairs.
- **The time-abstract key hides concrete tick differences**, and peek-only
  monitor fields may not be in the key at all.

Recommended:
- (a) A deterministic write detector: deep-snapshot the model (objs, log, rv,
  uid), every process's memory and caches, the world struct, and the actor
  fields before and after *every* transition. Assert that the recorded write
  set covers every change. This catches missing write hooks on every
  transition.
- (b) A pairwise audit for reads, stratified by transition kind (crash,
  relist, delivery, Create/Delete, timed park, guard-only), comparing the
  concrete state with `last` and the counters excluded.
- (c) CE1 and CE2 as exhaustive unit tests in sched_test.go.
- (d) A differential case with the CE1 and CE2 patterns before the rule is
  used on away and back.

## Proof sketch with F1 + F2 + F3

Setting: s is free; π runs from s to s' and contains no jumps; s' is firm (F2).
Every transition of π is independent of A's union footprint. With F1, A's union
writes miss the last actor's guard reads at every state of π, s' included.

- **A from s' matches a variant from s.** By induction over π: π writes nothing
  A reads (guards included). A's timed waits change deadlines, which conflict
  with π's ticks. Ticks commute.
- **π's costs are the same in A·π.**
  - The first step is free, because a is disabled after A.
  - At every later step, `last` is the same and its enabledness is unchanged:
    by F1 for guards; and for time because ticks never reach a pending
    deadline, and deadline changes conflict with ticks.
  - No forced jumps appear, because π's next step is always enabled.
- **After both orders, the states are equal except `last` (a vs L).**
  - L is firm (F2), so it stays disabled across jumps and quiet resumes.
  - So ρ's first visible step is free in A·π·ρ, and `last` coincides from then
    on.
  - Hence cost(A·π·ρ) ≤ cost(π·A·ρ).
- **Interrupted continuations** are explored (F3).
- **Coverage by induction** on DFS order: a's subtree precedes.

Assumptions:
- guards don't read the clock;
- the D3 hooks are complete;
- the jump options at s' are computed unfiltered (D5).
