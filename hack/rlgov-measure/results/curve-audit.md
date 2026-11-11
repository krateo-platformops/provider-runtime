# Plant curve: independent audit and the decision's caveats

An independent read-only audit of the plant-curve measurement (stages 1 and 2, head
fa28147) recomputed everything from the run logs. **The protocol was followed and every
committed number reproduces.** Settings were committed before run 1, the runs follow the
pre-registered orders (seeds 20261110 and 20261111), each log was committed once, the node
was constant across all 22 runs, and the two amendments (the burn-counter fix, and the
re-run of the interrupted pc-n6-r3 attempt) don't depend on U. The pre-registered rule
was applied correctly: S* = 4.14, bootstrap 95% interval [3.92, 4.38], width 0.45,
against 0.15·Ŝ* = 0.62, so **determined**.

By the user's decision (2026-11-11), the gate uses **S* = 4.14 as the rule produced it**,
with these caveats stated:

1. **The delta-method check disagrees.** The pre-registered delta interval is 0.64 wide
   with z (0.73 with t on 10 df), and Fieller's ratio interval is wider still. Both are
   above 0.62. The bootstrap is narrower because, when a resample's U(4) exceeds 0.225, it
   crosses on the steeper 3–4 segment. The curve is concave, so the bootstrap's treatment
   is the more realistic one: a monotone (PCHIP) bootstrap also gives width 0.44.
2. **The interval undercovers.** A simulation of this design puts the percentile
   bootstrap's real coverage at about 85–87%, not 95%. Corrected widths still pass: 0.47
   (√(k/(k−1))), 0.52 (t10) and 0.575 (t5). The margin under 0.62 shrinks to 7–24%.
3. **Withdrawn by the auditor (2026-11-11).** It had read a late-window rise at n = 5 as
   a too-short warm-up, with S* ≈ 4.05–4.15. A closed-form check (mean-value analysis of a
   processor-sharing closed queue at the measured 3157 ms of CPU per reconcile, fitting
   nothing) reproduces every window mean within 0.004 cores, and puts the stationary S* at
   4.33, inside [3.92, 4.38]. So the rise isn't evidence of a higher, or lower, steady state,
   and 4.14 isn't known to be biased.
4. **k*'s factor 0.71 assumes S* midway between the bracketing n.** At 4.16 the weight
   gives k* = 8, not 6. The rule was followed as pre-registered, and the decision uses the
   realized width.
5. **Stage 2 is one time block** (11 Nov, n = 4 before n = 5). An exploratory,
   post-hoc split by day gives S* of 4.28 (day 1) against 3.94 (day 2). The design can't
   separate a time effect from drift.

Smaller notes: the 4.03–4.26 leave-one-out range; smooth interpolations give 4.09–4.14;
method B checks the counters rather than the plant; and the "18:44Z" in 3aa8174's message
is most likely local time.

Lesson for the GKE re-measure after PR 5: randomize across days, and check window means
against the queueing model at the measured CPU per reconcile.
