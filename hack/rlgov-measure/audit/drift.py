#!/usr/bin/env python3
"""Audit §5 (M5) and the withdrawn caveat 3: time effects and the window.

The day x n permutation test (exploratory, chosen after seeing the data), S*
by day and by stage, each n = 4-5 run's 300-s block means, the per-n mean
profile in 150-s blocks from t = 0, and S* against the warm-up. Caveat 3 read
the late-window rise at n = 5 as a short warm-up; it is withdrawn, because
MVA at the measured c reproduces the 300-2100 s window means (mva.py)."""
import argparse
import itertools
import math
import random
import statistics as st

import auditlib as al

T = al.TARGET


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    a = ap.parse_args()
    runs = al.load_runs(a.results)
    day2 = {k for k, v in runs.items() if v["started"].startswith("2026-11-11")}
    x = {n: {k: v["U"] for k, v in runs.items() if v["n"] == n} for n in (4, 5)}

    def eff(n, grp):
        return st.mean(x[n][k] for k in grp) - st.mean(x[n][k] for k in x[n] if k not in grp)

    o4, o5 = eff(4, [k for k in x[4] if k in day2]), eff(5, [k for k in x[5] if k in day2])
    c4, c5 = list(itertools.combinations(x[4], 3)), list(itertools.combinations(x[5], 3))
    p = sum(1 for g4 in c4 for g5 in c5 if abs(eff(4, g4) - eff(5, g5)) >= abs(o4 - o5) - 1e-12) / (len(c4) * len(c5))
    print("day 2 - day 1: n4 %+.4f, n5 %+.4f; interaction permutation p = %.4f of %d" % (o4, o5, p, len(c4) * len(c5)))
    for n, o, c in ((4, o4, c4), (5, o5, c5)):
        print("  day effect n=%d: p = %.3f" % (n, sum(1 for gg in c if abs(eff(n, gg)) >= abs(o) - 1e-12) / len(c)))
    u3 = st.mean(v["U"] for v in runs.values() if v["n"] == 3)

    def s_of(u4, u5):
        return 4 + (T - u4) / (u5 - u4) if u4 <= T else 3 + (T - u3) / (u4 - u3)

    for lab, keep in (("day 1", lambda k: k not in day2), ("day 2", lambda k: k in day2),
                      ("stage 1", lambda k: "-s2-" not in k), ("stage 2 only", lambda k: "-s2-" in k)):
        u4 = st.mean(x[4][k] for k in x[4] if keep(k))
        u5 = st.mean(x[5][k] for k in x[5] if keep(k))
        print("  %-12s U4 %.4f U5 %.4f S* %.4f" % (lab, u4, u5, s_of(u4, u5)))

    print("\n300-s block means in the window (n = 4-5):")
    for k in sorted(k for k in runs if runs[k]["n"] in (4, 5)):
        u = [c for t, c in runs[k]["series"] if t >= al.WARMUP]
        bl = [st.mean(u[i * 300:(i + 1) * 300]) for i in range(6)]
        print("  %-12s %s" % (k, " ".join("%.4f" % b for b in bl)))
    print("\nper-n mean profile, 150-s blocks from t = 0:")
    ns = sorted({v["n"] for v in runs.values()})
    for n in ns:
        prof = [st.mean(c for v in runs.values() if v["n"] == n for t, c in v["series"] if b * 150 < t <= (b + 1) * 150)
                for b in range(14)]
        print("  n=%d %s" % (n, " ".join("%.3f" % q for q in prof)))
    print("\nS* against the warm-up (bootstrap B=10000, seed 5):")
    for wu in (300, 450, 600, 900, 1200, 1500):
        g = {}
        for v in runs.values():
            g.setdefault(v["n"], []).append(st.mean(c for t, c in v["series"] if t >= wu))
        mm = {n: st.mean(q) for n, q in g.items()}
        S = al.cross(mm)[0]
        lo, hi = al.pct(al.bootstrap(g, lambda d: al.cross(d)[0], random.Random(5), 10000))
        print("  %4d s: S* %.4f [%.3f, %.3f] width %.3f vs 0.15S* %.3f" % (wu, S, lo, hi, hi - lo, 0.15 * S))


if __name__ == "__main__":
    main()
