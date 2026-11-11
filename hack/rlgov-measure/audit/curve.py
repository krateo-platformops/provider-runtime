#!/usr/bin/env python3
"""Audit §3-5: stage 1's k*, the final S* and every interval the audit quoted.

Stage 1 (Ŝ*, β̂, pooled ŝ, t, k*, the midpoint factor), the final per-n means,
S*, the bootstrap (several seeds, and curve_report.py's own seed and quantile
convention), the delta method, Fieller's interval, small-sample-corrected
bootstraps, a PCHIP bootstrap, leave-one-out, and alternative interpolants."""
import argparse
import math
import random
import statistics as st

import auditlib as al

T = al.TARGET


def lagrange_cross(means, pts):
    xs = list(pts)
    ys = [means[x] for x in xs]

    def f(x):
        return sum(ys[i] * math.prod((x - xs[j]) / (xs[i] - xs[j]) for j in range(len(xs)) if j != i)
                   for i in range(len(xs)))

    lo, hi = 3.1, 5.0
    for _ in range(100):
        mid = (lo + hi) / 2
        if (f(lo) - T) * (f(mid) - T) <= 0:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    ap.add_argument("--b", type=int, default=50000, help="bootstrap resamples (default 50000)")
    a = ap.parse_args()
    runs = al.load_runs(a.results)

    # ---- stage 1
    g = al.by_n(runs, keep=lambda name: "-s2-" not in name)
    m = {n: st.mean(v) for n, v in g.items()}
    s = {n: st.stdev(v) for n, v in g.items()}
    S, na, nb = al.cross(m)
    beta = (m[nb] - m[na]) / (nb - na)
    ka, kb = len(g[na]), len(g[nb])
    df = ka + kb - 2
    pooled = math.sqrt(((ka - 1) * s[na] ** 2 + (kb - 1) * s[nb] ** 2) / df)
    t = al.t_ppf(0.975, df)
    raw = (2 * t * 0.71 * pooled / (beta * 0.15 * S)) ** 2
    f = S - na
    w = math.sqrt((1 - f) ** 2 + f ** 2)
    raw_w = (2 * t * w * pooled / (beta * 0.15 * S)) ** 2
    print("STAGE 1 means:", {n: round(m[n], 5) for n in sorted(m)})
    print("  S*=%.4f between %d and %d; beta=%.5f; pooled sd=%.5f on %d df; t=%.3f; k*=%d (raw %.2f)" % (
        S, na, nb, beta, pooled, df, t, math.ceil(raw), raw))
    print("  weight factor at S*: %.3f (formula 0.71) -> k*=%d (raw %.2f)" % (w, math.ceil(raw_w), raw_w))

    # ---- final
    g = al.by_n(runs)
    ns = sorted(g)
    m = {n: st.mean(v) for n, v in g.items()}
    s = {n: (st.stdev(v) if len(v) > 1 else float("nan")) for n, v in g.items()}
    print("\nFINAL per n:")
    for n in ns:
        k = len(g[n])
        print("  n=%d k=%d mean %.5f sd %.5f  95%% t CI ±%.5f" % (n, k, m[n], s[n], al.t_ppf(0.975, k - 1) * s[n] / math.sqrt(k)))
    for x, y in zip(ns, ns[1:]):
        print("  slope %d->%d: %.5f" % (x, y, m[y] - m[x]))
    S, na, nb = al.cross(m)
    print("S* = %.4f between %d and %d; 0.15*S* = %.4f" % (S, na, nb, 0.15 * S))

    def sfn(mm):
        return al.cross(mm)[0]

    for seed in (1, 2, 3, 20261110):
        out = al.bootstrap(g, sfn, random.Random(seed), a.b)
        lo, hi = al.pct(out)
        n34 = sum(1 for x in out if x < 4)
        print("  bootstrap seed %d B=%d: [%.4f, %.4f] width %.4f; no crossing %d; crossing in 3-4: %.3f" % (
            seed, a.b, lo, hi, hi - lo, sum(math.isinf(x) for x in out), n34 / a.b))
    # curve_report.py's own: seed 20261110, B=10000, statistics.mean over rng.choice per n in sorted order.
    rng = random.Random(20261110)
    out = []
    for _ in range(10000):
        out.append(al.cross({n: st.mean(rng.choice(g[n]) for _ in g[n]) for n in ns})[0])
    out.sort()
    print("  curve_report.py's seed and convention: [%.4f, %.4f]" % (out[250], out[9749]))

    va, vb = s[na] ** 2 / len(g[na]), s[nb] ** 2 / len(g[nb])
    ua, ub = m[na], m[nb]
    d = ub - ua
    se = math.sqrt(((T - ub) / d ** 2) ** 2 * va + ((T - ua) / d ** 2) ** 2 * vb)
    dfw = len(g[na]) + len(g[nb]) - 2
    print("DELTA: SE %.4f; width z %.4f; t(%d) %.4f; t(5) %.4f" % (
        se, 2 * 1.96 * se, dfw, 2 * al.t_ppf(0.975, dfw) * se, 2 * al.t_ppf(0.975, 5) * se))
    sp2 = ((len(g[na]) - 1) * s[na] ** 2 + (len(g[nb]) - 1) * s[nb] ** 2) / dfw
    for lab, tq, VA, VB in (("Fieller z", 1.96, va, vb), ("Fieller t10, separate sds", al.t_ppf(0.975, dfw), va, vb),
                            ("Fieller t10, pooled sd", al.t_ppf(0.975, dfw), sp2 / len(g[na]), sp2 / len(g[nb]))):
        N, D = T - ua, d
        A_, B_, C_ = D * D - tq * tq * (VA + VB), -2 * (N * D - tq * tq * VA), N * N - tq * tq * VA
        r1 = (-B_ - math.sqrt(B_ * B_ - 4 * A_ * C_)) / (2 * A_)
        r2 = (-B_ + math.sqrt(B_ * B_ - 4 * A_ * C_)) / (2 * A_)
        print("%s: [%.4f, %.4f] width %.4f" % (lab, na + r1, na + r2, r2 - r1))

    print("\nSMALL-SAMPLE-CORRECTED BOOTSTRAPS (residuals scaled):")
    for lab, c in (("plain", 1.0), ("sqrt(6/5)", math.sqrt(6 / 5)), ("sqrt(6/5)*t10/z", math.sqrt(6 / 5) * al.t_ppf(0.975, 10) / 1.96),
                   ("sqrt(6/5)*t5/z", math.sqrt(6 / 5) * al.t_ppf(0.975, 5) / 1.96)):
        lo, hi = al.pct(al.bootstrap(g, sfn, random.Random(7), 40000, scale=c))
        print("  %-18s [%.4f, %.4f] width %.4f" % (lab, lo, hi, hi - lo))
    lo, hi = al.pct(al.bootstrap(g, al.pchip_cross, random.Random(11), 10000))
    print("  PCHIP bootstrap    [%.4f, %.4f] width %.4f (PCHIP point %.4f)" % (lo, hi, hi - lo, al.pchip_cross(m)))

    print("\nLEAVE ONE OUT:")
    for n in (4, 5):
        for i, x in enumerate(g[n]):
            mm = dict(m)
            mm[n] = st.mean(g[n][:i] + g[n][i + 1:])
            print("  drop n=%d run %d (%.4f): S* %.4f" % (n, i, x, al.cross(mm)[0]))
    print("\nINTERPOLANTS: linear %.4f; quadratic 3-4-5 %.4f; 4-5-6 %.4f; cubic 3-6 %.4f; PCHIP %.4f" % (
        S, lagrange_cross(m, (3, 4, 5)), lagrange_cross(m, (4, 5, 6)), lagrange_cross(m, (3, 4, 5, 6)), al.pchip_cross(m)))
    best = None
    for n0 in [x / 100 for x in range(0, 300, 2)]:
        for c in [x / 100 for x in range(20, 400, 2)]:
            sse = sum(len(g[n]) * (m[n] - 0.25 * (1 - math.exp(-(n - n0) / c))) ** 2 for n in ns)
            if best is None or sse < best[0]:
                best = (sse, n0, c)
    _, n0, c = best
    print("  saturating fit U = 0.25(1 - exp(-(n - %.2f)/%.2f)): crossing %.4f" % (n0, c, n0 - c * math.log(1 - T / 0.25)))


if __name__ == "__main__":
    main()
