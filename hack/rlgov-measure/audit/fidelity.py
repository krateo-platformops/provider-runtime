#!/usr/bin/env python3
"""The engine's committed drain curve against GKE (plan §4.5's review).

Per n = 3-8: engine mean (t on 9 df), GKE mean, the difference's z (per-n
GKE sd) and its 90% Welch interval with GKE's pooled sd (n = 3-5 and 6-7),
and how small the difference must be for the equivalence test at
delta = rbar*rho*L to pass, at the engine's seed count and at --k. Then the
variance-ratio F-tests, and the engine's mean-curve S* with its seed
bootstrap, small-sample-corrected widths and jackknife SE, and the width at
more seeds."""
import argparse
import math
import random
import statistics as st

import auditlib as al

DELTA = al.RBAR * al.TARGET  # 0.01125 cores


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    ap.add_argument("--engine", default=al.ENGINE)
    ap.add_argument("--k", type=int, default=40, help="engine seeds for the power line (default 40)")
    a = ap.parse_args()
    g = al.by_n(al.load_runs(a.results))
    gm = {n: st.mean(v) for n, v in g.items()}
    gs = {n: (st.stdev(v) if len(v) > 1 else 0.0) for n, v in g.items()}
    curves = al.engine_curves(a.engine)
    K = len(curves)
    em = {n: st.mean(c[n - 1] for c in curves.values()) for n in range(1, 11)}
    es = {n: st.stdev(c[n - 1] for c in curves.values()) for n in range(1, 11)}

    def pooled(ns):
        df = sum(len(g[n]) - 1 for n in ns)
        return math.sqrt(sum((len(g[n]) - 1) * gs[n] ** 2 for n in ns) / df), df

    p35, p67 = pooled((3, 4, 5)), pooled((6, 7))
    print("GKE pooled sd: n=3-5 %.4f on %d df; n=6-7 %.4f on %d df; delta %.5f cores" % (p35[0], p35[1], p67[0], p67[1], DELTA))
    print(" n  engine ±95%%(t9)    GKE (k)     diff     z     90%% Welch            df    passes only if |diff| < (K=%d)  (K=%d)" % (K, a.k))
    for n in range(3, 9):
        k = len(g[n])
        half_e = al.t_ppf(0.975, K - 1) * es[n] / math.sqrt(K)
        d = em[n] - gm[n]
        z = d / math.sqrt(es[n] ** 2 / K + gs[n] ** 2 / k) if gs[n] > 0 else float("nan")
        if n <= 7:
            sp, dfg = p35 if n <= 5 else p67

            def welch(kk):
                ve, vg = es[n] ** 2 / kk, sp ** 2 / k
                df = (ve + vg) ** 2 / (ve ** 2 / (kk - 1) + vg ** 2 / dfg)
                return al.t_ppf(0.95, df) * math.sqrt(ve + vg), df

            h, df = welch(K)
            hk, _ = welch(a.k)
            print(" %d  %.4f ± %.4f   %.4f (%d)  %+.4f  %+.1f  [%+.4f, %+.4f]  %5.1f   %.4f                     %.4f" % (
                n, em[n], half_e, gm[n], k, d, z, d - h, d + h, df, DELTA - h, DELTA - hk))
        else:
            print(" %d  %.4f ± %.4f   %.4f (%d)  %+.4f  (GKE sd 0: reported)" % (n, em[n], half_e, gm[n], k, d))
    print("\nvariance ratio, engine/GKE (F, one-sided p):")
    for ns in ((4,), (5,), (4, 5)):
        ve = sum((K - 1) * es[n] ** 2 for n in ns) / (len(ns) * (K - 1))
        vg = sum((len(g[n]) - 1) * gs[n] ** 2 for n in ns) / sum(len(g[n]) - 1 for n in ns)
        d1, d2 = len(ns) * (K - 1), sum(len(g[n]) - 1 for n in ns)
        print("  n=%s: F %.2f on (%d, %d) df, p %.3f" % ("-".join(map(str, ns)), ve / vg, d1, d2, al.f_sf(ve / vg, d1, d2)))

    cl = list(curves.values())

    def sstar(mean_curve):
        return al.cross({i + 1: u for i, u in enumerate(mean_curve)})[0]

    def boot(scale, b=20000, seed=1):
        mu = [st.mean(c[i] for c in cl) for i in range(10)]
        res = [[mu[i] + scale * (c[i] - mu[i]) for i in range(10)] for c in cl]
        rng = random.Random(seed)
        out = sorted(sstar([sum(p[i] for p in pick) / K for i in range(10)]) for pick in
                     ([rng.choice(res) for _ in range(K)] for _ in range(b)))
        return al.pct(out)

    S = sstar([em[n] for n in range(1, 11)])
    print("\nengine mean-curve S* %.4f; 0.15*S* %.4f" % (S, 0.15 * S))
    for lab, sc in (("seed bootstrap", 1.0), ("x sqrt(K/(K-1))", math.sqrt(K / (K - 1))),
                    ("x sqrt(K/(K-1)) t9/z", math.sqrt(K / (K - 1)) * al.t_ppf(0.975, K - 1) / 1.96)):
        lo, hi = boot(sc)
        print("  %-22s [%.3f, %.3f] width %.3f" % (lab, lo, hi, hi - lo))
    jk = [sstar([st.mean(c[i] for j, c in enumerate(cl) if j != x) for i in range(10)]) for x in range(K)]
    mj = st.mean(jk)
    se = math.sqrt((K - 1) / K * sum((v - mj) ** 2 for v in jk))
    print("  jackknife SE %.3f; normal width at K seeds: %s" % (
        se, " ".join("K=%d %.2f" % (kk, 2 * 1.96 * se * math.sqrt(K / kk)) for kk in (10, 20, 30, 40))))


if __name__ == "__main__":
    main()
