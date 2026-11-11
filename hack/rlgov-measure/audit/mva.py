#!/usr/bin/env python3
"""Mean-value analysis of the drain plant: the zero-parameter reference.

A closed network of one processor-sharing station (the 250m quota; service
demand c/L) and one delay station (the 28.5 s hold). BCMP: its stationary
means depend only on the mean demands, so MVA is exact for them. Prints MVA's
curve against GKE's window means and the engine's committed curve, S* at each
c, and drain0's derived values (S*0, X*0 = rho*L/c, D*0 = S*0/X*0 by Little,
tau = D*0/rbar, and X_cap,0 scaled from the frozen 0.162/s in
pkg/ratelimit/internal/costmodel/sim/frozen_test.go)."""
import argparse
import math
import statistics as st

import auditlib as al

FROZEN_XCAP0 = 0.1620  # frozen_test.go, drain0, at 1500 ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    ap.add_argument("--engine", default=al.ENGINE)
    ap.add_argument("--c", type=float, nargs="*", default=[3.0, 3.134, 3.157], help="drain plant CPU per reconcile, s")
    ap.add_argument("--c0", type=float, nargs="*", default=[1.5, 1.634, 1.657], help="drain0 CPU per reconcile, s")
    a = ap.parse_args()
    g = al.by_n(al.load_runs(a.results))
    gm = {n: st.mean(v) for n, v in g.items()}
    e = al.engine_table(a.engine)
    print("GKE window means:", {n: round(v, 4) for n, v in sorted(gm.items())})
    print("engine (committed, c = 3.000):", {n: v[0] for n, v in sorted(e.items())})
    for c in a.c:
        m = al.mva(c)
        print("\nc = %.3f s: MVA S* %.3f" % (c, al.mva_sstar(c)))
        for n in range(1, 11):
            u, x, r = m[n]
            print("  n=%2d U %.4f X %.4f R %5.1f s   MVA-GKE %s   engine-MVA %s" % (
                n, u, x, r, ("%+.4f" % (u - gm[n])) if n in gm else "   -   ",
                ("%+.4f" % (e[n][0] - u)) if n in e else "-"))
    print("\ndrain0:")
    for c in a.c0:
        s0 = al.mva_sstar(c)
        x0 = al.TARGET / c
        d0 = s0 / x0
        print("  c0 = %.3f s: S*0 %.3f, ceil %d; X*0 %.4f/s; D*0 %.2f s; tau = D*0/rbar %.0f s; "
              "X_cap,0 scaled %.4f/s (MVA's own L/c %.4f)" % (c, s0, math.ceil(s0), x0, d0, d0 / al.RBAR,
                                                           FROZEN_XCAP0 * 1.5 / c, al.LIMIT / c))


if __name__ == "__main__":
    main()
