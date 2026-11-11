#!/usr/bin/env python3
"""The calibration quantity: what GKE's cgroup CPU per completion is made of.

Per run: U, X (completions per second), U/X, the burns' share (method-B
counters over the window) and the CPU outside the burn. Pooled: drawn burn
per call + window-edge offset + non-burn CPU = cgroup CPU per completion.
Then the per-reconcile-or-background regression (non-burn U on X), and the
cycle decomposition n/X = burn wall time + hold + other."""
import argparse
import math
import statistics as st

import auditlib as al

HOLD = 28.5


def ols(xs, ys):
    mx, my = st.mean(xs), st.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    a = my - b * mx
    s2 = sum((y - a - b * x) ** 2 for x, y in zip(xs, ys)) / (len(xs) - 2)
    return a, b, math.sqrt(s2 * (1 / len(xs) + mx * mx / sxx)), math.sqrt(s2 / sxx)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    a = ap.parse_args()
    runs = al.load_runs(a.results)
    rows = []
    print("%-12s n  U      X        U/X    non-burn U  non-burn/compl  burn/call" % "run")
    for k, r in sorted(runs.items(), key=lambda kv: (kv[1]["n"], kv[0])):
        X = r["obs"] / r["elapsed"]
        burnU = r["B"]["req"] / 1000 / r["elapsed"]
        rows.append(dict(k=k, n=r["n"], U=r["U"], X=X, c=r["U"] / X, nb=r["U"] - burnU, nbc=(r["U"] - burnU) / X,
                         req=r["B"]["req"] / r["B"]["calls"], img=r["image"]))
        q = rows[-1]
        print("%-12s %d  %.4f %.5f  %.3f  %.4f      %.3f           %.0f" % (k, q["n"], q["U"], q["X"], q["c"], q["nb"], q["nbc"], q["req"]))
    c = [q["c"] for q in rows]
    print("U/X over %d runs: mean %.4f sd %.4f se %.4f" % (len(c), st.mean(c), st.stdev(c), st.stdev(c) / math.sqrt(len(c))))
    calls = sum(r["B"]["calls"] for r in runs.values())
    burn = sum(r["B"]["req"] for r in runs.values())
    cpu = sum(r["cpu_ms"] for r in runs.values())
    obs = sum(r["obs"] for r in runs.values())
    print("pooled: cgroup CPU per completion %.1f ms = drawn burn per call %.1f (expected 3000, se %.1f from the jitter)"
          " + window-edge offset %.1f (burns/completions %.4f) + non-burn %.1f" % (
              cpu / obs, burn / calls, 300 / math.sqrt(calls), burn / obs - burn / calls, calls / obs, (cpu - burn) / obs))
    nb = [(r["cpu_ms"] - r["B"]["req"]) / r["obs"] for r in runs.values()]
    print("per-run non-burn per completion: mean %.1f ms, se %.1f" % (st.mean(nb), st.stdev(nb) / math.sqrt(len(nb))))
    print("\nper n: X, U/X, non-burn per completion")
    for n in sorted({q["n"] for q in rows}):
        qs = [q for q in rows if q["n"] == n]
        print("  n=%d X %.5f U/X %.3f non-burn/compl %.3f" % (n, st.mean(q["X"] for q in qs), st.mean(q["c"] for q in qs), st.mean(q["nbc"] for q in qs)))
    a0, b0, sa, sb = ols([q["X"] for q in rows], [q["nb"] for q in rows])
    print("\nnon-burn U = %.4f (se %.4f) + %.3f (se %.3f) * X: intercept 95%% [%.3f, %.3f] cores (a background), slope in s per completion" % (
        a0, sa, b0, sb, a0 - al.t_ppf(0.975, len(rows) - 2) * sa, a0 + al.t_ppf(0.975, len(rows) - 2) * sa))
    print("\ncycle: n/X against burn wall time + %.1f s hold (other = n/X - burn wall - hold)" % HOLD)
    for n in sorted({r["n"] for r in runs.values()}):
        rs = [r for r in runs.values() if r["n"] == n]
        cyc = [n / (r["obs"] / r["elapsed"]) for r in rs]
        bw = [r["burn_wall_s"] for r in rs]
        print("  n=%d cycle %.2f s, burn wall %.2f s, other %+.2f s (runs: %s)" % (
            n, st.mean(cyc), st.mean(bw), st.mean(c_ - b_ - HOLD for c_, b_ in zip(cyc, bw)),
            " ".join("%+.2f" % (c_ - b_ - HOLD) for c_, b_ in zip(cyc, bw))))


if __name__ == "__main__":
    main()
