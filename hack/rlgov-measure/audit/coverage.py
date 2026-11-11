#!/usr/bin/env python3
"""Audit §5: the percentile bootstrap's real coverage for this design, by simulation,
and the sampling spread of the S* estimate as the noise grows.

Truth: the observed per-n means (linear, or PCHIP for the target), normal run
noise with the observed sds, or with the pooled n = 4-5 sd at n = 3-5; the
design's k per n. Slow: nsim x B resamples (the audit used 1000 x 1000, a few
minutes each)."""
import argparse
import math
import random
import statistics as st

import auditlib as al


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    ap.add_argument("--truth", choices=("linear", "pchip"), default="linear")
    ap.add_argument("--sd", choices=("obs", "pooled45"), default="pooled45")
    ap.add_argument("--nsim", type=int, default=1000)
    ap.add_argument("--b", type=int, default=1000)
    ap.add_argument("--spread", action="store_true", help="only the S*-hat spread against an sd multiplier")
    a = ap.parse_args()
    g = al.by_n(al.load_runs(a.results))
    ns = sorted(g)
    K = {n: len(g[n]) for n in ns}
    m0 = {n: st.mean(g[n]) for n in ns}
    sd = {n: (st.stdev(g[n]) if len(g[n]) > 1 else 0.0) for n in ns}
    sp = math.sqrt((5 * sd[4] ** 2 + 5 * sd[5] ** 2) / 10)
    if a.sd == "pooled45" or a.spread:
        sd[3] = sd[4] = sd[5] = sp

    def sfn(mm):
        return al.cross(mm)[0]

    if a.spread:
        print("pooled sd at n=3-5: %.5f" % sp)
        rng = random.Random(99)
        for mult in (1.0, 1.2, 1.3, 1.4, 1.5, 2.0):
            e = sorted(sfn({n: st.mean([rng.gauss(m0[n], sd[n] * mult) for _ in range(K[n])]) for n in ns}) for _ in range(40000))
            print("  sd x%.1f: S*-hat 2.5-97.5%% [%.4f, %.4f] span %.4f; P(bracket 3-4) %.3f" % (
                mult, e[1000], e[38999], e[38999] - e[1000], sum(1 for x in e if x < 4) / 40000))
        lo, hi = al.chi2_ppf(0.025, 10), al.chi2_ppf(0.975, 10)
        print("95%% CI of the pooled sd (10 df): x%.2f to x%.2f; P(sigma > 1.45 s-hat) = %.3f" % (
            math.sqrt(10 / hi), math.sqrt(10 / lo), al.chi2_cdf(10 / 1.45 ** 2, 10)))
        return
    true_S = sfn(m0) if a.truth == "linear" else al.pchip_cross(m0)
    rng = random.Random(12345)
    cov = det = 0
    widths, ests = [], []
    for _ in range(a.nsim):
        data = {n: [rng.gauss(m0[n], sd[n]) for _ in range(K[n])] for n in ns}
        s_hat = sfn({n: st.mean(v) for n, v in data.items()})
        ests.append(s_hat)
        lo, hi = al.pct(al.bootstrap(data, sfn, rng, a.b))
        cov += lo <= true_S <= hi
        widths.append(hi - lo)
        det += math.isfinite(lo) and math.isfinite(hi) and hi - lo < 0.15 * s_hat
    widths.sort()
    print("truth=%s (S* %.4f) sd=%s nsim=%d B=%d: coverage %.3f; 'determined' %.3f; width median %.3f; S*-hat sd %.4f" % (
        a.truth, true_S, a.sd, a.nsim, a.b, cov / a.nsim, det / a.nsim, widths[a.nsim // 2],
        st.stdev([e for e in ests if math.isfinite(e)])))


if __name__ == "__main__":
    main()
