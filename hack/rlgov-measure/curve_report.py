#!/usr/bin/env python3
"""The fixed-capacity plant curve's report (results/curve-settings.txt).

Usage:
  curve_report.py results/pc-*.log      the report
  curve_report.py --check <log>         one run's validity; exit 1 if it fails

The unit is the run: per run, the mean U (cores) over the measured window
(t >= 300 s), its completions, the CPU per completion, throttled periods per
sample and the per-second stall share. Per n: the mean of the run means with
a t interval on k - 1 degrees of freedom and the between-run spread. S*: where
the per-n means cross rho*L = 0.225 cores, interpolated between the bracketing
n; its CI by a bootstrap over runs (resampled within each n, re-interpolated)
and, as a check, the delta method on the bracketing means. Then the decision
under the pre-registered rule.
"""
import math
import random
import re
import statistics
import sys
from collections import defaultdict

WARMUP = 300.0
LIMIT, RHO, RBAR = 0.25, 0.9, 0.05
TARGET = RHO * LIMIT
CPU_PER_COMPLETION_MS = 3000.0

# Two-sided 95% t quantiles by degrees of freedom.
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
       9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
       20: 2.086, 30: 2.042}


def t95(df):
    if df in T95:
        return T95[df]
    return T95[max(k for k in T95 if k <= df)] if df > 0 else float("nan")


def field(line, key):
    m = re.search(r"(?:^| )" + re.escape(key) + r'=("[^"]*"|\S+)', line)
    if not m:
        return None
    return m.group(1).strip('"')


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse(path):
    """One run: its n, whether it was discarded, and its measured window."""
    run = {"path": path, "n": None, "discarded": [], "model": None, "u": [], "obs": 0,
           "cpu_ms": 0.0, "thr_frac": [], "stall": [], "avg10": [], "secs": 0.0, "burn": []}
    prev = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("# run="):
                m = re.search(r"WORKERS=(\d+)", line)
                if m:
                    run["n"] = int(m.group(1))
            elif line.startswith("# DISCARDED"):
                run["discarded"].append(line)
            elif line.startswith("LOG node"):
                run["model"] = field(line, "cpu_model")
            elif line.startswith("STAT "):
                t = num(field(line, "t"))
                cur = {k: num(field(line, k)) for k in ("nr_periods", "nr_throttled", "some_total_us")}
                if t is not None and t >= WARMUP:
                    cpu = num(field(line, "cpu_ms/s"))
                    obs = num(field(line, "obs/s"))
                    if cpu is not None:
                        run["u"].append(cpu / 1000.0)
                        run["cpu_ms"] += cpu
                        run["secs"] += 1
                    if obs is not None:
                        run["obs"] += int(obs)
                    bc = num(field(line, "burn_cpu_ms/call"))
                    if bc is not None:
                        run["burn"].append(bc)
                    a10 = num(field(line, "cpu_some"))
                    if a10 is not None:
                        run["avg10"].append(a10)
                    if prev is not None and None not in cur.values() and None not in prev.values():
                        dp = cur["nr_periods"] - prev["nr_periods"]
                        dt = cur["nr_throttled"] - prev["nr_throttled"]
                        if dp > 0:
                            run["thr_frac"].append(dt / dp)
                        ds = cur["some_total_us"] - prev["some_total_us"]
                        run["stall"].append(100.0 * ds / 1e6)
                prev = cur
    return run


def check(run):
    """The settings' validity checks. Returns a list of failures."""
    fails = []
    if run["discarded"]:
        fails.append("discarded by prun.sh: " + "; ".join(run["discarded"]))
    if run["n"] is None:
        fails.append("no WORKERS in the run line")
    if run["secs"] < 1700:
        fails.append("only %.0f s measured, want 1800" % run["secs"])
    if run["obs"] == 0:
        fails.append("no completions")
    else:
        cpc = run["cpu_ms"] / run["obs"]
        if abs(cpc - CPU_PER_COMPLETION_MS) > 0.05 * CPU_PER_COMPLETION_MS:
            fails.append("CPU per completion %.0f ms, want 3000 ±5%%" % cpc)
    return fails


def quantiles(xs, qs=(0.1, 0.5, 0.9)):
    if not xs:
        return ["na"] * len(qs)
    s = sorted(xs)
    return ["%.1f" % s[min(len(s) - 1, int(q * len(s)))] for q in qs]


def crossing(ns, means):
    """The first n where the curve crosses TARGET, interpolated, and the
    number of crossings."""
    found, count = None, 0
    for a, b in zip(ns, ns[1:]):
        ua, ub = means[a], means[b]
        if (ua - TARGET) * (ub - TARGET) <= 0 and ua != ub:
            count += 1
            if found is None:
                found = (a + (TARGET - ua) / (ub - ua) * (b - a), a, b)
    return found, count


def main(argv):
    if len(argv) >= 2 and argv[0] == "--check":
        run = parse(argv[1])
        fails = check(run)
        for f in fails:
            print("INVALID %s: %s" % (argv[1], f))
        if not fails:
            print("valid %s: n=%s U=%.4f completions=%d CPU/completion=%.0f ms" % (
                argv[1], run["n"], statistics.mean(run["u"]), run["obs"], run["cpu_ms"] / run["obs"]))
        return 1 if fails else 0

    runs = [parse(p) for p in argv]
    print("runs: %d" % len(runs))
    by_n = defaultdict(list)
    for r in runs:
        fails = check(r)
        state = "valid" if not fails else "INVALID: " + "; ".join(fails)
        cpc = r["cpu_ms"] / r["obs"] if r["obs"] else float("nan")
        mu = statistics.mean(r["u"]) if r["u"] else float("nan")
        burn = statistics.median(r["burn"]) if r["burn"] else float("nan")
        print("  %s n=%s U=%.4f completions=%d CPU/completion=%.0f ms (burn per call %.0f ms) model=%s: %s" % (
            r["path"], r["n"], mu, r["obs"], cpc, burn, r["model"], state))
        if not fails:
            by_n[r["n"]].append(r)

    print("\nper n (valid runs; the unit is the run):")
    ns = sorted(by_n)
    means, ses = {}, {}
    for n in ns:
        rm = [statistics.mean(r["u"]) for r in by_n[n]]
        k = len(rm)
        m = statistics.mean(rm)
        sd = statistics.stdev(rm) if k > 1 else float("nan")
        half = t95(k - 1) * sd / math.sqrt(k) if k > 1 else float("nan")
        means[n], ses[n] = m, (sd / math.sqrt(k) if k > 1 else float("nan"))
        thr = [x for r in by_n[n] for x in r["thr_frac"]]
        st = [x for r in by_n[n] for x in r["stall"]]
        a10 = [x for r in by_n[n] for x in r["avg10"]]
        print("  n=%d k=%d mean U %.4f cores, 95%% CI ±%.4f, between-run sd %.4f" % (n, k, m, half, sd))
        print("       throttled periods per sample: mean %.2f, all throttled in %.0f%% of samples" % (
            statistics.mean(thr) if thr else float("nan"), 100.0 * sum(1 for x in thr if x >= 0.999) / max(len(thr), 1)))
        print("       per-second stall share: p10/p50/p90 %s, over 50 in %.0f%% of samples; avg10 p50 %s" % (
            "/".join(quantiles(st)), 100.0 * sum(1 for x in st if x > 50) / max(len(st), 1), quantiles(a10, (0.5,))[0]))

    if len(ns) < 2:
        print("\nS*: fewer than two n with valid runs")
        return 0
    found, count = crossing(ns, means)
    if found is None:
        print("\nS*: the per-n means don't cross %.3f cores" % TARGET)
        print("decision: determinacy fails; the long drain's band is reported, not judged.")
        return 0
    s, a, b = found
    if len(by_n[a]) < 2 or len(by_n[b]) < 2:
        print("\nS* = %.2f slots (between n=%d and n=%d), but a bracketing n has fewer than two valid runs: no CI" % (s, a, b))
        print("decision: determinacy fails; the long drain's band is reported, not judged.")
        return 0
    if count > 1:
        print("\nthe curve crosses %.3f cores %d times: non-monotone; S* is the first crossing" % (TARGET, count))
    # Bootstrap over runs.
    rng = random.Random(20261110)
    boots, none = [], 0
    for _ in range(10000):
        bm = {}
        for n in ns:
            rm = [statistics.mean(r["u"]) for r in by_n[n]]
            bm[n] = statistics.mean(rng.choice(rm) for _ in rm)
        f, _ = crossing(ns, bm)
        if f is None:
            none += 1
        else:
            boots.append(f[0])
    boots.sort()
    lo = boots[int(0.025 * len(boots))] if boots else float("nan")
    hi = boots[int(0.975 * len(boots)) - 1] if boots else float("nan")
    # The delta method on the bracketing means.
    ua, ub = means[a], means[b]
    d = ub - ua
    ds_dua = (b - a) * (TARGET - ub) / d ** 2
    ds_dub = -(b - a) * (TARGET - ua) / d ** 2
    se = math.sqrt((ds_dua * ses[a]) ** 2 + (ds_dub * ses[b]) ** 2)
    print("\nS* = %.2f slots (between n=%d and n=%d)" % (s, a, b))
    print("  bootstrap 95%% CI [%.2f, %.2f], width %.2f; %d of 10000 resamples with no crossing" % (lo, hi, hi - lo, none))
    print("  delta method: SE %.2f, 95%% CI ±%.2f (width %.2f)" % (se, 1.96 * se, 2 * 1.96 * se))
    width, allowed = hi - lo, 3 * RBAR * s
    print("\ndecision (pre-registered): the CI's width %.2f against the band's half-width above, S*·3r̄ = %.2f" % (width, allowed))
    if none == 0 and width < allowed:
        print("  The 95%% CI of S* is narrower than the band's half-width above: the band is judged with the measured S* = %.2f." % s)
    else:
        print("  The 95% CI of S* is not narrower than the band's half-width above: determinacy fails, and the long drain's band is reported, not judged.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
