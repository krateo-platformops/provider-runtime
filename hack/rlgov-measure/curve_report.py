#!/usr/bin/env python3
"""The fixed-capacity plant curve's report (results/curve-settings.txt).

Usage:
  curve_report.py results/pc-*.log           the report and the decision
  curve_report.py --stage1 results/pc-*.log  stage 1's k* and what follows
  curve_report.py --check <log>              one run's validity; exit 1 if it fails
  curve_report.py --node <log>               the run's node name and CPU model

The unit is the run: per run, the mean U (cores) over the measured window
(t >= 300 s), its completions, the CPU the burns asked for and measured,
throttled periods per sample and the per-second stall share. The burn check's
method, A (idle moments, curve1's runs) or B (window), is printed per run. Per n: the mean
of the run means with a t interval on k - 1 degrees of freedom and the
between-run spread. S*: where the per-n means cross rho*L = 0.225 cores,
interpolated between the bracketing n; its CI by a bootstrap over runs
(resampled within each n, re-interpolated; a resample with no crossing sorts to
the end it lies at, -inf or +inf), with the delta method as a check. Then the
decision under the pre-registered rule.
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
BURN_TOL = 0.01      # the burn's measured CPU within 1% of what it asked for
# The burn check's method (results/curve-settings.txt). curve1 counted a
# burn's request when it started and its CPU when it ended, so its window
# difference isn't comparable: its runs are checked between the window's first
# and last idle-burn moments (method A), where no burn is unfinished, read as
# |requested - measured| under IDLE_MS, the smallest jittered request (0.1 x
# 3000 ms). Every later image counts both at the end, and its runs are checked
# over the whole measured window (method B).
METHOD_A_IMAGES = {"curve1"}
IDLE_MS = 300.0
KSTAR_MAX = 13       # stage 2 only if k* <= 13
K_STAGE1 = 4         # runs at each bracketing n in stage 1's schedule

# Two-sided 95% t quantiles by degrees of freedom.
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
       9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
       20: 2.086, 24: 2.064, 30: 2.042}


def t95(df):
    if df <= 0:
        return float("nan")
    if df in T95:
        return T95[df]
    return T95[max(k for k in T95 if k <= df)]


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
    """One run: its n, node, whether it was discarded, and its measured window."""
    run = {"path": path, "n": None, "node": None, "model": None, "discarded": [], "u": [],
           "obs": 0, "cpu_ms": 0.0, "thr_frac": [], "stall": [], "avg10": [], "secs": 0.0,
           "burn_first": None, "burn_last": None, "image": None,
           "idle_first": None, "idle_last": None, "idle_n": 0}
    prev = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("# run="):
                run["image"] = field(line, "image")
                m = re.search(r"WORKERS=(\d+)", line)
                if m:
                    run["n"] = int(m.group(1))
            elif line.startswith("# node="):
                run["node"] = line[len("# node="):].strip()
            elif line.startswith("# DISCARDED"):
                run["discarded"].append(line)
            elif line.startswith("LOG node"):
                run["model"] = field(line, "cpu_model")
            elif line.startswith("STAT "):
                t = num(field(line, "t"))
                cur = {k: num(field(line, k)) for k in ("nr_periods", "nr_throttled", "some_total_us")}
                burn = tuple(num(field(line, k)) for k in ("burns_total", "burn_cpu_total_ms", "burn_req_total_ms"))
                if t is not None and t >= WARMUP:
                    cpu = num(field(line, "cpu_ms/s"))
                    obs = num(field(line, "obs/s"))
                    if cpu is not None:
                        run["u"].append(cpu / 1000.0)
                        run["cpu_ms"] += cpu
                        run["secs"] += 1
                    if obs is not None:
                        run["obs"] += int(obs)
                    if None not in burn:
                        if run["burn_first"] is None:
                            run["burn_first"] = burn
                        run["burn_last"] = burn
                        if abs(burn[2] - burn[1]) < IDLE_MS:
                            if run["idle_first"] is None:
                                run["idle_first"] = burn
                            run["idle_last"] = burn
                            run["idle_n"] += 1
                    a10 = num(field(line, "cpu_some"))
                    if a10 is not None:
                        run["avg10"].append(a10)
                    if prev is not None and None not in cur.values() and None not in prev.values():
                        dp = cur["nr_periods"] - prev["nr_periods"]
                        dt = cur["nr_throttled"] - prev["nr_throttled"]
                        if dp > 0:
                            run["thr_frac"].append(dt / dp)
                        run["stall"].append(100.0 * (cur["some_total_us"] - prev["some_total_us"]) / 1e6)
                prev = cur
    return run


def burn_method(run):
    return "A" if run["image"] in METHOD_A_IMAGES else "B"


def burn_stats(run):
    """The burns over the check's window: calls, measured and requested CPU (ms).

    Method B: the measured window. Method A: from the window's first idle-burn
    moment to its last."""
    first, last = ("idle_first", "idle_last") if burn_method(run) == "A" else ("burn_first", "burn_last")
    if run[first] is None or run[last] is None:
        return 0, 0.0, 0.0
    return tuple(b - a for a, b in zip(run[first], run[last]))


def method_text(run):
    if burn_method(run) == "A":
        return "burn check A (idle moments, %d in the window)" % run["idle_n"]
    return "burn check B (window)"


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
    calls, cpu, req = burn_stats(run)
    if burn_method(run) == "A" and run["idle_n"] < 2:
        fails.append("burn check A needs two idle-burn moments in the window, found %d" % run["idle_n"])
    elif calls <= 0 or req <= 0:
        fails.append("no burns in the check's window")
    elif abs(cpu / req - 1) > BURN_TOL:
        fails.append("the burns measured %.2f%% off what they asked for, want within 1%%" % (100 * (cpu / req - 1)))
    return fails


def quantiles(xs, qs=(0.1, 0.5, 0.9)):
    if not xs:
        return ["na"] * len(qs)
    s = sorted(xs)
    return ["%.1f" % s[min(len(s) - 1, int(q * len(s)))] for q in qs]


def crossing(ns, means):
    """The first n where the curve crosses TARGET, interpolated, with its
    bracketing pair and the number of crossings; with no crossing, -inf when
    every mean is above TARGET and +inf when every mean is below it."""
    found, count = None, 0
    for a, b in zip(ns, ns[1:]):
        ua, ub = means[a], means[b]
        if (ua - TARGET) * (ub - TARGET) <= 0 and ua != ub:
            count += 1
            if found is None:
                found = (a + (TARGET - ua) / (ub - ua) * (b - a), a, b)
    if found is None:
        side = math.inf if all(means[n] < TARGET for n in ns) else -math.inf
        return (side, None, None), 0
    return found, count


def run_means(by_n):
    return {n: [statistics.mean(r["u"]) for r in rs] for n, rs in by_n.items()}


def per_n(by_n):
    rm = run_means(by_n)
    means = {n: statistics.mean(v) for n, v in rm.items()}
    sds = {n: (statistics.stdev(v) if len(v) > 1 else float("nan")) for n, v in rm.items()}
    return rm, means, sds


def bootstrap(ns, rm, seed=20261110, b=10000):
    rng = random.Random(seed)
    out = []
    for _ in range(b):
        bm = {n: statistics.mean(rng.choice(rm[n]) for _ in rm[n]) for n in ns}
        (s, _, _), _ = crossing(ns, bm)
        out.append(s)
    out.sort()
    return out[int(0.025 * b)], out[int(0.975 * b) - 1], sum(1 for x in out if math.isinf(x))


def load_runs(paths, verbose=True):
    runs = [parse(p) for p in paths]
    by_n = defaultdict(list)
    for r in runs:
        fails = check(r)
        if verbose:
            calls, cpu, req = burn_stats(r)
            cpc = r["cpu_ms"] / r["obs"] if r["obs"] else float("nan")
            edge = 100.0 * (r["n"] or 0) / r["obs"] if r["obs"] else float("nan")
            mu = statistics.mean(r["u"]) if r["u"] else float("nan")
            print("  %s n=%s node=%s model=%s U=%.4f completions=%d %s: %d calls, %.0f ms asked, measured %+.4f%%; "
                  "cgroup CPU per completion %.0f ms (edge allowance ±%.1f%%, not judged): %s" % (
                      r["path"], r["n"], r["node"], r["model"], mu, r["obs"], method_text(r), calls,
                      req / calls if calls else float("nan"), 100 * (cpu / req - 1) if req else float("nan"), cpc, edge,
                      "valid" if not fails else "INVALID: " + "; ".join(fails)))
        if not fails:
            by_n[r["n"]].append(r)
    return runs, by_n


def stage1(paths):
    print("stage 1 runs:")
    _, by_n = load_runs(paths)
    ns = sorted(by_n)
    rm, means, sds = per_n(by_n)
    (s, a, b), count = crossing(ns, means)
    if a is None:
        print("stage 1: the per-n means don't cross %.3f cores" % TARGET)
        print("stage 2: STOP, determinacy fails (futility at stage 1): no crossing")
        return 0
    beta = (means[b] - means[a]) / (b - a)
    ka, kb = len(rm[a]), len(rm[b])
    df = ka + kb - 2
    pooled = math.sqrt(((ka - 1) * sds[a] ** 2 + (kb - 1) * sds[b] ** 2) / df) if df > 0 else float("nan")
    t = t95(df)
    print("Ŝ* = %.2f slots between n=%d (k=%d) and n=%d (k=%d); %d crossing(s)" % (s, a, ka, b, kb, count))
    print("β̂ = %.4f cores per slot; pooled between-run sd ŝ = %.4f cores on %d df; t = %.3f" % (beta, pooled, df, t))
    if not beta > 0 or math.isnan(pooled):
        print("stage 2: STOP, determinacy fails (futility at stage 1): the slope between the bracketing n is not positive")
        return 0
    kstar = math.ceil((2 * t * 0.71 * pooled / (beta * 0.15 * s)) ** 2)
    print("k* = ⌈(2·t·0.71·ŝ/(β̂·0.15·Ŝ*))²⌉ = %d (approximate: β̂'s own error isn't covered)" % kstar)
    if kstar > KSTAR_MAX:
        du = beta * 0.3
        print("stage 2: STOP, determinacy fails (futility at stage 1): k* = %d > %d, β̂ = %.4f, ŝ = %.4f" % (kstar, KSTAR_MAX, beta, pooled))
        print("At that slope U changes by about %.4f cores (%.1f%% of L) across ±0.3 slots, far inside the r̄ = 5%% the design's "
              "guarantee is stated in: S* being undetermined is the substantive answer, since the guarantee is about U." % (du, 100 * du / LIMIT))
        return 0
    m = max(kstar - ka, kstar - kb)
    if m <= 0:
        print("stage 2: none needed (k* = %d); decide on stage 1's runs" % kstar)
        return 0
    print("stage 2: continue with n=%d and n=%d, %d more runs each" % (a, b, m))
    return 0


def report(paths):
    print("runs:")
    _, by_n = load_runs(paths)
    ns = sorted(by_n)
    if len(ns) < 2:
        print("S*: fewer than two n with valid runs")
        return 0
    rm, means, sds = per_n(by_n)
    print("\nper n (valid runs; the unit is the run):")
    for n in ns:
        k = len(rm[n])
        half = t95(k - 1) * sds[n] / math.sqrt(k) if k > 1 else float("nan")
        thr = [x for r in by_n[n] for x in r["thr_frac"]]
        st = [x for r in by_n[n] for x in r["stall"]]
        a10 = [x for r in by_n[n] for x in r["avg10"]]
        print("  n=%d k=%d mean U %.4f cores, 95%% CI ±%.4f, between-run sd %.4f" % (n, k, means[n], half, sds[n]))
        print("       throttled periods per sample: mean %.2f, all throttled in %.0f%% of samples" % (
            statistics.mean(thr) if thr else float("nan"), 100.0 * sum(1 for x in thr if x >= 0.999) / max(len(thr), 1)))
        print("       per-second stall share: p10/p50/p90 %s, over 50 in %.0f%% of samples; avg10 p50 %s" % (
            "/".join(quantiles(st)), 100.0 * sum(1 for x in st if x > 50) / max(len(st), 1), quantiles(a10, (0.5,))[0]))
    (s, a, b), count = crossing(ns, means)
    if a is None:
        print("\nS*: the per-n means don't cross %.3f cores" % TARGET)
        print("decision: determinacy fails; the long drain's band is reported, not judged.")
        return 0
    if count > 1:
        print("\nthe curve crosses %.3f cores %d times: non-monotone; S* is the first crossing" % (TARGET, count))
    print("\nS* = %.2f slots (between n=%d and n=%d)" % (s, a, b))
    if len(rm[a]) < 2 or len(rm[b]) < 2:
        print("a bracketing n has fewer than two valid runs: no CI")
        print("decision: determinacy fails; the long drain's band is reported, not judged.")
        return 0
    lo, hi, inf = bootstrap(ns, rm)
    width = hi - lo
    print("  bootstrap 95%% interval [%s, %s], width %s; %d of 10000 resamples with no crossing (sorted to ±inf)" % (
        "%.2f" % lo, "%.2f" % hi, "%.2f" % width, inf))
    ua, ub = means[a], means[b]
    d = ub - ua
    se = math.sqrt(((b - a) * (TARGET - ub) / d ** 2 * sds[a] / math.sqrt(len(rm[a]))) ** 2 +
                   ((b - a) * (TARGET - ua) / d ** 2 * sds[b] / math.sqrt(len(rm[b]))) ** 2)
    print("  delta method: SE %.2f, 95%% CI ±%.2f (width %.2f)" % (se, 1.96 * se, 2 * 1.96 * se))
    allowed = 3 * RBAR * s
    print("\ndecision (pre-registered): the interval's width against the band's half-width above, S*·3r̄ = %.2f" % allowed)
    if math.isfinite(lo) and math.isfinite(hi) and width < allowed:
        print("  The 95%% CI of S* is narrower than the band's half-width above: the band is judged with the measured S* = %.2f." % s)
    else:
        print("  The 95% CI of S* is not narrower than the band's half-width above: determinacy fails, and the long drain's band is reported, not judged.")
    return 0


def main(argv):
    if len(argv) >= 2 and argv[0] == "--check":
        run = parse(argv[1])
        fails = check(run)
        for f in fails:
            print("INVALID %s (%s): %s" % (argv[1], method_text(run), f))
        if not fails:
            calls, cpu, req = burn_stats(run)
            print("valid %s: n=%s U=%.4f completions=%d; %s: burns measured %+.4f%% of %.0f ms asked" % (
                argv[1], run["n"], statistics.mean(run["u"]), run["obs"], method_text(run), 100 * (cpu / req - 1), req))
        return 1 if fails else 0
    if len(argv) >= 2 and argv[0] == "--node":
        run = parse(argv[1])
        print("node=%s model=%s" % (run["node"], run["model"]))
        return 0
    if argv and argv[0] == "--stage1":
        return stage1(argv[1:])
    return report(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
