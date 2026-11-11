"""Shared helpers for the plant-curve audit (see README.md).

Standard library only: no numpy or scipy on the driver. Reads the committed run
logs in ../results and the engine's committed curves in the sim's testdata;
nothing here writes outside stdout.
"""
import glob
import math
import os
import re
import statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.normpath(os.path.join(HERE, "..", "results"))
ENGINE = os.path.normpath(os.path.join(
    HERE, "..", "..", "..", "pkg", "ratelimit", "internal", "costmodel", "sim", "testdata",
    "curve-engine-b2e5757.txt"))

WARMUP = 300.0
LIMIT, RHO, RBAR = 0.25, 0.9, 0.05
TARGET = RHO * LIMIT  # 0.225 cores


def kv(line):
    """key=value pairs of one log line; quoted values unquoted."""
    out = {}
    for m in re.finditer(r'(\S+?)=("[^"]*"|\S*)', line):
        out.setdefault(m.group(1), m.group(2).strip('"'))
    return out


def load_runs(results=RESULTS):
    """Every pc-*.log, parsed independently of curve_report.py.

    Per run: n, image, seed, the STAT series (t, cpu cores), the measured
    window's U (total CPU over elapsed time), completions, the burn counters
    over the window (method B) and between its first and last idle moments
    (method A), and the window's burn wall time."""
    runs = {}
    for p in sorted(glob.glob(os.path.join(results, "pc-*.log"))):
        name = os.path.basename(p)[:-4]
        lines = open(p, encoding="utf-8", errors="replace").read().splitlines()
        head = next(l for l in lines if l.startswith("# run="))
        hdr = kv(head[2:])
        n = int(re.search(r"WORKERS=(\d+)", head).group(1))
        seed = re.search(r"LOAD_SEED=(\d+)", head).group(1)
        node = next((l[7:].strip() for l in lines if l.startswith("# node=")), None)
        nodeline = next((kv(l) for l in lines if l.startswith("LOG node")), {})
        stats = [kv(l[5:]) for l in lines if l.startswith("STAT ")]
        series = [(float(d["t"]), float(d["cpu_ms/s"]) / 1000.0) for d in stats]
        win = [d for d in stats if float(d["t"]) >= WARMUP]
        before = [d for d in stats if float(d["t"]) < WARMUP]
        cpu = [float(d["cpu_ms/s"]) for d in win]
        t0 = float(before[-1]["t"]) if before else float(win[0]["t"]) - 1
        secs = float(win[-1]["t"]) - t0
        obs = sum(int(d["obs/s"]) for d in win)

        def burn(d):
            return (float(d["burns_total"]), float(d["burn_cpu_total_ms"]), float(d["burn_req_total_ms"]))

        b0, b1 = burn(win[0]), burn(win[-1])
        idle = [burn(d) for d in win if abs(burn(d)[2] - burn(d)[1]) < 300]
        idle_all = [burn(d) for d in stats if abs(burn(d)[2] - burn(d)[1]) < 300]
        # Burn wall time per call over the window, from the per-second averages.
        wall, wcalls, prev = 0.0, 0, None
        for d in stats:
            bt = int(d["burns_total"])
            if float(d["t"]) >= WARMUP and prev is not None and d["burn_wall_ms/call"] != "na" and bt > prev:
                wall += float(d["burn_wall_ms/call"]) * (bt - prev)
                wcalls += bt - prev
            prev = bt
        runs[name] = dict(
            name=name, n=n, image=hdr.get("image"), started=hdr.get("started"), seed=seed,
            node=node, model=nodeline.get("cpu_model"), gomaxprocs=nodeline.get("gomaxprocs"),
            cpu_max=nodeline.get("cpu_max"),
            discarded=[l for l in lines if l.startswith("# DISCARDED")],
            restarts=next((l.split("=", 1)[1] for l in lines if l.startswith("# restarts=")), None),
            series=series, win_secs=len(win), elapsed=secs, U_mean=st.mean(cpu) / 1000.0,
            U=sum(cpu) / 1000.0 / secs, obs=obs, cpu_ms=sum(cpu),
            B=dict(calls=b1[0] - b0[0], cpu=b1[1] - b0[1], req=b1[2] - b0[2]),
            A=(dict(n_idle=len(idle), n_idle_all=len(idle_all), calls=idle[-1][0] - idle[0][0],
                    cpu=idle[-1][1] - idle[0][1], req=idle[-1][2] - idle[0][2]) if len(idle) >= 2 else None),
            burn_wall_s=(wall / wcalls / 1000.0) if wcalls else None,
        )
    return runs


def by_n(runs, keep=lambda name: True, key="U"):
    out = {}
    for name, r in runs.items():
        if keep(name):
            out.setdefault(r["n"], []).append(r[key])
    return out


def cross(means, target=TARGET):
    """The first crossing of target, linear between whole n (curve_report.py's rule);
    +inf if every mean is below target, -inf if every mean is above."""
    ns = sorted(means)
    for a, b in zip(ns, ns[1:]):
        ua, ub = means[a], means[b]
        if (ua - target) * (ub - target) <= 0 and ua != ub:
            return a + (target - ua) / (ub - ua) * (b - a), a, b
    return (math.inf if all(means[n] < target for n in ns) else -math.inf), None, None


def pchip_cross(means, target=TARGET, step=0.01):
    """First crossing of a Fritsch-Carlson monotone cubic through the per-n means."""
    xs = sorted(means)
    ys = [means[x] for x in xs]
    dl = [(ys[i + 1] - ys[i]) / (xs[i + 1] - xs[i]) for i in range(len(xs) - 1)]
    dv = [0.0] * len(xs)
    for i in range(1, len(xs) - 1):
        if dl[i - 1] * dl[i] > 0:
            dv[i] = 2 / (1 / dl[i - 1] + 1 / dl[i])
    dv[0], dv[-1] = dl[0], dl[-1]

    def f(x):
        i = max(0, min(len(xs) - 2, int(math.floor(x - xs[0]))))
        h = xs[i + 1] - xs[i]
        t = (x - xs[i]) / h
        return ((2 * t ** 3 - 3 * t ** 2 + 1) * ys[i] + (t ** 3 - 2 * t ** 2 + t) * h * dv[i]
                + (-2 * t ** 3 + 3 * t ** 2) * ys[i + 1] + (t ** 3 - t ** 2) * h * dv[i + 1])

    g = xs[0]
    while g + step <= xs[-1] + 1e-9:
        lo, hi = g, g + step
        if (f(lo) - target) * (f(hi) - target) <= 0 and f(lo) != f(hi):
            for _ in range(50):
                mid = (lo + hi) / 2
                if (f(lo) - target) * (f(mid) - target) <= 0:
                    hi = mid
                else:
                    lo = mid
            return (lo + hi) / 2
        g += step
    return math.inf if all(y < target for y in ys) else -math.inf


def bootstrap(groups, fn, rng, b, scale=1.0):
    """Resample within each group (runs within n), apply fn to the dict of
    resampled means. scale > 1 inflates the residuals around each group mean
    (a small-sample variance correction). Returns the sorted values."""
    mu = {k: st.mean(v) for k, v in groups.items()}
    res = {k: [mu[k] + scale * (x - mu[k]) for x in v] for k, v in groups.items()}
    out = []
    for _ in range(b):
        out.append(fn({k: sum(rng.choice(v) for _ in v) / len(v) for k, v in res.items()}))
    out.sort()
    return out


def pct(sorted_vals, lo=0.025, hi=0.975):
    """curve_report.py's percentile convention: out[int(lo*B)], out[int(hi*B) - 1]."""
    b = len(sorted_vals)
    return sorted_vals[int(lo * b)], sorted_vals[int(hi * b) - 1]


def mva(c, think=28.5, limit=LIMIT, nmax=40):
    """Exact mean-value analysis of a closed network: one processor-sharing
    station of rate `limit` cores (service demand c/limit seconds) and one
    delay station (think time). Returns {n: (U cores, X per s, R s)}."""
    d = c / limit
    q, out = 0.0, {}
    for n in range(1, nmax + 1):
        r = d * (1 + q)
        x = n / (r + think)
        q = x * r
        out[n] = (x * c, x, r)
    return out


def mva_sstar(c, **kw):
    m = mva(c, **kw)
    return cross({n: v[0] for n, v in m.items()})[0]


def engine_curves(path=ENGINE):
    """The engine's per-seed curves under the GKE protocol, n = 1..10."""
    out = {}
    for l in open(path, encoding="utf-8"):
        m = re.match(r"gke-protocol drain seed (\d+) U: (.*?) crossings:", l)
        if m:
            out[int(m.group(1))] = [float(x) for x in m.group(2).split()]
    return out


def engine_table(path=ENGINE):
    """The engine's per-n summary lines: n -> (meanU, sd, X, c/compl)."""
    out = {}
    for l in open(path, encoding="utf-8"):
        m = re.match(r"gke-protocol drain n=\s*(\d+) meanU (\S+) sd (\S+) X (\S+) c/compl (\S+)", l)
        if m:
            out[int(m.group(1))] = tuple(float(m.group(i)) for i in range(2, 6))
    return out


# ---- distributions (standard library only) ----

def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1, a - 1
    c, d = 1.0, 1 - qab * x / qap
    d = 1 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1) < 1e-12:
            break
    return h


def betainc(a, b, x):
    """Regularized incomplete beta I_x(a, b)."""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lb = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    if x < (a + 1) / (a + b + 2):
        return math.exp(lb) * _betacf(a, b, x) / a
    return 1 - math.exp(lb) * _betacf(b, a, 1 - x) / b


def t_cdf(t, df):
    x = df / (df + t * t)
    p = 0.5 * betainc(df / 2, 0.5, x)
    return 1 - p if t > 0 else p


def t_ppf(p, df):
    lo, hi = -100.0, 100.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_cdf(mid, df) < p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def f_sf(f, d1, d2):
    """P(F > f) for F(d1, d2)."""
    return betainc(d2 / 2, d1 / 2, d2 / (d2 + d1 * f))


def chi2_cdf(x, k):
    """Regularized lower gamma P(k/2, x/2), by series."""
    a, x = k / 2, x / 2
    if x <= 0:
        return 0.0
    term = s = 1 / a
    for i in range(1, 500):
        term *= x / (a + i)
        s += term
        if term < s * 1e-15:
            break
    return s * math.exp(-x + a * math.log(x) - math.lgamma(a))


def chi2_ppf(p, k):
    lo, hi = 0.0, 1000.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if chi2_cdf(mid, k) < p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2
