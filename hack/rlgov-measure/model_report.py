"""The capacity model's acceptance block, from the raw logs: every run, each
arm, then the bars of results/<prefix>-settings.txt.

Usage: model_report.py <prefix>

Reads results/<prefix>-{off,step,model}-*.log, written by model_block.sh.
Figures are from 40s on, after the startup ramp.
"""
import glob, os, re, statistics, sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
PREFIX = sys.argv[1]
SETTING_MS, WAIT_S, TARGET = 20.0, 0.4, 0.9


def nums(rows, key):
    out = []
    for r in rows:
        try:
            out.append(float(r[key]))
        except (KeyError, ValueError):
            pass
    return out


def mean(xs):
    return statistics.mean(xs) if xs else None


def run(path):
    t = open(path).read()
    if "# DISCARDED setup failed" in t:
        return None
    rows = [dict(re.findall(r"(\S+?)=(\S+)", l)) for l in t.splitlines() if l.startswith("STAT ")]
    body = [r for r in rows if float(r["t"]) >= 40]
    loc = nums(body, "local")
    burn, wall = nums(body, "burn_cpu_ms/call"), nums(body, "burn_wall_ms/call")
    return dict(
        name=os.path.basename(path)[:-4],
        limit=float(re.search(r"^# run=.* cpu=(\d+)m ", t, re.M).group(1)), seconds=len(body),
        restarts=int(re.search(r"# restarts=(\d*)", t).group(1) or 0),
        liveness=sum(int(x) for x in re.findall(r"^Killing\s+(\d+)\s+.*liveness", t, re.M)),
        unhealthy=sum(int(x) for x in re.findall(r"^Unhealthy\s+(\d+)", t, re.M)),
        cpu=mean(nums(body, "cpu_ms/s")) or 0, some=mean(nums(body, "cpu_some")) or 0, obs=mean(nums(body, "obs/s")) or 0,
        local=mean(loc), minlocal=min(loc) if loc else None, sdlocal=statistics.pstdev(loc) if loc else None,
        model=mean(nums(body, "model")), inflight=statistics.median(nums(body, "inflight")) if body else None,
        burn=statistics.median(burn) if burn else None, wall=statistics.median(wall) if wall else None,
        cuts=len(re.findall(r"^LOG Controller saturated", t, re.M)),
        discarded=re.findall(r"^# DISCARDED (.*)$", t, re.M),
    )


def fmt(v, spec=".1f"):
    return "-" if v is None else format(v, spec)


def med(runs, key):
    xs = [r[key] for r in runs if r[key] is not None]
    return statistics.median(xs) if xs else None


arms = {}
for arm in ("off", "step", "model"):
    runs = [r for r in (run(p) for p in sorted(glob.glob(f"{R}/{PREFIX}-{arm}-*.log"))) if r]
    runs.sort(key=lambda r: int(r["name"].rsplit("-", 1)[1]))
    arms[arm] = runs
    print(f"== {arm}")
    for r in runs:
        flags = []
        if r["burn"] is None or not SETTING_MS <= r["burn"] <= SETTING_MS + 0.5:
            flags.append(f"burn per call {r['burn']} not {SETTING_MS}-{SETTING_MS + 0.5}ms")
        if arm == "off" and r["cpu"] < 0.9 * r["limit"]:
            flags.append(f"off not saturated: cpu {r['cpu']:.0f}ms/s")
        print(f"{r['name']:12} cpu={r['cpu']:.0f}ms/s ({100 * r['cpu'] / r['limit']:.0f}% of {r['limit']:.0f}) pressure={r['some']:.1f} obs/s={r['obs']:.2f} "
              f"local={fmt(r['local'])} min={fmt(r['minlocal'], '.0f')} sd={fmt(r['sdlocal'])} M={fmt(r['model'])} cuts={r['cuts']} inflight_med={fmt(r['inflight'], '.0f')} "
              f"burn_cpu={fmt(r['burn'])} burn_wall={fmt(r['wall'], '.0f')}ms restarts={r['restarts']} liveness_kills={r['liveness']} failed_probes={r['unhealthy']} "
              f"seconds={r['seconds']} discarded={len(r['discarded'])}" + (f" FLAGGED: {'; '.join(flags)}" if flags else ""))
    if runs:
        print(f"-- {len(runs)} runs, medians: cpu {statistics.median(100 * r['cpu'] / r['limit'] for r in runs):.1f}% of the limit, pressure {med(runs, 'some'):.1f}, "
              f"obs/s {med(runs, 'obs'):.2f}, local {fmt(med(runs, 'local'))} (sd {fmt(med(runs, 'sdlocal'))}), burn_wall {fmt(med(runs, 'wall'), '.0f')}ms; "
              f"cuts {sum(r['cuts'] for r in runs)}, restarts {sum(r['restarts'] for r in runs)}, liveness kills {sum(r['liveness'] for r in runs)}, "
              f"failed probes {sum(r['unhealthy'] for r in runs)}, discarded {sum(bool(r['discarded']) for r in runs)}")

off, step, model = arms["off"], arms["step"], arms["model"]
if off and step and model:
    limit = model[0]["limit"] / 1000
    c = statistics.median(r["cpu"] / r["obs"] for r in off) / 1000  # CPU seconds per Observe, ungoverned
    knee = TARGET * limit / c * (c + WAIT_S)
    local = med(model, "local")
    cpu = med(model, "cpu") / (limit * 1000)
    thr = med(model, "obs") / med(step, "obs")
    wall = med(model, "wall") / med(step, "wall")
    restarts = sum(r["restarts"] for r in model)
    print(f"== the bars, model arm against the step arm, {limit * 1000:.0f}m; knee {knee:.1f} slots (off arm: {c * 1000:.1f}ms of CPU per Observe)")
    for label, ok in ((f"wall time {med(model, 'wall'):.0f}ms, {100 * (1 - wall):.1f}% lower than the step arm's {med(step, 'wall'):.0f}ms (at least 20% lower)", wall <= 0.8),
                      (f"throughput {100 * thr:.1f}% of the step arm's (at least 95)", thr >= 0.95),
                      (f"CPU used {100 * cpu:.1f}% of the limit (at least 85)", cpu >= 0.85),
                      (f"local {local:.1f}, within 20% of the knee ({0.8 * knee:.1f} to {1.2 * knee:.1f})", 0.8 * knee <= local <= 1.2 * knee),
                      (f"restarts {restarts} (0)", restarts == 0)):
        print(f"{'MET    ' if ok else 'NOT MET'} {label}")
