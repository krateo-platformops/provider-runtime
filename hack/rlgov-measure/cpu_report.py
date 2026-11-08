"""The CPU threshold block, from the raw logs: every run, then each arm.

Usage: cpu_report.py [prefix]   (default c2, the re-run; c is the first block)

Kills are reported by cause: each termination's reason from the kubelet, and the
"failed liveness" Killing events. Unhealthy events are failed probes, whether or
not they led to a kill. local, pressure and calls in flight are from 40s on,
after the startup ramp. Cuts are the "lowering local capacity" lines.

With the local gate in the queue (provider-runtime#40) a gated arm defers
nothing for local capacity, so engagement shows as cuts, local below the ceiling
and fewer calls in flight, not as deferrals. The first block's logs still carry
ramping and saturated deferrals; they are printed when present.

Validity, decided before the re-run's first run (results/cpu2-settings.txt):
each run's median burn_cpu_ms/call within 0.5ms above the setting, and the off
arm saturated: CPU at least 90% of the limit and pressure over 50. Calls in
flight are printed as a diagnostic only: the check of 12 or more was
mis-specified, counting calls inside Observe rather than reconciles, and was
dropped after the calibration run, before run 1. A run that fails a check is
flagged, not dropped.
"""
import glob, os, re, statistics, sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
PREFIX = sys.argv[1] if len(sys.argv) > 1 else "c2"
SETTING_MS, LIMIT_MS = 20.0, 250.0


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def measured(paths):
    """Drops the logs of runs whose setup failed, after naming them: prun.sh
    wrote them before anything was measured, so they have no data."""
    out = []
    for p in sorted(paths):
        if "# DISCARDED setup failed" in open(p).read():
            print(f"{os.path.basename(p)[:-4]}: setup failed, nothing measured")
        else:
            out.append(p)
    return out


def run(path):
    t = open(path).read()
    rows = [dict(re.findall(r"(\S+?)=(\S+)", l)) for l in t.splitlines() if l.startswith("STAT ")]
    terms = {f: r for r, f in re.findall(r"^# term \d+ (\S+) \S+ (\S+)$", t, re.M)}
    body = [r for r in rows if float(r["t"]) >= 40]
    loc = [num(r.get("local")) for r in body if num(r.get("local")) is not None]
    some = [num(r.get("cpu_some")) for r in body if num(r.get("cpu_some")) is not None]
    burn = [num(r.get("burn_cpu_ms/call")) for r in body if num(r.get("burn_cpu_ms/call")) is not None]
    wall = [num(r.get("burn_wall_ms/call")) for r in body if num(r.get("burn_wall_ms/call")) is not None]
    infl = [num(r.get("inflight")) for r in body if num(r.get("inflight")) is not None]
    defer = re.findall(r"deferrals=map\[([^\]]*)\]", t)
    last = dict(kv.split(":") for kv in defer[-1].split()) if defer and defer[-1] else {}
    return dict(
        name=os.path.basename(path)[:-4],
        restarts=int(re.search(r"# restarts=(\d*)", t).group(1) or 0),
        oom=sum(r == "OOMKilled" for r in terms.values()),
        other=[r for r in terms.values() if r != "OOMKilled"],
        liveness=sum(int(x) for x in re.findall(r"^Killing\s+(\d+)\s+.*liveness", t, re.M)),
        unhealthy=sum(int(x) for x in re.findall(r"^Unhealthy\s+(\d+)", t, re.M)),
        thr=sum(float(r["obs/s"]) for r in rows) / len(rows) if rows else 0,
        local=statistics.mean(loc) if loc else None,
        minlocal=min(loc) if loc else None,
        some=statistics.mean(some) if some else 0,
        inflight=statistics.median(infl) if infl else None,
        burn=statistics.median(burn) if burn else None,
        wall=statistics.median(wall) if wall else None,
        cuts=len(re.findall(r"^LOG Controller saturated", t, re.M)),
        deferrals={k: int(v) for k, v in last.items()},
        cpu=statistics.mean(float(r["cpu_ms/s"]) for r in body) if body else 0,
        load=statistics.mean(float(r["node_load"]) for r in rows) if rows else 0,
        discarded=re.findall(r"^# DISCARDED (.*)$", t, re.M),
    )


def flags(r, arm):
    out = []
    if r["burn"] is None or not SETTING_MS <= r["burn"] <= SETTING_MS + 0.5:
        out.append(f"burn per call {r['burn']} not {SETTING_MS}-{SETTING_MS + 0.5}ms")
    if arm == "off":
        if r["cpu"] < 0.9 * LIMIT_MS:
            out.append(f"off not saturated: cpu {r['cpu']:.0f}ms/s")
        if r["some"] <= 50:
            out.append(f"off not saturated: pressure {r['some']:.1f}")
    return out


for arm in ("off", "75", "50"):
    runs = sorted((run(p) for p in measured(glob.glob(f"{R}/{PREFIX}-{arm}-*.log"))), key=lambda r: int(r["name"].rsplit("-", 1)[1]))
    print(f"== {arm}")
    for r in runs:
        f = flags(r, arm) if PREFIX != "c" else []
        print(f"{r['name']:10} restarts={r['restarts']} liveness_kills={r['liveness']} oom={r['oom']} other={r['other']} failed_probes={r['unhealthy']} "
              f"obs/s={r['thr']:.2f} cpu_some={r['some']:.1f} local={r['local']} min={r['minlocal']} inflight_med={r['inflight']} cuts={r['cuts']} "
              f"burn_cpu_ms/call={r['burn']} burn_wall_ms/call={r['wall']} cpu_ms/s={r['cpu']:.0f} deferrals={r['deferrals']} node_load={r['load']:.2f} "
              f"discarded={len(r['discarded'])}" + (f" FLAGGED: {'; '.join(f)}" if f else ""))
    if runs:
        thr = [r["thr"] for r in runs]
        print(f"-- {len(runs)} runs: restarts {sum(r['restarts'] for r in runs)}, liveness kills {sum(r['liveness'] for r in runs)}, oom {sum(r['oom'] for r in runs)}, "
              f"failed probes {sum(r['unhealthy'] for r in runs)}, obs/s median {statistics.median(thr):.2f} range {min(thr):.2f}-{max(thr):.2f}, "
              f"cpu_some mean {statistics.mean(r['some'] for r in runs):.1f}, cuts {sum(r['cuts'] for r in runs)}, discarded {sum(bool(r['discarded']) for r in runs)}")
