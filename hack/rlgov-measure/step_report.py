"""The CPU step block, from the raw logs: every run, each arm, then the bars.

Usage: step_report.py <prefix>

Reads results/<prefix>-{off,old,new}-*.log, written by step_block.sh. The
limit comes from each log's header (cpu=250m). Figures are from 40s on, after
the startup ramp. The bars are the ones in results/<prefix>-settings.txt,
applied to the new arm.
"""
import glob, os, re, statistics, sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
PREFIX = sys.argv[1]
SETTING_MS = 20.0


def nums(rows, key):
    out = []
    for r in rows:
        try:
            out.append(float(r[key]))
        except (KeyError, ValueError):
            pass
    return out


def run(path):
    t = open(path).read()
    if "# DISCARDED setup failed" in t:
        return None
    rows = [dict(re.findall(r"(\S+?)=(\S+)", l)) for l in t.splitlines() if l.startswith("STAT ")]
    body = [r for r in rows if float(r["t"]) >= 40]
    limit = float(re.search(r"^# run=.* cpu=(\d+)m ", t, re.M).group(1))
    loc, burn, wall, infl = nums(body, "local"), nums(body, "burn_cpu_ms/call"), nums(body, "burn_wall_ms/call"), nums(body, "inflight")
    return dict(
        name=os.path.basename(path)[:-4], limit=limit, seconds=len(body),
        restarts=int(re.search(r"# restarts=(\d*)", t).group(1) or 0),
        liveness=sum(int(x) for x in re.findall(r"^Killing\s+(\d+)\s+.*liveness", t, re.M)),
        unhealthy=sum(int(x) for x in re.findall(r"^Unhealthy\s+(\d+)", t, re.M)),
        cpu=statistics.mean(nums(body, "cpu_ms/s")) if body else 0,
        some=statistics.mean(nums(body, "cpu_some")) if body else 0,
        obs=statistics.mean(nums(body, "obs/s")) if body else 0,
        local=statistics.mean(loc) if loc else None, minlocal=min(loc) if loc else None,
        inflight=statistics.median(infl) if infl else None,
        burn=statistics.median(burn) if burn else None, wall=statistics.median(wall) if wall else None,
        cuts=len(re.findall(r"^LOG Controller saturated", t, re.M)),
        discarded=re.findall(r"^# DISCARDED (.*)$", t, re.M),
    )


def fmt(v, spec=".1f"):
    return "-" if v is None else format(v, spec)


arms = {}
for arm in ("off", "old", "new"):
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
        if arm == "off" and r["some"] <= 50:
            flags.append(f"off not saturated: pressure {r['some']:.1f}")
        print(f"{r['name']:12} cpu={r['cpu']:.0f}ms/s ({100 * r['cpu'] / r['limit']:.0f}% of {r['limit']:.0f}) pressure={r['some']:.1f} obs/s={r['obs']:.2f} "
              f"local={fmt(r['local'])} min={fmt(r['minlocal'], '.0f')} cuts={r['cuts']} inflight_med={fmt(r['inflight'], '.0f')} "
              f"burn_cpu={fmt(r['burn'])} burn_wall={fmt(r['wall'], '.0f')}ms restarts={r['restarts']} liveness_kills={r['liveness']} failed_probes={r['unhealthy']} "
              f"seconds={r['seconds']} discarded={len(r['discarded'])}" + (f" FLAGGED: {'; '.join(flags)}" if flags else ""))
    if runs:
        print(f"-- {len(runs)} runs: cpu median {statistics.median(100 * r['cpu'] / r['limit'] for r in runs):.1f}% of the limit, "
              f"pressure mean {statistics.mean(r['some'] for r in runs):.1f}, obs/s median {statistics.median(r['obs'] for r in runs):.2f}, "
              f"local mean {fmt(statistics.mean(r['local'] for r in runs) if runs[0]['local'] is not None else None)}, "
              f"burn_wall median {fmt(statistics.median(r['wall'] for r in runs if r['wall'] is not None) if any(r['wall'] is not None for r in runs) else None, '.0f')}ms, "
              f"cuts {sum(r['cuts'] for r in runs)}, restarts {sum(r['restarts'] for r in runs)}, liveness kills {sum(r['liveness'] for r in runs)}, "
              f"failed probes {sum(r['unhealthy'] for r in runs)}, discarded {sum(bool(r['discarded']) for r in runs)}")

off, new = arms["off"], arms["new"]
if off and new:
    limit = new[0]["limit"]
    cpu = statistics.median(100 * r["cpu"] / r["limit"] for r in new)
    some = statistics.mean(r["some"] for r in new)
    thr = 100 * statistics.median(r["obs"] for r in new) / statistics.median(r["obs"] for r in off)
    restarts = sum(r["restarts"] for r in new)
    print(f"== the bars, new arm, {limit:.0f}m" + ("" if limit == 250 else " (information only: the user set the bars at 250m)"))
    for label, ok in ((f"CPU used {cpu:.1f}% of the limit (at least 90)", cpu >= 90), (f"pressure {some:.1f} (40 to 60)", 40 <= some <= 60),
                      (f"throughput {thr:.1f}% of off (at least 90)", thr >= 90), (f"restarts {restarts} (0)", restarts == 0)):
        print(f"{'MET    ' if ok else 'NOT MET'} {label}")
