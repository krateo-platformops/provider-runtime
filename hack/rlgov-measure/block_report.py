"""The confirmation block, from the raw logs. Every run, then each arm against the
bar. A run counts as clean with 0 restarts and at most 0 terminations recorded.
Throughput is Observes per second the controller was up.
"""
import glob, os, re, statistics

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def run(path):
    t = open(path).read()
    rows = [dict(re.findall(r"(\S+?)=(\S+)", l)) for l in t.splitlines() if l.startswith("STAT ")]
    terms = {f: r for r, f in re.findall(r"^# term \d+ (\S+) \S+ (\S+)$", t, re.M)}
    kills = sum(int(x) for x in re.findall(r"^Killing\s+(\d+)\s+.*liveness", t, re.M))
    obs = sum(float(r["obs/s"]) for r in rows)
    return dict(
        name=os.path.basename(path)[:-4],
        restarts=int(re.search(r"# restarts=(\d*)", t).group(1) or 0),
        terms=len(terms),
        oom=sum(r == "OOMKilled" for r in terms.values()),
        other=[r for r in terms.values() if r != "OOMKilled"],
        liveness=kills,
        unhealthy=sum(int(x) for x in re.findall(r"^Unhealthy\s+(\d+)", t, re.M)),
        thr=obs / len(rows) if rows else 0,
        peak=max((float(r["ws_mib"]) for r in rows), default=0),
        load=statistics.mean(float(r["node_load"]) for r in rows) if rows else 0,
        maxload=max((float(r["node_load"]) for r in rows), default=0),
        discarded=re.findall(r"^# DISCARDED (.*)$", t, re.M),
        rollout=re.search(r"# rollout=(\S+)", t).group(1),
        rows=len(rows),
    )


def key(r):
    return int(r["name"].rsplit("-", 1)[1])


for mode in ("ramp", "cold"):
    print(f"== {mode}")
    arms = {}
    for arm in ("new", "ctrl"):
        runs = sorted((run(p) for p in glob.glob(f"{R}/b-{mode}-{arm}-*.log")), key=key)
        arms[arm] = runs
        for r in runs:
            print(f"{r['name']:18} restarts={r['restarts']} terms={r['terms']} oom={r['oom']} liveness={r['liveness']} unhealthy={r['unhealthy']} "
                  f"obs/s={r['thr']:.2f} peakWS={r['peak']:.0f} load={r['load']:.2f}/{r['maxload']:.2f} rollout={r['rollout']} other={r['other']} discarded={len(r['discarded'])}")
    for arm, runs in arms.items():
        clean = [r for r in runs if r["restarts"] == 0 and r["terms"] == 0]
        thr = [r["thr"] for r in runs]
        cthr = [r["thr"] for r in clean]
        print(f"-- {arm}: {len(runs)} runs, clean {len(clean)}, restarts {[r['restarts'] for r in runs]}, restarts total {sum(r['restarts'] for r in runs)}, "
              f"terminations {sum(r['terms'] for r in runs)}, oom {sum(r['oom'] for r in runs)}, liveness kills {sum(r['liveness'] for r in runs)}, unhealthy {sum(r['unhealthy'] for r in runs)}, "
              f"crash-looped (restarts>=3) {sum(r['restarts'] >= 3 for r in runs)}, discarded {sum(bool(r['discarded']) for r in runs)}")
        print(f"   obs/s median {statistics.median(thr):.2f} range {min(thr):.2f}-{max(thr):.2f}; clean runs median {statistics.median(cthr) if cthr else float('nan'):.2f} range {min(cthr) if cthr else 0:.2f}-{max(cthr) if cthr else 0:.2f}; "
              f"peakWS {min(r['peak'] for r in runs):.0f}-{max(r['peak'] for r in runs):.0f}; node load mean {statistics.mean(r['load'] for r in runs):.2f}, max {max(r['maxload'] for r in runs):.2f}")
