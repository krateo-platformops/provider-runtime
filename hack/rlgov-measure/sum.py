"""Per run: restarts split by cause, throughput, memory, local and node load.
Args: globs of run names under results/."""
import glob, os, re, statistics, sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def parse(path):
    t = open(path).read()
    head = re.search(r"# run=(\S+) mode=(\S+) image=(\S+).*env=(.*) seconds=(\d+)", t)
    rows = [dict(re.findall(r"(\S+?)=(\S+)", l)) for l in t.splitlines() if l.startswith("STAT ")]
    terms = re.findall(r"^# term (\d+) (\S+) (\S+) (\S+)$", t, re.M)
    # Split by what the kubelet reported for each termination. restartCount can
    # move before lastState does, so one termination can show under two counts:
    # its finish time identifies it.
    by_finish = {finished: (reason, code) for _, reason, code, finished in terms}
    oom = sum(1 for reason, _ in by_finish.values() if reason == "OOMKilled")
    other = [(reason, code) for reason, code in by_finish.values() if reason != "OOMKilled"]
    live = sum(int(x) for x in re.findall(r"^Killing\s+(\d+)\s+Container measure failed liveness", t, re.M))
    unhealthy = sum(int(x) for x in re.findall(r"^Unhealthy\s+(\d+)", t, re.M))
    discarded = re.findall(r"^# DISCARDED (.*)$", t, re.M)
    rs = re.search(r"# restarts=(\d*)", t)
    rs = int(rs.group(1)) if rs and rs.group(1) else -1
    secs = int(head.group(5)) if head else 0
    obs = sum(float(r.get("obs/s", 0)) for r in rows)
    return dict(name=os.path.basename(path)[:-4], discarded=discarded, restarts=rs, oom=oom, other=other, live=live, unhealthy=unhealthy,
                rows=rows, secs=secs, total_obs=obs, thr=obs / secs if secs else float("nan"),
                img=head.group(3) if head else "?", env=head.group(4) if head else "?", mode=head.group(2) if head else "?")


def main():
    pats = sys.argv[1:] or ["*"]
    paths = []
    for p in pats:
        paths += sorted(glob.glob(f"{R}/{p}.log"))
    print(f"{'run':24} {'rst':>3} {'oom':>3} {'live':>4} {'unh':>3} {'obs/s':>6} {'obs/up':>6} {'loc~':>5} {'peakWS':>6} {'peakLive':>8} {'gc/s':>5} {'cpuSome':>7} {'nodeLd':>6} other")
    for p in paths:
        r = parse(p)
        rows = r["rows"]
        if r["discarded"]:
            print(f"{r['name']:24} DISCARDED: {r['discarded'][0][:90]}")
            continue
        f = lambda k: [float(x[k]) for x in rows if x.get(k) not in (None, "na", "-", "off")]
        loc, ws, live, some, gc, nl = f("local"), f("ws_mib"), f("live_mib"), f("cpu_some"), f("gc_cycles"), f("node_load")
        up = len(rows)
        gcs = (max(gc) - min(gc)) / up if gc and up else float("nan")
        mean = lambda v: statistics.mean(v) if v else float("nan")
        print(f"{r['name']:24} {r['restarts']:3} {r['oom']:3} {r['live']:4} {r['unhealthy']:3} {r['thr']:6.2f} {(r['total_obs'] / up if up else 0):6.2f} "
              f"{mean(loc):5.1f} {(max(ws) if ws else 0):6.0f} {(max(live) if live else 0):8.0f} {gcs:5.1f} {mean(some):7.1f} {mean(nl):6.2f} {r['other']}")


main()
