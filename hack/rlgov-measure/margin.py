"""How close each run came to the limit, and what it cost. Args: run-name globs.

margin: 256 MiB minus the peak working set, the room left at the worst sample.
cuts: capacity cuts logged, and how many went straight to 1.
first: seconds into the first container at the first cut, and in-flight then.
inf>10: seconds with more than 10 calls in flight.
gc%: share of the CPU the controller used that went to the GC, by the runtime's
own count, over the last container.
"""
import glob, os, re, sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
print(f"{'run':20} {'rst':>3} {'obs/up':>6} {'margin':>6} {'peakWS':>6} {'%>=80':>5} {'cuts':>4} {'to1':>3} {'first':>5} {'inflMax':>7} {'inf>10':>6} {'gc%':>5} {'ms/s':>5}")
paths = []
for p in sys.argv[1:]:
    paths += sorted(glob.glob(f"{R}/{p}.log"))
for p in paths:
    n = os.path.basename(p)[:-4]
    t = open(p).read()
    if "# DISCARDED" in t:
        continue
    rs = re.search(r"# restarts=(\d*)", t).group(1)
    lines = t.splitlines()
    rows, cuts, to1, first, cur = [], 0, 0, None, None
    for l in lines:
        if l.startswith("STAT "):
            cur = dict(re.findall(r"(\S+?)=(\S+)", l))
            rows.append(cur)
        elif l.startswith("LOG Controller saturated"):
            cuts += 1
            m = re.search(r"from (\d+) to (\d+)", l)
            to1 += m.group(2) == "1"
            if first is None and cur:
                first = (cur["t"], cur["inflight"])
    ws = [float(r["ws_mib"]) for r in rows]
    infl = [int(r["inflight"]) for r in rows]
    cpu = sum(float(r["cpu_ms/s"]) for r in rows[1:]) / 1000
    gc = float(rows[-1]["gc_cpu_s"]) if rows else 0
    last_up = float(rows[-1]["t"]) if rows else 1
    cpu_last = sum(float(r["cpu_ms/s"]) for r in rows[-int(last_up):] if r) / 1000 or 1
    obs = sum(float(r["obs/s"]) for r in rows)
    print(f"{n:20} {rs:>3} {obs / len(rows):6.2f} {256 - max(ws):6.0f} {max(ws):6.0f} {100 * sum(w >= 204.8 for w in ws) / len(ws):5.1f} {cuts:4} {to1:3} "
          f"{(first[0] if first else '-'):>5} {max(infl):7} {sum(i > 10 for i in infl):6} {100 * gc / cpu_last:5.1f} {sum(float(r['cpu_ms/s']) for r in rows) / len(rows):5.0f}")
