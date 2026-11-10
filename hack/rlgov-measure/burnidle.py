#!/usr/bin/env python3
"""The burns' true accounting error, per log: usage burnidle.py results/pc-*.log

Where burn_req_total_ms - burn_cpu_total_ms is under 300 ms (the smallest
jittered request), no burn is unfinished, so the residual is the error over
every burn so far. It prints how many such moments the log has and the last
three. This is the idle-moment check (method A) of results/curve-settings.txt.
"""
import re, sys

def f(line, k):
    m = re.search(r'(?:^| )' + re.escape(k) + r'=("[^"]*"|\S+)', line)
    return m.group(1) if m else None

# Where req - cpu is near zero, no burn was unfinished: the residual is the
# burns' true accounting error over every burn so far.
for p in sys.argv[1:]:
    idle = []
    for line in open(p):
        if line.startswith("STAT "):
            try:
                t = float(f(line, "t")); n = int(f(line, "burns_total"))
                cpu = float(f(line, "burn_cpu_total_ms")); req = float(f(line, "burn_req_total_ms"))
            except (TypeError, ValueError):
                continue
            if abs(req - cpu) < 300:
                idle.append((t, n, req - cpu, req))
    print(p.split("/")[-1], "rows with no burn unfinished:", len(idle))
    for r in idle[-3:]:
        print("  t=%.0f burns=%d req-cpu=%.1f ms of %.1f ms (%.4f%%)" % (r[0], r[1], r[2], r[3], 100 * r[2] / r[3]))
