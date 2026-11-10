#!/usr/bin/env python3
"""The burn check's edge effect, per log: usage burnacct.py results/pc-*.log

curve1's main.go counts a burn's request when it starts and its measured CPU
when it ends, so differencing both totals over the measured window (t >= 300 s)
compares requests started in the window with burns finished in it. This prints
the window's figure next to the unfinished requests at its two ends, which are
the whole gap. Evidence for the counter fix (results/curve-settings.txt).
"""
import re, sys

def f(line, k):
    m = re.search(r'(?:^| )' + re.escape(k) + r'=("[^"]*"|\S+)', line)
    return m.group(1) if m else None

for p in sys.argv[1:]:
    rows = []
    for line in open(p):
        if line.startswith("STAT "):
            try:
                rows.append((float(f(line, "t")), int(f(line, "burns_total")),
                             float(f(line, "burn_cpu_total_ms")), float(f(line, "burn_req_total_ms"))))
            except (TypeError, ValueError):
                pass
    w = [r for r in rows if r[0] >= 300]
    a, b = w[0], w[-1]
    calls = b[1] - a[1]; cpu = b[2] - a[2]; req = b[3] - a[3]
    print(p)
    print("  window: calls=%d cpu=%.1f req=%.1f -> %.2f%%" % (calls, cpu, req, 100 * (cpu / req - 1)))
    print("  unfinished requested at window start %.1f ms, at end %.1f ms" % (a[3] - a[2], b[3] - b[2]))
    # Full run: at t=0 nothing is unfinished, so only the end's unfinished requests bias it.
    e = rows[-1]
    print("  full run: calls=%d cpu=%.1f req=%.1f unfinished at end=%.1f ms" % (e[1], e[2], e[3], e[3] - e[2]))
