"""Timeline of a run: one line every N seconds, marking container restarts.
Usage: tl.py <run> [every]"""
import os, re, sys
R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
n = sys.argv[1]
every = int(sys.argv[2]) if len(sys.argv) > 2 else 15
prev = -1
for l in open(f"{R}/{n}.log"):
    if l.startswith("LOG") and ("lowering" in l or "memory limit" in l):
        print("   ", l.strip()[:120])
    if not l.startswith("STAT "):
        continue
    r = dict(re.findall(r"(\S+?)=(\S+)", l))
    t = int(float(r["t"]))
    if t < prev:
        print(f"--- restart (previous container's last t={prev})")
    prev = t
    if t % every == 0:
        print(f"t={t:3} local={r.get('local'):>3} inflight={r.get('inflight'):>3} obs={r.get('obs/s'):>3} ws={r.get('ws_mib'):>4} live={r.get('live_mib'):>4} goal={r.get('goal_mib'):>4} mapped={r.get('mapped_mib'):>4} lim={r.get('golimit_mib')} cpu={r.get('cpu_some')} ms/s={r.get('cpu_ms/s')}")
