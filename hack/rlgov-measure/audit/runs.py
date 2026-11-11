#!/usr/bin/env python3
"""Audit §1-2: protocol conformance and every run's validity, from the logs.

Prints, per run: n, image, whether LOAD_SEED is the POSIX cksum of the run's
name, node and CPU model, restarts and DISCARDED lines, the measured window,
U, completions, and the burn check by method A (idle moments) and B (window).
Then the schedules regenerated from the settings' seeds."""
import argparse
import random

import auditlib as al


def cksum(data):
    """POSIX cksum(1): CRC-32 (poly 0x04C11DB7, unreflected) over the data and its length."""
    crc = 0

    def feed(byte):
        nonlocal crc
        crc ^= byte << 24
        for _ in range(8):
            crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF if crc & 0x80000000 else (crc << 1) & 0xFFFFFFFF

    for b in data:
        feed(b)
    n = len(data)
    while n:
        feed(n & 0xFF)
        n >>= 8
    return (~crc) & 0xFFFFFFFF


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=al.RESULTS)
    a = ap.parse_args()
    runs = al.load_runs(a.results)
    print("%-12s %s %-6s %-6s %-8s %-4s %-4s %5s %6s %4s  %-30s  %s" % (
        "run", "n", "image", "seedOK", "node", "rst", "disc", "win_s", "U", "obs", "burn check B (calls, dev)",
        "burn check A (idle in window/all, calls, dev)"))
    nodes = set()
    for name, r in sorted(runs.items(), key=lambda kv: kv[1]["started"]):
        b = r["B"]
        A = r["A"]
        nodes.add((r["node"], r["model"], r["gomaxprocs"], r["cpu_max"]))
        print("%-12s %d %-6s %-6s %-8s %-4s %-4d %5d %.4f %4d  %4d calls %+.4f%%            %s" % (
            name, r["n"], r["image"], str(int(r["seed"]) == cksum(name.encode())), (r["node"] or "")[-6:],
            r["restarts"], len(r["discarded"]), r["win_secs"], r["U"], r["obs"], b["calls"],
            100 * (b["cpu"] / b["req"] - 1),
            ("%d/%d, %d calls, %+.4f%%" % (A["n_idle"], A["n_idle_all"], A["calls"], 100 * (A["cpu"] / A["req"] - 1))) if A else "-"))
    print("\ndistinct (node, cpu model, GOMAXPROCS, cpu.max): %d" % len(nodes))
    for x in nodes:
        print("  ", x)
    print("U by mean of 1-s samples equals U by total/elapsed to 4 decimals in every run: %s" % all(
        round(r["U"], 4) == round(r["U_mean"], 4) for r in runs.values()))
    # The settings' schedules (results/curve-settings.txt, POINTS AND ORDER).
    r = random.Random(20261110)
    reps = []
    for x in ([4, 5, 6, 3, 7], [4, 5, 6, 8], [4, 5, 6, 3, 7], [4, 5, 6, 8]):
        x = list(x)
        r.shuffle(x)
        reps.append(x)
    print("\nstage 1 order from random.Random(20261110), shuffling [4,5,6,anchors] per repetition:", reps)
    r = random.Random(20261111)
    s2 = []
    for _ in range(2):
        x = [4, 5]
        r.shuffle(x)
        s2.append(x)
    print("stage 2 order from random.Random(20261111), m = 2:", s2)
    print("run order by start time:", [n for n, _ in sorted(((v["n"], v["started"]) for v in runs.values()), key=lambda p: p[1])])


if __name__ == "__main__":
    main()
