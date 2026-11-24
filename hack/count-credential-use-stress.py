"""Counts what TestStress's go test -v outputs, named as arguments, show: the
gate of a long run (docs/design-credential-use-finalizer.md §14).

An iteration is green, excused, or red. It is excused only if every one of its
error lines is an audit's of the API server's own H1 (AUDIT H1-RECREATE or
AUDIT H1-LOST-A2); one error line of any other kind, an AUDIT or not, and it is
red. Then come the summaries' counts, added up.

The excused classes have ceilings, by sub-test (STATED): the rates of an
unmutated run on 4 CPUs, before H1's wait, which §14 states. They depend on the
machine and on that wait, so measure them again when either changes. A class is
over its ceiling if the lower end of its rate's interval is above the stated
rate, or, where none was seen, above that run's upper bound. The output says
how many iterations that takes; a run too short to have so many can't show it.

The intervals are exact 95% Poisson ones, on the count. A count of 0 has the
upper bound 3 over the iterations, which is one-sided.

It exits 1 on a red iteration, an error line outside every iteration, a class
over its ceiling, a data race, a panic, or a file without an iteration.
"""
import collections, math, re, sys

H1 = ("H1-RECREATE", "H1-LOST-A2")
# (seen, of so many iterations), by sub-test: 79c6868, 2026-11-24, a 4-CPU VM,
# unloaded. Every one seen was read by hand.
STATED = {
    "2_processes,_no_leader_election": {"H1-RECREATE": (5, 207), "H1-LOST-A2": (0, 207)},
}
SUMMARY = re.compile(r"\d+ processes, \S+, seed \d+: ")
NAMES = re.compile(r"^(\d+) (operations|namespace teardowns|passes)$")
ERROR = re.compile(r"\s+\S+\.go:\d+: (.*)$")
EXCUSED = re.compile("AUDIT (" + "|".join(H1) + "):")


def poisson(n):
    """The exact 95% interval of a Poisson mean, by bisection."""
    def cdf(k, m):
        term, s = math.exp(-m), 0.0
        for i in range(k + 1):
            s += term
            term *= m / (i + 1)
        return s

    def solve(f):
        lo, hi = 0.0, 10.0 * (n + 5)
        for _ in range(100):
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if f(mid) else (lo, mid)
        return lo
    if n == 0:
        return 0.0, 3.0
    return solve(lambda m: 1 - cdf(n - 1, m) < 0.025), solve(lambda m: cdf(n, m) > 0.025)


def rate(n, of):
    lo, hi = poisson(n)
    return f"{n} in {of}: {100 * n / of:.2f}% [{100 * lo / of:.2f}, {100 * hi / of:.2f}]"


def ceiling(subtest, cls):
    """The stated rate of cls in subtest, or None where none was measured."""
    seen, of = STATED.get(subtest, {}).get(cls, (0, 0))
    return None if of == 0 else max(seen, 3 if seen == 0 else 0) / of


def over(subtest, cls, n, of):
    """Whether n in of iterations is over cls's ceiling, and what to print."""
    c = ceiling(subtest, cls)
    if c is None:
        return False, "no ceiling: not measured for this sub-test"
    need = next((k for k in range(1, of + 1) if poisson(k)[0] / of > c), None)
    if need is None:
        return False, f"ceiling {100 * c:.2f}%: {of} iterations are too few to show it exceeded"
    return n >= need, f"ceiling {100 * c:.2f}%: exceeded from {need} on" + (": OVER" if n >= need else "")


def count(path):
    """Prints what path shows; returns whether it is bad."""
    text = open(path, errors="replace").read()
    states, classes = collections.Counter(), collections.defaultdict(collections.Counter)
    total, seen, took, reds, per = collections.Counter(), collections.Counter(), [], [], collections.Counter()
    errors = []
    for line in text.split("\n"):
        m = ERROR.match(line)
        if m and SUMMARY.match(m[1]):
            head, _, rest = m[1].split(": ", 1)[1].partition("; ")
            for part in head.split(", ") + rest.split("; "):
                n = NAMES.match(part)
                name, v = (n[2], n[1]) if n else (part.rpartition(" ")[0], part.rpartition(" ")[2])
                if v.isdigit():
                    total[name] += int(v)
                    seen[name] += int(v) > 0
        elif m:
            errors.append(m[1])
        end = re.match(r"\s+--- (PASS|FAIL): TestStress/(\S+) \(([0-9.]+)s\)", line)
        if not end:
            continue
        took.append(float(end[3]))
        per[end[2]] += 1
        classes[end[2]].update({c for e in errors for c in re.findall(r"^AUDIT ([A-Z0-9-]+):", e)})
        if end[1] == "PASS":
            states["green"] += 1
        elif errors and all(EXCUSED.match(e) for e in errors):
            states["excused"] += 1
        else:
            states["red"] += 1
            reds.append([e[:200] for e in errors if not EXCUSED.match(e)][:4])
        errors = []
    n = len(took)
    races, panics = text.count("DATA RACE"), len(re.findall(r"^panic:", text, re.M))
    bad = not n or states["red"] > 0 or bool(errors) or races > 0 or panics > 0
    print(f"{path}: {n} iterations, {sum(took) / max(1, n):.1f}s each (max {max(took, default=0):.1f}); "
          f"{states['green']} green, {states['excused']} excused, {states['red']} RED; data races {races}, panics {panics}")
    if errors:
        print(f"  RED, after the last iteration: {errors[:4]}")
    for sub, of in sorted(per.items()):
        print(f"  {sub}: {of} iterations")
        for c in H1:
            exceeded, what = over(sub, c, classes[sub][c], of)
            bad = bad or exceeded
            print(f"    {c}: {rate(classes[sub][c], of)} of iterations; {what}")
        others = {c: k for c, k in sorted(classes[sub].items()) if c not in H1}
        print(f"    other classes, by iterations: {others or 'none'}")
        if not states["red"] and of > 3:
            print(f"    no red iteration in {of}: a defect these audits see is in under {100 * 3 / of:.2f}% of iterations (95%), at envtest's timings")
    for e in reds:
        print("  RED:", e)
    for name in sorted(total):
        print(f"    {name}: {total[name]} in {seen[name]} iterations")
    return bad


if __name__ == "__main__":
    results = [count(p) for p in sys.argv[1:]]
    sys.exit(1 if any(results) or not results else 0)
