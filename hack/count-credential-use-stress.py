"""Counts what TestStress's go test -v outputs, named as arguments, show: the
gate of a long run (docs/design-credential-use-finalizer.md §14).

An iteration is green, excused, or red. It is excused only if every one of its
error lines is an audit's of the API server's own H1 (AUDIT H1-RECREATE or
AUDIT H1-LOST-A2); one error line of any other kind, an AUDIT or not, and it is
red. The excused iterations, and each of the two classes, are reported as rates
per iteration, with 95% Poisson intervals; a count of 0 has the upper bound 3
over the iterations. Then come the summaries' counts, added up.

It exits 1 on a red iteration, a data race, a panic, or a file without an
iteration.
"""
import collections, math, re, sys

H1 = ("H1-RECREATE", "H1-LOST-A2")
SUMMARY = re.compile(r"\d+ processes, \S+, seed \d+: ")
NAMES = re.compile(r"^(\d+) (operations|namespace teardowns|passes)$")


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


bad = False
for path in sys.argv[1:]:
    text = open(path, errors="replace").read()
    states, classes = collections.Counter(), collections.Counter()
    total, seen, took, reds = collections.Counter(), collections.Counter(), [], []
    errors = []
    for line in text.split("\n"):
        m = re.match(r"\s+\S+\.go:\d+: (.*)$", line)
        if m and SUMMARY.match(m[1]):
            head, _, rest = m[1].split(": ", 1)[1].partition("; ")
            for part in head.split(", ") + rest.split("; "):
                n = NAMES.match(part)
                name, v = (n[2], int(n[1])) if n else (part.rpartition(" ")[0], part.rpartition(" ")[2])
                if str(v).isdigit():
                    total[name] += int(v)
                    seen[name] += int(v) > 0
        elif m:
            errors.append(m[1])
        end = re.match(r"\s+--- (PASS|FAIL): TestStress/\S+ \(([0-9.]+)s\)", line)
        if not end:
            continue
        took.append(float(end[2]))
        found = {c for e in errors for c in re.findall(r"^AUDIT ([A-Z0-9-]+):", e)}
        classes.update(found)
        if end[1] == "PASS":
            states["green"] += 1
        elif errors and all(re.match(r"AUDIT (" + "|".join(H1) + "):", e) for e in errors):
            states["excused"] += 1
        else:
            states["red"] += 1
            reds.append([e[:200] for e in errors if not re.match(r"AUDIT (" + "|".join(H1) + "):", e)][:4])
        errors = []
    n = len(took)
    races, panics = text.count("DATA RACE"), len(re.findall(r"^panic:", text, re.M))
    print(f"{path}: {n} iterations, {sum(took) / max(1, n):.1f}s each (max {max(took, default=0):.1f}); "
          f"{states['green']} green, {states['excused']} excused, {states['red']} RED; data races {races}, panics {panics}")
    if errors:
        print(f"  after the last iteration: {errors[:4]}")
    if n:
        print(f"  excused: {rate(states['excused'], n)} of iterations")
        for c in H1:
            print(f"  {c}: {rate(classes[c], n)} of iterations")
        others = {c: k for c, k in sorted(classes.items()) if c not in H1}
        print(f"  other classes, by iterations: {others or 'none'}")
        if not states["red"]:
            print(f"  no red iteration in {n}: a defect these audits see is in under {100 * 3 / n:.2f}% of iterations (95%), at envtest's timings")
        for e in reds:
            print("  RED:", e)
        for name in sorted(total):
            print(f"    {name}: {total[name]} in {seen[name]} iterations")
    bad = bad or not n or states["red"] > 0 or races > 0 or panics > 0
sys.exit(1 if bad or len(sys.argv) < 2 else 0)
