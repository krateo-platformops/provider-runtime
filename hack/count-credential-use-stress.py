"""Counts what TestStress's go test -v outputs, named as arguments, show: the
gate of a long run (docs/design-credential-use-finalizer.md §14).

An iteration is green, excused, or red. It is excused only if every one of its
error lines is an audit's of the API server's own H1 (AUDIT H1-RECREATE or
AUDIT H1-LOST-A2, and not H1-RECREATE-UNBOUNDED), if they are two at most, and
if the class's rate has been stated for its sub-test (STATED): one error line of
any other kind, an AUDIT or not, and it is red. Then come the summaries' counts,
added up.

STATED is empty: H1's wait changes the rates, so none is stated before they are
measured on top of it. Each entry carries the commit it was measured at, and is
to be measured again when the wait, the audits or the machine change. A stated
rate is compared as what it is, a count of its own: n in this run's iterations
is over it if, were the two rates the same, n or more of the two runs' total
would fall in this one with a probability under 2.5% (a binomial). The output
says from which n on that is. So a defect that adds less than that to a rate
isn't seen: from 5 in 207, 19 or fewer in 300 pass.

The iterations of all the files are added up, by sub-test, before they are
judged: several short runs are one long one. An error line belongs to the
sub-test that go test's own === lines say is running.

The intervals are exact 95% Poisson ones, on the count. A count of 0 has the
upper bound 3 over the iterations, which is one-sided.

It exits 1 on a red iteration, an error line outside every iteration, a class
over its stated rate, a data race, a panic, or a file without an iteration.
"""
import collections, math, re, sys

H1 = ("H1-RECREATE", "H1-LOST-A2")
# By sub-test: the commit it was measured at, and, by class, (seen, of so many
# iterations), every one of them read by hand. For example
#   "2_processes,_no_leader_election": {"commit": "0123abc", "H1-RECREATE": (5, 207)}
STATED = {}
MAX_H1 = 2  # findings in one excused iteration
SUMMARY = re.compile(r"\d+ processes, \S+, seed \d+: ")
NAMES = re.compile(r"^(\d+) (operations|namespace teardowns|passes)$")
ERROR = re.compile(r"\s+\S+\.go:\d+: (.*)$")
EXCUSED = re.compile("AUDIT (" + "|".join(H1) + "):")
MORE = re.compile(r"AUDIT [A-Z0-9-]+: and \d+ more$")
RUNS = re.compile(r"=== (?:RUN|CONT|NAME)\s+(\S+)$")
ENDS = re.compile(r"\s+--- (PASS|FAIL): TestStress/(\S+) \(([0-9.]+)s\)")


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


def h1_alone(errors):
    """Every error line is an audit's of the API server's H1; there are some,
    and no more findings than one iteration of it has."""
    return bool(errors) and all(EXCUSED.match(e) for e in errors) and sum(not MORE.match(e) for e in errors) <= MAX_H1


def classes(errors):
    return {c for e in errors for c in re.findall(r"^AUDIT ([A-Z0-9-]+):", e)}


def excused(subtest, errors):
    return h1_alone(errors) and all(c in STATED.get(subtest, {}) for c in classes(errors))


def tail(n, of, seen, m):
    """The probability of n or more of n+seen falling in of of of+m."""
    p, k = of / (of + m), n + seen
    return sum(math.comb(k, i) * p**i * (1 - p) ** (k - i) for i in range(n, k + 1))


def over(subtest, cls, n, of):
    """Whether n in of iterations is over cls's stated rate, and what to print."""
    stated = STATED.get(subtest, {})
    if cls not in stated:
        return False, "no rate is stated for this sub-test: an iteration with it is red"
    seen, m = stated[cls]
    need = next((k for k in range(1, of + 1) if tail(k, of, seen, m) < 0.025), None)
    what = f"stated: {seen} in {m} at {stated['commit']}"
    if need is None:
        return False, what + f"; {of} iterations are too few to lie over it"
    return n >= need, what + f"; over it from {need} on" + (": OVER" if n >= need else "")


class Iteration:
    def __init__(self, subtest, state, seconds, errors):
        self.subtest, self.seconds, self.errors = subtest, seconds, errors
        self.state = "green" if state == "PASS" else "excused" if excused(subtest, errors) else "red"


def read(path):
    """Prints what path shows; returns its iterations, and whether it is bad
    but for them."""
    text = open(path, errors="replace").read()
    total, seen = collections.Counter(), collections.Counter()
    its, errors, outside, current = [], collections.defaultdict(list), [], ""
    for line in text.split("\n"):
        run = RUNS.match(line)
        if run:
            current = run[1].removeprefix("TestStress").removeprefix("/")
            continue
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
            (errors[current] if current else outside).append(m[1])
        end = ENDS.match(line)
        if end:
            its.append(Iteration(end[2], end[1], float(end[3]), errors.pop(end[2], [])))
    for es in errors.values():  # of a sub-test that never ended, or of another test
        outside += es
    states = collections.Counter(i.state for i in its)
    races, panics = text.count("DATA RACE"), len(re.findall(r"^panic:", text, re.M))
    took = [i.seconds for i in its]
    print(f"{path}: {len(its)} iterations, {sum(took) / max(1, len(its)):.1f}s each (max {max(took, default=0):.1f}); "
          f"{states['green']} green, {states['excused']} excused, {states['red']} RED; data races {races}, panics {panics}")
    for i in its:
        if i.state == "red":
            print(f"  RED: {i.subtest}:", [e[:200] for e in i.errors if not EXCUSED.match(e)][:4] or [e[:200] for e in i.errors][:4])
    if outside:
        print(f"  RED, outside every iteration: {outside[:4]}")
    for name in sorted(total):
        print(f"    {name}: {total[name]} in {seen[name]} iterations")
    return its, not its or bool(outside) or races > 0 or panics > 0


def judge(its):
    """Prints what all the iterations show, by sub-test; returns whether it is
    bad."""
    bad = False
    for sub in sorted({i.subtest for i in its}):
        mine = [i for i in its if i.subtest == sub]
        of, reds = len(mine), sum(i.state == "red" for i in mine)
        per = collections.Counter(c for i in mine for c in classes(i.errors))
        print(f"{sub}: {of} iterations, {sum(i.state == 'green' for i in mine)} green, {sum(i.state == 'excused' for i in mine)} excused, {reds} RED")
        for c in H1:
            exceeded, what = over(sub, c, per[c], of)
            bad = bad or exceeded
            print(f"  {c}: {rate(per[c], of)} of iterations; {what}")
        print(f"  other classes, by iterations: { {c: k for c, k in sorted(per.items()) if c not in H1} or 'none'}")
        if not reds and of > 3:
            print(f"  no red iteration in {of}: a defect these audits see is in under {100 * 3 / of:.2f}% of iterations (95%), at envtest's timings")
        bad = bad or reds > 0
    return bad


if __name__ == "__main__":
    files = [read(p) for p in sys.argv[1:]]
    bad = judge([i for its, _ in files for i in its])
    sys.exit(1 if bad or not files or any(b for _, b in files) else 0)
