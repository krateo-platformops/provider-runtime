"""Revert check for the wait before the release of an object being deleted (§6.4,
H1, of docs/design-credential-use-finalizer.md): break each of its rules in turn
and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant runs the tests named for it in KILLERS, and the baseline runs
them all. They are unit tests.

Usage:
  python3 hack/revert-check-credential-use-h1-wait.py > hack/revert-check-credential-use-h1-wait.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["release.go", "releasewait.go", "shared.go", "tracker.go", "writeloops.go"]

R1 = "p.mustWait != nil && !p.namespaceDeletingCached(id.Namespace) && p.mustWait(n, w)"
WAITS = "if w := (Waiting{id, m.UID}); p.mustWait != nil && p.mustWait(n, w) {\n\t\t\t\t\tres.Waiting = append(res.Waiting, w)\n\t\t\t\t\tcontinue\n\t\t\t\t}\n"
SETTLE = "\ts.waits.settle(k, slices.DeleteFunc(slices.Clone(ids), undecided)...)\n"
OVER = "return ok && e.uid == o.UID && !now.Before(e.end)"
DROPPED = "\t\ts.waits.settle(k, ids...)\n"

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("nothing waits", "release.go", [(R1, "false")]),
    ("a namespace being deleted waits too", "release.go",
     [(R1, "p.mustWait != nil && p.mustWait(n, w)")]),
    ("a namespace deleted inside a wait waits on", "release.go",
     [(R1, "p.mustWait != nil && p.mustWait(n, w)")]),
    ("what isn't being deleted waits too", "release.go",
     [("\t\t\tr := r1Read{id: id, meta: m}\n\t\t\tif m.DeletionTimestamp != nil {",
       "\t\t\tr := r1Read{id: id, meta: m}\n\t\t\tif m.DeletionTimestamp == nil {\n\t\t\t\t" + WAITS + "\t\t\t}\n\t\t\tif m.DeletionTimestamp != nil {")]),
    ("the wait begins inside the grace", "release.go",
     [("\t\t\t\tif u := cachedUsers[id]; now.Before(r.graceEnd)", "\t\t\t\t" + WAITS + "\t\t\t\tif u := cachedUsers[id]; now.Before(r.graceEnd)")]),
    ("nothing queues what waits", "writeloops.go", [("\ts.wait(k, res.Waiting)\n", "")]),
    ("a wait never ends", "releasewait.go", [(OVER, "return ok && e.uid == o.UID && e.end.IsZero()")]),
    ("a wait ends at once", "releasewait.go", [(OVER, "return ok && e.uid == o.UID && (true || now.Before(e.end))")]),
    ("another incarnation has the first's ended wait", "releasewait.go", [(OVER, "return ok && !now.Before(e.end)")]),
    ("another incarnation's wait isn't begun", "releasewait.go", [("return ok && e.uid == o.UID\n", "return ok\n")]),
    ("what is decided keeps its wait while its batch fails", "writeloops.go",
     [(SETTLE, "\tif err == nil {\n\t" + SETTLE + "\t}\n")]),
    ("what a failed prune is of keeps its wait", "writeloops.go",
     [(SETTLE, ""), ("\tdecided := slices.DeleteFunc(", SETTLE + "\tdecided := slices.DeleteFunc(")]),
    ("each batch begins the wait again", "releasewait.go",
     [("\t\tif s.waits.begun(k, o) {\n", "\t\tif false {\n")]),
    ("one wait serves the whole batch", "releasewait.go",
     [("\tfor _, o := range objs {\n\t\tif s.waits.begun(k, o) {", "\td0 := s.releaseWait()\n\tfor _, o := range objs {\n\t\tif s.waits.begun(k, o) {"),
      ("\t\td := s.releaseWait()\n", "\t\td := d0\n")]),
    ("a decided object doesn't wait again", "writeloops.go", [(SETTLE, "")]),
    ("a failed release waits again", "writeloops.go",
     [(SETTLE, SETTLE.replace("undecided", "func(id ID) bool { return waiting[id] }"))]),
    ("what still waits is settled", "writeloops.go",
     [(SETTLE, SETTLE.replace("undecided", "func(id ID) bool { return failed[id] }"))]),
    ("what still waits is settled in the queue", "writeloops.go",
     [("\tdecided := slices.DeleteFunc(slices.Clone(ids), undecided)\n",
       "\tdecided := slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return failed[id] })\n")]),
    ("what still waits leaves the gauge", "writeloops.go",
     [("\tt.keptUncached(decided, res.Kept)\n",
       "\tt.keptUncached(slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return failed[id] }), res.Kept)\n")]),
    ("a dropped batch keeps its waits", "writeloops.go", [(DROPPED, "")]),
    ("a parked batch keeps its waits", "writeloops.go",
     [(DROPPED, "\t\ts.mu.Lock()\n\t\tparked := s.tracking[k.Holder] != nil\n\t\ts.mu.Unlock()\n\t\tif !parked {\n\t" + DROPPED + "\t\t}\n")]),
    ("a stop keeps the waits", "writeloops.go", [("\t\ts.waits.clear()\n", "")]),
    ("production has no wait", "shared.go", [("\t\treleaseWait: randomReleaseWait,\n", "")]),
    ("the wait is up to 4s", "releasewait.go", [("rand.N(releaseWaitMax)", "rand.N(2 * releaseWaitMax)")]),
    ("the wait isn't random", "releasewait.go", [("return rand.N(releaseWaitMax)", "return min(releaseWaitMax/2, rand.N(releaseWaitMax)+releaseWaitMax)")]),
    ("a tracker's release has no wait", "tracker.go",
     [("func(n string, o Waiting) bool { return s.mustWait(batchKey{gk, n}, o) }", "nil")]),
]

KILLERS = {
    "nothing waits": "TestAWaitBeforeReleasingADeletingObject",
    "a namespace being deleted waits too": "TestNoWaitInANamespaceBeingDeleted",
    "a namespace deleted inside a wait waits on": "TestANamespaceDeletedInsideAWait",
    "another incarnation has the first's ended wait": "TestARecreatedObjectWaitsOfItsOwn",
    "another incarnation's wait isn't begun": "TestARecreatedObjectWaitsOfItsOwn",
    "what still waits leaves the gauge": "TestAWaitingObjectStaysCounted",
    "a parked batch keeps its waits": "TestAParkedBatchWaitsAgain",
    "what isn't being deleted waits too": "TestOnlyADeletingObjectWaits",
    "the wait begins inside the grace": "TestTheWaitBeginsAtTheGracesEnd",
    "nothing queues what waits": "TestAWaitBeforeReleasingADeletingObject",
    "a wait never ends": "TestAWaitBeforeReleasingADeletingObject",
    "a wait ends at once": "TestAWaitBeforeReleasingADeletingObject",
    "what is decided keeps its wait while its batch fails": "TestAReleasedObjectKeepsNoWait|TestAKeptObjectKeepsNoWaitThoughItsBatchFails",
    "what a failed prune is of keeps its wait": "TestAFailedPruneKeepsNoWait",
    "each batch begins the wait again": "TestAWaitBeforeReleasingADeletingObject",
    "one wait serves the whole batch": "TestEachObjectWaitsItsOwnTime",
    "a decided object doesn't wait again": "TestAReplacedObjectKeepsNoWait",
    "a failed release waits again": "TestARetryDoesNotWaitAgain",
    "what still waits is settled": "TestAWaitBeforeReleasingADeletingObject|TestTwoProcessesWithTwoWaits",
    "what still waits is settled in the queue": "TestAWaitLeavesTheBackoff",
    "a dropped batch keeps its waits": "TestADroppedBatchForgetsItsWaits",
    "a stop keeps the waits": "TestAStopDropsTheWaits",
    "production has no wait": "TestTheProductionWait",
    "the wait is up to 4s": "TestTheProductionWait",
    "the wait isn't random": "TestTheProductionWait",
    "a tracker's release has no wait": "TestAWaitBeforeReleasingADeletingObject",
}

ALL = sorted({t for ts in KILLERS.values() for t in ts.split("|")})


def anchored(tests):
    return "^(" + tests + ")$"


class Run:
    def __init__(self, returncode, stdout, stderr):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "900"))


def go_test(tests):
    r = subprocess.run(["go", "test", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", anchored(tests), "./" + PKG],
                       cwd=ROOT, env=dict(os.environ), capture_output=True, text=True, timeout=TIMEOUT + 300)
    return Run(r.returncode, r.stdout, r.stderr)


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, PKG, f), "rb").read())
    return h.hexdigest()


def killers(out):
    failed = set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))
    return sorted(t for t in failed if not any(o.startswith(t + "/") for o in failed))


def classify(r):
    out = r.stdout + r.stderr
    if r.returncode == 0:
        return "SURVIVED", ""
    if "build failed" in out or "[setup failed]" in out:
        return "BUILD", ""
    if "panic: test timed out" in out:
        return "TIMEOUT", ""
    if re.search(r"^panic:|\[recovered\]", out, re.M):
        m = re.search(r"panic: .*", out)
        return "PANIC", m.group(0)[:90] if m else ""
    return "KILLED", "killed in: " + ", ".join(killers(out))


def utc():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def commit():
    return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()


start = digest()
missing = [name for name, _, _ in MUTANTS if name not in KILLERS]
if missing:
    sys.exit(f"no killing tests named for {missing}")
base = go_test("|".join(ALL))
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print(f"baseline passed, commit {commit()}\n")

restore = None


def on_signal(signum, _frame):
    if restore:
        open(restore[0], "w").write(restore[1])
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

ONLY = os.environ.get("REVERT_ONLY")
if ONLY:
    print(f"only the mutants matching {ONLY!r}\n")
counts = {}
run = 0
for num, (name, f, edits) in enumerate(MUTANTS, 1):
    if ONLY and not re.search(ONLY, name):
        continue
    run += 1
    full = os.path.join(ROOT, PKG, f)
    src = open(full).read()
    bad = [old for old, _ in edits if src.count(old) != 1]
    if bad:
        print(f"NOT-APPLIED ({[src.count(b) for b in bad]} matches)  {name}", flush=True)
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src
    for old, new in edits:
        mutated = mutated.replace(old, new)
    restore = (full, src)
    began = utc()
    try:
        open(full, "w").write(mutated)
        state, detail = classify(go_test(KILLERS[name]))
    finally:
        open(full, "w").write(src)
        restore = None
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}\n          ran: {KILLERS[name]}\n          mutant {num}: {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {run}" + (f" (of {len(MUTANTS)})" if ONLY else ""))
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
