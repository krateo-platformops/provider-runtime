"""Revert check for the queue's bound without the write loops (design §6.2, "Without
the write loops"): break each rule, and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Usage:
  python3 hack/revert-check-credential-use-standby.py > hack/revert-check-credential-use-standby.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
QU, UC, WL = [C + f for f in ("queue.go", "uncached.go", "writeloops.go")]
FILES = [QU, UC, WL]

# (name, file, [(old, new), ...]): every old must occur exactly once in file. The
# last of each is the killing tests, all of pkg/credentialuse.
LIST = [
    ("there is no bound", QU,
     [("\t\tif !known && !q.draining {\n", "\t\tif false && !known && !q.draining {\n")], "TestAStandbysQueueIsBounded"),
    ("the bound holds while the write loops run", QU,
     [("\t\tif !known && !q.draining {\n", "\t\tif !known {\n")], "TestTheBoundIsForAProcessWithoutWriteLoops"),
    ("a collapsed key takes names again", QU,
     [("\tif q.collapsed[k] {\n\t\tq.listLocked(k)\n", "\tif false && q.collapsed[k] {\n\t\tq.listLocked(k)\n")], "TestAStandbysQueueIsBounded"),
    ("the key with the fewest names collapses", QU,
     [("\t\tif ls.names > n {\n", "\t\tif ls.names > 0 && (n == 0 || ls.names < n) {\n")], "TestAFloodCollapsesItsOwnNamespace"),
    ("a collapse drops what is known to be held", QU,
     [("\t\tif p.held {\n\t\t\tkept[id] = p\n", "\t\tif false && p.held {\n\t\t\tkept[id] = p\n")], "TestAFloodCollapsesItsOwnNamespace"),
    ("a collapsed namespace isn't listed", QU,
     [("\tq.listLocked(most)\n", "")], "TestTheBoundIsForAProcessWithoutWriteLoops"),
    ("what waits when the loops end stays", QU,
     [("\tfor !on && q.names > maxStandbyNames && q.collapseLocked() {\n", "\tfor false && !on && q.names > maxStandbyNames && q.collapseLocked() {\n")], "TestTheBoundIsForAProcessWithoutWriteLoops"),
    ("another kind's collapses are its own", QU,
     [("\t\tif k.Holder == gk {\n", "\t\tif true || k.Holder == gk {\n")], "TestTheBoundIsForAProcessWithoutWriteLoops"),
    ("a collapse is never settled", QU,
     [("\t\tdelete(q.collapsed, k)\n", "\t\t_ = k\n")], "TestTheBoundIsForAProcessWithoutWriteLoops"),
    ("what is taken is still counted", QU,
     [("\t\t\tif !p.held {\n\t\t\t\tq.names--\n\t\t\t\tls.names--\n", "\t\t\tif false && !p.held {\n\t\t\t\tq.names--\n\t\t\t\tls.names--\n")], "TestTheCountOfWaitingNames"),
    ("what is found to be held is still counted", QU,
     [("\tif was, waits := set[id]; waits && !was.held {\n", "\tif was, waits := set[id]; false && waits && !was.held {\n")], "TestTheCountOfWaitingNames"),
    ("what is put back isn't counted", QU,
     [("\t\tq.setLocked(set, ls, id, p)\n\t\tback[p.held]", "\t\tset[id] = p\n\t\tback[p.held]")], "TestTheCountOfWaitingNames"),
    ("what is collapsed is still counted", QU,
     [("\tq.names -= ls.names\n", "")], "TestTheCountOfWaitingNames|TestAFloodCollapsesItsOwnNamespace"),
    ("ShutDown keeps the count", QU,
     [("\tq.ready, q.pending, q.names = nil, map[batchKey]places{}, 0\n", "\tq.ready, q.pending = nil, map[batchKey]places{}\n")], "TestTheCountOfWaitingNames"),
    ("the loops' start doesn't end the bound", WL,
     [("\ts.queue.setDraining(true)\n", "")], "TestTheRecordsOfACollapsedNamespaceAreQueuedAgain"),
    ("the loops' end doesn't begin the bound", WL,
     [("\t\ts.queue.setDraining(false)\n", "")], "TestTheFullListIsForAnOverflow"),
    ("nothing makes up for a collapse", UC,
     [("\tif p, ok := s.collapsedPart(ts); ok {\n", "\tif p, ok := s.collapsedPart(ts); false && ok {\n")], "TestAnElectionAfterAFloodReleasesWhatIsHeld|TestATrackAfterTheStartMakesUpForItsFlood"),
    ("the collapsed namespaces' records aren't queued again", UC,
     [("\t\tif !records {\n", "\t\tif false && !records {\n")], "TestTheRecordsOfACollapsedNamespaceAreQueuedAgain"),
    ("every namespace's records are queued again", UC,
     [("metadataOf(obj); ok && collapsed[batchKey{t.gk, m.Namespace}] {", "metadataOf(obj); ok {")], "TestTheRecordsOfACollapsedNamespaceAreQueuedAgain"),
    ("the list is of what has the held label", UC,
     [("\t\t\terr := eachPage(ctx, s.c, gk, \"\", \"\", func(", "\t\t\terr := eachPage(ctx, s.c, gk, \"\", heldSelector(), func(")], "TestAnElectionAfterAFloodReleasesWhatIsHeld"),
    ("the list is of the collapsed namespace", UC,
     [("\t\t\terr := eachPage(ctx, s.c, gk, \"\", \"\", func(", "\t\t\terr := eachPage(ctx, s.c, gk, keys[0].Namespace, \"\", func(")], "TestAnElectionAfterAFloodReleasesWhatIsHeld"),
    ("a PerName kind is listed", UC,
     [("\t\tfor _, gk := range sweepKinds(t) {\n\t\t\tif !slices.Contains(kinds, gk) {\n", "\t\tfor _, pk := range t.o.Protected {\n\t\t\tgk := pk.GVK.GroupKind()\n\t\t\tif !slices.Contains(kinds, gk) {\n")], "TestAPerNameKindIsntListedAfterAFlood"),
    ("a kind that can't be listed stops the others", UC,
     [("\t\t\t\terrs = append(errs, err)\n\t\t\t\tcontinue\n\t\t\t}\n\t\t\tdone[gk] = true\n", "\t\t\t\treturn err\n\t\t\t}\n\t\t\tdone[gk] = true\n")], "TestAKindThatCantBeListedInFullStopsNoOther"),
    ("a kind that was listed is listed again", UC,
     [("\t\t\tdone[gk] = true\n", "")], "TestAKindThatCantBeListedInFullStopsNoOther"),
    ("a collapse is settled before its lists succeed", UC,
     [("\t\tif len(errs) == 0 {\n\t\t\ts.queue.settleCollapsed(keys...)\n", "\t\tif true || len(errs) == 0 {\n\t\t\ts.queue.settleCollapsed(keys...)\n")], "TestTheFullListIsRetried"),
    ("a list that fails isn't retried", UC,
     [("\t\treturn errors.Join(errs...)\n\t}}, true\n", "\t\treturn errors.Join(errs[:0]...)\n\t}}, true\n")], "TestTheFullListIsRetried|TestAKindThatCantBeListedInFullStopsNoOther"),
]
MUTANTS = [m[:3] for m in LIST]
KILLERS = {m[0]: {C: m[3]} for m in LIST}

# Mutants a run showed no test can kill, with why.
EQUIVALENT = {}


def merged(ks):
    out = {}
    for k in ks:
        for pkg, tests in k.items():
            out.setdefault(pkg, set()).update(tests.split("|"))
    return {pkg: "|".join(sorted(ts)) for pkg, ts in out.items()}


ALL = merged(KILLERS.values())


def anchored(tests):
    return "^(" + tests + ")$"


class Run:
    def __init__(self, returncode, stdout, stderr):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "900"))


def go_test(per_pkg):
    """Runs, package by package, the tests named for each; the result is the
    first failing package's run, or the last run when all pass."""
    env = dict(os.environ, CI="1")
    last = Run(0, "", "")
    for pkg, tests in sorted(per_pkg.items()):
        r = subprocess.run(["go", "test", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", anchored(tests), "./" + pkg],
                           cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
        last = Run(r.returncode, r.stdout, r.stderr)
        if r.returncode != 0:
            return last
    return last


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, f), "rb").read())
    return h.hexdigest()


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


def killers(out):
    failed = set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))
    return sorted(t for t in failed if not any(o.startswith(t + "/") for o in failed))


def utc():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def commit():
    return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()


start = digest()
missing = [name for name, _, _ in MUTANTS if name not in KILLERS]
if missing:
    sys.exit(f"no killing tests named for {missing}")
extra = [name for name in KILLERS if name not in [m[0] for m in MUTANTS]]
if extra:
    sys.exit(f"killing tests named for no mutant: {extra}")


def check_only():
    """Checks that every mutant applies exactly once and compiles, tests
    included, and restores each file."""
    bad = 0
    for num, (name, f, edits) in enumerate(MUTANTS, 1):
        full = os.path.join(ROOT, f)
        src = open(full).read()
        counts = [src.count(old) for old, _ in edits]
        if any(c != 1 for c in counts):
            print(f"NOT-APPLIED {counts}  {name}")
            bad += 1
            continue
        mutated = src
        for old, new in edits:
            mutated = mutated.replace(old, new)
        global restore
        restore = (full, src)
        try:
            open(full, "w").write(mutated)
            r = subprocess.run(["go", "vet", "./" + os.path.dirname(f) + "/"], cwd=ROOT, capture_output=True, text=True)
        finally:
            open(full, "w").write(src)
            restore = None
        state = "COMPILES" if r.returncode == 0 else "BUILD"
        if r.returncode != 0:
            bad += 1
        print(f"{state:9} mutant {num}: {name}" + ("" if r.returncode == 0 else "\n" + (r.stdout + r.stderr)[-800:]))
    print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
    sys.exit(1 if bad else 0)


restore = None
if os.environ.get("REVERT_CHECK_ONLY"):
    check_only()
if not os.environ.get("KUBEBUILDER_ASSETS"):
    sys.exit("KUBEBUILDER_ASSETS is not set: the envtests need it")
base = go_test(ALL)
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print(f"baseline passed, commit {commit()}\n")


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
    full = os.path.join(ROOT, f)
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
    if state == "SURVIVED" and name in EQUIVALENT:
        state, detail = "EQUIVALENT", EQUIVALENT[name]
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}\n          ran: {KILLERS[name]}\n          mutant {num}: {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {run}" + (f" (of {len(MUTANTS)})" if ONLY else ""))
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
