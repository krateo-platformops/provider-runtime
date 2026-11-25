"""Revert check for what an unparseable record kept (design §4, §6.1, §7.4, §10.2): break
each rule, and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Usage:
  python3 hack/revert-check-credential-use-unparseable.py > hack/revert-check-credential-use-unparseable.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
IN, DE, DI, TR, WL = [C + f for f in ("informers.go", "decide.go", "disown.go", "tracker.go", "writeloops.go")]
FILES = [IN, DE, DI, TR, WL]

LOG = "\tif usesAll && h.t.unparseable.first(consentKey{holder: cur.UID}) {\n"
FILL = "\tif gap || usedAll && !usesAll {\n"

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # What is queued (§10.2).
    ("what it kept isn't queued", IN,
     [(FILL, "\tif gap || false && usedAll && !usesAll {\n")]),
    ("one that still uses everything queues it", IN,
     [(FILL, "\tif _ = usesAll; gap || usedAll {\n")]),
    ("one that isn't a user counts as one", IN,
     [("r.Unparseable && !notAUser(m, h.t.o.OwnerFinalizers)\n", "r.Unparseable\n")]),
    ("one that a relist finds gone queues its last version's refs only", IN,
     [("\th.enqueue(m, nil, gap)\n", "\th.enqueue(m, nil, false && gap)\n")]),
    ("any that is gone queues the namespace", IN,
     [("\th.enqueue(m, nil, gap)\n", "\th.enqueue(m, nil, true || gap)\n")]),
    ("one that is replaced queues the refs of both only", IN,
     [("\t\th.enqueue(o, n, replaced)\n", "\t\th.enqueue(o, n, false && replaced)\n")]),
    ("any update queues the namespace", IN,
     [("\t\th.enqueue(o, n, replaced)\n", "\t\th.enqueue(o, n, true || replaced)\n")]),
    ("the namespace is a batch of its own", IN,
     [(FILL + "\t\tids = append(ids, fillAll)\n", FILL + "\t\th.t.s.queue.Add(batchKey{h.t.gk, old.Namespace})\n")]),
    ("the namespace's list that fails is lost", WL,
     [("\t\t\tfail(err, fillAll)\n", "\t\t\tfail(err)\n")]),
    # What is forgotten.
    ("one that is replaced keeps its write", IN,
     [("\t\th.t.proto.written.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its backoff", IN,
     [("\t\th.t.proto.backoff.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its turns", IN,
     [("\t\th.t.proto.gateTurns.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its conflicts", IN,
     [("\t\th.t.proto.conflicts.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its reported refs", IN,
     [("\t\th.t.proto.reported.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its unrecorded reads", IN,
     [("\t\th.t.unrecorded.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its degraded log", IN,
     [("\t\th.t.degradedLogged.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its misplaced log", IN,
     [("\t\th.t.misplaced.forget(o.UID)\n", "")]),
    ("one that is replaced keeps its unparseable log", IN,
     [("\t\th.t.unparseable.forget(o.UID)\n", "")]),
    ("one that is gone keeps its unparseable log", IN,
     [("\th.t.unparseable.forget(m.UID)\n", "")]),
    ("Forget keeps the unparseable log", TR,
     [("\tt.unparseable.forget(obj.GetUID())\n", "")]),
    # The log (§4).
    ("no log", IN,
     [(LOG, "\tif false && usesAll && h.t.unparseable.first(consentKey{holder: cur.UID}) {\n")]),
    ("a log for one that isn't a user", IN,
     [(LOG, "\tif is.Unparseable && h.t.unparseable.first(consentKey{holder: cur.UID}) {\n")]),
    ("a log for every event", IN,
     [(LOG, "\tif usesAll {\n\t\th.t.unparseable.first(consentKey{holder: cur.UID})\n")]),
    ("a log for any record", IN,
     [(LOG, "\tif cur != nil && h.t.unparseable.first(consentKey{holder: cur.UID}) {\n")]),
    ("the log has the record", IN,
     [("\"kind\", h.t.gk.String())\n\t}\n}\n\n// recordOf", "\"kind\", h.t.gk.String(), \"record\", cur.Annotations[AnnotationRefs])\n\t}\n}\n\n// recordOf")]),
    ("the old version marks the holder logged", IN,
     [("\twas, usedAll := h.recordOf(old)\n", "\twas, usedAll := h.recordOf(old)\n\tif was.Unparseable {\n\t\th.t.unparseable.first(consentKey{holder: old.UID})\n\t}\n")]),
    # Who is a user (§6.1).
    ("one being deleted without its owner finalizer is a user", DE,
     [("\treturn !notAUser(holder, ownerFinalizers) && ReadRecord(holder).Names(p)\n", "\treturn ReadRecord(holder).Names(p)\n")]),
    # Disown (§7.4).
    ("a switch for one kind strips the record", DI,
     [("\t\tcase rec.Unparseable:\n", "")]),
    ("a switch for one kind counts the record", DI,
     [("\tif set.all {\n\t\treturn true\n", "\tif set.all || rec.Unparseable {\n\t\treturn true\n")]),
    ("all leaves the record", DI,
     [("\t\tcase set.all:\n", "\t\tcase set.all && !rec.Unparseable:\n")]),
    ("all doesn't count the record", DI,
     [("\tif set.all {\n\t\treturn true\n", "\tif set.all && !rec.Unparseable {\n\t\treturn true\n")]),
]

QUEUES = {C: "TestWhatAnUnparseableRecordsEventQueues"}
REPLACED = {C: "TestAHolderReplacedInOneEventLeavesNothing"}
LOGGED = {C: "TestAnUnparseableRecordIsLoggedOnce"}
STRIP = {C: "TestDisownHolderPatch"}
KILLERS = {
    "what it kept isn't queued": {C: "TestAnUnparseableHolderThatGoesReleasesWhatItKept|TestAnUnparseableHoldersHoldsInOtherNamespaces"},
    "one that still uses everything queues it": QUEUES,
    "one that isn't a user counts as one": QUEUES,
    "one that a relist finds gone queues its last version's refs only": QUEUES,
    "any that is gone queues the namespace": QUEUES,
    "one that is replaced queues the refs of both only": QUEUES,
    "any update queues the namespace": QUEUES,
    "the namespace is a batch of its own": {C: "TestARecordsChangeIsOneBatch"},
    "the namespace's list that fails is lost": {C: "TestTogglesWhileABatchFails|TestTogglesDuringAGrace"},
    "one that is replaced keeps its turns": REPLACED,
    "one that is replaced keeps its conflicts": REPLACED,
    "one that is replaced keeps its write": REPLACED,
    "one that is replaced keeps its backoff": REPLACED,
    "one that is replaced keeps its reported refs": REPLACED,
    "one that is replaced keeps its unrecorded reads": REPLACED,
    "one that is replaced keeps its degraded log": REPLACED,
    "one that is replaced keeps its misplaced log": REPLACED,
    "one that is replaced keeps its unparseable log": REPLACED,
    "one that is gone keeps its unparseable log": LOGGED,
    "Forget keeps the unparseable log": LOGGED,
    "no log": LOGGED,
    "a log for one that isn't a user": LOGGED,
    "a log for every event": LOGGED,
    "a log for any record": LOGGED,
    "the log has the record": LOGGED,
    "the old version marks the holder logged": LOGGED,
    "one being deleted without its owner finalizer is a user": {C: "TestUsers|TestAnUnparseableHolderThatGoesReleasesWhatItKept"},
    "a switch for one kind strips the record": STRIP,
    "a switch for one kind counts the record": STRIP,
    "all leaves the record": STRIP,
    "all doesn't count the record": STRIP,
}

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
