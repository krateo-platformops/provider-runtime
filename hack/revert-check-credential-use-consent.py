"""Revert check for the gate of recorded refs (design §11.3): break each rule, and
see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Usage:
  python3 hack/revert-check-credential-use-consent.py > hack/revert-check-credential-use-consent.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
KE, AQ, CO, UN, IN, TR = [C + f for f in ("kept.go", "acquire.go", "consent.go", "unheld.go", "informers.go", "tracker.go")]
FILES = [KE, AQ, CO, UN, IN, TR]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # What is gated, and when.
    ("a pass doesn't gate what the record names", AQ,
     [("\trecorded, err := p.gateRecorded(ctx, obj, &a)\n", "\trecorded, err := recordedGate{}, error(nil)\n")]),
    ("a pass that names nothing doesn't gate", TR,
     [("\t\tif _, gerr := t.proto.gateRecorded(ctx, obj, &a); errors.Is(gerr, ErrHolderConflict) {", "\t\tif gerr := error(nil); errors.Is(gerr, ErrHolderConflict) {")]),
    ("a conflict of the gate's A1 isn't returned", AQ,
     [("\tif errors.Is(err, ErrHolderConflict) {\n\t\treturn a, err\n\t}\n\tif err != nil {\n\t\treturn fail(err, \"Announce\")\n\t}\n\n\tbyID", "\tif err != nil {\n\t\treturn fail(err, \"Announce\")\n\t}\n\n\tbyID")]),
    ("the holder's own namespace is asked about", KE,
     [("id.Namespace == \"\" || id.Namespace == obj.GetNamespace() || ", "id.Namespace == \"\" || ")]),
    ("a cluster-scoped object is asked about", KE,
     [("id.Namespace == \"\" || id.Namespace == obj.GetNamespace() || ", "id.Namespace == obj.GetNamespace() || ")]),
    ("a kind that Disown covers is asked about", KE,
     [("p.isDisowned(gk) || p.protects != nil", "p.protects != nil")]),
    ("a kind that isn't protected is asked about", KE,
     [(" || p.protects != nil && !p.protects(gk) {", " {")]),
    ("a cached yes doesn't serve", KE,
     [("\t\tif p.cachedYes(obj, id) || p.cacheLacks", "\t\tif p.cacheLacks")]),
    ("what its cache lacks is read", KE,
     [(" || p.cacheLacks != nil && p.cacheLacks(id) {", " {")]),
    ("an unsynced cache lacks", IN,
     [("\tif !p.inf.HasSynced() {\n\t\treturn false\n\t}\n\t_, ok := p.get(id.Namespace, id.Name)\n", "\t_, ok := p.get(id.Namespace, id.Name)\n")]),
    ("an unsynced index lacks", IN,
     [("\t\treturn t.isPerName(id.GroupKind()) && t.idx.inf.HasSynced()\n", "\t\treturn t.isPerName(id.GroupKind())\n")]),
    ("a kind no cache covers lacks", IN,
     [("\t\treturn t.isPerName(id.GroupKind()) && t.idx.inf.HasSynced()\n", "\t\treturn !t.isPerName(id.GroupKind()) || t.idx.inf.HasSynced()\n")]),
    # What it drops.
    ("a ref that can't be read is dropped", KE,
     [("\t\tcase gateNotHeld:\n\t\t\t// First again", "\t\tcase gateNotHeld, gateUnknown:\n\t\t\t// First again"), ("\t\tcase gateUnknown:\n\t\tdefault:", "\t\tdefault:")]),
    ("a ref that is gone is dropped", KE,
     [("\t\tcase gateNotHeld:\n\t\t\t// First again", "\t\tcase gateNotHeld, gateMissing:\n\t\t\t// First again")]),
    ("a withdrawn ref stays", KE,
     [("func(id ID) bool { return g.withdrawn[id] || p.isDisowned(id.GroupKind()) })", "func(id ID) bool { return p.isDisowned(id.GroupKind()) })")]),
    ("a Disowned kind's refs go back in the A1", KE,
     [("func(id ID) bool { return g.withdrawn[id] || p.isDisowned(id.GroupKind()) })", "func(id ID) bool { return g.withdrawn[id] })")]),
    ("the withdrawn aren't listed", KE,
     [("\tunheld := p.listWithdrawn(ReadUnheld(obj.GetAnnotations()), withdrawn)\n", "\tunheld := p.listWithdrawn(ReadUnheld(obj.GetAnnotations()), nil)\n")]),
    ("a withdrawal isn't reported", KE,
     [("\t\tp.reports.ConsentWithdrawn(p.holder, obj, id)\n", "")]),
    ("a withdrawn ref isn't in Acquired", KE,
     [("\t\ta.Unheld = append(a.Unheld, Unheld{Ref: Ref{GroupKind: id.GroupKind(), Namespace: id.Namespace, Name: id.Name}, Reason: ReasonConsentWithdrawn})\n", "")]),
    ("a named withdrawn ref is reported again", AQ,
     [("\t\tcase recorded.withdrawn[id]: // reported already, and in a.Unheld\n", "\t\tcase recorded.withdrawn[id]: // reported already, and in a.Unheld\n\t\t\tp.reports.ConsentWithdrawn(p.holder, obj, id)\n")]),
    ("Acquired has the bare ref of one that the pass names", AQ,
     [("\t\t\t\tif a.Unheld[i].Ref.ID() == id {\n", "\t\t\t\tif false && a.Unheld[i].Ref.ID() == id {\n")]),
    ("what the gate read is read again", AQ,
     [("\t\t\tif !read {\n", "\t\t\tif true || !read {\n")]),
    ("a ref is dropped once ctx has ended", KE,
     [("\tif err := ctx.Err(); err != nil {\n\t\tif len(withdrawn) == 0 {", "\tif err := error(nil); err != nil {\n\t\tif len(withdrawn) == 0 {")]),
    # The A1 refused for its size.
    ("an A1 refused for its size isn't written again", KE,
     [("\tif tooLarge(err) {\n", "\tif false && tooLarge(err) {\n")]),
    ("any refused A1 is written again", KE,
     [("\tif tooLarge(err) {\n", "\tif err != nil && !errors.Is(err, ErrHolderConflict) {\n")]),
    ("a 413 isn't for the size", KE,
     [("(apierrors.IsInvalid(err) || apierrors.IsRequestEntityTooLargeError(err))", "apierrors.IsInvalid(err)")]),
    ("a 422 isn't for the size", KE,
     [("(apierrors.IsInvalid(err) || apierrors.IsRequestEntityTooLargeError(err))", "apierrors.IsRequestEntityTooLargeError(err)")]),
    # The bounds, and the waiting order.
    ("no bound on the reads", KE,
     [("p.gateTurns.next(uid, ask, maxGateReads)", "p.gateTurns.next(uid, ask, len(ask))")]),
    ("a Namespace is read for each ref", CO,
     [("\t\tnamespaces[id.Namespace] = ns\n", "")]),
    ("a new ref goes ahead of those that wait", KE,
     [("\t\t\tat = t.n\n", "\t\t\tat = 0\n")]),
    ("no order", KE,
     [("func(a, b ID) int { return cmp.Compare(now[a], now[b]) }", "func(a, b ID) int { return cmp.Compare(now[a], now[a]) }")]),
    ("no order at 16 or fewer", KE,
     [("\tslices.SortStableFunc(out, func(a, b ID) int { return cmp.Compare(now[a], now[b]) })\n", "\tif len(ids) > n {\n\t\tslices.SortStableFunc(out, func(a, b ID) int { return cmp.Compare(now[a], now[b]) })\n\t}\n")]),
    ("a ref that was read doesn't go behind", KE,
     [("\t\tt.n++\n\t\tat[id] = t.n\n", "\t\tat[id] = t.n\n")]),
    ("a ref is read once picked", KE,
     [("\treturn out[:n]\n}", "\tt.n++\n\tfor _, id := range out[:n] {\n\t\tnow[id] = t.n\n\t}\n\treturn out[:n]\n}")]),
    ("refs no longer asked about are remembered", KE,
     [("\twas, now := t.at[holder], map[ID]uint64{}\n", "\twas, now := t.at[holder], map[ID]uint64{}\n\tfor id, at := range was {\n\t\tnow[id] = at\n\t}\n")]),
    ("a holder with nothing to ask about is remembered", KE,
     [("\t\tdelete(t.at, holder)\n\t\treturn nil\n", "\t\treturn nil\n")]),
    ("a read that is cut always goes behind", KE,
     [("\t\t\tif i == 0 {\n", "\t\t\tif i >= 0 {\n")]),
    ("a read that is cut never goes behind", KE,
     [("\t\t\tif i == 0 {\n", "\t\t\tif i < 0 {\n")]),
    ("a read starts after the deadline", KE,
     [("\t\tif reads.Err() != nil {\n\t\t\tbreak\n\t\t}\n\t\tfound", "\t\tfound")]),
    ("a withdrawn ref whose A1 failed goes behind", KE,
     [("\t\t\tknown++\n\t\t\tcontinue\n", "\t\t\tknown++\n")]),
    # The deadlines.
    ("no deadline of the gate's own", TR,
     [("\t\tgateContext:     gateDeadline,\n", "\t\tgateContext:     context.WithCancel,\n")]),
    ("the A1 under the gate's deadline", KE,
     [("\t_, err := p.announce(ctx, obj, want, &unheld, a)\n", "\t_, err := p.announce(reads, obj, want, &unheld, a)\n")]),
    ("the reads under the pass's context", KE,
     [("\t\tfound := p.liveConsentIn(reads, obj, id, namespaces)\n\t\tif reads.Err() != nil {", "\t\tfound := p.liveConsentIn(ctx, obj, id, namespaces)\n\t\tif ctx.Err() != nil {")]),
    ("all that the pass has left", KE,
     [("time.Until(end)/2)", "time.Until(end))")]),
    ("no 5s under the pass's deadline", KE,
     [("\t\td = min(d, time.Until(end)/2)\n", "\t\td = time.Until(end) / 2\n")]),
    ("half of 5s at most", KE,
     [("const gateReadTimeout = 5 * time.Second", "const gateReadTimeout = 2500 * time.Millisecond")]),
    # The backoff.
    ("what couldn't be read is known", KE,
     [("\t\tcase gateUnknown:\n\t\tdefault:", "\t\tcase gateUnknown:\n\t\t\tknown++\n\t\tdefault:")]),
    ("no retry for what isn't known", KE,
     [("\t\ta.retryIn(p.backoff.fail(rest))\n", "\t\tp.backoff.fail(rest)\n")]),
    ("the gate's backoff isn't reset", KE,
     [("\t\tp.backoff.succeed(rest)\n", "\t\t_ = rest\n")]),
    # The unheld list of released holds that no pass meets.
    ("a pass that can't follow drops the entries", AQ,
     [("\t\tif unfollowed {\n\t\t\tu = append(u, keptEntries(", "\t\tif false && unfollowed {\n\t\t\tu = append(u, keptEntries(")]),
    ("the entries of refs the pass met stay", KE,
     [("\t\tcase released(e) && !refs[e.Ref]:\n", "\t\tcase released(e):\n")]),
    ("the count of the folded is lost", KE,
     [("\t\t\tn := e.Unmet\n", "\t\t\tn := 0\n")]),
    ("the folded that the pass met stay in the count", KE,
     [("\t\t\t\tif refs[ref] {\n", "\t\t\t\tif false && refs[ref] {\n")]),
    ("a folded entry of the gate's isn't counted", KE,
     [("\t\tout[i].unmet = released(out[i])\n", "")]),
    ("a folded entry of the last A1's isn't counted", KE,
     [("\t\t\te.unmet = true\n", "")]),
    ("More doesn't carry what is unmet", UN,
     [("\t\tunmet += e.Unmet\n", "")]),
    # What is forgotten.
    ("a holder that is gone keeps its turns", IN,
     [("\th.t.proto.gateTurns.forget(m.UID)\n", "")]),
    ("a holder that is gone keeps its backoff", IN,
     [("\th.t.proto.backoff.forget(m.UID)\n", "")]),
    ("a holder that is gone keeps its reported refs", IN,
     [("\th.t.proto.reported.forget(m.UID)\n", "")]),
    ("Forget keeps the turns", AQ,
     [("\tp.gateTurns.forget(obj.GetUID())\n", "")]),
]

GATED = {C: "TestKeptRefsInAnotherNamespaceAreGated|TestTheGateOfKeptRefs"}
NOTHING = {C: "TestAPassThatNamesNothingGatesKeptRefs"}
GATE = {C: "TestTheGateOfKeptRefs"}
ONE = {C: "TestTheGateOfKeptRefsWritesOneA1"}
COSTS = {C: "TestAForgedRecordCostsABoundedNumberOfReads"}
STARVE = {C: "TestAForgedRecordCantStarveARef"}
BACKOFF = {C: "TestTheBackoffOfForgedRefs|TestTheGateOfKeptRefs"}
LACKS = {C: "TestCacheLacks|TestCacheLacksPerName"}
LIST = {C: "TestTheUnheldListOfKeptRefs"}
COUNT = {C: "TestTheCountOfKeptReleases"}
GONE = {C: "TestAHolderGoneWithoutAForgetLeavesNothing|TestCacheLacks"}
KILLERS = {
    "a pass doesn't gate what the record names": GATED,
    "a pass that names nothing doesn't gate": NOTHING,
    "a conflict of the gate's A1 isn't returned": ONE,
    "the holder's own namespace is asked about": {C: "TestAKeptRefInTheHoldersNamespaceIsKept"},
    "a cluster-scoped object is asked about": COSTS,
    "a kind that Disown covers is asked about": GATE,
    "a kind that isn't protected is asked about": COSTS,
    "a cached yes doesn't serve": GATE,
    "what its cache lacks is read": COSTS,
    "an unsynced cache lacks": LACKS,
    "an unsynced index lacks": LACKS,
    "a kind no cache covers lacks": LACKS,
    "a ref that can't be read is dropped": GATE,
    "a ref that is gone is dropped": GATE,
    "a withdrawn ref stays": GATED,
    "a Disowned kind's refs go back in the A1": GATE,
    "the withdrawn aren't listed": LIST,
    "a withdrawal isn't reported": ONE,
    "a withdrawn ref isn't in Acquired": ONE,
    "a named withdrawn ref is reported again": ONE,
    "Acquired has the bare ref of one that the pass names": ONE,
    "what the gate read is read again": GATE,
    "a ref is dropped once ctx has ended": ONE,
    "an A1 refused for its size isn't written again": ONE,
    "any refused A1 is written again": ONE,
    "a 413 isn't for the size": ONE,
    "a 422 isn't for the size": ONE,
    "no bound on the reads": COSTS,
    "a Namespace is read for each ref": COSTS,
    "a new ref goes ahead of those that wait": STARVE,
    "no order": STARVE,
    "no order at 16 or fewer": STARVE,
    "a ref that was read doesn't go behind": STARVE,
    "a ref is read once picked": STARVE,
    "refs no longer asked about are remembered": COSTS,
    "a holder with nothing to ask about is remembered": COSTS,
    "a read that is cut always goes behind": STARVE,
    "a read that is cut never goes behind": STARVE,
    "a read starts after the deadline": STARVE,
    "a withdrawn ref whose A1 failed goes behind": STARVE,
    "no deadline of the gate's own": LACKS,
    "the A1 under the gate's deadline": STARVE,
    "the reads under the pass's context": STARVE,
    "all that the pass has left": STARVE,
    "no 5s under the pass's deadline": LACKS,
    "half of 5s at most": LACKS,
    "what couldn't be read is known": BACKOFF,
    "no retry for what isn't known": BACKOFF,
    "the gate's backoff isn't reset": BACKOFF,
    "a pass that can't follow drops the entries": LIST,
    "the entries of refs the pass met stay": LIST,
    "the count of the folded is lost": COUNT,
    "the folded that the pass met stay in the count": COUNT,
    "a folded entry of the gate's isn't counted": COUNT,
    "a folded entry of the last A1's isn't counted": COUNT,
    "More doesn't carry what is unmet": COUNT,
    "a holder that is gone keeps its turns": GONE,
    "a holder that is gone keeps its backoff": GONE,
    "a holder that is gone keeps its reported refs": GONE,
    "Forget keeps the turns": {C: "TestAcquireOutcomes"},
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
