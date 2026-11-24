"""Revert check for what a release costs (design §6.2 "A batch is a turn", §6.6):
break each rule, and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Not mutated: the default times (10s, 60s, a minute a page) but for how R1's grow,
since no test waits for them; and which line the protected handlers, the rescan,
the rechecks and A2 add to, which only orders.

Usage:
  python3 hack/revert-check-credential-use-release-cost.py > hack/revert-check-credential-use-release-cost.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
BR, RL, WL, QU, SV, MK, TR, IN = [C + f for f in ("batchrecords.go", "release.go", "writeloops.go", "queue.go", "server.go", "missedkinds.go", "tracker.go", "informers.go")]
FILES = [BR, RL, WL, QU, SV, MK, TR, IN]

# (name, file, [(old, new), ...]): every old must occur exactly once in file. The
# last of each is the killing tests, all of pkg/credentialuse.
LIST = [
    # A batch parses a record once.
    ("a record is parsed for each object", BR,
     [("\tif n, known := b.byHolder[h]; known {\n", "\tif n, known := b.byHolder[h]; false && known {\n"),
      ("\t} else if !known {\n", "\t} else if true || !known {\n")], "TestABatchParsesARecordOnce"),
    ("R2 parses what R0 did", BR,
     [("\t} else if !known {\n", "\t} else if true || !known {\n")], "TestABatchParsesARecordOnce"),
    ("a record that can't be parsed names nothing", BR,
     [("\t\tn.all = r.Unparseable\n", "\t\tn.all = false && r.Unparseable\n")], "TestABatchsRecordsAgreeWithIsUser"),
    ("no record is an empty one", BR,
     [("\tif !present {\n", "\tif false && !present {\n")], "TestABatchsRecordsAgreeWithIsUser"),
    ("every object has the first's place", BR,
     [("\t\t\tb.place[id] = len(b.place)\n", "\t\t\tb.place[id] = 0\n")], "TestABatchsRecordsAgreeWithIsUser"),
    ("no record is known to name an object of another batch", BR,
     [("\treturn n.all || !ours || n.ids != nil && n.ids[i]\n", "\treturn n.all || ours && n.ids != nil && n.ids[i]\n")], "TestABatchsRecordsAgreeWithIsUser"),
    ("a deleting holder without its owner finalizer is a user", BR,
     [("\tif notAUser(h, b.ownerFinalizers) {\n", "\tif false && notAUser(h, b.ownerFinalizers) {\n")], "TestABatchsRecordsAgreeWithIsUser"),
    ("a deleting user needn't be deleting", BR,
     [("\treturn h.DeletionTimestamp != nil && b.isUser(h, id)\n", "\treturn b.isUser(h, id)\n")], "TestABatchsRecordsAgreeWithIsUser|TestABatchKeepsRecordsApart"),
    # The bounded turn.
    ("a turn reads 65", RL,
     [("\tmaxTurnReads  = 64\n", "\tmaxTurnReads  = 65\n")], "TestAKeysTurnIsBounded"),
    ("the others have no part of their own", RL,
     [("max(minTurnOthers, maxTurnReads-len(carry))", "max(0, maxTurnReads-len(carry))")], "TestAHoldTheCachesDontShowIsReadInTime"),
    ("what is known to be held isn't read first", RL,
     [("\t\tif shown(id) {\n", "\t\tif false && shown(id) {\n")], "TestWhatTheCachesShowIsReadFirst"),
    ("what the caches show isn't known", RL,
     [("\t\treturn cachedUsers[id].carries || p.known != nil && p.known(n, id)\n", "\t\treturn p.known != nil && p.known(n, id)\n")], "TestWhatTheCachesShowIsReadFirst"),
    ("how it was queued doesn't say what is known", RL,
     [("\t\treturn cachedUsers[id].carries || p.known != nil && p.known(n, id)\n", "\t\treturn cachedUsers[id].carries\n")], "TestAWholeNamespacesTurns"),
    ("the rest is lost", RL,
     [("\tres.Rest = append(res.Rest, rest...)\n", "\t_ = rest\n")], "TestAKeysTurnIsBounded"),
    ("a batch is in the order of the names", RL,
     [("\tfor _, id := range pending {\n\t\tif _, again", "\tfor _, id := range SortedIDs(pending) {\n\t\tif _, again")], "TestAKeysTurnIsBounded"),
    ("R0 goes on once the context has ended", RL,
     [("\t\tif ctx.Err() != nil {\n\t\t\tres.Rest = append(res.Rest, id)\n\t\t\tcontinue\n\t\t}\n\t\tu := ", "\t\tif false && ctx.Err() != nil {\n\t\t\tres.Rest = append(res.Rest, id)\n\t\t\tcontinue\n\t\t}\n\t\tu := ")], "TestAnEndedContextReadsNothingMore"),
    ("R1 goes on once the context has ended", RL,
     [("\t\t\tif ctx.Err() != nil || !first[id] && window.Err() != nil {\n", "\t\t\tif !first[id] && window.Err() != nil {\n")], "TestAnEndedContextReadsNothingMore"),
    ("R1 goes on once the turn's time is over", RL,
     [("\t\t\tif ctx.Err() != nil || !first[id] && window.Err() != nil {\n", "\t\t\tif ctx.Err() != nil || false && !first[id] && window.Err() != nil {\n")], "TestAReadThatDoesntAnswer|TestARealSlowClientsTurns"),
    ("no sort's first is read once the turn's time is over", RL,
     [("\t\t\tif ctx.Err() != nil || !first[id] && window.Err() != nil {\n", "\t\t\tif ctx.Err() != nil || (true || !first[id]) && window.Err() != nil {\n")], "TestEachSortsFirstIsRead"),
    ("one sort's first is read once the turn's time is over", RL,
     [("\tfor _, sort := range [][]ID{carry[:c], others[:o]} {\n", "\tfor _, sort := range [][]ID{carry[:c]} {\n")], "TestEachSortsFirstIsRead"),
    ("a conflict is the next turn's", RL,
     [("\t\ttodo = next\n", "\t\tres.Rest, todo = append(res.Rest, next...), nil\n")], "TestAConflictsReadIsItsTurns"),
    ("the rest isn't put back", WL,
     [("\ts.queue.PutBack(k, res.Rest...)\n", "")], "TestAKeysTurnIsBounded"),
    ("the rest goes behind what was added", WL,
     [("\ts.queue.PutBack(k, res.Rest...)\n", "\tif len(res.Rest) > 0 {\n\t\ts.queue.Add(k, res.Rest...)\n\t}\n")], "TestWhatIsAddedDoesntDelayWhatWaits"),
    ("the rest is the turn's", WL,
     [("\tids = slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return rest[id] }) // from here on, the turn's\n", "\t_ = rest\n")], "TestTheRestIsNeitherDecidedNorFailed"),
    ("a namespace's list moves what was queued before it", WL,
     [("\t\tids = expand(ids, held)\n", "\t\tids = SortedIDs(expand(ids, held))\n")], "TestAWholeNamespacesTurns"),
    ("a kind outside Protected isn't read", WL,
     [("\tres, err := t.proto.release(ctx, k.Namespace, ids)\n", "\tids = slices.DeleteFunc(slices.Clone(ids), func(id ID) bool {\n\t\treturn !slices.ContainsFunc(t.o.Protected, func(pk ProtectedKind) bool { return pk.GVK.GroupKind() == id.GroupKind() })\n\t})\n\tres, err := t.proto.release(ctx, k.Namespace, ids)\n")], "TestAKindOutsideProtectedIsStillReleased"),
    # Each read's time.
    ("a read has no time", RL,
     [("\tctx, cancel := context.WithTimeout(ctx, limit)\n\tdefer cancel()\n\tm, err :=", "\tctx, cancel := context.WithTimeout(ctx, time.Hour+limit)\n\tdefer cancel()\n\tm, err :=")], "TestAReadThatDoesntAnswer"),
    ("the reads share the turn's time", RL,
     [("\t\t\tm, err := p.read(ctx, n, id)\n", "\t\t\tm, err := p.read(window, n, id)\n")], "TestARealSlowClientsTurns"),
    ("a read's time doesn't grow", RL,
     [("\treturn min(releaseReadLimit<<min(n, 3), maxReadLimit)\n", "\treturn min(releaseReadLimit, maxReadLimit)\n")], "TestAReadsTimeGrows"),
    ("a read's time grows past the API server's", RL,
     [("\treturn min(releaseReadLimit<<min(n, 3), maxReadLimit)\n", "\treturn max(releaseReadLimit<<min(n, 3), maxReadLimit)\n")], "TestAReadsTimeGrows"),
    ("a read's time grows with no failure", QU,
     [("\t\treturn f.n\n\t}\n\treturn 0\n", "\t\treturn 0\n\t}\n\treturn 0\n")], "TestAReadsTimeGrows"),
    ("a Namespace's read has no time", RL,
     [("\tctx, cancel := context.WithTimeout(ctx, limit)\n\tdefer cancel()\n\treturn p.srv.Get(ctx, namespaceGroupKind", "\tctx, cancel := context.WithTimeout(ctx, time.Hour+limit)\n\tdefer cancel()\n\treturn p.srv.Get(ctx, namespaceGroupKind")], "TestANamespaceThatDoesntAnswer"),
    ("a Namespace that failed is read again", RL,
     [("\t\t\tif nsFailed[ns] {\n", "\t\t\tif false && nsFailed[ns] {\n")], "TestANamespaceThatDoesntAnswer"),
    ("a Namespace is read once the time is over", RL,
     [("\t\t\t\tif len(nsReadAt)+len(nsFailed) > 0 && window.Err() != nil {\n", "\t\t\t\tif false && len(nsReadAt)+len(nsFailed) > 0 && window.Err() != nil {\n")], "TestANamespaceThatDoesntAnswer"),
    ("a failed Namespace's other objects are lost", RL,
     [("\t\t\tif nsFailed[ns] {\n\t\t\t\tres.Failed = append(res.Failed, r.id)\n", "\t\t\tif nsFailed[ns] {\n")], "TestANamespaceThatDoesntAnswer"),
    ("what waits for a Namespace is lost", RL,
     [("\t\t\t\tif len(nsReadAt)+len(nsFailed) > 0 && window.Err() != nil {\n\t\t\t\t\tres.Rest = append(res.Rest, r.id)\n", "\t\t\t\tif len(nsReadAt)+len(nsFailed) > 0 && window.Err() != nil {\n")], "TestANamespaceThatDoesntAnswer"),
    ("a page has no time", SV,
     [("\t\tl, err := listPage(ctx, limit, c, opts)\n", "\t\tl, err := listPage(ctx, time.Hour+limit, c, opts)\n")], "TestAListsPagesHaveATime"),
    ("a page's time is its list's", SV,
     [("\tfor {\n\t\tl, err := listPage(ctx, limit, c, opts)\n", "\tctx, cancel := context.WithTimeout(ctx, limit)\n\tdefer cancel()\n\tfor {\n\t\tl, err := listPage(ctx, limit, c, opts)\n")], "TestAListsPagesHaveATime"),
    # The kinds that couldn't be mapped.
    ("a miss isn't remembered", RL,
     [("\tif meta.IsNoMatchError(err) {\n", "\tif false && meta.IsNoMatchError(err) {\n")], "TestAKindThatCantBeMappedIsLookedForOnce"),
    ("any failure is a miss", RL,
     [("\tif meta.IsNoMatchError(err) {\n", "\tif err != nil || meta.IsNoMatchError(err) {\n")], "TestTheRestIsNeitherDecidedNorFailed"),
    ("a miss is remembered for good", MK,
     [("\tif !ok || c.Since(missed.at) >= missedKindTime {\n", "\tif !ok || false && c.Since(missed.at) >= missedKindTime {\n")], "TestMissedKindsAreBounded|TestAKindThatIsBack"),
    ("the memory of misses has no bound", MK,
     [("\tif len(m.at) <= maxMissedKinds {\n", "\tif true || len(m.at) <= maxMissedKinds {\n")], "TestMissedKindsAreBounded"),
    ("the newest miss goes first", MK,
     [("missed.at.Before(m.at[oldest].at)", "missed.at.After(m.at[oldest].at)")], "TestMissedKindsAreBounded"),
    ("each holder kind has its own misses", TR,
     [("\t\tmissed:          s.missed,\n", "\t\tmissed:          &missedKinds{},\n")], "TestAKindThatCantBeMappedIsLookedForOnce"),
    # The queue's places and lines.
    ("adding again moves an object", QU,
     [("\t\tif waits && (p.held || !known) {\n", "\t\tif false && waits && (p.held || !known) {\n"),
      ("\t\tif !waits {\n\t\t\tq.placed++\n", "\t\tif true || !waits {\n\t\t\tq.placed++\n")], "TestBatchQueueEveryOrdering"),
    ("what is put back has a new place", QU,
     [("\t\tp, had := q.taken[k][id]\n", "\t\tp, had := place{}, false\n")], "TestBatchQueueEveryOrdering"),
    ("what is put back is taken last", QU,
     [("\t\tl.back = append(es, l.back...)\n", "\t\tl.added = append(l.added, es...)\n")], "TestTheQueuesLines"),
    ("one added again meanwhile isn't put back where it was", QU,
     [("\t\t\tif again.at <= p.at {\n", "\t\t\tif true || again.at <= p.at {\n")], "TestBatchQueueEveryOrdering|TestTheQueuesLines"),
    ("a worker takes all that waits", QU,
     [("\t\tl.take(lineWindow, func(", "\t\tl.take(1<<30, func(")], "TestATurnsWorkDoesntGrowWithWhatWaits"),
    ("what is known to be held waits with the rest", QU,
     [("\t\tl := ls.of(known)\n", "\t\tl := ls.of(false)\n")], "TestTheQueuesLines"),
    ("one found to be held stays with the rest", QU,
     [("\t\tif waits && (p.held || !known) {\n", "\t\tif waits {\n")], "TestTheQueuesLines"),
    ("a stale entry takes its object", QU,
     [("\t\t\tif !waits || p.at != e.at {\n", "\t\t\tif !waits {\n")], "TestTheQueuesLines"),
    ("a held object's retry waits with the rest", QU,
     [("\t\tf.held = f.held || q.taken[k][id].held\n", "")], "TestTheQueuesLines"),
    ("a time asked for waits with the rest", QU,
     [("\t\t\treturn !e.retry || f != nil && f.held\n", "\t\t\treturn f != nil && f.held\n")], "TestTheQueuesLines"),
    ("a namespace's list waits with the rest", QU,
     [("\tq.addLocked(k, ids, func(id ID) bool { return id == fillAll })\n", "\tq.addLocked(k, ids, func(id ID) bool { return false })\n")], "TestTheQueuesLines"),
    ("what a namespace's list finds isn't known", QU,
     [("\treturn p.held || !added && fill\n", "\treturn p.held || false && !added && fill\n")], "TestTheQueuesLines"),
    ("Done remembers what was taken", QU,
     [("\tdelete(q.taken, k)\n", "")], "TestTheQueuesLines"),
    ("an emptied key is remembered", QU,
     [("\tif len(set) == 0 {\n\t\tdelete(q.pending, k)\n", "\tif len(set) == 0 && false {\n\t\tdelete(q.pending, k)\n")], "TestBatchQueueRequeuesWhatArrivesDuringWork"),
    ("ShutDown leaves the lines", QU,
     [("\tclear(q.lines)\n", "")], "TestTheQueuesLines"),
    ("a PutBack after ShutDown queues", QU,
     [("\tif len(ids) == 0 || q.stopped {\n", "\tif len(ids) == 0 {\n")], "TestTheQueuesLines"),
    ("a PutBack of nothing queues its key", QU,
     [("\tif len(ids) == 0 || q.stopped {\n", "\tif q.stopped {\n")], "TestAKeysTurnIsBounded"),
    ("a record's held names wait with the rest", IN,
     [("; ok && hasFinalizer(m, f) {\n\t\t\theld =", "; false && ok && hasFinalizer(m, f) {\n\t\t\theld =")], "TestAHeldObjectDoesntWaitBehindAFlood"),
    ("a record's other names aren't queued", IN,
     [("\tif len(rest) > 0 {\n\t\tt.s.queue.Add(k, rest...)\n", "\tif false && len(rest) > 0 {\n\t\tt.s.queue.Add(k, rest...)\n")], "TestAHeldObjectDoesntWaitBehindAFlood"),
    ("a record of held names alone asks for its namespace", IN,
     [("\tif len(rest) > 0 {\n\t\tt.s.queue.Add(k, rest...)\n", "\tif true || len(rest) > 0 {\n\t\tt.s.queue.Add(k, rest...)\n")], "TestAHoldTheCachesDontShowIsReadInTime"),
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
