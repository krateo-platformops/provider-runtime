"""Revert check for the retries (design §6.2, §10.5): break each rule, and see
whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Usage:
  python3 hack/revert-check-credential-use-retries.py > hack/revert-check-credential-use-retries.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
QU, RL, WL, UC, IX = [C + f for f in ("queue.go", "release.go", "writeloops.go", "uncached.go", "index.go")]
FILES = [QU, RL, WL, UC, IX]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # The queue's one pending retry per key.
    ("a retry is another timer", QU,
     [("\tif old := q.retries[k]; old != nil {\n", "\tif old := q.retries[k]; false && old != nil {\n")]),
    ("a later retry replaces a sooner one", QU,
     [("\t\tif !e.at.Before(old.at) {\n", "\t\tif false && !e.at.Before(old.at) {\n")]),
    ("a sooner retry waits for the later one", QU,
     [("\t\tif !e.at.Before(old.at) {\n", "\t\tif true || !e.at.Before(old.at) {\n")]),
    ("a retry that joins loses its objects", QU,
     [("\t\t\told.ids = e.ids\n", "")]),
    ("a retry that takes over loses what there was", QU,
     [("\t\tdelete(q.timers, old)\n", "\t\tdelete(q.timers, old)\n\t\te.ids = slices.Clone(ids)\n")]),
    ("the retry taken over still counts", QU,
     [("\t\tdelete(q.timers, old)\n", "")]),
    ("the timer taken over isn't stopped", QU,
     [("\t\tlater.Stop()\n", "\t\t_ = later\n")]),
    ("the timer taken over is stopped under the lock", QU,
     [("\tq.retries[k] = e\n\tq.mu.Unlock()\n\tif later != nil {\n\t\tlater.Stop()\n\t}\n", "\tq.retries[k] = e\n\tif later != nil {\n\t\tlater.Stop()\n\t}\n\tq.mu.Unlock()\n")]),
    ("a fired retry stays pending", QU,
     [("\t\tif q.retries[k] == e {\n", "\t\tif false && q.retries[k] == e {\n")]),
    ("a timer that fires unpends any retry", QU,
     [("\t\tif q.retries[k] == e {\n", "\t\tif true || q.retries[k] == e {\n")]),
    ("ShutDown keeps the retries", QU,
     [("\tclear(q.retries)\n", "")]),
    ("Retry reads the clock under its lock", QU,
     [("\te := &queueTimer{ids: slices.Clone(ids), at: q.clock.Now().Add(d)}\n\tq.mu.Lock()\n", "\tq.mu.Lock()\n\te := &queueTimer{ids: slices.Clone(ids), at: q.clock.Now().Add(d)}\n")]),
    ("a retry with no objects covers none", QU,
     [("\te := &queueTimer{ids: slices.Clone(ids), at: q", "\tif len(ids) == 1 && ids[0] == fillAll {\n\t\tids = nil\n\t}\n\te := &queueTimer{ids: slices.Clone(ids), at: q")]),
    ("a Retry after ShutDown sets a timer", QU,
     [("at: q.clock.Now().Add(d)}\n\tq.mu.Lock()\n\tif q.stopped {\n", "at: q.clock.Now().Add(d)}\n\tq.mu.Lock()\n\tif false && q.stopped {\n")]),
    ("a retry with no delay waits", QU,
     [("func (q *batchQueue) Retry(k batchKey, d time.Duration, ids ...ID) {\n\tif d <= 0 {\n", "func (q *batchQueue) Retry(k batchKey, d time.Duration, ids ...ID) {\n\tif d < 0 {\n")]),
    ("a fired timer is added though it was taken over", QU,
     [("\t\tif !q.timers[e] {\n", "\t\tif false && !q.timers[e] {\n")]),
    # What a batch couldn't decide.
    ("R1's failed read isn't reported", RL,
     [("\t\t\t\tfail(fmt.Errorf(\"credential-use: reading %s: %w\", id, err), id)\n", "\t\t\t\tfail(fmt.Errorf(\"credential-use: reading %s: %w\", id, err))\n")]),
    ("a rejected release isn't reported", RL,
     [("ErrReleaseRejected, id, rv), id)\n", "ErrReleaseRejected, id, rv))\n")]),
    ("a failed repair of the epoch isn't reported", RL,
     [("\t\t\t\t\tfail(err, id)\n", "\t\t\t\t\tfail(err)\n")]),
    ("a failed repair of the epoch is looked at again at once too", RL,
     [("\t\t\t\t\tfail(err, id)\n\t\t\t\t\tcontinue\n", "\t\t\t\t\tfail(err, id)\n")]),
    ("a Namespace that can't be read isn't reported", RL,
     [("reading namespace %s: %w\", ns, err), r.id)\n", "reading namespace %s: %w\", ns, err))\n")]),
    ("a holder list that fails isn't reported", RL,
     [("\t\t\tfor _, r := range decidable {\n\t\t\t\tres.Failed = append(res.Failed, r.id)\n\t\t\t}\n\t\t\tbreak\n", "\t\t\tbreak\n")]),
    ("a failed release isn't reported", RL,
     [("\"credential-use: releasing %s: %w\", r.id, err), r.id)\n", "\"credential-use: releasing %s: %w\", r.id, err))\n")]),
    ("an object that kept changing isn't reported", RL,
     [("todo, maxReleaseRounds), todo...)\n", "todo, maxReleaseRounds))\n")]),
    # The release loop.
    ("the whole batch is retried", WL,
     [("s.retryBatch(ctx, t, k, SortedIDs(keysOf(failed)), errors", "s.retryBatch(ctx, t, k, ids, errors")]),
    ("a retry isn't the key's one", WL,
     [("\ts.queue.Retry(k, d, ids...)\n", "\ts.queue.AddAfter(k, d, ids...)\n")]),
    ("the grace's end waits for the backoff", WL,
     [("\tif !res.RequeueAt.IsZero() { //", "\tif len(errs) == 0 && !res.RequeueAt.IsZero() { //")]),
    ("the grace's end covers what failed", WL,
     [("res.RequeueAt.Sub(s.clock.Now()), decided...)", "res.RequeueAt.Sub(s.clock.Now()), ids...)")]),
    ("a failed list of the namespace stops its batch", WL,
     [("\t\t\tfailed[fillAll] = true\n\t\t}\n", "\t\t\tfailed[fillAll] = true\n\t\t\tids, held = nil, nil\n\t\t}\n")]),
    ("a failed list of the namespace isn't retried", WL,
     [("\t\t\tfailed[fillAll] = true\n", "")]),
    ("a failed list of the namespace is settled", WL,
     [("\tif fill && !failed[fillAll] {\n", "\tif fill {\n")]),
    ("a namespace's list that succeeds isn't settled", WL,
     [("\tif fill && !failed[fillAll] {\n", "\tif false && fill && !failed[fillAll] {\n")]),
    ("what was decided isn't settled", WL,
     [("\ts.backoff.settle(k, decided...)\n", "")]),
    ("what failed is settled", WL,
     [("\ts.backoff.settle(k, decided...)\n", "\ts.backoff.settle(k, ids...)\n")]),
    ("a failed prune isn't retried for its objects", WL,
     [("\t\t\tfor _, id := range bare {\n\t\t\t\tfailed[id] = true\n\t\t\t}\n", "")]),
    ("what failed is counted as decided", WL,
     [("\tt.keptUncached(decided, res.Kept)\n", "\tt.keptUncached(ids, res.Kept)\n")]),
    ("heldIn returns nothing after an error", WL,
     [("\t\tout, err = s.liveHeldIn(ctx, t, n)\n", "\t\tif out, err = s.liveHeldIn(ctx, t, n); err != nil {\n\t\t\treturn nil, err\n\t\t}\n")]),
    ("any success starts the backoff again", WL,
     [("\tif len(f.ids) == 0 {\n", "\tif true || len(f.ids) == 0 {\n")]),
    ("the backoff never starts again", WL,
     [("\tif len(f.ids) == 0 {\n", "\tif false && len(f.ids) == 0 {\n")]),
    ("a failure forgets its objects", WL,
     [("\t\tf.ids[id] = true\n", "\t\t_ = id\n")]),
    # The live lists, and the startup recheck.
    ("a kind that can't be listed stops the others of its batch", UC,
     [("\t\t\terrs = append(errs, err)\n\t\t\tcontinue\n", "\t\t\treturn nil, err\n")]),
    ("a kind that can't be listed is no error", UC,
     [("\treturn out, errors.Join(errs...)\n", "\treturn out, errors.Join(errs[:0]...)\n")]),
    ("the recheck stops at what fails", UC,
     [("\t\t\tfailed = append(failed, p)\n", "\t\t\tfailed = append(failed, p)\n\t\t\tif len(failed) > 0 {\n\t\t\t\tbreak\n\t\t\t}\n")]),
    ("the recheck retries everything", UC,
     [("\t\tif left = failed; len(left) == 0 {\n", "\t\tif len(failed) == 0 {\n")]),
    ("the recheck isn't retried", UC,
     [("\t\tif left = failed; len(left) == 0 {\n", "\t\tif left = failed; true || len(left) == 0 {\n")]),
    ("an index's recheck starts over", UC,
     [("return s.recheckIndex(ctx, t, done)", "return s.recheckIndex(ctx, t, func() map[ID]bool { clear(done); return done }())")]),
    ("an index's recheck reads again what it dealt with", IX,
     [("\t\tif done[id] {\n", "\t\tif false && done[id] {\n")]),
    ("a held object isn't dealt with", IX,
     [("\t\tif held {\n\t\t\tdone[id] = true\n\t\t}\n", "")]),
    ("a refused object isn't dealt with", IX,
     [("\t\t\t\tix.denied(id, err)\n\t\t\t\tdone[id] = true\n", "\t\t\t\tix.denied(id, err)\n")]),
    ("a vouched object isn't dealt with", IX,
     [("\t\t\t\t// neither reported nor retried (§10.6.6).\n\t\t\t\tdone[id] = true\n", "\t\t\t\t// neither reported nor retried (§10.6.6).\n")]),
    ("what couldn't be pruned is dealt with", IX,
     [("\t} else {\n\t\tfor _, id := range bare {\n\t\t\tdone[id] = true\n\t\t}\n\t}\n", "\t}\n\tfor _, id := range bare {\n\t\tdone[id] = true\n\t}\n")]),
    ("what was pruned is read again", IX,
     [("\t\tfor _, id := range bare {\n\t\t\tdone[id] = true\n\t\t}\n", "")]),
]

ONE = {C: "TestRetryIsOnePerKey|TestBatchQueueEveryOrdering"}
STRESS = {C: "TestRetryUnderStress"}
SAYS = {C: "TestReleaseSaysWhatItCouldntDecide"}
ALONE = {C: "TestAFailingObjectIsRetriedAlone"}
KIND = {C: "TestAKindThatCantBeListedStopsNoOther"}
IDX = {C: "TestTheIndexsRecheckRetriesWhatFailed"}
KILLERS = {
    "a retry is another timer": ONE,
    "a later retry replaces a sooner one": ONE,
    "a sooner retry waits for the later one": ONE,
    "a retry that joins loses its objects": ONE,
    "a retry that takes over loses what there was": ONE,
    "the retry taken over still counts": ONE,
    "the timer taken over isn't stopped": ONE,
    "the timer taken over is stopped under the lock": STRESS,
    "a fired retry stays pending": ONE,
    "a timer that fires unpends any retry": ONE,
    "ShutDown keeps the retries": ONE,
    "Retry reads the clock under its lock": STRESS,
    "a retry with no objects covers none": ONE,
    "a Retry after ShutDown sets a timer": ONE,
    "a retry with no delay waits": ONE,
    "a fired timer is added though it was taken over": {C: "TestRetryIsOnePerKey|TestBatchQueueEveryOrdering|TestRetryUnderStress"},
    "R1's failed read isn't reported": SAYS,
    "a rejected release isn't reported": SAYS,
    "a failed repair of the epoch isn't reported": SAYS,
    "a failed repair of the epoch is looked at again at once too": SAYS,
    "a Namespace that can't be read isn't reported": SAYS,
    "a holder list that fails isn't reported": SAYS,
    "a failed release isn't reported": SAYS,
    "an object that kept changing isn't reported": SAYS,
    "the whole batch is retried": ALONE,
    "a retry isn't the key's one": ALONE,
    "the grace's end waits for the backoff": ALONE,
    "the grace's end covers what failed": ALONE,
    "a failed list of the namespace stops its batch": KIND,
    "a failed list of the namespace isn't retried": KIND,
    "a failed list of the namespace is settled": KIND,
    "a namespace's list that succeeds isn't settled": KIND,
    "what was decided isn't settled": ALONE,
    "what failed is settled": ALONE,
    "a failed prune isn't retried for its objects": {C: "TestAFailedPruneIsRetriedAlone"},
    "what failed is counted as decided": {C: "TestAFailingObjectStaysCounted"},
    "heldIn returns nothing after an error": KIND,
    "any success starts the backoff again": {C: "TestBackoffLastsWhileAnObjectFails"},
    "the backoff never starts again": {C: "TestBackoffLastsWhileAnObjectFails"},
    "a failure forgets its objects": {C: "TestBackoffLastsWhileAnObjectFails"},
    "a kind that can't be listed stops the others of its batch": KIND,
    "a kind that can't be listed is no error": KIND,
    "the recheck stops at what fails": KIND,
    "the recheck retries everything": KIND,
    "the recheck isn't retried": KIND,
    "an index's recheck starts over": KIND,
    "an index's recheck reads again what it dealt with": IDX,
    "a held object isn't dealt with": IDX,
    "a refused object isn't dealt with": IDX,
    "a vouched object isn't dealt with": IDX,
    "what couldn't be pruned is dealt with": IDX,
    "what was pruned is read again": IDX,
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
