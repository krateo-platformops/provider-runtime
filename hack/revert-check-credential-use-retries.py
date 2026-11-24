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
    # The queue's failures: one pending retry per object, on its own backoff.
    ("an object that waits gets another retry", QU,
     [("\t\tif f.waits != nil {\n\t\t\tcontinue\n", "\t\tif false && f.waits != nil {\n\t\t\tcontinue\n")]),
    ("a run an event queued is counted", QU,
     [("\t\tif f.waits != nil {\n\t\t\tcontinue\n", "\t\tif f.waits != nil {\n\t\t\tf.n++\n\t\t\tcontinue\n")]),
    ("the backoff doesn't grow", QU,
     [("\t\tf.n++\n\t\te := timers[d]\n", "\t\te := timers[d]\n")]),
    ("the backoff is the key's", QU,
     [("\t\td := q.backoff(f.n)\n", "\t\tfor _, o := range m {\n\t\t\tf.n = max(f.n, o.n)\n\t\t}\n\t\td := q.backoff(f.n)\n")]),
    ("every failure gets the base delay's timer", QU,
     [("\t\td := q.backoff(f.n)\n", "\t\td := q.backoff(0)\n")]),
    ("the delay returned isn't the soonest", QU,
     [("soonest == 0 || d < soonest", "soonest == 0 || d > soonest")]),
    ("a Fail after ShutDown sets a timer", QU,
     [("\tif q.stopped || len(ids) == 0 {\n", "\tif len(ids) == 0 {\n")]),
    ("a Fail of nothing remembers its key", QU,
     [("\tif q.stopped || len(ids) == 0 {\n", "\tif q.stopped {\n")]),
    ("a settled object is remembered", QU,
     [("\t\tdelete(m, id)\n", "")]),
    ("a key whose objects are all settled is remembered", QU,
     [("\tif m != nil && len(m) == 0 {\n", "\tif false && m != nil && len(m) == 0 {\n")]),
    ("a timer nothing waits for stays", QU,
     [("\t\t\tif e.left--; e.left == 0 {\n", "\t\t\tif e.left--; false && e.left == 0 {\n")]),
    ("a timer others wait for goes", QU,
     [("\t\t\tif e.left--; e.left == 0 {\n", "\t\t\tif e.left--; e.left >= 0 {\n")]),
    ("a timer nothing waits for isn't stopped", QU,
     [("\t\t\t\t\tstop = append(stop, e.t)\n\t\t\t\t}\n\t\t\t}\n", "\t\t\t\t\t_ = stop\n\t\t\t\t}\n\t\t\t}\n")]),
    ("Settle stops timers under the lock", QU,
     [("\t\tdelete(q.failing, k)\n\t}\n\tq.mu.Unlock()\n\tfor _, t := range stop {\n\t\tt.Stop()\n\t}\n", "\t\tdelete(q.failing, k)\n\t}\n\tfor _, t := range stop {\n\t\tt.Stop()\n\t}\n\tq.mu.Unlock()\n")]),
    ("the clock is asked for a timer under the lock", QU,
     [("\tt := q.clock.AfterFunc(", "\tq.mu.Lock()\n\tt := q.clock.AfterFunc("),
      ("\t})\n\n\tq.mu.Lock()\n\tlive := q.timers[e]\n", "\t})\n\n\tlive := q.timers[e]\n")]),
    ("a retry adds what another now waits for", QU,
     [("f != nil && f.waits == e {", "f != nil {")]),
    ("a retry that fired is still waited for", QU,
     [("\t\t\t\t\tf.waits = nil\n", "")]),
    ("a retry adds what was settled", QU,
     [("\t\tif e.retry {\n", "\t\tif false && e.retry {\n")]),
    ("a timer taken is added", QU,
     [("\t\tif !q.timers[e] {\n", "\t\tif false && !q.timers[e] {\n")]),
    ("ShutDown keeps the failures", QU,
     [("\tclear(q.failing)\n", "")]),
    ("a dropped batch's failures are remembered", WL,
     [("\t\ts.queue.Settle(k, ids...) // dropped", "\t\t// dropped")]),
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
    ("the grace's end waits for the backoff", WL,
     [("\tfor _, a := range res.Asked {\n", "\tfor _, a := range res.Asked {\n\t\tif len(errs) > 0 {\n\t\t\tbreak\n\t\t}\n")]),
    ("the grace's end covers what failed", WL,
     [("\tat := map[time.Time][]ID{} // what", "\tat := map[time.Time][]ID{}\n\tif len(res.Asked) > 0 {\n\t\tat[res.RequeueAt] = res.Failed\n\t}\n\t// what")]),
    ("the grace's end covers what didn't ask", WL,
     [("\tat := map[time.Time][]ID{} // what", "\tat := map[time.Time][]ID{}\n\tif len(res.Asked) > 0 {\n\t\tat[res.RequeueAt] = decided\n\t}\n\t// what")]),
    ("a time asked for again sets another timer", QU,
     [("!q.asked[f] {\n", "true || !q.asked[f] {\n")]),
    ("a time is another object's", QU,
     [("askedFor{k, id, e.at}); !q", "askedFor{k, ID{}, e.at}); !q")]),
    ("a time is another time's", QU,
     [("askedFor{k, id, e.at}); !q", "askedFor{k, id, 0}); !q")]),
    ("a time is another key's", QU,
     [("askedFor{k, id, e.at}); !q", "askedFor{batchKey{}, id, e.at}); !q")]),
    ("a time that came is remembered", QU,
     [("\t\t\t\tdelete(q.asked, askedFor{k, id, e.at})\n", "\t\t\t\t_ = id\n")]),
    ("ShutDown remembers the times", QU,
     [("\tclear(q.asked)\n", "")]),
    ("an AddAt after ShutDown sets a timer", QU,
     [("\te := &queueTimer{at: at.UnixNano()}\n\tq.mu.Lock()\n\tif q.stopped {\n", "\te := &queueTimer{at: at.UnixNano()}\n\tq.mu.Lock()\n\tif false && q.stopped {\n")]),
    ("a time that is past waits", QU,
     [("\tif d <= 0 {\n\t\tq.Add(k, ids...)\n\t\treturn\n\t}\n\te := &queueTimer{at:", "\tif d < -time.Hour {\n\t\tq.Add(k, ids...)\n\t\treturn\n\t}\n\te := &queueTimer{at:")]),
    ("a time no object asks for is the namespace's", QU,
     [("\tif len(ids) == 0 {\n\t\treturn\n\t}\n\td := at.Sub", "\tif len(ids) == 0 {\n\t\tq.Add(k)\n\t\treturn\n\t}\n\td := at.Sub")]),
    ("every object that asked is looked at at the soonest time", WL,
     [("\t\tat[a.At] = append(at[a.At], a.ID)\n", "\t\tat[res.RequeueAt] = append(at[res.RequeueAt], a.ID)\n")]),
    ("one object of those that ask at a time is looked at", WL,
     [("\t\tat[a.At] = append(at[a.At], a.ID)\n", "\t\tat[a.At] = []ID{a.ID}\n")]),
    ("what asked isn't recorded", RL,
     [("\tr.Asked = append(r.Asked, Asked{id, at})\n", "\t_ = id\n")]),
    ("a failed list of the namespace stops its batch", WL,
     [("\t\t\tfailed[fillAll] = true\n\t\t}\n", "\t\t\tfailed[fillAll] = true\n\t\t\tids, held = nil, nil\n\t\t}\n")]),
    ("a failed list of the namespace isn't retried", WL,
     [("\t\t\tfailed[fillAll] = true\n", "")]),
    ("a failed list of the namespace is settled", WL,
     [("\tif fill && !failed[fillAll] {\n", "\tif fill {\n")]),
    ("a namespace's list that succeeds isn't settled", WL,
     [("\tif fill && !failed[fillAll] {\n", "\tif false && fill && !failed[fillAll] {\n")]),
    ("what was decided isn't settled", WL,
     [("\ts.queue.Settle(k, decided...)\n", "")]),
    ("what failed is settled", WL,
     [("\ts.queue.Settle(k, decided...)\n", "\ts.queue.Settle(k, ids...)\n")]),
    ("a failed prune isn't retried for its objects", WL,
     [("\t\t\tfor _, id := range bare {\n\t\t\t\tfailed[id] = true\n\t\t\t}\n", "")]),
    ("what failed is counted as decided", WL,
     [("\tt.keptUncached(decided, res.Kept)\n", "\tt.keptUncached(ids, res.Kept)\n")]),
    ("heldIn returns nothing after an error", WL,
     [("\t\tout, err = s.liveHeldIn(ctx, t, n)\n", "\t\tif out, err = s.liveHeldIn(ctx, t, n); err != nil {\n\t\t\treturn nil, err\n\t\t}\n")]),
    ("a stopped batch waits for its backoff", WL,
     [("\tif ctx.Err() != nil {\n\t\tif len(ids) > 0 {\n\t\t\ts.queue.Add(k, ids...)\n\t\t}\n\t\treturn\n\t}\n\tretryIn", "\tretryIn")]),
    ("a stopped batch that failed at nothing asks for its namespace", WL,
     [("\t\tif len(ids) > 0 {\n\t\t\ts.queue.Add(k, ids...)\n\t\t}\n", "\t\ts.queue.Add(k, ids...)\n")]),
    ("a failure of nothing says it is retried", WL,
     [("\t\tretryIn = \"never\"\n", "")]),
    ("a batch's log has every error", WL,
     [("\tif len(lines) > maxLoggedErrors {\n", "\tif false && len(lines) > maxLoggedErrors {\n")]),
    ("a batch's log doesn't say how many more", WL,
     [("\treturn text + more\n", "\t_ = more\n\treturn text\n")]),
    ("a long error isn't cut", WL,
     [("\tif len(text) > maxLoggedBytes {\n", "\tif false && len(text) > maxLoggedBytes {\n")]),
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

ONE = {C: "TestFailKeepsOneRetryPerObject|TestBatchQueueEveryOrdering"}
GRACE = {C: "TestAGracesTimersDontAddUp|TestBatchQueueEveryOrdering|TestATimeIsItsObjects"}
SAYS = {C: "TestReleaseSaysWhatItCouldntDecide"}
ALONE = {C: "TestAFailingObjectIsRetriedAlone"}
KIND = {C: "TestAKindThatCantBeListedStopsNoOther"}
IDX = {C: "TestTheIndexsRecheckRetriesWhatFailed"}
KILLERS = {
    "an object that waits gets another retry": ONE,
    "a run an event queued is counted": ONE,
    "the backoff doesn't grow": ONE,
    "the backoff is the key's": ONE,
    "every failure gets the base delay's timer": ONE,
    "the delay returned isn't the soonest": ONE,
    "a Fail after ShutDown sets a timer": ONE,
    "a settled object is remembered": ONE,
    "a Fail of nothing remembers its key": {C: "TestFailOfNothing"},
    "a timer nothing waits for stays": ONE,
    "a timer others wait for goes": ONE,
    "a timer nothing waits for isn't stopped": ONE,
    "Settle stops timers under the lock": ONE,
    "the clock is asked for a timer under the lock": ONE,
    "a retry adds what another now waits for": ONE,
    "a retry that fired is still waited for": ONE,
    "a retry adds what was settled": ONE,
    "ShutDown keeps the failures": ONE,
    "a key whose objects are all settled is remembered": {C: "TestADroppedBatchForgetsItsFailures"},
    "a timer taken is added": {C: "TestFailKeepsOneRetryPerObject|TestBatchQueueEveryOrdering|TestFailBetweenItsTwoSteps|TestFailUnderStress"},
    "a dropped batch's failures are remembered": {C: "TestADroppedBatchForgetsItsFailures"},
    "R1's failed read isn't reported": SAYS,
    "a rejected release isn't reported": SAYS,
    "a failed repair of the epoch isn't reported": SAYS,
    "a failed repair of the epoch is looked at again at once too": SAYS,
    "a Namespace that can't be read isn't reported": SAYS,
    "a holder list that fails isn't reported": SAYS,
    "a failed release isn't reported": SAYS,
    "an object that kept changing isn't reported": SAYS,
    "the whole batch is retried": ALONE,
    "the grace's end waits for the backoff": ALONE,
    "the grace's end covers what failed": {C: "TestAFailingObjectIsRetriedAlone|TestAGracesEndBesideAPermanentFailure"},
    "a failed list of the namespace stops its batch": KIND,
    "a failed list of the namespace isn't retried": KIND,
    "a failed list of the namespace is settled": KIND,
    "a namespace's list that succeeds isn't settled": KIND,
    "what was decided isn't settled": ALONE,
    "what failed is settled": {C: "TestAFailingObjectIsRetriedAlone|TestEventsDontLengthenTheBackoff|TestAnHonestFailureBesideAPermanentOne"},
    "a failed prune isn't retried for its objects": {C: "TestAFailedPruneIsRetriedAlone"},
    "the grace's end covers what didn't ask": {C: "TestOnlyWhatAskedIsLookedAtAgain"},
    "a time asked for again sets another timer": GRACE,
    "a time is another object's": GRACE,
    "a time is another time's": GRACE,
    "a time is another key's": GRACE,
    "a time that came is remembered": GRACE,
    "ShutDown remembers the times": GRACE,
    "an AddAt after ShutDown sets a timer": GRACE,
    "a time that is past waits": GRACE,
    "a time no object asks for is the namespace's": GRACE,
    "every object that asked is looked at at the soonest time": {C: "TestOnlyWhatAskedIsLookedAtAgain"},
    "one object of those that ask at a time is looked at": {C: "TestOnlyWhatAskedIsLookedAtAgain"},
    "what asked isn't recorded": {C: "TestOnlyWhatAskedIsLookedAtAgain"},
    "a stopped batch that failed at nothing asks for its namespace": {C: "TestABatchThatFailsAtNothing"},
    "a failure of nothing says it is retried": {C: "TestABatchThatFailsAtNothing"},
    "a stopped batch waits for its backoff": {C: "TestAStoppedBatchLeavesWhatFailed"},
    "a batch's log has every error": {C: "TestAFailedBatchsLogIsBounded"},
    "a batch's log doesn't say how many more": {C: "TestAFailedBatchsLogIsBounded"},
    "a long error isn't cut": {C: "TestAFailedBatchsLogIsBounded"},
    "what failed is counted as decided": {C: "TestAFailingObjectStaysCounted"},
    "heldIn returns nothing after an error": KIND,
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
