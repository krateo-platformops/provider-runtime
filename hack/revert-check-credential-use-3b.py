"""Revert check for pkg/credentialuse, PR 3b of
docs/plan-credential-use-implementation.md (the write loops: the release
loop and its rescan, the label janitor, the missing-ref prober, §12's
waiting gauge and events, and the namespace-restricted caches): break each
rule the PR adds in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant runs the tests named for it in KILLERS, and the baseline runs
them all; some are envtests, so KUBEBUILDER_ASSETS must be set (CI=1 makes an
envtest fail rather than skip without it).

Usage:
  python3 hack/revert-check-credential-use-3b.py > hack/revert-check-credential-use-3b.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["writeloops.go", "janitor.go", "prober.go", "tracker.go", "informers.go", "shared.go", "listwatch.go", "release.go", "acquire.go", "uncached.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # writeloops.go: RunWriteLoops and the release loop (§7.5, §6.2).
    ("the write loops don't wait for the informers' sync", "writeloops.go",
     [("\tselect {\n\tcase <-s.syncedCh:\n\tcase <-s.runDone:\n\t\treturn errInformersEnded\n\tcase <-ctx.Done():\n\t\treturn nil\n\t}\n\ts.rescan()",
       "\ts.rescan()")]),
    ("the write loops don't rescan", "writeloops.go",
     [("\ts.rescan()\n\n\tctx, cancel", "\n\tctx, cancel")]),
    ("a batch of an untracked kind is worked with another tracker", "writeloops.go",
     [("\tt := s.trackerOrPark(k, ids)\n\tif t == nil {\n\t\treturn\n\t}",
       "\tt := s.trackerOrPark(k, ids)\n\tif t == nil {\n\t\tif all := s.trackerList(); len(all) > 0 {\n\t\t\tt = all[0]\n\t\t} else {\n\t\t\treturn\n\t\t}\n\t}")]),
    ("an empty batch isn't filled", "writeloops.go",
     [("\tif len(ids) == 0 {\n\t\tvar err error\n\t\tids, err = s.heldIn(ctx, t, k.Namespace)", "\tif false {\n\t\tvar err error\n\t\tids, err = s.heldIn(ctx, t, k.Namespace)")]),
    ("RequeueAt isn't honoured", "writeloops.go",
     [("\tif !res.RequeueAt.IsZero() {\n\t\ts.queue.AddAfter(", "\tif false {\n\t\ts.queue.AddAfter(")]),
    ("a failed batch isn't retried", "writeloops.go",
     [("\ts.queue.AddAfter(k, d, ids...)\n}", "\t_ = d\n}")]),
    ("the backoff doesn't double", "writeloops.go",
     [("\t\td *= 2\n", "\t\td *= 1\n")]),
    ("an interrupted batch waits a backoff", "writeloops.go",
     [("\tif ctx.Err() != nil {\n\t\ts.queue.Add(k, ids...)\n\t\treturn\n\t}\n\td := s.backoff.fail(k)", "\td := s.backoff.fail(k)")]),
    # writeloops.go: §12's events.
    ("no WaitingForCredentialUsers", "writeloops.go",
     [("\t\ts.events.record(s.events.regarding(kp.P), \"Normal\", EventWaitingForUsers, note)\n", "\t\t_ = note\n")]),
    ("no CredentialDeleting", "writeloops.go",
     [("\t\t\ts.events.record(s.events.regarding(holder), \"Warning\", EventProtectedDeleting,", "\t\t\t_, _ = holder, fmt.Sprint(")]),
    ("CredentialDeleting warns the deleting users too", "writeloops.go",
     [("\t\t\tif deleting[u] {\n\t\t\t\tcontinue\n\t\t\t}\n", "")]),
    # janitor.go: the label janitor (§10.1, §6.2 L1).
    ("the janitor cleans nothing", "janitor.go",
     [("\t\tif err := s.clean(ctx, id); err != nil {", "\t\tif err := error(nil); err != nil {")]),
    ("the informers don't feed the janitor", "janitor.go",
     [("\tif h.s.needsJanitor(m) {", "\tif false && h.s.needsJanitor(m) {")]),
    ("the rescan doesn't feed the janitor", "writeloops.go",
     [("\t\t\tif s.needsJanitor(m) {\n\t\t\t\ts.janitor.Add(id)", "\t\t\tif false {\n\t\t\t\ts.janitor.Add(id)")]),
    ("the janitor's rejection isn't detected", "janitor.go",
     [("\t\tif rv, ok := s.janitorState.built(id); ok && rv == m.ResourceVersion {", "\t\tif rv, ok := s.janitorState.built(id); false && ok && rv == m.ResourceVersion {")]),
    # prober.go and tracker.go: the missing-ref prober (§10.3).
    ("Missing refs aren't recorded", "tracker.go",
     [("\tt.missing.set(holder, missing, t.s.clock.Now(), t.o.ProbeInterval)", "\tt.missing.set(holder, nil, t.s.clock.Now(), t.o.ProbeInterval)")]),
    ("a ref still missing isn't backed off", "prober.go",
     [("\t\tr.backoff = min(2*r.backoff, probeBackoffMax)", "\t\tr.backoff = min(1*r.backoff, probeBackoffMax)")]),
    ("a pass that finds a ref Missing doesn't reset its backoff", "prober.go",
     [("\t\tr.holders[holder] = true\n\t\tr.backoff, r.due = base, now.Add(base)",
       "\t\tr.holders[holder] = true\n\t\tif r.backoff == 0 {\n\t\t\tr.backoff, r.due = base, now.Add(base)\n\t\t}")]),
    ("many names don't take one LIST", "prober.go",
     [("\t\tif len(ids) > probeGetLimit {", "\t\tif false && len(ids) > probeGetLimit {")]),
    ("a LIST that RBAC refuses doesn't fall back to GETs", "prober.go",
     [("\t\t\tcase Classify(err) == ClassRBAC:\n", "\t\t\tcase false:\n")]),
    ("a found ref doesn't enqueue its holders", "prober.go",
     [("\t\t\t\tt.enqueue(h)\n", "\t\t\t\t_ = h\n")]),
    ("the prober never runs", "prober.go",
     [("\t\t\tfor _, t := range s.trackerList() {\n\t\t\t\tt.probe(ctx)", "\t\t\tfor _, t := range s.trackerList()[:0] {\n\t\t\t\tt.probe(ctx)")]),
    # tracker.go and informers.go: the waiting gauge (§12).
    ("the waiting gauge isn't reported", "tracker.go",
     [("\tsinkMetrics{t}.WaitingObjects(kindLabel(t.gk), waiting)\n", "\t_ = waiting\n")]),
    ("the waiting gauge counts objects that aren't being deleted", "informers.go",
     [("\tif m.DeletionTimestamp != nil && len(held) > 0 {", "\tif len(held) > 0 {")]),
    # shared.go, listwatch.go, tracker.go, acquire.go: namespace-restricted
    # caches (§10.5).
    ("Namespaces doesn't restrict the protected informers", "shared.go",
     [("\tif len(s.o.Namespaces) > 0 && m.Scope.Name() == meta.RESTScopeNameNamespace {", "\tif false && len(s.o.Namespaces) > 0 && m.Scope.Name() == meta.RESTScopeNameNamespace {")]),
    ("a namespace's informer watches every namespace", "listwatch.go",
     [("\tif namespace != \"\" {\n\t\tclient = md.Resource(gvr).Namespace(namespace)\n\t}\n", "")]),
    ("the access check ignores the namespace", "shared.go",
     [("\"resource\": gvr.Resource, \"namespace\": namespace,", "\"resource\": gvr.Resource,")]),
    ("a deleting object outside the caches isn't enqueued", "tracker.go",
     [("\tif !t.s.cached(id) {\n\t\tt.s.queue.Add(batchKey{t.gk, namespace}, id)", "\tif false && !t.s.cached(id) {\n\t\tt.s.queue.Add(batchKey{t.gk, namespace}, id)")]),
    ("an A2 that holds a deleting object doesn't report it", "acquire.go",
     [("\tif p.heldDeleting != nil && out != nil && out.DeletionTimestamp != nil {", "\tif false {")]),
    # shared.go: Run runs both halves.
    ("Run doesn't run the write loops", "shared.go",
     [("\tgo func() { writes <- s.RunWriteLoops(ctx) }()", "\tgo func() { <-ctx.Done(); writes <- nil }()")]),
    # release.go: what a batch keeps (§12).
    ("R0's keep isn't reported", "release.go",
     [("\t\t\tres.Kept = append(res.Kept, Kept{P: id, Why: KeptUserDeleting, Users: u.users, DeletingUsers: u.deletingUsers})\n", "")]),
    ("R1's grace isn't reported", "release.go",
     [("\t\t\t\t\tres.Kept = append(res.Kept, Kept{P: id, Why: KeptGrace, GraceEnd: r.graceEnd, Users: u.users, DeletingUsers: u.deletingUsers})\n", "")]),
    ("R2's namespace row is reported as a deleting user", "release.go",
     [("\t\t\t\t\tif in.NamespaceDeleting {\n\t\t\t\t\t\tkept.Why = KeptNamespaceDeleting\n\t\t\t\t\t}\n", "")]),
    # The review of #114, defect 1: a batch of a kind being tracked waits for its Track.
    ("a batch of a kind being tracked is dropped", "shared.go",
     [("\tif in := s.tracking[k.Holder]; in != nil {\n\t\tin.parked = append(in.parked, parkedBatch{key: k, ids: append([]ID(nil), ids...)})\n\t}\n", "")]),
    ("a parked batch isn't requeued when its Track succeeds", "shared.go",
     [("\t\tfor _, b := range parked {\n\t\t\ts.queue.Add(b.key, b.ids...)\n\t\t}\n", "\t\t_ = parked\n")]),
    # §10.5, the user's decision of 2026-11-19: held objects outside the caches.
    ("the write loops' start doesn't recheck outside the caches", "writeloops.go",
     [("\t\ts.recheckUncached(ctx, trackers)\n", "\t\t_ = trackers\n")]),
    ("a Track after the write loops start doesn't recheck outside the caches", "shared.go",
     [("\t\t\t\ts.recheckUncached(wctx, []*Tracker{t})\n", "")]),
    ("an uncached namespace's deletion enqueues nothing", "informers.go",
     [("\t\th.s.uncachedDeleting(n.Name)\n", "")]),
    ("an empty batch outside the caches isn't filled live", "writeloops.go",
     [("\tif !s.namespaceCached(n) {\n\t\tlive, err := s.liveHeldIn(ctx, t, n)", "\tif false {\n\t\tlive, err := s.liveHeldIn(ctx, t, n)")]),
    ("a restricted cache doesn't need a cluster-wide list", "shared.go",
     [("clusterList: p.ns != \"\"}", "clusterList: false}")]),
    ("the waiting gauge misses kept objects outside the caches", "tracker.go",
     [("\twaiting += len(t.waitingUncached)\n", "")]),
    # The review of #114's gaps: the prober's drops and retry, the janitor's retries, the marker sweep.
    ("the prober keeps a deleted holder's refs", "informers.go",
     [("\th.t.missing.set(types.NamespacedName{Namespace: m.Namespace, Name: m.Name}, nil, h.now(), h.t.o.ProbeInterval)\n", "")]),
    ("the prober keeps a forgotten holder's refs", "tracker.go",
     [("\tt.missing.set(types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, nil, t.s.clock.Now(), t.o.ProbeInterval)\n\treturn t.proto.forget(obj)", "\treturn t.proto.forget(obj)")]),
    ("a LIST that fails otherwise isn't put back on its backoff", "prober.go",
     [("\t\t\tdefault:\n\t\t\t\tfor _, id := range ids {\n\t\t\t\t\tt.missing.retry(id, now)\n\t\t\t\t}\n\t\t\t\tcontinue", "\t\t\tdefault:\n\t\t\t\tcontinue")]),
    ("the janitor gives up on a changed object", "janitor.go",
     [("\t\t\t\ts.janitorState.build(id, m.ResourceVersion)\n\t\t\t\tchanged = true", "\t\t\t\ts.janitorState.build(id, m.ResourceVersion)")]),
    ("the janitor doesn't retry a failure", "janitor.go",
     [("\t\t\tq.AddRateLimited(id)\n", "")]),
    ("objects that kept only a marker aren't swept", "uncached.go",
     [("\tif err := s.sweepMarkers(ctx, ts); err != nil {\n\t\treturn err\n\t}\n", "")]),
]

EQUIVALENT = {}

# The tests that must kill each mutant. A run of every 3b test per mutant
# would cost about 66 CPU-minutes, past the local budget; each mutant runs
# the tests aimed at its rule instead, and the baseline runs them all. The
# stress test isn't one: it is a broad net, not aimed at a rule.
KILLERS = {
    "the write loops don't wait for the informers' sync": 'TestWriteLoopsWaitForTheInformers',
    "the write loops don't rescan": 'TestWriteLoopsRescan',
    'a batch of an untracked kind is worked with another tracker': 'TestReleaseLoopDropsBatchesOfUntrackedKinds',
    "an empty batch isn't filled": 'TestReleaseBatchFillsAnEmptyBatch',
    "RequeueAt isn't honoured": 'TestReleaseLoopRequeuesAtTheGracesEnd|TestReleaseLoopKeepsForTheGraceAndReports',
    "a failed batch isn't retried": 'TestReleaseLoopBacksOff',
    "the backoff doesn't double": 'TestReleaseLoopBacksOff',
    'an interrupted batch waits a backoff': 'TestReleaseLoopKeepsAnInterruptedBatch',
    'no WaitingForCredentialUsers': 'TestReleaseLoopKeepsForTheGraceAndReports|TestReportKeptWarnsTheLiveUsers|TestANamespaceDeletionJustAfterTheGrace',
    'no CredentialDeleting': 'TestReleaseLoopKeepsForTheGraceAndReports|TestReportKeptWarnsTheLiveUsers',
    'CredentialDeleting warns the deleting users too': 'TestReportKeptWarnsTheLiveUsers',
    'the janitor cleans nothing': 'TestJanitor',
    "the informers don't feed the janitor": 'TestJanitor',
    "the rescan doesn't feed the janitor": 'TestJanitor',
    "the janitor's rejection isn't detected": 'TestJanitor',
    "Missing refs aren't recorded": 'TestProber|TestU5AndTheGrace',
    "a ref still missing isn't backed off": 'TestProber',
    "a pass that finds a ref Missing doesn't reset its backoff": 'TestProber',
    "many names don't take one LIST": 'TestProber',
    "a LIST that RBAC refuses doesn't fall back to GETs": 'TestProber',
    "a found ref doesn't enqueue its holders": 'TestProber|TestU5AndTheGrace',
    'the prober never runs': 'TestProber|TestU5AndTheGrace',
    "the waiting gauge isn't reported": 'TestReleaseLoopKeepsForTheGraceAndReports|TestProtectedHandler',
    "the waiting gauge counts objects that aren't being deleted": 'TestProtectedHandler',
    "Namespaces doesn't restrict the protected informers": 'TestNamespaceRestrictedCaches|TestNamespaceRestrictedLiveRead',
    "a namespace's informer watches every namespace": 'TestNamespaceRestrictedCaches|TestNamespaceRestrictedLiveRead',
    'the access check ignores the namespace': 'TestNamespaceRestrictedLiveRead',
    "a deleting object outside the caches isn't enqueued": 'TestNamespaceRestrictedCaches',
    "an A2 that holds a deleting object doesn't report it": 'TestNamespaceRestrictedCaches',
    "Run doesn't run the write loops": 'TestNamespaceTeardown|TestU5AndTheGrace',
    "R0's keep isn't reported": 'TestReleaseReportsWhatItKeeps|TestAHolderDeletionBetweenR2AndR3',
    "R1's grace isn't reported": 'TestReleaseReportsWhatItKeeps|TestReleaseLoopKeepsForTheGraceAndReports',
    "R2's namespace row is reported as a deleting user": 'TestReleaseReportsWhatItKeeps|TestANamespaceDeletionJustAfterTheGrace',
    'a batch of a kind being tracked is dropped': 'TestReleaseLoopParksBatchesOfAKindBeingTracked',
    "a parked batch isn't requeued when its Track succeeds": 'TestReleaseLoopParksBatchesOfAKindBeingTracked',
    "the write loops' start doesn't recheck outside the caches": 'TestUncachedHoldsAreRecheckedWhenTheWriteLoopsStart',
    "a Track after the write loops start doesn't recheck outside the caches": 'TestATrackAfterTheWriteLoopsStartRechecksOutsideTheCaches',
    "an uncached namespace's deletion enqueues nothing": 'TestAnUncachedNamespaceDeletionReleasesItsHolds',
    "an empty batch outside the caches isn't filled live": 'TestAnUncachedNamespaceDeletionReleasesItsHolds',
    "a restricted cache doesn't need a cluster-wide list": 'TestRestrictedCachesNeedAClusterList',
    'the waiting gauge misses kept objects outside the caches': 'TestTheWaitingGaugeCountsKeptObjectsOutsideTheCaches',
    "the prober keeps a deleted holder's refs": 'TestProber',
    "the prober keeps a forgotten holder's refs": 'TestProber',
    "a LIST that fails otherwise isn't put back on its backoff": 'TestProber',
    'the janitor gives up on a changed object': 'TestJanitor',
    "the janitor doesn't retry a failure": 'TestJanitor',
    "objects that kept only a marker aren't swept": 'TestJanitor',
}

ALL = sorted({t for ts in KILLERS.values() for t in ts.split("|")})


def anchored(tests):
    return "^(" + tests + ")$"


class Run:
    def __init__(self, returncode, stdout, stderr):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "900"))


def go_test(tests):
    env = dict(os.environ, CI="1")
    r = subprocess.run(["go", "test", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", anchored(tests), "./" + PKG],
                       cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
    return Run(r.returncode, r.stdout, r.stderr)


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, PKG, f), "rb").read())
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


if not os.environ.get("KUBEBUILDER_ASSETS"):
    sys.exit("KUBEBUILDER_ASSETS is not set: the envtests need it")
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
    if state == "SURVIVED" and name in EQUIVALENT:
        state, detail = "EQUIVALENT", EQUIVALENT[name]
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}\n          ran: {KILLERS[name]}\n          mutant {num}: {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {run}" + (f" (of {len(MUTANTS)})" if ONLY else ""))
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
