"""Revert check for pkg/credentialuse, PR 3a of
docs/plan-credential-use-implementation.md (the core's client-go runtime: the
server, the ListerWatcher, the informers, Shared, Track and the published
settings): break each rule the PR adds in turn and see whether a test
notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

It runs PR 3a's unit tests and its envtests, so KUBEBUILDER_ASSETS must be
set (CI=1 makes an envtest fail rather than skip without it).

Usage:
  python3 hack/revert-check-credential-use-3a.py > hack/revert-check-credential-use-3a.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["listwatch.go", "informers.go", "shared.go", "setup.go", "tracker.go", "queue.go", "acquire.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # listwatch.go: the relist floor (§6.5 L2, A20), and its order (G1 of the
    # review of #109).
    ("a List's completion isn't recorded", "listwatch.go",
     [("\t\tl.completed()\n\t}\n\treturn out, nil", "\t}\n\treturn out, nil")]),
    ("a watch-list stream's completion isn't recorded", "listwatch.go",
     [("\t\t\t\tw.lw.completed()\n", "")]),
    ("the bookmark is recorded after it is passed on", "listwatch.go",
     [("\t\t\tif w.initial && ev.Type == watch.Bookmark && isInitialEventsEnd(ev.Object) {\n\t\t\t\tw.lw.completed()\n\t\t\t}\n"
       "\t\t\tselect {\n\t\t\tcase w.out <- ev:\n\t\t\tcase <-w.done:\n\t\t\t\treturn\n\t\t\t}\n",
       "\t\t\tselect {\n\t\t\tcase w.out <- ev:\n\t\t\tcase <-w.done:\n\t\t\t\treturn\n\t\t\t}\n"
       "\t\t\tif w.initial && ev.Type == watch.Bookmark && isInitialEventsEnd(ev.Object) {\n\t\t\t\tw.lw.completed()\n\t\t\t}\n")]),
    ("a List's completion is recorded at the next Watch", "listwatch.go",
     [("\t\tl.completed()\n\t}\n\treturn out, nil", "\t}\n\treturn out, nil"),
      ("func (l *listWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {\n",
       "func (l *listWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {\n"
       "\tif options.SendInitialEvents == nil || !*options.SendInitialEvents {\n\t\tl.completed()\n\t}\n")]),
    ("the holder informer's lists don't reach the verified set", "tracker.go",
     [("\tt.ver.listCompleted(at)\n", "")]),
    # informers.go: the holder and Namespace handlers (§6.3, §10.2, §11.3).
    ("a holder update doesn't reach the verified set", "informers.go",
     [("\th.t.ver.holderEvent(n.UID, n, at)\n", "")]),
    ("the Namespace handler acts on every change", "informers.go",
     [("\tif !newlyDeleting && (o.DeletionTimestamp == nil) == (n.DeletionTimestamp == nil) &&\n\t\tmaps.Equal(o.Annotations, n.Annotations) {\n\t\treturn\n\t}\n",
       "\t_ = maps.Equal(o.Annotations, n.Annotations)\n")]),
    ("a namespace's deletion doesn't enqueue its release batches", "informers.go",
     [("\t\t\t\tif newlyDeleting {\n\t\t\t\t\tfor ns := range holds(t, m) {", "\t\t\t\tif false {\n\t\t\t\t\tfor ns := range holds(t, m) {")]),
    ("the Namespace handler doesn't enqueue holders", "informers.go",
     [("\t\t\t\tt.enqueueHolders(id)\n\t\t\t\tif newlyDeleting {", "\t\t\t\tif newlyDeleting {")]),
    # informers.go: the protected handler (G2, G4, G5, G6, N3, N5).
    ("a protected add enqueues nothing", "informers.go",
     [("\t\th.s.queue.Add(batchKey{h.t.gk, n}, id)\n", "\t\t_ = n\n")]),
    ("a protected update enqueues nothing", "informers.go",
     [("newlyDeleting || replaced {\n\t\t\th.s.queue.Add(batchKey{t.gk, ns}, id)\n", "newlyDeleting || replaced {\n")]),
    ("a protected update doesn't reach the verified set", "informers.go",
     [("\tt.ver.protectedEvent(o, n)\n", "")]),
    ("a deletion's grace sets no timer", "informers.go",
     [("\t\th.s.queue.AddAfter(batchKey{h.t.gk, n}, end.Sub(now), id)\n", "\t\t_, _, _ = end, id, n\n")]),
    ("a deleting object under a new UID gets no grace timer", "informers.go",
     [("\tif n.DeletionTimestamp != nil && (newlyDeleting || replaced) {", "\tif n.DeletionTimestamp != nil && newlyDeleting {")]),
    ("losing a finalizer doesn't enqueue the holders", "informers.go",
     [("\t\tlost = lost || (inB && (!inA || replaced))\n", "")]),
    ("a protected object leaving the cache doesn't enqueue its holders", "informers.go",
     [("\tt.enqueueHolders(id)\n\t// The object left the cache", "\t// The object left the cache")]),
    ("a deleted object's first sight is kept", "informers.go",
     [("\th.s.seen.forget(m.UID)\n\th.count(-len(held))", "\th.count(-len(held))")]),
    ("a replaced object's first sight is kept", "informers.go",
     [("\t\th.s.seen.forget(o.UID)\n", "")]),
    ("the held gauge counts objects, not pairs", "informers.go",
     [("\th.count(len(held))\n", "\th.count(min(len(held), 1))\n")]),
    ("an unparseable record's holder isn't enqueued", "informers.go",
     [("idx := t.holders.GetIndexer()\n\tfor _, key := range []string{id.String(), unparseableKey} {",
       "idx := t.holders.GetIndexer()\n\tfor _, key := range []string{id.String()} {")]),
    ("the held gauge isn't reported", "tracker.go",
     [("\tsinkMetrics{t}.HeldObjects(kindLabel(t.gk), sum)\n", "\t_ = sum\n")]),
    # shared.go: Run's checks and shutdown (§7.3, §7.5; N2, G6).
    ("start doesn't check the namespace verbs", "shared.go",
     [("\tif err := s.canAccess(ctx, namespaceGVR, \"get\", \"list\", \"watch\"); err != nil {\n\t\treturn fmt.Errorf(\"%w (%w)\", errNamespaceSync, err)\n\t}\n", "")]),
    ("start doesn't check the informers' verbs", "shared.go",
     [("\tfor _, inf := range infs {\n\t\tif err := inf.preflight(ctx, s); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n", "")]),
    ("without a dynamic client the check passes", "shared.go",
     [("\tif s.c.Dynamic == nil {\n\t\treturn errors.New(\"credential-use: Clients.Dynamic is required\")\n\t}\n\tfor _, verb := range verbs {",
       "\tif s.c.Dynamic == nil {\n\t\treturn nil\n\t}\n\tfor _, verb := range verbs {")]),
    ("RunInformers leaves the queue running", "shared.go",
     [("\tdefer s.queue.ShutDown()\n", "")]),
    # shared.go: Track (§7.3; D2, D3, D4, and items 2–4 of the re-review).
    ("a Track doesn't wait for another of its kind", "shared.go",
     [("\t\tother := s.tracking[gk]\n", "\t\tvar other *inflight\n")]),
    ("Tracks of different kinds wait for each other", "shared.go",
     [("\t\tother := s.tracking[gk]\n", "\t\tvar other *inflight\n\t\tfor _, x := range s.tracking {\n\t\t\tother = x\n\t\t}\n")]),
    ("a Track doesn't stop when RunInformers ends", "shared.go",
     [("\t\tcase <-s.runDone:\n\t\t\tcancel()\n", "\t\tcase <-s.runDone:\n")]),
    ("a Track after Run doesn't check the holders' verbs", "shared.go",
     [("\tif err := t.informer().preflight(ctx, s); err != nil {\n\t\treturn err\n\t}\n\thctx", "\thctx")]),
    ("a Track after Run doesn't check a protected informer's verbs", "shared.go",
     [("\t\terr := p.informer().preflight(check, s)\n", "\t\tvar err error\n\t\t_ = check\n")]),
    ("nobody starts a protected informer created after Run", "shared.go",
     [("\tstarter := !p.starting\n", "\tstarter := false\n")]),
    ("a Track that didn't start a shared informer doesn't wait for it", "shared.go",
     [("\tif starter {\n\t\ts.startShared(ictx, p)\n\t}\n\treturn s.waitShared(ctx, p)",
       "\tif !starter {\n\t\treturn nil\n\t}\n\ts.startShared(ictx, p)\n\treturn s.waitShared(ctx, p)")]),
    ("a shared informer runs under its starter's budget", "shared.go",
     [("\tif starter {\n\t\ts.startShared(ictx, p)\n\t}\n", "\tif starter {\n\t\ts.startShared(ctx, p)\n\t}\n")]),
    ("a shared informer's forbidden list isn't its start error", "shared.go",
     [("\t\t\tp.fail(fmt.Errorf(\"%w: the protected informer of %s: %w\", errForbidden, p.gk, err))\n", "\t\t\t_ = err\n")]),
    ("an informer another Track claims is removed", "shared.go",
     [("\t\tif p.claims > 0 || p.runOwned {", "\t\tif p.runOwned {")]),
    ("a Track joins before its holder sync", "shared.go",
     [("\thctx, stop := context.WithCancel(ictx)\n\tt.stopHolders = stop\n"
       "\tif err := s.startAndSync(hctx, ctx, t.holders, \"the holder informer of \"+t.gk.String()); err != nil {\n\t\treturn err\n\t}\n"
       "\tfor _, p := range ps {\n\t\tif err := p.ensureStarted(ctx, ictx, s); err != nil {\n\t\t\treturn err\n\t\t}\n"
       "\t\tif err := t.join(p); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n",
       "\tfor _, p := range ps {\n\t\tif err := p.ensureStarted(ctx, ictx, s); err != nil {\n\t\t\treturn err\n\t\t}\n"
       "\t\tif err := t.join(p); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n"
       "\thctx, stop := context.WithCancel(ictx)\n\tt.stopHolders = stop\n"
       "\tif err := s.startAndSync(hctx, ctx, t.holders, \"the holder informer of \"+t.gk.String()); err != nil {\n\t\treturn err\n\t}\n")]),
    ("a cancelled Track is held against Run", "shared.go",
     [("\t\tif caller.Err() != nil {\n\t\t\treturn err\n\t\t}\n\t\treturn s.setupFailed(err)", "\t\treturn s.setupFailed(err)")]),
    ("a Track ignores its caller's ctx", "shared.go",
     [("\tctx, cancel := context.WithTimeout(parent, s.timeout())", "\tctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.timeout())")]),
    ("a 403 after a shared informer's sync is its start error", "shared.go",
     [("\t\tif apierrors.IsForbidden(err) && !p.inf.HasSynced() {", "\t\tif apierrors.IsForbidden(err) {")]),
    ("a failed Track keeps its claims", "shared.go",
     [("\t\tstops := s.releaseLocked(ps)\n", "\t\tvar stops []context.CancelFunc\n")]),
    ("an informer nobody claims isn't removed", "shared.go",
     [("\t\tif s.protected[p.gk] == p {\n\t\t\tdelete(s.protected, p.gk)\n\t\t}\n", "")]),
    ("a failed Track's handlers stay", "shared.go",
     [("\t\tt.stopInformers()\n\t\tt.leaveAll()\n", "\t\tt.stopInformers()\n")]),
    ("a failed Track's informers keep running", "shared.go",
     [("\t\tt.stopInformers()\n\t\tt.leaveAll()\n", "\t\tt.leaveAll()\n"),
      ("\t\tfor _, stop := range stops {\n\t\t\tstop()\n\t\t}\n", "\t\t_ = stops\n")]),
    ("a Track after Run doesn't wait for the replay", "shared.go",
     [("\tfor _, p := range ps {\n\t\tif err := s.waitSynced(ctx, t.regs[p].HasSynced,",
       "\tfor _, p := range ps[:0] {\n\t\tif err := s.waitSynced(ctx, t.regs[p].HasSynced,")]),
    # shared.go and tracker.go: equal Options, and the sinks (N1, the user's
    # decision of 2026-11-18).
    ("a second Track with other options returns the first tracker", "shared.go",
     [("\t\t\tif !equalOptions(t.o, o) {\n\t\t\t\treturn nil, fmt.Errorf(\"credential-use: %s is already tracked with other options\", gk)\n\t\t\t}\n\t\t\tt.rebind(o)",
       "\t\t\tt.rebind(o)")]),
    ("Protected is compared in order", "shared.go",
     [("\t\tsameSet(a.Protected, b.Protected) &&", "\t\tlen(a.Protected) == len(b.Protected) && (len(a.Protected) == 0 || a.Protected[0] == b.Protected[0]) &&")]),
    ("OwnerFinalizers are compared in order", "shared.go",
     [("\t\tsameSet(a.OwnerFinalizers, b.OwnerFinalizers) &&", "\t\tlen(a.OwnerFinalizers) == len(b.OwnerFinalizers) && (len(a.OwnerFinalizers) == 0 || a.OwnerFinalizers[0] == b.OwnerFinalizers[0]) &&")]),
    ("MaxDepth isn't compared", "shared.go",
     [("\t\ta.MaxDepth == b.MaxDepth && a.DeletionGrace == b.DeletionGrace &&", "\t\ta.DeletionGrace == b.DeletionGrace &&")]),
    ("ProbeInterval isn't compared", "shared.go",
     [("\t\ta.ProbeInterval == b.ProbeInterval && a.DisownInterval == b.DisownInterval &&", "\t\ta.DisownInterval == b.DisownInterval &&")]),
    ("a rebind keeps the old sinks", "tracker.go",
     [("\tt.sink = sinksOf(o)\n\tt.sinkMu.Unlock()", "\tt.sinkMu.Unlock()")]),
    ("a rebind doesn't wait for calls in flight", "tracker.go",
     [("\tt.sinkMu.RLock()\n\tdefer t.sinkMu.RUnlock()\n\tt.sink.enqueue(nn)", "\tt.sinkMu.RLock()\n\tf := t.sink.enqueue\n\tt.sinkMu.RUnlock()\n\tf(nn)")]),
    ("the new Metrics doesn't get the held gauge", "tracker.go",
     [("\tt.sinkMu.Unlock()\n\tt.reportHeld()", "\tt.sinkMu.Unlock()")]),
    ("Disown's bound isn't checked", "shared.go",
     [("\tif time.Duration(o.DisownPasses)*o.DisownInterval < 2*o.TimeoutBound {", "\tif false {")]),
    ("a refused Track doesn't stop Run", "shared.go",
     [("\t\ts.setupErr = errors.Join(s.setupErr, err)\n", "")]),
    # setup.go: the published settings (§7.3, L3, L4; G3).
    ("published settings that disagree are accepted", "setup.go",
     [("\t\tif d := disagreement(want, got); d != \"\" {", "\t\tif d := disagreement(want, got); false && d != \"\" {")]),
    ("the settings aren't published", "setup.go",
     [("\t\t_, err = crds.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})\n", "\t\t_, _ = types.MergePatchType, patch\n\t\terr = nil\n")]),
    ("the patch isn't guarded by the resourceVersion", "setup.go",
     [("\t\t\t\"resourceVersion\": crd.GetResourceVersion(),\n", "\t\t\t\"name\":            crd.GetName(),\n")]),
    ("every patch error is retried", "setup.go",
     [("\t\tif Classify(err) != ClassConflict {", "\t\tif false && Classify(err) != ClassConflict {")]),
    ("the attempts aren't bounded at five", "setup.go",
     [("\tfor range maxCRDWriteAttempts {", "\tfor range 2 * maxCRDWriteAttempts {")]),
    ("no read after the last conflict", "setup.go",
     [("\tif _, done, err := check(); err != nil || done {\n\t\treturn err\n\t}\n\to.Metrics.CRDWriteStarved", "\to.Metrics.CRDWriteStarved")]),
    ("a starved write isn't counted", "setup.go",
     [("\to.Metrics.CRDWriteStarved(kindLabel(gk))\n", "")]),
    # queue.go: buffering, timers, and the lock order with the clock (M1 of
    # the review of revision 16; D1 of the review of #109).
    ("the queue drops what a batch covered before", "queue.go",
     [("\tset := q.pending[k]\n\tif set == nil {", "\tset := map[ID]bool(nil)\n\tif set == nil {")]),
    ("a key added during work isn't queued again", "queue.go",
     [("\tif _, again := q.pending[k]; again && !q.queued[k] && !q.stopped {", "\tif false {")]),
    ("ShutDown leaves the timers running", "queue.go",
     [("\tfor _, t := range stop {\n\t\tt.Stop()\n\t}\n", "")]),
    ("AddAfter calls the clock under the queue's lock", "queue.go",
     [("\te := &queueTimer{}\n\tq.mu.Lock()\n\tif q.stopped {\n\t\tq.mu.Unlock()\n\t\treturn\n\t}\n\tq.timers[e] = true\n\tq.mu.Unlock()\n\n\tt := q.clock.AfterFunc(",
       "\te := &queueTimer{}\n\tq.mu.Lock()\n\tdefer q.mu.Unlock()\n\tif q.stopped {\n\t\treturn\n\t}\n\tq.timers[e] = true\n\te.t = q.clock.AfterFunc("),
      ("\t})\n\n\tq.mu.Lock()\n\tlive := q.timers[e]\n\tif live {\n\t\te.t = t\n\t}\n\tq.mu.Unlock()\n\tif !live {",
       "\t})\n\tif false {\n\t\tt := e.t")]),
    ("ShutDown stops the timers under the queue's lock", "queue.go",
     [("\tq.cond.Broadcast()\n\tq.mu.Unlock()\n\tfor _, t := range stop {\n\t\tt.Stop()\n\t}\n",
       "\tfor _, t := range stop {\n\t\tt.Stop()\n\t}\n\tq.cond.Broadcast()\n\tq.mu.Unlock()\n")]),
    # acquire.go: class (e) is counted (L3 of the review of revision 16).
    ("class (e) isn't counted", "acquire.go",
     [("\t\t\t\tp.reports.AdmissionDenied(p.holder)\n", "")]),
]

EQUIVALENT = {}

TESTS = ("^(TestRelistResetsRecordSince|TestTrackAfterStartAndTwice|TestPublishedSettings|TestRunNeedsNamespaceListAndWatch|"
         "TestNamespaceHandlerActsOnlyOnItsFields|TestStrippingEnqueuesTheHolders|TestDeletingSecretRefusesANewHolder|"
         "TestBatchQueue.*|TestAdmissionDenialIsCounted|TestPublishedSettingsCompareAsValues|TestTrackValidates|"
         "TestConcurrentTracksWaitForTheSync|TestFailedTrackLeavesNothing|TestTrackAfterRunRechecksTheSharedCache|"
         "TestProtectedHandler|TestRunInformersShutsTheQueueDown|TestRunInformersChecksAccess|TestPublishConfig|"
         "TestTrackOfOneKindDoesntWaitForAnother|TestTrackStopsWaitingWhenRunInformersFails|"
         "TestEqualOptionsComparesEveryOtherField|TestTrackRebindsTheSinks|TestRebindWaitsForCallsInFlight|"
         "TestSharedInformerWaitersKeepTheirOwnBudget|TestSharedInformerStartFailureReachesEveryTrack|TestTrackJoinsAfterItsHolderSync|"
         "TestTrackHonoursTheCallersContext|TestATransient403AfterSyncDoesntPoisonASharedInformer)$")


class Run:
    def __init__(self, returncode, stdout, stderr):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "900"))


def go_test():
    env = dict(os.environ, CI="1")
    r = subprocess.run(["go", "test", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", TESTS, "./" + PKG],
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
base = go_test()
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
        state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
        restore = None
    if state == "SURVIVED" and name in EQUIVALENT:
        state, detail = "EQUIVALENT", EQUIVALENT[name]
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}\n          mutant {num}: {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {run}" + (f" (of {len(MUTANTS)})" if ONLY else ""))
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
