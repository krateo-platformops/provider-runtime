"""Revert check for PR 7 of docs/plan-credential-use-implementation.md (the
provider-runtime adapter and the reconciler wiring): break each rule the PR
adds in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

PR 7 spans several packages, so each mutant names, in KILLERS, the tests to
run in each package: {package: tests}. The baseline runs every named test of
every package. pkg/credentialuse/e2e holds envtests with a manager, so
KUBEBUILDER_ASSETS must be set (CI=1 makes an envtest fail rather than skip
without it).

Usage:
  python3 hack/revert-check-credential-use-7.py > hack/revert-check-credential-use-7.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
R = "pkg/reconciler/"
RC = R + "credentialuse.go"
RR = R + "reconciler.go"
C = "pkg/credentialuse/"
CA = C + "adapter.go"
E = "pkg/credentialuse/e2e/"
RT = "pkg/reconciler/reconcilertest/"
T = "pkg/telemetry/"
FILES = [RC, RR, CA, "pkg/tracing/names.go", C + "tracker.go", C + "shared.go", C + "setup.go", C + "acquire.go", C + "informers.go", RT + "credentialuse.go", T + "credentialuse.go", "pkg/resource/api.go"]

ACQUIRE_BLOCK = """	// Credential-use's step 5 (design-credential-use-finalizer §8): hold what Connect will read,
	// on every pass past the Orphan check, deletion passes included, before the governor's gate,
	// so a deferred pass has already acquired. pristine is taken below, after it.
	// A holder being deleted that carries none of its owner finalizers has been finalized
	// already: it isn't a user (§6.1), so there is nothing to protect, and a record it wrote
	// would be ignored. Its pass skips Acquire (§8 step 5; the user's decision, 2026-11-22).
	if r.cu.active() && (!meta.WasDeleted(managed) || credentialuse.IsEstablished(managed, r.cu.owners)) {
		var res reconcile.Result
		var stop bool
		managed, externalCtx, res, stop, err = r.acquireCredentials(ctx, externalCtx, log, managed, pass, cup, writeStatus)
		if stop {
			return res, err
		}
	}
"""
GATE = "\trelease, deferral := r.gate(ctx, log, r.external.keyer, managed)\n"

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # §8 step 1 and M4: the source start check.
    ("no source check before the Get", RR,
     [("\tif err = r.cu.ready(r.kind); err != nil {", "\tif err = r.cu.ready(r.kind); false && err != nil {")]),
    ("an unstarted source doesn't fail the manager", RC,
     [("\t\terr := sourceNotStarted(kind)\n\t\tif c.wiring != nil {", "\t\terr := sourceNotStarted(kind)\n\t\tif false {")]),
    ("the timer never fails", RC,
     [("\t\tif !src.started.Load() && now.Sub(from) >= sourceStartTimeout {", "\t\tif false && !src.started.Load() && now.Sub(from) >= sourceStartTimeout {")]),
    # §7.5: setup errors refuse to start.
    ("a finalizer without a name is no setup error", RC,
     [("\t\tc.fail(fmt.Errorf(\"credential-use: %s: the Finalizer given", "\t\t_ = (fmt.Errorf(\"credential-use: %s: the Finalizer given")]),
    ("a Track error doesn't fail the manager", RC,
     [("\tt, err := s.Track(context.Background(), o)\n\tif err != nil {\n\t\tc.fail(err)", "\tt, err := s.Track(context.Background(), o)\n\tif err != nil {\n\t\t_ = err")]),
    ("APIFinalizer names no finalizer", "pkg/resource/api.go",
     [("func (a *APIFinalizer) FinalizerName() string { return a.finalizer }", "func (a *APIFinalizer) FinalizerName() string { return \"\" }")]),
    # §7.5: the registry, the runnables, the source.
    ("a Shared per Reconciler", RC,
     [("\t\ts = reg.defaults[m]\n", "\t\ts = nil\n"),
      ("\tif w := reg.wirings[s]; w != nil {", "\tif w := reg.wirings[s]; false && w != nil {")]),
    ("the informers need leader election", RC,
     [("func (informerRunnable) NeedLeaderElection() bool { return false }", "func (informerRunnable) NeedLeaderElection() bool { return true }")]),
    ("a source's Start doesn't start the informers", RC,
     [("\tif s.wiring != nil {\n\t\ts.wiring.startInformers()\n\t}\n\treturn nil", "\treturn nil")]),
    ("WaitForSync doesn't wait", RC,
     [("\tfor !t.HasSynced() {", "\tfor false {")]),
    ("Enqueue doesn't reach the queue", RC,
     [("\t\t(*q).Add(reconcile.Request{NamespacedName: nn})", "\t\t_ = q")]),
    ("HasSynced is always true", CA,
     [("\tif !t.holders.HasSynced() || !t.crd.HasSynced() {\n\t\treturn false\n\t}\n\tfor _, reg := range t.regs {\n\t\tif !reg.HasSynced() {\n\t\t\treturn false\n\t\t}\n\t}\n\treturn true",
       "\treturn true")]),
    # §8 steps 4 and 7: Forget in the finalizer's write, or on its own.
    ("no Forget on the orphan path", RR,
     [("\t\t// finalizer's write (design-credential-use-finalizer §8 step 4).\n\t\tstarted = time.Now()\n\t\terr = r.removeFinalizer(ctx, managed)",
       "\t\t// finalizer's write (design-credential-use-finalizer §8 step 4).\n\t\tstarted = time.Now()\n\t\terr = r.managed.RemoveFinalizer(ctx, managed)")]),
    ("no Forget on the delete path", RR,
     [("\t\tstarted = time.Now()\n\t\terr = r.removeFinalizer(ctx, managed)\n\t\tr.recordFinalizerRemove(ctx, started, err)\n\t\tif err != nil {\n\t\t\t// If this is the first time we encounter this issue we'll be\n\t\t\t// requeued implicitly when we update our status with the new error\n\t\t\t// condition. If not, we requeue explicitly, which will trigger\n\t\t\t// backoff.\n\t\t\tlog.Error(err, \"Cannot remove managed resource finalizer\")\n\t\t\tif kerrors.IsConflict(err) {\n\t\t\t\tpass.conflict(tracing.StepFinalizer)\n\t\t\t\treturn rateLimitedRequeue(), nil\n\t\t\t}\n\t\t\tmanaged.SetConditions(start.keep(prv1.Deleting()), prv1.ReconcileError(err))\n\t\t\tpassErr := errors.Wrap(err, errUpdateManagedStatus)\n\t\t\tif err := writeStatus(passErr); err != nil {\n\t\t\t\treturn reconcile.Result{}, err\n\t\t\t}\n\t\t\treturn reconcile.Result{}, passErr\n\t\t}\n\t\tpass.forget()\n\n\t\t// We've successfully deleted",
       "\t\tstarted = time.Now()\n\t\terr = r.managed.RemoveFinalizer(ctx, managed)\n\t\tr.recordFinalizerRemove(ctx, started, err)\n\t\tif err != nil {\n\t\t\t// If this is the first time we encounter this issue we'll be\n\t\t\t// requeued implicitly when we update our status with the new error\n\t\t\t// condition. If not, we requeue explicitly, which will trigger\n\t\t\t// backoff.\n\t\t\tlog.Error(err, \"Cannot remove managed resource finalizer\")\n\t\t\tif kerrors.IsConflict(err) {\n\t\t\t\tpass.conflict(tracing.StepFinalizer)\n\t\t\t\treturn rateLimitedRequeue(), nil\n\t\t\t}\n\t\t\tmanaged.SetConditions(start.keep(prv1.Deleting()), prv1.ReconcileError(err))\n\t\t\tpassErr := errors.Wrap(err, errUpdateManagedStatus)\n\t\t\tif err := writeStatus(passErr); err != nil {\n\t\t\t\treturn reconcile.Result{}, err\n\t\t\t}\n\t\t\treturn reconcile.Result{}, passErr\n\t\t}\n\t\tpass.forget()\n\n\t\t// We've successfully deleted")]),
    ("no standalone Forget", RC,
     [("\tif !r.cu.active() || !r.cu.tracker.Forget(mg) || credentialuse.IsEstablished(mg, r.cu.owners) {",
       "\tif !r.cu.active() || !r.cu.tracker.Forget(mg) || true {")]),
    # §8 step 5: Acquire.
    ("no Acquire", RR,
     [("\tif r.cu.active() && (!meta.WasDeleted(managed) || credentialuse.IsEstablished(managed, r.cu.owners)) {", "\tif false && (!meta.WasDeleted(managed) || credentialuse.IsEstablished(managed, r.cu.owners)) {")]),
    ("a finalized deleting holder acquires", RR,
     [("\tif r.cu.active() && (!meta.WasDeleted(managed) || credentialuse.IsEstablished(managed, r.cu.owners)) {", "\tif r.cu.active() {")]),
    ("pristine taken before Acquire", RR,
     [(ACQUIRE_BLOCK, ""), (GATE, ACQUIRE_BLOCK + GATE)]),
    ("a record conflict isn't requeued", RC,
     [("\tif errors.Is(err, credentialuse.ErrHolderConflict) {", "\tif false && errors.Is(err, credentialuse.ErrHolderConflict) {")]),
    ("a new holder that must not connect connects", RC,
     [("\tif a.MustNotConnect {", "\tif false {")]),
    ("no re-read after a webhook changed A1", RC,
     [("\tif a.Reread {", "\tif false {")]),
    ("the re-read ignores the live reader", RC,
     [("\t\tif r.apiReader != nil {\n\t\t\trd = r.apiReader", "\t\tif false {\n\t\t\trd = r.apiReader")]),
    ("no resolved chain on Connect's context", RC,
     [("\texternalCtx = credentialuse.WithResolved(externalCtx, a)\n", "")]),
    ("no holder on Connect's context", RC,
     [("\texternalCtx = withCredentialUseHolder(externalCtx, r.cu.tracker, managed)\n", "")]),
    # §8 step 8.
    ("no BeforeEstablish", RR,
     [("\tif r.cu.active() && !credentialuse.IsEstablished(managed, r.cu.owners) {", "\tif false && !credentialuse.IsEstablished(managed, r.cu.owners) {")]),
    # IP2: RetryAfter.
    ("RetryAfter is ignored", RC,
     [("\tif p.retryAfter <= 0 || err != nil || p.deferred || isRateLimitedRequeue(res) {", "\tif true {")]),
    ("RetryAfter cuts a deferral", RC,
     [("\tif p.retryAfter <= 0 || err != nil || p.deferred || isRateLimitedRequeue(res) {", "\tif p.retryAfter <= 0 || err != nil || isRateLimitedRequeue(res) {")]),
    # §12: the pointer to the unheld refs.
    ("no pointer on a Connect failure", RR,
     [("\t\tpassErr := r.withUnheldSuffix(errors.Wrap(err, errReconcileConnect), managed)", "\t\tpassErr := errors.Wrap(err, errReconcileConnect)")]),
    ("no pointer on an Observe failure", RR,
     [("\t\twerr = r.withUnheldSuffix(werr, managed)\n", "")]),
    ("the count isn't capped", RC,
     [("\tn := min(sum.Count, 9999)", "\tn := sum.Count")]),
    ("the reason isn't cut", RC,
     [("\tif len(reason) > unheldSuffixReasonMax {", "\tif false {")]),
    ("More isn't counted", CA,
     [("\t\t\ts.Count += e.Count\n", "\t\t\ts.Count++\n")]),
    ("the first entry is named, not the most recent", CA,
     [("\t\tif !found || e.Since.After(s.Since) {", "\t\tif !found {")]),
    # M6 and L8: the holder event filter.
    ("the predicate drops every update", RC,
     [("\t\treturn !t.IsOwnRecordWrite(e.ObjectOld, e.ObjectNew)", "\t\treturn false")]),
    ("any record write counts as this process's", CA,
     [("\tif last, ok := t.proto.written.get(newObj.GetUID()); !ok || last != after {", "\tif false {")]),
    ("other changes ride with the record write", CA,
     [("\treturn sameButRecord(oldObj, newObj)\n}", "\treturn true\n}")]),
    ("Forget keeps the record write", C + "acquire.go",
     [("\tp.written.forget(obj.GetUID())\n", "")]),
    ("a deleted holder's record write is kept", C + "informers.go",
     [("\th.t.proto.written.forget(m.UID)\n", "")]),
    # §11.9: unrecorded reads.
    ("CheckRead ignores the record", CA,
     [("\tif ReadRecord(holder).Names(id) {", "\tif false {")]),
    ("CheckRead ignores the refs", CA,
     [("\tif refs, err := t.o.Refs(holder); err == nil {", "\tif refs, err := t.o.Refs(holder); false && err == nil {")]),
    ("an unrecorded read isn't counted", CA,
     [("\tsinkMetrics{t}.UnrecordedReads(kindLabel(t.gk))\n", "")]),
    ("an unnamed read passes the cover check", RT + "credentialuse.go",
     [("\t\tif _, ok := named[id]; !ok {", "\t\tif _, ok := named[id]; false && !ok {")]),
    ("a ref that resolves to nothing passes the cover check", RT + "credentialuse.go",
     [("\t\t\tif kerrors.IsNotFound(err) {\n\t\t\t\tt.Errorf(", "\t\t\tif kerrors.IsNotFound(err) {\n\t\t\t\t_ = fmt.Sprintf(")]),
    # docs/design-tracing.md §4.5: the credentialuse.acquire event (the user's decision, 2026-11-22).
    ("no credentialuse.acquire event", RC,
     [("\tpass.event(tracing.EventCredentialUseAcquire, acquireAttributes(a, err)...)\n", "")]),
    ("the event drops the counts by reason", RC,
     [("\t\tbyReason[u.Reason]++\n", "\t\t_ = u\n")]),
    ("the event's outcome ignores must-not-connect", RC,
     [("\tcase a.MustNotConnect:\n\t\toutcome = tracing.CredentialUseMustNotConnect\n", "")]),
    # §8: a degraded pass is logged once per object.
    ("a degraded pass logs every time", C + "tracker.go",
     [("\tif !t.degradedLogged.first(key) {\n\t\treturn\n\t}\n", "")]),
    ("a pass that isn't degraded doesn't clear the log", C + "tracker.go",
     [("\t\tt.degradedLogged.clear(key)\n", "")]),
    ("Forget doesn't clear the log", C + "tracker.go",
     [("\tt.degradedLogged.forget(obj.GetUID())\n", "")]),
    # The review of PR 7.
    ("the interval isn't derived", C + "shared.go",
     [("\t\to.DisownInterval = defaultDisownInterval(o.DisownPasses, disownSpan(o.TimeoutBound, s.landingMargin()))", "\t\to.DisownInterval = DefaultDisownInterval")]),
    ("the derived interval isn't rounded to a second", C + "shared.go",
     [("\tif r := d % time.Second; r != 0 {\n\t\td += time.Second - r\n\t}\n", "")]),
    ("the derived interval isn't published", C + "setup.go",
     [("\t\tDisownInterval: formatDuration(o.DisownInterval),", "\t\tDisownInterval: formatDuration(DefaultDisownInterval),")]),
    ("a BeforeEstablish error is ignored", RR,
     [("\t\tif err := r.cu.tracker.BeforeEstablish(ctx, managed, cup.acquired); err != nil {", "\t\tif err := r.cu.tracker.BeforeEstablish(ctx, managed, cup.acquired); false && err != nil {")]),
    ("an established holder fails closed", RC,
     [("\tif a.MustNotConnect {", "\tif a.MustNotConnect || a.Degraded {")]),
    ("the standalone Forget ignores the finalizer", RC,
     [("\tif !r.cu.active() || !r.cu.tracker.Forget(mg) || credentialuse.IsEstablished(mg, r.cu.owners) {", "\tif !r.cu.active() || !r.cu.tracker.Forget(mg) || false {")]),
    ("the write loops don't need leader election", RC,
     [("func (writeLoopsRunnable) NeedLeaderElection() bool { return true }", "func (writeLoopsRunnable) NeedLeaderElection() bool { return false }")]),
    ("TimeoutBound without the grace", RC,
     [("\t\tTimeoutBound: r.timeout + reconcileGracePeriod,", "\t\tTimeoutBound: r.timeout,")]),
    ("PollInterval isn't passed", RC,
     [("\t\tPollInterval:   r.pollInterval,\n", "")]),
    ("the event's outcome ignores degraded", RC,
     [("\tcase a.Degraded:\n\t\toutcome = tracing.CredentialUseDegraded\n", "")]),
    ("the event's outcome ignores a conflict", RC,
     [("\tcase errors.Is(err, credentialuse.ErrHolderConflict):\n\t\toutcome = tracing.CredentialUseConflict\n", "")]),
    # §12: the metrics.
    ("the metrics lose holder_kind", T + "credentialuse.go",
     [("\treturn metric.WithAttributes(AttrHolderKind.String(k))", "\treturn metric.WithAttributes()")]),
]

KILLERS = {
    "no source check before the Get": {R: "TestCredentialUseSourceNotStartedFailsTheFirstReconcile", E: "TestStartFailsWithoutTheSource"},
    "an unstarted source doesn't fail the manager": {R: "TestCredentialUseSourceNotStartedFailsTheFirstReconcile", E: "TestStartFailsWithoutTheSource"},
    "the timer never fails": {R: "TestCredentialUseSourceTimer"},
    "a finalizer without a name is no setup error": {R: "TestCredentialUseFinalizerWithoutNameFailsStart"},
    "a Track error doesn't fail the manager": {R: "TestCredentialUseTrackErrorFailsStart"},
    "APIFinalizer names no finalizer": {R: "TestCredentialUseSourceNotStartedFailsTheFirstReconcile|TestIgnoreCredentialUseRecord"},
    "a Shared per Reconciler": {R: "TestCredentialUseOneSharedPerManager"},
    "the informers need leader election": {R: "TestCredentialUseRunnablesElection"},
    "a source's Start doesn't start the informers": {R: "TestCredentialUseSourceStartStartsTheInformers"},
    "WaitForSync doesn't wait": {R: "TestCredentialUseSourceWaitsAndForwards"},
    "Enqueue doesn't reach the queue": {R: "TestCredentialUseSourceWaitsAndForwards"},
    "HasSynced is always true": {C: "TestTrackerHasSynced"},
    "no Forget on the orphan path": {R: "TestCredentialUseStandaloneForgetOnTheOrphanPath", E: "TestStandaloneForget"},
    "no Forget on the delete path": {E: "TestStandaloneForget|TestForegroundDeletionOfAnOwnerCompletes"},
    "no standalone Forget": {R: "TestCredentialUseStandaloneForgetOnTheOrphanPath", E: "TestStandaloneForget"},
    "no Acquire": {R: "TestIgnoreCredentialUseRecord", E: "TestNamespaceTeardown"},
    "a finalized deleting holder acquires": {R: "TestCredentialUseSkipsAcquireOnAFinalizedDeletingHolder", E: "TestForegroundDeletionOfAnOwnerCompletes"},
    "pristine taken before Acquire": {R: "TestCredentialUseDeferralRightAfterTheRecordWrite"},
    "a record conflict isn't requeued": {R: "TestCredentialUseRecordConflictRequeues|TestTracingStatusMapping"},
    "a new holder that must not connect connects": {R: "TestStatusConflictOnEveryPath", E: "TestNewHolderOnADeletingSecretDeletesFreely"},
    "no re-read after a webhook changed A1": {R: "TestCredentialUseRereadsAfterAWebhookChangedTheRecordWrite"},
    "the re-read ignores the live reader": {R: "TestCredentialUseRereadsAfterAWebhookChangedTheRecordWrite"},
    "no resolved chain on Connect's context": {C: "TestCredentialUseResolvedOnConnectsContext"},
    "no holder on Connect's context": {R: "TestCredentialUseCheckingReader"},
    "no BeforeEstablish": {R: "TestCredentialUseBeforeEstablishHoldsARefThatAppeared|TestStatusConflictOnEveryPath"},
    "RetryAfter is ignored": {R: "TestCredentialUseRetryAfter"},
    "RetryAfter cuts a deferral": {R: "TestCredentialUseRetryAfter"},
    "no pointer on a Connect failure": {R: "TestCredentialUseSuffixOnlyOnAConnectFailure", E: "TestTheSyncedMessageNamesTheUnheldRefs"},
    "no pointer on an Observe failure": {R: "TestCredentialUseSuffixOnAnObserveFailure"},
    "the count isn't capped": {R: "TestUnheldSuffix"},
    "the reason isn't cut": {R: "TestUnheldSuffix"},
    "More isn't counted": {C: "TestSummarizeUnheld", R: "TestUnheldSuffix"},
    "the first entry is named, not the most recent": {C: "TestSummarizeUnheld", R: "TestUnheldSuffix"},
    "the predicate drops every update": {R: "TestIgnoreCredentialUseRecord", E: "TestARecordChangeThisProcessDidntWriteStartsAPass"},
    "any record write counts as this process's": {C: "TestIsOwnRecordWrite", R: "TestIgnoreCredentialUseRecord"},
    "other changes ride with the record write": {C: "TestIsOwnRecordWrite", R: "TestIgnoreCredentialUseRecord"},
    "Forget keeps the record write": {C: "TestIsOwnRecordWrite"},
    "a deleted holder's record write is kept": {C: "TestRecordWritesGoWithTheHolder"},
    "CheckRead ignores the record": {C: "TestCheckRead"},
    "CheckRead ignores the refs": {C: "TestCheckRead"},
    "an unrecorded read isn't counted": {C: "TestCheckRead"},
    "an unnamed read passes the cover check": {RT: "TestAssertCredentialRefsCover"},
    "a ref that resolves to nothing passes the cover check": {RT: "TestAssertCredentialRefsCover"},
    "the metrics lose holder_kind": {T: "TestCredentialUseMetrics"},
    "the interval isn't derived": {C: "TestDisownIntervalDerivedFromTheTimeoutBound", R: "TestCredentialUseStartsWithALongTimeout"},
    "the derived interval isn't rounded to a second": {C: "TestDisownIntervalDerivedFromTheTimeoutBound"},
    "the derived interval isn't published": {C: "TestDisownIntervalDerivedFromTheTimeoutBound", R: "TestCredentialUseStartsWithALongTimeout"},
    "a BeforeEstablish error is ignored": {R: "TestCredentialUseBeforeEstablishErrorAddsNoFinalizer"},
    "an established holder fails closed": {R: "TestCredentialUseEstablishedHolderFailsOpen"},
    "the standalone Forget ignores the finalizer": {R: "TestCredentialUseStandaloneForgetOnTheOrphanPath"},
    "the write loops don't need leader election": {R: "TestCredentialUseRunnablesElection"},
    "TimeoutBound without the grace": {R: "TestCredentialUseTimeoutBoundIsTheTimeoutPlusTheGrace"},
    "PollInterval isn't passed": {R: "TestCredentialUsePollIntervalCapsTheConsentBackoff"},
    "the event's outcome ignores degraded": {R: "TestTracingCredentialUseAcquireEvent"},
    "the event's outcome ignores a conflict": {R: "TestTracingCredentialUseAcquireEvent"},
    "a degraded pass logs every time": {C: "TestDegradedIsLoggedOncePerHolder"},
    "a pass that isn't degraded doesn't clear the log": {C: "TestDegradedIsLoggedOncePerHolder"},
    "Forget doesn't clear the log": {C: "TestDegradedIsLoggedOncePerHolder"},
    "no credentialuse.acquire event": {R: "TestTracingCredentialUseAcquireEvent"},
    "the event drops the counts by reason": {R: "TestTracingCredentialUseAcquireEvent"},
    "the event's outcome ignores must-not-connect": {R: "TestTracingCredentialUseAcquireEvent"},
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


if not os.environ.get("KUBEBUILDER_ASSETS"):
    sys.exit("KUBEBUILDER_ASSETS is not set: the envtests need it")
start = digest()
missing = [name for name, _, _ in MUTANTS if name not in KILLERS]
if missing:
    sys.exit(f"no killing tests named for {missing}")
base = go_test(ALL)
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
