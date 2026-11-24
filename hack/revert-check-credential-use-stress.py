"""Revert check for TestStress (docs/design-credential-use-finalizer.md §14,
"What the stress test shows"): break the protocol, and count how many
iterations of the stress it takes until an audit finds it.

Unlike the other revert checks, a mutant here is a race as often as not, so
one run says little. Each mutant runs TestStress's sub-test, at CI's length,
up to REVERT_ITERATIONS times (default 20) in one test binary that stops at
the first red, and that REVERT_REPEATS times (default 3). The table has the
iteration each run went red at, and what the audits found. The reds over all
the iterations run are an estimate of the share of CI runs that would see the
mutant, as rough as their number.

A mutant counts as found only if an audit found one of the classes it names: a
line AUDIT <class>. OTHER is a red without one, as when a namespace doesn't
empty, or when the only classes are others; its output is printed. BUILD,
PANIC and TIMEOUT aren't finds either. An unmutated run may go red on the API
server's own H1 (H1_CLASSES). A run whose every error line is of those is
excused, and the iterations left go on; one other error line, and it isn't.
The baseline must pass REVERT_BASELINE iterations (default 3), so excused, or
the run is invalid. Each source file is restored after its mutant, and the end
checks the tree is as it started.

Two kinds of mutant: as a defect would be, and, where its window is a few
milliseconds, the same with 200ms slept inside it. The second says the audit
can see it, and nothing of how often CI would.

Usage, on a VM (the envtests need KUBEBUILDER_ASSETS):
  python3 hack/revert-check-credential-use-stress.py > hack/revert-check-credential-use-stress.txt

REVERT_SUBTEST is plain (default), cross, chained, three or outside; a mutant
of the consent gate runs in cross alone, and one of a skip not outside the
caches, where no pass skips. REVERT_SEED is passed on
as CREDENTIALUSE_STRESS_SEED (default: CI's fixed seed; vary for others).
REVERT_ONLY, a regular expression, runs only the mutants whose names it
matches. REVERT_TIMEOUT_S bounds each go test (default 3600).
REVERT_CHECK_ONLY=1 checks that every mutant applies exactly once, and
compiles (go vet, tests included), and runs no test.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
AQ, RE, DE, PA, VE = C + "acquire.go", C + "release.go", C + "decide.go", C + "patch.go", C + "verified.go"
CO, IN, SE, QU = C + "consent.go", C + "informers.go", C + "server.go", C + "queue.go"
FILES = [AQ, RE, DE, PA, VE, CO, IN, SE, QU]

NO_EPOCH_TEST = (PA, "\t\t{Op: \"test\", Path: annotation, Value: epoch},\n", "")
NO_UID_TEST = (PA, "\t\t{Op: \"test\", Path: \"/metadata/uid\", Value: string(uid)},\n", "")
R3 = "\t\t\tpt, err := ReleasePatch(r.meta, p.holder, n)\n"
LATE_R3 = (RE, R3, "\t\t\ttime.Sleep(200 * time.Millisecond)\n" + R3)
REREAD = ("\t\t\tif again, err := p.srv.Get(ctx, r.id.GroupKind(), r.id.Namespace, r.id.Name); err == nil && hasFinalizer(again, f) {\n"
          "\t\t\t\tr.meta = again\n\t\t\t}\n")
R2 = "\t\tholders, err := p.srv.List(ctx, p.holder, n)\n"
KEPT_LISTS = [(RE, "\t\"fmt\"\n\t\"time\"\n", "\t\"fmt\"\n\t\"sync\"\n\t\"time\"\n"),
              (RE, "// cachedUse is R0's reading of one object's users in the caches.\n",
               "var keptLists sync.Map\n\n// cachedUse is R0's reading of one object's users in the caches.\n")]
LIVE_READ = "\tns, err := p.srv.Get(ctx, namespaceGroupKind, \"\", id.Namespace)\n"
LIVE_YES = "\tif consents(o.Annotations[AnnotationAllowHoldersFrom], ns.Annotations[AnnotationAllowHoldersFrom], obj.GetNamespace()) {\n\t\treturn gateHoldLive"
GRACE_END = "\treturn firstSeen.Add(ahead + grace)\n"
SKIP = "\t\t\t\tif cached, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), obj.GetResourceVersion(), id, cached.GetUID(), namespace) {"
WROTE = "\t\tp.ver.wrote(%s, namespace, issued)\n\t\tp.wroteDeleting(id, namespace, out)\n\t\treturn ClassNone, out.UID, nil\n\t}\n\tvar last error\n"
SEND_GET = "\treturn c.Get(ctx, name, metav1.GetOptions{})\n"
A2_LABELS = "\t\t\"labels\":      map[string]string{%sk.marker: \"true\"},\n\t}}\n\tdata, err := json.Marshal(body)\n\treturn Patch{Type: types.StrategicMergePatchType, Data: data}, err\n"
MISSING = "\t\t\t\tcase ClassNotFound:\n%s\t\t\t\t\tunfollowed = unfollowed || r.Next != nil\n\t\t\t\tcase ClassDeleting:\n"

OVER, EARLY, UNREAD = "RELEASED-OVER-A-USER", "RELEASED-INSIDE-THE-GRACE", "RELEASED-WHAT-IT-HADNT-READ"
NO_A2, NO_CONSENT, UNACCOUNTED = "HELD-WITHOUT-A2", "HELD-WITHOUT-CONSENT", "REF-UNACCOUNTED"
DROP, DIFFERS, UNANNOUNCED = "RECORD-DROP", "RECORD-DIFFERS", "HELD-BEFORE-THE-RECORD"
KEPT = "CANARY-NOT-KEPT"
CACHED = ("plain", "cross", "chained", "three")

# (name, [(file, old, new), ...], the classes that find it): every old must
# occur exactly once in file. A fourth element names the only sub-tests that
# can see the mutant.
MUTANTS = [
    # R2 ignoring deleting users.
    ("a holder being deleted is no user",
     [(DE, "\tif holder.GetDeletionTimestamp() != nil && !IsEstablished(holder, ownerFinalizers) {\n\t\treturn false",
       "\tif holder.GetDeletionTimestamp() != nil {\n\t\treturn false")], [OVER]),
    ("after the grace, users being deleted don't keep the object (R0 and R2)",
     [(DE, "\treturn holder.GetDeletionTimestamp() != nil && IsUser(holder, p, ownerFinalizers)\n", "\treturn false\n")], [OVER, KEPT]),
    ("the same, in R2 alone",
     [(RE, "\t\t\t\tif IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {", "\t\t\t\tif false && IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {")], [OVER, KEPT]),
    # R3 without the epoch test.
    ("R3 doesn't test the epoch", [NO_EPOCH_TEST], [UNREAD, OVER]),
    ("R3 doesn't test the epoch, and is 200ms late", [NO_EPOCH_TEST, LATE_R3], [UNREAD, OVER, EARLY]),
    ("every A2 writes the same epoch", [(AQ, "\tepoch, err := p.newEpoch()\n", "\tepoch, err := \"e\", error(nil)\n")], [UNREAD, OVER]),
    # A release that skips the grace.
    ("a release skips the grace", [(DE, GRACE_END, "\treturn firstSeen.Add(ahead)\n")], [EARLY]),
    ("the grace is half as long", [(DE, GRACE_END, "\treturn firstSeen.Add(ahead + grace/2)\n")], [EARLY]),
    ("a grace that never ends", [(DE, GRACE_END, "\treturn firstSeen.Add(ahead + grace + time.Hour)\n")], ["CANARY-NOT-RELEASED", "TOO-LITTLE"]),
    ("a release ignores a terminating namespace",
     [(RE, "\t\t\t\t\tnsDeleting[ns] = m.DeletionTimestamp != nil\n", "\t\t\t\t\tnsDeleting[ns] = m == nil\n")], [OVER, KEPT]),
    # R2's and the gate's reads.
    ("R2 lists before R1",
     [(RE, "\tbuiltFrom := map[ID]string{} // the resourceVersion each failed release was built from\n",
       "\tbuiltFrom := map[ID]string{} // the resourceVersion each failed release was built from\n\tearly, earlyErr := p.srv.List(ctx, p.holder, n)\n"),
      (RE, R2, "\t\tholders, err := early, earlyErr\n")], [UNREAD, OVER]),
    ("R2 decides on the list of the batch before",
     KEPT_LISTS + [(RE, R2, R2 + "\t\tif err == nil {\n\t\t\tif old, ok := keptLists.Swap(fmt.Sprintf(\"%p/%s\", p, n), holders); ok {\n"
                            "\t\t\t\tholders = old.([]metav1.PartialObjectMetadata)\n\t\t\t}\n\t\t}\n")], [OVER]),
    ("every second batch makes no list, and decides on the one before",
     KEPT_LISTS + [(RE, R2, "\t\tvar holders []metav1.PartialObjectMetadata\n\t\tvar err error\n"
                            "\t\tif old, ok := keptLists.LoadAndDelete(fmt.Sprintf(\"%p/%s\", p, n)); ok {\n"
                            "\t\t\tholders = old.([]metav1.PartialObjectMetadata)\n\t\t} else if holders, err = p.srv.List(ctx, p.holder, n); err == nil {\n"
                            "\t\t\tkeptLists.Store(fmt.Sprintf(\"%p/%s\", p, n), holders)\n\t\t}\n")], [UNREAD]),
    ("the Namespace is read at resourceVersion 0",
     [(SE, SEND_GET, "\tvar o metav1.GetOptions\n\tif gk == namespaceGroupKind {\n\t\to.ResourceVersion = \"0\"\n\t}\n\treturn c.Get(ctx, name, o)\n")], [OVER, NO_CONSENT]),
    ("every read is at resourceVersion 0",
     [(SE, SEND_GET, "\treturn c.Get(ctx, name, metav1.GetOptions{ResourceVersion: \"0\"})\n")], [UNREAD, NO_CONSENT]),
    ("one key goes to two workers", [(QU, "\tif !q.queued[k] && !q.working[k] {\n", "\tif !q.queued[k] {\n")], ["RELEASED-TWICE"]),
    # Records.
    ("A1 drops the refs of a holder being deleted",
     [(AQ, "\trec := ReadRecord(obj)\n\tsameRecord := ", "\tif obj.GetDeletionTimestamp() != nil {\n\t\twant = nil\n\t}\n\trec := ReadRecord(obj)\n\tsameRecord := ")],
     [DROP, DIFFERS, UNANNOUNCED]),
    ("A1 keeps the refs the holder names no more",
     [(AQ, "\trec := ReadRecord(obj)\n\tsameRecord := ", "\trec := ReadRecord(obj)\n\tif !rec.Unparseable {\n\t\twant = SortedIDs(append(want, rec.IDs...))\n\t}\n\tsameRecord := ")],
     [DIFFERS]),
    ("A2 comes before A1",
     [(AQ, "\t\tadded, err := p.announce(ctx, obj, want, unheldNow, &a)\n", "\t\tadded, err := map[ID]bool{}, error(nil)\n\t\t_, _ = want, unheldNow\n")], [UNANNOUNCED]),
    # A release of the wrong incarnation, or of more than its own.
    ("R3 is built from a second read", [(RE, R3, REREAD + R3)], [UNREAD]),
    ("R3 is built from a second read, 200ms after the list",
     [(RE, R3, "\t\t\ttime.Sleep(200 * time.Millisecond)\n" + REREAD + R3)], [UNREAD, OVER, EARLY]),
    ("R3 tests neither the uid nor the epoch, and is 200ms late", [NO_UID_TEST, NO_EPOCH_TEST, LATE_R3,
     (PA, "\t\treturn Patch{}, fmt.Errorf(\"credential-use: a release needs the object's uid\")", "\t\treturn Patch{}, fmt.Errorf(\"credential-use: a release needs the object's uid %s\", uid)")],
     [UNREAD, OVER, EARLY]),
    ("R3 takes every finalizer off",
     [(PA, "\t\tjsonPatchOp{Op: \"remove\", Path: index},\n", "\t\tjsonPatchOp{Op: \"remove\", Path: \"/metadata/finalizers\"},\n")],
     ["FINALIZER-LOST", "UNEXPLAINED-REMOVAL"]),
    ("an A2 takes the other kind's finalizer off",
     [(PA, "\tdata, err := json.Marshal(body)\n\treturn Patch{Type: types.StrategicMergePatchType, Data: data}, err\n",
       "\tif strings.Contains(k.finalizer, \".widget.\") {\n\t\tbody[\"metadata\"].(map[string]any)[\"$deleteFromPrimitiveList/finalizers\"] = []string{strings.Replace(k.finalizer, \".widget.\", \".target.\", 1)}\n\t}\n"
       "\tdata, err := json.Marshal(body)\n\treturn Patch{Type: types.StrategicMergePatchType, Data: data}, err\n")], ["FINALIZER-LOST"], ("plain",)),
    ("an A2 doesn't set the held label", [(PA, A2_LABELS % "LabelHeld: \"true\", ", A2_LABELS % "")], ["LABELS"]),
    # A pass that skips A2 and reports Held.
    ("a deletion pass skips A2 and reports Held",
     [(AQ, "\t\t\theld := false\n\t\t\tif !deleting && !added[id] {", "\t\t\theld := deleting\n\t\t\tif deleting {\n\t\t\t\ta.Held = append(a.Held, r)\n\t\t\t}\n\t\t\tif !deleting && !added[id] {")], [NO_A2]),
    ("a live pass skips A2 on any cached object", [(AQ, SKIP, "\t\t\t\tif _, ok := p.views.Protected(id); ok {")], [NO_A2], CACHED),
    ("a live pass skips on a write from before the record named the ref",
     [(VE, "\tskip := obs.Before(issued)\n", "\tskip := obs.Before(issued) || true\n")], [NO_A2], CACHED),
    ("a skip goes by the object's name, not its uid",
     [(AQ, SKIP, "\t\t\t\tif _, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), obj.GetResourceVersion(), id, types.UID(id.String()), namespace) {"),
      (AQ, WROTE % "out.UID", WROTE % "types.UID(id.String())")], [NO_A2], CACHED),
    ("what a process wrote is dropped by no event",
     [(VE, "func (v *verified) protectedEvent(old, current metav1.Object) {\n\tv.access()\n", "func (v *verified) protectedEvent(old, current metav1.Object) {\n\tv.access()\n\tif v != nil {\n\t\treturn\n\t}\n")],
     [NO_A2], CACHED),
    # The consent gate.
    ("the consent gate says yes, and reads nothing", [(CO, LIVE_READ, "\tif p != nil {\n\t\treturn gateHoldLive\n\t}\n" + LIVE_READ)], [NO_CONSENT], ("cross",)),
    ("the consent gate says no, and reads nothing", [(CO, LIVE_READ, "\tif p != nil {\n\t\treturn gateNotHeld\n\t}\n" + LIVE_READ)], [UNACCOUNTED, DROP], ("cross",)),
    ("the consent gate says no, whatever it read", [(CO, LIVE_YES, LIVE_YES.replace("if consents", "if false && consents"))], [UNACCOUNTED, DROP], ("cross",)),
    ("the consent gate says yes, whatever it read", [(CO, LIVE_YES, LIVE_YES.replace("if consents", "if true || consents"))], [NO_CONSENT], ("cross",)),
    ("a cached no isn't confirmed",
     [(CO, "\t\t\t\treturn gateHoldCached\n\t\t\t}\n", "\t\t\t\treturn gateHoldCached\n\t\t\t} else if known {\n\t\t\t\treturn gateNotHeld\n\t\t\t}\n")], [UNACCOUNTED, DROP], ("cross",)),
    # What a pass says, and what never happens.
    ("every third acquire fails",
     [(AQ, "\t\"sync\"\n\t\"time\"\n", "\t\"sync\"\n\t\"sync/atomic\"\n\t\"time\"\n"),
      (AQ, "// resolve keeps o as what was read for r (Resolved)", "var acquireSeq atomic.Int64\n\n// resolve keeps o as what was read for r (Resolved)"),
      (AQ, "\tmaxDepth := p.maxDepth\n", "\tif acquireSeq.Add(1)%3 == 0 {\n\t\treturn fail(errors.New(\"mutant\"), \"Refs\")\n\t}\n\tmaxDepth := p.maxDepth\n")], ["ACQUIRE-FAILED"]),
    ("a Missing ref is left out",
     [(AQ, MISSING % "\t\t\t\t\ta.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})\n", MISSING % "")], [UNACCOUNTED]),
    ("a ref that a record drops queues no release",
     [(IN, "\tfor _, m := range []*metav1.PartialObjectMetadata{old, cur} {\n\t\tif m == nil {\n", "\tfor _, m := range []*metav1.PartialObjectMetadata{old, cur} {\n\t\tif m == nil || m == old && cur != nil {\n")],
     ["TOO-LITTLE", "CANARY-NOT-RELEASED"]),
]

SUBTESTS = {
    "plain": "^2_processes,_no_leader_election$",
    "cross": "^2_processes,_cross-namespace_refs_whose_consent_toggles$",
    "chained": "^2_processes,_Widgets_through_Targets$",
    "three": "^3_processes,_no_leader_election$",
    "outside": "^2_processes,_every_hold_outside_the_caches$",
}
WHERE = os.environ.get("REVERT_SUBTEST", "plain")
SUBTEST = SUBTESTS[WHERE]
ITERATIONS = int(os.environ.get("REVERT_ITERATIONS", "20"))
REPEATS = int(os.environ.get("REVERT_REPEATS", "3"))
BASELINE = int(os.environ.get("REVERT_BASELINE", "3"))
TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "3600"))
H1_CLASSES = ("H1-RECREATE", "H1-LOST-A2")


def errors(out):
    """The error lines of a go test -v output, less the stress's summaries."""
    return [m for m in re.findall(r"^\s+\S+\.go:\d+: (.*)$", out, re.M) if not re.match(r"\d+ processes, \S+, seed \d+: ", m)]


def h1_alone(out):
    """Every error line is an audit's, of the API server's H1, and there is one."""
    es = errors(out)
    return bool(es) and all(e.startswith(tuple("AUDIT " + c + ":" for c in H1_CLASSES)) for e in es)


class Run:
    def __init__(self, returncode, out, seconds):
        self.returncode, self.out, self.seconds, self.excused = returncode, out, seconds, 0
        self.iterations = len(re.findall(r"^--- (?:PASS|FAIL): TestStress \(", out, re.M))
        self.classes = sorted(set(re.findall(r"AUDIT ([A-Z0-9-]+):", out)))


def hunt(iterations, classes=()):
    """Runs the stress up to iterations times, to its first red that isn't the
    API server's H1 alone, which an unmutated run has too."""
    total = None
    while True:
        r = stress(iterations - (total.iterations if total else 0))
        if total:
            r.iterations, r.seconds, r.excused = total.iterations + r.iterations, total.seconds + r.seconds, total.excused
        if classify(r, classes) != "OTHER" or not h1_alone(r.out):
            return r
        r.excused += 1
        if r.iterations >= iterations:
            r.returncode = 0
            return r
        total = r


def stress(iterations):
    env = dict(os.environ, CI="1")
    if os.environ.get("REVERT_SEED"):
        env["CREDENTIALUSE_STRESS_SEED"] = os.environ["REVERT_SEED"]
    began = time.time()
    r = subprocess.run(["go", "test", "-race", "-v", "-failfast", f"-count={iterations}", "-timeout", f"{TIMEOUT}s",
                        "-run", f"^TestStress$/{SUBTEST}", "./" + C], cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
    return Run(r.returncode, r.stdout + r.stderr, time.time() - began)


def classify(r, classes):
    if r.returncode == 0:
        return "GREEN"
    if "build failed" in r.out or "[setup failed]" in r.out:
        return "BUILD"
    if "panic: test timed out" in r.out:
        return "TIMEOUT"
    if re.search(r"^panic:|\[recovered\]|DATA RACE", r.out, re.M):
        return "PANIC"
    if set(r.classes) & set(classes):
        return "FOUND"
    return "OTHER"


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, f), "rb").read())
    return h.hexdigest()


def utc():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def commit():
    return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()


originals = {f: open(os.path.join(ROOT, f)).read() for f in FILES}
start = digest()


def restore_all():
    for f, src in originals.items():
        open(os.path.join(ROOT, f), "w").write(src)


def apply(edits):
    """Applies edits, all or none; returns what doesn't apply exactly once."""
    texts = dict(originals)
    bad = []
    for f, old, new in edits:
        if texts[f].count(old) != 1:
            bad.append(f"{f}: {texts[f].count(old)} of {old!r}")
            continue
        texts[f] = texts[f].replace(old, new)
    if not bad:
        for f in {f for f, _, _ in edits}:
            open(os.path.join(ROOT, f), "w").write(texts[f])
    return bad


def on_signal(signum, _frame):
    restore_all()
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

ONLY = os.environ.get("REVERT_ONLY")
CHECK_ONLY = os.environ.get("REVERT_CHECK_ONLY")
chosen = [m[:3] for m in MUTANTS if (not ONLY or re.search(ONLY, m[0])) and (CHECK_ONLY or len(m) < 4 or WHERE in m[3])]

if CHECK_ONLY:
    wrong = 0
    for name, edits, _ in chosen:
        try:
            bad = apply(edits)
            if bad:
                print(f"NOT-APPLIED {name}: {bad}")
                wrong += 1
                continue
            r = subprocess.run(["go", "vet", "./" + C], cwd=ROOT, capture_output=True, text=True)
        finally:
            restore_all()
        print(f"{'COMPILES' if r.returncode == 0 else 'BUILD':9} {name}" + ("" if r.returncode == 0 else "\n" + (r.stdout + r.stderr)[-800:]))
        wrong += r.returncode != 0
    print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
    sys.exit(1 if wrong else 0)

if not os.environ.get("KUBEBUILDER_ASSETS"):
    sys.exit("KUBEBUILDER_ASSETS is not set: the envtests need it")
base = hunt(BASELINE)
if base.returncode != 0 or base.iterations != BASELINE:
    print(f"BASELINE FAILED after {base.iterations} iterations: the run is invalid\n" + base.out[-6000:])
    sys.exit(1)
print(f"baseline passed: {BASELINE} iterations, {base.excused} of them red on H1 alone, {base.seconds / BASELINE:.0f}s each, commit {commit()}, sub-test {SUBTEST}")
print(f"each mutant: up to {ITERATIONS} iterations, {REPEATS} times, seed {os.environ.get('REVERT_SEED') or 'fixed'}\n")

totals = {}
for name, edits, classes in chosen:
    began = utc()
    runs = []
    try:
        bad = apply(edits)
        if bad:
            print(f"NOT-APPLIED {name}: {bad}")
            totals["NOT-APPLIED"] = totals.get("NOT-APPLIED", 0) + 1
            continue
        for _ in range(REPEATS):
            runs.append(hunt(ITERATIONS, classes))
            if classify(runs[-1], classes) == "BUILD":
                break
    finally:
        restore_all()
    states = [classify(r, classes) for r in runs]
    reds = states.count("FOUND")
    ran = sum(r.iterations for r in runs)
    state = "FOUND" if reds == len(states) else "SOMETIMES" if reds else states[0] if len(set(states)) == 1 else "MIXED"
    totals[state] = totals.get(state, 0) + 1
    print(f"{state:9} {name} (by {' or '.join(classes)})")
    for s, r in zip(states, runs):
        print(f"          {s} at iteration {r.iterations} of {ITERATIONS}, {r.seconds:.0f}s: {' '.join(r.classes) or 'no audit'}" + (f"; {r.excused} excused before, red on H1 alone" if r.excused else ""))
        if s in ("OTHER", "PANIC", "BUILD", "TIMEOUT"):
            print("          " + "\n          ".join(errors(r.out)[:6] or [r.out[-1500:]]))
    print(f"          {reds} reds in {ran} iterations" + (f": about {100 * reds / ran:.0f}% of them" if reds and ran else "") + f"; {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(totals.items())) + f" of {len(chosen)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
