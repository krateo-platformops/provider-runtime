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

REVERT_TIMEOUT_S sets go test's timeout per run (default 900).
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["listwatch.go", "informers.go", "shared.go", "setup.go", "tracker.go", "queue.go", "acquire.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # listwatch.go: the relist floor (§6.5 L2, A20).
    ("a List's completion isn't recorded", "listwatch.go",
     [("\tif out.Continue == \"\" {\n\t\tl.lists.Add(1)\n\t\tl.completed()\n", "\tif out.Continue == \"\" {\n\t\tl.lists.Add(1)\n")]),
    ("a watch-list stream's completion isn't recorded", "listwatch.go",
     [("\t\t\t\tw.lw.streams.Add(1)\n\t\t\t\tw.lw.completed()\n", "\t\t\t\tw.lw.streams.Add(1)\n")]),
    ("the holder informer's lists don't reach the verified set", "tracker.go",
     [("s.clock, t.ver.listCompleted)", "s.clock, nil)")]),
    # informers.go: the handlers (§6.3, §10.2, §11.3).
    ("a holder update doesn't reach the verified set", "informers.go",
     [("\th.t.ver.holderEvent(n.UID, n, at)\n", "")]),
    ("a protected object leaving the cache doesn't enqueue its holders", "informers.go",
     [("\t\tt.enqueueHolders(id)\n\t}\n}\n\n// graceTimers", "\t}\n}\n\n// graceTimers")]),
    ("the Namespace handler acts on every change", "informers.go",
     [("\tif !newlyDeleting && (o.DeletionTimestamp == nil) == (n.DeletionTimestamp == nil) &&\n\t\tmaps.Equal(o.Annotations, n.Annotations) {\n\t\treturn\n\t}\n",
       "\t_ = maps.Equal(o.Annotations, n.Annotations)\n")]),
    ("a namespace's deletion doesn't enqueue its release batches", "informers.go",
     [("\t\t\t\tif newlyDeleting {\n\t\t\t\t\tfor ns := range holds(t, m) {", "\t\t\t\tif false {\n\t\t\t\t\tfor ns := range holds(t, m) {")]),
    ("the Namespace handler doesn't enqueue holders", "informers.go",
     [("\t\t\t\tt.enqueueHolders(id)\n\t\t\t\tif newlyDeleting {", "\t\t\t\tif newlyDeleting {")]),
    # shared.go and setup.go: setup (§7.3, §7.5).
    ("start doesn't check the namespace verbs", "shared.go",
     [("\tif err := s.canWatchNamespaces(ctx); err != nil {\n\t\treturn err\n\t}\n", "")]),
    ("a second Track with other options returns the first tracker", "shared.go",
     [("\t\tif !equalOptions(t.o, o) {\n\t\t\treturn nil, fmt.Errorf(\"credential-use: %s is already tracked with other options\", gk)\n\t\t}\n\t\treturn t, nil\n\t}\n\ts.mu.Unlock()",
       "\t\treturn t, nil\n\t}\n\ts.mu.Unlock()")]),
    ("Disown's bound isn't checked", "shared.go",
     [("\tif time.Duration(o.DisownPasses)*o.DisownInterval < 2*o.TimeoutBound {", "\tif false {")]),
    ("a refused Track doesn't stop Run", "shared.go",
     [("\t\ts.setupErr = errors.Join(s.setupErr, err)\n", "")]),
    ("published settings that disagree are accepted", "setup.go",
     [("\t\t\tif d := disagreement(want, got); d != \"\" {", "\t\t\tif d := disagreement(want, got); false && d != \"\" {")]),
    ("the settings aren't published", "setup.go",
     [("\t\t_, err = crds.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})\n", "\t\t_, _ = types.MergePatchType, patch\n\t\terr = nil\n")]),
    # queue.go: buffering and timers (M1 of the review of revision 16).
    ("the queue drops what a batch covered before", "queue.go",
     [("\tset := q.pending[k]\n\tif set == nil {", "\tset := map[ID]bool(nil)\n\tif set == nil {")]),
    ("a key added during work isn't queued again", "queue.go",
     [("\tif _, again := q.pending[k]; again && !q.queued[k] && !q.stopped {", "\tif false {")]),
    ("ShutDown leaves the timers running", "queue.go",
     [("\tfor t := range q.timers {\n\t\tt.Stop()\n\t}\n", "")]),
    # acquire.go: class (e) is counted (L3 of the review of revision 16).
    ("class (e) isn't counted", "acquire.go",
     [("\t\t\t\tp.reports.AdmissionDenied(p.holder)\n", "")]),
]

EQUIVALENT = {}

TESTS = ("^(TestRelistResetsRecordSince|TestTrackAfterStartAndTwice|TestPublishedSettings|TestRunNeedsNamespaceListAndWatch|"
         "TestNamespaceHandlerActsOnlyOnItsFields|TestStrippingEnqueuesTheHolders|TestDeletingSecretRefusesANewHolder|"
         "TestBatchQueue.*|TestAdmissionDenialIsCounted|TestPublishedSettingsCompareAsValues|TestTrackValidates)$")


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

counts = {}
for num, (name, f, edits) in enumerate(MUTANTS, 1):
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

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
