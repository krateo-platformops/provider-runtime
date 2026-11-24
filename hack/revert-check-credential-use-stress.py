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

A mutant counts as found only if an audit found it: a line AUDIT <class>, of a
class other than INCOMPLETE and STALE-LIST, which say that the recording, or
the API server, can't be trusted. OTHER is a red without one, as when a
namespace doesn't empty; BUILD, PANIC and TIMEOUT aren't finds either. The
baseline must pass REVERT_BASELINE iterations (default 3) or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Two kinds of mutant: as a defect would be, and, where its window is a few
milliseconds, the same with 200ms slept inside it. The second says the audit
can see it, and nothing of how often CI would.

Usage, on a VM (the envtests need KUBEBUILDER_ASSETS):
  python3 hack/revert-check-credential-use-stress.py > hack/revert-check-credential-use-stress.txt

REVERT_SUBTEST is plain (default), cross or chained. REVERT_SEED is passed on
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
FILES = [AQ, RE, DE, PA, VE]

NO_EPOCH_TEST = (PA, "\t\t{Op: \"test\", Path: annotation, Value: epoch},\n", "")
NO_UID_TEST = (PA, "\t\t{Op: \"test\", Path: \"/metadata/uid\", Value: string(uid)},\n", "")
R3 = "\t\t\tpt, err := ReleasePatch(r.meta, p.holder, n)\n"
LATE_R3 = (RE, R3, "\t\t\ttime.Sleep(200 * time.Millisecond)\n" + R3)
REREAD = ("\t\t\tif again, err := p.srv.Get(ctx, r.id.GroupKind(), r.id.Namespace, r.id.Name); err == nil && hasFinalizer(again, f) {\n"
          "\t\t\t\tr.meta = again\n\t\t\t}\n")

# (name, [(file, old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # R2 ignoring deleting users.
    ("a holder being deleted is no user",
     [(DE, "\tif holder.GetDeletionTimestamp() != nil && !IsEstablished(holder, ownerFinalizers) {\n\t\treturn false",
       "\tif holder.GetDeletionTimestamp() != nil {\n\t\treturn false")]),
    ("after the grace, users being deleted don't keep the object (R0 and R2)",
     [(DE, "\treturn holder.GetDeletionTimestamp() != nil && IsUser(holder, p, ownerFinalizers)\n", "\treturn false\n")]),
    ("the same, in R2 alone",
     [(RE, "\t\t\t\tif IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {", "\t\t\t\tif false && IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {")]),
    # R3 without the epoch test.
    ("R3 doesn't test the epoch", [NO_EPOCH_TEST]),
    ("R3 doesn't test the epoch, and is 200ms late", [NO_EPOCH_TEST, LATE_R3]),
    # A release that skips the grace.
    ("a release skips the grace",
     [(DE, "\treturn firstSeen.Add(ahead + grace)\n", "\treturn firstSeen.Add(ahead)\n")]),
    ("the grace is half as long",
     [(DE, "\treturn firstSeen.Add(ahead + grace)\n", "\treturn firstSeen.Add(ahead + grace/2)\n")]),
    ("a release ignores a terminating namespace",
     [(RE, "\t\t\t\t\tnsDeleting[ns] = m.DeletionTimestamp != nil\n", "\t\t\t\t\tnsDeleting[ns] = m == nil\n")]),
    ("R2 lists before R1",
     [(RE, "\tbuiltFrom := map[ID]string{} // the resourceVersion each failed release was built from\n",
       "\tbuiltFrom := map[ID]string{} // the resourceVersion each failed release was built from\n\tearly, earlyErr := p.srv.List(ctx, p.holder, n)\n"),
      (RE, "\t\tholders, err := p.srv.List(ctx, p.holder, n)\n", "\t\tholders, err := early, earlyErr\n")]),
    # A record drop.
    ("A1 drops the refs of a holder being deleted",
     [(AQ, "\trec := ReadRecord(obj)\n\tsameRecord := ", "\tif obj.GetDeletionTimestamp() != nil {\n\t\twant = nil\n\t}\n\trec := ReadRecord(obj)\n\tsameRecord := ")]),
    # A release of the wrong incarnation.
    ("R3 is built from a second read", [(RE, R3, REREAD + R3)]),
    ("R3 is built from a second read, 200ms after the list",
     [(RE, R3, "\t\t\ttime.Sleep(200 * time.Millisecond)\n" + REREAD + R3)]),
    ("R3 tests neither the uid nor the epoch, and is 200ms late", [NO_UID_TEST, NO_EPOCH_TEST, LATE_R3,
     (PA, "\t\treturn Patch{}, fmt.Errorf(\"credential-use: a release needs the object's uid\")", "\t\treturn Patch{}, fmt.Errorf(\"credential-use: a release needs the object's uid %s\", uid)")]),
    # A pass that skips A2 and reports Held.
    ("a deletion pass skips A2 and reports Held",
     [(AQ, "\t\t\theld := false\n\t\t\tif !deleting && !added[id] {", "\t\t\theld := deleting\n\t\t\tif deleting {\n\t\t\t\ta.Held = append(a.Held, r)\n\t\t\t}\n\t\t\tif !deleting && !added[id] {")]),
    ("a live pass skips A2 on any cached object",
     [(AQ, "\t\t\t\tif cached, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), obj.GetResourceVersion(), id, cached.GetUID(), namespace) {",
       "\t\t\t\tif _, ok := p.views.Protected(id); ok {")]),
    ("a live pass skips on a write from before the record named the ref",
     [(VE, "\tskip := obs.Before(issued)\n", "\tskip := obs.Before(issued) || true\n")]),
]

SUBTESTS = {
    "plain": "^2_processes,_no_leader_election$",
    "cross": "^2_processes,_cross-namespace_refs_whose_consent_toggles$",
    "chained": "^2_processes,_Widgets_through_Targets$",
}
SUBTEST = SUBTESTS[os.environ.get("REVERT_SUBTEST", "plain")]
ITERATIONS = int(os.environ.get("REVERT_ITERATIONS", "20"))
REPEATS = int(os.environ.get("REVERT_REPEATS", "3"))
BASELINE = int(os.environ.get("REVERT_BASELINE", "3"))
TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "3600"))
NOT_FINDS = {"INCOMPLETE", "STALE-LIST"}


class Run:
    def __init__(self, returncode, out, seconds):
        self.returncode, self.out, self.seconds = returncode, out, seconds
        self.iterations = len(re.findall(r"^--- (?:PASS|FAIL): TestStress \(", out, re.M))
        self.classes = sorted(set(re.findall(r"AUDIT ([A-Z0-9-]+):", out)))


def stress(iterations):
    env = dict(os.environ, CI="1")
    if os.environ.get("REVERT_SEED"):
        env["CREDENTIALUSE_STRESS_SEED"] = os.environ["REVERT_SEED"]
    began = time.time()
    r = subprocess.run(["go", "test", "-race", "-v", "-failfast", f"-count={iterations}", "-timeout", f"{TIMEOUT}s",
                        "-run", f"^TestStress$/{SUBTEST}", "./" + C], cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
    return Run(r.returncode, r.stdout + r.stderr, time.time() - began)


def classify(r):
    if r.returncode == 0:
        return "GREEN"
    if "build failed" in r.out or "[setup failed]" in r.out:
        return "BUILD"
    if "panic: test timed out" in r.out:
        return "TIMEOUT"
    if re.search(r"^panic:|\[recovered\]|DATA RACE", r.out, re.M):
        return "PANIC"
    if set(r.classes) - NOT_FINDS:
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
chosen = [m for m in MUTANTS if not ONLY or re.search(ONLY, m[0])]

if os.environ.get("REVERT_CHECK_ONLY"):
    wrong = 0
    for name, edits in chosen:
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
base = stress(BASELINE)
if base.returncode != 0 or base.iterations != BASELINE:
    print(f"BASELINE FAILED after {base.iterations} iterations: the run is invalid\n" + base.out[-6000:])
    sys.exit(1)
print(f"baseline passed: {BASELINE} iterations, {base.seconds / BASELINE:.0f}s each, commit {commit()}, sub-test {SUBTEST}")
print(f"each mutant: up to {ITERATIONS} iterations, {REPEATS} times, seed {os.environ.get('REVERT_SEED') or 'fixed'}\n")

totals = {}
for name, edits in chosen:
    began = utc()
    runs = []
    try:
        bad = apply(edits)
        if bad:
            print(f"NOT-APPLIED {name}: {bad}")
            totals["NOT-APPLIED"] = totals.get("NOT-APPLIED", 0) + 1
            continue
        for _ in range(REPEATS):
            runs.append(stress(ITERATIONS))
            if classify(runs[-1]) == "BUILD":
                break
    finally:
        restore_all()
    states = [classify(r) for r in runs]
    reds = states.count("FOUND")
    ran = sum(r.iterations for r in runs)
    state = "FOUND" if reds == len(states) else "SOMETIMES" if reds else states[0] if len(set(states)) == 1 else "MIXED"
    totals[state] = totals.get(state, 0) + 1
    print(f"{state:9} {name}")
    for s, r in zip(states, runs):
        print(f"          {s} at iteration {r.iterations} of {ITERATIONS}, {r.seconds:.0f}s: {' '.join(r.classes) or 'no audit'}")
        if s in ("OTHER", "PANIC", "BUILD", "TIMEOUT"):
            print("          " + r.out[-1500:].replace("\n", "\n          "))
    print(f"          {reds} reds in {ran} iterations" + (f": about {100 * reds / ran:.0f}% of them" if reds and ran else "") + f"; {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(totals.items())) + f" of {len(chosen)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
