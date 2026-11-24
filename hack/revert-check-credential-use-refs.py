"""Revert check for refs of one object that conflict, and
AssertCredentialRefsCover's walk (§9.1, §11.9, §12): break each rule in turn
and see whether a test notices. It is also the proof that each new test fails
without its rule.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant names, in KILLERS, the tests to run in each package: {package:
tests}. The baseline runs every named test of every package. None is an
envtest.

Usage:
  python3 hack/revert-check-credential-use-refs.py > hack/revert-check-credential-use-refs.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
REVERT_CHECK_ONLY=1 checks that every mutant applies exactly once, and
compiles (go vet of its package, tests included), and runs no test.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
T = "pkg/telemetry/"
H = "pkg/reconciler/reconcilertest/"
AQ = C + "acquire.go"
RF = C + "ref.go"
CH = C + "chain.go"
CF = C + "conflicts.go"
IN = C + "informers.go"
EV = C + "events.go"
TR = C + "tracker.go"
TE = T + "credentialuse.go"
R = "pkg/reconciler/"
RC = R + "credentialuse.go"
HE = H + "credentialuse.go"
FILES = [AQ, RF, CH, CF, IN, EV, TR, TE, RC, HE]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # What asks for more (§9.1).
    ("a hold after a Guaranteed ref asks for no more", RF,
     [("first.Guaranteed && !r.Guaranteed, ", "false, ")]),
    ("a Guaranteed ref after a hold asks for more", RF,
     [("first.Guaranteed && !r.Guaranteed, ", "first.Guaranteed != r.Guaranteed, ")]),
    ("a hold after a hold asks for more", RF,
     [("first.Guaranteed && !r.Guaranteed, ", "!r.Guaranteed, ")]),
    ("a Next after none asks for no more", RF,
     [("first.Next == nil && r.Next != nil", "false")]),
    ("no Next after one asks for more", RF,
     [("first.Next == nil && r.Next != nil", "(first.Next == nil) != (r.Next == nil)")]),
    ("a Next after a Next asks for more", RF,
     [("first.Next == nil && r.Next != nil", "r.Next != nil")]),
    # The first wins, and a later one is reported, once (§9.1).
    ("the last ref wins", AQ,
     [("\t\t\tif met(r) {\n\t\t\t\tcontinue\n\t\t\t}\n", "\t\t\tmet(r)\n")]),
    ("a ref is compared with the last, not the first", AQ,
     [("\t\tif hold || next {\n\t\t\tp.conflicts.tell", "\t\tseen[id] = r\n\t\tif hold || next {\n\t\t\tp.conflicts.tell")]),
    ("a conflict at one level isn't reported", AQ,
     [("\t\t\tif met(r) {\n", "\t\t\tif _, ok := seen[id]; ok {\n")]),
    ("a conflict behind a Next isn't reported", AQ,
     [("slices.DeleteFunc(next, met)", "slices.DeleteFunc(next, func(r Ref) bool { _, ok := seen[r.ID()]; return ok })")]),
    ("no conflict is reported", AQ,
     [("\t\tif hold || next {\n", "\t\tif false && (hold || next) {\n")]),
    ("every ref met again is reported", AQ,
     [("\t\tif hold || next {\n", "\t\tif true || hold || next {\n")]),
    ("a hold is reported on every pass", AQ,
     [("hold && !told.hold &&", "hold &&")]),
    ("a Next is reported on every pass", AQ,
     [("next = next && !told.next", "next = next && (true || !told.next)")]),
    ("a Next's report is a hold's", AQ,
     [("next = next && !told.next", "next = next && !told.hold")]),
    ("a hold's report forgets the Next's", CF,
     [("asks{t.hold || a.hold, t.next || a.next}", "asks{a.hold || false && t.hold, a.next}")]),
    ("a report is once for all holders", AQ,
     [("p.conflicts.told(obj.GetUID(), id)", "p.conflicts.told(\"\", id)"),
      ("p.conflicts.tell(obj.GetUID(), id,", "p.conflicts.tell(\"\", id,")]),
    ("a report is once for all objects", AQ,
     [("p.conflicts.told(obj.GetUID(), id)", "p.conflicts.told(obj.GetUID(), ID{})"),
      ("p.conflicts.tell(obj.GetUID(), id,", "p.conflicts.tell(obj.GetUID(), ID{},"),
      ("\t\tif !unfollowed {\n\t\t\tp.conflicts.keep", "\t\tif false && !unfollowed {\n\t\t\tp.conflicts.keep")]),
    # What was reported is forgotten (§9.1).
    ("a forgotten holder's conflicts stay reported", AQ,
     [("\tp.conflicts.forget(obj.GetUID())\n", "")]),
    ("a holder with no record isn't forgotten", AQ,
     [("\tp.conflicts.forget(obj.GetUID())\n", ""),
      ("\tp.reported.forget(obj.GetUID())\n", "\tp.reported.forget(obj.GetUID())\n\tp.conflicts.forget(obj.GetUID())\n")]),
    ("a deleted holder's conflicts stay reported", IN,
     [("\th.t.proto.conflicts.forget(m.UID)\n", "")]),
    ("a conflict that is gone stays reported", CF,
     [("\t\t\tdelete(c.m[holder], id)\n", "\t\t\t_ = id\n")]),
    ("what is no longer asked for stays reported", CF,
     [("asks{t.hold && a.hold, t.next && a.next}", "t")]),
    ("one thing no longer asked for stays reported", CF,
     [("\t\t\tc.m[holder][id] = a\n\t\t}\n\t}\n\tif len", "\t\t\t_ = a\n\t\t}\n\t}\n\tif len")]),
    ("a holder with nothing reported is kept", CF,
     [("\tif len(c.m[holder]) == 0 {\n\t\tdelete(c.m, holder)\n\t}\n", "")]),
    ("only a pass that ends forgets", AQ,
     [("\t\t\tmetAll()\n\t\t\tu := p.unheldEntries", "\t\t\tu := p.unheldEntries")]),
    ("only a pass whose last level leads nowhere forgets", AQ,
     [("\t\tlevel = slices.DeleteFunc(next, met)\n\t}\n\tmetAll()\n", "\t\tlevel = slices.DeleteFunc(next, met)\n\t}\n")]),
    ("a pass that couldn't follow a hop forgets", AQ,
     [("\t\tif !unfollowed {\n\t\t\tp.conflicts.keep", "\t\tif true || !unfollowed {\n\t\t\tp.conflicts.keep")]),
    ("every pass forgets as it ends", AQ,
     [("\tlevel := refs\n\tfor depth := 1;", "\tdefer metAll()\n\tlevel := refs\n\tfor depth := 1;")]),
    # A Disowned kind's (§9.1).
    ("a Disowned kind's hold is reported", AQ,
     [("hold && !told.hold && !p.isDisowned(id.GroupKind())", "hold && !told.hold")]),
    ("a Disowned kind's Next isn't reported", AQ,
     [("next = next && !told.next", "next = next && !told.next && !p.isDisowned(id.GroupKind())")]),
    # The recorder (§12).
    ("a recorder that is none isn't logged", RC,
     [("\t} else if r.metrics != nil {\n", "\t} else if false && r.metrics != nil {\n")]),
    ("no recorder is logged", RC,
     [("\t} else if r.metrics != nil {\n", "\t} else {\n")]),
    # The log and the metric (§12).
    ("a hold isn't counted", EV,
     [("\tif hold {\n\t\tasked", "\tif false && hold {\n\t\tasked")]),
    ("a Next isn't counted", EV,
     [("\tif next {\n\t\tasked", "\tif false && next {\n\t\tasked")]),
    ("what is asked for is mislabelled", EV,
     [("\taskedNext = \"next\"\n", "\taskedNext = \"hold\"\n")]),
    ("a conflict isn't logged", EV,
     [("\tif r.warn == nil {\n\t\treturn\n\t}\n\tr.warn(", "\tif r.warn == nil || true {\n\t\treturn\n\t}\n\tr.warn(")]),
    ("the log doesn't name the holder", EV,
     [("h.GetNamespace()+\"/\"+h.GetName(), \"object\"", "\"\", \"object\"")]),
    ("the log doesn't name the object", EV,
     [("\"object\", ref.String()", "\"object\", \"\"")]),
    ("a tracker remembers no conflict", TR,
     [("\t\tconflicts:       newConflictSet(),\n", "")]),
    ("a tracker's conflicts aren't logged", TR,
     [(", warn: t.warn}", "}")]),
    ("a tracker's conflicts aren't counted", TR,
     [("func(r MetricsRecorder) { r.RefsConflicting(k, asked) }", "func(r MetricsRecorder) {}")]),
    ("the metric isn't labelled with what was asked", TE,
     [(", AttrAsked.String(asked)))", "))")]),
    ("the metric isn't recorded", TE,
     [("\tm.cu.refsConflicting.Add(context.Background(), 1, metric.WithAttributes(AttrHolderKind.String(holderKindLabel), AttrAsked.String(asked)))\n", "")]),
    # Listers (§9.3), and what a Next may change.
    ("a lister that returns nothing gives an empty object", CH,
     [("\t\t\treturn nil, fmt.Errorf(\"credential-use: the lister for %s returned neither an object nor an error\", r.GroupKind)\n",
       "\t\t\treturn &unstructured.Unstructured{Object: map[string]any{}}, nil\n")]),
    ("a Next is given the lister's own object", CH,
     [("r.Next(obj.DeepCopy())", "r.Next(obj)")]),
    ("NextObject keeps the status", CH,
     [("\treturn asCached(u.DeepCopy())\n", "\treturn u.DeepCopy()\n")]),
    # AssertCredentialRefsCover (§11.9).
    ("the helper doesn't refuse a hold", HE,
     [("\t\tif hold {\n\t\t\tt.Errorf", "\t\tif false && hold {\n\t\t\tt.Errorf")]),
    ("the helper doesn't refuse a Next", HE,
     [("\t\tif next {\n\t\t\tt.Errorf", "\t\tif false && next {\n\t\t\tt.Errorf")]),
    ("the helper refuses a conflict at one level only", HE,
     [("slices.DeleteFunc(next, met)", "slices.DeleteFunc(next, func(r credentialuse.Ref) bool { _, ok := named[r.ID()]; return ok })")]),
    ("the helper refuses a conflict behind a Next only", HE,
     [("\t\t\tif met(r) {\n", "\t\t\tif _, ok := named[r.ID()]; ok {\n")]),
    ("the helper's last ref wins", HE,
     [("\t\t\tif met(r) {\n\t\t\t\tcontinue\n\t\t\t}\n", "\t\t\tmet(r)\n")]),
    ("the helper counts a level of refs already met", HE,
     [("\t\tlevel = slices.DeleteFunc(next, met)\n", "\t\tlevel = next\n")]),
    ("the helper ignores the kind's MaxDepth", HE,
     [("\tmaxDepth := o.MaxDepth\n\tif maxDepth == 0 {", "\tmaxDepth := o.MaxDepth\n\tif true || maxDepth == 0 {")]),
    ("the helper doesn't end at MaxDepth", HE,
     [("\t\tif depth > maxDepth {", "\t\tif depth > maxDepth+16 {")]),
    ("the helper allows a held Next without Content", HE,
     [("if !r.Guaranteed && !content[r.GroupKind] {", "if false && !r.Guaranteed && !content[r.GroupKind] {")]),
    ("the helper wants Content of a Guaranteed Next", HE,
     [("if !r.Guaranteed && !content[r.GroupKind] {", "if !content[r.GroupKind] {")]),
    ("any protected kind has Content", HE,
     [("content[pk.GVK.GroupKind()] = pk.Content", "content[pk.GVK.GroupKind()] = true")]),
    ("the helper follows held refs in their level's order", HE,
     [("\t\tslices.SortFunc(held, func(a, b credentialuse.Ref) int { return compareIDs(a.ID(), b.ID()) })\n", "\t\t_ = compareIDs\n")]),
    ("the helper follows Guaranteed refs first", HE,
     [("append(held, guaranteed...)", "append(guaranteed, held...)")]),
    ("the helper gives a Next the whole object", HE,
     [("r.Next(credentialuse.NextObject(obj))", "r.Next(obj.DeepCopy())")]),
    ("the helper gives Resolved the whole object", HE,
     [("resolved[id] = credentialuse.NextObject(obj)", "resolved[id] = obj")]),
]

CONFLICT = "TestRefsConflicting"
FORGOT = "TestRefsConflictingForgotten"
REPORTED = "TestRefsConflictingReported"
COVER = "TestAssertCredentialRefsCoverRefsThatConflict"
RULES = "TestAssertCredentialRefsCoverEnforcesTheRuntimesRules"
SHAPE = "TestAssertCredentialRefsCoverGivesNextTheRuntimesObject"

KILLERS = {
    "a hold after a Guaranteed ref asks for no more": {C: CONFLICT, H: COVER},
    "a Guaranteed ref after a hold asks for more": {C: CONFLICT, H: COVER},
    "a hold after a hold asks for more": {C: CONFLICT, H: COVER},
    "a Next after none asks for no more": {C: CONFLICT, H: COVER},
    "no Next after one asks for more": {C: CONFLICT, H: COVER},
    "a Next after a Next asks for more": {C: CONFLICT, H: COVER},
    "the last ref wins": {C: CONFLICT},
    "a ref is compared with the last, not the first": {C: CONFLICT},
    "a conflict at one level isn't reported": {C: CONFLICT},
    "a conflict behind a Next isn't reported": {C: CONFLICT},
    "no conflict is reported": {C: CONFLICT},
    "every ref met again is reported": {C: CONFLICT},
    "a hold is reported on every pass": {C: CONFLICT},
    "a Next is reported on every pass": {C: CONFLICT},
    "a Next's report is a hold's": {C: CONFLICT},
    "a report is once for all holders": {C: CONFLICT},
    "a report is once for all objects": {C: CONFLICT},
    "a hold's report forgets the Next's": {C: "TestConflictSet"},
    "a forgotten holder's conflicts stay reported": {C: CONFLICT},
    "a holder with no record isn't forgotten": {C: CONFLICT},
    "a deleted holder's conflicts stay reported": {C: FORGOT},
    "a conflict that is gone stays reported": {C: FORGOT},
    "what is no longer asked for stays reported": {C: FORGOT},
    "one thing no longer asked for stays reported": {C: FORGOT},
    "a holder with nothing reported is kept": {C: "TestConflictSet"},
    "only a pass that ends forgets": {C: FORGOT},
    "only a pass whose last level leads nowhere forgets": {C: FORGOT},
    "a pass that couldn't follow a hop forgets": {C: FORGOT},
    "every pass forgets as it ends": {C: FORGOT},
    "a Disowned kind's hold is reported": {C: REPORTED},
    "a Disowned kind's Next isn't reported": {C: REPORTED},
    "a recorder that is none isn't logged": {R: "TestCredentialUseMetricsRecorderThatIsNone"},
    "no recorder is logged": {R: "TestCredentialUseMetricsRecorderThatIsNone"},
    "a hold isn't counted": {C: "TestRefsConflictingReport"},
    "a Next isn't counted": {C: "TestRefsConflictingReport"},
    "what is asked for is mislabelled": {C: "TestRefsConflictingReport"},
    "a conflict isn't logged": {C: "TestRefsConflictingReport"},
    "the log doesn't name the holder": {C: "TestRefsConflictingReport"},
    "the log doesn't name the object": {C: "TestRefsConflictingReport"},
    "a tracker remembers no conflict": {C: "TestRefsConflictingOfATracker"},
    "a tracker's conflicts aren't logged": {C: "TestRefsConflictingOfATracker"},
    "a tracker's conflicts aren't counted": {C: "TestRefsConflictingOfATracker"},
    "the metric isn't labelled with what was asked": {T: "TestCredentialUseMetrics"},
    "the metric isn't recorded": {T: "TestCredentialUseMetrics"},
    "a lister that returns nothing gives an empty object": {C: "TestChainGuaranteedRefs"},
    "a Next is given the lister's own object": {C: "TestChainNextCannotChangeTheListersObject"},
    "NextObject keeps the status": {H: SHAPE},
    "the helper doesn't refuse a hold": {H: COVER},
    "the helper doesn't refuse a Next": {H: COVER},
    "the helper refuses a conflict at one level only": {H: COVER},
    "the helper refuses a conflict behind a Next only": {H: COVER},
    "the helper's last ref wins": {H: COVER + "|" + RULES},
    "the helper counts a level of refs already met": {H: RULES},
    "the helper ignores the kind's MaxDepth": {H: RULES},
    "the helper doesn't end at MaxDepth": {H: RULES},
    "the helper allows a held Next without Content": {H: RULES},
    "the helper wants Content of a Guaranteed Next": {H: RULES},
    "any protected kind has Content": {H: RULES},
    "the helper follows held refs in their level's order": {H: RULES},
    "the helper follows Guaranteed refs first": {H: RULES},
    "the helper gives a Next the whole object": {H: SHAPE},
    "the helper gives Resolved the whole object": {H: SHAPE},
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
