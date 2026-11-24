"""Revert check for the fix of unserved resources' 404s (design §6.2): break the
rule, and each caller of it, and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. The envtests need KUBEBUILDER_ASSETS.

Usage:
  python3 hack/revert-check-credential-use-unserved.py > hack/revert-check-credential-use-unserved.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
CL, AQ, CS, RL, IX, JN, PB, DS, DP, CH, SV = [C + f for f in ("classify.go", "acquire.go", "consent.go", "release.go", "index.go", "janitor.go", "prober.go", "disown.go", "disownprefix.go", "chain.go", "server.go")]
FILES = [CL, AQ, CS, RL, IX, JN, PB, DS, DP, CH, SV]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("an unserved resource's 404 is NotFound", CL,
     [("\t\tif namesObject(st) {\n", "\t\tif true || namesObject(st) {\n")]),
    ("any cause makes a 404 unserved", CL,
     [("\t\treturn c.Type == metav1.CauseTypeUnexpectedServerResponse\n", "\t\treturn c.Type != \"\"\n")]),
    ("no 404 is NotFound", CL,
     [("\t\tif namesObject(st) {\n", "\t\tif false && namesObject(st) {\n")]),
    ("an event calls an unserved kind's object notfound", DS,
     [("apierrors.IsNotFound(err) && !namesObject(st) {", "apierrors.IsNotFound(err) && !namesObject(st) && false {")]),
    ("a nameless 404 without the cause is NotFound", CL,
     [('st.Details != nil && st.Details.Name != "" && !slices', '(st.Details == nil || st.Details.Name == "" || st.Details.Name != "") && (st.Details == nil || !slices'), ("\t\treturn c.Type == metav1.CauseTypeUnexpectedServerResponse\n\t})\n", "\t\treturn c.Type == metav1.CauseTypeUnexpectedServerResponse\n\t}))\n")]),
    ("a 404 with no details is NotFound", CL,
     [('return st.Details != nil && st.Details.Name != ""', 'return st.Details == nil || st.Details.Name != ""')]),
    ("the cause doesn't matter when the 404 names something", CL,
     [("\t\treturn c.Type == metav1.CauseTypeUnexpectedServerResponse\n", "\t\treturn false && c.Type == metav1.CauseTypeUnexpectedServerResponse\n")]),
    # An object named without its namespace.
    ("an object without its namespace is asked for", SV,
     [('\tif namespaced && namespace == "" {', '\tif false && namespaced && namespace == "" {')]),
    ("a Get asks for an object without its namespace", SV,
     [("\tc, err := s.object(gk, namespace, name)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn c.Get(", "\tc, err := s.client(gk, namespace)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn c.Get(")]),
    ("a patch asks for an object without its namespace", SV,
     [("\tc, err := s.object(gk, namespace, name)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn c.Patch(", "\tc, err := s.client(gk, namespace)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn c.Patch(")]),
    ("the answer for it names nothing", SV,
     [("\t\treturn nil, apierrors.NewNotFound(gvr.GroupResource(), name)\n", "\t\treturn nil, apierrors.NewNotFound(gvr.GroupResource(), \"\")\n")]),
    ("a cluster-scoped object is NotFound", SV,
     [('\tif namespaced && namespace == "" {', '\tif (namespaced || true) && namespace == "" {')]),
    ("a list without a namespace is NotFound", SV,
     [("\tc, err := s.client(gk, namespace)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\tvar out", "\tc, err := s.object(gk, namespace, \"x\")\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\tvar out")]),
    ("content without its namespace is asked for", CH,
     [('\t\tif namespace == "" {\n', '\t\tif false && namespace == "" {\n')]),
    ("cluster-scoped content is NotFound", CH,
     [("\tif m.Scope.Name() == meta.RESTScopeNameNamespace {\n\t\tif namespace", "\tif namespace == \"\" || m.Scope.Name() == meta.RESTScopeNameNamespace {\n\t\tif namespace")]),
    ("the prober lists objects without their namespace", PB,
     [(' && (g.ns != "" || !t.namespaced(g.gk)) {', ' && (true || g.ns != "" || !t.namespaced(g.gk)) {')]),
    ("the prober never lists a kind without a namespace", PB,
     [("\treturn err == nil && m.Scope.Name() == meta.RESTScopeNameNamespace\n", "\treturn err == nil && (true || m.Scope.Name() == meta.RESTScopeNameNamespace)\n")]),
    ("an unheld hop that can't be read ends the chain", AQ,
     [("\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the unheld hop %s: %w\", id, err)\n", "\t\t\treturn nil, nil\n")]),
    ("a disowned hop that can't be read ends the chain", AQ,
     [("\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the disowned hop %s: %w\", id, err)\n", "\t\t\treturn nil, nil\n")]),
    ("an unheld hop that is gone fails the pass", AQ,
     [("\t\tif Classify(err) == ClassNotFound {\n\t\t\treturn nil, nil\n\t\t}\n\t\tif err != nil {\n\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the unheld", "\t\tif err != nil {\n\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the unheld")]),
    ("a disowned hop that is gone fails the pass", AQ,
     [("\t\tif Classify(err) == ClassNotFound {\n\t\t\treturn nil, nil\n\t\t}\n\t\tif err != nil {\n\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the disowned", "\t\tif err != nil {\n\t\t\treturn nil, fmt.Errorf(\"credential-use: reading the disowned")]),
    ("a batch stops at an object it can't read", RL,
     [("\t\t\t\terrs = append(errs, fmt.Errorf(\"credential-use: reading %s: %w\", id, err))\n\t\t\t\tcontinue\n", "\t\t\t\treturn res, fmt.Errorf(\"credential-use: reading %s: %w\", id, err)\n")]),
    ("no event names the kind Disown can't list", DS,
     [("\tt.s.events.record(crd, \"Warning\", EventDisownUnreadable, \"Disown can't list ", "\tif false {\n\t\treturn\n\t}\n\t(*eventSink)(nil).record(crd, \"Warning\", EventDisownUnreadable, \"Disown can't list ")]),
    ("a stopped pass reports the kind it couldn't list", DS,
     [("\tif ctx.Err() != nil {\n\t\treturn\n\t}\n\tcrd :=", "\tcrd :=")]),
    # Each caller, taking the failure for gone.
    ("A2 takes it for Missing", AQ,
     [("\t\t\t\tcase ClassNotFound:\n\t\t\t\t\ta.Unheld", "\t\t\t\tcase ClassNotFound, ClassOther:\n\t\t\t\t\ta.Unheld")]),
    ("BeforeEstablish takes it for Missing", AQ,
     [("\t\t\tcase ClassNotFound:\n\t\t\t\tcontinue\n", "\t\t\tcase ClassNotFound, ClassOther:\n\t\t\t\tcontinue\n")]),
    ("the gate takes it for Missing", CS,
     [("\to, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)\n\tswitch Classify(err) {\n\tcase ClassNone:\n\tcase ClassNotFound:\n",
       "\to, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)\n\tswitch Classify(err) {\n\tcase ClassNone:\n\tcase ClassNotFound, ClassOther:\n")]),
    ("R1 takes it for gone", RL,
     [("\t\t\tcase ClassNotFound:\n\t\t\t\tp.seen.forget(uidOf", "\t\t\tcase ClassNotFound, ClassOther:\n\t\t\t\tp.seen.forget(uidOf")]),
    ("R3 takes it for gone", RL,
     [("\t\t\tcase ClassNotFound:\n\t\t\t\tres.Bare = append(res.Bare, r.id) // gone", "\t\t\tcase ClassNotFound, ClassOther:\n\t\t\t\tres.Bare = append(res.Bare, r.id) // gone")]),
    ("the epoch's repair takes it for gone", RL,
     [("c != ClassStateChanged && c != ClassNotFound {", "c != ClassStateChanged && c != ClassNotFound && c != ClassOther {")]),
    ("a holder list that fails so has no users", RL,
     [("\t\tholders, err := p.srv.List(ctx, p.holder, n)\n\t\tif err != nil {", "\t\tholders, err := p.srv.List(ctx, p.holder, n)\n\t\tif err != nil && Classify(err) != ClassOther {")]),
    ("the index takes it for bare", IX,
     [("\tcase ClassNotFound:\n\t\tix.readable(id)\n\t\treturn carriesNot\n", "\tcase ClassNotFound, ClassOther:\n\t\tix.readable(id)\n\t\treturn carriesNot\n")]),
    ("the startup recheck takes it for gone", IX,
     [("\t\tcase ClassNotFound:\n\t\t\tix.readable(id)\n\t\t\tbare", "\t\tcase ClassNotFound, ClassOther:\n\t\t\tix.readable(id)\n\t\t\tbare")]),
    ("the janitor's read takes it for gone", JN,
     [("\t\tcase ClassNone:\n\t\tcase ClassNotFound:\n", "\t\tcase ClassNone:\n\t\tcase ClassNotFound, ClassOther:\n")]),
    ("the janitor's patch takes it for gone", JN,
     [("\t\t\tcase ClassNotFound:\n", "\t\t\tcase ClassNotFound, ClassOther:\n")]),
    ("the prober takes it for missing", PB,
     [("\t\t\t\tcase ClassNotFound:\n", "\t\t\t\tcase ClassNotFound, ClassOther:\n")]),
    ("Disown's count takes it for gone", DS,
     [("\t\t\tcase ClassNotFound:\n\t\t\t\tcontinue\n", "\t\t\tcase ClassNotFound, ClassOther:\n\t\t\t\tcontinue\n")]),
    ("a switch that can't be read is none", DS,
     [("\tif Classify(err) == ClassNotFound {\n\t\treturn disownSet{}, nil\n", "\tif c := Classify(err); c == ClassNotFound || c == ClassOther {\n\t\treturn disownSet{}, nil\n")]),
    ("a strip takes it for gone", DP,
     [("\t\tcase ClassNotFound:\n\t\t\treturn false, 0, nil\n", "\t\tcase ClassNotFound, ClassOther:\n\t\t\treturn false, 0, nil\n")]),
    ("a strip's second read takes it for gone", DP,
     [("\t\tif Classify(err) == ClassNotFound {\n\t\t\treturn false, 0, nil\n", "\t\tif c := Classify(err); c == ClassNotFound || c == ClassOther {\n\t\t\treturn false, 0, nil\n")]),
    ("DisownPrefix takes it for gone", DP,
     [("\t\t\tif Classify(err) == ClassNotFound {\n\t\t\t\tcontinue\n", "\t\t\tif c := Classify(err); c == ClassNotFound || c == ClassOther {\n\t\t\t\tcontinue\n")]),
    ("Disown doesn't say which kind it can't list", DS,
     [("\tt.warn(\"credential-use: Disown can't list a protected kind;", "\tt.sinkInfo(\"credential-use: Disown can't list a protected kind;")]),
    ("DisownPrefix stops at a kind it can't list", DP,
     [("\t\t\tif err != nil {\n\t\t\t\terrs = append(errs, err)\n\t\t\t\tcontinue\n", "\t\t\tif err != nil {\n\t\t\t\treturn rep, err\n")]),
    ("DisownPrefix stops at an object it can't read", DP,
     [("\t\t\t\terrs = append(errs, fmt.Errorf(\"reading %s: %w\", id, err))\n\t\t\t\tcontinue\n", "\t\t\t\treturn rep, fmt.Errorf(\"reading %s: %w\", id, err)\n")]),
    ("an unheld hop's failed read is remembered", CH,
     [("\tif err != nil && Classify(err) != ClassNotFound {\n", "\tif err != nil && Classify(err) != ClassNotFound && Classify(err) != ClassOther {\n")]),
]

REL = "TestReleaseOfAnUnservedKind"
DIS = "TestDisownAndTheJanitorOfAnUnservedKind"
ENV = "TestAProtectedKindThatStopsBeingServed"
KILLERS = {
    "an unserved resource's 404 is NotFound": {C: "TestAnUnservedResourceIsntNotFound|TestPremiseUnservedResource"},
    "any cause makes a 404 unserved": {C: "TestAnUnservedResourceIsntNotFound"},
    "no 404 is NotFound": {C: "TestAnUnservedResourceIsntNotFound|TestClassify"},
    "an event calls an unserved kind's object notfound": {C: "TestAnUnservedResourceIsntNotFound"},
    "A2 takes it for Missing": {C: "TestAcquireOfAnUnservedKind"},
    "BeforeEstablish takes it for Missing": {C: "TestAcquireOfAnUnservedKind"},
    "the gate takes it for Missing": {C: "TestConsentOfAnUnservedKind"},
    "R1 takes it for gone": {C: REL},
    "R3 takes it for gone": {C: REL},
    "the epoch's repair takes it for gone": {C: REL},
    "a holder list that fails so has no users": {C: REL},
    "the index takes it for bare": {C: "TestIndexLinesOfAnUnservedKind"},
    "the startup recheck takes it for gone": {C: "TestIndexLinesOfAnUnservedKind"},
    "the janitor's read takes it for gone": {C: DIS},
    "the janitor's patch takes it for gone": {C: DIS},
    "the prober takes it for missing": {C: "TestProbeOfAnUnservedKind"},
    "Disown's count takes it for gone": {C: ENV},
    "a switch that can't be read is none": {C: DIS},
    "a strip takes it for gone": {C: DIS},
    "a strip's second read takes it for gone": {C: DIS},
    "DisownPrefix takes it for gone": {C: DIS},
    "Disown doesn't say which kind it can't list": {C: "TestDisownSaysWhichKindItCantList"},
    "DisownPrefix stops at a kind it can't list": {C: "TestDisownPrefixStripsAroundAnUnservedKind"},
    "DisownPrefix stops at an object it can't read": {C: "TestDisownPrefixStripsAroundAnUnservedKind"},
    "a nameless 404 without the cause is NotFound": {C: "TestAnUnservedResourceIsntNotFound|TestPremiseUnservedResource"},
    "a 404 with no details is NotFound": {C: "TestAnUnservedResourceIsntNotFound"},
    "the cause doesn't matter when the 404 names something": {C: "TestAnUnservedResourceIsntNotFound"},
    "an object without its namespace is asked for": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing|TestARefWithoutItsNamespaceIsMissing"},
    "a Get asks for an object without its namespace": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "a patch asks for an object without its namespace": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "the answer for it names nothing": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "a cluster-scoped object is NotFound": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "a list without a namespace is NotFound": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "content without its namespace is asked for": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "cluster-scoped content is NotFound": {C: "TestAnObjectNamedWithoutItsNamespaceIsMissing"},
    "the prober lists objects without their namespace": {C: "TestProbeOfObjectsNamedWithoutTheirNamespace"},
    "the prober never lists a kind without a namespace": {C: "TestProbeOfObjectsNamedWithoutTheirNamespace"},
    "an unheld hop that can't be read ends the chain": {C: "TestAnUnheldHopThatCantBeReadFailsThePass"},
    "a disowned hop that can't be read ends the chain": {C: "TestAnUnheldHopThatCantBeReadFailsThePass"},
    "an unheld hop that is gone fails the pass": {C: "TestAnUnheldHopThatCantBeReadFailsThePass"},
    "a disowned hop that is gone fails the pass": {C: "TestAnUnheldHopThatCantBeReadFailsThePass"},
    "a batch stops at an object it can't read": {C: "TestReleaseAroundAnUnservedObject"},
    "no event names the kind Disown can't list": {C: "TestDisownSaysWhichKindItCantList"},
    "a stopped pass reports the kind it couldn't list": {C: "TestDisownSaysWhichKindItCantList"},
    "an unheld hop's failed read is remembered": {C: "TestAnUnheldHopOfAnUnservedKind"},
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
