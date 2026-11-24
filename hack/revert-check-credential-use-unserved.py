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
CL, AQ, CS, RL, IX, JN, PB, DS, DP, CH = [C + f for f in ("classify.go", "acquire.go", "consent.go", "release.go", "index.go", "janitor.go", "prober.go", "disown.go", "disownprefix.go", "chain.go")]
FILES = [CL, AQ, CS, RL, IX, JN, PB, DS, DP, CH]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("an unserved resource's 404 is NotFound", CL,
     [("\t\tif unserved(st) {\n", "\t\tif false && unserved(st) {\n")]),
    ("any cause makes a 404 unserved", CL,
     [("\t\treturn c.Type == metav1.CauseTypeUnexpectedServerResponse\n", "\t\treturn c.Type != \"\"\n")]),
    ("no 404 is NotFound", CL,
     [("\t\tif unserved(st) {\n", "\t\tif true || unserved(st) {\n")]),
    ("an event calls an unserved kind's object notfound", DS,
     [("ok && unserved(st) && apierrors.IsNotFound(err)", "ok && unserved(st) && false")]),
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
     [("\t\t\tt.warn(\"credential-use: Disown can't list a protected kind;", "\t\t\tt.sinkInfo(\"credential-use: Disown can't list a protected kind;")]),
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
