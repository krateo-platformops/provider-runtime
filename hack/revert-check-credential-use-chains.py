"""Revert check for the refusal of chains in the first tag
(docs/plan-credential-use-implementation.md, "The first tag refuses chains";
design §9, "Not in this release"), after the gate of what the record names
(§11.3): break each rule in turn and see whether a
test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant names, in KILLERS, the tests to run in each package: {package:
tests}. The baseline runs every named test of every package. None is an
envtest, so the runs unset KUBEBUILDER_ASSETS.

Usage:
  python3 hack/revert-check-credential-use-chains.py > hack/revert-check-credential-use-chains.txt

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
AQ = C + "acquire.go"
CH = C + "chain.go"
TR = C + "tracker.go"
SH = C + "shared.go"
R = "pkg/reconciler/"
RC = R + "credentialuse.go"
RT = "pkg/reconciler/reconcilertest/"
RTC = RT + "credentialuse.go"
IN = C + "informers.go"
FILES = [AQ, CH, TR, SH, IN, RC, RTC]


def sub(s, old, new):
    """s with its one old replaced: a mutant built from a string that no
    longer has it would change nothing."""
    if s.count(old) != 1:
        sys.exit(f"{s.count(old)} of {old!r} in {s!r}")
    return s.replace(old, new)


IF = "if refused := p.refuseChains(refs); refused != nil {"
FAIL = "return fail(errors.Join(refused, err), reasonChainsRefused)"
APART = "\t\tif a.gateErr = err; err != nil && established {\n\t\t\tp.reports.Degraded(p.holder, \"Announce\")\n\t\t}\n"
REFUSE = "\t" + IF + "\n" + APART + "\t\t" + FAIL + "\n\t}\n"
FIRST = "\t" + IF + "\n\t\treturn fail(refused, reasonChainsRefused)\n\t}\n"
GATE = "\t// The gate of what the record names, first (§11.3)."
EACH = "\t\tif r.Next != nil {\n\t\t\treturn chainRefused{r.ID()}\n\t\t}\n"
BEFORE = "\tfor _, u := range a.Unheld {\n\t\tif err := p.refuseChains([]Ref{u.Ref}); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n"

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # After the gate of what the record names, and only after it (§11.3).
    ("a chain is refused before the gate", AQ, [(GATE, FIRST + GATE)]),
    ("a new holder's chain is refused before the gate", AQ,
     [(GATE, sub(FIRST, "refused != nil {", "refused != nil && !established {") + GATE)]),
    ("the gate's error hides the refusal", AQ, [(REFUSE, "\tif err != nil {\n\t\treturn fail(err, \"Announce\")\n\t}\n" + REFUSE)]),
    ("the refusal hides the gate's error", AQ, [(REFUSE, sub(REFUSE, "errors.Join(refused, err)", "refused"))]),
    # Refused, in full, before any ref is looked at.
    ("a chain isn't refused", AQ, [(REFUSE, "")]),
    ("a chain is refused after its level's A1", AQ,
     [(REFUSE, ""),
      ("\t\t// A2. Write P, for each ref of this level.", REFUSE + "\t\t// A2. Write P, for each ref of this level.")]),
    ("a chain's level is refused when it is reached, past the gate and Disown", AQ,
     [(REFUSE, ""),
      ("\t\t// A1. Announce: exactly the chain", REFUSE + "\t\t// A1. Announce: exactly the chain")]),
    ("the refs without a Next are held, and the others dropped", AQ,
     [(REFUSE, "\trefs = slices.DeleteFunc(slices.Clone(refs), func(r Ref) bool { return !p.followChains && r.Next != nil })\n")]),
    ("only the first ref is checked", CH,
     [("\tfor _, r := range refs {\n\t\tif r.Next != nil {\n\t\t\treturn chainRefused",
       "\tfor _, r := range refs[:min(1, len(refs))] {\n\t\tif r.Next != nil {\n\t\t\treturn chainRefused")]),
    ("a Guaranteed ref may have a Next", CH,
     [(EACH, sub(EACH, "r.Next != nil", "r.Next != nil && !r.Guaranteed"))]),
    ("a second ref to one object isn't checked", CH,
     [("\tfor _, r := range refs {\n\t\tif r.Next != nil {\n\t\t\treturn chainRefused",
       "\tmet := map[ID]bool{}\n\tfor _, r := range refs {\n\t\tif met[r.ID()] {\n\t\t\tcontinue\n\t\t}\n\t\tmet[r.ID()] = true\n\t\tif r.Next != nil {\n\t\t\treturn chainRefused")]),
    ("a disowned kind's ref may have a Next", CH,
     [(EACH, sub(EACH, "r.Next != nil", "r.Next != nil && !p.isDisowned(r.GroupKind)"))]),
    ("a kind with Content may have a Next", CH,
     [(EACH, sub(EACH, "r.Next != nil", "r.Next != nil && !p.contentKinds[r.GroupKind]"))]),
    ("a deleting holder isn't refused", AQ,
     [(REFUSE, sub(REFUSE, "refused != nil {", "refused != nil && !deleting {"))]),
    ("an established holder isn't refused", AQ,
     [(REFUSE, sub(REFUSE, "refused != nil {", "refused != nil && !established {"))]),
    # What the holder sees.
    ("an established holder must not connect", AQ,
     [(REFUSE, sub(REFUSE, FAIL, "a.Err = refused\n\t\ta.MustNotConnect = true\n\t\treturn a, nil"))]),
    ("a new holder connects", AQ,
     [(REFUSE, sub(REFUSE, FAIL, "a.Err = refused\n\t\ta.Degraded = true\n\t\treturn a, nil"))]),
    ("the refusal isn't reported as degraded", AQ,
     [(REFUSE, sub(REFUSE, FAIL, "a.Err = refused\n\t\ta.Degraded, a.MustNotConnect = established, !established\n\t\treturn a, nil"))]),
    ("the refusal has no reason of its own", AQ,
     [(REFUSE, sub(REFUSE, "reasonChainsRefused", "\"Refs\""))]),
    ("the refusal is Acquire's error", AQ,
     [(REFUSE, sub(REFUSE, FAIL, "return a, refused"))]),
    ("a refused pass empties the record", AQ,
     [(REFUSE, sub(REFUSE, FAIL, "_, _ = p.announce(ctx, obj, nil, nil, &a)\n\t\t" + FAIL))]),
    ("the error doesn't name the ref", CH,
     [(EACH, sub(EACH, "chainRefused{r.ID()}", "ErrChainsNotSupported"))]),
    ("the error doesn't wrap the sentinel", CH,
     [("func (chainRefused) Unwrap() error { return ErrChainsNotSupported }", "func (chainRefused) Unwrap() error { return nil }")]),
    ("the error has the ref whole", CH, [("quotedLine(e.id))", "e.id)")]),
    ("the log line hasn't the ref whole", TR, [("\t\tkv = append(kv, \"ref\", chain.id.String()) // whole: the error cuts it\n", "\t\t_ = chain.id\n")]),
    # Its event and log line, once per holder.
    ("a joined error is suppressed as a refusal", TR,
     [("\trefused := errors.Is(a.Err, ErrChainsNotSupported) && a.gateErr == nil\n", "\trefused := errors.Is(a.Err, ErrChainsNotSupported)\n")]),
    ("a refused pass's failed gate isn't counted", AQ, [(APART, "\t\ta.gateErr = err\n")]),
    ("a refused new holder's failed gate is counted as degraded", AQ, [(APART, sub(APART, "err != nil && established", "err != nil"))]),
    ("the event is throttled only", TR, [("\tif refused && !t.refused.first(key) {\n\t\treturn\n\t}\n", "")]),
    ("the event is hidden by another failure's log line", TR, [("\tif refused && !t.refused.first(key) {", "\tif refused && !t.degradedLogged.first(key) {")]),
    ("the refusal hides another failure's event", TR, [("\tif refused && !t.refused.first(key) {", "\tif !t.refused.first(key) {")]),
    ("a whole pass doesn't clear the report", TR, [("\t\tt.refused.clear(key)\n", "")]),
    ("Forget doesn't clear the report", TR, [("\tt.refused.forget(obj.GetUID())\n", "")]),
    ("a holder's deletion leaves the report", IN, [("\th.t.refused.forget(m.UID)\n", "")]),
    ("the error doesn't say what to do", CH, [(", without a Next; ", "; ")]),
    ("a refused pass leaves the prober its refs", TR,
     [("\tt.noteMissing(obj, a)\n", "\tif !errors.Is(a.Err, ErrChainsNotSupported) {\n\t\tt.noteMissing(obj, a)\n\t}\n")]),
    ("a refused pass records no event", TR,
     [("\tt.noteMissing(obj, a)\n\tt.reportOutcome(obj, a)\n",
       "\tt.noteMissing(obj, a)\n\tif !errors.Is(a.Err, ErrChainsNotSupported) {\n\t\tt.reportOutcome(obj, a)\n\t}\n")]),
    # BeforeEstablish.
    ("BeforeEstablish establishes after a refused pass", AQ,
     [("\tif errors.Is(a.Err, ErrChainsNotSupported) {\n\t\treturn a.Err\n\t}\n", "")]),
    ("BeforeEstablish holds a ref with a Next", AQ, [(BEFORE, "")]),
    ("BeforeEstablish checks a ref only when it reaches it", AQ,
     [(BEFORE, ""),
      ("\t\tif u.Reason != ReasonMissing {\n\t\t\tcontinue\n\t\t}\n\t\tid := u.Ref.ID()\n",
       "\t\tif u.Reason != ReasonMissing {\n\t\t\tcontinue\n\t\t}\n\t\tif err := p.refuseChains([]Ref{u.Ref}); err != nil {\n\t\t\treturn err\n\t\t}\n\t\tid := u.Ref.ID()\n")]),
    # A list without a Next is the whole chain.
    ("a held direct ref is read", AQ, [("\t\t\tif held && r.Next != nil {", "\t\t\tif held {")]),
    ("a Guaranteed ref without a Next is read", AQ,
     [("\t\t\tif r.Next == nil {\n\t\t\t\tcontinue\n\t\t\t}\n", "")]),
    ("an unheld direct ref is read", AQ,
     [("\t\tnotHeld[id] = reason\n\t\tif r.Next == nil {", "\t\tnotHeld[id] = reason\n\t\tif false {")]),
    ("a disowned direct ref is read", AQ,
     [("Reason: ReasonDisowned})\n\t\tif r.Next == nil {", "Reason: ReasonDisowned})\n\t\tif false {")]),
    # The switch.
    ("production's trackers follow chains", TR,
     [("\t\tfollowChains:    s.hooks.followChains,\n", "\t\tfollowChains:    true,\n")]),
    ("an option turns chains on", TR,
     [("\t\tfollowChains:    s.hooks.followChains,\n", "\t\tfollowChains:    s.hooks.followChains || o.MaxDepth > 3,\n")]),
    ("a Shared turns chains on", SH,
     [("\ts.syncTimeout.Store(int64(2 * time.Minute))\n", "\ts.syncTimeout.Store(int64(2 * time.Minute))\n\ts.hooks.followChains = true\n")]),
    ("Track turns chains on for a kind with Content", TR,
     [("\t\t\tt.proto.contentKinds[pk.GVK.GroupKind()] = true\n",
       "\t\t\tt.proto.contentKinds[pk.GVK.GroupKind()] = true\n\t\t\tt.proto.followChains = true\n")]),
    ("the tests' switch doesn't reach the tracker", TR,
     [("\t\tfollowChains:    s.hooks.followChains,\n", "")]),
    # The chain code stays under test, through the reconciler too.
    ("no resolved chain on Connect's context", RC,
     [("\texternalCtx = credentialuse.WithResolved(externalCtx, a)\n", "")]),
    # reconcilertest.
    ("AssertCredentialRefsCover follows chains", RTC,
     [("\t\tif r.Next != nil && !followChains {", "\t\tif false {")]),
    ("AssertCredentialRefsCover follows chains by default", RTC,
     [("\nvar followChains bool\n", "\nvar followChains = true\n")]),
    ("AssertCredentialRefsCover checks the first ref alone", RTC,
     [("\tfor _, r := range refs {\n\t\tif r.Next != nil && !followChains {",
       "\tfor _, r := range refs[:min(1, len(refs))] {\n\t\tif r.Next != nil && !followChains {")]),
    ("AssertCredentialRefsCover lets a Guaranteed ref have a Next", RTC,
     [("\t\tif r.Next != nil && !followChains {", "\t\tif r.Next != nil && !followChains && !r.Guaranteed {")]),
    ("AssertCredentialRefsCover reports a chain, and connects", RTC,
     [("\t\t\tt.Fatalf(\"%v: ref %s has a Next\", credentialuse.ErrChainsNotSupported, r.ID())\n\t\t\treturn nil, nil\n",
       "\t\t\tt.Errorf(\"%v: ref %s has a Next\", credentialuse.ErrChainsNotSupported, r.ID())\n")]),
]

REFUSED = {C: "TestAcquireRefusesAChainBeforeAnyWrite"}
WHOLE = {C: "TestDirectRefsAreNotRefused|TestNoRefsPastTheFirstLevelWithoutANext"}
PRODUCTION = {C: "TestTheTrackerRefusesChains|TestNothingInProductionFollowsChains", R: "TestCredentialUseRefusesAChain"}
SCANNED = {C: "TestNothingInProductionFollowsChains"}
COVER = {RT: "TestAssertCredentialRefsCoverRefusesAChain"}

GATED = {C: "TestARefusedPassGatesWhatTheRecordNames"}
BOTH = {C: "TestARefusedPassGatesWhatTheRecordNames|TestKeptRefsInAnotherNamespaceAreGated"}
ONCE = {C: "TestARefusedChainIsReportedOnce"}

KILLERS = {
    "a chain is refused before the gate": BOTH,
    "a new holder's chain is refused before the gate": BOTH,
    "the gate's error hides the refusal": GATED,
    "the refusal hides the gate's error": GATED,
    "a joined error is suppressed as a refusal": {C: "TestARefusedChainsGateFailureIsReported"},
    "a refused pass's failed gate isn't counted": {C: "TestARefusedChainsGateFailureIsReported|TestARefusedPassGatesWhatTheRecordNames"},
    "a refused new holder's failed gate is counted as degraded": GATED,
    "the event is throttled only": ONCE,
    "the event is hidden by another failure's log line": ONCE,
    "the refusal hides another failure's event": ONCE,
    "a whole pass doesn't clear the report": ONCE,
    "Forget doesn't clear the report": ONCE,
    "a holder's deletion leaves the report": ONCE,
    "a chain isn't refused": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestTheTrackerRefusesChains|TestARefusedChainKeepsWhatItHeld|TestFollowChainsReachesTheTracker", R: "TestCredentialUseRefusesAChain"},
    "a chain is refused after its level's A1": REFUSED,
    "a chain's level is refused when it is reached, past the gate and Disown": REFUSED,
    "the refs without a Next are held, and the others dropped": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestARefusedChainKeepsWhatItHeld", R: "TestCredentialUseRefusesAChain"},
    "only the first ref is checked": REFUSED,
    "a Guaranteed ref may have a Next": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestFollowChainsReachesTheTracker", R: "TestCredentialUseRefusesAChain"},
    "a second ref to one object isn't checked": REFUSED,
    "a disowned kind's ref may have a Next": REFUSED,
    "a kind with Content may have a Next": REFUSED,
    "a deleting holder isn't refused": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestARefusedChainKeepsWhatItHeld"},
    "an established holder isn't refused": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestARefusedChainKeepsWhatItHeld"},
    "an established holder must not connect": {C: "TestAcquireRefusesAChainBeforeAnyWrite", R: "TestCredentialUseRefusesAChain"},
    "a new holder connects": {C: "TestAcquireRefusesAChainBeforeAnyWrite", R: "TestCredentialUseRefusesAChain"},
    "the refusal isn't reported as degraded": REFUSED,
    "the refusal has no reason of its own": REFUSED,
    "the refusal is Acquire's error": REFUSED,
    "a refused pass empties the record": {C: "TestARefusedChainKeepsWhatItHeld", R: "TestCredentialUseRefusesAChain"},
    "the error doesn't name the ref": {C: "TestAcquireRefusesAChainBeforeAnyWrite|TestTheTrackerRefusesChains", R: "TestCredentialUseRefusesAChain"},
    "the error doesn't wrap the sentinel": {C: "TestAcquireRefusesAChainBeforeAnyWrite", R: "TestCredentialUseRefusesAChain"},
    "the error has the ref whole": {C: "TestARefusalOfALongName"},
    "the log line hasn't the ref whole": {C: "TestARefusalOfALongName"},
    "the error doesn't say what to do": {C: "TestAcquireRefusesAChainBeforeAnyWrite", R: "TestCredentialUseRefusesAChain"},
    "a refused pass leaves the prober its refs": {C: "TestTheTrackerRefusesChains"},
    "a refused pass records no event": {C: "TestTheTrackerRefusesChains"},
    "BeforeEstablish establishes after a refused pass": {C: "TestBeforeEstablishRefusesAChain"},
    "BeforeEstablish holds a ref with a Next": {C: "TestBeforeEstablishRefusesAChain"},
    "BeforeEstablish checks a ref only when it reaches it": {C: "TestBeforeEstablishRefusesAChain"},
    "a held direct ref is read": WHOLE,
    "a Guaranteed ref without a Next is read": WHOLE,
    "an unheld direct ref is read": {C: "TestNoRefsPastTheFirstLevelWithoutANext"},
    "a disowned direct ref is read": {C: "TestNoRefsPastTheFirstLevelWithoutANext"},
    "production's trackers follow chains": PRODUCTION,
    "an option turns chains on": SCANNED,
    "a Shared turns chains on": PRODUCTION,
    "Track turns chains on for a kind with Content": SCANNED,
    "the tests' switch doesn't reach the tracker": {C: "TestFollowChainsReachesTheTracker|TestCredentialUseResolvedOnConnectsContext|TestNothingInProductionFollowsChains"},
    "no resolved chain on Connect's context": {C: "TestCredentialUseResolvedOnConnectsContext"},
    "AssertCredentialRefsCover follows chains": COVER,
    "AssertCredentialRefsCover follows chains by default": {C: "TestNothingInProductionFollowsChains", RT: "TestAssertCredentialRefsCoverRefusesAChain"},
    "AssertCredentialRefsCover checks the first ref alone": COVER,
    "AssertCredentialRefsCover lets a Guaranteed ref have a Next": COVER,
    "AssertCredentialRefsCover reports a chain, and connects": COVER,
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
    env = {k: v for k, v in os.environ.items() if k != "KUBEBUILDER_ASSETS"}
    last = Run(0, "", "")
    for pkg, tests in sorted(per_pkg.items()):
        r = subprocess.run(["go", "test", "-v", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", anchored(tests), "./" + pkg],
                           cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
        last = Run(r.returncode, r.stdout, r.stderr)
        if r.returncode != 0:
            return last
        missing = [t for t in tests.split("|") if not re.search(rf"^--- PASS: {t} ", r.stdout, re.M)]
        if missing:
            return Run(1, "", f"tests that didn't run: {missing}")
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
    if "tests that didn't run" in out:
        return "NOT-RUN", out
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


def on_signal(signum, _frame):
    if restore:
        open(restore[0], "w").write(restore[1])
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

if os.environ.get("REVERT_CHECK_ONLY"):
    check_only()
base = go_test(ALL)
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print(f"baseline passed, commit {commit()}\n")

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
