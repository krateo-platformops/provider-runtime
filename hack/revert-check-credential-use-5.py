"""Revert check for pkg/credentialuse, PR 5 of
docs/plan-credential-use-implementation.md (the cross-namespace consent
gate, the unheld annotation, the shared read of an unheld hop, and the
acquirer's own release batch): break each rule the PR adds in turn and see whether a test
notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant runs the tests named for it in KILLERS, and the baseline runs
them all; some are envtests, so KUBEBUILDER_ASSETS must be set (CI=1 makes an
envtest fail rather than skip without it).

Usage:
  python3 hack/revert-check-credential-use-5.py > hack/revert-check-credential-use-5.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["acquire.go", "consent.go", "unheld.go", "informers.go", "chain.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("the object's list is ignored", 'consent.go',
     [('\tif slices.Contains(obj, "*") || slices.Contains(obj, holderNS) {',
       '\tif false {\n\t\t_ = obj')]),
    ('* on a Namespace counts', 'consent.go',
     [('\treturn slices.Contains(consentList(nsList), holderNS)',
       '\treturn slices.Contains(consentList(nsList), holderNS) || slices.Contains(consentList(nsList), "*")')]),
    ("* on an object doesn't count", 'consent.go',
     [('\tif slices.Contains(obj, "*") || slices.Contains(obj, holderNS) {',
       '\tif slices.Contains(obj, holderNS) {')]),
    ("an object's list narrows its Namespace's", 'consent.go',
     [('\treturn slices.Contains(consentList(nsList), holderNS)',
       '\treturn len(obj) == 0 && slices.Contains(consentList(nsList), holderNS)')]),
    ('a skipping pass reads consent live', 'consent.go',
     [('\tif cachedOK && p.namespaceViews != nil {',
       '\tif false && cachedOK && p.namespaceViews != nil {')]),
    ('a cached no drops without a live read', 'consent.go',
     [('\t\t\t\treturn gateHoldCached\n\t\t\t}\n\t\t}\n\t}',
       '\t\t\t\treturn gateHoldCached\n\t\t\t}\n\t\t\treturn gateNotHeld\n\t\t}\n\t}')]),
    ('a deleting holder reads consent from the cache', 'acquire.go',
     [('\t\t\tswitch g := p.gate(ctx, obj, id, rec0.Names(id) && !deleting); g {',
       '\t\t\tswitch g := p.gate(ctx, obj, id, rec0.Names(id)); g {')]),
    ('an A2 rests on a cached consent', 'acquire.go',
     [('\t\t\tif !held && cross[id] && !liveOK[id] {',
       '\t\t\tif false && !held && cross[id] && !liveOK[id] {')]),
    ('a failed read drops the hold', 'acquire.go',
     [('\t\t\t\tif rec0.Names(id) {\n\t\t\t\t\t// A failed read keeps the hold (A5)',
       '\t\t\t\tif false {\n\t\t\t\t\t// A failed read keeps the hold (A5)')]),
    ("the backoff doesn't double", 'consent.go',
     [('\tnext := 2 * d\n',
       '\tnext := d\n')]),
    ("a read that succeeds doesn't reset the backoff", 'acquire.go',
     [('\t\tif liveOK[id] {\n\t\t\tp.backoff.succeed(consentKey{obj.GetUID(), id})\n\t\t}',
       '\t\t_ = liveOK[id]')]),
    ('NotFound is retried', 'consent.go',
     [('\tns, err := p.srv.Get(ctx, namespaceGroupKind, "", id.Namespace)\n\tswitch Classify(err) {\n\tcase ClassNone:\n\tcase ClassNotFound:\n\t\treturn gateMissing',
       '\tns, err := p.srv.Get(ctx, namespaceGroupKind, "", id.Namespace)\n\tswitch Classify(err) {\n\tcase ClassNone:\n\tcase ClassNotFound:\n\t\treturn gateUnknown')]),
    ('a missing cross-namespace ref gets an A2', 'acquire.go',
     [('\t\t\tcase gateMissing:\n\t\t\t\t// Nothing to hold;',
       '\t\t\tcase gateMissing:\n\t\t\t\ttoHold = append(toHold, id)\n\t\t\t\t// Nothing to hold;')]),
    ('no batch after a cross-namespace A2', 'acquire.go',
     [('\t\t\t\t\tif cross[id] && p.enqueueBatch != nil {',
       '\t\t\t\t\tif false && cross[id] && p.enqueueBatch != nil {')]),
    ('a withdrawal is reported as never consented', 'acquire.go',
     [('\t\tcase rec0.Names(id):\n\t\t\treason = UnheldConsentWithdrawn',
       '\t\tcase false:\n\t\t\treason = UnheldConsentWithdrawn')]),
    ('RefNotHeld is reported on every pass', 'acquire.go',
     [('\t\t\tif !listed[unheldRef(id)] {',
       '\t\t\tif true {')]),
    ('since moves on every pass', 'consent.go',
     [('\t\tcase had && (e.Reason == reason || reason == UnheldNotConsented):',
       '\t\tcase had && false:')]),
    ('an entry outlives its hold', 'consent.go',
     [('\tfor id := range keep {\n\t\tif e, had := prev[unheldRef(id)]; had {',
       '\tfor _, e := range prev {\n\t\tif true {')]),
    ('unheld is written in a patch of its own', 'acquire.go',
     [('\t\tif last := !chained && len(next) == 0; last {',
       '\t\tif last := !chained && len(next) == 0 && false; last {')]),
    ("unheld isn't capped", 'unheld.go',
     [('\tif len(es) > maxUnheld {',
       '\tif false {')]),
    ('NotConsented is kept ahead of withdrawals', 'unheld.go',
     [('\t\tif e.Reason == UnheldNotConsented {\n\t\t\treturn 1\n\t\t}\n\t\treturn 0',
       '\t\tif e.Reason == UnheldNotConsented {\n\t\t\treturn 0\n\t\t}\n\t\treturn 1')]),
    ("an unheld hop isn't followed", 'acquire.go',
     [('\t\tif r.Next == nil {\n\t\t\treturn nil, nil\n\t\t}\n\t\to, err := p.unheldReads.get(ctx, p.contents, id)',
       '\t\tif true {\n\t\t\treturn nil, nil\n\t\t}\n\t\to, err := p.unheldReads.get(ctx, p.contents, id)')]),
    ("an unheld hop's read isn't shared", 'chain.go',
     [('\tif e, ok := s.m[id]; ok && now.Sub(e.at) < unheldReadTTL {',
       '\tif e, ok := s.m[id]; ok && false {\n\t\t_ = e')]),
    ('a withdrawn ref stays with an unfollowed hop', 'acquire.go',
     [('\t\treturn slices.DeleteFunc(want, func(id ID) bool { _, dropped := notHeld[id]; return dropped })',
       '\t\treturn want')]),
    ("a consent change doesn't enqueue the holders", 'informers.go',
     [('\tif lost || consentChanged || h.p.content',
       '\t_ = consentChanged\n\tif lost || h.p.content')]),
    ("a consent change counts as the tracker's own", 'informers.go',
     [('\t\treturn isCredentialUseKey(k) && k != AnnotationAllowHoldersFrom',
       '\t\treturn isCredentialUseKey(k)')]),
]

EQUIVALENT = {
    # A deleting pass never skips its A2 (§6.3), and every A2 of a
    # cross-namespace ref follows a live consent read in the same pass
    # (§11.3 rule 1): with the cache read at the gate, the live read still
    # comes before any write, at A2 time. The explicit rule stays, for clarity.
    'a deleting holder reads consent from the cache': 'a deleting pass reads consent live at A2 time anyway',
}

# The tests that must kill each mutant: each runs the tests aimed at its
# rule, and the baseline runs them all.
KILLERS = {
    "the object's list is ignored": 'TestConsentSources',
    '* on a Namespace counts': 'TestConsentSources',
    "* on an object doesn't count": 'TestConsentSources',
    "an object's list narrows its Namespace's": 'TestConsentSources',
    'a skipping pass reads consent live': 'TestConsentGateOnEveryPass',
    'a cached no drops without a live read': 'TestConsentGateOnEveryPass',
    'a deleting holder reads consent from the cache': 'TestConsentGateOnEveryPass',
    'an A2 rests on a cached consent': 'TestConsentGateOnEveryPass',
    'a failed read drops the hold': 'TestConsentReadFailures',
    "the backoff doesn't double": 'TestConsentReadFailures',
    "a read that succeeds doesn't reset the backoff": 'TestConsentReadFailures',
    'NotFound is retried': 'TestConsentReadFailures',
    'a missing cross-namespace ref gets an A2': 'TestConsentReadFailures',
    'no batch after a cross-namespace A2': 'TestConsentWithdrawalDropsTheHold',
    'a withdrawal is reported as never consented': 'TestConsentWithdrawalDropsTheHold',
    'RefNotHeld is reported on every pass': 'TestConsentNotHeldIsReportedOnceAndConnectsAsToday',
    'since moves on every pass': 'TestConsentNotHeldIsReportedOnceAndConnectsAsToday|TestConsentWithdrawalDropsTheHold',
    'an entry outlives its hold': 'TestConsentWithdrawalDropsTheHold|TestUnheldAnnotation',
    'unheld is written in a patch of its own': 'TestConsentWithdrawalDropsTheHold|TestOneWriteForANewHoldAndANewUnheldEntry',
    "unheld isn't capped": 'TestUnheldAnnotation',
    'NotConsented is kept ahead of withdrawals': 'TestUnheldAnnotation',
    "an unheld hop isn't followed": 'TestConsentChains',
    "an unheld hop's read isn't shared": 'TestConsentChains',
    'a withdrawn ref stays with an unfollowed hop': 'TestAWithdrawnRefLeavesTheRecordEvenWithAHopUnfollowed',
    "a consent change doesn't enqueue the holders": 'TestConsentWithdrawalReleasesTheHold',
    "a consent change counts as the tracker's own": 'TestForeignChange',
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
