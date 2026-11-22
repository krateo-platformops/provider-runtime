"""Revert check for pkg/credentialuse, PR 6 of
docs/plan-credential-use-implementation.md (Disown and DisownPrefix: the
switch on the holder CRD, Acquire's Disowned refs, the strip loop, the live
count and its settling): break each rule the PR adds in turn and see whether
a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant runs the tests named for it in KILLERS, and the baseline runs
them all; some are envtests, so KUBEBUILDER_ASSETS must be set (CI=1 makes an
envtest fail rather than skip without it).

Usage:
  python3 hack/revert-check-credential-use-6.py > hack/revert-check-credential-use-6.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["acquire.go", "disown.go", "disownprefix.go", "patch.go", "shared.go", "tracker.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("all doesn't cover every kind", 'disown.go',
     [('func (d disownSet) covers(gk schema.GroupKind) bool { return d.all || d.kinds[gk] }',
       'func (d disownSet) covers(gk schema.GroupKind) bool { return d.kinds[gk] }')]),
    ("a listed kind's group is dropped", 'disown.go',
     [('\t\t\td.kinds[schema.GroupKind{Group: group, Kind: kind}] = true',
       '\t\t\t_ = group\n\t\t\td.kinds[schema.GroupKind{Kind: kind}] = true')]),
    ('a disowned ref is held', 'acquire.go',
     [('\t\t\tif p.isDisowned(r.GroupKind) {\n\t\t\t\tmore, err := disown(r)',
       '\t\t\tif false {\n\t\t\t\tmore, err := disown(r)')]),
    ('no check just before each A2', 'acquire.go',
     [('\t\t\tif p.isDisowned(id.GroupKind()) {\n\t\t\t\t// The switch was set after',
       '\t\t\tif false {\n\t\t\t\t// The switch was set after')]),
    ('a disowned ref stays recorded', 'acquire.go',
     [('\t\treturn dropped || disowned[id] || p.isDisowned(id.GroupKind())',
       '\t\treturn dropped')]),
    ("a disowned hop isn't followed", 'acquire.go',
     [('\t\ta.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonDisowned})\n\t\tif r.Next == nil {',
       '\t\ta.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonDisowned})\n\t\tif true {')]),
    ('the intermediate A1 writes back disowned refs', 'acquire.go',
     [('\t\t\twant = SortedIDs(append(want, slices.DeleteFunc(slices.Clone(rec.IDs), unrecorded)...))',
       '\t\t\twant = SortedIDs(append(want, rec.IDs...))')]),
    ('BeforeEstablish holds a disowned ref', 'acquire.go',
     [('\t\tif p.isDisowned(id.GroupKind()) {\n\t\t\tcontinue // the switch',
       '\t\tif false {\n\t\t\tcontinue // the switch')]),
    ("the CRD informer isn't started by RunInformers", 'shared.go',
     [('\t\tinfs = append(infs, t.informer(), t.crdInformer())',
       '\t\tinfs = append(infs, t.informer())')]),
    ("a protected object's holds aren't counted", 'disown.go',
     [('\t\t\tcount += n\n\t\t\tobjs = append(objs, object{gk, &items[i], n})',
       '\t\t\tobjs = append(objs, object{gk, &items[i], n})')]),
    ("a holder's record isn't counted", 'disown.go',
     [('\t\tif recordsDisowned(&holders[i], set) {\n\t\t\tcount++',
       '\t\tif false && recordsDisowned(&holders[i], set) {\n\t\t\tcount++')]),
    ('a failed list counts as known', 'disown.go',
     [('\t\t\tknown = false\n\t\t\tcontinue',
       '\t\t\tcontinue')]),
    ('an unknown count is written as zero', 'disown.go',
     [('\tcur := "unknown"\n\tif known {',
       '\tcur := "0"\n\tif known {')]),
    ("another process's count doesn't reset the run", 'disown.go',
     [('\t\tif prev != "" && prev != "0" {',
       '\t\tif false {')]),
    ('settled after one zero pass', 'disown.go',
     [('\tsettled := cur == "0" && t.disown.zeros >= t.o.DisownPasses',
       '\tsettled := cur == "0"')]),
    ('an unchanged count is written', 'disown.go',
     [('\t\tif prev == cur && (ann[AnnotationDisownSettled] == "true") == settled {',
       '\t\tif false {')]),
    ('the switch off leaves the count', 'disown.go',
     [('\t\t\t\ts.disownOff(ctx, t, now)',
       '\t\t\t\t_ = now')]),
    ("the switch's change doesn't enqueue the holders", 'disown.go',
     [('\tif !ok1 || !ok2 || o.Annotations[AnnotationDisown] == n.Annotations[AnnotationDisown] {',
       '\tif true {\n\t\t_, _, _, _ = o, n, ok1, ok2')]),
    ('one kind strips the whole record', 'disown.go',
     [('\t\tcase set.all || rec.Unparseable || len(keep) == 0:',
       '\t\tcase true:')]),
    ('all keeps the unheld list', 'disown.go',
     [('\tif set.all && hasUnheld {',
       '\tif false && hasUnheld {')]),
    ('the strip keeps the epochs', 'patch.go',
     [('\t\tif _, ok := annotations[finalizers[i]]; ok {',
       '\t\tif false {\n\t\t\t_ = annotations')]),
    ('the strip keeps the marker', 'patch.go',
     [('\tif hasMarker {\n\t\tops = append(ops, jsonPatchOp{Op: "remove", Path: "/metadata/labels/" + escapePointer(marker)})',
       '\tif false {\n\t\tops = append(ops, jsonPatchOp{Op: "remove", Path: "/metadata/labels/" + escapePointer(marker)})')]),
    ("the held label goes despite another kind's finalizer", 'patch.go',
     [('\tremoveHeld := hasHeld && !otherOfAny',
       '\tremoveHeld := hasHeld\n\t_ = otherOfAny')]),
    ("the strip isn't guarded by the version read", 'patch.go',
     [('\tops, err := testResourceVersion(read)\n\tif err != nil {\n\t\treturn Patch{}, err\n\t}\n\t// From the last index down',
       '\tvar ops []jsonPatchOp\n\t// From the last index down')]),
    ("a changed object isn't read again", 'disownprefix.go',
     [('\t\tcase ClassStateChanged, ClassConflict:\n\t\t\tlast = err',
       '\t\tcase ClassStateChanged, ClassConflict:\n\t\t\treturn false, err')]),
    ('DisownPrefix runs while the CRD exists', 'disownprefix.go',
     [('\tif err := refuseWhileCRDExists(ctx, c, o.Holder); err != nil {',
       '\tif err := refuseWhileCRDExists(ctx, c, o.Holder); false {\n\t\t_ = err')]),
    ('a dry run strips', 'disownprefix.go',
     [('\t\tif o.DryRun {\n\t\t\tcontinue\n\t\t}',
       '\t\t_ = o.DryRun')]),
    ('the setup bound counts passes, not their span', 'shared.go',
     [('\tif time.Duration(o.DisownPasses-1)*o.DisownInterval < 2*o.TimeoutBound {',
       '\tif time.Duration(o.DisownPasses)*o.DisownInterval < 2*o.TimeoutBound {')]),
    ("the count patch isn't guarded", 'disown.go',
     [('\t\t\t"resourceVersion": crd.GetResourceVersion(),\n\t\t\t"annotations":     map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: settledValue},',
       '\t\t\t"annotations": map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: settledValue},')]),
    ("disownOff's patch isn't guarded", 'disown.go',
     [('\t\t"resourceVersion": m.ResourceVersion,\n\t\t"annotations":     map[string]any{AnnotationHeldCount: nil, AnnotationDisownSettled: nil},',
       '\t\t"annotations": map[string]any{AnnotationHeldCount: nil, AnnotationDisownSettled: nil},')]),
    ("a conflict isn't retried", 'disown.go',
     [('\t\tif Classify(err) != ClassConflict {\n\t\t\tt.warn("credential-use: Disown couldn\'t write its count',
       '\t\tif true {\n\t\t\tt.warn("credential-use: Disown couldn\'t write its count')]),
    ('starved on the first conflicting pass', 'disown.go',
     [('\tstarved := t.disown.starved >= 3',
       '\tstarved := t.disown.starved >= 1')]),
    ("a count for another switch is written", 'disown.go',
     [('\t\tif parseDisown(ann[AnnotationDisown]).key() != set.key() {',
       '\t\tif false {')]),
    ("a switch change doesn't start the run over", 'disown.go',
     [('\t\t\tif k := set.key(); t.disown.value != k {',
       '\t\t\tif k := set.key(); false {\n\t\t\t\t_ = k')]),
    ("what a strip removed isn't counted", 'disown.go',
     [('\t\tcount += max(0, removed-o.listed)',
       '\t\t_ = max(0, removed-o.listed)')]),
    ("a strip doesn't report what it removed", 'disownprefix.go',
     [('\t\t\treturn true, countPrefix(cur.Finalizers, prefix), nil',
       '\t\t\treturn true, countPrefix(m.Finalizers, prefix), nil')]),
    ("no live read before an A2", 'acquire.go',
     [('\t\t\t\tcase d:\n\t\t\t\t\tmore, err := disown(r)',
       '\t\t\t\tcase false && d:\n\t\t\t\t\tmore, err := disown(r)')]),
    ("a failed live read is ignored", 'acquire.go',
     [('\t\t\t\tcase err != nil:\n\t\t\t\t\treturn fail(fmt.Errorf("credential-use: reading the Disown switch before holding %s: %w", id, err), "Disown")',
       '\t\t\t\tcase false && err != nil:\n\t\t\t\t\treturn fail(fmt.Errorf("credential-use: reading the Disown switch before holding %s: %w", id, err), "Disown")')]),
    ('the live read is made for every A2', 'acquire.go',
     [('\t\tif set == nil {\n\t\t\ts, err := p.disownLive(ctx)',
       '\t\tif true {\n\t\t\ts, err := p.disownLive(ctx)')]),
    ("no live read before BeforeEstablish's A2", 'acquire.go',
     [('\t\tcase d:\n\t\t\tcontinue\n\t\t}\n\t\tif class, err := p.write(ctx, id, obj.GetNamespace()); class != ClassNone {',
       '\t\tcase false && d:\n\t\t\tcontinue\n\t\t}\n\t\tif class, err := p.write(ctx, id, obj.GetNamespace()); class != ClassNone {')]),
    ("the CRD informer isn't field-selected", 'disown.go',
     [('\tlw.field = "metadata.name=" + name',
       '\t_ = name')]),
    ("the CRD informer's preflight doesn't name the CRD", 'tracker.go',
     [('\treturn informer{inf: t.crd, gvr: crdGVR, name: t.crdName, what: "the CRD informer of " + t.gk.String()}',
       '\treturn informer{inf: t.crd, gvr: crdGVR, what: "the CRD informer of " + t.gk.String()}')]),
    ('DisownPrefix proceeds when its CRD list fails', 'disownprefix.go',
     [('\tif err != nil {\n\t\treturn fmt.Errorf("credential-use: checking that the CRD of %s is gone: %w", holder, err)\n\t}\n\tfor _, crd := range l.Items {',
       '\tif err != nil {\n\t\treturn nil\n\t}\n\tfor _, crd := range l.Items {')]),
]

U = 'TestAcquireWhileDisowned'
E2 = 'TestDisownDrainsTwoProcesses'
SET = 'TestDisownSettlesAfterConsecutiveZeroPasses'
LATE = 'TestALateA2AfterTheDisownStrip'
KILLERS = {
    "all doesn't cover every kind": 'TestParseDisown|' + U,
    "a listed kind's group is dropped": 'TestParseDisown',
    'a disowned ref is held': U,
    'no check just before each A2': U + '|' + LATE,
    'a disowned ref stays recorded': U,
    "a disowned hop isn't followed": U,
    'the intermediate A1 writes back disowned refs': U,
    'BeforeEstablish holds a disowned ref': U,
    "the CRD informer isn't started by RunInformers": E2,
    "a protected object's holds aren't counted": SET + '|' + LATE,
    "a holder's record isn't counted": LATE + '|' + E2 + '|TestDisownWithThreeProcesses',
    'a failed list counts as known': E2,
    'an unknown count is written as zero': 'TestDisownSettling',
    "another process's count doesn't reset the run": 'TestDisownSettling',
    'settled after one zero pass': 'TestDisownSettling|' + SET,
    'an unchanged count is written': 'TestDisownSettling',
    'the switch off leaves the count': E2,
    "the switch's change doesn't enqueue the holders": E2,
    'one kind strips the whole record': 'TestDisownHolderPatch',
    'all keeps the unheld list': 'TestDisownHolderPatch',
    'the strip keeps the epochs': 'TestDisownPatch|TestDisownPrefix',
    'the strip keeps the marker': 'TestDisownPatch|TestDisownPrefix',
    "the held label goes despite another kind's finalizer": 'TestDisownPatch',
    "the strip isn't guarded by the version read": 'TestDisownPatch',
    "a changed object isn't read again": 'TestDisownPrefix|' + E2,
    'DisownPrefix runs while the CRD exists': 'TestDisownPrefix',
    'a dry run strips': 'TestDisownPrefix',
    'the setup bound counts passes, not their span': 'TestTrackValidates',
    "the count patch isn't guarded": 'TestDisownSettling',
    "disownOff's patch isn't guarded": 'TestDisownOffClearsTheCount',
    "a conflict isn't retried": 'TestDisownSettling',
    'starved on the first conflicting pass': 'TestDisownSettling',
    'a count for another switch is written': 'TestDisownSettling',
    "a switch change doesn't start the run over": E2 + '|' + SET,
    "what a strip removed isn't counted": LATE + '|' + SET,
    "a strip doesn't report what it removed": 'TestStripDisownedCountsWhatItRemoved',
    'no live read before an A2': U,
    'the live read is made for every A2': U,
    'a failed live read is ignored': U,
    "no live read before BeforeEstablish's A2": U,
    "the CRD informer isn't field-selected": 'TestNamespaceRestrictedLiveRead',
    "the CRD informer's preflight doesn't name the CRD": 'TestNamespaceRestrictedLiveRead',
    'DisownPrefix proceeds when its CRD list fails': 'TestDisownPrefixRefusesWhenTheCRDListFails',
}

# Mutants a run showed no test can kill, with why.
EQUIVALENT = {}

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
