"""Revert check for pkg/credentialuse, PR 4 of
docs/plan-credential-use-implementation.md (chains: each hop held before it
is read, the record between levels, MaxDepth, Resolved, Guaranteed refs and
their listers, content caches, and the self-write filter that enqueues a
chain's holders): break each rule the PR adds in turn and see whether a test
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
  python3 hack/revert-check-credential-use-4.py > hack/revert-check-credential-use-4.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches, for a run
over a small delta; the full run's output stays the record for the rest.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["acquire.go", "chain.go", "informers.go", "shared.go", "listwatch.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ('an A1 between levels writes the exact chain', 'acquire.go',
     [('\t\tif chained {\n\t\t\tif rec := ReadRecord(obj); !rec.Unparseable {',
       '\t\tif false {\n\t\t\tif rec := ReadRecord(obj); !rec.Unparseable {')]),
    ("the last A1 isn't written", 'acquire.go',
     [('\tif !final {\n',
       '\tif !final && false {\n')]),
    ("a hop that isn't held is read and followed", 'acquire.go',
     [('\t\t\tif held && r.Next != nil {',
       '\t\t\tif r.Next != nil {')]),
    ("MaxDepth isn't enforced", 'acquire.go',
     [('\t\tif depth > maxDepth {',
       '\t\tif false {')]),
    ('a ref met again is followed again', 'acquire.go',
     [('\t\t\tif seen[id] {\n\t\t\t\tcontinue\n\t\t\t}\n',
       '')]),
    ('a Guaranteed ref is held', 'acquire.go',
     [('\t\t\tcase r.Guaranteed:\n',
       '\t\t\tcase false && r.Guaranteed:\n')]),
    ("a Guaranteed ref that can't be read is Missing", 'acquire.go',
     [('\t\t\t\treturn fail(fmt.Errorf("credential-use: reading the guaranteed ref %s: %w", r.ID(), err), "Refs")',
       '\t\t\t\ta.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})\n\t\t\t\tcontinue')]),
    ('a ref in another namespace with a Next is only not consented', 'acquire.go',
     [('\t\t\t\tif r.Next != nil {\n\t\t\t\t\treturn fail(errUnheldChainNotYet, "Refs")\n\t\t\t\t}\n',
       '')]),
    ('BeforeEstablish establishes with a chain unheld', 'acquire.go',
     [('\t\tif u.Ref.Next != nil {\n\t\t\t// It is held now',
       '\t\tif false {\n\t\t\t// It is held now')]),
    ('a Next error is ignored', 'chain.go',
     [('\t\treturn nil, fmt.Errorf("credential-use: following %s: %w", r.ID(), err)',
       '\t\treturn nil, nil')]),
    ('Resolved shares its object with the caller', 'chain.go',
     [('\treturn u.DeepCopy(), true\n}',
       '\treturn u, true\n}')]),
    ('a content kind is read as metadata', 'chain.go',
     [('\tif p.contentKinds[gk] {\n\t\tif p.contentCache != nil {',
       '\tif false {\n\t\tif p.contentCache != nil {')]),
    ('a kind without Content is read as content', 'chain.go',
     [('\tif p.contentKinds[gk] {\n\t\tif p.contentCache != nil {',
       '\tif true {\n\t\tif p.contentCache != nil {')]),
    ("the caller's lister isn't used", 'chain.go',
     [('\tif l := p.listers[r.GroupKind]; l != nil {',
       '\tif l := p.listers[r.GroupKind]; false {\n\t\t_ = l')]),
    ("a foreign change to a content kind doesn't enqueue its holders", 'informers.go',
     [('\tif lost || h.p.content && (o.Generation != n.Generation || foreignChange(oldObj, newObj)) {',
       '\tif lost {')]),
    ("our annotations count as someone else's", 'informers.go',
     [('\tc.SetAnnotations(dropKeys(c.GetAnnotations(), isCredentialUseKey))\n',
       '')]),
    ("our labels count as someone else's", 'informers.go',
     [('\tc.SetLabels(dropKeys(c.GetLabels(), isCredentialUseKey))\n',
       '')]),
    ("our finalizers count as someone else's", 'informers.go',
     [('\tfs := slices.DeleteFunc(c.GetFinalizers(), isCredentialUseKey)',
       '\tfs := c.GetFinalizers()')]),
    ('an empty finalizer list differs from none', 'informers.go',
     [('\tif len(fs) == 0 {\n\t\tfs = nil // absent and empty are one\n\t}\n',
       '')]),
    ('the resourceVersion counts as a change', 'informers.go',
     [('\tc.SetResourceVersion("")\n',
       '')]),
    ("someone else's change counts as ours", 'informers.go',
     [('\treturn !equality.Semantic.DeepEqual(othersView(o), othersView(n))',
       '\treturn false && !equality.Semantic.DeepEqual(othersView(o), othersView(n))')]),
    ('the content transform keeps status', 'informers.go',
     [('\tdelete(out.Object, "status")\n',
       '')]),
    ('the content transform keeps managedFields', 'informers.go',
     [('\t\tdelete(md, "managedFields")\n',
       '')]),
    ("a content object isn't seen as metadata", 'informers.go',
     [("\tcase *unstructured.Unstructured:\n\t\t// A content kind's object (§10.4): its metadata, trimmed as the\n\t\t// metadata informers' transform leaves theirs.\n\t\treturn metadataOfContent(o), true\n",
       '')]),
    ("a content kind's informer caches metadata only", 'shared.go',
     [('\t\tif pk.Content {\n\t\t\tp.lw = newContentListWatch(',
       '\t\tif false {\n\t\t\tp.lw = newContentListWatch(')]),
    ('holder kinds may disagree on Content', 'shared.go',
     [('\t\tif p.content != built[k].content {',
       '\t\tif false {')]),
]

EQUIVALENT = {}

# The tests that must kill each mutant: each runs the tests aimed at its
# rule, and the baseline runs them all.
KILLERS = {
    'an A1 between levels writes the exact chain': 'TestChainRepointKeepsTheOldRefUntilTheLastAnnounce',
    "the last A1 isn't written": 'TestChainRepointKeepsTheOldRefUntilTheLastAnnounce|TestChainEndingInACycleStillWritesTheExactRecord',
    "a hop that isn't held is read and followed": 'TestChainFailures',
    "MaxDepth isn't enforced": 'TestChainFailures',
    'a ref met again is followed again': 'TestChainFailures|TestAcquireANewHolder',
    'a Guaranteed ref is held': 'TestChainGuaranteedRefs',
    "a Guaranteed ref that can't be read is Missing": 'TestChainGuaranteedRefs',
    'a ref in another namespace with a Next is only not consented': 'TestChainFailures',
    'BeforeEstablish establishes with a chain unheld': 'TestChainFailures',
    'a Next error is ignored': 'TestChainFailures',
    'Resolved shares its object with the caller': 'TestResolvedIsTheHeldReadNotANewerOne',
    'a content kind is read as metadata': 'TestChainHoldsEachHopBeforeReadingIt',
    'a kind without Content is read as content': 'TestChainNextOnAKindWithoutContentGetsMetadata',
    "the caller's lister isn't used": 'TestChainGuaranteedRefs',
    "a foreign change to a content kind doesn't enqueue its holders": 'TestARepointedTargetEnqueuesItsHolders|TestARepointedConfigMapEnqueuesItsHolders',
    "our annotations count as someone else's": 'TestForeignChange',
    "our labels count as someone else's": 'TestForeignChange',
    "our finalizers count as someone else's": 'TestForeignChange',
    'an empty finalizer list differs from none': 'TestForeignChange',
    'the resourceVersion counts as a change': 'TestForeignChange',
    "someone else's change counts as ours": 'TestForeignChange',
    'the content transform keeps status': 'TestContentTransform',
    'the content transform keeps managedFields': 'TestContentTransform',
    "a content object isn't seen as metadata": 'TestChainTeardown|TestARepointedTargetEnqueuesItsHolders',
    "a content kind's informer caches metadata only": 'TestARepointedTargetEnqueuesItsHolders|TestARepointedConfigMapEnqueuesItsHolders',
    'holder kinds may disagree on Content': 'TestContentMustAgreeAcrossKinds',
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
