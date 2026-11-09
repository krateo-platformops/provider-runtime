"""Revert check for pkg/credentialuse, PR 2 of
docs/plan-credential-use-implementation.md (the protocol core and the verified
sets): break each rule in turn and see whether a test notices, the model's
schedules included.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

It runs the protocol's unit tests and the model's exhaustive searches
(model_test.go's profiles), which need no envtest.

Usage:
  python3 hack/revert-check-credential-use-2.py > hack/revert-check-credential-use-2.txt
"""
import hashlib, os, re, signal, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
PKGS = ["./" + PKG]
FILES = ["acquire.go", "release.go", "verified.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # acquire.go: A1 and A2 (§6.2, §6.3, §8).
    ("a deletion pass may skip A2", "acquire.go", [("\t\tif !deleting && !added[id] {\n", "\t\tif !added[id] {\n")]),
    ("a ref this pass's A1 added may skip A2", "acquire.go", [("\t\tif !deleting && !added[id] {\n", "\t\tif !deleting {\n")]),
    ("A1 isn't guarded by the holder's resourceVersion", "acquire.go", [('\t\t"resourceVersion": obj.GetResourceVersion(),\n', "")]),
    ("A1 doesn't set observed for the refs it adds", "acquire.go", [("\tp.ver.announced(obj.GetUID(), addedList, removed, at)\n", "\tp.ver.announced(obj.GetUID(), nil, removed, at)\n")]),
    ("A1 doesn't drop observed for the refs it removes", "acquire.go", [("\tp.ver.announced(obj.GetUID(), addedList, removed, at)\n", "\tp.ver.announced(obj.GetUID(), addedList, nil, at)\n")]),
    ("an unparseable record's refs aren't treated as added", "acquire.go", [("\tif rec.Unparseable {\n\t\t// Nothing was known to be named", "\tif false && rec.Unparseable {\n\t\t// Nothing was known to be named")]),
    ("a new holder connects on a Deleting ref", "acquire.go", [("\t\t\tif !established {\n\t\t\t\treturn fail(fmt.Errorf(\"credential %s", "\t\t\tif false {\n\t\t\t\treturn fail(fmt.Errorf(\"credential %s")]),
    ("an A2 error is ignored", "acquire.go", [("\t\tdefault:\n\t\t\treturn fail(fmt.Errorf(\"credential-use: holding %s: %w\", id, err), class.String())\n", "\t\tdefault:\n\t\t\ta.Held = append(a.Held, r)\n")]),
    ("a ref in another namespace is held", "acquire.go", [("\t\tif r.Namespace != namespace {\n", "\t\tif false && r.Namespace != namespace {\n")]),
    ("A2 on a custom resource doesn't retry a conflict", "acquire.go", [("\tfor range maxGuardedAttempts {\n", "\tfor range 1 {\n")]),
    ("written isn't recorded for a built-in kind", "acquire.go", [("\t\tp.ver.wrote(out.UID, namespace, issued)\n\t\treturn ClassNone, nil\n\t}\n\tvar last error", "\t\t_ = issued\n\t\treturn ClassNone, nil\n\t}\n\tvar last error")]),
    ("BeforeEstablish skips Missing refs", "acquire.go", [("\t\tif u.Reason != ReasonMissing {\n\t\t\tcontinue\n", "\t\tif true {\n\t\t\tcontinue\n")]),
    ("BeforeEstablish ignores a ref it can't hold", "acquire.go", [("\t\tif class, err := p.write(ctx, id, obj.GetNamespace()); class != ClassNone {\n", "\t\tif class, err := p.write(ctx, id, obj.GetNamespace()); false && class != ClassNone {\n")]),
    ("Forget doesn't clear observed", "acquire.go", [("\tp.ver.forgot(obj.GetUID())\n", "")]),
    ("Forget leaves the record", "acquire.go", [("\tdelete(an, AnnotationRefs)\n", "")]),
    ("a changed holder isn't flagged for a re-read", "acquire.go", [("\t\ta.Reread = true\n", "")]),
    ("A1's response isn't copied", "acquire.go", [("\tcopyMeta(obj, &out.ObjectMeta)\n", "")]),

    # release.go: R0 to R3 (§5, §6.2).
    ("R0 settles a deleting P on live users", "release.go", [("\treturn users, deletingUsers > 0\n", "\treturn users, users > 0\n")]),
    ("R0 settles a P the cache doesn't have", "release.go", [("\tif !ok {\n\t\treturn users, false\n\t}\n", "\tif !ok {\n\t\treturn users, users > 0\n\t}\n")]),
    ("the Namespace is read from the cache", "release.go", [("\t\t\t\t\tnsDeleting[ns] = m.DeletionTimestamp != nil\n", "\t\t\t\t\tnsDeleting[ns] = m.DeletionTimestamp != nil && p.namespaceDeletingCached(ns)\n")]),
    ("a missing Namespace reads as live", "release.go", [("\t\t\t\t\tnsDeleting[ns] = true\n", "\t\t\t\t\tnsDeleting[ns] = false\n")]),
    ("the grace ignores first sight", "release.go", [("GraceEnd(m.DeletionTimestamp.Time, p.seen.see(m.UID, now), p.grace)", "GraceEnd(m.DeletionTimestamp.Time, m.DeletionTimestamp.Time, p.grace)")]),
    ("§6.1's exclusion isn't applied", "release.go", [("\t\t\t\tif IsUser(&holders[i], r.id, p.ownerFinalizers) {\n", "\t\t\t\tif ReadRecord(&holders[i]).Names(r.id) {\n")]),
    ("deleting users aren't counted", "release.go", [("\t\t\t\tif IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {\n\t\t\t\t\tin.DeletingUsers++\n", "\t\t\t\tif false {\n\t\t\t\t\tin.DeletingUsers++\n")]),
    ("written isn't dropped on a release", "release.go", [("\t\t\t\tp.ver.released(r.meta.UID)\n", "")]),
    ("a state change isn't retried", "release.go", [("\t\t\t\tnext = append(next, r.id) // back to R1\n", "")]),
    ("a release rejected by an unchanged object is retried", "release.go", [("\t\t\tif rv, ok := builtFrom[id]; ok && rv == m.ResourceVersion {\n", "\t\t\tif rv, ok := builtFrom[id]; false && ok && rv == m.ResourceVersion {\n")]),
    ("a missing epoch isn't restored", "release.go", [("\t\t\t\tif err := p.restoreEpoch(ctx, id, m, n); err != nil {\n", "\t\t\t\tif err := error(nil); err != nil {\n")]),
    ("R2 decides with the grace measured before the reads", "release.go", [("\t\tholders, err := p.srv.List(ctx, p.holder, n)\n\t\tif err != nil {\n\t\t\terrs = append(errs, fmt.Errorf(\"credential-use: listing %s in %s: %w\", p.holder, n, err))\n\t\t\tbreak\n\t\t}\n\t\tnow := p.clock.Now()\n",
                                                                                 "\t\tnow := p.clock.Now().Add(p.grace)\n\t\tholders, err := p.srv.List(ctx, p.holder, n)\n\t\tif err != nil {\n\t\t\terrs = append(errs, fmt.Errorf(\"credential-use: listing %s in %s: %w\", p.holder, n, err))\n\t\t\tbreak\n\t\t}\n")]),

    # verified.go: the skip and its handlers (§6.3, §6.5).
    ("t_obs ignores recordSince", "verified.go", [("\tif hasR && (!hasO || r.After(t)) {\n", "\tif hasR && !hasO {\n")]),
    ("t_obs ignores the last relist", "verified.go", [("\tif v.lastList.After(t) {\n", "\tif false {\n")]),
    ("equal times skip", "verified.go", [("\treturn ok && obs.Before(issued)\n", "\treturn ok && !obs.After(issued)\n")]),
    ("a skip needs no observation", "verified.go", [("\tobs, ok := v.tObs(h, p)\n\treturn ok && obs.Before(issued)\n", "\tobs, _ := v.tObs(h, p)\n\treturn obs.Before(issued)\n")]),
    ("recordSince restarts on every event", "verified.go", [("\t\tif _, ok := v.recordSince[k]; !ok {\n\t\t\tv.recordSince[k] = at\n", "\t\tif true {\n\t\t\tv.recordSince[k] = at\n")]),
    ("a version without P doesn't end the run", "verified.go", [("\t\tif k.holder == uid && !keep[k.ref] {\n\t\t\tdelete(v.recordSince, k)\n", "\t\tif false && k.holder == uid && !keep[k.ref] {\n\t\t\tdelete(v.recordSince, k)\n")]),
    ("a recreate keeps written", "verified.go", [("\t\tif current == nil || current.GetUID() != k.uid {\n", "\t\tif current == nil {\n")]),
    ("a lost finalizer keeps written", "verified.go", [("\t\tif err != nil || !hasFinalizer(current, f) {\n", "\t\tif err != nil {\n")]),
    ("a release keeps written", "verified.go", [("\t\tif k.uid == pUID {\n\t\t\tdelete(v.written, k)\n", "\t\tif false && k.uid == pUID {\n\t\t\tdelete(v.written, k)\n")]),
]


# Mutants that are expected to survive, because another check makes them
# equivalent. They run anyway, and are reported as EQUIVALENT only if they
# survive; a kill is reported as a kill.
EQUIVALENT = {}


# The tests that exercise the protocol: its unit tests and the model's
# schedules, searched exhaustively with one fault and with one preemption, as
# CI's model step does (its random walk is left out, to keep runs
# reproducible). The envtest premise tests pin the API facts and don't run
# the protocol, so they are left out too.
TESTS = "^(TestCanSkip|TestCanSkipAwayAndBack|TestProtectedEvent|TestAcquire.*|TestRelease.*|TestModel.*)$"


def go_test():
    env = dict(os.environ, CI="1", CREDENTIALUSE_MODEL_BOUNDS="0,1,200000;1,0,200000")
    return subprocess.run(["go", "test", "-count=1", "-timeout", "3000s", "-run", TESTS, *PKGS], cwd=ROOT, env=env, capture_output=True, text=True, timeout=3600)


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
    return "KILLED", ",".join(sorted(set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))))[:220]



start = digest()
base = go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print("baseline passed\n")

# A run stopped by a signal restores the file it is mutating before it exits:
# a terminated process skips finally blocks.
restore = None


def on_signal(signum, _frame):
    if restore:
        open(restore[0], "w").write(restore[1])
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

counts = {}
for name, f, edits in MUTANTS:
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
    try:
        open(full, "w").write(mutated)
        state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
    if state == "SURVIVED" and name in EQUIVALENT:
        state, detail = "EQUIVALENT", EQUIVALENT[name]
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
