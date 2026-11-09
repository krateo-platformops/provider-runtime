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
    ("a deletion pass may skip A2", "acquire.go", [("\t\tif !deleting && !added[id] {\n", "\t\tif (deleting || !deleting) && !added[id] {\n")]),
    ("a ref this pass's A1 added may skip A2", "acquire.go", [("\t\tif !deleting && !added[id] {\n", "\t\tif !deleting && (added[id] || !added[id]) {\n")]),
    ("A1 isn't guarded by the holder's resourceVersion", "acquire.go", [('\t\t"resourceVersion": obj.GetResourceVersion(),\n', "")]),
    ("A1 doesn't set observed for the refs it adds", "acquire.go", [("\tp.ver.announced(obj.GetUID(), addedList, removed, at)\n", "\tp.ver.announced(obj.GetUID(), nil, removed, at)\n")]),
    ("A1 doesn't drop observed for the refs it removes", "acquire.go", [("\tp.ver.announced(obj.GetUID(), addedList, removed, at)\n", "\tp.ver.announced(obj.GetUID(), addedList, nil, at)\n")]),
    ("an unparseable record's refs aren't treated as added", "acquire.go", [("\tif rec.Unparseable {\n\t\t// Nothing was known to be named", "\tif false && rec.Unparseable {\n\t\t// Nothing was known to be named")]),
    ("a new holder connects on a Deleting ref", "acquire.go", [("\t\t\tif !established {\n\t\t\t\treturn fail(fmt.Errorf(\"credential %s", "\t\t\tif false {\n\t\t\t\treturn fail(fmt.Errorf(\"credential %s")]),
    ("an A2 error is ignored", "acquire.go", [("\t\tdefault:\n\t\t\treturn fail(fmt.Errorf(\"credential-use: holding %s: %w\", id, err), class.String())\n", "\t\tdefault:\n\t\t\ta.Held = append(a.Held, r)\n")]),
    ("a ref in another namespace is held", "acquire.go", [("\t\tif r.Namespace != namespace {\n", "\t\tif false && r.Namespace != namespace {\n")]),
    ("A2 on a custom resource doesn't retry a conflict", "acquire.go", [("\tfor range maxGuardedAttempts {\n", "\tfor range 1 {\n")]),
    ("written isn't recorded for a built-in kind", "acquire.go", [("\t\tp.ver.wrote(out.UID, namespace, issued)\n\t\treturn ClassNone, nil\n\t}\n\tvar last error", "\t\t_, _ = out, issued\n\t\treturn ClassNone, nil\n\t}\n\tvar last error")]),
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
    ("written isn't dropped on a release", "release.go", [("\t\t\t\tp.ver.released(r.meta.UID, n)\n", "")]),
    ("first sight isn't pruned on a release", "release.go", [("\t\t\t\tp.seen.forget(r.meta.UID)\n", "")]),
    ("a state change isn't retried", "release.go", [("\t\t\t\tnext = append(next, r.id) // back to R1\n", "")]),
    ("a release rejected by an unchanged object is retried", "release.go", [("\t\t\tif rv, ok := builtFrom[id]; ok && rv == m.ResourceVersion {\n", "\t\t\tif rv, ok := builtFrom[id]; false && ok && rv == m.ResourceVersion {\n")]),
    ("a missing epoch isn't restored", "release.go", [("\t\t\t\tif err := p.restoreEpoch(ctx, id, m, n); err != nil {\n", "\t\t\t\tif err := error(nil); err != nil {\n")]),
    # M4 of #59's review: the clock is read before R2's first read. This
    # mutant moves the reading after the list, where 04b389d had it.
    ("R2 measures the grace after its reads", "release.go", [
        ("\t\tnow := p.clock.Now()\n\t\tnsDeleting := map[string]bool{}\n", "\t\tnsDeleting := map[string]bool{}\n"),
        ("\t\t\tbreak\n\t\t}\n\n\t\t// Decide, then R3.\n", "\t\t\tbreak\n\t\t}\n\t\tnow := p.clock.Now()\n\n\t\t// Decide, then R3.\n")]),
    ("the grace runs out early", "release.go", [("\t\tnow := p.clock.Now()\n\t\tnsDeleting := map[string]bool{}\n", "\t\tnow := p.clock.Now().Add(p.grace)\n\t\tnsDeleting := map[string]bool{}\n")]),

    # verified.go: the skip and its handlers (§6.3, §6.5).
    ("a skip needs no delivered version (H1)", "verified.go", [("\tif v.delivered[h] != hRV {\n", "\tif false {\n")]),
    ("a skip needs no version naming P", "verified.go", [("\tif !named {\n", "\tif false && !named {\n")]),
    ("a write for another namespace counts", "verified.go", [("\tissued, ok := v.written[pUID][n]\n", "\tvar issued time.Time\n\tok := false\n\tfor _, t := range v.written[pUID] {\n\t\tissued, ok = t, true\n\t}\n")]),
    ("t_obs ignores recordSince", "verified.go", [("\tobs := since\n", "\tvar obs time.Time\n\t_ = since\n")]),
    ("t_obs ignores observed", "verified.go", [("\tif o, ok := v.observed[h][p]; ok && o.After(obs) {\n", "\tif o, ok := v.observed[h][p]; false && ok && o.After(obs) {\n")]),
    ("t_obs ignores the last relist", "verified.go", [("\tif v.lastList.After(obs) {\n", "\tif false {\n")]),
    ("equal times skip", "verified.go", [("\treturn obs.Before(issued)\n", "\treturn !obs.After(issued)\n")]),
    ("recordSince restarts on every event", "verified.go", [("\t\tif _, ok := runs[p]; !ok {\n", "\t\tif true {\n")]),
    ("a version without P doesn't end the run", "verified.go", [("\t\tif !names[p] {\n\t\t\tdelete(runs, p)\n", "\t\tif false && !names[p] {\n\t\t\tdelete(runs, p)\n")]),
    ("a version that ends the run keeps observed (H1)", "verified.go", [("\t\t\tdelete(runs, p)\n\t\t\tdelete(v.observed[uid], p)\n", "\t\t\tdelete(runs, p)\n")]),
    ("a holder's deletion keeps what was delivered", "verified.go", [("\t\tdelete(v.delivered, uid)\n\t\tdelete(v.recordSince, uid)\n\t\tdelete(v.observed, uid)\n", "")]),
    ("a recreate keeps written", "verified.go", [("\tif current == nil || current.GetUID() != old.GetUID() {\n", "\tif current == nil {\n")]),
    ("a lost finalizer keeps written", "verified.go", [("\t\tif err != nil || !hasFinalizer(current, f) {\n", "\t\tif err != nil || (f != f) {\n")]),
    ("a release keeps written", "verified.go", [("\tdelete(v.written[pUID], n)\n", "")]),
    ("a release drops every namespace's entry", "verified.go", [("\tdelete(v.written[pUID], n)\n\tif len(v.written[pUID]) == 0 {\n", "\tdelete(v.written[pUID], n)\n\tif true {\n")]),
]


# Mutants that are expected to survive, because another check makes them
# equivalent. They run anyway, and are reported as EQUIVALENT only if they
# survive; a kill is reported as a kill.
EQUIVALENT = {
    # Revision 17's exact-version rule (H1 of #59's review) makes these two
    # guards defence in depth. A skip needs the informer to have delivered
    # the very version the pass works from, and it delivers every version in
    # order or relists (lastList bounds t_obs). So the run naming P that the
    # version belongs to began at a delivery after the A1 that started it:
    # either this pass's A1, whose response the informer can't deliver before
    # it lands, or an earlier one. An entry written before that delivery
    # can't satisfy t_obs < t_issue, and none can be written between this
    # pass's A1 and its own A2.
    "a ref this pass's A1 added may skip A2":
        "equivalent: t_obs is at least the delivery of the run's first version, after this pass's A1",
    "an unparseable record's refs aren't treated as added":
        "equivalent: an unparseable record names nothing, so no delivered run names P until A1 rewrites it",
}


# The tests that exercise the protocol: its unit tests first, and only if they
# pass the model's schedules, searched exhaustively with one fault and with one
# preemption, as CI's model step does (its random walk is left out, to keep
# runs reproducible). -failfast ends a run at its first failure. The envtest
# premise tests pin the API facts and don't run the protocol, so they are
# left out. So is TestModelTwoProcessesAcquireOneHolder: its one-fault search
# alone is 2.6 million schedules, about 45 minutes of CPU, per mutant.
UNIT = "^(TestCanSkip.*|TestHolderEvent.*|TestProtectedEvent|TestSkip.*|TestAcquire.*|TestRelease.*)$"
MODEL = "^TestModel(Teardown|AcquireAgainstRelease|TwoReleasers|AwayAndBack|DeletionsAroundTheGrace|RecreateBehindACachedUID|BoundsAreExhausted)$"


def go_test():
    env = dict(os.environ, CI="1", CREDENTIALUSE_MODEL_BOUNDS="0,1,200000;1,0,200000")
    for tests in (UNIT, MODEL):
        r = subprocess.run(["go", "test", "-count=1", "-failfast", "-timeout", "3000s", "-run", tests, *PKGS],
                           cwd=ROOT, env=env, capture_output=True, text=True, timeout=3600)
        if r.returncode != 0:
            return r
    return r


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
