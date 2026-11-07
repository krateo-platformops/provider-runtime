"""Revert check for pkg/scaletozero (docs/design-scale-to-zero.md): break each
rule in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. The source file is restored after each mutant, and the end checks the
tree is as it started.

The tests need envtest: KUBEBUILDER_ASSETS must name its binaries. CI=1 is set
so a missing kube-apiserver fails the run instead of skipping the tests.

Usage:
  KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.35.0) \\
    python3 hack/revert-check-scale-to-zero.py > hack/revert-check-scale-to-zero.txt
"""
import hashlib, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = "pkg/scaletozero/scaletozero.go"
PKGS = ["./pkg/scaletozero/"]

META_IMPORT = '\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n'
LIST_ERR = '\t\tif err := r.List(ctx, list, opts...); err != nil {\n'
LIST_ERR_RET = '\t\t\treturn false, fmt.Errorf("scaletozero: listing %s: %w", gvk, err)\n'
OPTS = "client.MatchingLabelsSelector{Selector: sel}, client.Limit(pageSize)}"
FOUND = "\t\tif len(list.Items) > 0 {\n\t\t\treturn true, nil\n\t\t}\n"
END = "\t\tif list.Continue == \"\" {\n\t\t\treturn false, nil\n\t\t}\n"
SAME = "\t\tif list.Continue == cont {\n"
EXISTS_CALL = "\texists, err := Exists(ctx, c, gvk, sel)\n\tif err != nil {\n\t\treturn fail(err)\n\t}\n"
FAIL_RET = "\t\treturn Result{Replicas: r, Reason: ReasonUnknown, LastSeen: lastSeen}, err\n"
NEXT = "\tif exists || lastSeen.IsZero() {\n\t\treturn now"

# (name, [(old, new), ...]): every old must occur exactly once.
MUTANTS = [
    # Exists.
    ("a nil selector is accepted", [("\tif sel == nil {\n", "\tif false && sel == nil {\n")]),
    ("the selector isn't applied", [(OPTS, "client.Limit(pageSize)}")]),
    ("one namespace is listed, not all", [(OPTS, OPTS[:-1] + ", client.InNamespace(\"default\")}")]),
    ("no Limit", [(OPTS, "client.MatchingLabelsSelector{Selector: sel}}")]),
    ("full objects, not metadata-only", [
        (META_IMPORT, META_IMPORT + '\t"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"\n'),
        ("\t\tlist := &metav1.PartialObjectMetadataList{}\n", "\t\tlist := &unstructured.UnstructuredList{}\n"),
        (END, END.replace("list.Continue", "list.GetContinue()")),
        (SAME, SAME.replace("list.Continue", "list.GetContinue()")),
        ("\t\tcont = list.Continue\n", "\t\tcont = list.GetContinue()\n"),
    ]),
    ("an empty page with a continue token is taken as the end", [(END, "\t\treturn false, nil\n")]),
    ("the continue token isn't sent", [("\t\t\topts = append(opts, client.Continue(cont))\n", "\t\t\t_ = opts\n")]),
    ("a repeated continue token isn't caught", [(SAME, "\t\tif false && list.Continue == cont {\n")]),
    ("terminating instances are skipped", [(FOUND, "\t\tfor _, it := range list.Items {\n\t\t\tif it.DeletionTimestamp == nil {\n\t\t\t\treturn true, nil\n\t\t\t}\n\t\t}\n")]),
    ("a no-match (missing CRD, unserved version) is false", [
        (META_IMPORT, META_IMPORT + '\t"k8s.io/apimachinery/pkg/api/meta"\n'),
        (LIST_ERR, LIST_ERR + "\t\t\tif meta.IsNoMatchError(err) {\n\t\t\t\treturn false, nil\n\t\t\t}\n"),
    ]),
    ("a NotFound LIST (CRD deleted) is false", [
        (META_IMPORT, META_IMPORT + '\tapierrors "k8s.io/apimachinery/pkg/api/errors"\n'),
        (LIST_ERR, LIST_ERR + "\t\t\tif apierrors.IsNotFound(err) {\n\t\t\t\treturn false, nil\n\t\t\t}\n"),
    ]),
    ("a LIST error is swallowed as false", [(LIST_ERR_RET, "\t\t\treturn false, nil\n")]),
    # Decide.
    ("an instance doesn't keep it up", [("\tif exists || holdDown <= 0 {", "\tif holdDown <= 0 {")]),
    ("Decide accepts a holdDown <= 0", [("\tif exists || holdDown <= 0 {", "\tif exists {")]),
    ("first sight scales down at once", [("\tif lastSeen.IsZero() {\n\t\treturn 1, holdDown\n\t}\n", "")]),
    ("a clock gone backwards isn't handled", [("\tif elapsed < 0 {\n\t\treturn 1, holdDown\n\t}\n", "")]),
    ("the hold-down's end is exclusive", [("\tif elapsed >= holdDown {", "\tif elapsed > holdDown {")]),
    ("requeue waits the whole hold-down, not the rest", [("\treturn 1, holdDown - elapsed\n", "\treturn 1, holdDown\n")]),
    # nextLastSeen.
    ("an instance doesn't refresh lastSeen", [(NEXT, "\tif lastSeen.IsZero() {\n\t\treturn now")]),
    ("first sight doesn't start the hold-down", [(NEXT, "\tif exists {\n\t\treturn now")]),
    ("a future lastSeen is reset to now", [(NEXT, "\tif exists || lastSeen.IsZero() || lastSeen.After(now) {\n\t\treturn now")]),
    # Reconcile: arguments and errors.
    ("Reconcile accepts a holdDown <= 0", [("\tif holdDown <= 0 {\n\t\treturn fail(", "\tif false && holdDown <= 0 {\n\t\treturn fail(")]),
    ("Reconcile accepts an active < 1", [("\tif active < 1 {\n\t\treturn fail(", "\tif false && active < 1 {\n\t\treturn fail(")]),
    ("a failed LIST is taken as empty", [(EXISTS_CALL, "\texists, _ := Exists(ctx, c, gvk, sel)\n")]),
    ("a failed LIST is taken as active", [(EXISTS_CALL, "\texists, err := Exists(ctx, c, gvk, sel)\n\tif err != nil {\n\t\texists = true\n\t}\n")]),
    ("an error passes back lastSeen = now", [(FAIL_RET, FAIL_RET.replace("LastSeen: lastSeen", "LastSeen: time.Now()"))]),
    ("an error reads as 0 replicas", [(FAIL_RET, "\t\treturn Result{Reason: ReasonUnknown, LastSeen: lastSeen}, err\n")]),
    ("an error reports the current replicas even at 0", [("\t\tif read && current > 0 {\n", "\t\tif read {\n")]),
    ("an error ignores the current replicas", [("\t\tif read && current > 0 {\n", "\t\tif false && read && current > 0 {\n")]),
    ("an error with active < 1 reports it", [("\t\tif r < 1 {\n\t\t\tr = 1\n\t\t}\n", "")]),
    ("an error has no Unknown reason", [(FAIL_RET, "\t\treturn Result{Replicas: r, LastSeen: lastSeen}, err\n")]),
    # Reconcile: what it patches.
    ("it patches on every call", [("\tif target == current {\n\t\treturn res, nil\n\t}\n", "")]),
    ("a running controller is patched to active", [("\tcase current == 0:\n\t\ttarget = active\n", "\tdefault:\n\t\ttarget = active\n")]),
    ("waking goes to 1, not active", [("\tcase current == 0:\n\t\ttarget = active\n", "\tcase current == 0:\n\t\ttarget = 1\n")]),
    ("idle isn't scaled to 0", [("\tcase running == 0:\n\t\ttarget = 0\n", "\tcase running == 0:\n\t\ttarget = current\n")]),
    ("replicas are read from the Deployment's spec", [
        ("\tscale := &autoscalingv1.Scale{}\n\tif err := c.SubResource(\"scale\").Get(ctx, obj, scale); err != nil {",
         "\tscale := &autoscalingv1.Scale{}\n\tif err := c.Get(ctx, deploy, obj); err != nil {"),
        ("\tread, current = true, scale.Spec.Replicas\n", "\tread, current = true, *obj.Spec.Replicas\n"),
    ]),
    ("the Deployment is patched, not its scale", [("c.SubResource(\"scale\").Patch(ctx, obj, patch, client.WithSubResourceBody(scale))", "c.Patch(ctx, obj, patch)")]),
    # The first version's claim: ScaleSpec.Replicas is omitempty, so a merge
    # patch computed from the Scale sends null for 0 and breaks the scale to
    # 0. It survives: the API server reads the null as 0. An equivalent
    # mutant; the code comment and the design now say so.
    ("the patch is computed from the Scale (omitempty claim)", [
        ("\tpatch := client.RawPatch(types.MergePatchType, fmt.Appendf(nil, `{\"spec\":{\"replicas\":%d}}`, target))\n",
         "\torig := scale.DeepCopy()\n\tscale.Spec.Replicas = target\n\tpatch := client.MergeFrom(orig)\n"),
    ]),
]


def go_test():
    env = dict(os.environ, CI="1")
    return subprocess.run(["go", "test", "-count=1", "-timeout", "180s", *PKGS], cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)


def digest():
    return hashlib.sha256(open(os.path.join(ROOT, SRC), "rb").read()).hexdigest()


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


if not os.environ.get("KUBEBUILDER_ASSETS"):
    print("KUBEBUILDER_ASSETS is not set: the run is invalid")
    sys.exit(1)

start = digest()
base = go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print("baseline passed\n")

full = os.path.join(ROOT, SRC)
counts = {}
for name, edits in MUTANTS:
    src = open(full).read()
    bad = [old for old, _ in edits if src.count(old) != 1]
    if bad:
        print(f"NOT-APPLIED ({[src.count(b) for b in bad]} matches)  {name}", flush=True)
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src
    for old, new in edits:
        mutated = mutated.replace(old, new)
    try:
        open(full, "w").write(mutated)
        state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
