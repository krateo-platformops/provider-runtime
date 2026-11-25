"""Revert check for pkg/scaletozero (docs/design-scale-to-zero.md): break each
rule in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. The source file is restored after each mutant, and the end checks the
tree is as it started.

The tests need envtest: KUBEBUILDER_ASSETS must name its binaries. CI=1 is set
so a missing kube-apiserver fails the run instead of skipping the tests.

Every mutant must compile under the full `go vet` that `go test` runs: no
unused variable, no unreachable code (`_ = x` and `if false &&` keep a
mutant valid).

Usage:
  KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0) \\
    python3 hack/revert-check-scale-to-zero.py > hack/revert-check-scale-to-zero.txt
"""
import hashlib, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = "pkg/scaletozero/scaletozero.go"
HELD = "pkg/scaletozero/held.go"
FILES = [SRC, HELD]
PKGS = ["./pkg/scaletozero/"]

META_IMPORT = '\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n'
LIST_ERR = '\t\tif err := r.List(ctx, list, opts...); err != nil {\n'
LIST_ERR_RET = '\t\t\treturn false, fmt.Errorf("scaletozero: listing %s: %w", gvk, err)\n'
OPTS = "client.MatchingLabelsSelector{Selector: sel}, client.Limit(pageSize)}"
FOUND = "\t\tfor i := range list.Items {\n\t\t\tif match(&list.Items[i]) {\n\t\t\t\treturn true, nil\n\t\t\t}\n\t\t}\n"
END = "\t\tif list.Continue == \"\" {\n\t\t\treturn false, nil\n\t\t}\n"
SAME = "\t\tif list.Continue == cont {\n"
EXISTS_EXPR = "ExistsWithOptions(ctx, c, gvk, sel, append([]Option{WithRESTMapper(c.RESTMapper())}, opts...)...)"
EXISTS_CALL = "\texists, err := " + EXISTS_EXPR + "\n\tif err != nil {\n\t\treturn fail(err)\n\t}\n"
FAIL_RET = "\t\treturn Result{Replicas: r, Reason: ReasonUnknown, LastSeen: lastSeen}, err\n"
NEXT = "\tif exists || lastSeen.IsZero() {\n\t\treturn now"

# Held objects (held.go and ExistsWithOptions).
HELD_TAIL = "\tif !o.held() {\n\t\treturn false, nil\n\t}\n\treturn o.heldExists(ctx, r, gvk)\n"
CHECK_OPTS = "\to, err := newOptions(opts)\n\tif err != nil {\n\t\treturn false, err\n\t}\n"
LIST_KIND = "\tfound, err := first(ctx, r, gvk, sel, anyObject)\n\tif err != nil || found {\n\t\treturn found, err\n\t}\n"
SWITCH = "\tunsettled, err := disownUnsettled(ctx, r, o.mapper, gvk)\n"
RULE = "\treturn ann[credentialuse.AnnotationDisown] != \"\" && !credentialuse.DisownSettled(ann), nil\n"
CARRIES = "\tcarries := func(m *metav1.PartialObjectMetadata) bool { return hasPrefix(m.Finalizers, prefix) }\n"
PER_KIND = "\t\tfound, err := first(ctx, r, p, sel, carries)\n"
PER_KIND_ERR = PER_KIND + "\t\tif err != nil || found {\n\t\t\treturn found, err\n\t\t}\n"
KINDS_LOOP = "\tfor _, p := range o.protected {\n"
TO_INDEX = "\tif o.index != nil {\n\t\treturn indexHeld("
CRD_GET_ERR = "\t\treturn false, fmt.Errorf(\"scaletozero: reading CRD %s for its Disown switch: %w\", name, err)\n"
CRD_MAP_ERR = "\t\treturn false, fmt.Errorf(\"scaletozero: mapping %s for the name of its CRD: %w\", gvk, err)\n"
INDEX_GET = "\tif err := r.Get(ctx, client.ObjectKey(index), cm); err != nil {\n"
INDEX_GET_ERR = INDEX_GET + "\t\treturn false, fmt.Errorf(\"scaletozero: reading the index %s: %w\", index, err)\n"
LINES = "\tfor _, id := range credentialuse.IndexIDs(data) {\n"
LINES_LOOP = LINES + "\t\theld, err := lineHeld(ctx, r, mapper, id, prefix)\n\t\tif err != nil || held {\n\t\t\treturn held, err\n\t\t}\n\t}\n\treturn false, nil\n"
LINE_MAP_ERR = "\t\treturn false, fmt.Errorf(\"scaletozero: mapping %s, of the index line %q: %w\", id.GroupKind(), id, err)\n"
LINE_GONE = "\tif apierrors.IsNotFound(err) {\n\t\treturn false, nil\n\t}\n"
LINE_GET_ERR = "\t\treturn false, fmt.Errorf(\"scaletozero: reading %q, of the index: %w\", id, err)\n"
LINE_PREFIX = "\treturn hasPrefix(obj.Finalizers, prefix), nil\n"

# (name, [(old, new), ...]) of SRC, or (name, [(old, new), ...], file): every
# old must occur exactly once in its file.
MUTANTS = [
    # Exists.
    ("a nil selector is accepted", [("\tif sel == nil {\n", "\tif false && sel == nil {\n")]),
    ("the selector isn't applied", [(OPTS, "client.Limit(pageSize)}")]),
    ("one namespace is listed, not all", [(OPTS, OPTS[:-1] + ", client.InNamespace(\"default\")}")]),
    ("no Limit", [(OPTS, "client.MatchingLabelsSelector{Selector: sel}}")]),
    ("full objects, not metadata-only", [
        (META_IMPORT, META_IMPORT + '\t"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"\n'),
        ("\t\tlist := &metav1.PartialObjectMetadataList{}\n", "\t\tlist := &unstructured.UnstructuredList{}\n"),
        (FOUND, "\t\tif len(list.Items) > 0 && match != nil {\n\t\t\treturn true, nil\n\t\t}\n"),
        (END, END.replace("list.Continue", "list.GetContinue()")),
        (SAME, SAME.replace("list.Continue", "list.GetContinue()")),
        ("\t\tcont = list.Continue\n", "\t\tcont = list.GetContinue()\n"),
    ]),
    ("an empty page with a continue token is taken as the end", [(END, "\t\treturn false, nil\n")]),
    ("the continue token isn't sent", [("\t\t\topts = append(opts, client.Continue(cont))\n", "\t\t\t_ = opts\n")]),
    ("a repeated continue token isn't caught", [(SAME, "\t\tif false && list.Continue == cont {\n")]),
    ("terminating objects are skipped", [("\t\t\tif match(&list.Items[i]) {\n", "\t\t\tif list.Items[i].DeletionTimestamp == nil && match(&list.Items[i]) {\n")]),
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
    ("a failed LIST is taken as empty", [(EXISTS_CALL, "\texists, _ := " + EXISTS_EXPR + "\n")]),
    ("a failed LIST is taken as active", [(EXISTS_CALL, "\texists, err := " + EXISTS_EXPR + "\n\tif err != nil {\n\t\texists = true\n\t}\n")]),
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
    # Held objects: the options (docs/design-credential-use-finalizer.md
    # §11.6, the plan's PR 10).
    ("held options are ignored", [(HELD_TAIL, "\t_ = o\n\treturn false, nil\n")]),
    ("WithHeldIndex alone doesn't count", [("func (o options) held() bool { return o.withHeld || o.index != nil }", "func (o options) held() bool { return o.withHeld }")], HELD),
    ("the options are checked only on an empty Kind", [(CHECK_OPTS + LIST_KIND, LIST_KIND + CHECK_OPTS)]),
    ("ReconcileWithOptions drops its options", [(EXISTS_EXPR, "ExistsWithOptions(ctx, c, gvk, sel, WithRESTMapper(c.RESTMapper()))")]),
    ("ReconcileWithOptions doesn't take the client's mapper", [(EXISTS_EXPR, "ExistsWithOptions(ctx, c, gvk, sel, opts...)")]),
    ("a held option without a mapper is accepted", [("\tif o.held() && o.mapper == nil {\n", "\tif false && o.held() && o.mapper == nil {\n")], HELD),
    ("WithHeld with no kind is accepted", [("\tif o.withHeld && len(o.protected) == 0 {\n", "\tif false && o.withHeld && len(o.protected) == 0 {\n")], HELD),
    ("a protected kind without a version is accepted", [("\t\tif p.Version == \"\" || p.Kind == \"\" {\n", "\t\tif false && (p.Version == \"\" || p.Kind == \"\") {\n")], HELD),
    ("an index without a namespace or a name is accepted", [("\tif o.index != nil && (o.index.Namespace == \"\" || o.index.Name == \"\") {\n", "\tif false && o.index != nil && (o.index.Namespace == \"\" || o.index.Name == \"\") {\n")], HELD),
    ("a second WithHeld replaces the first's kinds", [("\t\to.protected = append(o.protected, p...)\n", "\t\to.protected = p\n")], HELD),
    # The Disown switch (decided 2026-11-23 and 2026-11-25).
    ("the switch isn't read", [(SWITCH, "\tunsettled, err := false, error(nil)\n")], HELD),
    ("a failed GET of the CRD is taken as settled", [(CRD_GET_ERR, "\t\treturn false, nil\n")], HELD),
    ("a holder kind the mapper can't map is taken as settled", [(CRD_MAP_ERR, "\t\treturn false, nil\n")], HELD),
    ("the CRD is named by the kind, not the plural", [("\tname := m.Resource.Resource + \".\" + gvk.Group\n", "\tname := strings.ToLower(gvk.Kind) + \".\" + gvk.Group\n\t_ = m\n")], HELD),
    ("an empty switch counts", [(RULE, "\treturn !credentialuse.DisownSettled(ann), nil\n")], HELD),
    ("held-count isn't checked", [(RULE, "\treturn ann[credentialuse.AnnotationDisown] != \"\" && ann[credentialuse.AnnotationDisownSettled] != ann[credentialuse.AnnotationDisown], nil\n")], HELD),
    ("any flag settles it, of whatever value", [(RULE, "\treturn ann[credentialuse.AnnotationDisown] != \"\" && !(ann[credentialuse.AnnotationDisownSettled] != \"\" && ann[credentialuse.AnnotationHeldCount] == \"0\"), nil\n")], HELD),
    ("a flag of true settles it", [(RULE, "\treturn ann[credentialuse.AnnotationDisown] != \"\" && !(credentialuse.DisownSettled(ann) || ann[credentialuse.AnnotationDisownSettled] == \"true\" && ann[credentialuse.AnnotationHeldCount] == \"0\"), nil\n")], HELD),
    ("a switch that names nothing doesn't count", [(RULE, "\treturn strings.Trim(ann[credentialuse.AnnotationDisown], \", \") != \"\" && !credentialuse.DisownSettled(ann), nil\n")], HELD),
    # WithHeld: the lists.
    ("the marker label isn't selected", [(PER_KIND, "\t\t_ = sel\n\t\tfound, err := first(ctx, r, p, labels.Everything(), carries)\n")], HELD),
    ("the prefix isn't checked: a collision counts", [(PER_KIND, "\t\t_ = carries\n\t\tfound, err := first(ctx, r, p, sel, anyObject)\n")], HELD),
    ("a collision ends the search: no paging past it", [(CARRIES, "\tseen := 0\n\tcarries := func(m *metav1.PartialObjectMetadata) bool { seen++; return seen == 1 && hasPrefix(m.Finalizers, prefix) }\n")], HELD),
    ("the list doesn't stop at the first hold", [
        (CARRIES, "\tsaw := false\n\tcarries := func(m *metav1.PartialObjectMetadata) bool { saw = saw || hasPrefix(m.Finalizers, prefix); return false }\n"),
        (PER_KIND, PER_KIND + "\t\tfound = found || saw\n"),
    ], HELD),
    ("a failed LIST of a protected kind is false", [(PER_KIND_ERR, "\t\tfound, _ := first(ctx, r, p, sel, carries)\n\t\tif found {\n\t\t\treturn found, nil\n\t\t}\n")], HELD),
    ("only the first protected kind is listed", [(KINDS_LOOP + PER_KIND, "\tfor _, p := range o.protected[:min(1, len(o.protected))] {\n" + PER_KIND)], HELD),
    ("WithHeldIndex replaces the lists", [(KINDS_LOOP + PER_KIND, KINDS_LOOP + "\t\tif o.index != nil {\n\t\t\tbreak\n\t\t}\n" + PER_KIND)], HELD),
    ("WithHeld replaces the index", [(TO_INDEX, "\tif o.index != nil && !o.withHeld {\n\t\treturn indexHeld(")], HELD),
    # WithHeldIndex: the index and its lines (§10.6.5; decided 2026-11-25).
    ("a missing index is false", [
        (INDEX_GET, INDEX_GET + "\t\tif apierrors.IsNotFound(err) {\n\t\t\treturn false, nil\n\t\t}\n"),
    ], HELD),
    ("an unreadable index is false", [(INDEX_GET_ERR, INDEX_GET + "\t\treturn false, nil\n")], HELD),
    ("only the first line is read", [(LINES, "\tfor _, id := range credentialuse.IndexIDs(data)[:min(1, len(credentialuse.IndexIDs(data)))] {\n")], HELD),
    ("the lines don't stop at the first hold", [
        (LINES_LOOP, "\tfound := false\n" + LINES + "\t\theld, err := lineHeld(ctx, r, mapper, id, prefix)\n\t\tif err != nil {\n\t\t\treturn held, err\n\t\t}\n\t\tfound = found || held\n\t}\n\treturn found, nil\n"),
    ], HELD),
    ("a line of a kind the mapper can't map is skipped", [(LINE_MAP_ERR, "\t\treturn false, nil\n")], HELD),
    ("a line that is gone is an error", [(LINE_GONE, "")], HELD),
    ("a failed GET of a line is false", [(LINE_GET_ERR, "\t\treturn false, nil\n")], HELD),
    ("a line's prefix isn't checked", [(LINE_PREFIX, "\t_ = prefix\n\treturn true, nil\n")], HELD),
    ("a line that can't exist is read", [("\tif !mayExist(", "\tif false && !mayExist(")], HELD),
    ("a cluster-scoped line isn't read", [("\treturn (namespace != \"\") == namespaced && ", "\treturn namespace != \"\" && namespaced && ")], HELD),
]


def go_test():
    env = dict(os.environ, CI="1")
    return subprocess.run(["go", "test", "-count=1", "-timeout", "600s", *PKGS], cwd=ROOT, env=env, capture_output=True, text=True, timeout=1200)


def go_vet():
    return subprocess.run(["go", "vet", *PKGS], cwd=ROOT, capture_output=True, text=True, timeout=600)


def applies(src, edits):
    """The counts of the olds that don't occur exactly once in src."""
    return [src.count(old) for old, _ in edits if src.count(old) != 1]


# --applies: only check that every mutant's olds occur exactly once, and change
# nothing. --vet: build each mutant under the full go vet, in place of the
# tests, to show that none fails to compile; it mutates and restores the
# files as a full run does.
if "--applies" in sys.argv:
    failed = 0
    for name, edits, *where in MUTANTS:
        bad = applies(open(os.path.join(ROOT, where[0] if where else SRC)).read(), edits)
        if bad:
            failed += 1
            print(f"NOT-APPLIED ({bad} matches)  {name}")
    print(f"{len(MUTANTS) - failed} of {len(MUTANTS)} apply")
    sys.exit(1 if failed else 0)
VET = "--vet" in sys.argv


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
    return "KILLED", ",".join(sorted(set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))))[:220]


if not VET and not os.environ.get("KUBEBUILDER_ASSETS"):
    print("KUBEBUILDER_ASSETS is not set: the run is invalid")
    sys.exit(1)

start = digest()
base = go_vet() if VET else go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print("baseline passed\n")

counts = {}
for name, edits, *where in MUTANTS:
    full = os.path.join(ROOT, where[0] if where else SRC)
    src = open(full).read()
    bad = applies(src, edits)
    if bad:
        print(f"NOT-APPLIED ({bad} matches)  {name}", flush=True)
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src
    for old, new in edits:
        mutated = mutated.replace(old, new)
    try:
        open(full, "w").write(mutated)
        if VET:
            r = go_vet()
            state, detail = ("VETTED", "") if r.returncode == 0 else ("BUILD", (r.stdout + r.stderr).strip()[-300:])
        else:
            state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
