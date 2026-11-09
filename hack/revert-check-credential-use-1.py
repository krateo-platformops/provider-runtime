"""Revert check for pkg/credentialuse, PR 1 of
docs/plan-credential-use-implementation.md: break each rule in turn and see
whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

The premise tests need envtest: KUBEBUILDER_ASSETS must name its binaries. CI=1
is set so a missing kube-apiserver fails the run instead of skipping them.

Usage:
  KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0) \\
    python3 hack/revert-check-credential-use-1.py > hack/revert-check-credential-use-1.txt
"""
import hashlib, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
PKGS = ["./" + PKG]
FILES = ["names.go", "ref.go", "record.go", "classify.go", "patch.go", "decide.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # names.go: the keys a hold writes (§3, §10.1).
    ("a namespace isn't validated", "names.go", [
        ("} else if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {", "} else if errs := []string(nil); len(errs) > 0 {")]),
    ("the qualified-name check is skipped", "names.go", [
        ("if errs := validation.IsQualifiedName(name); len(errs) > 0 {", "if errs := []string(nil); len(errs) > 0 {")]),
    ("the prefix isn't checked as a DNS subdomain", "names.go", [
        ("if errs := validation.IsDNS1123Subdomain(prefix); len(errs) > 0 {", "if errs := []string(nil); len(errs) > 0 {")]),
    ("a holder kind needs no group", "names.go", [
        ('if holder.Kind == "" || holder.Group == "" {', 'if holder.Kind == "" {')]),
    ("the kind isn't lowercased", "names.go", [
        ('prefix := finalizerFamily + strings.ToLower(holder.Kind) + "." + holder.Group', 'prefix := finalizerFamily + holder.Kind + "." + holder.Group')]),
    ("a cluster-scoped kind gets no name part", "names.go", [
        ("\t\tpart = ClusterScopedName\n", "\t\tpart = \"\"\n")]),
    ("a credential-use finalizer needs no /", "names.go", [
        ('\tprefix, _, ok := strings.Cut(f, "/")\n\treturn ok && strings.HasPrefix(prefix, finalizerFamily)', "\treturn strings.HasPrefix(f, finalizerFamily)")]),
    ("the marker depends on the kind's case", "names.go", [
        ('sum := sha256.Sum256([]byte(strings.ToLower(holder.Kind) + "." + holder.Group))', 'sum := sha256.Sum256([]byte(holder.Kind + "." + holder.Group))')]),
    ("the epoch is 64 bits", "names.go", [("\tvar b [16]byte\n", "\tvar b [8]byte\n")]),

    # ref.go and record.go: the record (§4).
    ("the record isn't deduplicated", "ref.go", [("\treturn slices.Compact(out)\n", "\treturn out\n")]),
    ("the record isn't sorted", "ref.go", [("\tslices.SortFunc(out, compareIDs)\n", "")]),
    ("unknown fields are accepted", "record.go", [("\tdec.DisallowUnknownFields()\n", "")]),
    ("trailing data is accepted", "record.go", [("\tif dec.More() {\n", "\tif false && dec.More() {\n")]),
    ("null reads as an empty record", "record.go", [
        ('\tif ids == nil {\n\t\t// "null" is not an array.\n', '\tif false && ids == nil {\n\t\t// "null" is not an array.\n')]),
    ("an entry without a kind or name is read", "record.go", [
        ('\t\tif id.Kind == "" || id.Name == "" {\n\t\t\treturn nil, fmt.Errorf("record entry', '\t\tif false {\n\t\t\treturn nil, fmt.Errorf("record entry')]),
    ("an unparseable record names nothing", "record.go", [
        ("\tif r.Unparseable {\n\t\treturn true\n\t}\n", "\tif r.Unparseable {\n\t\treturn false\n\t}\n")]),
    ("no refs encode as null", "record.go", [("\tif ids == nil {\n\t\tids = []ID{}\n\t}\n", "")]),
    ("an entry without a kind or name is written", "record.go", [
        ('\t\tif id.Kind == "" || id.Name == "" {\n\t\t\treturn "", fmt.Errorf', '\t\tif false {\n\t\t\treturn "", fmt.Errorf')]),

    # classify.go: §6.2's table.
    ("any 422 is Deleting", "classify.go", [("\t\tcase hasFinalizersForbiddenCause(st):\n", "\t\tcase true:\n")]),
    ("a policy's 422 is read as Deleting", "classify.go", [
        ("\t\tswitch {\n\t\tcase config:\n\t\t\treturn ClassPolicyConfig\n\t\tcase policy:\n\t\t\treturn ClassPolicyDenied\n\t\tcase hasFinalizersForbiddenCause(st):\n",
         "\t\tswitch {\n\t\tcase hasFinalizersForbiddenCause(st) || config || policy:\n\t\t\treturn ClassDeleting\n\t\tcase config:\n\t\t\treturn ClassPolicyConfig\n\t\tcase policy:\n\t\t\treturn ClassPolicyDenied\n\t\tcase false:\n")]),
    ("a policy's text only as a message prefix (revision 15's rule)", "classify.go", [
        ("\t\t\tif strings.HasPrefix(c.Message, policyText) {\n", "\t\t\tif false {\n"),
        ('\tif i := strings.Index(st.Message, "forbidden: "+policyText); i >= 0 {\n\t\ttexts = append(texts, st.Message[i+len("forbidden: "):])\n',
         "\tif strings.HasPrefix(st.Message, policyText) {\n\t\ttexts = append(texts, st.Message)\n")]),
    # The cause and the message carry the same text, by design, so each of
    # these two alone is expected to survive; together they are the mutant
    # above.
    ("a policy's text only from the message", "classify.go", [("\t\t\tif strings.HasPrefix(c.Message, policyText) {\n", "\t\t\tif false {\n")]),
    ("a policy's text only from a cause", "classify.go", [
        ('\tif i := strings.Index(st.Message, "forbidden: "+policyText); i >= 0 {\n', '\tif i := -1; i >= 0 {\n')]),
    ("a config error reads as a denial", "classify.go", [("\t\t\t\tconfig = true\n", "")]),
    ("any 403 without a policy is RBAC (revision 15's rule)", "classify.go", [("\t\tcase rbacMessage.MatchString(st.Message):\n", "\t\tcase true:\n")]),
    ("RBAC is never recognised", "classify.go", [("\t\tcase rbacMessage.MatchString(st.Message):\n", "\t\tcase false:\n")]),
    ("an RBAC message may have trailing text without a colon", "classify.go", [('|path "[^"]*")(?::|$)`)', '|path "[^"]*")`)')]),
    ("AlreadyExists is a conflict", "classify.go", [
        ("\t\tif st.Reason == metav1.StatusReasonAlreadyExists {\n\t\t\treturn ClassAlreadyExists\n\t\t}\n", "")]),
    ("a JSON-patch 422 with causes is a state change", "classify.go", [
        ("st.Reason == metav1.StatusReasonInvalid && !hasCauses(st) {", "st.Reason == metav1.StatusReasonInvalid {")]),
    ("ClassifyJSONPatch can return Deleting", "classify.go", [
        ("\tif c != ClassOther && c != ClassDeleting {\n", "\tif c != ClassOther {\n")]),
    ("a non-API error is classified by its text", "classify.go", [
        ("\tst, ok := statusOf(err)\n\tif !ok {\n\t\treturn ClassOther\n\t}\n\tswitch st.Code {", "\tst, _ := statusOf(err)\n\tswitch st.Code {")]),

    # patch.go: A2, R3, the epoch repair and the janitor (§6.2, §10.1).
    ("A2 (strategic) adds no finalizer", "patch.go", [('"finalizers":  []string{k.finalizer},', '"finalizers":  []string{},')]),
    ("A2 (strategic) sets no marker label", "patch.go", [('"labels":      map[string]string{LabelHeld: "true", k.marker: "true"},', '"labels":      map[string]string{LabelHeld: "true"},')]),
    ("A2 (strategic) sets no epoch", "patch.go", [('"annotations": map[string]string{k.finalizer: epoch},', '"annotations": map[string]string{},')]),
    ("A2 (guarded) isn't guarded", "patch.go", [('\t\t"resourceVersion": rv,\n', "")]),
    ("A2 (guarded) drops others' finalizers", "patch.go", [("\tfinalizers := slices.Clone(current.GetFinalizers())\n", "\tfinalizers := []string{}\n")]),
    ("A2 (guarded) duplicates a held finalizer", "patch.go", [("\tif !slices.Contains(finalizers, k.finalizer) {\n\t\tfinalizers = append", "\tif true {\n\t\tfinalizers = append")]),
    ("R3 doesn't test the epoch", "patch.go", [("\t\t{Op: \"test\", Path: annotation, Value: epoch},\n", "")]),
    ("R3 doesn't test the index", "patch.go", [("\t\t{Op: \"test\", Path: index, Value: k.finalizer},\n", "")]),
    ("R3 doesn't test the whole array", "patch.go", [
        ('\tif removeMarker || removeHeld {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: "/metadata/finalizers"', '\tif false {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: "/metadata/finalizers"')]),
    ("R3 doesn't test the marker's value", "patch.go", [
        ('\tif removeMarker {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: markerPath', '\tif false {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: markerPath')]),
    ("R3 doesn't test the held label's value", "patch.go", [
        ('\tif removeHeld {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: heldPath', '\tif false {\n\t\tops = append(ops, jsonPatchOp{Op: "test", Path: heldPath')]),
    ("R3 removes K's marker while K holds elsewhere", "patch.go", [("\tremoveMarker := !otherOfK && hasMarker\n", "\tremoveMarker := (otherOfK || !otherOfK) && hasMarker\n")]),
    ("R3 removes the held label while another kind holds", "patch.go", [("\tremoveHeld := !otherOfAny && hasHeld\n", "\tremoveHeld := (otherOfAny || !otherOfAny) && hasHeld\n")]),
    ("R3 removes an absent marker", "patch.go", [("\tremoveMarker := !otherOfK && hasMarker\n", "\tremoveMarker := !otherOfK && (hasMarker || !hasMarker)\n")]),
    ("R3 never removes the marker", "patch.go", [("\tremoveMarker := !otherOfK && hasMarker\n", "\tremoveMarker := false && !otherOfK && hasMarker\n")]),
    ("R3 never removes the held label", "patch.go", [("\tremoveHeld := !otherOfAny && hasHeld\n", "\tremoveHeld := false && !otherOfAny && hasHeld\n")]),
    ("R3 counts another kind's finalizer as K's", "patch.go", [("\t\tif strings.HasPrefix(f, k.prefix) {\n\t\t\totherOfK = true", "\t\tif IsCredentialUseFinalizer(f) {\n\t\t\totherOfK = true")]),
    ("R3 tests an empty epoch as is", "patch.go", [('\tif epoch == "" {\n\t\t// Absent', '\tif false {\n\t\t// Absent')]),
    ("R3 on an object without the finalizer", "patch.go", [("\tif i < 0 {\n\t\treturn Patch{}, ErrNotHeld\n\t}\n\tepoch :=", "\tif i < 0 {\n\t\ti = 0\n\t}\n\tepoch :=")]),
    ("~ isn't escaped", "patch.go", [('strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")', 'strings.ReplaceAll(key, "/", "~1")')]),
    ("~ is escaped after /", "patch.go", [('strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")', 'strings.ReplaceAll(strings.ReplaceAll(key, "/", "~1"), "~", "~0")')]),
    ("a test op loses its value", "patch.go", [('\tValue any    `json:"value,omitempty"`\n', '\tValue any    `json:"-"`\n')]),
    ("the epoch repair isn't guarded", "patch.go", [
        ("\tops, err := testResourceVersion(read)\n\tif err != nil {\n\t\treturn Patch{}, err\n\t}\n\tif read.GetAnnotations() == nil {",
         "\tops, err := []jsonPatchOp(nil), error(nil)\n\tif err != nil {\n\t\treturn Patch{}, err\n\t}\n\tif read.GetAnnotations() == nil {")]),
    ("the epoch repair overwrites others' annotations", "patch.go", [("\tif read.GetAnnotations() == nil {\n", "\tif true {\n")]),
    ("the janitor isn't guarded", "patch.go", [
        ("\tops, err := testResourceVersion(read)\n\tif err != nil {\n\t\treturn Patch{}, err\n\t}\n\tif removeMarker {",
         "\tops, err := []jsonPatchOp(nil), error(nil)\n\tif err != nil {\n\t\treturn Patch{}, err\n\t}\n\tif removeMarker {")]),
    ("the janitor removes K's marker while K holds", "patch.go", [("\tremoveMarker := hasMarker && !anyOfK\n", "\tremoveMarker := hasMarker && (anyOfK || !anyOfK)\n")]),
    ("the janitor removes the held label while a kind holds", "patch.go", [("\tremoveHeld := hasHeld && !anyOfAny\n", "\tremoveHeld := hasHeld && (anyOfAny || !anyOfAny)\n")]),

    # decide.go: §5's table and §6.1.
    ("a deleting holder without the owner finalizer counts", "decide.go", [
        ("\tif holder.GetDeletionTimestamp() != nil && !IsEstablished(holder, ownerFinalizers) {\n\t\treturn false\n\t}\n", "")]),
    ("every deleting holder is excluded", "decide.go", [
        ("\tif holder.GetDeletionTimestamp() != nil && !IsEstablished(holder, ownerFinalizers) {\n", "\tif holder.GetDeletionTimestamp() != nil {\n")]),
    ("a deleting user needn't use P", "decide.go", [
        ("\treturn holder.GetDeletionTimestamp() != nil && IsUser(holder, p, ownerFinalizers)\n", "\treturn holder.GetDeletionTimestamp() != nil\n")]),
    ("only the first owner finalizer counts", "decide.go", [
        ("\t\treturn slices.Contains(ownerFinalizers, f)\n", "\t\treturn len(ownerFinalizers) > 0 && f == ownerFinalizers[len(ownerFinalizers)-1]\n")]),
    ("a deleting P with no user waits", "decide.go", [("\tcase in.Users == 0:\n", "\tcase in.Users == 0 && !in.Deleting:\n")]),
    ("a live P with users is released", "decide.go", [("\tcase !in.Deleting:\n\t\treturn Keep\n", "\tcase !in.Deleting:\n\t\treturn Release\n")]),
    ("the namespace row ignores cluster scope", "decide.go", [("\tcase in.NamespaceDeleting && !in.ClusterScoped:\n", "\tcase in.NamespaceDeleting:\n")]),
    ("the namespace row is skipped", "decide.go", [("\tcase in.NamespaceDeleting && !in.ClusterScoped:\n\t\treturn Keep\n", "")]),
    ("the grace isn't waited", "decide.go", [("\tcase !in.GraceOver:\n\t\treturn WaitForGrace\n", "")]),
    ("a deleting user doesn't keep it after the grace", "decide.go", [("\tcase in.DeletingUsers > 0:\n\t\treturn Keep\n", "")]),
    ("the grace ignores first sight", "decide.go", [("\tif firstSeen.After(from) {\n", "\tif false {\n")]),
    ("the grace ignores the timestamp", "decide.go", [
        ("\tfrom := deletionTimestamp\n\tif firstSeen.After(from) {\n\t\tfrom = firstSeen\n\t}\n", "\tfrom := firstSeen\n")]),
]


# Mutants that are expected to survive, because another check makes them
# equivalent. They run anyway, and are reported as EQUIVALENT only if they
# survive; a kill is reported as a kill.
EQUIVALENT = {
    "the qualified-name check is skipped":
        "the namespace's DNS-label check and the prefix's DNS-subdomain check together imply it; it stays as the API server's own rule",
    "a policy's text only from the message":
        "the dispatcher puts the same text in a cause and in the message, by design; dropping both is the revision-15 mutant, which is killed",
    "a policy's text only from a cause":
        "as above",
    "a non-API error is classified by its text":
        "a zero Status has code 0, which no case matches, so it is ClassOther either way",
}


def go_test():
    env = dict(os.environ, CI="1")
    return subprocess.run(["go", "test", "-count=1", "-timeout", "300s", *PKGS], cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)


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


if not os.environ.get("KUBEBUILDER_ASSETS"):
    print("KUBEBUILDER_ASSETS is not set: the run is invalid")
    sys.exit(1)

start = digest()
base = go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print("baseline passed\n")

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
