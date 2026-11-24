"""Revert check for PR 8 of docs/plan-credential-use-implementation.md
(PerName mode, the index ConfigMap): break each rule the PR adds in turn and
see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant names, in KILLERS, the tests to run in each package: {package:
tests}. The baseline runs every named test of every package. The envtests need
KUBEBUILDER_ASSETS (CI=1 makes an envtest fail rather than skip without it).
The TestModelIndex* tests are the model's (§14), which CI runs in its model
job; they take well under a second here.

Usage:
  python3 hack/revert-check-credential-use-8.py > hack/revert-check-credential-use-8.txt

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
IX = C + "index.go"
PN = C + "pername.go"
AQ = C + "acquire.go"
FILES = [IX, PN, AQ, C + "release.go", C + "writeloops.go", C + "disown.go", C + "disownprefix.go", C + "uncached.go",
         C + "prober.go", C + "shared.go", C + "tracker.go", C + "informers.go", C + "adapter.go", C + "policy_envtest_test.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # §10.6.2: Dekker on the acquirer's side.
    ("no line before A2", AQ,
     [("\t\t\t\t\t// a full index, stops before A2.\n\t\t\t\t\tif err := p.lines.ensure(ctx, id); err != nil {",
       "\t\t\t\t\t// a full index, stops before A2.\n\t\t\t\t\tif err := error(nil); err != nil {")]),
    ("no check after A2", AQ,
     [("\terr := p.lines.postCheck(ctx, id)\n", "\terr := error(nil)\n")]),
    ("a failed check after A2 keeps the written entry", AQ,
     [("\tp.ver.released(uid, n)\n\treturn fmt.Errorf(\"credential-use: checking the index line", "\treturn fmt.Errorf(\"credential-use: checking the index line")]),
    ("a skip doesn't check the line", AQ,
     [("\t\t\tif held && p.keepsLine(id.GroupKind()) {", "\t\t\tif false && held && p.keepsLine(id.GroupKind()) {")]),
    # §10.6.2: Dekker on the pruner's side, and A14.
    ("the prune doesn't check after", IX,
     [("\t\tif ix.keepsLine(ctx, id, vouched) {", "\t\tif false && ix.keepsLine(ctx, id, vouched) {")]),
    ("a line a record names is pruned", IX,
     [("\t\tif ix.recorded != nil && ix.recorded(id) {", "\t\tif false {")]),
    ("a batch prunes nothing", C + "writeloops.go",
     [("\t\tif perr := t.idx.prune(ctx, t.idx.prunable(res.Bare)); perr != nil {", "\t\tif perr := error(nil); perr != nil {")]),
    ("R3's bare objects aren't reported", C + "release.go",
     [("\t\t\t\tif out != nil && countPrefix(out.Finalizers, prefix) == 0 {", "\t\t\t\tif false && out != nil && countPrefix(out.Finalizers, prefix) == 0 {")]),
    ("R1's bare objects aren't reported", C + "release.go",
     [("\t\t\t\tif countPrefix(m.Finalizers, prefix) == 0 {", "\t\t\t\tif false {")]),
    # Every write merges (§10.6.2, L5).
    ("an add overwrites", IX,
     [("\t\tnext := union(cur.lines, lines)\n", "\t\tnext := lines\n")]),
    ("an add's conflict isn't retried", IX,
     [("\t\tv, err := ix.store.update(ctx, cur, next, rebuilt)\n\t\tif Classify(err) == ClassConflict {\n\t\t\tlast = err\n\t\t\tcontinue\n\t\t}\n\t\tif err != nil {\n\t\t\treturn ix.writeErr(\"adding to\", err)",
       "\t\tv, err := ix.store.update(ctx, cur, next, rebuilt)\n\t\tif false {\n\t\t\tlast = err\n\t\t\tcontinue\n\t\t}\n\t\tif err != nil {\n\t\t\treturn ix.writeErr(\"adding to\", err)")]),
    ("a remove drops every line", IX,
     [("\t\t\tif slices.Contains(lines, l) {", "\t\t\tif true {")]),
    # §10.6.4: the cap, the size, the warning.
    ("a full index takes the line", IX,
     [("\tif len(next) > before && len(next) > ix.maxLines {", "\tif false {")]),
    ("no size check", IX,
     [("; size > indexMaxBytes {", "; false && size > indexMaxBytes {")]),
    ("no 80% warning", IX,
     [("\t\tif ratio >= indexFillWarning {", "\t\tif false {")]),
    # §10.6.6: tampering, deletion, the rebuild.
    ("a dropped line is never checked", IX,
     [("\t\t\tcheck = append(check, id)\n", "\t\t\t_ = id\n")]),
    ("this process's own drop is checked", IX,
     [("\t\t\tif ix.pruned[l] > 0 {", "\t\t\tif false {")]),
    ("an R3 in the per-name cache isn't used", IX,
     [("\t\t\tif ix.released != nil && ix.released(id) {", "\t\t\tif false {")]),
    ("a restored line isn't reported", IX,
     [("\t\t\tcase r.tampered && slices.Contains(back, id):\n", "\t\t\tcase false:\n")]),
    ("no rebuild after a deletion", IX,
     [("\t// index says otherwise.\n\th.t.idx.markLost()\n", "\t// index says otherwise.\n")]),
    ("the rebuild ignores the records", IX,
     [("\t\t\t\tnamed[id] = true\n", "\t\t\t\t_ = named\n")]),
    ("a created index isn't rebuilt", IX,
     [("\tif err := ix.rebuild(ctx); err != nil {\n\t\tix.markLost()\n", "\tif err := error(nil); err != nil {\n\t\tix.markLost()\n")]),
    # §10.6.5, §10.2: the startup recheck from the index.
    ("no startup recheck", C + "uncached.go",
     [("\t\t\tif err := s.recheckIndex(ctx, t); err != nil {", "\t\t\tif err := error(nil); err != nil {")]),
    ("the startup recheck enqueues nothing", IX,
     [("\t\t\t\tenqueue(id, n)\n", "\t\t\t\t_ = n\n")]),
    ("the startup recheck prunes nothing", IX,
     [("\tif err := ix.prune(ctx, ix.prunable(bare)); err != nil {\n\t\terrs = append(errs, err)\n\t}\n", "")]),
    # §10.6.1, §7.3: the pointer, the namespace, its holders.
    ("a disagreeing pointer starts", IX,
     [("\t\tif cur != want {", "\t\tif false {")]),
    ("an absent namespace isn't named", IX,
     [("\t\treturn fmt.Errorf(\"credential-use: index namespace %s not found: install it (installer release) before enabling PerName\", ix.namespace)",
       "\t\t_ = fmt.Errorf(\"credential-use: index namespace %s not found: install it (installer release) before enabling PerName\", ix.namespace)")]),
    ("holders in the index namespace start", IX,
     [("\tif err := t.noHoldersIn(ctx, ix.namespace); err != nil {", "\tif err := error(nil); err != nil {")]),
    ("a holder in the index namespace is held", C + "tracker.go",
     [("\tif t.idx != nil && obj.GetNamespace() != \"\" && obj.GetNamespace() == t.idx.namespace {", "\tif false {")]),
    ("a cap above the ceiling is accepted", C + "shared.go",
     [("s.o.IndexMaxLines > IndexMaxLinesCeiling {", "false {")]),
    # §7.4, A15: Disown in PerName mode.
    ("Disown ignores the index", C + "disown.go",
     [("\t\t\tif perName[id.GroupKind()] {\n\t\t\t\tnamed[id] = true\n\t\t\t\tlined = append(lined, id)",
       "\t\t\tif perName[id.GroupKind()] {\n\t\t\t\tlined = append(lined, id)")]),
    ("Disown doesn't empty the index", C + "disown.go",
     [("\t\tif err := t.idx.prune(ctx, lined); err != nil {", "\t\tif err := error(nil); err != nil {")]),
    ("an unreadable index counts", C + "disown.go",
     [("\t\tcur, err := t.idx.store.get(ctx)\n\t\tif err != nil {\n\t\t\tknown = false", "\t\tcur, err := t.idx.store.get(ctx)\n\t\tif false && err != nil {\n\t\t\tknown = false")]),
    ("DisownPrefix ignores Index", C + "disownprefix.go",
     [("\t\tif o.Index != nil {", "\t\tif false {")]),
    # §13 shape 1: per-name watches, GETs only, no lists.
    ("per-name watches don't follow the index", PN,
     [("\t\t\tif w := n.newWatch(id); w != nil {", "\t\t\tif w := (*nameWatch)(nil); w != nil {")]),
    ("a pruned line keeps its watch", PN,
     [("\t\tif !keep[id] {\n\t\t\tn.stopLocked(id, w)", "\t\tif false {\n\t\t\tn.stopLocked(id, w)")]),
    ("HasSynced doesn't wait on the index", C + "adapter.go",
     [("\tif t.idx != nil && !t.idx.inf.HasSynced() {", "\tif false && t.idx != nil && !t.idx.inf.HasSynced() {")]),
    ("a stopped watch stays in the gauge", C + "tracker.go",
     [("\tdelete(t.heldBy, k)\n\tdelete(t.waitingBy, k)\n", "")]),
    ("a stopped watch's late count reaches the gauge", C + "tracker.go",
     [("\tif t.gone || w != nil && w.dead {", "\tif t.gone {")]),
    ("the test policy matches cluster-scoped writes", C + "policy_envtest_test.go",
     [(", Scope: ptr.To(admissionregistrationv1.NamespacedScope)}", "}")]),
    ("a failed re-add after a prune isn't queued", IX,
     [("\t\tix.queue(back, false)\n", "")]),
    ("a failed live check isn't retried", IX,
     [("\tfor _, id := range failed {\n\t\tif r := ix.pending[id]; r != nil {", "\tfor _, id := range failed {\n\t\tdelete(ix.pending, id)\n\t\tif r := ix.pending[id]; r != nil {")]),
    ("the pointer is written over an existing one", IX,
     [("\t\tif !ok {\n\t\t\treturn crd, false, nil\n\t\t}\n\t\tif cur != want {", "\t\tif true || !ok {\n\t\t\treturn crd, false, nil\n\t\t}\n\t\tif cur != want {")]),
    ("a failed setup rebuild isn't left to the write loops", IX,
     [("\t\tix.markLost()\n\t\tt.warn(\"credential-use: rebuilding the new index failed", "\t\tt.warn(\"credential-use: rebuilding the new index failed")]),
    ("a failed post-check isn't queued", IX,
     [("\tif err := ix.add(ctx, []ID{id}); err != nil {\n\t\tix.queue([]ID{id}, false)\n", "\tif err := ix.add(ctx, []ID{id}); err != nil {\n")]),
    ("the marker is set before the rebuild completes", IX,
     [("\t\tif _, err := ix.store.create(ctx, nil, false); err != nil && Classify(err) != ClassAlreadyExists {", "\t\tif _, err := ix.store.create(ctx, nil, true); err != nil && Classify(err) != ClassAlreadyExists {")]),
    ("the marker isn't checked at start", IX,
     [("func (ix *index) needsRebuild(v indexVersion) bool { return !v.rebuilt }", "func (ix *index) needsRebuild(v indexVersion) bool { return false }")]),
    ("the rebuild fails on an unreadable object", IX,
     [("\t\tcase carriesUnknown, carriesRBACDenied:\n\t\t\tif vouched[id] {\n\t\t\t\tcontinue // vouched by an operator", "\t\tcase carriesUnknown, carriesRBACDenied:\n\t\t\tif true {\n\t\t\t\treturn fmt.Errorf(\"credential-use: reading %s to rebuild the index %s\", id, ix.ref())\n\t\t\t}\n\t\t\tif vouched[id] {\n\t\t\t\tcontinue // vouched by an operator")]),
    ("the recheck returns at an unreadable line", IX,
     [("\t\t\terrs = append(errs, fmt.Errorf(\"reading %s: %w\", id, err))\n\t\t\tcontinue\n", "\t\t\treturn errors.Join(append(errs, fmt.Errorf(\"reading %s: %w\", id, err))...)\n")]),
    ("the recheck outside the caches returns at an index's error", C + "uncached.go",
     [("\t\t\tif err := s.recheckIndex(ctx, t); err != nil {\n\t\t\t\terrs = append(errs, err)\n", "\t\t\tif err := s.recheckIndex(ctx, t); err != nil {\n\t\t\t\treturn err\n")]),
    ("an unreadable object counts as read", C + "disown.go",
     [("\t\t\t\tknown = false\n\t\t\t\tunreadable = append(", "\t\t\t\tunreadable = append(")]),
    ("Disown names no unreadable object", C + "disown.go",
     [("\tt.s.events.record(crd, \"Warning\", EventDisownUnreadable, note)\n", "\t_, _ = crd, note\n")]),
    ("a 403 on a recheck line is retried", IX,
     [("\t\t\tif Classify(err) == ClassRBAC {\n\t\t\t\t// The authorizer refused it", "\t\t\tif false {\n\t\t\t\t// The authorizer refused it")]),
    ("a policy's 403 counts as RBAC's", IX,
     [("\t\t\tif Classify(err) == ClassRBAC {\n\t\t\t\t// The authorizer refused it", "\t\t\tif c := Classify(err); c == ClassRBAC || c == ClassPolicyDenied {\n\t\t\t\t// The authorizer refused it")]),
    ("any 403 counts as RBAC's (IsForbidden)", IX,
     [("\t\"k8s.io/apimachinery/pkg/api/meta\"\n", "\tapierrors \"k8s.io/apimachinery/pkg/api/errors\"\n\t\"k8s.io/apimachinery/pkg/api/meta\"\n"),
      ("\tif Classify(err) == ClassRBAC {\n\t\tix.denied(id, err)", "\tif apierrors.IsForbidden(err) {\n\t\tix.denied(id, err)"),
      ("\t\t\tif Classify(err) == ClassRBAC {\n\t\t\t\t// The authorizer refused it", "\t\t\tif apierrors.IsForbidden(err) {\n\t\t\t\t// The authorizer refused it")]),
    ("an unreadable restore counts as tampering", IX,
     [("\t\t\tcase r.tampered && slices.Contains(unread, id) && slices.Contains(back, id):\n", "\t\t\tcase false && r.tampered && slices.Contains(unread, id) && slices.Contains(back, id):\n")]),
    ("a read doesn't clear a refusal", IX,
     [("\tdefer ix.mu.Unlock()\n\tdelete(ix.deniedSeen, id)\n", "\tdefer ix.mu.Unlock()\n")]),
    ("Disown counts a vouched unreadable object", C + "disown.go",
     [("\t\t\t\tif vouched[id] {\n\t\t\t\t\t// An operator checked by hand", "\t\t\t\tif false {\n\t\t\t\t\t// An operator checked by hand")]),
    ("a vouch hides a readable held object from Disown", C + "disown.go",
     [("\t\t\tm, err := live.Get(ctx, id.GroupKind(), id.Namespace, id.Name)\n", "\t\t\tif vouched[id] {\n\t\t\t\tcontinue\n\t\t\t}\n\t\t\tm, err := live.Get(ctx, id.GroupKind(), id.Namespace, id.Name)\n")]),
    ("the recheck skips vouched lines unread", IX,
     [("\tfor _, id := range ix.ids(cur.lines) {\n\t\tm, err := ix.srv.Get(", "\tfor _, id := range ix.ids(cur.lines) {\n\t\tif vouched[id] {\n\t\t\tcontinue\n\t\t}\n\t\tm, err := ix.srv.Get(")]),
    ("the tamper check restores a vouched line", IX,
     [("\t\tif vouched[id] && c != carriesYes && c != carriesNot {", "\t\tif false {")]),
    ("the tamper check drops a vouched line that carries", IX,
     [("\t\tif vouched[id] && c != carriesYes && c != carriesNot {", "\t\tif vouched[id] {")]),
    ("a prune keeps a vouched line", IX,
     [("\treturn !vouched[id]\n}", "\treturn true\n}")]),
    ("a rebuild adds a vouched line", IX,
     [("\t\t\tif vouched[id] {\n\t\t\t\tcontinue // vouched by an operator", "\t\t\tif false {\n\t\t\t\tcontinue // vouched by an operator")]),
    ("a malformed entry voids the vouch", IX,
     [("\t\t\tbad = append(bad, e)\n\t\t\tcontinue\n", "\t\t\tbad = append(bad, e)\n\t\t\tout = map[ID]bool{}\n\t\t\tbreak\n")]),
    ("the index's writes drop the vouch", IX,
     [("\tu := cur.obj.DeepCopy()\n\tif err := unstructured.SetNestedField(u.Object, encodeIndex(lines), \"data\", indexDataKey); err != nil {", "\tu := cur.obj.DeepCopy()\n\tu.SetAnnotations(nil)\n\tif err := unstructured.SetNestedField(u.Object, encodeIndex(lines), \"data\", indexDataKey); err != nil {")]),
    ("a 403 in a live check is retried", IX,
     [("\t\tcase carriesRBACDenied:\n\t\t\t// It may carry", "\t\tcase carriesRBACDenied + 100:\n\t\t\t// It may carry")]),
    ("a refused read is reported every time", IX,
     [("\tfirst := !ix.deniedSeen[id]\n", "\tfirst := true\n")]),
    ("watch modes needn't agree", PN,
     [("\t\tif c := s.modes[gk]; c != nil && c.mode != pk.Watch {", "\t\tif c := s.modes[gk]; false && c != nil && c.mode != pk.Watch {")]),
    ("the prober lists a PerName kind", C + "prober.go",
     [("\t\tif len(ids) > probeGetLimit && !t.isPerName(g.gk) && (", "\t\tif len(ids) > probeGetLimit && (")]),
    ("the marker sweep lists a PerName kind", C + "uncached.go",
     [("\t\t\tif pk.Watch == PerName {\n\t\t\t\tcontinue\n\t\t\t}\n\t\t\tif gk := pk.GVK.GroupKind(); !slices.Contains(kinds, gk) {",
       "\t\t\tif gk := pk.GVK.GroupKind(); !slices.Contains(kinds, gk) {")]),
    ("a PerName kind is uncached", C + "uncached.go",
     [("\t\tif pk.Watch == PerName {\n\t\t\t// Its per-name watches cover every namespace, and the lists\n\t\t\t// would be forbidden under resourceNames RBAC (§13 shape 1).\n\t\t\tcontinue\n\t\t}\n", "")]),
    # §10.6.1: class (c), matched by its text.
    ("the missing parameters aren't recognised", IX,
     [("\treturn strings.Contains(msg, \"ValidatingAdmissionPolicy '\"+policyName+\"'\") &&", "\treturn false && strings.Contains(msg, \"ValidatingAdmissionPolicy '\"+policyName+\"'\") &&")]),
]

KILLERS = {
    "no line before A2": {C: "TestIndexLinesBeforeA2AndPrunedAfterR3|TestIndexFillsAndFails|TestASkipStillChecksTheLine"},
    "no check after A2": {C: "TestModelIndexSuperset|TestModelIndexStartupPrune|TestIndexSupersetInterleaving|TestIndexA1BeforeTheDisownSwitch|TestASkipStillChecksTheLine"},
    "a failed check after A2 keeps the written entry": {C: "TestAFailedLineCheckDropsTheWrittenEntry"},
    "a skip doesn't check the line": {C: "TestASkipStillChecksTheLine"},
    "the prune doesn't check after": {C: "TestIndexPruneChecksAfter|TestModelIndexSuperset|TestModelIndexStartupPrune|TestIndexSupersetInterleaving"},
    "a line a record names is pruned": {C: "TestIndexPrunable|TestIndexLinesBeforeA2AndPrunedAfterR3"},
    "a batch prunes nothing": {C: "TestIndexLinesBeforeA2AndPrunedAfterR3|TestIndexRestartBetweenForgetAndR3"},
    "R3's bare objects aren't reported": {C: "TestReleaseReportsBare"},
    "R1's bare objects aren't reported": {C: "TestReleaseReportsBare"},
    "an add overwrites": {C: "TestIndexWritesMerge|TestIndexRebuildMergesWithAConcurrentAdd"},
    "an add's conflict isn't retried": {C: "TestIndexWritesMerge"},
    "a remove drops every line": {C: "TestIndexWritesMerge|TestIndexPruneChecksAfter"},
    "a full index takes the line": {C: "TestIndexFull|TestIndexFillsAndFails"},
    "no size check": {C: "TestIndexFull"},
    "no 80% warning": {C: "TestIndexFillsAndFails"},
    "a dropped line is never checked": {C: "TestIndexObservesDrops|TestIndexTampering"},
    "this process's own drop is checked": {C: "TestIndexObservesDrops"},
    "an R3 in the per-name cache isn't used": {C: "TestIndexObservesDrops|TestIndexTampering"},
    "a restored line isn't reported": {C: "TestIndexRecheck|TestIndexTampering"},
    "no rebuild after a deletion": {C: "TestIndexRebuiltAfterDeletion|TestIndexRebuildMergesWithAConcurrentAdd"},
    "the rebuild ignores the records": {C: "TestIndexRebuild|TestIndexRebuiltAfterDeletion"},
    "a created index isn't rebuilt": {C: "TestIndexCreatedAtStartRebuildsFromRecords"},
    "no startup recheck": {C: "TestIndexStartupRecheckWithoutTheWatch|TestIndexRestartBetweenForgetAndR3|TestIndexNeverEstablishedHolderDeletedWhileDown|TestIndexSupersetStartupPrune|TestPerNameUnderResourceNamesRBAC"},
    "the startup recheck enqueues nothing": {C: "TestIndexStartupRecheckWithoutTheWatch|TestIndexRestartBetweenForgetAndR3|TestIndexNeverEstablishedHolderDeletedWhileDown"},
    "the startup recheck prunes nothing": {C: "TestIndexSupersetStartupPrune|TestPerNameUnderResourceNamesRBAC|TestModelIndexStartupPrune"},
    "a disagreeing pointer starts": {C: "TestIndexPointerWrittenOnce"},
    "an absent namespace isn't named": {C: "TestIndexNamespaceChecks"},
    "holders in the index namespace start": {C: "TestIndexNamespaceChecks"},
    "a holder in the index namespace is held": {C: "TestIndexNamespaceChecks"},
    "a cap above the ceiling is accepted": {C: "TestTrackValidatesIndex"},
    "Disown ignores the index": {C: "TestDisownCountsAndEmptiesTheIndex|TestIndexA1BeforeTheDisownSwitch"},
    "Disown doesn't empty the index": {C: "TestDisownEmptiesTheIndexItself|TestDisownCountsAndEmptiesTheIndex|TestIndexA1BeforeTheDisownSwitch"},
    "an unreadable index counts": {C: "TestIndexUnreadableKeepsTheCountUnknown"},
    "DisownPrefix ignores Index": {C: "TestDisownPrefixFromTheIndex"},
    "per-name watches don't follow the index": {C: "TestIndexLinesBeforeA2AndPrunedAfterR3|TestIndexTampering|TestPerNameUnderResourceNamesRBAC"},
    "a pruned line keeps its watch": {C: "TestIndexLinesBeforeA2AndPrunedAfterR3|TestPerNameUnderResourceNamesRBAC"},
    "HasSynced doesn't wait on the index": {C: "TestHasSyncedWaitsOnTheIndex"},
    "a stopped watch stays in the gauge": {C: "TestAStoppedPerNameWatchLeavesTheGauge"},
    "a stopped watch's late count reaches the gauge": {C: "TestAStoppedPerNameWatchLeavesTheGauge"},
    "the test policy matches cluster-scoped writes": {C: "TestIndexNamespacePolicy"},
    "a failed re-add after a prune isn't queued": {C: "TestIndexPruneChecksAfter"},
    "a failed live check isn't retried": {C: "TestIndexRecheck"},
    "the pointer is written over an existing one": {C: "TestIndexPointerWrittenOnce"},
    "a failed setup rebuild isn't left to the write loops": {C: "TestIndexSetupRebuildFailureIsRetried"},
    "a failed post-check isn't queued": {C: "TestAFullPostCheckIsRetriedByTheWriteLoops"},
    "the marker is set before the rebuild completes": {C: "TestIndexCrashBeforeTheRebuildIsRebuiltAtRestart"},
    "the marker isn't checked at start": {C: "TestTheRebuiltMarker|TestIndexCrashBeforeTheRebuildIsRebuiltAtRestart|TestIndexAcquirerCreatedIndexIsRebuiltAtStart"},
    "the rebuild fails on an unreadable object": {C: "TestRecheckWithAnUnreadableRecordedObject|TestIndexRecheckWithAnUnreadableRecordedObject"},
    "the recheck returns at an unreadable line": {C: "TestRecheckWithAnUnreadableRecordedObject|TestIndexRecheckWithAnUnreadableRecordedObject"},
    "the recheck outside the caches returns at an index's error": {C: "TestIndexRecheckWithAnUnreadableRecordedObject"},
    "an unreadable object counts as read": {C: "TestDisownWithAnUnreadableIndexLine"},
    "Disown names no unreadable object": {C: "TestDisownWithAnUnreadableIndexLine"},
    "a 403 on a recheck line is retried": {C: "TestADeniedLineIsReportedOnceAndNotRetried|TestIndexRecheckDoesntRetryADeniedLine"},
    "a policy's 403 counts as RBAC's": {C: "TestADeniedLineIsReportedOnceAndNotRetried"},
    "any 403 counts as RBAC's (IsForbidden)": {C: "TestADeniedLineIsReportedOnceAndNotRetried"},
    "an unreadable restore counts as tampering": {C: "TestARestoredUnreadableLineIsNotTampering"},
    "a read doesn't clear a refusal": {C: "TestARefusalAfterAReadIsReportedAgain"},
    "Disown counts a vouched unreadable object": {C: "TestDisownWithAnUnreadableIndexLine"},
    "a vouch hides a readable held object from Disown": {C: "TestDisownCountsAVouchedObjectThatCarries"},
    "the recheck skips vouched lines unread": {C: "TestAVouchedLine"},
    "the tamper check restores a vouched line": {C: "TestAVouchedLine|TestDisownWithAnUnreadableIndexLine"},
    "the tamper check drops a vouched line that carries": {C: "TestAVouchedLine"},
    "a prune keeps a vouched line": {C: "TestAVouchedLine"},
    "a rebuild adds a vouched line": {C: "TestAVouchedLine"},
    "a malformed entry voids the vouch": {C: "TestParseVouched"},
    "the index's writes drop the vouch": {C: "TestIndexWritesPreserveTheVouch"},
    "a 403 in a live check is retried": {C: "TestADeniedLineIsReportedOnceAndNotRetried"},
    "a refused read is reported every time": {C: "TestADeniedLineIsReportedOnceAndNotRetried|TestIndexRecheckDoesntRetryADeniedLine"},
    "watch modes needn't agree": {C: "TestWatchModesAgree"},
    "the prober lists a PerName kind": {C: "TestProbePerNameGetsOnly"},
    "the marker sweep lists a PerName kind": {C: "TestPerNameUnderResourceNamesRBAC"},
    "a PerName kind is uncached": {C: "TestPerNameUnderResourceNamesRBAC"},
    "the missing parameters aren't recognised": {C: "TestPolicyParamsMissing|TestIndexNamespacePolicy"},
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
