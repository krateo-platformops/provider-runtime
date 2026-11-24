"""Revert check for the fix of the index's lost lines and Disown's settling
(§7.2, §7.4, §7.5, §10.6.2, §10.6.6): break each rule in turn and see whether
a test notices. It is also the proof that each new test fails without its
rule.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant names, in KILLERS, the tests to run in each package: {package:
tests}. The baseline runs every named test of every package. The envtests need
KUBEBUILDER_ASSETS (CI=1 makes an envtest fail rather than skip without it).
The TestModel* tests don't run under the race detector, and this doesn't use
it.

Usage:
  python3 hack/revert-check-credential-use-index.py > hack/revert-check-credential-use-index.txt

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
R = "pkg/reconciler/"
IX = C + "index.go"
AQ = C + "acquire.go"
D = C + "disown.go"
SH = C + "shared.go"
RC = R + "credentialuse.go"
SV = C + "server.go"
FILES = [IX, AQ, D, SH, SV, RC]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # §10.6.6: whose drop it is, and a check that a drop outlives.
    ("a drop is own in any version", IX,
     [("func(o ownRemoval) bool { return o.rv != \"\" && o.rv == cur.rv }", "func(o ownRemoval) bool { return o.rv != \"\" }")]),
    ("an early event isn't taken back", IX,
     [("if r := ix.pending[id]; r != nil && rv != \"\" && r.drop == rv {", "if r := ix.pending[id]; false && r != nil && rv != \"\" && r.drop == rv {")]),
    ("a drop queued for another reason is taken back", IX,
     [("\t\t\tr.drop = \"\"\n", "")]),
    ("own removals aren't bounded", IX,
     [("if n := len(ix.own) - maxOwnRemovals; n > 0 {", "if n := len(ix.own) - maxOwnRemovals; false && n > 0 {")]),
    ("a check takes a drop delivered during it", IX,
     [("\t\t\tif r.queued != queued[id] {", "\t\t\tif false && r.queued != queued[id] {")]),
    ("no drop is checked", IX,
     [("\tix.queueLocked(check, true, cur.rv)", "\tix.queueLocked(check[:0], true, cur.rv)")]),
    ("a bare object's dropped line is read on every tick", IX,
     [("\t\t\tif r.queued != queued[id] {", "\t\t\tif r.queued != queued[id] || slices.Contains(done, id) {")]),
    # §10.6.2: what is checked after a removal, and what cuts it short.
    ("only what a successful write removed is checked", IX,
     [("\t\ttried = union(tried, present)\n", ""),
      ("\t\tix.removed(v.rv, present)\n\t\treturn tried, nil\n", "\t\tix.removed(v.rv, present)\n\t\treturn union(tried, present), nil\n")]),
    ("a failed removal isn't checked", IX,
     [("\tif len(tried) == 0 {\n\t\treturn err\n", "\tif err != nil || len(tried) == 0 {\n\t\treturn err\n")]),
    ("a prune is cut short by its context", IX,
     [("context.WithTimeout(context.WithoutCancel(parent), ix.bound)", "context.WithTimeout(parent, ix.bound)")]),
    ("a prune is one write", IX,
     [("\tpruneChunk = 50\n", "\tpruneChunk = 100000\n")]),
    ("a prune's re-add has no time of its own", IX,
     [("ix.checkRemoved(ctx, whole, tried, kept)", "ix.checkRemoved(ctx, ctx, tried, kept)")]),
    ("a check cut short is no error", IX,
     [("\tif err := check.Err(); err != nil {\n\t\tcut =", "\tif err := check.Err(); err != nil {\n\t\t_ =")]),

    ("a prune begins after a stop", IX,
     [("\tfor chunk := range slices.Chunk(ids, pruneChunk) {\n\t\tif err := ctx.Err(); err != nil {\n", "\tfor chunk := range slices.Chunk(ids, pruneChunk) {\n\t\tif err := ctx.Err(); false && err != nil {\n")]),
    ("a prune's vouch is the one last seen", IX,
     [("\tif cur, err := ix.store.get(ctx); err == nil {\n\t\tvouched = ix.parseVouched(cur.vouched)\n\t}\n\tfor _, id := range carry {", "\tvouched = ix.vouchedNow()\n\tfor _, id := range carry {")]),
    # §7.4: the span, and its clock.
    ("no landing margin", SH,
     [("\treturn max(2*timeoutBound, timeoutBound+margin)\n", "\treturn 2 * timeoutBound\n")]),
    ("no factor of two", SH,
     [("\treturn max(2*timeoutBound, timeoutBound+margin)\n", "\treturn timeoutBound + margin\n")]),
    ("the next pass is due from before this one", D,
     [("\tnow := s.clock.Now()\n\tt.disown.mu.Lock()\n\tdefer t.disown.mu.Unlock()\n\tt.disown.next = now.Add(t.o.DisownInterval)", "\tnow := s.clock.Now().Add(-30 * time.Second)\n\tt.disown.mu.Lock()\n\tdefer t.disown.mu.Unlock()\n\tt.disown.next = now.Add(t.o.DisownInterval)")]),
    # §7.4: a lost index, and a count that isn't sure.
    ("a restored copy's marker counts", IX,
     [("rebuilt: uid != \"\" && ann[AnnotationRebuilt] == uid,", "rebuilt: ann[AnnotationRebuilt] != \"\",")]),
    ("an index with no uid is marked", IX,
     [("rebuilt: uid != \"\" && ann[AnnotationRebuilt] == uid,", "rebuilt: ann[AnnotationRebuilt] == uid,")]),
    ("a rebuild that creates the index doesn't mark it", IX,
     [("\t\t\tif rebuilt {\n\t\t\t\t// The marker is", "\t\t\tif false && rebuilt {\n\t\t\t\t// The marker is")]),
    ("a rebuild under Disown sets the marker", IX,
     [("\t\tmark = !on\n", "\t\tmark = !on || on\n")]),
    ("a switch's change asks for no rebuild", IX,
     [("if s := ix.snap.Load(); s != nil && !s.rebuilt {", "if s := ix.snap.Load(); false && s != nil && !s.rebuilt {")]),
    ("every kind is new", SH,
     [("\treturn !pointer && !config, nil\n", "\treturn !pointer && !config || true, nil\n")]),
    ("no kind is new", SH,
     [("\treturn !pointer && !config, nil\n", "\treturn !pointer && !config && false, nil\n")]),
    ("only the pointer is a history", SH,
     [("\treturn !pointer && !config, nil\n", "\treturn !pointer && (config || !config), nil\n")]),
    ("only the settings are a history", SH,
     [("\treturn !pointer && !config, nil\n", "\treturn !config && (pointer || !pointer), nil\n")]),
    ("any vouch for an index will do", IX,
     [("if v.rebuilt || v.uid == \"\" || uid != v.uid {", "if v.rebuilt || v.uid == \"\" || uid == \"\" {")]),
    ("a vouch is for every kind", IX,
     [("\t\tif !vouched[gk] {\n\t\t\treturn false\n", "\t\tif !vouched[gk] {\n\t\t\treturn true\n")]),
    ("a lost index's report names every kind", D,
     [("\tvouch := vouchFor(uid, kinds)\n", "\tvouch := vouchFor(uid, t.idx.kinds)\n")]),
    ("the report doesn't say when", D,
     [("(t.o.TimeoutBound + t.s.landingMargin()).String()", "t.o.TimeoutBound.String()")]),
    ("a lost index counts as known", D,
     [("\t\tlost := Classify(err) == ClassNotFound || err == nil && !cur.complete(perName)\n", "\t\tlost := Classify(err) == ClassNotFound\n")]),
    ("a lost index is reported as unreadable too", D,
     [("\t\t\tt.reportIndexLost(cur, perName)\n\t\tcase err != nil:\n", "\t\t\tt.reportIndexLost(cur, perName)\n\t\t\tif err != nil {\n\t\t\t\tt.reportDisownFailed(ctx, \"read the index\", err, \"\", \"\")\n\t\t\t}\n\t\tcase err != nil:\n")]),
    ("a lost index isn't reported", D,
     [("\tt.s.events.record(crd, \"Warning\", EventIndexLost, note)\n", "\t_, _ = crd, note\n")]),
    ("any failed check makes the count unknown", D,
     [("\t\t_ = t.idx.recheckTick(ctx, s.clock.Now())\n", "\t\tif err := t.idx.recheckTick(ctx, s.clock.Now()); err != nil {\n\t\t\tknown = false\n\t\t}\n")]),
    ("a pending check counts as sure", D,
     [("\t\tif !sure {", "\t\tif false && !sure {")]),
    ("what is owed is read after the index only", IX,
     [("\tsure = !ix.unsure(kinds)\n", "\tsure = true\n")]),
    ("what is owed is read before the index only", IX,
     [("\treturn cur, sure && !ix.unsure(kinds), err\n", "\treturn cur, sure, err\n")]),
    ("a rebuild due counts as sure", IX,
     [("\tif ix.lost {\n\t\treturn true\n\t}\n\tfor id := range ix.pending {", "\tif false {\n\t\treturn true\n\t}\n\tfor id := range ix.pending {")]),
    ("an A2 of unknown outcome counts as sure", IX,
     [("\t\tif kinds[l.id.GroupKind()] {\n\t\t\treturn true", "\t\tif kinds[l.id.GroupKind()] {\n\t\t\treturn false")]),
    # §7.4, E and point 4: when Disown empties the index, and of what.
    ("Disown empties before it has settled", D,
     [("\tif !s.writeDisownCount(ctx, t, set, count, known) || len(lined) == 0 {", "\tif s.writeDisownCount(ctx, t, set, count, known); len(lined) == 0 {")]),
    ("Disown empties every line, in every pass", D,
     [("\tif !s.writeDisownCount(ctx, t, set, count, known) || len(lined) == 0 {", "\tif s.writeDisownCount(ctx, t, set, count, known); len(lined) == 0 {"),
      ("t.freeLines(ctx, srv, lined, unstripped, vouched)", "lined")]),
    ("Disown never empties the index", D,
     [("\tif err := t.idx.empty(ctx, t.freeLines(ctx, srv, lined, unstripped, vouched)); err != nil {", "\tif err := error(nil); err != nil {")]),
    ("a switch over another kind keeps every line", D,
     [("\treturn parseDisown(ann[AnnotationDisown])\n}\n\n// disownKeeps", "\tset := parseDisown(ann[AnnotationDisown])\n\tset.all = set.on()\n\treturn set\n}\n\n// disownKeeps")]),
    ("a line that can't be read is free", D,
     [("\t\t\tif !vouched[id] {\n\t\t\t\tcontinue\n\t\t\t}\n\t\t}\n\t\tfree", "\t\t\tif false && !vouched[id] {\n\t\t\t\tcontinue\n\t\t\t}\n\t\t}\n\t\tfree")]),
    ("a held object's line is free", D,
     [("\t\t\tif countPrefix(m.Finalizers, t.prefix) > 0 {\n\t\t\t\tcontinue\n", "\t\t\tif false && countPrefix(m.Finalizers, t.prefix) > 0 {\n\t\t\t\tcontinue\n")]),
    ("an unstripped object's line is free", D,
     [("\t\tif unstripped[id] {\n\t\t\tcontinue\n", "\t\tif false && unstripped[id] {\n\t\t\tcontinue\n")]),
    ("a prune doesn't read the switch before it", IX,
     [("\tif ix.keptLive != nil && disown {", "\tif false && ix.keptLive != nil && disown {")]),
    ("a prune goes on with the switch unread", IX,
     [("\t\tif err != nil {\n\t\t\treturn fmt.Errorf(\"credential-use: reading the Disown switch before pruning", "\t\tif false && err != nil {\n\t\t\treturn fmt.Errorf(\"credential-use: reading the Disown switch before pruning")]),
    ("a prune doesn't read the switch after it", IX,
     [("\tif disown && ix.keptLive != nil {", "\tif false && disown && ix.keptLive != nil {")]),
    ("a switch that can't be read keeps nothing", IX,
     [("\t\t\tkept = func(schema.GroupKind) bool { return true }\n", "\t\t\tkept = func(schema.GroupKind) bool { return false }\n")]),
    ("a prune ignores the cached switch", IX,
     [("\tif ix.keptCached != nil {", "\tif false && ix.keptCached != nil {")]),
    ("the emptying keeps the lines", IX,
     [("\treturn ix.removeAndCheck(ctx, ids, false)\n", "\treturn ix.removeAndCheck(ctx, ids, true)\n")]),
    ("a record is stripped without its lines", D,
     [("\t\tif len(add) > 0 {\n", "\t\tif false && len(add) > 0 {\n")]),
    ("a record whose lines don't fit is stripped", D,
     [("func(id ID) bool { return unlined[id] || unstripped[id] })", "func(id ID) bool { return unstripped[id] })")]),
    ("the records are stripped before the objects", D,
     [("\tsrv := s.newServer()\n\tfor _, o := range objs {", "\tsrv := s.newServer()\n\tfor i := range holders {\n\t\tif patch, ok := disownHolderPatch(&holders[i], set); ok {\n\t\t\t_, _ = srv.Patch(ctx, t.gk, holders[i].Namespace, holders[i].Name, types.MergePatchType, patch)\n\t\t}\n\t}\n\tfor _, o := range objs {")]),
    ("a record whose object can't be read is stripped", D,
     [("\t\t\t\tunstripped[id] = true\n\t\t\t\tknown = false\n", "\t\t\t\tknown = false\n")]),
    ("a record whose object isn't stripped is", D,
     [("func(id ID) bool { return unlined[id] || unstripped[id] })", "func(id ID) bool { return unlined[id] })")]),
    ("a record that doesn't fit keeps every record", D,
     [("\t\tadd, over, full := t.idx.fitting(cur.lines, sets)\n", "\t\tadd, over, full := t.idx.fitting(cur.lines, [][]ID{slices.Concat(sets...)})\n")]),
    ("the largest record is lined first", IX,
     [("return cmp.Or(cmp.Compare(len(a), len(b)), cmp.Compare(size(a), size(b)))", "return cmp.Or(cmp.Compare(len(b), len(a)), cmp.Compare(size(b), size(a)))")]),
    ("what fits may pass the cap", IX,
     [("(count+len(fresh) > ix.maxLines || bytes+n > indexMaxBytes)", "(bytes+n > indexMaxBytes)")]),
    ("what fits may pass the size", IX,
     [("(count+len(fresh) > ix.maxLines || bytes+n > indexMaxBytes)", "(count+len(fresh) > ix.maxLines)")]),
    ("a line that is there needs room", IX,
     [("fresh := slices.DeleteFunc(SortedIDs(set), func(id ID) bool { return in[id.String()] })", "fresh := SortedIDs(set)")]),
    ("what doesn't fit isn't named", D,
     [("t.reportDisownUnlined(err, holdersNaming(holders, unlined))", "t.reportDisownUnlined(err, \"\")")]),
    ("a full index isn't unknown", D,
     [("\t\tkeep := func(ids []ID, err error) {\n\t\t\tknown = false\n", "\t\tkeep := func(ids []ID, err error) {\n")]),
    ("a full index isn't reported", D,
     [("\tif !errors.Is(err, errIndexFull) {", "\tif true || !errors.Is(err, errIndexFull) {")]),
    ("a bare object's drop stays, unsettled", IX,
     [("\t\t\tcase keep:\n", "\t\t\tcase false && keep:\n")]),
    ("a bare object's drop stays, the switch unread", IX,
     [("\t\t\tcase err != nil:\n\t\t\t\tfailed = append(failed, id)\n", "\t\t\tcase err != nil:\n\t\t\t\tdone = append(done, id)\n")]),
    ("a bare object's restored line is tampering", IX,
     [("\t\t\tcase slices.Contains(bare, id): // nothing is held\n", "\t\t\tcase false && slices.Contains(bare, id): // nothing is held\n")]),
    # §7.4: what disown-settled says.
    ("any flag is settled", D,
     [("\treturn v != \"\" && ann[AnnotationDisownSettled] == v && ann[AnnotationHeldCount] == \"0\"\n", "\treturn v != \"\" && ann[AnnotationDisownSettled] != \"\" && ann[AnnotationHeldCount] == \"0\"\n")]),
    ("no switch can have settled", D,
     [("\treturn v != \"\" && ann[AnnotationDisownSettled] == v", "\treturn ann[AnnotationDisownSettled] == v")]),
    ("the flag is true", D,
     [("\t\t\tsettledValue = ann[AnnotationDisown]\n", "\t\t\tsettledValue = \"true\"\n")]),
    ("a hold leaves a stale flag", D,
     [("\tif staleSettled(m.Annotations) {", "\tif false && staleSettled(m.Annotations) {")]),
    ("a stale flag's removal isn't guarded", D,
     [("\t\t\t\"resourceVersion\": m.ResourceVersion,\n\t\t\t\"annotations\":     map[string]any{AnnotationDisownSettled: nil},", "\t\t\t\"annotations\": map[string]any{AnnotationDisownSettled: nil},")]),
    ("a stale flag that stays lets a hold through", D,
     [("\t\tdefault:\n\t\t\treturn disownSet{}, fmt.Errorf(\"removing the leftover %s from CRD %s, without which nothing may be held: %w\", AnnotationDisownSettled, t.crdName, err)\n", "\t\tdefault:\n")]),
    ("a refused removal doesn't name the grant", D,
     [("\t\tcase ClassRBAC:\n\t\t\treturn disownSet{}, fmt.Errorf(\"removing", "\t\tcase ClassRBAC + 100:\n\t\t\treturn disownSet{}, fmt.Errorf(\"removing")]),
    ("a run goes on over a changed CRD", D,
     [("\tif d.zeros > 1 && rv != d.rv {", "\tif false && d.zeros > 1 && rv != d.rv {")]),
    ("a pass counts over a CRD changed during it", D,
     [("\tif d.startRV != \"\" && rv != d.startRV {", "\tif false && d.startRV != \"\" && rv != d.startRV {")]),
    ("a process clears another's flag", D,
     [("\t\twrite = staleSettled(ann)\n", "\t\twrite = flag != \"\"\n")]),
    ("a pass that couldn't read its CRD is of a run", D,
     [("\trv := unreadRV\n", "\trv := \"\"\n")]),
    ("restarted runs aren't counted", D,
     [("\t\tsinkMetrics{t}.DisownRestarts(kindLabel(t.gk))\n", "\t\t_ = changed\n")]),
    ("one restarted run is reported", D,
     [("changed && lasted >= 2*time.Duration(t.o.DisownPasses-1)*t.o.DisownInterval &&", "changed && lasted >= 0 &&")]),
    ("every restarted run is reported", D,
     [("d.began.Sub(d.reported) >= eventThrottle)", "d.began.Sub(d.reported) >= 0)")]),
    ("the count's own writes are named", D,
     [("func(e metav1.ManagedFieldsEntry) bool { return ownsOnlyDisownFields(e) }", "func(e metav1.ManagedFieldsEntry) bool { return false && ownsOnlyDisownFields(e) }")]),
    ("the oldest writers are named", D,
     [("\t\treturn bt.Compare(at)\n", "\t\treturn at.Compare(bt)\n")]),
    ("only the last writer is kept", D,
     [("d.writers = lastWriters(now[:min(1, len(now))], d.writers)", "d.writers = lastWriters(now[:min(1, len(now))], nil)")]),
    ("the writers outlive their streak", D,
     [("\td.restarts, d.wanting, d.writers = 0, time.Time{}, nil\n", "\td.restarts, d.wanting = 0, time.Time{}\n")]),
    ("every writer is named", D,
     [("\t\tif len(out) < maxWriters && !slices", "\t\tif !slices")]),
    ("a writer is named more than once", D,
     [("len(out) < maxWriters && !slices.ContainsFunc(out, func(o crdWriter) bool { return o.manager == w.manager }) {", "len(out) < maxWriters {")]),
    # §6.2, §7.2, §10.6.2: an A2 cut short, and one of unknown outcome.
    ("an A2 is cut short by its caller", AQ,
     [("\t\tissuedOne = true\n\t\tout, err := p.srv.Patch(cc.write, gk, id.Namespace, id.Name, pt.Type, pt.Data)\n\t\tif err != nil {", "\t\tissuedOne = true\n\t\tout, err := p.srv.Patch(ctx, gk, id.Namespace, id.Name, pt.Type, pt.Data)\n\t\tif err != nil {")]),
    ("an A2 outlives its caller's deadline", AQ,
     [("context.WithDeadline(base, d)", "context.WithDeadline(base, d.Add(time.Hour))")]),
    ("an A2 without a deadline isn't bounded", AQ,
     [("write, endWrite = context.WithTimeout(base, p.a2Bound)", "write, endWrite = context.WithCancel(base)")]),
    ("an A2 begins after a stop", AQ,
     [("\tif err := ctx.Err(); err != nil {\n\t\treturn ClassOther, \"\", err\n\t}\n\tissuedOne", "\tissuedOne")]),
    ("an A2 in flight goes on too long", AQ,
     [("time.AfterFunc(a2, endWrite)", "time.AfterFunc(a2+10*time.Second, endWrite)")]),
    ("a line check goes on too long", AQ,
     [("time.AfterFunc(call, endCheck)", "time.AfterFunc(call+10*time.Second, endCheck)"),
      ("context.WithTimeout(cc.check, call)", "context.WithTimeout(cc.check, call+10*time.Second)")]),
    ("an A2 of unknown outcome isn't checked", AQ,
     [("\t\tif issuedOne && mayHaveLanded(err) && ", "\t\tif false && issuedOne && mayHaveLanded(err) && ")]),
    ("an A2 refused with an unconfirmed 404 is of unknown outcome", AQ,
     [("\tif errors.As(err, &refused) {\n", "\tif false && errors.As(err, &refused) {\n")]),
    ("a 404 no read confirms isn't marked as a refusal", SV,
     [("\t\t\terr = refusedPatch{err}\n", "\t\t\t_ = refusedPatch{err}\n")]),
    ("a refusal loses its cause", SV,
     [("func (e refusedPatch) Unwrap() error { return e.err }\n", "")]),
    ("a refused A2 is of unknown outcome", AQ,
     [("st.Code < 400 || st.Code >= 500 ||", "st.Code > 0 ||")]),
    ("an A2 of unknown outcome owes no batch", AQ,
     [("\t\t\tif p.enqueueBatch != nil {\n\t\t\t\tp.enqueueBatch(id, namespace)\n\t\t\t}\n\t\t}\n\t}()", "\t\t\tif false && p.enqueueBatch != nil {\n\t\t\t\tp.enqueueBatch(id, namespace)\n\t\t\t}\n\t\t}\n\t}()")]),
    ("no check at once", IX,
     [("\t_ = ix.postCheck(ctx, id) // what fails is queued\n", "\t_ = ctx\n")]),
    ("no late check", IX,
     [("\tix.late[lateHold{id, n}] = due\n", "\t_, _ = due, n\n")]),
    ("the late check comes before the margin is over", IX,
     [("\t\tif !now.Before(at) {\n\t\t\tdue = append(due, l)", "\t\tif true || !now.Before(at) {\n\t\t\tdue = append(due, l)")]),
    ("the late check reads nothing", IX,
     [("\t\t\tix.queueLocked([]ID{l.id}, false, \"\")\n", "")]),
    ("the write loops make no late check", IX,
     [("\t\t\t\tt.idx.lateTick(now, func(id ID, n string) { s.queue.Add(batchKey{t.gk, n}, id) })\n", "")]),
    # §7.5: no pass without the write loops.
    ("a standby's pass runs", RC,
     [("\t\treturn c.wiring.notWriting(kind, time.Now())\n", "\t\treturn nil\n")]),
    ("a standby that reconciles doesn't fail the manager", RC,
     [("\tif waited >= sourceStartTimeout {", "\tif false && waited >= sourceStartTimeout {")]),
    ("the write loops' start isn't seen", RC,
     [("\tw.writing.Store(true)\n", "")]),
]

KILLERS = {
    "a drop is own in any version": {C: "TestIndexObservesDrops"},
    "an early event isn't taken back": {C: "TestIndexObservesDrops"},
    "a drop queued for another reason is taken back": {C: "TestIndexObservesDrops"},
    "own removals aren't bounded": {C: "TestIndexObservesDrops"},
    "a check takes a drop delivered during it": {C: "TestADropDuringItsCheckSurvives|TestModelIndexDrops"},
    "no drop is checked": {C: "TestIndexTampering|TestIndexObservesDrops"},
    "a bare object's dropped line is read on every tick": {C: "TestIndexTampering"},
    "only what a successful write removed is checked": {C: "TestPruneChecksWhatItTried"},
    "a failed removal isn't checked": {C: "TestPruneChecksWhatItTried"},
    "a prune is cut short by its context": {C: "TestAPruneOutlivesAStop"},
    "a prune is one write": {C: "TestAPruneOfManyLines"},
    "a prune's re-add has no time of its own": {C: "TestAPruneCutShort"},
    "a check cut short is no error": {C: "TestAPruneCutShort"},

    "a prune begins after a stop": {C: "TestAPruneOutlivesAStop|TestAPruneCutShort"},
    "a prune's vouch is the one last seen": {C: "TestAVouchWithdrawnDuringAPrune"},
    "no landing margin": {C: "TestDisownIntervalDerivedFromTheTimeoutBound|TestTrackValidates"},
    "no factor of two": {C: "TestDisownIntervalDerivedFromTheTimeoutBound|TestTrackValidates"},
    "the next pass is due from before this one": {C: "TestDisownPassesAreAnIntervalApart"},
    "a restored copy's marker counts": {C: "TestIndexWritesPreserveTheVouch|TestARestoredIndexIsALostOne"},
    "an index with no uid is marked": {C: "TestIndexWritesPreserveTheVouch"},
    "a rebuild that creates the index doesn't mark it": {C: "TestARebuildThatCreatesTheIndexMarksIt"},
    "a rebuild under Disown sets the marker": {C: "TestARebuildUnderDisownLeavesNoMarker|TestIndexLostUnderDisown|TestIndexUnreadableKeepsTheCountUnknown"},
    "a switch's change asks for no rebuild": {C: "TestARebuildUnderDisownLeavesNoMarker"},
    "every kind is new": {C: "TestIndexLostUnderDisown"},
    "no kind is new": {C: "TestIndexLostUnderDisown"},
    "only the pointer is a history": {C: "TestIndexLostUnderDisown"},
    "only the settings are a history": {C: "TestIndexLostUnderDisown"},
    "any vouch for an index will do": {C: "TestIndexVersionComplete|TestIndexLostUnderDisown"},
    "a vouch is for every kind": {C: "TestIndexVersionComplete|TestALostIndexReportNamesTheCoveredKinds"},
    "a lost index's report names every kind": {C: "TestALostIndexReportNamesTheCoveredKinds"},
    "the report doesn't say when": {C: "TestALostIndexReportNamesTheCoveredKinds"},
    "a lost index counts as known": {C: "TestIndexLostUnderDisown"},
    "a lost index is reported as unreadable too": {C: "TestALostIndexHasOneReport"},
    "a lost index isn't reported": {C: "TestIndexLostUnderDisown"},
    "any failed check makes the count unknown": {C: "TestAFailedCheckOfAnotherKind"},
    "a pending check counts as sure": {C: "TestDisownWaitsForAPendingCheck"},
    "what is owed is read after the index only": {C: "TestReadSure"},
    "what is owed is read before the index only": {C: "TestReadSure"},
    "a rebuild due counts as sure": {C: "TestIndexUnsure"},
    "an A2 of unknown outcome counts as sure": {C: "TestTheLateCheck"},
    "Disown empties before it has settled": {C: "TestIndexA1BeforeTheDisownSwitch|TestTwoProcessesSettleApart"},
    "Disown empties every line, in every pass": {C: "TestTwoProcessesDisownAnUnreadableHold|TestDisownWithAnUnreadableIndexLine"},
    "Disown never empties the index": {C: "TestDisownEmptiesTheIndexItself"},
    "a switch over another kind keeps every line": {C: "TestDisownOfAnotherKindPrunesThisOne"},
    "a line that can't be read is free": {C: "TestFreeLines"},
    "a held object's line is free": {C: "TestFreeLines"},
    "an unstripped object's line is free": {C: "TestFreeLines"},
    "a prune doesn't read the switch before it": {C: "TestAPruneWhileDisownHasntSettled"},
    "a prune goes on with the switch unread": {C: "TestAPruneWhileDisownHasntSettled"},
    "a prune doesn't read the switch after it": {C: "TestAPruneWhileDisownHasntSettled"},
    "a switch that can't be read keeps nothing": {C: "TestAPruneWhileDisownHasntSettled"},
    "a prune ignores the cached switch": {C: "TestAPruneWhileDisownHasntSettled"},
    "the emptying keeps the lines": {C: "TestAPruneWhileDisownHasntSettled"},
    "a record is stripped without its lines": {C: "TestDisownLinesWhatTheRecordsName"},
    "a record whose lines don't fit is stripped": {C: "TestDisownWithAFullIndex"},
    "the records are stripped before the objects": {C: "TestDisownPassCounts"},
    "a record whose object can't be read is stripped": {C: "TestDisownKeepsTheRecordOfAnUnreadableSecret"},
    "a record whose object isn't stripped is": {C: "TestDisownPassCounts"},
    "a record that doesn't fit keeps every record": {C: "TestDisownWithARecordThatDoesntFit"},
    "the largest record is lined first": {C: "TestIndexFitting"},
    "what fits may pass the cap": {C: "TestIndexFitting"},
    "what fits may pass the size": {C: "TestIndexFitting"},
    "a line that is there needs room": {C: "TestIndexFitting"},
    "what doesn't fit isn't named": {C: "TestDisownWithARecordThatDoesntFit"},
    "a full index isn't unknown": {C: "TestDisownWithAFullIndex"},
    "a full index isn't reported": {C: "TestDisownWithAFullIndex"},
    "a bare object's drop stays, unsettled": {C: "TestABareDropWhileDisownHasntSettled"},
    "a bare object's drop stays, the switch unread": {C: "TestABareDropWhileDisownHasntSettled"},
    "a bare object's restored line is tampering": {C: "TestABareDropWhileDisownHasntSettled"},
    "any flag is settled": {C: "TestDisownSettledIsTheSwitchsValue"},
    "no switch can have settled": {C: "TestDisownSettledIsTheSwitchsValue"},
    "the flag is true": {C: "TestDisownSettling|TestDisownUnderAnOldTrue"},
    "a hold leaves a stale flag": {C: "TestAHoldClearsAStaleFlag|TestDisownTurnedOffAndOnAgain"},
    "a stale flag's removal isn't guarded": {C: "TestAHoldClearsAStaleFlag"},
    "a stale flag that stays lets a hold through": {C: "TestAHoldClearsAStaleFlag"},
    "a refused removal doesn't name the grant": {C: "TestAHoldClearsAStaleFlag"},
    "a run goes on over a changed CRD": {C: "TestDisownRunsOverAnUnchangedCRD"},
    "a pass counts over a CRD changed during it": {C: "TestDisownRunsOverAnUnchangedCRD|TestDisownReportsARunThatKeepsRestarting"},
    "a pass that couldn't read its CRD is of a run": {C: "TestDisownsFirstPassNeedsItsCRD"},
    "restarted runs aren't counted": {C: "TestDisownReportsARunThatKeepsRestarting"},
    "one restarted run is reported": {C: "TestDisownReportsARunThatKeepsRestarting"},
    "every restarted run is reported": {C: "TestDisownReportsARunThatKeepsRestarting|TestDisownUnderTwoWritersThatNeverAgree"},
    "the count's own writes are named": {C: "TestDisownReportsARunThatKeepsRestarting"},
    "the oldest writers are named": {C: "TestDisownReportsARunThatKeepsRestarting"},
    "only the last writer is kept": {C: "TestDisownReportsWritersThatOverwriteEachOther|TestDisownUnderTwoWritersThatNeverAgree"},
    "the writers outlive their streak": {C: "TestDisownReportsWritersThatOverwriteEachOther"},
    "every writer is named": {C: "TestDisownReportsWritersThatOverwriteEachOther|TestDisownReportsARunThatKeepsRestarting"},
    "a writer is named more than once": {C: "TestDisownReportsWritersThatOverwriteEachOther"},
    "a process clears another's flag": {C: "TestDisownRunsOverAnUnchangedCRD|TestDisownLinesLeftByTheSettler"},
    "an A2 is cut short by its caller": {C: "TestAnA2OutlivesItsCaller|TestALostLeaseWithAnA2InFlight"},
    "an A2 outlives its caller's deadline": {C: "TestAnA2KeepsItsCallersDeadline"},
    "an A2 without a deadline isn't bounded": {C: "TestAnA2KeepsItsCallersDeadline"},
    "an A2 begins after a stop": {C: "TestAnA2OutlivesItsCaller"},
    "an A2 in flight goes on too long": {C: "TestACallReturnsThoughTheServerHangs"},
    "a line check goes on too long": {C: "TestACallReturnsThoughTheServerHangs"},
    "an A2 of unknown outcome isn't checked": {C: "TestAnA2OfUnknownOutcomeIsChecked|TestALateA2IsReleased"},
    "an A2 refused with an unconfirmed 404 is of unknown outcome": {C: "TestAnA2RefusedWithA404DidntLand"},
    "a 404 no read confirms isn't marked as a refusal": {C: "TestAnA2RefusedWithA404DidntLand"},
    "a refusal loses its cause": {C: "TestPatchConfirmsA404WithARead"},
    "a refused A2 is of unknown outcome": {C: "TestAnA2OfUnknownOutcomeIsChecked"},
    "an A2 of unknown outcome owes no batch": {C: "TestAnA2OfUnknownOutcomeIsChecked"},
    "no check at once": {C: "TestTheLateCheck"},
    "no late check": {C: "TestTheLateCheck|TestALateA2IsReleased"},
    "the late check comes before the margin is over": {C: "TestTheLateCheck"},
    "the late check reads nothing": {C: "TestTheLateCheck"},
    "the write loops make no late check": {C: "TestALateA2IsReleased"},
    "a standby's pass runs": {R: "TestCredentialUseNeedsTheWriteLoops"},
    "a standby that reconciles doesn't fail the manager": {R: "TestCredentialUseNeedsTheWriteLoops"},
    "the write loops' start isn't seen": {R: "TestCredentialUseNeedsTheWriteLoops"},
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
