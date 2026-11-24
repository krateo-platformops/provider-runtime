"""Revert check for a holder kind that is gone, and for the informers' failures
(design §6.4 "The holder kind itself gone", §6.2 R0, §10.2, §12): break each
rule, and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started. No test here is an envtest, but KUBEBUILDER_ASSETS is
asked for as in the others.

Usage:
  python3 hack/revert-check-credential-use-crd-gone.py > hack/revert-check-credential-use-crd-gone.txt

REVERT_TIMEOUT_S, REVERT_ONLY and REVERT_CHECK_ONLY are as in the others.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
C = "pkg/credentialuse/"
CG, WF, RL, TR, DI, IX, SH, PN, LW = [C + f for f in (
    "crdgone.go", "watchfail.go", "release.go", "tracker.go", "disown.go", "index.go", "shared.go", "pername.go", "listwatch.go")]
R = "pkg/reconciler/"
RC = R + "credentialuse.go"
FILES = [CG, WF, RL, TR, DI, IX, SH, PN, LW, RC]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    # R2 without a list of holders.
    ("R2 never asks whether the kind is gone", RL,
     [("\tif err != nil && p.noHolders != nil {\n", "\tif false && err != nil && p.noHolders != nil {\n")]),
    ("R2 takes any failed list for an empty one", RL,
     [("\t\terr = p.noHolders(ctx, err)\n", "\t\t_, err = p.noHolders(ctx, err), error(nil)\n")]),
    ("R2 asks though its list succeeded", RL,
     [("\tif err != nil && p.noHolders != nil {\n", "\tif p.noHolders != nil {\n")]),
    ("the tracker's protocol asks nobody", TR,
     [("\t\tnoHolders:  t.noHolders,\n", "")]),
    ("any failed list is worth the GET", CG,
     [("\tif !kindUnserved(listErr) {\n", "\tif false && !kindUnserved(listErr) {\n")]),
    ("a list's 404 isn't worth the GET", CG,
     [("\treturn ok && st.Code == http.StatusNotFound\n}", "\treturn false && ok && st.Code == http.StatusNotFound\n}")]),
    ("a mapper's no match isn't worth the GET", CG,
     [("\tif meta.IsNoMatchError(err) {\n", "\tif false && meta.IsNoMatchError(err) {\n")]),
    ("the list's 404 is the evidence", CG,
     [("\tcase err == nil:\n\t\treturn listErr\n\tcase !crd", "\tcase err == nil:\n\tcase !crd")]),
    ("a GET that fails is the evidence", CG,
     [("\tcase !crdNotFound(err, t.crdName):\n", "\tcase false && !crdNotFound(err, t.crdName):\n")]),
    ("a GET that fails says nothing of its own", CG,
     [("\", listErr, what, err)\n}", "\", listErr, what, listErr)\n}")]),
    ("any read that fails advises a grant", CG,
     [("\tif apierrors.IsForbidden(err) {\n", "\tif true || apierrors.IsForbidden(err) {\n")]),
    ("a refused read advises no grant", CG,
     [("\tif apierrors.IsForbidden(err) {\n", "\tif false && apierrors.IsForbidden(err) {\n")]),
    ("the GET may be answered by the watch cache", CG,
     [("Get(ctx, t.crdName, metav1.GetOptions{})", "Get(ctx, t.crdName, metav1.GetOptions{ResourceVersion: \"0\"})")]),
    # The discovery's veto.
    ("discovery isn't asked", CG,
     [("\tresource, gv, err := t.servedElsewhere(ctx)\n", "\tresource, gv, err := \"\", \"\", error(nil)\n")]),
    ("its own resource asks for a restart", CG,
     [("\tcase plural == t.gvr.Resource:\n", "\tcase false && plural == t.gvr.Resource:\n")]),
    ("its own subresource asks for a restart", CG,
     [("\tplural, _, _ := strings.Cut(resource, \"/\")\n", "\tplural := resource\n")]),
    ("another group's kind is a veto", CG,
     [("\t\t\tif r.Group != \"\" && r.Group != t.gk.Group {\n", "\t\t\tif false && r.Group != \"\" && r.Group != t.gk.Group {\n")]),
    ("its own group's kind, named, is no veto", CG,
     [("\t\t\tif r.Group != \"\" && r.Group != t.gk.Group {\n", "\t\t\tif r.Group != \"\" {\n")]),
    ("discovery is asked before the GET", CG,
     [("\t_, err = t.s.c.Metadata.Resource(crdGVR)", "\tif resource, _, err := t.servedElsewhere(ctx); err != nil || resource != \"\" {\n\t\treturn listErr\n\t}\n\t_, err = t.s.c.Metadata.Resource(crdGVR)")]),
    ("a discovery that can't be read is no veto", CG,
     [("\tcase err != nil:\n\t\treturn unread(listErr, \"whether another", "\tcase false && err != nil:\n\t\treturn unread(listErr, \"whether another")]),
    ("another resource of the kind is no veto", CG,
     [("\tcase resource != \"\":\n", "\tcase false && resource != \"\":\n")]),
    ("without a client a kind may be gone", CG,
     [("\t\treturn \"\", \"\", errors.New(\"Clients.Discovery isn't set\")\n", "\t\t_ = errors.New(\"x\")\n\t\treturn \"\", \"\", nil\n")]),
    ("a discovery that hangs holds the worker", CG,
     [("context.WithTimeout(ctx, discoveryTimeout)", "context.WithCancel(ctx)")]),
    ("a group that can't be read isn't served", CG,
     [("\tif st, ok := statusOf(err); ok && st.Code == http.StatusNotFound {\n\t\treturn \"\", \"\", nil // the", "\tif err != nil {\n\t\treturn \"\", \"\", nil // the")]),
    ("a group that isn't served can't be read", CG,
     [("\tif st, ok := statusOf(err); ok && st.Code == http.StatusNotFound {\n\t\treturn \"\", \"\", nil // the", "\tif st, ok := statusOf(err); false && ok && st.Code == http.StatusNotFound {\n\t\treturn \"\", \"\", nil // the")]),
    ("any document is the group's", CG,
     [("\tif group.Kind != \"APIGroup\" || group.Name != t.gk.Group || len(group.Versions) == 0 {\n", "\tif false {\n")]),
    ("a version that can't be read is skipped", CG,
     [("t.gk.Group, v.Version); err != nil {\n\t\t\treturn \"\", \"\", err\n","t.gk.Group, v.Version); err != nil {\n\t\t\tcontinue\n")]),
    ("only the first version is read", CG,
     [("\tfor _, v := range group.Versions {\n", "\tfor _, v := range group.Versions[:1] {\n")]),
    ("any document is the version's", CG,
     [("\t\tif list.Kind != \"APIResourceList\" || list.GroupVersion != v.GroupVersion {\n", "\t\tif false {\n")]),
    ("provider-runtime sets no Discovery", RC,
     [("\t\tDiscovery: disc.RESTClient(),\n", "\t\t// disc\n"), ("\tdisc, err := discovery", "\t_, err = discovery")]),
    ("a Track without Discovery says nothing", SH,
     [("\tif s.c.Discovery == nil {\n", "\tif false && s.c.Discovery == nil {\n")]),
    ("every Track warns of Discovery", SH,
     [("\tif s.c.Discovery == nil {\n", "\tif true || s.c.Discovery == nil {\n")]),
    ("the kind's case matters", CG,
     [("strings.EqualFold(r.Kind, t.gk.Kind)", "r.Kind == t.gk.Kind")]),
    ("a 404 that isn't the API server's is the evidence", CG,
     [("\treturn ok && Classify(err) == ClassNotFound && st.", "\treturn ok && st.Code == http.StatusNotFound && st.Details != nil && st.")]),
    ("a 404 that names nothing is the evidence", CG,
     [("\treturn ok && Classify(err) == ClassNotFound && st.",
       "\tif ok && st.Code == http.StatusNotFound && (st.Details == nil || st.Details.Name == \"\") {\n\t\treturn true\n\t}\n"
       "\treturn ok && Classify(err) == ClassNotFound && st.")]),
    ("a 404 that names another CRD is the evidence", CG,
     [(" && st.Details.Name == name && ", " && st.Details.Name != \"\" && ")]),
    ("a 404 of another resource is the evidence", CG,
     [(" && st.Details.Group == crdGVR.Group\n", " && st.Details.Group != \"\"\n")]),
    ("a kind that moved to another plural is gone", CG,
     [("\tcase err == nil && m.Resource.GroupResource() != t.gvr.GroupResource():\n",
       "\tcase false && err == nil && m.Resource.GroupResource() != t.gvr.GroupResource():\n")]),
    ("a mapper that can't tell is taken for no match", CG,
     [("\tcase err != nil && !meta.IsNoMatchError(err):\n", "\tcase false && err != nil && !meta.IsNoMatchError(err):\n")]),
    ("what is released for want of the kind isn't logged", RL,
     [("\t\t\t\tif gone && p.releasedGone != nil {\n", "\t\t\t\tif false && gone && p.releasedGone != nil {\n")]),
    ("any release is logged as for want of the kind", RL,
     [("\t\t\t\tif gone && p.releasedGone != nil {\n", "\t\t\t\tif (gone || true) && p.releasedGone != nil {\n")]),
    ("every release for want of the kind is logged", CG,
     [("\tif t.goneLogged.CompareAndSwap(false, true) {\n", "\tif true || t.goneLogged.CompareAndSwap(false, true) {\n")]),
    ("a kind that goes again isn't logged again", DI,
     [("OnAdd(any, bool) { h.t.goneLogged.Store(false) }", "OnAdd(any, bool) {}")]),
    # R0 and the holder cache.
    ("R0 trusts the cache of a kind that is gone", RL,
     [("\tabsent := p.kindAbsent != nil && p.kindAbsent()\n", "\tabsent := false && p.kindAbsent != nil && p.kindAbsent()\n")]),
    ("R0 never trusts the cache", RL,
     [("\t\tif !absent {\n", "\t\tif false && !absent {\n")]),
    ("the tracker's protocol doesn't know of its CRD", TR,
     [("\t\tkindAbsent: t.crdAbsent,\n", "")]),
    ("an unsynced informer's CRD is absent", CG,
     [("\tif t.crd == nil || !t.crd.HasSynced() {\n", "\tif t.crd == nil {\n")]),
    ("a CRD is never absent", CG,
     [("\treturn err == nil && !ok\n}", "\treturn false && err == nil && !ok\n}")]),
    ("the CRD's deletion queues nothing", DI,
     [("func (h crdHandler) OnDelete(any)    { h.t.s.requeueHeld(h.t) }\n", "func (h crdHandler) OnDelete(any)    {}\n")]),
    ("the CRD's deletion queues nothing of the per-name caches", CG,
     [("append(s.protectedInformers(), t.names.all()...)", "s.protectedInformers()")]),
    ("the CRD's deletion rechecks nothing outside the caches", CG,
     [("\tif len(s.uncachedKinds(t)) > 0 || t.idx != nil {\n", "\tif t.idx != nil {\n")]),
    ("the CRD's deletion doesn't recheck the index", CG,
     [("\tif len(s.uncachedKinds(t)) > 0 || t.idx != nil {\n", "\tif len(s.uncachedKinds(t)) > 0 {\n")]),
    ("each deletion starts a recheck", CG,
     [("\tif t.rechecking {\n", "\tif false && t.rechecking {\n")]),
    ("a recheck asked for meanwhile is lost", CG,
     [("\t\tt.recheckAgain = true\n", "")]),
    ("a recheck is a tracker's last", CG,
     [("\t\t\tt.rechecking, t.recheckAgain = again, false\n", "\t\t\tt.recheckAgain = false\n")]),
    ("a recheck starts while the write loops end", CG,
     [("\tif ctx == nil || ctx.Err() != nil {\n", "\tif ctx == nil {\n")]),
    ("the write loops don't wait for a recheck", CG,
     [("\ts.writeWG.Add(1)\n", ""), ("\t\tdefer s.writeWG.Done()\n", "")]),
    ("a ghost's record keeps its line", IX,
     [("\tif t.crdAbsent() {\n", "\tif false && t.crdAbsent() {\n")]),
    # The informers' failures.
    ("a failure is logged at every retry", WF,
     [("\treturn first\n}", "\treturn first || true\n}")]),
    ("a failure is never logged", WF,
     [("\treturn first\n}", "\treturn first && false\n}")]),
    ("a recovery forgets nothing", WF,
     [("\t\tif k.what != what {\n", "\t\tif true || k.what != what {\n")]),
    ("one informer's recovery is every one's", WF,
     [("\t\tif k.what != what {\n", "\t\tif false && k.what != what {\n")]),
    ("every cause is one", WF,
     [("\tcase !s.failures.failed(what, shortError(err), ID{}):\n", "\tcase !s.failures.failed(what, \"\", ID{}):\n")]),
    ("what fails as the process stops is logged", WF,
     [("\tcase ctx.Err() != nil:\n", "\tcase false && ctx.Err() != nil:\n")]),
    ("a watch's end is logged", WF,
     [("\tcase routineWatchEnd(err):\n", "\tcase false && routineWatchEnd(err):\n")]),
    ("an EOF in a broken connection is routine", WF,
     [("import (\n\t\"context\"\n", "import (\n\t\"context\"\n\t\"errors\"\n"), ("err == io.EOF ||", "errors.Is(err, io.EOF) ||")]),
    ("the start's 403 is logged too", WF,
     [("\tcase !synced && apierrors.IsForbidden(err):\n", "\tcase false && !synced && apierrors.IsForbidden(err):\n")]),
    ("a tracker's failure goes to klog", WF,
     [("\tif t == nil {\n", "\tif true || t == nil {\n")]),
    ("a per-name 403 is one for the kind", WF,
     [("\tif apierrors.IsForbidden(err) {\n\t\tn.forbidden", "\tif false && apierrors.IsForbidden(err) {\n\t\tn.forbidden")]),
    ("a per-name watch that is back isn't warned of again", WF,
     [("\t\tdelete(n.forbidden, id)\n", "")]),
    ("a kind whose per-name watches are back isn't warned of again", WF,
     [("\t\tn.failures.recovered(id.GroupKind().String(), id)\n", "")]),
    ("a watch that stopped records a failure", WF,
     [("\tfirst := n.by[id] == w && n.failures.failed(", "\tfirst := n.failures.failed(")]),
    ("a watch that stopped records a refusal", PN,
     [("\tfirst := n.by[id] == w && !n.forbidden[id]\n", "\tfirst := !n.forbidden[id]\n")]),
    ("a watch that stopped keeps its kind failing", PN,
     [("\tn.failures.recovered(id.GroupKind().String(), id)\n", "")]),
    ("a name refused is warned of at every retry", PN,
     [("\t\tn.forbidden[id] = true\n", "")]),
    ("an informer's failure isn't logged", SH,
     [("\t\ts.watchFailed(ctx, r, i.t, i.what, i.inf.HasSynced(), err)\n", "\t\t_, _ = ctx, r\n")]),
    ("a late shared informer's failure isn't logged", SH,
     [("\t\ts.watchFailed(ctx, r, nil, p.what(), p.inf.HasSynced(), err)\n", "\t\t_, _ = ctx, r\n")]),
    ("a refusal outlives its watch", PN,
     [("\tdelete(n.forbidden, id)\n", "")]),
    ("a failed Track's holder informer's failure lasts", SH,
     [("\t\ts.failures.recovered(t.informer().what, ID{})\n", "")]),
    ("a failed Track's CRD informer's failure lasts", SH,
     [("\t\ts.failures.recovered(t.crdInformer().what, ID{})\n", "")]),
    ("a failed Track's protected informer's failure lasts", SH,
     [("\t\t\t\ts.failures.recovered(p.what(), ID{})\n", "")]),
    ("the index informer never recovers", IX,
     [("\tlw.field, lw.recovered = \"metadata.name=\"+t.idx.name, s.watchRecovered(t.indexInformer().what)\n", "\tlw.field = \"metadata.name=\" + t.idx.name\n")]),
    ("the index informer's failure goes to klog", IX,
     [("\treturn informer{t: t, inf: t.idx.inf,", "\treturn informer{inf: t.idx.inf,")]),
    ("the holder informer never recovers", TR,
     [("\tlw.recovered = s.watchRecovered(t.informer().what)\n", "")]),
    ("the CRD informer never recovers", TR,
     [("s.clock, s.watchRecovered(t.crdInformer().what)); err", "s.clock, nil); err")]),
    ("the Namespace informer never recovers", SH,
     [("\t\ts.nsLW.recovered = s.watchRecovered(namespaceInformer)\n", "")]),
    ("a protected kind's informer never recovers", SH,
     [("\t\tp.lw.recovered = s.watchRecovered(p.what())\n", "")]),
    ("a list alone is a recovery", LW,
     [("\tif cont == \"\" {\n\t\tl.completed()\n", "\tif cont == \"\" {\n\t\tl.completed()\n\t\tl.watches()\n")]),
    ("a watch that opens isn't a recovery", LW,
     [("\tif !initial {\n\t\tl.watches()\n", "\tif false && !initial {\n\t\tl.watches()\n")]),
    ("a stream that opens is a recovery", LW,
     [("\tif !initial {\n\t\tl.watches()\n", "\tif true || !initial {\n\t\tl.watches()\n")]),
    ("a stream's initial list isn't a recovery", LW,
     [("\t\t\t\tw.lw.watches()\n", "")]),
]

RELEASE = {C: "TestReleaseWithoutAHolderKind|TestReleaseListsWhatThereIs"}
NONE = {C: "TestNoHolders"}
GONE = {C: "TestAHoldOfAKindThatIsGoneIsReleased"}
GHOST = {C: "TestTheCRDsDeletionReleasesWhatAGhostHolds"}
R0 = {C: "TestR0WithoutTheHolderKind"}
ABSENT = {C: "TestCRDAbsent"}
QUEUES = {C: "TestTheCRDsDeletionQueuesTheKindsHolds"}
ONCE = {C: "TestAnInformersFailureIsLoggedOnce"}
WHAT = {C: "TestWhatAFailureLogs"}
NAMES = {C: "TestAPerNameWatchsFailureIsLoggedOnce"}
STOPPED = {C: "TestAStoppedPerNameWatchForgetsItsFailure"}
STREAM = {C: "TestAStreamsInitialListIsARecovery"}
VETO = {C: "TestTheDiscoverysVeto"}
LOGGED = {C: "TestAReleaseForWantOfTheKindIsLoggedOnce"}
RECHECK = {C: "TestOneRecheckPerTracker"}
WAIT = {C: "TestTheWriteLoopsWaitForTheRecheck|TestTheRecheckAfterACRDsDeletionEndsWithTheWriteLoops"}
NEXT = {C: "TestAFailedTracksFailuresAreLoggedByTheNext"}
INDEX = {C: "TestTheIndexInformersFailureIsLoggedOnce"}
KILLERS = {
    "R2 never asks whether the kind is gone": RELEASE,
    "R2 takes any failed list for an empty one": RELEASE,
    "R2 asks though its list succeeded": RELEASE,
    "the tracker's protocol asks nobody": GONE,
    "any failed list is worth the GET": NONE,
    "a list's 404 isn't worth the GET": NONE,
    "a mapper's no match isn't worth the GET": NONE,
    "the list's 404 is the evidence": NONE,
    "a GET that fails is the evidence": NONE,
    "a GET that fails says nothing of its own": NONE,
    "any read that fails advises a grant": NONE,
    "a refused read advises no grant": NONE,
    "the GET may be answered by the watch cache": {C: "TestTheCRDsGetIsAConsistentRead"},
    "discovery isn't asked": VETO,
    "its own resource asks for a restart": VETO,
    "its own subresource asks for a restart": VETO,
    "another group's kind is a veto": VETO,
    "its own group's kind, named, is no veto": VETO,
    "discovery is asked before the GET": VETO,
    "a discovery that can't be read is no veto": VETO,
    "another resource of the kind is no veto": {C: "TestTheDiscoverysVeto|TestAnotherPluralsHoldsArentStripped"},
    "without a client a kind may be gone": VETO,
    "a discovery that hangs holds the worker": VETO,
    "a group that can't be read isn't served": VETO,
    "a group that isn't served can't be read": VETO,
    "any document is the group's": VETO,
    "a version that can't be read is skipped": VETO,
    "only the first version is read": VETO,
    "any document is the version's": VETO,
    "the kind's case matters": VETO,
    "provider-runtime sets no Discovery": {R: "TestTheDefaultSharedHasEveryClient"},
    "a Track without Discovery says nothing": {C: "TestTrackWarnsWithoutDiscovery"},
    "every Track warns of Discovery": {C: "TestTrackWarnsWithoutDiscovery"},
    "a 404 that isn't the API server's is the evidence": NONE,
    "a 404 that names nothing is the evidence": NONE,
    "a 404 that names another CRD is the evidence": NONE,
    "a 404 of another resource is the evidence": NONE,
    "a kind that moved to another plural is gone": NONE,
    "a mapper that can't tell is taken for no match": NONE,
    "what is released for want of the kind isn't logged": LOGGED,
    "any release is logged as for want of the kind": RELEASE,
    "every release for want of the kind is logged": LOGGED,
    "a kind that goes again isn't logged again": LOGGED,
    "R0 trusts the cache of a kind that is gone": R0,
    "R0 never trusts the cache": R0,
    "the tracker's protocol doesn't know of its CRD": GHOST,
    "an unsynced informer's CRD is absent": ABSENT,
    "a CRD is never absent": ABSENT,
    "the CRD's deletion queues nothing": QUEUES,
    "the CRD's deletion queues nothing of the per-name caches": QUEUES,
    "the CRD's deletion rechecks nothing outside the caches": GHOST,
    "the CRD's deletion doesn't recheck the index": {C: "TestTheCRDsDeletionRechecksTheIndex"},
    "each deletion starts a recheck": RECHECK,
    "a recheck asked for meanwhile is lost": RECHECK,
    "a recheck is a tracker's last": RECHECK,
    "a recheck starts while the write loops end": WAIT,
    "the write loops don't wait for a recheck": WAIT,
    "a ghost's record keeps its line": {C: "TestAGhostsRecordKeepsNoLine"},
    "a failure is logged at every retry": WHAT,
    "a failure is never logged": WHAT,
    "a recovery forgets nothing": WHAT,
    "one informer's recovery is every one's": WHAT,
    "every cause is one": WHAT,
    "what fails as the process stops is logged": WHAT,
    "a watch's end is logged": WHAT,
    "an EOF in a broken connection is routine": WHAT,
    "the start's 403 is logged too": WHAT,
    "a tracker's failure goes to klog": ONCE,
    "a per-name 403 is one for the kind": NAMES,
    "a per-name watch that is back isn't warned of again": NAMES,
    "a kind whose per-name watches are back isn't warned of again": NAMES,
    "a watch that stopped records a failure": STOPPED,
    "a watch that stopped records a refusal": STOPPED,
    "a watch that stopped keeps its kind failing": STOPPED,
    "a name refused is warned of at every retry": NAMES,
    "an informer's failure isn't logged": ONCE,
    "a late shared informer's failure isn't logged": ONCE,
    "a refusal outlives its watch": STOPPED,
    "a failed Track's holder informer's failure lasts": NEXT,
    "a failed Track's CRD informer's failure lasts": NEXT,
    "a failed Track's protected informer's failure lasts": NEXT,
    "the index informer never recovers": INDEX,
    "the index informer's failure goes to klog": INDEX,
    "the holder informer never recovers": ONCE,
    "the CRD informer never recovers": ONCE,
    "the Namespace informer never recovers": ONCE,
    "a protected kind's informer never recovers": ONCE,
    "a list alone is a recovery": {C: "TestAListAloneIsNoRecovery"},
    "a watch that opens isn't a recovery": STREAM,
    "a stream that opens is a recovery": STREAM,
    "a stream's initial list isn't a recovery": STREAM,
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
