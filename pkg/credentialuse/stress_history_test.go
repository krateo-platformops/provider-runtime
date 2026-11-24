package credentialuse_test

// What a stress run did, and what it must not have done (§14, "What the
// stress test shows").
//
// The audits here work on recorded data alone, so that they are tested on
// made-up histories without an API server (stress_audit_test.go). They rest
// on three things the recorder brings (stress_record_test.go):
//
//   - every version of every Widget, Target, Secret and Namespace, from the
//     test's own watches. The order is etcd's: envtest has one, so
//     resourceVersions order writes of different kinds;
//   - every call a process made through its metadata client, with its answer,
//     the goroutine it ran on, and the pass it belongs to;
//   - every DELETE and create the test made, with its answer.
//
// No rule asks the package under test what it knew: records, epochs and users
// are read from the versions, by the code here.

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// A stObj names a Secret, a Widget, a Target or a Namespace.
type stObj struct{ kind, ns, name string }

func (o stObj) String() string {
	if o.ns == "" {
		return o.kind + "/" + o.name
	}
	return o.kind + "/" + o.ns + "/" + o.name
}

func nsObj(name string) stObj { return stObj{"Namespace", "", name} }

// A stVersion is one version of one object, from the test's watch.
type stVersion struct {
	stObj
	// gone: the DELETED event. The rest is the object's last state, and rv
	// the revision of its removal.
	gone      bool
	rv        uint64
	uid       string
	at        time.Time // when the event arrived: an upper bound of when it happened
	deleting  bool
	deletedAt time.Time // the deletionTimestamp, whole seconds, rounded down
	fins      []string
	epochs    map[string]string // the epoch of each credential-use finalizer
	rec       []stObj           // a holder's record
	recBad    bool              // it can't be parsed
	spec      string            // what a holder or a Target names
	consent   string            // its allow-holders-from
}

// names: the record names o, or can't be parsed (§6.1).
func (v stVersion) names(o stObj) bool { return v.recBad || slices.Contains(v.rec, o) }
func (v stVersion) owner() bool        { return slices.Contains(v.fins, ownerFinalizer) }

// user and deletingUser are §6.1's.
func (v stVersion) user(o stObj) bool         { return v.names(o) && (!v.deleting || v.owner()) }
func (v stVersion) deletingUser(o stObj) bool { return v.deleting && v.user(o) }

func (v stVersion) String() string {
	s := fmt.Sprintf("%s rv=%d uid=%s", v.stObj, v.rv, short(v.uid))
	if v.gone {
		s += " GONE; last state:"
	}
	if v.deleting {
		s += " deleting"
	}
	if len(v.fins) > 0 {
		s += fmt.Sprintf(" fins=%v", v.fins)
	}
	for _, f := range slices.Sorted(maps.Keys(v.epochs)) {
		s += fmt.Sprintf(" epoch=%s", short(v.epochs[f]))
	}
	if v.spec != "" {
		s += " names=" + v.spec
	}
	if v.recBad {
		s += " record=UNPARSEABLE"
	} else if len(v.rec) > 0 {
		s += fmt.Sprintf(" record=%v", v.rec)
	}
	if v.consent != "" {
		s += " allow-holders-from=" + v.consent
	}
	return s
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// holderOfFinalizer is the holder kind and namespace of a credential-use
// finalizer of one of the stress's holder kinds.
func holderOfFinalizer(f string) (kind, ns string, ok bool) {
	prefix, ns, ok := strings.Cut(f, "/")
	if !ok {
		return "", "", false
	}
	for _, k := range stressKinds {
		if prefix == "credential-use."+strings.ToLower(k.gvk.Kind)+"."+k.gvk.Group {
			return k.gvk.Kind, ns, true
		}
	}
	return "", "", false
}

func credentialUse(fins []string) []string {
	var out []string
	for _, f := range fins {
		if _, _, ok := holderOfFinalizer(f); ok {
			out = append(out, f)
		}
	}
	return out
}

// parseRecord reads a record (§4) on its own, apart from the package's
// reader: exactly a list of group, kind, namespace and name.
func parseRecord(annotations map[string]string) (rec []stObj, bad bool) {
	v, ok := annotations[cu.AnnotationRefs]
	if !ok {
		return nil, false
	}
	var es []struct {
		Group, Kind, Namespace, Name *string
	}
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&es); err != nil || es == nil {
		return nil, true
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, true
	}
	for _, e := range es {
		if e.Group == nil || e.Kind == nil || e.Namespace == nil || e.Name == nil || *e.Kind == "" || *e.Name == "" {
			return nil, true
		}
		kind := *e.Kind
		if g := *e.Group; kind == "Secret" && g != "" || kind != "Secret" && g != cutest.Group {
			kind += "." + g
		}
		rec = append(rec, stObj{kind, *e.Namespace, *e.Name})
	}
	return rec, false
}

// refsOf are the refs a holder of mode, whose spec is spec, names directly:
// what its RefsFunc returns, by the test's own reading.
func refsOf(mode stressMode, ns, spec string) []stObj {
	if spec == "" {
		return nil
	}
	switch mode {
	case stressChained:
		return []stObj{{"Target", ns, spec}}
	case stressCross:
		if other, name, ok := strings.Cut(spec, "/"); ok {
			return []stObj{{"Secret", other, name}}
		}
	}
	return []stObj{{"Secret", ns, spec}}
}

// consented: the union of the two lists, "*" on the object only (§11.3).
func consented(object, namespace, holderNS string) bool {
	for _, l := range []struct {
		v    string
		wild bool
	}{{object, true}, {namespace, false}} {
		for _, s := range strings.Split(l.v, ",") {
			if s = strings.TrimSpace(s); s == holderNS || l.wild && s == "*" {
				return true
			}
		}
	}
	return false
}

// A stCall is one call of a process's metadata client.
type stCall struct {
	proc int
	g    string // the goroutine it ran on: a release batch's calls run on one
	pass int64  // the pass it belongs to, or 0: a write loop's, or an informer's
	// op: get, list, a1, a2, r3, or another patch's type.
	op         string
	obj        stObj // a list's kind and namespace
	fin        string
	start, end time.Time
	err        string // the answer's error, or ""
	code       int    // its HTTP code, or 0
	notFound   bool
	// refused: an A2 refused since the object is being deleted and lacks the
	// finalizer (§6.2, class (a)).
	refused bool
	unknown bool // the call failed without an answer, or with a 5xx: it may have landed
	// items are the holders a list answered; continued and more: it is a
	// page after the first, and isn't the last.
	items           []stItem
	continued, more bool
	rv              uint64 // the answer's; a list's revision
	uid             string
	deleting        bool
	fins            []string
	epochs          map[string]string
	consent         string
	testedUID       string // r3: the uid and the epoch it tests
	tested          string
	wrote           string // a2: the epoch it writes
}

// A stItem is one holder of a list's answer.
type stItem struct {
	name, uid       string
	rv              uint64
	deleting, owner bool
	rec             []stObj
	recBad          bool
}

func (i stItem) String() string {
	return fmt.Sprintf("%s(rv=%d uid=%s deleting=%v owner=%v)", i.name, i.rv, short(i.uid), i.deleting, i.owner)
}

func (c stCall) ok() bool { return c.err == "" }

// removal: an R3 that took the last finalizer off an object being deleted.
// The API server stores no such version: it removes the object. The answer
// is the version the patch's tests ran on, without the finalizer, so its rv
// is that version's.
func (c stCall) removal() bool { return c.op == "r3" && c.ok() && c.deleting && len(c.fins) == 0 }

func (c stCall) String() string {
	s := fmt.Sprintf("p%d g%s ", c.proc, c.g)
	if c.pass != 0 {
		s += fmt.Sprintf("pass %d ", c.pass)
	}
	s += c.op + " " + c.obj.String()
	if c.wrote != "" {
		s += " epoch " + short(c.wrote)
	}
	if c.op == "r3" {
		s += fmt.Sprintf(" of %s, tests uid %s epoch %s", c.fin, short(c.testedUID), short(c.tested))
	}
	switch {
	case c.unknown:
		return s + " -> UNKNOWN OUTCOME: " + c.err
	case c.err != "":
		return s + " -> " + c.err
	case c.op == "list":
		return s + fmt.Sprintf(" -> revision %d, %d holders", c.rv, len(c.items))
	}
	s += fmt.Sprintf(" -> rv=%d uid=%s", c.rv, short(c.uid))
	if c.deleting {
		s += " deleting"
	}
	for _, f := range slices.Sorted(maps.Keys(c.epochs)) {
		s += " epoch=" + short(c.epochs[f])
	}
	return s + fmt.Sprintf(" fins=%d", len(c.fins))
}

// A stPass is one pass of one holder.
type stPass struct {
	id     int64
	proc   int
	holder stObj
	began  time.Time
	// rv, uid, deleting and refs are the holder's as the pass read it, and
	// what it named directly.
	read     bool
	rv       uint64
	uid      string
	deleting bool
	refs     []stObj
	done     time.Time // zero: it never returned
	held     []stObj
	unheld   map[stObj]string // and why
	mustNot  bool
	degraded bool
	conflict bool   // its A1 conflicted: the acquire stopped there
	acqErr   string // the acquire's own error
	err      string // the pass's
	cut      bool   // its controller was stopping: nothing is learnt from it
}

// stopped: the acquire didn't go through all of the holder's refs.
func (p *stPass) stopped() bool { return p.mustNot || p.conflict || p.acqErr != "" }

func (p stPass) String() string {
	s := fmt.Sprintf("p%d pass %d of %s rv=%d deleting=%v", p.proc, p.id, p.holder, p.rv, p.deleting)
	if p.done.IsZero() {
		return s + ": never returned"
	}
	s += fmt.Sprintf(": held %v unheld %v", p.held, p.unheld)
	if p.mustNot {
		s += " MustNotConnect"
	}
	if p.degraded {
		s += " Degraded"
	}
	if p.acqErr != "" {
		s += " (" + p.acqErr + ")"
	}
	if p.err != "" {
		s += " error: " + p.err
	}
	return s
}

// A stRead is a Connect's read that failed.
type stRead struct {
	pass       int64
	holder     stObj
	obj        stObj
	start, end time.Time
	err        string
	notFound   bool
}

// How the API server answered a DELETE of the test's.
const (
	// answerStatus: a Status of success, whose details name a uid: the
	// incarnation the DELETE removed. (An object being deleted already, with
	// finalizers, is answered with answerMarked.)
	answerStatus = "status"
	// answerMarked: the object, being deleted: it waits for its finalizers.
	answerMarked   = "marked"
	answerNotFound = "not found"
	answerCreated  = "created"
	answerRefused  = "refused" // an error from the API server, not a 5xx
	answerUnknown  = "unknown"
)

// A stWrite is a DELETE or a create the test made.
type stWrite struct {
	op         string // "DELETE" or "create"
	obj        stObj
	start, end time.Time
	answer     string
	uid        string
	rv         uint64
	err        string
}

func (w stWrite) String() string {
	return fmt.Sprintf("the test's %s of %s -> %s uid=%s %s", w.op, w.obj, w.answer, short(w.uid), w.err)
}

// The classes of what the audits find.
const (
	// findIncomplete: the recording can't be trusted, so nor can a green. A
	// DELETE or a create of the test's that fails with a 5xx, or without an
	// answer, is one, by design: what it did isn't known. A call of a process
	// that its stop cuts short isn't: by then everything is released.
	findIncomplete = "INCOMPLETE"
	// findStaleList: a release's list answered something else than the watch
	// has at its revision, or an older state than the watch had already
	// delivered: the API server broke the premise.
	findStaleList = "STALE-LIST"
	// findReleasedOverUser: a release of an object with a user, which §5's
	// table keeps.
	findReleasedOverUser = "RELEASED-OVER-A-USER"
	// findReleasedEarly: the same, inside the grace.
	findReleasedEarly = "RELEASED-INSIDE-THE-GRACE"
	// findReleasedUnread: a release of a state its batch hadn't read: another
	// incarnation, or one an A2 had written since.
	findReleasedUnread = "RELEASED-WHAT-IT-HADNT-READ"
	// findFinalizerLost: a finalizer went, and no release took it.
	findFinalizerLost = "FINALIZER-LOST"
	// findUnexplainedRemoval: an object went, and neither a release of this
	// incarnation nor a DELETE of the test's that named it took it.
	findUnexplainedRemoval = "UNEXPLAINED-REMOVAL"
	// findRecreate: the API server's second release of one incarnation took
	// the next one (§14, H1's other half). It fails the run, under its own
	// name: it isn't the package's.
	findRecreate = "H1-RECREATE"
	// findLostRead: a deletion pass failed to read what it had held.
	findLostRead = "LOST-READ"
	// findHeldWithoutA2: a pass reported a ref held, and neither an A2 of
	// its own nor the skip rule stands behind it.
	findHeldWithoutA2 = "HELD-WITHOUT-A2"
	// findHeldUnannounced: an A2 of a pass came before the record named the
	// object.
	findHeldUnannounced = "HELD-BEFORE-THE-RECORD"
	// findUnreported: a pass that may connect says nothing, or something no
	// answer supports, of a ref its holder names.
	findUnreported = "REF-UNACCOUNTED"
	// findHeldUnconsented: a pass held an object in another namespace, and
	// its own live reads of its consent don't allow it (§11.3); or it skipped,
	// and the consent had been gone for long.
	findHeldUnconsented = "HELD-WITHOUT-CONSENT"
	// findAcquireFailed: a pass's acquire stopped, or left the holder
	// Degraded, and no answer of the API server's explains it.
	findAcquireFailed = "ACQUIRE-FAILED"
	// findRecordDrop: a record write dropped a ref the holder still names.
	findRecordDrop = "RECORD-DROP"
	// findRecordDiffers: a pass that went through every ref left a record
	// that isn't exactly what the holder names, less what isn't consented.
	findRecordDiffers = "RECORD-DIFFERS"
	// findEstablished: a holder got its owner finalizer, and a ref of its
	// record was neither held nor found absent by its pass (§6.7).
	findEstablished = "ESTABLISHED-UNHELD"
	// findNotHeld and findNotReleased are the canaries': what must happen in
	// every run didn't (stress_envtest_test.go).
	findNotHeld     = "CANARY-NOT-HELD"
	findNotReleased = "CANARY-NOT-RELEASED"
)

// A stFinding is something a run must not have done.
type stFinding struct {
	class string
	text  string
	// What to print: everything that happened to obj and the holders.
	obj      stObj
	holders  []stObj
	from, to time.Time
	zero     time.Time
}

// A stReport is what the audits found, and how much they looked at.
type stReport struct {
	findings []stFinding
	counts   map[string]int
}

// The counts. A run that checked next to nothing proves nothing.
const (
	countReleases       = "releases judged"
	countReleasesLive   = "releases judged, of objects not being deleted"
	countReleasesUsers  = "releases judged, over live users after the grace"
	countDeletionPasses = "deletion passes"
	countHeldByA2       = "deletion passes' holds backed by their own A2"
	countSkips          = "skips checked"
	countSkipsUnchecked = "skips unchecked"
	countRemovals       = "removals explained"
	countH3             = "H3: held objects a DELETE removed"
	countH3Reads        = "H3: failed reads exempt"
	countLostA2         = "H1: lost A2s"
	countLiveReads      = "failed reads of passes that began live"
	countHolderGone     = "failed reads after the holder was gone"
	countDrops          = "chained: record drops behind a Target"
	countUnknown        = "finalizers gone with a release of unknown outcome"
	countCrossHolds     = "cross-namespace A2s after a consenting read"
	countWithdrawn      = "cross-namespace holds released, and their holder still names them"
	countRecords        = "records compared"
	countEstablished    = "establishments checked"
	countDegraded       = "deletion passes Degraded, on an object being deleted without their finalizer"
	countAPIErrors      = "acquires stopped by a 5xx or a lost answer"
)

// A stFloor is the least a run must have of a count. The runs on file have
// three times as much or more, or the canaries make it happen once.
type stFloor struct {
	count string
	min   int
}

// floors are a run's: a bug that means "never" shows in nothing else. outside:
// every hold is outside the caches, where no pass skips.
func (h *stHistory) floors(outside bool) []stFloor {
	fs := []stFloor{{countReleases, 10}, {countReleasesLive, 1}, {countReleasesUsers, 1}, {countHeldByA2, 10}, {countRemovals, 10}, {countEstablished, 5}}
	if !outside {
		fs = append(fs, stFloor{countSkips, 10})
	}
	if h.mode != stressChained {
		fs = append(fs, stFloor{countRecords, 50})
	}
	if h.mode == stressCross {
		fs = append(fs, stFloor{countCrossHolds, 5}, stFloor{countWithdrawn, 1})
	}
	return fs
}

// cacheWindow is how far behind the watch a process's caches may be, for
// the audits that a lag must not fail.
const cacheWindow = 5 * time.Second

func (r *stReport) String() string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(r.counts)) {
		parts = append(parts, fmt.Sprintf("%s %d", k, r.counts[k]))
	}
	return strings.Join(parts, "; ")
}

func (r *stReport) find(f stFinding) { r.findings = append(r.findings, f) }

// A stHistory is a run's record.
type stHistory struct {
	mode    stressMode
	grace   time.Duration
	objs    map[stObj][]stVersion // by rv
	holders map[string][]stObj    // by kind and namespace: see holderKey
	calls   []stCall              // by end
	byObj   map[stObj][]int       // the calls on each object
	byPass  map[int64][]int       // the calls of each pass
	passes  map[int64]*stPass
	reads   []stRead
	writes  map[stObj][]stWrite
	gaps    []string
}

func holderKey(kind, ns string) string { return kind + "/" + ns }

func newStHistory(mode stressMode, grace time.Duration, vs []stVersion, calls []stCall, passes []*stPass, reads []stRead, writes []stWrite, gaps []string) *stHistory {
	h := &stHistory{mode: mode, grace: grace, objs: map[stObj][]stVersion{}, holders: map[string][]stObj{}, byObj: map[stObj][]int{},
		byPass: map[int64][]int{}, passes: map[int64]*stPass{}, reads: reads, writes: map[stObj][]stWrite{}, gaps: gaps}
	for _, v := range vs {
		if _, ok := h.objs[v.stObj]; !ok && h.holderKind(v.kind) {
			k := holderKey(v.kind, v.ns)
			h.holders[k] = append(h.holders[k], v.stObj)
		}
		h.objs[v.stObj] = append(h.objs[v.stObj], v)
	}
	for _, o := range h.objs {
		slices.SortStableFunc(o, func(a, b stVersion) int { return cmp.Compare(a.rv, b.rv) })
	}
	h.calls = slices.Clone(calls)
	slices.SortStableFunc(h.calls, func(a, b stCall) int { return a.end.Compare(b.end) })
	for i, c := range h.calls {
		h.byObj[c.obj] = append(h.byObj[c.obj], i)
		if c.pass != 0 {
			h.byPass[c.pass] = append(h.byPass[c.pass], i)
		}
	}
	for _, p := range passes {
		h.passes[p.id] = p
	}
	for _, w := range writes {
		h.writes[w.obj] = append(h.writes[w.obj], w)
	}
	return h
}

// holderKind: objects of kind are holders in this run.
func (h *stHistory) holderKind(kind string) bool {
	return kind == "Widget" || kind == "Target" && h.mode == stressPlain
}

// protectedKind: objects of kind are protected in this run.
func (h *stHistory) protectedKind(kind string) bool {
	return kind == "Secret" || kind == "Target" && h.mode == stressChained
}

// state is o as it was at revision rv, if it existed then.
func (h *stHistory) state(o stObj, rv uint64) (stVersion, bool) {
	vs := h.objs[o]
	i, found := slices.BinarySearchFunc(vs, rv, func(v stVersion, rv uint64) int { return cmp.Compare(v.rv, rv) })
	if !found {
		i--
	}
	if i < 0 {
		return stVersion{}, false
	}
	return vs[i], !vs[i].gone
}

// version is the version of o at exactly rv.
func (h *stHistory) version(o stObj, rv uint64) (stVersion, bool) {
	v, ok := h.state(o, rv)
	return v, ok && v.rv == rv
}

// removal is the end of incarnation uid of o.
func (h *stHistory) removal(o stObj, uid string) (stVersion, bool) {
	for _, v := range h.objs[o] {
		if v.gone && v.uid == uid {
			return v, true
		}
	}
	return stVersion{}, false
}

// run is the first version of the unbroken run of holder versions, up to and
// including v, whose record names p.
func (h *stHistory) run(v stVersion, p stObj) (stVersion, bool) {
	first, ok := stVersion{}, false
	vs := h.objs[v.stObj]
	for i := len(vs) - 1; i >= 0; i-- {
		w := vs[i]
		if w.rv > v.rv {
			continue
		}
		if w.uid != v.uid || w.gone || w.recBad || !slices.Contains(w.rec, p) {
			break
		}
		first, ok = w, true
	}
	return first, ok
}

// callsOn are the calls on o that f accepts.
func (h *stHistory) callsOn(o stObj, f func(c stCall) bool) []stCall {
	var out []stCall
	for _, i := range h.byObj[o] {
		if f(h.calls[i]) {
			out = append(out, h.calls[i])
		}
	}
	return out
}

// callsOf are p's calls that f accepts, in order.
func (h *stHistory) callsOf(p *stPass, f func(c stCall) bool) []stCall {
	var out []stCall
	for _, i := range h.byPass[p.id] {
		if f(h.calls[i]) {
			out = append(out, h.calls[i])
		}
	}
	return out
}

// audit runs every audit.
func (h *stHistory) audit() *stReport {
	r := &stReport{counts: map[string]int{}}
	h.auditCompleteness(r)
	h.auditReleases(r)
	h.auditFinalizers(r)
	h.auditRemovals(r)
	h.auditPasses(r)
	h.auditRecords(r)
	h.auditEstablished(r)
	h.auditReads(r)
	slices.SortStableFunc(r.findings, func(a, b stFinding) int {
		return cmp.Or(cmp.Compare(a.class, b.class), a.zero.Compare(b.zero), cmp.Compare(a.text, b.text))
	})
	return r
}

func gkOf(kind string) schema.GroupKind { return schema.GroupKind{Group: cutest.Group, Kind: kind} }

// auditCompleteness: the watches missed nothing, and every write the
// processes and the test know succeeded is among their versions.
func (h *stHistory) auditCompleteness(r *stReport) {
	for _, g := range h.gaps {
		r.find(stFinding{class: findIncomplete, text: g})
	}
	lacks := func(o stObj, at time.Time, text string) {
		r.find(stFinding{class: findIncomplete, text: text, obj: o, from: at.Add(-time.Second), to: at.Add(time.Second), zero: at})
	}
	written := func(o stObj, rv uint64, uid string) (stVersion, bool) {
		v, ok := h.version(o, rv)
		return v, ok && v.uid == uid
	}
	for _, c := range h.calls {
		if !c.ok() || c.op != "a1" && c.op != "a2" && c.op != "r3" {
			continue
		}
		if _, ok := written(c.obj, c.rv, c.uid); !ok {
			lacks(c.obj, c.end, "the watch has no version of "+c.String())
		}
	}
	for _, ws := range h.writes {
		for _, w := range ws {
			ok := true
			switch w.answer {
			case answerCreated:
				_, ok = written(w.obj, w.rv, w.uid)
			case answerMarked:
				var v stVersion
				v, ok = written(w.obj, w.rv, w.uid)
				ok = ok && v.deleting
			case answerStatus:
				_, ok = h.removal(w.obj, w.uid)
			case answerUnknown:
				lacks(w.obj, w.end, "the test doesn't know what came of "+w.String())
			}
			if !ok {
				lacks(w.obj, w.end, "the watch has no version of "+w.String())
			}
		}
	}
}

// A stBatch is what a release patch was decided on.
type stBatch struct {
	list, r1, ns          stCall
	hasList, hasR1, hasNS bool
}

// batchOf finds, on the goroutine of the R3 at index i, R1: the object's last
// GET that answered, before the patch. And between the two, R2: a whole list
// of the holders, and the Namespace's GET. A batch reads all its objects
// before its one list, so the three aren't next to each other. A list from
// before R1 is an older round's, or an older batch's, and doesn't count.
func (h *stHistory) batchOf(i int) stBatch {
	var b stBatch
	c := h.calls[i]
	kind, n, _ := holderOfFinalizer(c.fin)
	paging := false // b.list's earlier pages are still to come
	for j := i - 1; j >= 0 && !b.hasR1; j-- {
		d := h.calls[j]
		if d.proc != c.proc || d.g != c.g || d.pass != 0 || d.end.After(c.start) || !d.ok() && !d.notFound {
			continue
		}
		switch {
		case d.op == "get" && d.obj == c.obj && d.ok():
			b.r1, b.hasR1 = d, true
		case d.op == "get" && d.obj == nsObj(c.obj.ns) && !b.hasNS:
			b.ns, b.hasNS = d, true
		case d.op != "list" || d.obj != (stObj{kind, n, ""}) || !d.ok():
		case paging && d.more && d.rv == b.list.rv:
			b.list.items = slices.Concat(d.items, b.list.items)
			b.list.start, paging = d.start, d.continued
		case !b.hasList && !paging && !d.more:
			b.list, b.hasList, paging = d, true, d.continued
		}
	}
	b.hasList = b.hasList && !paging
	return b
}

// deletionBegan is a lower bound of when incarnation uid of o was marked as
// being deleted: when the first DELETE of the test's that named it began, or
// the deletionTimestamp, which is rounded down.
func (h *stHistory) deletionBegan(o stObj, uid string) time.Time {
	var t time.Time
	for _, w := range h.writes[o] {
		if w.op == "DELETE" && w.uid == uid && (t.IsZero() || w.start.Before(t)) {
			t = w.start
		}
	}
	for _, v := range h.objs[o] {
		if v.uid == uid && v.deleting {
			if v.deletedAt.After(t) {
				t = v.deletedAt
			}
			break
		}
	}
	return t
}

// auditReleases judges every successful R3 at the moment it lands, whatever
// becomes of the object (§5, §6.2, §6.4).
//
// It must apply to exactly the state its batch read: the same incarnation,
// with no A2 since. And §5's table must let it go, for the users there were:
//
//   - at the revision of R2's list, by the watch. The list is a consistent
//     read and its answer carries its revision, so nothing is compared by
//     time;
//   - or at the release itself, among those whose A2 on this incarnation had
//     landed: a hold it takes from under them. §6.4 says there are none that
//     the list didn't have, so these catch a batch that lists too early, or
//     lands late. A holder that had announced, and whose A2 hadn't landed, is
//     §6.4's third case, and no user here.
//
// What §5 allows is judged on the same reads: the object as R1's answer has
// it, the Namespace as R2's answer has it, and the grace by when the test
// began the DELETE. A deletion of the namespace or of a holder after those
// reads is §6.4's accepted window.
func (h *stHistory) auditReleases(r *stReport) {
	for i, c := range h.calls {
		if c.op != "r3" || !c.ok() {
			continue
		}
		// The version the patch applied to, and the revision it landed at.
		var on stVersion
		var landed uint64
		var ok bool
		if c.removal() {
			on, ok = h.version(c.obj, c.rv)
			if d, gone := h.removal(c.obj, c.uid); gone {
				landed = d.rv
			}
		} else {
			on, ok = h.state(c.obj, c.rv-1)
			landed = c.rv
		}
		if !ok || landed == 0 {
			r.find(stFinding{class: findIncomplete, text: fmt.Sprintf("%s: the watch doesn't have the version it applied to (%v), or its landing (%d)", c, ok, landed),
				obj: c.obj, from: c.start.Add(-5 * time.Second), to: c.end.Add(2 * time.Second), zero: c.end})
			continue
		}
		h.judge(r, i, on, landed)
	}
}

// judge judges the R3 at index i, which applied to version on, and landed at
// revision landed.
func (h *stHistory) judge(r *stReport, i int, on stVersion, landed uint64) {
	r.counts[countReleases]++
	c := h.calls[i]
	b := h.batchOf(i)
	from := c.start.Add(-5 * time.Second)
	find := func(class string, holders []stObj, f string, a ...any) {
		r.find(stFinding{class: class, text: fmt.Sprintf("%s; its batch: R1 %s; namespace %s; list %s: ", c, b.r1, b.ns, b.list) + fmt.Sprintf(f, a...),
			obj: c.obj, holders: holders, from: from, to: c.end.Add(2 * time.Second), zero: c.end})
	}
	kind, n, ok := holderOfFinalizer(c.fin)
	if !ok {
		find(findReleasedUnread, nil, "it removed a finalizer that isn't a holder kind's")
		return
	}
	if !b.hasR1 {
		find(findReleasedUnread, nil, "its goroutine never read the object")
		return
	}
	if b.r1.start.Add(-time.Second).Before(from) {
		from = b.r1.start.Add(-time.Second)
	}
	if !b.hasList {
		find(findReleasedUnread, nil, "no whole list of the holders lies between its last read of the object and it")
	}
	if c.obj.ns != n {
		h.countWithdrawn(r, c.obj, kind, n, landed)
	}

	// It applied to what R1 read.
	a2s := h.callsOn(c.obj, func(a stCall) bool { return a.op == "a2" && a.ok() && a.fin == c.fin && a.uid == on.uid })
	switch {
	case on.uid != b.r1.uid:
		find(findReleasedUnread, nil, "it read incarnation %s, and released %s", short(b.r1.uid), on)
	case on.epochs[c.fin] != b.r1.epochs[c.fin]:
		find(findReleasedUnread, nil, "it read epoch %s, and released %s", short(b.r1.epochs[c.fin]), on)
	default:
		for _, a := range a2s {
			if a.rv > b.r1.rv && a.rv <= on.rv {
				find(findReleasedUnread, nil, "an A2 had landed since it read the object: %s", a)
				break
			}
		}
	}

	// The users (§6.1).
	var users, deleting, seen []stObj
	for _, o := range h.holders[holderKey(kind, n)] {
		user, del := false, false
		if b.hasList {
			if why := h.listDiffers(b.list, o); why != "" {
				find(findStaleList, []stObj{o}, "%s", why)
			}
			if v, ok := h.state(o, b.list.rv); ok && v.user(c.obj) {
				user, del = true, v.deleting
			}
		}
		if v, ok := h.state(o, landed-1); ok && v.user(c.obj) {
			if first, ok := h.run(v, c.obj); ok {
				for _, a := range a2s {
					p := h.passes[a.pass]
					if a.rv < first.rv || a.rv > on.rv || p == nil || p.holder != o || p.uid != v.uid {
						continue
					}
					user, del = true, del || p.deleting
				}
			}
		}
		if user {
			users = append(users, o)
			if del {
				deleting = append(deleting, o)
			}
		}
	}
	if !b.r1.deleting {
		r.counts[countReleasesLive]++
	}
	if len(users) == 0 {
		return
	}
	// Those being deleted first: the ones it hurts.
	seen = slices.Concat(deleting, slices.DeleteFunc(slices.Clone(users), func(o stObj) bool { return slices.Contains(deleting, o) }))
	if len(seen) > 4 {
		seen = seen[:4]
	}
	const over = "users %v, of them being deleted %v: "
	began := h.deletionBegan(c.obj, on.uid)
	switch {
	case !b.r1.deleting:
		find(findReleasedOverUser, seen, over+"the object isn't being deleted (§5 row 1)", users, deleting)
	case !b.hasNS:
		find(findReleasedOverUser, seen, over+"its namespace wasn't read (§5)", users, deleting)
	case b.ns.notFound || b.ns.deleting:
		find(findReleasedOverUser, seen, over+"its namespace is being deleted (§5 row 3)", users, deleting)
	case len(deleting) > 0:
		find(findReleasedOverUser, seen, over+"a user is being deleted (§5 row 4)", users, deleting)
	case b.ns.start.Before(began.Add(h.grace)):
		find(findReleasedEarly, seen, over+"the Namespace was read %s after its deletion began, inside the grace of %s", users, deleting, b.ns.start.Sub(began), h.grace)
	default:
		r.counts[countReleasesUsers]++
	}
}

// listDiffers is how list l's answer differs, for holder o, from what the
// watch has at its revision, or had before it began; or "".
func (h *stHistory) listDiffers(l stCall, o stObj) string {
	for _, v := range h.objs[o] {
		if v.at.Before(l.start) && v.rv > l.rv {
			return fmt.Sprintf("the watch had delivered %s before the list, of revision %d, began", v, l.rv)
		}
	}
	v, exists := h.state(o, l.rv)
	i := slices.IndexFunc(l.items, func(i stItem) bool { return i.name == o.name })
	switch {
	case exists != (i >= 0):
		return fmt.Sprintf("at the list's revision %d the watch has %s there: %v; the list: %v", l.rv, o, exists, i >= 0)
	case exists && (l.items[i].rv != v.rv || l.items[i].uid != v.uid):
		return fmt.Sprintf("at the list's revision %d the watch has %s; the list: %s", l.rv, v, l.items[i])
	}
	return ""
}

// countWithdrawn counts the release, landed at revision landed, of another
// namespace's object p, if a holder still names it and its record, which did,
// doesn't any more: its consent was withdrawn.
func (h *stHistory) countWithdrawn(r *stReport, p stObj, kind, n string, landed uint64) {
	for _, o := range h.holders[holderKey(kind, n)] {
		v, ok := h.state(o, landed-1)
		if !ok || v.names(p) || !slices.Contains(refsOf(h.mode, n, v.spec), p) {
			continue
		}
		if slices.ContainsFunc(h.objs[o], func(w stVersion) bool { return w.uid == v.uid && w.rv < v.rv && w.names(p) }) {
			r.counts[countWithdrawn]++
			return
		}
	}
}

// auditFinalizers: a credential-use finalizer leaves an object that stays
// only by a release that answered with that very version. So neither an A2,
// nor anything else of the package, dropped one. A release of unknown outcome
// explains a loss too, if it was built for that very hold and began before
// it, and only one; and is then judged as any other.
func (h *stHistory) auditFinalizers(r *stReport) {
	explained := map[int]bool{}
	for o, vs := range h.objs {
		if !h.protectedKind(o.kind) {
			continue
		}
		for i := 1; i < len(vs); i++ {
			prev, cur := vs[i-1], vs[i]
			if prev.gone || cur.gone || prev.uid != cur.uid {
				continue
			}
			for _, f := range credentialUse(prev.fins) {
				if slices.Contains(cur.fins, f) {
					continue
				}
				if len(h.callsOn(o, func(c stCall) bool { return c.op == "r3" && c.ok() && c.fin == f && c.rv == cur.rv })) > 0 {
					continue
				}
				if i := slices.IndexFunc(h.byObj[o], func(i int) bool {
					c := h.calls[i]
					return c.op == "r3" && c.unknown && c.fin == f && c.testedUID == prev.uid && c.tested == prev.epochs[f] && c.start.Before(cur.at) && !explained[i]
				}); i >= 0 {
					explained[h.byObj[o][i]] = true
					r.counts[countUnknown]++
					h.judge(r, h.byObj[o][i], prev, cur.rv)
					continue
				}
				r.find(stFinding{class: findFinalizerLost, text: fmt.Sprintf("%s went between %s and %s, and no release did it", f, prev, cur),
					obj: o, from: cur.at.Add(-5 * time.Second), to: cur.at.Add(2 * time.Second), zero: cur.at})
			}
		}
	}
}

// heldSince is the first version of the unbroken run, up to removal d, of
// versions of that incarnation with a credential-use finalizer.
func (h *stHistory) heldSince(d stVersion) (stVersion, bool) {
	first, ok := stVersion{}, false
	vs := h.objs[d.stObj]
	for i := len(vs) - 1; i >= 0; i-- {
		v := vs[i]
		if v.gone || v.uid != d.uid || v.rv > d.rv {
			continue
		}
		if len(credentialUse(v.fins)) == 0 {
			break
		}
		first, ok = v, true
	}
	return first, ok
}

// removedByDELETE reports whether a DELETE of the test's removed incarnation
// d of an object, and if not, why not: the API server's answer to it names
// that uid, the incarnation was never marked as being deleted, and, if it
// went held, the DELETE began before that hold could be seen. (A DELETE that
// begins later reads the finalizer, and marks the object: H3 is a DELETE that
// read it without one.)
func (h *stHistory) removedByDELETE(d stVersion) (bool, string) {
	for _, v := range h.objs[d.stObj] {
		if v.uid == d.uid && v.deleting {
			return false, fmt.Sprintf("it had been marked as being deleted: %s", v)
		}
	}
	var seen time.Time
	if f, held := h.heldSince(d); held {
		seen = f.at
		for _, a := range h.callsOn(d.stObj, func(a stCall) bool { return a.op == "a2" && a.ok() && a.rv == f.rv }) {
			if a.end.Before(seen) {
				seen = a.end
			}
		}
	}
	why := "no DELETE of the test's was answered with its uid"
	for _, w := range h.writes[d.stObj] {
		if w.op != "DELETE" || w.answer != answerStatus || w.uid != d.uid {
			continue
		}
		if !seen.IsZero() && w.start.After(seen) {
			why = fmt.Sprintf("the DELETE that named it began %s after it was held", w.start.Sub(seen))
			continue
		}
		return true, ""
	}
	return false, why
}

// auditRemovals: every protected object that went was taken by a release of
// this incarnation, or by a DELETE of the test's that named it.
func (h *stHistory) auditRemovals(r *stReport) {
	for o, vs := range h.objs {
		if !h.protectedKind(o.kind) {
			continue
		}
		for _, d := range vs {
			if !d.gone {
				continue
			}
			find := func(f string, a ...any) {
				r.find(stFinding{class: findUnexplainedRemoval, text: d.String() + ": " + fmt.Sprintf(f, a...),
					obj: o, from: d.at.Add(-5 * time.Second), to: d.at.Add(2 * time.Second), zero: d.at})
			}
			held := credentialUse(d.fins)
			if !d.deleting {
				if ok, why := h.removedByDELETE(d); !ok {
					if c, recreate := h.recreate(d); recreate {
						r.find(stFinding{class: findRecreate, text: fmt.Sprintf("%s: it wasn't being deleted, and %s; a second release of the incarnation before it was in flight: %s", d, why, c),
							obj: o, from: d.at.Add(-2 * time.Second), to: d.at.Add(500 * time.Millisecond), zero: d.at})
						continue
					}
					find("it wasn't being deleted, and %s", why)
					continue
				}
				if len(held) > 0 {
					r.counts[countH3]++
				}
				r.counts[countRemovals]++
				continue
			}
			rs := h.callsOn(o, func(c stCall) bool { return c.removal() && c.uid == d.uid })
			if len(rs) == 0 || len(held) != 1 {
				find("it was being deleted, with %d holds, and %d releases that answered took the last", len(held), len(rs))
				continue
			}
			r.counts[countRemovals]++
			// The last version is the one a release's tests ran on, or an A2
			// landed after the tests and went with the object: H1.
			last := uint64(0)
			for _, v := range vs {
				if v.uid == d.uid && !v.gone {
					last = v.rv
				}
			}
			if !slices.ContainsFunc(rs, func(c stCall) bool { return c.rv == last }) {
				r.counts[countLostA2]++
			}
		}
	}
}

// recreate reports whether removal d has the whole shape of H1's recreate: a
// release that answered, of the incarnation just before this one, was in
// flight while d lived; that incarnation had gone already; and another
// release that answered had taken it.
func (h *stHistory) recreate(d stVersion) (stCall, bool) {
	began := h.createBegan(d.stObj, d.uid)
	if began.IsZero() {
		return stCall{}, false
	}
	last := stVersion{} // the removal before d
	for _, v := range h.objs[d.stObj] {
		if v.gone && v.rv < d.rv {
			last = v
		}
	}
	rs := h.callsOn(d.stObj, func(c stCall) bool { return c.removal() && c.uid == last.uid })
	for _, c := range rs {
		if len(rs) > 1 && c.start.Before(d.at) && c.end.After(began) {
			return c, true
		}
	}
	return stCall{}, false
}

// createBegan is a lower bound of when incarnation uid came to be: when the
// test began the create that was answered with it, or the zero time.
func (h *stHistory) createBegan(o stObj, uid string) time.Time {
	for _, w := range h.writes[o] {
		if w.op == "create" && w.answer == answerCreated && w.uid == uid {
			return w.start
		}
	}
	return time.Time{}
}

// auditPasses: what a pass reports of the refs its holder names is what its
// own calls support. A pass that began as a deletion pass holds each with an
// A2 of its own. One that began live may skip, against a write its process
// issued after the record named the ref (§6.3). A pass that began live and
// met its holder's deletion is checked as a live one: it isn't left out.
//
// Of a ref in another namespace, every A2 follows, in its pass, live reads of
// the Namespace and of the object whose answers consent (§11.3). A skip makes
// no read, rightly: a cached yes serves.
//
// A reason why a ref isn't held, and the acquire's own failure, must each
// have an answer of the API server's behind it. And a pass that went through
// every ref leaves a record that names exactly the refs of the version it
// read, less those without consent.
func (h *stHistory) auditPasses(r *stReport) {
	for _, id := range slices.Sorted(maps.Keys(h.passes)) {
		p := h.passes[id]
		if !p.read || p.done.IsZero() || p.cut {
			continue
		}
		if p.deleting {
			r.counts[countDeletionPasses]++
		}
		find := func(class string, o stObj, f string, a ...any) {
			r.find(stFinding{class: class, text: fmt.Sprintf("%s: %s: ", p, o) + fmt.Sprintf(f, a...),
				obj: o, holders: []stObj{p.holder}, from: p.began.Add(-5 * time.Second), to: p.done.Add(2 * time.Second), zero: p.began})
		}
		kind, n := p.holder.kind, p.holder.ns
		fin, err := cu.FinalizerName(gkOf(kind), n)
		if err != nil {
			find(findIncomplete, p.holder, "%v", err)
			continue
		}
		mine := func(o stObj, f func(c stCall) bool) bool {
			return len(h.callsOn(o, func(c stCall) bool { return c.pass == p.id && f(c) })) > 0
		}
		for _, a2 := range h.callsOf(p, func(c stCall) bool { return c.op == "a2" && c.ok() && c.obj.ns != n }) {
			switch read, consents := h.consentRead(p, a2.obj, a2.start); {
			case !read:
				find(findHeldUnconsented, a2.obj, "the pass wrote A2 on it, and read no consent of it before: %s", a2)
			case !consents:
				find(findHeldUnconsented, a2.obj, "the pass wrote A2 on it, after a read that refuses: %s", a2)
			default:
				r.counts[countCrossHolds]++
			}
		}
		for _, o := range p.held {
			a2s := h.callsOn(o, func(c stCall) bool { return c.pass == p.id && c.op == "a2" && c.ok() && c.fin == fin })
			if len(a2s) > 0 {
				last := a2s[len(a2s)-1]
				v, ok := h.version(o, last.rv)
				switch {
				case ok && !slices.Contains(v.fins, fin):
					find(findHeldWithoutA2, o, "its A2 left no finalizer: %s; %s", last, v)
				case !h.announced(p, o, a2s[0]):
					find(findHeldUnannounced, o, "its A2 came before its record named it: %s", a2s[0])
				case p.deleting:
					r.counts[countHeldByA2]++
				}
				continue
			}
			if o.ns != n && h.unreadable(p, o) {
				continue // its consent couldn't be read: the hold is kept (§11.3)
			}
			if p.deleting {
				find(findHeldWithoutA2, o, "a deletion pass holds it without an A2")
				continue
			}
			h.checkSkip(r, p, o, fin, find)
		}
		for o, why := range p.unheld {
			ok := false
			switch cu.Reason(why) {
			case cu.ReasonMissing:
				notFound := func(c stCall) bool { return c.notFound }
				ok = mine(o, notFound) || mine(nsObj(o.ns), notFound)
			case cu.ReasonDeleting:
				ok = mine(o, func(c stCall) bool { return c.refused })
			case cu.ReasonNotConsented, cu.ReasonConsentWithdrawn:
				read, consents := h.consentRead(p, o, p.done)
				ok = read && !consents
			case cu.ReasonConsentUnreadable:
				ok = h.unreadable(p, o)
			}
			if !ok {
				find(findUnreported, o, "it is %s, and no answer to the pass says so", why)
			}
		}
		h.checkStopped(r, p, find)
		if p.stopped() {
			continue
		}
		for _, o := range p.refs {
			if _, unheld := p.unheld[o]; !unheld && !slices.Contains(p.held, o) {
				find(findUnreported, o, "the holder names it, and the pass neither holds it nor says why not")
			}
		}
		h.checkRecord(r, p, find)
	}
}

// consentRead reports whether p read, live, the Namespace's and the object's
// consent of o before t, and whether the last answers to it consent.
func (h *stHistory) consentRead(p *stPass, o stObj, t time.Time) (read, consents bool) {
	var ns, obj stCall
	for _, c := range h.callsOf(p, func(c stCall) bool { return c.op == "get" && c.ok() && !c.end.After(t) }) {
		switch c.obj {
		case nsObj(o.ns):
			ns = c
		case o:
			obj = c
		}
	}
	if ns.op == "" || obj.op == "" {
		return false, false
	}
	return true, consented(obj.consent, ns.consent, p.holder.ns)
}

// unreadable: a read of o's consent by p failed, and not with a NotFound.
func (h *stHistory) unreadable(p *stPass, o stObj) bool {
	return len(h.callsOf(p, func(c stCall) bool {
		return c.op == "get" && !c.ok() && !c.notFound && (c.obj == o || c.obj == nsObj(o.ns))
	})) > 0
}

// checkStopped: a pass whose acquire stopped, or left the holder Degraded,
// was answered something that explains it: a conflict; that the object is
// being deleted without the finalizer; that the holder is gone; or nothing,
// or a 5xx, which is counted. In a chained run, the read of a held Target's
// content is made by a client that isn't recorded.
func (h *stHistory) checkStopped(r *stReport, p *stPass, find func(class string, o stObj, f string, a ...any)) {
	if !p.stopped() && !p.degraded {
		return
	}
	var refused, unknown, explained bool
	// A patch answered with a 404, and the read that confirms it finds the
	// object, recreated between the two: an ordinary error (§6.2).
	for _, c := range h.callsOf(p, func(c stCall) bool { return c.op == "a2" && c.notFound }) {
		explained = explained || len(h.callsOf(p, func(g stCall) bool { return g.op == "get" && g.ok() && g.obj == c.obj && !g.start.Before(c.end) })) > 0
	}
	for _, c := range h.callsOf(p, func(c stCall) bool { return !c.ok() }) {
		refused = refused || c.refused
		unknown = unknown || c.unknown
		switch {
		case p.conflict:
			explained = explained || c.op == "a1" && c.code == 409
		default:
			explained = explained || c.refused || c.unknown || c.op == "a1" && c.notFound || c.op == "a2" && c.code == 409
		}
	}
	switch {
	case !explained && h.mode == stressChained && strings.HasPrefix(p.acqErr, "credential-use: reading "):
	case !explained:
		find(findAcquireFailed, p.holder, "nothing the API server answered it explains that it stopped, or is Degraded")
	case unknown && !refused:
		r.counts[countAPIErrors]++
	case refused && p.deleting:
		r.counts[countDegraded]++
	}
}

// checkRecord: p went through every ref, so the record it left, its last
// A1's or the one it found, names exactly the refs of the version it read,
// less those whose consent refuses. A Missing ref, and one being deleted,
// stay. Only where every consent could be read; and not in a chained run,
// whose record names more (§9).
func (h *stHistory) checkRecord(r *stReport, p *stPass, find func(class string, o stObj, f string, a ...any)) {
	if h.mode == stressChained {
		return
	}
	var want []stObj
	for _, o := range p.refs {
		if o.ns != p.holder.ns && h.unreadable(p, o) {
			return
		}
		switch cu.Reason(p.unheld[o]) {
		case cu.ReasonNotConsented, cu.ReasonConsentWithdrawn:
		default:
			want = append(want, o)
		}
	}
	v, ok := h.version(p.holder, h.lastRead(p))
	if !ok {
		find(findIncomplete, p.holder, "the watch doesn't have the version it left, rv=%d", h.lastRead(p))
		return
	}
	if v.recBad || len(v.rec) != len(want) || slices.ContainsFunc(want, func(o stObj) bool { return !slices.Contains(v.rec, o) }) {
		find(findRecordDiffers, p.holder, "the holder it read names %v, and the record it left is %v: %s", want, v.rec, v)
		return
	}
	r.counts[countRecords]++
}

// lastRead is the resourceVersion of the holder as p last had it: its last
// A1's, or the one it read.
func (h *stHistory) lastRead(p *stPass) uint64 {
	rv := p.rv
	for _, c := range h.callsOf(p, func(c stCall) bool { return c.op == "a1" && c.ok() && c.obj == p.holder }) {
		rv = c.rv
	}
	return rv
}

// announced: the record named o before p's A2 on it: in the version p read,
// or in one an A1 of p's wrote (§6.2).
func (h *stHistory) announced(p *stPass, o stObj, a2 stCall) bool {
	if v, ok := h.version(p.holder, p.rv); ok && slices.Contains(v.rec, o) {
		return true
	}
	return slices.ContainsFunc(h.callsOn(p.holder, func(c stCall) bool { return c.pass == p.id && c.op == "a1" && c.ok() && c.rv < a2.rv }), func(c stCall) bool {
		v, ok := h.version(p.holder, c.rv)
		return ok && slices.Contains(v.rec, o)
	})
}

// checkSkip checks a live pass's hold without an A2: the version the pass
// worked from names the ref, and its process had a successful A2 on it that
// began after the A1 that began the run of versions naming it (§6.3). That
// A2 wrote an incarnation that the process's caches may still have had; the
// object carried the finalizer in some version they may have had; and so did
// its consent, of another namespace's. The caches may be cacheWindow behind.
func (h *stHistory) checkSkip(r *stReport, p *stPass, o stObj, fin string, find func(class string, o stObj, f string, a ...any)) {
	v, ok := h.version(p.holder, p.rv)
	if !ok {
		find(findIncomplete, o, "the watch doesn't have the holder's version rv=%d", p.rv)
		return
	}
	first, ok := h.run(v, o)
	if !ok {
		find(findHeldWithoutA2, o, "it skips A2, and the record it read doesn't name it: %s", v)
		return
	}
	a1s := h.callsOn(p.holder, func(c stCall) bool { return c.op == "a1" && c.ok() && c.rv == first.rv })
	if len(a1s) == 0 {
		r.counts[countSkipsUnchecked]++
		return
	}
	from := p.began.Add(-cacheWindow)
	a2s := h.callsOn(o, func(c stCall) bool {
		return c.proc == p.proc && c.op == "a2" && c.ok() && c.fin == fin && c.start.After(a1s[0].start) && c.start.Before(p.done)
	})
	if len(a2s) == 0 {
		find(findHeldWithoutA2, o, "it skips A2, and its process wrote none since the record named it, at %s", first)
		return
	}
	if !slices.ContainsFunc(a2s, func(c stCall) bool {
		d, gone := h.removal(o, c.uid)
		return !gone || !d.at.Before(from)
	}) {
		find(findHeldWithoutA2, o, "it skips A2, and every incarnation its process wrote had gone %s before it began: the last: %s", cacheWindow, a2s[len(a2s)-1])
		return
	}
	if !h.during(from, p.done, []stObj{o}, func(vs []stVersion) bool { return slices.Contains(vs[0].fins, fin) }) {
		find(findHeldWithoutA2, o, "it skips A2, and no version since %s before it began carries the finalizer", cacheWindow)
		return
	}
	if o.ns != p.holder.ns && !h.during(from, p.done, []stObj{o, nsObj(o.ns)}, func(vs []stVersion) bool {
		return consented(vs[0].consent, vs[1].consent, p.holder.ns)
	}) {
		find(findHeldUnconsented, o, "it skips A2, and its consent has been gone since %s before it began", cacheWindow)
		return
	}
	r.counts[countSkips]++
}

// during reports whether f held, of the versions of objs together, at some
// time from from to to, by when the watch delivered them. They all existed
// then.
func (h *stHistory) during(from, to time.Time, objs []stObj, f func(vs []stVersion) bool) bool {
	var all []stVersion
	for _, o := range objs {
		all = append(all, h.objs[o]...)
	}
	slices.SortStableFunc(all, func(a, b stVersion) int { return cmp.Compare(a.rv, b.rv) })
	now := make([]stVersion, len(objs))
	for i, v := range all {
		now[slices.Index(objs, v.stObj)] = v
		if v.at.After(to) || i+1 < len(all) && all[i+1].at.Before(from) {
			continue
		}
		if !slices.ContainsFunc(now, func(v stVersion) bool { return v.uid == "" || v.gone }) && f(now) {
			return true
		}
	}
	return false
}

// auditRecords: no record write drops a ref the holder still names, and none
// can't be parsed. In a chained run the refs behind a Target are only
// counted: a pass on an older chain drops them (H2, open).
func (h *stHistory) auditRecords(r *stReport) {
	for o, vs := range h.objs {
		if !h.holderKind(o.kind) {
			continue
		}
		for i := 1; i < len(vs); i++ {
			prev, cur := vs[i-1], vs[i]
			if prev.gone || cur.gone || prev.uid != cur.uid || cur.deleting && !cur.owner() {
				continue
			}
			find := func(p stObj, f string, a ...any) {
				r.find(stFinding{class: findRecordDrop, text: fmt.Sprintf("%s -> %s: ", prev, cur) + fmt.Sprintf(f, a...),
					obj: p, holders: []stObj{o}, from: cur.at.Add(-5 * time.Second), to: cur.at.Add(2 * time.Second), zero: cur.at})
			}
			if cur.recBad && !prev.recBad {
				find(stObj{}, "the record can't be parsed")
				continue
			}
			refs := refsOf(h.mode, o.ns, cur.spec)
			for _, p := range prev.rec {
				if cur.names(p) {
					continue
				}
				if !slices.Contains(refs, p) {
					if h.mode == stressChained && h.behindTarget(cur, p) {
						r.counts[countDrops]++
					}
					continue
				}
				if p.ns != o.ns && h.refusedConsent(o, cur, p) {
					continue // §11.3: a withdrawal, read live
				}
				find(p, "the holder names %s, and its record doesn't any more", p)
			}
		}
	}
}

// behindTarget: p is the Secret behind the Target that holder version v
// names, as the API server had it at v.
func (h *stHistory) behindTarget(v stVersion, p stObj) bool {
	t, ok := h.state(stObj{"Target", v.ns, v.spec}, v.rv)
	return ok && p == (stObj{"Secret", v.ns, t.spec})
}

// refusedConsent: the pass that wrote holder version v read p's consent, and
// its answers refuse it.
func (h *stHistory) refusedConsent(holder stObj, v stVersion, p stObj) bool {
	for _, w := range h.callsOn(holder, func(c stCall) bool { return c.op == "a1" && c.ok() && c.rv == v.rv && c.pass != 0 }) {
		if by := h.passes[w.pass]; by != nil {
			if read, consents := h.consentRead(by, p, w.start); read && !consents {
				return true
			}
		}
	}
	return false
}

// auditEstablished is §6.7: when a holder gets its owner finalizer, each ref
// its record names has been written by an A2 of its pass, or skipped one that
// auditPasses checks, or been found absent by a read of that pass that came
// after its last A2; or, in another namespace, without consent by one. The
// pass is one that worked from the version the finalizer was added to.
func (h *stHistory) auditEstablished(r *stReport) {
	byHolder := map[stObj][]*stPass{}
	for _, id := range slices.Sorted(maps.Keys(h.passes)) {
		p := h.passes[id]
		byHolder[p.holder] = append(byHolder[p.holder], p)
	}
	for o, vs := range h.objs {
		if !h.holderKind(o.kind) {
			continue
		}
		for i := 1; i < len(vs); i++ {
			prev, cur := vs[i-1], vs[i]
			if prev.gone || cur.gone || prev.uid != cur.uid || prev.owner() || !cur.owner() {
				continue
			}
			var problems []string
			ok := false
			for _, p := range byHolder[o] {
				if !p.read || p.uid != cur.uid || p.deleting || p.mustNot || p.conflict || h.lastRead(p) != prev.rv {
					continue
				}
				why := h.unestablished(p, cur)
				if why == "" {
					ok = true
					break
				}
				problems = append(problems, fmt.Sprintf("%s: %s", p, why))
			}
			if ok {
				r.counts[countEstablished]++
				continue
			}
			text := "no pass worked from it"
			if len(problems) > 0 {
				text = strings.Join(problems, "; ")
			}
			r.find(stFinding{class: findEstablished, text: fmt.Sprintf("%s -> %s: %s", prev, cur, text),
				obj: o, holders: []stObj{o}, from: cur.at.Add(-5 * time.Second), to: cur.at.Add(2 * time.Second), zero: cur.at})
		}
	}
}

// unestablished is why p may not establish its holder as version cur, or "".
func (h *stHistory) unestablished(p *stPass, cur stVersion) string {
	if cur.recBad {
		return "its record can't be parsed"
	}
	var lastA2 time.Time
	for _, c := range h.callsOf(p, func(c stCall) bool { return c.op == "a2" }) {
		lastA2 = c.end
	}
	for _, ref := range cur.rec {
		if slices.Contains(p.held, ref) || len(h.callsOf(p, func(c stCall) bool { return c.op == "a2" && c.ok() && c.obj == ref })) > 0 {
			continue
		}
		absent := len(h.callsOf(p, func(c stCall) bool {
			return c.op == "get" && c.notFound && (c.obj == ref || c.obj == nsObj(ref.ns)) && !c.start.Before(lastA2)
		})) > 0
		read, consents := h.consentRead(p, ref, p.done)
		if absent || ref.ns != p.holder.ns && read && !consents {
			continue
		}
		return fmt.Sprintf("%s is neither held nor found absent after the pass's last A2", ref)
	}
	return ""
}

// auditReads is §14's I1: a pass that began as a deletion pass reads what it
// has just held. It must not fail, unless the API server itself removed the
// object over its first hold (H3), on proof:
//
//   - a DELETE of the test's was answered with the very incarnation this
//     pass's A2 held (removedByDELETE), which carried the finalizer when it
//     went, and was never marked as being deleted;
//   - and no release of the name that succeeded overlaps that hold, and none
//     before the read has an outcome that isn't known.
//
// Nothing else is exempt: not by the error's text, and not by how the holder
// is when it is read again. Whether the holder still existed is the watch's
// word, at the revision the object went.
func (h *stHistory) auditReads(r *stReport) {
	for _, f := range h.reads {
		p := h.passes[f.pass]
		find := func(text string, a ...any) {
			r.find(stFinding{class: findLostRead, text: fmt.Sprintf("%s read %s: %s: ", f.holder, f.obj, f.err) + fmt.Sprintf(text, a...),
				obj: f.obj, holders: []stObj{f.holder}, from: f.start.Add(-5 * time.Second), to: f.end.Add(3 * time.Second), zero: f.start})
		}
		if p == nil || !p.read {
			find("its pass wasn't recorded")
			continue
		}
		if !p.deleting {
			r.counts[countLiveReads]++
			continue
		}
		if !f.notFound {
			find("the API server answered with an error")
			continue
		}
		fin, _ := cu.FinalizerName(gkOf(p.holder.kind), p.holder.ns)
		a2s := h.callsOn(f.obj, func(c stCall) bool {
			return c.pass == p.id && c.op == "a2" && c.ok() && c.fin == fin && !c.end.After(f.start)
		})
		if len(a2s) == 0 {
			find("its pass wrote no A2 on it")
			continue
		}
		a2 := a2s[len(a2s)-1]
		d, ok := h.removal(f.obj, a2.uid)
		if !ok {
			find("the watch saw no removal of the incarnation it held, %s", short(a2.uid))
			continue
		}
		if v, ok := h.state(p.holder, d.rv-1); !ok || v.uid != p.uid {
			r.counts[countHolderGone]++
			continue
		}
		if ok, why := h.exempt(f, d, fin); !ok {
			find("it isn't H3: %s; the object's end: %s; the pass's A2: %s", why, d, a2)
			continue
		}
		r.counts[countH3Reads]++
	}
}

// exempt reports whether removal d, which read f met, is H3 on proof, and if
// not, why not.
func (h *stHistory) exempt(f stRead, d stVersion, fin string) (bool, string) {
	if !slices.Contains(d.fins, fin) {
		return false, "it didn't carry the finalizer when it went"
	}
	if ok, why := h.removedByDELETE(d); !ok {
		return false, why
	}
	// The window is from the A2 that began the hold it went with, to the read.
	// The API server answers a release only once it has done with it, so one
	// answered before that A2 began touched nothing since. One whose outcome
	// isn't known may still have been at work, however early it began.
	began := h.createBegan(d.stObj, d.uid)
	if v, held := h.heldSince(d); held {
		for _, a := range h.callsOn(d.stObj, func(a stCall) bool { return a.op == "a2" && a.ok() && a.rv == v.rv }) {
			began = a.start
		}
	}
	rs := h.callsOn(d.stObj, func(c stCall) bool {
		return c.op == "r3" && !c.start.After(f.end) && (c.unknown || c.ok() && !c.end.Before(began))
	})
	if len(rs) > 0 {
		return false, fmt.Sprintf("a release of the name overlaps the hold: %s", rs[0])
	}
	return true, ""
}

// maxTimeline bounds one finding's timeline.
const maxTimeline = 400

// timeline is everything recorded of finding f's object, its namespace and
// its holders, in f's window, with the revision of each write and read first:
// that is the order, and the times are when the test saw them. Whatever the
// window, it has the object's whole life in versions, its create, its
// holders' versions and their A1s, so that a finding can be told from its
// print alone.
func (h *stHistory) timeline(f stFinding) string {
	type line struct {
		at   time.Time
		rv   uint64
		keep bool
		s    string
	}
	var ls []line
	in := func(t time.Time) bool { return !t.Before(f.from) && !t.After(f.to) }
	addRV := func(at time.Time, rv uint64, always bool, s string, a ...any) {
		if always || in(at) {
			ls = append(ls, line{at, rv, always, fmt.Sprintf(s, a...)})
		}
	}
	add := func(at time.Time, s string, a ...any) { addRV(at, 0, false, s, a...) }
	uids := map[string]bool{}
	for _, v := range h.objs[f.obj] {
		if in(v.at) {
			uids[v.uid] = true
		}
	}
	objs := append([]stObj{f.obj, nsObj(f.obj.ns)}, f.holders...)
	for _, o := range f.holders {
		objs = append(objs, nsObj(o.ns))
		if h.mode == stressChained {
			for _, v := range h.objs[o] {
				if t := (stObj{"Target", o.ns, v.spec}); in(v.at) {
					objs = append(objs, t)
				}
			}
		}
	}
	slices.SortFunc(objs, func(a, b stObj) int { return cmp.Compare(a.String(), b.String()) })
	objs = slices.Compact(objs)
	for _, o := range objs {
		for _, v := range h.objs[o] {
			addRV(v.at, v.rv, o == f.obj && uids[v.uid] || o.kind == "Namespace" || slices.Contains(f.holders, o), "watch   %s", v)
		}
		for _, w := range h.writes[o] {
			add(w.start, "began   %s", w.op+" of "+w.obj.String()+" by the test")
			addRV(w.end, w.rv, w.op == "create" && o == f.obj && uids[w.uid], "answer  %s (began %s earlier)", w, w.end.Sub(w.start).Round(time.Microsecond))
		}
	}
	lists := map[string]bool{} // the namespaces whose holders' lists matter
	for _, o := range objs {
		if o.kind == "Namespace" {
			lists[o.name] = true
		}
	}
	for _, c := range h.callsOn(f.obj, func(c stCall) bool { return c.op == "r3" && in(c.end) }) {
		_, n, _ := holderOfFinalizer(c.fin)
		lists[n] = true
	}
	for _, c := range h.calls {
		if c.op == "list" && h.holderKind(c.obj.kind) && lists[c.obj.ns] {
			var naming []stItem
			for _, i := range c.items {
				if i.recBad || slices.Contains(i.rec, f.obj) {
					naming = append(naming, i)
				}
			}
			addRV(c.end, c.rv, false, "call    %s (began %s earlier), whose records name %s: %v", c, c.end.Sub(c.start).Round(time.Microsecond), f.obj, naming)
		} else if slices.Contains(objs, c.obj) {
			addRV(c.end, c.rv, c.op == "a1" && c.ok() && slices.Contains(f.holders, c.obj), "call    %s (began %s earlier)", c, c.end.Sub(c.start).Round(time.Microsecond))
		}
	}
	for _, p := range h.passes {
		if !slices.Contains(f.holders, p.holder) {
			continue
		}
		add(p.began, "pass    p%d pass %d of %s begins, on rv=%d deleting=%v", p.proc, p.id, p.holder, p.rv, p.deleting)
		if !p.done.IsZero() {
			add(p.done, "pass    %s", p)
		}
	}
	for _, rd := range h.reads {
		if rd.obj == f.obj || slices.Contains(f.holders, rd.holder) {
			add(rd.start, "read    pass %d of %s reads %s", rd.pass, rd.holder, rd.obj)
			add(rd.end, "read    pass %d of %s read %s: %s", rd.pass, rd.holder, rd.obj, rd.err)
		}
	}
	slices.SortStableFunc(ls, func(a, b line) int { return a.at.Compare(b.at) })
	var b strings.Builder
	if n := len(ls) - maxTimeline; n > 0 {
		fmt.Fprintf(&b, "    (the %d earliest lines that aren't kept are left out)\n", n)
		ls = slices.DeleteFunc(ls, func(l line) bool {
			if l.keep || n == 0 {
				return false
			}
			n--
			return true
		})
	}
	for _, l := range ls {
		rv := ""
		if l.rv != 0 {
			rv = fmt.Sprintf("rv=%d", l.rv)
		}
		fmt.Fprintf(&b, "    %-12s %+10.1fms  %s\n", rv, float64(l.at.Sub(f.zero).Microseconds())/1000, l.s)
	}
	return b.String()
}
