package credentialuse

import (
	"crypto/sha256"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The abstract state of docs/credential-use-model-reduction.md §10, for the
// stateful search: everything a schedule's future depends on, encoded
// canonically, then hashed.
//
//   - Names compared only for equality (UIDs, resourceVersions, epochs) are
//     renamed in first-occurrence order of a canonical traversal, which
//     orders by object key and actor registration and never by a raw name.
//   - Times are terms: each is written as whole units U relative to the
//     earliest live term's, plus the rank of its fine part (its offset from
//     a whole U, counted from apimodel.Start) among all terms' fine parts
//     (§10.2). The terms include every registered deadline, passed or
//     pending. The invariant this relies on is the scheduler's: a tick never
//     moves now's fine part past any live term's (apimodel.Sched.tick), so
//     the global order of fine parts changes only when a term is added, and
//     now's place in it doesn't depend on tick sizes. Every constant the code
//     adds to a time is a multiple of U, so t + c against u is decided by the
//     whole parts and then this order, including when t and u sit at
//     different levels.
//   - Actors are their park, unit status, declared locals and the inputs of
//     their current unit (§10.1).

// rawKeys makes keys readable, for debugging the merge audit.
var rawKeys = os.Getenv("CREDENTIALUSE_MODEL_RAWKEYS") != ""

// timeUnit is U: every constant the code adds to a time is a multiple of it.
const timeUnit = apimodel.TimeUnit

type keyEncoder struct {
	b     strings.Builder
	names map[string]int
	terms []time.Time
	// rep, in a second pass, renders a time abstractly (as finish does), so
	// that unknown UIDs can be ordered by content canonically; ambiguous
	// says the first pass had to order two or more unknown UIDs.
	rep       func(time.Time) string
	ambiguous bool
}

func newKeyEncoder() *keyEncoder { return &keyEncoder{names: map[string]int{}} }

func (e *keyEncoder) str(s string) {
	e.b.WriteString(strconv.Quote(s))
	e.b.WriteByte(' ')
}

func (e *keyEncoder) tok(s string) {
	e.b.WriteString(s)
	e.b.WriteByte(' ')
}

// name writes a renamed name of class (uid, rv, epoch).
func (e *keyEncoder) name(class, v string) {
	if v == "" {
		e.tok(class + ":-")
		return
	}
	k := class + ":" + v
	i, ok := e.names[k]
	if !ok {
		i = len(e.names)
		e.names[k] = i
	}
	e.tok(class + "#" + strconv.Itoa(i))
}

func (e *keyEncoder) known(class, v string) (int, bool) {
	i, ok := e.names[class+":"+v]
	return i, ok
}

// timeStr and nameStr render a time and a name for uidOrder's content:
// abstractly, never by raw value.
func (e *keyEncoder) timeStr(t time.Time) string {
	if e.rep == nil || t.IsZero() {
		return "T?"
	}
	return e.rep(t)
}

func (e *keyEncoder) nameStr(class, v string) string {
	if i, ok := e.known(class, v); ok {
		return class + "#" + strconv.Itoa(i)
	}
	return class + "?"
}

// time writes a time term; the zero time is "no time".
func (e *keyEncoder) time(t time.Time) {
	if t.IsZero() {
		e.tok("T-")
		return
	}
	e.terms = append(e.terms, t)
	e.b.WriteString("\x01" + strconv.Itoa(len(e.terms)-1) + "\x01 ")
}

// meta writes an object's metadata, names renamed and times as terms.
func (e *keyEncoder) meta(m *metav1.ObjectMeta) {
	if m == nil {
		e.tok("nil")
		return
	}
	e.str(m.Namespace + "/" + m.Name)
	e.name("uid", string(m.UID))
	e.name("rv", m.ResourceVersion)
	e.time(m.CreationTimestamp.Time)
	if m.DeletionTimestamp != nil {
		e.time(m.DeletionTimestamp.Time)
	} else {
		e.tok("live")
	}
	e.tok("f[")
	for _, f := range m.Finalizers {
		e.str(f)
	}
	e.tok("]l[")
	for _, k := range sortedKeys(m.Labels) {
		e.str(k)
		e.str(m.Labels[k])
	}
	e.tok("]a[")
	for _, k := range sortedKeys(m.Annotations) {
		e.str(k)
		if isEpochKey(k) {
			e.name("epoch", m.Annotations[k])
		} else {
			e.str(m.Annotations[k])
		}
	}
	e.tok("]")
}

// isEpochKey: an epoch annotation's key is a credential-use finalizer name.
func isEpochKey(k string) bool {
	return strings.HasPrefix(k, "credential-use.") && !strings.HasPrefix(k, Domain+"/")
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// value writes an actor's input or local.
func (e *keyEncoder) value(v any) {
	if label, fault, ok := apimodel.CallInput(v); ok {
		e.tok("call")
		e.str(label)
		e.tok(fault.String())
		return
	}
	switch x := v.(type) {
	case nil:
		e.tok("nil")
	case apimodel.TimeInput:
		e.tok("now")
		e.time(time.Time(x))
	case time.Time:
		e.time(x)
	case bool:
		e.tok(strconv.FormatBool(x))
	case int:
		e.tok(strconv.Itoa(x))
	case string:
		e.str(x)
	case types.UID:
		e.name("uid", string(x))
	case resultInput:
		e.tok("res")
		e.str(x.err)
		e.value(x.out)
	case *metav1.PartialObjectMetadata:
		if x == nil {
			e.tok("nil")
		} else {
			e.meta(&x.ObjectMeta)
		}
	case []metav1.PartialObjectMetadata:
		e.tok("list[")
		for i := range x {
			e.meta(&x[i].ObjectMeta)
		}
		e.tok("]")
	case []*metav1.PartialObjectMetadata:
		e.tok("plist[")
		for _, o := range x {
			e.meta(&o.ObjectMeta)
		}
		e.tok("]")
	default:
		// An input the hash can't represent would make two different
		// states look alike: refuse it (§10.6).
		panic(fmt.Sprintf("state: an actor input of type %T has no encoding", v))
	}
}

// uidOrder orders map keys that are UIDs canonically: names already renamed
// by their index, the rest by their content, then renamed in that order.
func (e *keyEncoder) uidOrder(keys []types.UID, content func(types.UID) string) []types.UID {
	type item struct {
		uid     types.UID
		idx     int
		known   bool
		content string
	}
	items := make([]item, 0, len(keys))
	for _, u := range keys {
		i, ok := e.known("uid", string(u))
		it := item{uid: u, idx: i, known: ok}
		if !ok {
			it.content = content(u)
		}
		items = append(items, it)
	}
	slices.SortStableFunc(items, func(a, b item) int {
		switch {
		case a.known && b.known:
			return a.idx - b.idx
		case a.known:
			return -1
		case b.known:
			return 1
		}
		return strings.Compare(a.content, b.content)
	})
	out := make([]types.UID, len(items))
	for i, it := range items {
		out[i] = it.uid
	}
	unknown := 0
	for _, it := range items {
		if !it.known {
			unknown++
		}
	}
	if unknown > 1 {
		e.ambiguous = true
	}
	return out
}

// finish replaces the time placeholders with their representation (§10.2)
// and returns the state's digest.
func (e *keyEncoder) finish() string {
	s := e.b.String()
	if len(e.terms) > 0 {
		rep := e.timeRep()
		var out strings.Builder
		parts := strings.Split(s, "\x01")
		for i, p := range parts {
			if i%2 == 0 {
				out.WriteString(p)
				continue
			}
			k, _ := strconv.Atoi(p)
			out.WriteString(rep(e.terms[k]))
		}
		s = out.String()
	}
	if rawKeys {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return string(sum[:])
}

// stateKey is the scenario's part of the abstract state; the scheduler adds
// its last actor and the budgets (apimodel.Sched.Key). When the state holds
// two or more UIDs no live object carries, their order comes from their
// content, rendered abstractly, which takes a second pass.
func (w *world) stateKey() string {
	e := w.encode(nil)
	if e.ambiguous {
		e = w.encode(e.timeRep())
	}
	return e.finish()
}

// timeTerms are the state's live time terms, for the scheduler's tick.
func (w *world) timeTerms() []time.Time { return w.encode(nil).terms }

func (w *world) encode(rep func(time.Time) string) *keyEncoder {
	e := newKeyEncoder()
	e.rep = rep
	e.tok("now")
	e.time(w.s.Now())

	// The model.
	e.tok("model[")
	for _, ev := range w.m.Objects() {
		e.meta(&ev.Obj.ObjectMeta)
	}
	e.tok("]")
	// The caches, and the log they haven't consumed.
	minSeq := w.m.LastSeq()
	for _, p := range w.procOrder {
		if _, seq := (*p).cache.Objects(); seq < minSeq {
			minSeq = seq
		}
	}
	e.tok("log[")
	for _, ev := range w.m.EventsAfter(minSeq) {
		e.tok(string(ev.Type))
		e.meta(&ev.Obj.ObjectMeta)
	}
	e.tok("]")
	for _, p := range w.procOrder {
		e.tok("proc")
		e.str((*p).name)
		objs, seq := (*p).cache.Objects()
		e.tok("cache@" + strconv.Itoa(seq-minSeq) + "[")
		for _, ev := range objs {
			e.meta(&ev.Obj.ObjectMeta)
		}
		e.tok("]")
		w.encodeProcessMemory(e, *p)
	}

	// The actors.
	for _, v := range w.s.Views() {
		e.tok("actor")
		e.str(v.Name)
		e.str(v.Label)
		e.tok(fmt.Sprint(v.Done, v.AtCall, v.Unit))
		e.time(v.NotBefore)
		e.tok("locals[")
		for _, l := range v.Locals {
			e.value(l)
		}
		e.tok("]in[")
		for _, in := range v.Inputs {
			e.value(in)
		}
		e.tok("]")
	}

	// The monitors and the scenario's own state.
	e.tok("holds[")
	for _, h := range sortedKeys(w.holds) {
		for _, id := range sortedIDs(w.holds[h]) {
			e.str(h + "|" + id.String())
			e.time(w.holds[h][id].at)
		}
	}
	e.tok("]phase[")
	for _, a := range sortedKeys(w.deletionPhase) {
		e.str(a)
		e.tok(strconv.FormatBool(w.deletionPhase[a]))
	}
	e.tok("]counters[")
	for _, c := range []map[string]int{w.livePasses, w.started, w.batches, w.batchesStarted} {
		for _, k := range sortedKeys(c) {
			e.str(k)
			e.tok(strconv.Itoa(c[k]))
		}
		e.tok("|")
	}
	e.tok("]")
	e.time(w.nsDeleted)
	if d := w.lastAllowed; d != nil {
		e.tok("allowed")
		e.time(d.NamespaceReadAt)
		e.time(d.GraceEnd)
		e.time(d.at)
	} else {
		e.tok("noallowed")
	}
	e.tok(fmt.Sprint(w.releasedAny, w.u5Any, w.deletingRead, w.l6 > 0, w.h1 > 0))
	e.tok("thresholds[")
	for _, t := range w.thresholds {
		e.time(t)
	}
	// Every registered deadline is a term (§10.2), so that its place among
	// the times at its level is kept; actors' notBefores are in their part.
	e.tok("]deadlines[")
	ds := w.deadlines()
	slices.SortFunc(ds, func(a, b time.Time) int { return a.Compare(b) })
	for _, d := range slices.CompactFunc(ds, func(a, b time.Time) bool { return a.Equal(b) }) {
		e.time(d)
	}
	e.tok("]")
	e.tok("pinned[")
	for _, v := range w.pinned {
		e.value(v)
	}
	e.tok("]")
	return e
}

func sortedIDs[V any](m map[ID]V) []ID {
	ids := make([]ID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b ID) int { return strings.Compare(a.String(), b.String()) })
	return ids
}

// encodeProcessMemory writes a process's verified set and first sights.
func (w *world) encodeProcessMemory(e *keyEncoder, p *process) {
	v := p.ver
	timesByNS := func(m map[string]time.Time) string {
		var b strings.Builder
		for _, k := range sortedKeys(m) {
			fmt.Fprintf(&b, "%s=%s;", k, e.timeStr(m[k]))
		}
		return b.String()
	}
	e.tok("written[")
	for _, u := range e.uidOrder(mapKeys(v.written), func(u types.UID) string { return timesByNS(v.written[u]) }) {
		e.name("uid", string(u))
		for _, n := range sortedKeys(v.written[u]) {
			e.str(n)
			e.time(v.written[u][n])
		}
	}
	e.tok("]delivered[")
	for _, u := range e.uidOrder(mapKeys(v.delivered), func(u types.UID) string { return e.nameStr("rv", v.delivered[u]) }) {
		e.name("uid", string(u))
		e.name("rv", v.delivered[u])
	}
	idTimes := func(m map[ID]time.Time) string {
		var b strings.Builder
		for _, id := range sortedIDs(m) {
			fmt.Fprintf(&b, "%s=%s;", id, e.timeStr(m[id]))
		}
		return b.String()
	}
	for _, part := range []struct {
		name string
		m    map[types.UID]map[ID]time.Time
	}{{"recordSince", v.recordSince}, {"observed", v.observed}} {
		e.tok("]" + part.name + "[")
		for _, u := range e.uidOrder(mapKeys(part.m), func(u types.UID) string { return idTimes(part.m[u]) }) {
			e.name("uid", string(u))
			for _, id := range sortedIDs(part.m[u]) {
				e.str(id.String())
				e.time(part.m[u][id])
			}
		}
	}
	e.tok("]")
	e.time(v.lastList)
	e.tok("seen[")
	at := p.seen.at
	for _, u := range e.uidOrder(mapKeys(at), func(u types.UID) string { return e.timeStr(at[u]) }) {
		e.name("uid", string(u))
		e.time(at[u])
	}
	e.tok("]")
}

func mapKeys[V any](m map[types.UID]V) []types.UID {
	ks := make([]types.UID, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// timeRep returns the abstract representation of §10.2 for the encoder's
// terms: whole units relative to the earliest term's, and the global rank of
// the fine part. Fine parts are anchored at apimodel.Start, a whole U, as the
// scheduler's tick is.
func (e *keyEncoder) timeRep() func(time.Time) string {
	type rep struct {
		n    int64
		fine time.Duration
	}
	split := func(t time.Time) rep {
		d := t.Sub(apimodel.Start)
		n := (d + timeUnit/2) / timeUnit
		f := d - n*timeUnit
		if f >= timeUnit/2 || f < -timeUnit/2 {
			panic(fmt.Sprintf("state: a time's fine part %s isn't below U/2", f))
		}
		return rep{int64(n), f}
	}
	minN := int64(0)
	var fines []time.Duration
	for i, t := range e.terms {
		r := split(t)
		if i == 0 || r.n < minN {
			minN = r.n
		}
		fines = append(fines, r.fine)
	}
	slices.Sort(fines)
	fines = slices.Compact(fines)
	return func(t time.Time) string {
		r := split(t)
		rank, found := slices.BinarySearch(fines, r.fine)
		if !found {
			// A time that isn't a term of this state (uidOrder's content in a
			// second pass only renders terms).
			return "T" + strconv.FormatInt(r.n-minN, 10) + ".?"
		}
		return "T" + strconv.FormatInt(r.n-minN, 10) + "." + strconv.Itoa(rank)
	}
}
