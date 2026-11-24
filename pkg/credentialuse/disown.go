package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

// Disown (§7.4): the switch on the holder kind's CRD, the strip loop, and the
// live count every process writes back on the CRD.

// A disownSet is what credential-use.krateo.io/disown switches off: every
// protected kind ("all"), or the GroupKinds listed.
type disownSet struct {
	all   bool
	kinds map[schema.GroupKind]bool
}

// parseDisown reads the switch's value: "all", or a comma-separated list of
// GroupKinds written Kind or Kind.group, as Secret or
// KubernetesTarget.core.krateo.io. Blanks are dropped; an empty value is off.
func parseDisown(v string) disownSet {
	d := disownSet{kinds: map[schema.GroupKind]bool{}}
	for _, s := range strings.Split(v, ",") {
		s = strings.TrimSpace(s)
		switch s {
		case "":
		case "all":
			d.all = true
		default:
			kind, group, _ := strings.Cut(s, ".")
			d.kinds[schema.GroupKind{Group: group, Kind: kind}] = true
		}
	}
	return d
}

// on reports whether the switch disowns anything.
func (d disownSet) on() bool { return d.all || len(d.kinds) > 0 }

// covers reports whether the switch disowns protected kind gk.
func (d disownSet) covers(gk schema.GroupKind) bool { return d.all || d.kinds[gk] }

// key names what the switch covers, the same for every value that covers the
// same kinds: a run of zero passes belongs to one key.
func (d disownSet) key() string {
	if d.all {
		return "all"
	}
	var ks []string
	for gk := range d.kinds {
		ks = append(ks, gk.String())
	}
	slices.Sort(ks)
	return strings.Join(ks, ",")
}

// newCRDInformer builds the informer on the holder kind's own CRD, selected
// by name (§7.4), caching its metadata with only its credential-use.
// annotations.
func newCRDInformer(md metadata.Interface, name string, clk clock.PassiveClock) (cache.SharedIndexInformer, error) {
	lw := newListWatch(md, crdGVR, "", "", clk, nil)
	lw.field = "metadata.name=" + name
	inf := cache.NewSharedIndexInformerWithOptions(lw, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
		ObjectDescription: "credential-use CRD " + name,
	})
	if err := inf.SetTransform(transform(func(string) bool { return false }, isCredentialUseKey)); err != nil {
		return nil, err
	}
	return inf, nil
}

// disownSwitch is the switch as t's CRD informer has it: off while the
// informer hasn't the CRD, or the CRD has no switch.
func (t *Tracker) disownSwitch() disownSet {
	if t.crd == nil {
		return disownSet{}
	}
	obj, ok, err := t.crd.GetStore().GetByKey(t.crdName)
	if err != nil || !ok {
		return disownSet{}
	}
	m, ok := metadataOf(obj)
	if !ok {
		return disownSet{}
	}
	return parseDisown(m.Annotations[AnnotationDisown])
}

// disownLive reads the switch live, from the CRD itself, for a pass's check
// before its first A2 (§10.6.8, interleaving 3; the user's decisions,
// 2026-11-22): a stale CRD informer, whose watch fails, can't let an A2
// through after the switch. A CRD that is gone has no switch.
func (t *Tracker) disownLive(ctx context.Context) (disownSet, error) {
	m, err := t.s.c.Metadata.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{})
	if Classify(err) == ClassNotFound {
		return disownSet{}, nil
	}
	if err != nil {
		return disownSet{}, err
	}
	return parseDisown(m.Annotations[AnnotationDisown]), nil
}

// disownState is one tracker's progress towards disown-settled (§7.4).
type disownState struct {
	mu sync.Mutex
	// value is the key of the switch the run of zeros counts under: a
	// change of what it covers starts the run over, and makes a pass due at
	// once (the review of PR 6).
	value string
	zeros int       // consecutive passes this process counted zero
	next  time.Time // when the next pass is due
	// starved counts the passes in a row whose count write conflicted on
	// every attempt (§7.3).
	starved int
	// cleared: when the switch is off, when the next try to clear the
	// count from the CRD is due.
	cleared time.Time
}

// disownTick is how often the Disown loop looks for a tracker whose pass is
// due; each tracker's passes are DisownInterval apart.
const disownTick = time.Second

// runDisown runs the Disown loop until ctx ends (§7.4): a pass for each
// tracker whose switch is on, every DisownInterval. It runs with the write
// loops, since it writes.
func (s *Shared) runDisown(ctx context.Context) {
	for {
		now := s.clock.Now()
		for _, t := range s.trackerList() {
			set := t.disownSwitch()
			if !set.on() {
				s.disownOff(ctx, t, now)
				continue
			}
			t.disown.mu.Lock()
			if k := set.key(); t.disown.value != k {
				t.disown.value, t.disown.zeros, t.disown.next = k, 0, time.Time{}
			}
			due := !now.Before(t.disown.next)
			if due {
				t.disown.next = now.Add(t.o.DisownInterval)
			}
			t.disown.mu.Unlock()
			if due {
				s.disownPass(ctx, t, set)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(disownTick):
		}
	}
}

// disownPass is one pass of t's Disown loop (§7.4): it lists, live and
// cluster-wide, the holders of t's kind and the protected objects of the
// kinds the switch covers by t's marker label, so that a namespace outside
// SharedOptions.Namespaces is reached too, or, for a PerName kind, reads each
// object its index or a record names; counts what they hold; strips it,
// emptying the index of those kinds; and writes the count on the CRD.
//
// The count is what the pass found before its strip, and what the strip
// removed that the lists hadn't shown: a hold that a pass begun before the
// switch put back is counted, whether it landed before the lists or between
// them and the strip, so it resets the settling (§10.6.8, interleaving 3);
// a pass counts zero only when it stripped nothing. A list that fails makes
// the count unknown, never zero. A strip that fails is left for the next
// pass, whose count shows it.
func (s *Shared) disownPass(ctx context.Context, t *Tracker, set disownSet) {
	known := true
	count := 0
	holders, err := s.liveList(ctx, t.gk, "", "")
	if err != nil {
		known = false
		t.reportDisownFailed(ctx, "list the holders, "+kindLabel(t.gk), err, "grant list on it, in every namespace",
			"restore it, or, if another version of it is served, restart at that one")
	}
	for i := range holders {
		if recordsDisowned(&holders[i], set) {
			count++
		}
	}
	type object struct {
		gk     schema.GroupKind
		m      *metav1.PartialObjectMetadata
		listed int // the kind's finalizers the list showed
	}
	var objs []object
	for _, gk := range disownedKinds(t, set) {
		if t.isPerName(gk) {
			continue // from the index and the records, below
		}
		items, err := s.liveList(ctx, gk, "", MarkerLabel(t.gk))
		if err != nil {
			known = false
			t.reportDisownFailed(ctx, "list "+kindLabel(gk), err, "grant list on it, in every namespace",
				"restore it; or, once its objects are gone, stop naming it, let the passes drop it from the records, take it out of the tracker's protected kinds and restart; if another version of it is served, restart at that one instead")
			continue
		}
		for i := range items {
			n := countPrefix(items[i].Finalizers, t.prefix)
			count += n
			objs = append(objs, object{gk, &items[i], n})
		}
	}
	// A PerName kind can't be listed under resourceNames RBAC: its objects
	// are the union of the index's lines and the holders' records, read with
	// a GET each (§7.4, A15). The index is the superset of the holds
	// (§10.6.3), and the records must be stripped anyway. A missing or
	// unreadable index, or a read that fails, makes the count unknown, never
	// zero (§10.6.5).
	perName := t.disownPerName(set)
	var lined []ID          // the index's lines of the kinds set covers
	var unreadable []string // the objects of those that couldn't be read, with why
	var texts []string      // and the errors
	if len(perName) > 0 {
		named := map[ID]bool{}
		for i := range holders {
			if r := ReadRecord(&holders[i]); !r.Unparseable {
				for _, id := range r.IDs {
					if perName[id.GroupKind()] {
						named[id] = true
					}
				}
			}
		}
		cur, err := t.idx.store.get(ctx)
		if err != nil {
			known = false
			t.reportDisownFailed(ctx, "read the index "+t.idx.ref(), err, "grant get on it", "")
		}
		vouched := t.idx.parseVouched(cur.vouched)
		for _, id := range t.idx.ids(cur.lines) {
			if perName[id.GroupKind()] {
				named[id] = true
				lined = append(lined, id)
			}
		}
		live := s.newServer()
		for _, id := range SortedIDs(keysOf(named)) {
			m, err := live.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
			switch Classify(err) {
			case ClassNone:
			case ClassNotFound:
				continue
			default:
				if vouched[id] {
					// An operator checked by hand that it carries no
					// finalizer of the kind (§7.4): skipped while it can't
					// be read.
					continue
				}
				// It may carry the kind's finalizer: the count is unknown,
				// never zero, until it can be read (§7.4).
				known = false
				unreadable = append(unreadable, unheldRef(id)+": "+shortError(err))
				texts = append(texts, err.Error())
				continue
			}
			n := countPrefix(m.Finalizers, t.prefix)
			if n > 0 && vouched[id] {
				t.idx.vouchWrong(id) // readable, and held: counted as any other
			}
			count += n
			objs = append(objs, object{id.GroupKind(), m, n})
		}
	}

	srv := s.newServer()
	for i := range holders {
		h := &holders[i]
		patch, ok := disownHolderPatch(h, set)
		if !ok {
			continue
		}
		if _, err := srv.Patch(ctx, t.gk, h.Namespace, h.Name, types.MergePatchType, patch); err != nil && Classify(err) != ClassNotFound {
			t.warn("credential-use: Disown couldn't strip a holder's record; the next pass retries", "holder", h.Namespace+"/"+h.Name, "error", err.Error())
		}
	}
	for _, o := range objs {
		_, removed, err := stripDisowned(ctx, srv, o.gk, t.gk, o.m)
		count += max(0, removed-o.listed)
		if err != nil {
			t.warn("credential-use: Disown couldn't strip a protected object; the next pass retries",
				"object", unheldRef(ID{Group: o.gk.Group, Kind: o.gk.Kind, Namespace: o.m.Namespace, Name: o.m.Name}), "error", err.Error())
		}
	}
	if len(lined) > 0 {
		// Disown empties the index of the kinds it covers (§7.4), under the
		// prune's rule (§10.6.2): every pruned object is read again, and
		// keeps its line if it carries the kind's prefix, as an A2 from a
		// pass begun before the switch may have put back after the strip.
		// That line is the next pass's count. A line added since the read
		// above stays, and is counted by the next pass too.
		if err := t.idx.prune(ctx, lined); err != nil {
			t.warn("credential-use: Disown couldn't empty the index; the next pass retries", "index", t.idx.ref(), "error", err.Error())
		}
	}
	if len(perName) > 0 {
		t.reportDisownUnreadable(ctx, unreadable, texts)
	}
	s.writeDisownCount(ctx, t, set, count, known)
}

// EventDisownUnreadable (Warning, on the holder kind's CRD): Disown can't
// read its index, or objects that or the records name, or can't list the
// holders or a LabelScoped kind, so its count stays "unknown" (§7.4).
const EventDisownUnreadable = "CredentialUseDisownUnreadable"

// reportDisownUnreadable reports the objects a Disown pass couldn't read:
// the gauge on every pass, and, when there are some, a log line and a
// Warning event on the CRD naming each with why, which the event sink
// throttles while the set stays the same. texts are their errors, for the log:
// an event with them would be a new one at every pass whose errors vary. A pass
// the process's stop cut short says nothing, and leaves the gauge: its reads
// failed for that.
func (t *Tracker) reportDisownUnreadable(ctx context.Context, unreadable, texts []string) {
	if ctx.Err() != nil {
		return
	}
	sinkMetrics{t}.DisownUnreadable(kindLabel(t.gk), len(unreadable))
	if len(unreadable) == 0 {
		return
	}
	note := "Disown can't read " + strings.Join(unreadable, ", ") + ", so held-count stays unknown: grant get on each, " +
		"or confirm by hand that it carries no finalizer of " + kindLabel(t.gk) + " and add it to the " + AnnotationVouched +
		" annotation of the index " + t.idx.ref() + " (§7.4)"
	crd := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: t.crdName}}
	crd.SetGroupVersionKind(crdGVR.GroupVersion().WithKind("CustomResourceDefinition"))
	t.s.events.record(crd, "Warning", EventDisownUnreadable, note)
	t.warn("credential-use: Disown can't read objects its index or the records name; its count stays unknown",
		"kind", t.gk.String(), "crd", t.crdName, "unreadable", strings.Join(unreadable, ", "), "errors", strings.Join(texts, "; "))
}

// notServed reports whether err is a 404 that isn't about an object: its kind
// isn't served.
func notServed(err error) bool {
	st, ok := statusOf(err)
	return ok && apierrors.IsNotFound(err) && !namesObject(st)
}

// shortError is err as an event names it: its class, as "forbidden", never
// its text, which may be another one each time.
func shortError(err error) string {
	if notServed(err) {
		return "its kind isn't served" // the reason would say "notfound"
	}
	if r := apierrors.ReasonForError(err); r != metav1.StatusReasonUnknown {
		return strings.ToLower(string(r))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "error"
}

// disownPerName returns t's PerName kinds that set covers.
func (t *Tracker) disownPerName(set disownSet) map[schema.GroupKind]bool {
	out := map[schema.GroupKind]bool{}
	for _, gk := range disownedKinds(t, set) {
		if t.isPerName(gk) {
			out[gk] = true
		}
	}
	return out
}

// disownedKinds are t's protected kinds that set covers.
func disownedKinds(t *Tracker, set disownSet) []schema.GroupKind {
	var out []schema.GroupKind
	for _, pk := range t.o.Protected {
		if gk := pk.GVK.GroupKind(); set.covers(gk) {
			out = append(out, gk)
		}
	}
	return out
}

// recordsDisowned reports whether holder h's record still names a ref of a
// kind set covers; with "all", whether it has a record at all, an
// unparseable one included.
func recordsDisowned(h *metav1.PartialObjectMetadata, set disownSet) bool {
	rec := ReadRecord(h)
	if !rec.Present {
		return false
	}
	if set.all || rec.Unparseable {
		return true
	}
	return slices.ContainsFunc(rec.IDs, func(id ID) bool { return set.covers(id.GroupKind()) })
}

// disownHolderPatch is the merge patch that strips set's refs from holder h:
// its record without them (none at all for "all", an unparseable record
// included) and, for "all", its unheld list; guarded by h's resourceVersion.
// It reports false when h has nothing to strip.
func disownHolderPatch(h *metav1.PartialObjectMetadata, set disownSet) ([]byte, bool) {
	rec := ReadRecord(h)
	_, hasUnheld := h.Annotations[AnnotationUnheld]
	annotations := map[string]any{}
	if rec.Present {
		keep := slices.DeleteFunc(slices.Clone(rec.IDs), func(id ID) bool { return set.covers(id.GroupKind()) })
		switch {
		case set.all || rec.Unparseable || len(keep) == 0:
			annotations[AnnotationRefs] = nil
		case len(keep) != len(rec.IDs):
			v, err := EncodeRecord(keep)
			if err != nil {
				return nil, false
			}
			annotations[AnnotationRefs] = v
		}
	}
	if set.all && hasUnheld {
		annotations[AnnotationUnheld] = nil
	}
	if len(annotations) == 0 {
		return nil, false
	}
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": h.ResourceVersion,
		"annotations":     annotations,
	}})
	return data, err == nil
}

// writeDisownCount writes the count on t's CRD (§7.4, §7.3): held-count,
// "unknown" when the count couldn't be made, and disown-settled once this
// process has counted zero DisownPasses passes in a row.
//
// Each attempt is a GET, then a merge patch guarded by the resourceVersion
// it read; a conflict reads and decides again, at most maxCRDWriteAttempts
// times. A non-zero or unknown count read on the CRD, from another process
// or this one's earlier pass, starts the run of zeros over, so settled never
// lands over a newer non-zero count. A switch that no longer covers what the
// pass counted writes nothing, and starts the run over: the count is another
// switch's. The fields are written only when they change, since a CRD write
// rebuilds the API server's OpenAPI. A pass whose write conflicted on every
// attempt leaves it to the next; three in a row are logged and counted as
// starved.
func (s *Shared) writeDisownCount(ctx context.Context, t *Tracker, set disownSet, count int, known bool) {
	cur := "unknown"
	if known {
		cur = strconv.Itoa(count)
	}
	t.disown.mu.Lock()
	if cur == "0" {
		t.disown.zeros++
	} else {
		t.disown.zeros = 0
	}
	t.disown.mu.Unlock()
	crds := s.c.Dynamic.Resource(crdGVR)
	var last error
	for range maxCRDWriteAttempts {
		crd, err := crds.Get(ctx, t.crdName, metav1.GetOptions{})
		if err != nil {
			t.warn("credential-use: Disown couldn't read the CRD to write its count", "crd", t.crdName, "error", err.Error())
			return
		}
		ann := crd.GetAnnotations()
		if parseDisown(ann[AnnotationDisown]).key() != set.key() {
			t.disown.mu.Lock()
			t.disown.zeros = 0
			t.disown.mu.Unlock()
			return
		}
		prev := ann[AnnotationHeldCount]
		t.disown.mu.Lock()
		if prev != "" && prev != "0" {
			// Someone counted a hold: this pass's zero, if it is one, is
			// the first of a new run.
			t.disown.zeros = min(t.disown.zeros, 1)
			if cur != "0" {
				t.disown.zeros = 0
			}
		}
		settled := cur == "0" && t.disown.zeros >= t.o.DisownPasses
		t.disown.mu.Unlock()
		if prev == cur && (ann[AnnotationDisownSettled] == "true") == settled {
			s.disownWritten(t)
			return
		}
		var settledValue any // null removes it: a non-zero or unknown count clears the flag
		if settled {
			settledValue = "true"
		}
		data, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": crd.GetResourceVersion(),
			"annotations":     map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: settledValue},
		}})
		if err != nil {
			return
		}
		_, err = crds.Patch(ctx, t.crdName, types.MergePatchType, data, metav1.PatchOptions{})
		if err == nil {
			s.disownWritten(t)
			return
		}
		if Classify(err) != ClassConflict {
			t.warn("credential-use: Disown couldn't write its count on the CRD; the next pass writes it", "crd", t.crdName, "error", err.Error())
			return
		}
		last = err
	}
	t.disown.mu.Lock()
	t.disown.starved++
	starved := t.disown.starved >= 3
	if starved {
		t.disown.starved = 0
	}
	t.disown.mu.Unlock()
	if starved {
		sinkMetrics{t}.CRDWriteStarved(kindLabel(t.gk))
		t.warn("credential-use: Disown's count write conflicted on every attempt, three passes in a row; the next pass writes it",
			"crd", t.crdName, "attempts", maxCRDWriteAttempts, "lastConflict", last.Error())
	}
}

// disownWritten records a count write that landed, or had nothing to change.
func (s *Shared) disownWritten(t *Tracker) {
	t.disown.mu.Lock()
	t.disown.starved = 0
	t.disown.mu.Unlock()
}

// crdHandler enqueues every holder of t's kind when the Disown switch
// changes (§7.4): a holder's next pass then reads it, and stops holding the
// kinds it covers, or, once it is removed, holds them again.
type crdHandler struct{ t *Tracker }

func (crdHandler) OnAdd(any, bool) {}
func (crdHandler) OnDelete(any)    {}
func (h crdHandler) OnUpdate(oldObj, newObj any) {
	o, ok1 := metadataOf(oldObj)
	n, ok2 := metadataOf(newObj)
	if !ok1 || !ok2 || o.Annotations[AnnotationDisown] == n.Annotations[AnnotationDisown] {
		return
	}
	for _, obj := range h.t.holders.GetStore().List() {
		if m, ok := metadataOf(obj); ok {
			h.t.enqueue(types.NamespacedName{Namespace: m.Namespace, Name: m.Name})
		}
	}
}

// disownOff runs for a tracker whose switch is off: it forgets this
// process's run of zero passes, and removes held-count and disown-settled
// from the CRD if a switch left them. A settled flag must not outlive its
// switch: once the switch is gone the kind is protected again, so the flag
// would say an uninstall is safe when it isn't.
//
// The clearing is tried at most once per DisownInterval, and the first pass
// after the switch is set again runs at once.
func (s *Shared) disownOff(ctx context.Context, t *Tracker, now time.Time) {
	t.disown.mu.Lock()
	t.disown.zeros, t.disown.value = 0, ""
	t.disown.mu.Unlock()
	obj, ok, err := t.crd.GetStore().GetByKey(t.crdName)
	if err != nil || !ok {
		return
	}
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	_, hasCount := m.Annotations[AnnotationHeldCount]
	_, hasSettled := m.Annotations[AnnotationDisownSettled]
	if !hasCount && !hasSettled {
		t.disown.mu.Lock()
		t.disown.next, t.disown.cleared = time.Time{}, time.Time{}
		t.disown.mu.Unlock()
		return
	}
	t.disown.mu.Lock()
	due := !now.Before(t.disown.cleared)
	if due {
		t.disown.cleared = now.Add(t.o.DisownInterval)
	}
	t.disown.next = time.Time{}
	t.disown.mu.Unlock()
	if !due {
		return
	}
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": m.ResourceVersion,
		"annotations":     map[string]any{AnnotationHeldCount: nil, AnnotationDisownSettled: nil},
	}})
	if err != nil {
		return
	}
	// Guarded by the version the informer has: one that set the switch
	// again since conflicts, and the next look reads it.
	if _, err := s.c.Dynamic.Resource(crdGVR).Patch(ctx, t.crdName, types.MergePatchType, data, metav1.PatchOptions{}); err != nil && Classify(err) != ClassConflict {
		t.warn("credential-use: couldn't clear Disown's count from the CRD after the switch was removed", "crd", t.crdName, "error", err.Error())
	}
}

// reportDisownFailed reports something a Disown pass needs as a whole and
// couldn't read, which keeps the count unknown: a log line, and the Warning
// event the objects of an indexed kind get, saying what, the failure's class,
// and what to do about that class. An error the process's own stop caused isn't
// one.
func (t *Tracker) reportDisownFailed(ctx context.Context, what string, err error, grant, notServedRemedy string) {
	if ctx.Err() != nil {
		return
	}
	remedy := "the next pass retries"
	switch {
	case apierrors.IsForbidden(err):
		remedy = grant
	case notServed(err) && notServedRemedy != "":
		remedy = notServedRemedy + " (§6.2)"
	}
	crd := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: t.crdName}}
	crd.SetGroupVersionKind(crdGVR.GroupVersion().WithKind("CustomResourceDefinition"))
	t.s.events.record(crd, "Warning", EventDisownUnreadable, "Disown can't "+what+" ("+shortError(err)+"), so held-count stays unknown: "+remedy)
	t.warn("credential-use: Disown can't read what it counts; its count stays unknown until it can (§7.4)",
		"kind", t.gk.String(), "what", what, "why", shortError(err), "error", err.Error())
}
