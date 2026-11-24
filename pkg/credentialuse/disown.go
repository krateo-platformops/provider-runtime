package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
func (t *Tracker) disownSwitch() disownSet { return parseDisown(t.crdCached()[AnnotationDisown]) }

// crdCached is the CRD's annotations as t's informer has them, none while it
// hasn't the CRD.
func (t *Tracker) crdCached() map[string]string {
	if t.crd == nil {
		return nil
	}
	obj, ok, err := t.crd.GetStore().GetByKey(t.crdName)
	if err != nil || !ok {
		return nil
	}
	m, ok := metadataOf(obj)
	if !ok {
		return nil
	}
	return m.Annotations
}

// crdLive is the CRD's annotations, read live: none when it is gone.
func (t *Tracker) crdLive(ctx context.Context) (map[string]string, error) {
	m, err := t.s.c.Metadata.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{})
	if Classify(err) == ClassNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return m.Annotations, nil
}

// DisownSettled reports whether a holder kind's CRD, whose annotations are
// ann, says that Disown has settled under the switch as it is now: the
// switch is set and not empty, disown-settled is its value, as written, and
// held-count is "0". A flag left by another value of the switch, or under
// none, isn't it (§7.4; the user's decision, 2026-11-23). A reader outside Go
// compares the same three.
func DisownSettled(ann map[string]string) bool {
	v := ann[AnnotationDisown]
	return v != "" && ann[AnnotationDisownSettled] == v && ann[AnnotationHeldCount] == "0"
}

// staleSettled reports whether ann carries a disown-settled of another value
// of the switch than it has now, or of one that is gone.
func staleSettled(ann map[string]string) bool {
	f, ok := ann[AnnotationDisownSettled]
	return ok && f != ann[AnnotationDisown]
}

// keptKinds are the kinds whose index lines stay under the CRD's
// annotations ann: those the switch covers while it hasn't settled (§7.4).
func keptKinds(ann map[string]string) disownSet {
	if DisownSettled(ann) {
		return disownSet{}
	}
	return parseDisown(ann[AnnotationDisown])
}

// disownKeeps is keptKinds, read live.
func (t *Tracker) disownKeeps(ctx context.Context) (disownSet, error) {
	ann, err := t.crdLive(ctx)
	return keptKinds(ann), err
}

// disownLive reads the switch live, from the CRD itself. A CRD that is gone
// has no switch.
func (t *Tracker) disownLive(ctx context.Context) (disownSet, error) {
	ann, err := t.crdLive(ctx)
	return parseDisown(ann[AnnotationDisown]), err
}

// disownForHold is disownLive for a pass's check before its first A2
// (§10.6.8, interleaving 3; the user's decisions, 2026-11-22): a stale CRD
// informer, whose watch fails, can't let an A2 through after the switch.
//
// It also removes a disown-settled that a switch of another value left,
// guarded by the version it read: a hold made now must not stand under a
// flag that says nothing is held, should that value come back (§7.4; the
// user's decision, 2026-11-23). One it can't remove fails the pass: no A2.
func (t *Tracker) disownForHold(ctx context.Context) (disownSet, error) {
	m, err := t.s.c.Metadata.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{})
	if Classify(err) == ClassNotFound {
		return disownSet{}, nil
	}
	if err != nil {
		return disownSet{}, err
	}
	if staleSettled(m.Annotations) {
		data, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": m.ResourceVersion,
			"annotations":     map[string]any{AnnotationDisownSettled: nil},
		}})
		if err != nil {
			return disownSet{}, err
		}
		_, err = t.s.c.Dynamic.Resource(crdGVR).Patch(ctx, t.crdName, types.MergePatchType, data, metav1.PatchOptions{})
		switch Classify(err) {
		case ClassNone:
		case ClassRBAC:
			return disownSet{}, fmt.Errorf("removing the leftover %s from CRD %s, without which nothing may be held: grant patch on customresourcedefinitions %s (§7.3): %w",
				AnnotationDisownSettled, t.crdName, t.crdName, err)
		default:
			return disownSet{}, fmt.Errorf("removing the leftover %s from CRD %s, without which nothing may be held: %w", AnnotationDisownSettled, t.crdName, err)
		}
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
	// startRV is the CRD's version read as this pass starts, and rv the one
	// at the end of the pass before: a run of zeros is of passes over a CRD
	// nobody changed, so that no switch turned off and on again in between
	// goes unseen, whatever the informer delivered (§7.4).
	startRV, rv string
	// began is when this pass started. changed: the CRD changed under this
	// process's run, which starts over. restarts counts those in a row, and
	// wanting is when the first zero pass of them was; writers are the CRD's
	// newest at each, the last first; reported, when they were last reported
	// (EventDisownRestarted).
	began, wanting, reported time.Time
	changed                  bool
	restarts                 int
	writers                  []crdWriter
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
		for _, t := range s.trackerList() {
			set := t.disownSwitch()
			// After the switch is read, and for each tracker: the passes of
			// a run span its intervals from a time no earlier than the
			// switch, however long other trackers' passes take.
			now := s.clock.Now()
			if !set.on() {
				s.disownOff(ctx, t, now)
				continue
			}
			t.disown.mu.Lock()
			if k := set.key(); t.disown.value != k {
				t.disown.value, t.disown.zeros, t.disown.next = k, 0, time.Time{}
				t.disown.endRestarts()
			}
			due := !now.Before(t.disown.next)
			t.disown.mu.Unlock()
			if due {
				s.disownStarts(ctx, t)
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

// disownStarts reads the CRD's version as a pass of t starts, and then when
// the next is due: the passes of a run are an interval apart from a time no
// earlier than the version they run over.
func (s *Shared) disownStarts(ctx context.Context, t *Tracker) {
	rv := unreadRV
	if crd, err := s.c.Dynamic.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{}); err == nil {
		rv = crd.GetResourceVersion()
	}
	now := s.clock.Now()
	t.disown.mu.Lock()
	defer t.disown.mu.Unlock()
	t.disown.next = now.Add(t.o.DisownInterval)
	t.disown.startRV, t.disown.began = rv, now
}

// unreadRV is the version of a CRD that couldn't be read: no version is it,
// so the pass is of no run.
const unreadRV = "unread"

// decide is what a pass that counted cur does with the CRD it read, with
// annotations ann and version rv: whether this process has settled, in
// passes zero passes in a row, and whether to write. t.disown.mu is held.
//
// A process whose own run isn't done leaves the flag of one whose run is, if
// it is of the switch's value now: it clears it only with a count to write,
// or when it is another value's.
func (d *disownState) decide(ann map[string]string, rv, cur string, passes int) (settled, write bool) {
	prev, flag := ann[AnnotationHeldCount], ann[AnnotationDisownSettled]
	if prev != "" && prev != "0" {
		// Someone counted a hold: this pass's zero, if it is one, is
		// the first of a new run.
		d.zeros = min(d.zeros, 1)
		if cur != "0" {
			d.zeros = 0
		}
	}
	d.changed = false
	if d.zeros > 1 && rv != d.rv {
		d.zeros, d.changed = 1, true // the CRD changed since the pass before
	}
	if d.startRV != "" && rv != d.startRV {
		d.changed = d.changed || d.zeros > 0
		d.zeros = 0 // it changed during this one
	}
	settled = cur == "0" && d.zeros >= passes
	switch {
	case prev != cur:
		write = true
	case cur == "0" && settled:
		write = flag != ann[AnnotationDisown]
	case cur == "0":
		write = staleSettled(ann)
	default:
		write = flag != ""
	}
	return settled, write
}

// disownPass is one pass of t's Disown loop (§7.4): it lists, live and
// cluster-wide, the holders of t's kind and the protected objects of the
// kinds the switch covers by t's marker label, so that a namespace outside
// SharedOptions.Namespaces is reached too, or, for a PerName kind, reads each
// object its index or a record names; counts what they hold; strips it;
// writes the count on the CRD; and, once it has settled, empties the index
// of those kinds.
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
	holders, err := s.listHolders(ctx, t)
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
	var vouched map[ID]bool
	unlined := map[ID]bool{}    // recorded objects whose line couldn't be added
	unstripped := map[ID]bool{} // objects that couldn't be read, or stripped
	if len(perName) > 0 {
		// The live checks due are made first, so that one a drop queued a
		// moment ago doesn't make the count unknown.
		if err := t.idx.recheckTick(ctx, s.clock.Now()); err != nil {
			known = false
		}
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
		cur, sure, err := t.idx.readSure(ctx, perName)
		lost := Classify(err) == ClassNotFound || err == nil && !cur.complete()
		if err != nil || lost {
			known = false
		}
		switch {
		case lost && !t.idx.isFresh():
			t.reportIndexLost(cur)
		case err != nil:
			t.reportDisownFailed(ctx, "read the index "+t.idx.ref(), err, "grant get on it", "")
		}
		// A line whose check or re-add this process still owes may be
		// missing from what was read (§7.4).
		if !sure {
			known = false
		}
		vouched = t.idx.parseVouched(cur.vouched)
		for _, id := range t.idx.ids(cur.lines) {
			if perName[id.GroupKind()] {
				named[id] = true
				lined = append(lined, id)
			}
		}
		live := s.newServer(t.o.Holder)
		skipped := map[ID]bool{}
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
					skipped[id] = true
					continue
				}
				// It may carry the kind's finalizer: the count is unknown,
				// never zero, until it can be read (§7.4).
				unstripped[id] = true
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
		// The lines of the objects the records name are added before the
		// records are stripped: A1 comes before A2, so an A2 that lands after
		// the strip has its object's line there for a later pass's count,
		// even one a prune took before the switch. A vouched object that
		// can't be read is the one the operator answers for.
		add := slices.DeleteFunc(SortedIDs(keysOf(named)), func(id ID) bool { return slices.Contains(lined, id) || skipped[id] })
		if len(add) > 0 {
			if err := t.idx.add(ctx, add); err != nil {
				known = false
				for _, id := range add {
					unlined[id] = true
				}
				t.reportDisownUnlined(err)
			}
		}
	}

	// The objects are stripped before the records, and a record only once
	// every object it names has been: while one may be held, a record names
	// it, so that a pass cut short between the two, in which a prune may have
	// taken its line meanwhile, leaves it to the next (§7.4).
	srv := s.newServer(t.o.Holder)
	for _, o := range objs {
		_, removed, err := stripDisowned(ctx, srv, o.gk, t.gk, o.m)
		count += max(0, removed-o.listed)
		if err != nil {
			id := ID{Group: o.gk.Group, Kind: o.gk.Kind, Namespace: o.m.Namespace, Name: o.m.Name}
			unstripped[id] = true
			t.warn("credential-use: Disown couldn't strip a protected object; the next pass retries",
				"object", unheldRef(id), "error", err.Error())
		}
	}
	for i := range holders {
		h := &holders[i]
		patch, ok := disownHolderPatch(h, set)
		if !ok {
			continue
		}
		if slices.ContainsFunc(ReadRecord(h).IDs, func(id ID) bool { return unlined[id] || unstripped[id] }) {
			continue // its record may be all that names them
		}
		if _, err := srv.Patch(ctx, t.gk, h.Namespace, h.Name, types.MergePatchType, patch); err != nil && Classify(err) != ClassNotFound {
			t.warn("credential-use: Disown couldn't strip a holder's record; the next pass retries", "holder", h.Namespace+"/"+h.Name, "error", err.Error())
		}
	}
	if len(perName) > 0 {
		t.reportDisownUnreadable(ctx, unreadable, texts)
	}
	if !s.writeDisownCount(ctx, t, set, count, known) || len(lined) == 0 {
		return
	}
	// Disown empties the index of the kinds it covers once this process has
	// settled, and not before (§7.4; the user's decision, 2026-11-23): while
	// a hold can still land, its line is what the count finds it by. Even
	// then it removes only the lines it proves free in this pass, after its
	// strip, and each is read once more after the removal (§10.6.2). A line
	// added since the read above stays, and is the next pass's.
	if err := t.idx.empty(ctx, t.freeLines(ctx, srv, lined, unstripped, vouched)); err != nil {
		t.warn("credential-use: Disown couldn't empty the index; the next pass retries", "index", t.idx.ref(), "error", err.Error())
	}
}

// freeLines returns those of lined that a live read proves free: gone, or
// without a finalizer of the kind, or vouched for by an operator and not
// readable. One whose strip failed in this pass isn't.
func (t *Tracker) freeLines(ctx context.Context, srv server, lined []ID, unstripped, vouched map[ID]bool) []ID {
	var free []ID
	for _, id := range lined {
		if unstripped[id] {
			continue
		}
		m, err := srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
		switch Classify(err) {
		case ClassNone:
			if countPrefix(m.Finalizers, t.prefix) > 0 {
				continue
			}
		case ClassNotFound:
		default:
			if !vouched[id] {
				continue
			}
		}
		free = append(free, id)
	}
	return free
}

// EventDisownIndexFull (Warning, on the holder kind's CRD): the index can't
// take the lines of the objects the records name, so Disown doesn't strip
// those records, and its count stays "unknown" (§7.4).
const EventDisownIndexFull = "CredentialUseDisownIndexFull"

// reportDisownUnlined reports that the lines of recorded objects couldn't
// be added; on a full index, with the remedy, as an event on the CRD.
func (t *Tracker) reportDisownUnlined(err error) {
	t.warn("credential-use: Disown couldn't add the index lines of the objects the records name; it doesn't strip those records, and its count stays unknown",
		"kind", t.gk.String(), "index", t.idx.ref(), "error", err.Error())
	if !errors.Is(err, errIndexFull) {
		return
	}
	crd := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: t.crdName}}
	crd.SetGroupVersionKind(crdGVR.GroupVersion().WithKind("CustomResourceDefinition"))
	t.s.events.record(crd, "Warning", EventDisownIndexFull, "the index "+t.idx.ref()+
		" can't take the lines of the objects the records name, so Disown doesn't strip those records, and held-count stays unknown: raise SharedOptions.IndexMaxLines (§7.4): "+err.Error())
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

// EventIndexLost (Warning, on the holder kind's CRD): the index was lost
// while Disown is on, so its count stays "unknown" (§7.4).
const EventIndexLost = "CredentialUseIndexLost"

// reportIndexLost reports an index that is missing, or that no rebuild
// completed, while Disown is on: the records it would be rebuilt from may be
// stripped already, so only an operator can tell that nothing else is held.
// The event sink throttles it while cur is the same ConfigMap.
func (t *Tracker) reportIndexLost(cur indexVersion) {
	uid := cur.uid
	if uid == "" {
		uid = "<its metadata.uid, once it exists again>"
	}
	var kinds []string
	for gk := range t.idx.kinds {
		kinds = append(kinds, gk.String())
	}
	slices.Sort(kinds)
	note := "the index " + t.idx.ref() + " was lost, and Disown strips the records that named its holds, so held-count stays unknown: list the objects of " +
		strings.Join(kinds, ", ") + " labelled " + MarkerLabel(t.gk) + "=true, remove every finalizer that starts with " + t.prefix +
		" from them, then run: kubectl annotate configmap -n " + t.idx.namespace + " " + t.idx.name + " " + AnnotationVouchedIndex + "=" + uid + " (§7.4)"
	crd := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: t.crdName}}
	crd.SetGroupVersionKind(crdGVR.GroupVersion().WithKind("CustomResourceDefinition"))
	t.s.events.record(crd, "Warning", EventIndexLost, note)
	t.warn("credential-use: the index was lost while Disown is on; its count stays unknown until an operator vouches for the index",
		"kind", t.gk.String(), "crd", t.crdName, "index", t.idx.ref(), "uid", cur.uid)
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
// lands over a newer non-zero count, and so does any other change of the CRD.
// disown-settled is the switch's value, as written. A switch that no longer covers what the
// pass counted writes nothing, and starts the run over: the count is another
// switch's. The fields are written only when they change, since a CRD write
// rebuilds the API server's OpenAPI. A pass whose write conflicted on every
// attempt leaves it to the next; three in a row are logged and counted as
// starved. It reports whether this process has settled under set, and the
// CRD says so.
func (s *Shared) writeDisownCount(ctx context.Context, t *Tracker, set disownSet, count int, known bool) bool {
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
	for attempt := range maxCRDWriteAttempts {
		crd, err := crds.Get(ctx, t.crdName, metav1.GetOptions{})
		if err != nil {
			t.warn("credential-use: Disown couldn't read the CRD to write its count", "crd", t.crdName, "error", err.Error())
			return false
		}
		ann := crd.GetAnnotations()
		if parseDisown(ann[AnnotationDisown]).key() != set.key() {
			t.disown.mu.Lock()
			t.disown.zeros = 0
			t.disown.mu.Unlock()
			return false
		}
		t.disown.mu.Lock()
		settled, write := t.disown.decide(ann, crd.GetResourceVersion(), cur, t.o.DisownPasses)
		t.disown.mu.Unlock()
		if attempt == 0 {
			t.noteRestart(crd, cur)
		}
		if !write {
			s.disownWritten(t, crd.GetResourceVersion())
			return settled
		}
		var settledValue any // null removes it: a non-zero or unknown count clears the flag
		if settled {
			settledValue = ann[AnnotationDisown]
		}
		data, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": crd.GetResourceVersion(),
			"annotations":     map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: settledValue},
		}})
		if err != nil {
			return false
		}
		out, err := crds.Patch(ctx, t.crdName, types.MergePatchType, data, metav1.PatchOptions{})
		if err == nil {
			s.disownWritten(t, out.GetResourceVersion())
			return settled
		}
		if Classify(err) != ClassConflict {
			t.warn("credential-use: Disown couldn't write its count on the CRD; the next pass writes it", "crd", t.crdName, "error", err.Error())
			return false
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
	return false
}

// EventDisownRestarted (Warning, on the holder kind's CRD): the CRD keeps
// changing under Disown's runs of zero passes, each of which starts over, so
// it doesn't settle (§7.4).
const EventDisownRestarted = "CredentialUseDisownRestarted"

// noteRestart follows the runs of zero passes that start over because the
// CRD changed, after a pass that counted cur and read crd: each is counted
// in the metric, and when they have gone on for two spans of a run without
// one of them done, the CRD's newest writer at each of them is reported, the
// last three, at most once per eventThrottle: of two that write the same field
// over each other, managedFields keep only one at a time. A writer that comes
// in later is named by the next report. A count that isn't zero, this
// process's or another's, and a flag that stands, end them: then it isn't the
// CRD's changes that keep Disown from settling. The writes of the count and
// the flag themselves stop once every process counts the same, so they end
// too.
func (t *Tracker) noteRestart(crd *unstructured.Unstructured, cur string) {
	d, ann := &t.disown, crd.GetAnnotations()
	d.mu.Lock()
	prev := ann[AnnotationHeldCount]
	if cur != "0" || prev != "" && prev != "0" || DisownSettled(ann) || d.zeros >= t.o.DisownPasses {
		d.endRestarts()
		d.mu.Unlock()
		return
	}
	if d.wanting.IsZero() {
		d.wanting = d.began
	}
	changed := d.changed
	if changed {
		d.restarts++
		now := crdWriters(crd)
		d.writers = lastWriters(now[:min(1, len(now))], d.writers)
	}
	restarts, lasted := d.restarts, d.began.Sub(d.wanting)
	writers := writersText(d.writers)
	report := changed && lasted >= 2*time.Duration(t.o.DisownPasses-1)*t.o.DisownInterval &&
		(d.reported.IsZero() || d.began.Sub(d.reported) >= eventThrottle)
	if report {
		d.reported = d.began
	}
	d.mu.Unlock()
	if changed {
		sinkMetrics{t}.DisownRestarts(kindLabel(t.gk))
	}
	if !report {
		return
	}
	obj := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: t.crdName}}
	obj.SetGroupVersionKind(crdGVR.GroupVersion().WithKind("CustomResourceDefinition"))
	t.s.events.record(obj, "Warning", EventDisownRestarted, fmt.Sprintf(
		"Disown counts nothing held, and doesn't settle: the CRD changed under it %d times in %s, and each change starts its run over (§7.4). The newest writers then: %s",
		restarts, lasted.Round(time.Second), writers))
	t.warn("credential-use: Disown counts nothing held, and doesn't settle: the CRD keeps changing, and each change starts its run over",
		"kind", t.gk.String(), "crd", t.crdName, "restarts", restarts, "in", lasted.String(), "writers", writers)
}

// endRestarts forgets the runs that started over. d.mu is held.
func (d *disownState) endRestarts() {
	d.restarts, d.wanting, d.writers = 0, time.Time{}, nil
}

// A crdWriter is a writer of the CRD as its managedFields name it.
type crdWriter struct{ manager, text string }

// maxWriters is how many writers a report names.
const maxWriters = 3

// lastWriters is newer and then older, one entry per manager, the first,
// maxWriters at most.
func lastWriters(newer, older []crdWriter) []crdWriter {
	var out []crdWriter
	for _, w := range slices.Concat(newer, older) {
		if len(out) < maxWriters && !slices.ContainsFunc(out, func(o crdWriter) bool { return o.manager == w.manager }) {
			out = append(out, w)
		}
	}
	return out
}

// writersText names ws.
func writersText(ws []crdWriter) string {
	if len(ws) == 0 {
		return "none in its managedFields"
	}
	var out []string
	for _, w := range ws {
		out = append(out, w.text)
	}
	return strings.Join(out, "; ")
}

// crdWriters are the writers in crd's managedFields, newest first, other than
// of the count and the flag alone. managedFields keep the last writer of each
// field only.
func crdWriters(crd *unstructured.Unstructured) []crdWriter {
	entries := crd.GetManagedFields()
	entries = slices.DeleteFunc(entries, func(e metav1.ManagedFieldsEntry) bool { return ownsOnlyDisownFields(e) })
	slices.SortStableFunc(entries, func(a, b metav1.ManagedFieldsEntry) int {
		at, bt := time.Time{}, time.Time{}
		if a.Time != nil {
			at = a.Time.Time
		}
		if b.Time != nil {
			bt = b.Time.Time
		}
		return bt.Compare(at)
	})
	var out []crdWriter
	for _, e := range entries {
		w := e.Manager + " (" + string(e.Operation)
		if e.Subresource != "" {
			w += " of " + e.Subresource
		}
		if e.Time != nil {
			w += ", " + e.Time.UTC().Format(time.RFC3339)
		}
		out = append(out, crdWriter{e.Manager, w + ")"})
	}
	return out
}

// ownsOnlyDisownFields reports whether e owns nothing but annotations of the
// credential-use domain. One whose fields can't be read owns more.
func ownsOnlyDisownFields(e metav1.ManagedFieldsEntry) bool {
	if e.FieldsV1 == nil {
		return false
	}
	var fields map[string]map[string]map[string]any
	if json.Unmarshal(e.FieldsV1.GetRawBytes(), &fields) != nil || len(fields) != 1 || len(fields["f:metadata"]) != 1 {
		return false
	}
	n := 0
	for k := range fields["f:metadata"]["f:annotations"] {
		switch {
		case k == ".":
		case strings.HasPrefix(k, "f:"+Domain+"/"):
			n++
		default:
			return false
		}
	}
	return n > 0
}

// disownWritten records a count write that landed, or had nothing to change,
// leaving the CRD at version rv.
func (s *Shared) disownWritten(t *Tracker, rv string) {
	t.disown.mu.Lock()
	t.disown.starved, t.disown.rv = 0, rv
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
	// The run starts over, also for a switch turned off and on again between
	// two looks of the Disown loop: passes between may have held again.
	h.t.disown.mu.Lock()
	h.t.disown.value, h.t.disown.zeros = "", 0
	h.t.disown.mu.Unlock()
	if h.t.idx != nil {
		h.t.idx.switchChanged()
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
