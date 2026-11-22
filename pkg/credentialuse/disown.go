package credentialuse

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
		switch {
		case s == "":
		case s == "all":
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

// disownState is one tracker's progress towards disown-settled (§7.4).
type disownState struct {
	mu    sync.Mutex
	zeros int       // consecutive passes this process counted zero
	next  time.Time // when the next pass is due
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
// SharedOptions.Namespaces is reached too; counts what they hold; strips it;
// and writes the count on the CRD.
//
// The count is what the pass found, before its strip: a hold that a pass
// begun before the switch put back after an earlier strip is counted, so it
// resets the settling (§10.6.8, interleaving 3), and a pass counts zero only
// when it found nothing to strip. A list that fails makes the count unknown,
// never zero. A strip that fails is left for the next pass, whose count
// shows it.
func (s *Shared) disownPass(ctx context.Context, t *Tracker, set disownSet) {
	known := true
	count := 0
	holders, err := s.liveList(ctx, t.gk, "", "")
	if err != nil {
		known = false
	}
	for i := range holders {
		if recordsDisowned(&holders[i], set) {
			count++
		}
	}
	type object struct {
		gk schema.GroupKind
		m  *metav1.PartialObjectMetadata
	}
	var objs []object
	for _, gk := range disownedKinds(t, set) {
		items, err := s.liveList(ctx, gk, "", MarkerLabel(t.gk))
		if err != nil {
			known = false
			continue
		}
		for i := range items {
			count += countPrefix(items[i].Finalizers, t.prefix)
			objs = append(objs, object{gk, &items[i]})
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
		if _, err := stripDisowned(ctx, srv, o.gk, t.gk, o.m); err != nil {
			t.warn("credential-use: Disown couldn't strip a protected object; the next pass retries",
				"object", unheldRef(ID{Group: o.gk.Group, Kind: o.gk.Kind, Namespace: o.m.Namespace, Name: o.m.Name}), "error", err.Error())
		}
	}
	s.writeDisownCount(ctx, t, count, known)
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

// writeDisownCount writes the count on t's CRD (§7.4): held-count, "unknown"
// when the count couldn't be made, and disown-settled once this process has
// counted zero DisownPasses passes in a row. A non-zero or unknown count read
// on the CRD, from another process or this one's earlier pass, resets the run
// of zeros, so settled never lands over a newer non-zero count; the write is
// a merge patch guarded by the CRD's resourceVersion, after the GET that read
// the previous values, and is made only when a value changes, since a CRD
// write rebuilds the API server's OpenAPI.
func (s *Shared) writeDisownCount(ctx context.Context, t *Tracker, count int, known bool) {
	crds := s.c.Dynamic.Resource(crdGVR)
	crd, err := crds.Get(ctx, t.crdName, metav1.GetOptions{})
	if err != nil {
		t.warn("credential-use: Disown couldn't read the CRD to write its count", "crd", t.crdName, "error", err.Error())
		return
	}
	ann := crd.GetAnnotations()
	prev := ann[AnnotationHeldCount]
	cur := "unknown"
	if known {
		cur = strconv.Itoa(count)
	}
	t.disown.mu.Lock()
	if prev != "" && prev != "0" {
		t.disown.zeros = 0
	}
	if cur == "0" {
		t.disown.zeros++
	} else {
		t.disown.zeros = 0
	}
	settled := cur == "0" && t.disown.zeros >= t.o.DisownPasses
	t.disown.mu.Unlock()
	if prev == cur && (ann[AnnotationDisownSettled] == "true") == settled {
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
	if _, err := crds.Patch(ctx, t.crdName, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		// A conflict means another process wrote first: the next pass reads
		// its values.
		t.warn("credential-use: Disown couldn't write its count on the CRD; the next pass writes it", "crd", t.crdName, "error", err.Error())
	}
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
	t.disown.zeros = 0
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
