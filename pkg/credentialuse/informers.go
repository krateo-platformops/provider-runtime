package credentialuse

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// The core's informers (§10.1, §10.2, §11.3). Each is metadata-only, on the
// core's own listWatch, with a transform that keeps only what the protocol
// reads. Their handlers never block and never call the API server: they
// update the verified sets, and enqueue release batches and holders.

// refsIndex indexes holders by the IDs their record names; unparseableKey
// is where a record that can't be parsed goes, since it names every ID for
// R0 and R2 (§4).
const (
	refsIndex      = "credential-use-refs"
	unparseableKey = "\x00unparseable"
)

// prefixOf splits finalizer f of a kind with prefix into its holder
// namespace, and reports whether f is that kind's.
func namespaceOfFinalizer(f, prefix string) (string, bool) {
	part, ok := strings.CutPrefix(f, prefix)
	if !ok || part == "" {
		return "", false
	}
	if part == ClusterScopedName {
		return "", true
	}
	return part, true
}

// metadataOf returns obj's metadata, unwrapping a deletion tombstone.
func metadataOf(obj any) (*metav1.PartialObjectMetadata, bool) {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	m, ok := obj.(*metav1.PartialObjectMetadata)
	return m, ok
}

// keepOnly returns a copy of m restricted to the keys keep accepts.
func keepOnly(m map[string]string, keep func(string) bool) map[string]string {
	var out map[string]string
	for k, v := range m {
		if keep(k) {
			if out == nil {
				out = map[string]string{}
			}
			out[k] = v
		}
	}
	return out
}

func isCredentialUseKey(k string) bool { return strings.HasPrefix(k, "credential-use.") }

// trimmed is m with only what the core keeps, as the transforms leave it:
// name, namespace, UID, resourceVersion, generation, deletionTimestamp,
// finalizers, and the labels and annotations keep accepts.
func trimmed(m *metav1.PartialObjectMetadata, keepLabel, keepAnnotation func(string) bool) *metav1.PartialObjectMetadata {
	out := &metav1.PartialObjectMetadata{TypeMeta: m.TypeMeta}
	out.Name, out.Namespace, out.UID = m.Name, m.Namespace, m.UID
	out.ResourceVersion, out.Generation = m.ResourceVersion, m.Generation
	out.DeletionTimestamp = m.DeletionTimestamp
	out.Finalizers = slices.Clone(m.Finalizers)
	if keepLabel != nil {
		out.Labels = keepOnly(m.Labels, keepLabel)
	}
	if keepAnnotation != nil {
		out.Annotations = keepOnly(m.Annotations, keepAnnotation)
	}
	return out
}

func transform(keepLabel, keepAnnotation func(string) bool) cache.TransformFunc {
	return func(obj any) (any, error) {
		if m, ok := obj.(*metav1.PartialObjectMetadata); ok {
			return trimmed(m, keepLabel, keepAnnotation), nil
		}
		return obj, nil
	}
}

// --- the holder informer: one per tracker (§10.2) ---

func holderIndexer(obj any) ([]string, error) {
	m, ok := metadataOf(obj)
	if !ok {
		return nil, nil
	}
	r := ReadRecord(m)
	if r.Unparseable {
		return []string{unparseableKey}, nil
	}
	keys := make([]string, 0, len(r.IDs))
	for _, id := range r.IDs {
		keys = append(keys, id.String())
	}
	return keys, nil
}

// holderHandler keeps tracker t's verified set (§6.3) and maps holder events
// to release batches (§10.2): a create, an update whose record or owner
// finalizer changed, and a delete, enqueue the (K, N) batch of every
// protected object in the old and the new record.
type holderHandler struct{ t *Tracker }

func (h holderHandler) now() time.Time { return h.t.s.clock.Now() }

func (h holderHandler) observe(m *metav1.PartialObjectMetadata) {
	if f := h.t.s.hooks.holderEvent; f != nil {
		f(h.t, m)
	}
}

func (h holderHandler) OnAdd(obj any, _ bool) {
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	h.observe(m)
	h.t.ver.holderEvent(m.UID, m, h.now())
	h.enqueue(nil, m)
}

func (h holderHandler) OnUpdate(oldObj, newObj any) {
	o, ok1 := metadataOf(oldObj)
	n, ok2 := metadataOf(newObj)
	if !ok1 || !ok2 {
		return
	}
	h.observe(n)
	at := h.now()
	if o.UID != n.UID {
		h.t.ver.holderEvent(o.UID, nil, at)
	}
	h.t.ver.holderEvent(n.UID, n, at)
	if o.UID != n.UID || o.Annotations[AnnotationRefs] != n.Annotations[AnnotationRefs] ||
		IsEstablished(o, h.t.o.OwnerFinalizers) != IsEstablished(n, h.t.o.OwnerFinalizers) ||
		(o.DeletionTimestamp == nil) != (n.DeletionTimestamp == nil) {
		h.enqueue(o, n)
	}
}

func (h holderHandler) OnDelete(obj any) {
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	h.t.ver.holderEvent(m.UID, nil, h.now())
	h.enqueue(m, nil)
}

// enqueue adds the batch of every ID in old's and cur's records.
func (h holderHandler) enqueue(old, cur *metav1.PartialObjectMetadata) {
	for _, m := range []*metav1.PartialObjectMetadata{old, cur} {
		if m == nil {
			continue
		}
		r := ReadRecord(m)
		if len(r.IDs) > 0 {
			h.t.s.queue.Add(batchKey{h.t.gk, m.Namespace}, r.IDs...)
		}
	}
}

// --- the protected informers: one per protected kind, shared (§10.1) ---

func heldSelector() string { return LabelHeld + "=true" }

// protectedInformer is one protected kind's label-scoped cache, shared by
// every tracker that may hold that kind. Each such tracker has its own
// handler on it (protectedHandler).
type protectedInformer struct {
	gk  schema.GroupKind
	gvr schema.GroupVersionResource
	lw  *listWatch
	inf cache.SharedIndexInformer
	// Under Shared.mu: claims counts the Tracks and trackers that use the
	// informer; runOwned, that RunInformers starts it; starting, that a
	// Track after Run is starting it, or has; stop ends it then.
	claims   int
	runOwned bool
	starting bool
	stop     context.CancelFunc
	// started is closed once a Track's start of it has a result, startErr.
	started  chan struct{}
	startErr error

	mu       sync.Mutex
	trackers []*Tracker
}

func (p *protectedInformer) add(t *Tracker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trackers = append(p.trackers, t)
}

func (p *protectedInformer) remove(t *Tracker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trackers = slices.DeleteFunc(p.trackers, func(x *Tracker) bool { return x == t })
}

func (p *protectedInformer) snapshot() []*Tracker {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.trackers)
}

func (p *protectedInformer) get(namespace, name string) (*metav1.PartialObjectMetadata, bool) {
	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}
	obj, ok, err := p.inf.GetStore().GetByKey(key)
	if err != nil || !ok {
		return nil, false
	}
	m, ok := obj.(*metav1.PartialObjectMetadata)
	return m, ok
}

// inNamespace returns the cached objects in namespace.
func (p *protectedInformer) inNamespace(namespace string) []*metav1.PartialObjectMetadata {
	objs, err := p.inf.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil
	}
	out := make([]*metav1.PartialObjectMetadata, 0, len(objs))
	for _, o := range objs {
		if m, ok := o.(*metav1.PartialObjectMetadata); ok {
			out = append(out, m)
		}
	}
	return out
}

// protectedHandler is tracker t's handler on protected informer p. It maps
// protected-object events to t's release batches and holders (§10.2), drops
// t's written entries (§6.3), and counts t's holds in p for the held gauge
// (§12):
//   - any change to one of t's finalizers or epochs enqueues that (K, N)
//     batch; an add is one, which covers the initial list's recheck of every
//     hold;
//   - a new deletionTimestamp enqueues the object's batches at once and
//     again when t's grace ends, measured as §5 says; so does a relist that
//     brings a deleting object under a new UID;
//   - losing one of t's finalizers while staying in the cache, and leaving
//     the cache, as a deletion does and as losing the held label does (a
//     kubectl replace that strips our metadata, §6.5), enqueue the object's
//     holders through t's index.
//
// Each tracker registers its own handler, so that one registered on an
// informer that is already running gets the informer's store replayed as
// adds before any later event, in order: a Track after Run rechecks every
// hold already in the cache, deleting objects' graces included, without a
// gap or a race with the events that follow (D4 of the review of #109).
// client-go calls a handler's methods one at a time, so held needs no lock.
type protectedHandler struct {
	s *Shared
	p *protectedInformer
	t *Tracker

	held int // t's (object, finalizer) pairs in p's cache
}

func (h *protectedHandler) id(m *metav1.PartialObjectMetadata) ID {
	return ID{Group: h.p.gk.Group, Kind: h.p.gk.Kind, Namespace: m.Namespace, Name: m.Name}
}

func (h *protectedHandler) observe(m *metav1.PartialObjectMetadata) {
	if f := h.s.hooks.protectedEvent; f != nil {
		f(h.t, m)
	}
}

// count moves t's held gauge by d pairs.
func (h *protectedHandler) count(d int) {
	if d == 0 {
		return
	}
	h.held += d
	h.t.setHeld(h.p.gk, h.held)
}

// holds returns each of t's finalizers on m, by holder namespace, with its
// epoch.
func holds(t *Tracker, m *metav1.PartialObjectMetadata) map[string]string {
	out := map[string]string{}
	for _, f := range m.Finalizers {
		if n, ok := namespaceOfFinalizer(f, t.prefix); ok {
			out[n] = m.Annotations[f]
		}
	}
	return out
}

func (h *protectedHandler) OnAdd(obj any, _ bool) {
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	h.observe(m)
	id := h.id(m)
	held := holds(h.t, m)
	for n := range held {
		h.s.queue.Add(batchKey{h.t.gk, n}, id)
	}
	if m.DeletionTimestamp != nil {
		h.graceTimers(m, id)
	}
	h.count(len(held))
}

func (h *protectedHandler) OnUpdate(oldObj, newObj any) {
	o, ok1 := metadataOf(oldObj)
	n, ok2 := metadataOf(newObj)
	if !ok1 || !ok2 {
		return
	}
	h.observe(n)
	t := h.t
	id := h.id(n)
	replaced := o.UID != n.UID
	newlyDeleting := o.DeletionTimestamp == nil && n.DeletionTimestamp != nil
	t.ver.protectedEvent(o, n)
	before, after := holds(t, o), holds(t, n)
	lost := false
	for ns := range mergedKeys(before, after) {
		eb, inB := before[ns]
		ea, inA := after[ns]
		if inB != inA || eb != ea || newlyDeleting || replaced {
			h.s.queue.Add(batchKey{t.gk, ns}, id)
		}
		lost = lost || (inB && (!inA || replaced))
	}
	if lost {
		// The holds that were there are gone, as when the object leaves the
		// cache: its holders' next passes write A2 again (§6.5, G4 of the
		// review of #109).
		t.enqueueHolders(id)
	}
	if replaced {
		h.s.seen.forget(o.UID)
	}
	if n.DeletionTimestamp != nil && (newlyDeleting || replaced) {
		h.graceTimers(n, id)
	}
	h.count(len(after) - len(before))
}

func (h *protectedHandler) OnDelete(obj any) {
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	h.observe(m)
	t := h.t
	id := h.id(m)
	t.ver.protectedEvent(m, nil)
	held := holds(t, m)
	for n := range held {
		h.s.queue.Add(batchKey{t.gk, n}, id)
	}
	t.enqueueHolders(id)
	// The object left the cache: its first sight goes, so that firstSight
	// holds only deleting objects this process still sees (G6 of the review
	// of #109). If it comes back still deleting, its grace starts again from
	// then, which only delays a release.
	h.s.seen.forget(m.UID)
	h.count(-len(held))
}

// graceTimers enqueues m's batches for t again when t's grace for m ends
// (§5, §10.2). The releaser re-checks the grace in R1, so an early or a
// duplicate enqueue is harmless.
func (h *protectedHandler) graceTimers(m *metav1.PartialObjectMetadata, id ID) {
	now := h.s.clock.Now()
	first := h.s.seen.see(m.UID, now)
	end := GraceEnd(m.DeletionTimestamp.Time, first, h.t.o.DeletionGrace)
	for n := range holds(h.t, m) {
		h.s.queue.AddAfter(batchKey{h.t.gk, n}, end.Sub(now), id)
	}
}

func mergedKeys(a, b map[string]string) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

// --- the Namespace informer (§11.3, §5) ---

// namespaceHandler compares the transformed old and new Namespace (L5), and
// acts only on a change to a credential-use.* annotation or to the
// deletionTimestamp: it enqueues the holders of every held object in that
// namespace, and on a new deletionTimestamp those objects' release batches
// too (§5's namespace rows).
type namespaceHandler struct{ s *Shared }

func (namespaceHandler) OnAdd(any, bool) {}
func (namespaceHandler) OnDelete(any)    {}

func (h namespaceHandler) OnUpdate(oldObj, newObj any) {
	o, ok1 := metadataOf(oldObj)
	n, ok2 := metadataOf(newObj)
	if !ok1 || !ok2 {
		return
	}
	newlyDeleting := o.DeletionTimestamp == nil && n.DeletionTimestamp != nil
	if !newlyDeleting && (o.DeletionTimestamp == nil) == (n.DeletionTimestamp == nil) &&
		maps.Equal(o.Annotations, n.Annotations) {
		return
	}
	if f := h.s.hooks.namespaceAct; f != nil {
		f(n)
	}
	for _, p := range h.s.protectedInformers() {
		for _, m := range p.inNamespace(n.Name) {
			id := ID{Group: p.gk.Group, Kind: p.gk.Kind, Namespace: m.Namespace, Name: m.Name}
			for _, t := range p.snapshot() {
				t.enqueueHolders(id)
				if newlyDeleting {
					for ns := range holds(t, m) {
						h.s.queue.Add(batchKey{t.gk, ns}, id)
					}
				}
			}
		}
	}
}

// --- the views the protocol reads (§6.4) ---

// trackerViews are tracker t's views over the shared caches.
type trackerViews struct{ t *Tracker }

var _ views = trackerViews{}

func (v trackerViews) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	p := v.t.s.protectedInformer(id.GroupKind())
	if p == nil || !p.inf.HasSynced() {
		return nil, false
	}
	return p.get(id.Namespace, id.Name)
}

func (v trackerViews) HoldersNaming(id ID, namespace string) []*metav1.PartialObjectMetadata {
	idx := v.t.holders.GetIndexer()
	var out []*metav1.PartialObjectMetadata
	for _, key := range []string{id.String(), unparseableKey} {
		objs, err := idx.ByIndex(refsIndex, key)
		if err != nil {
			continue
		}
		for _, o := range objs {
			if m, ok := o.(*metav1.PartialObjectMetadata); ok && m.Namespace == namespace {
				out = append(out, m)
			}
		}
	}
	return out
}

func (v trackerViews) NamespaceDeleting(namespace string) (bool, bool) {
	obj, ok, err := v.t.s.namespaces.GetStore().GetByKey(namespace)
	if err != nil || !ok {
		return false, false
	}
	m, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return false, false
	}
	return m.DeletionTimestamp != nil, true
}

// enqueueHolders enqueues, through Options.Enqueue, every holder of t's kind
// whose cached record names id (§10.2), and every holder whose record can't
// be parsed. Such a record names every ID for R0 and R2 (§4), so its holder
// may be one of id's users; its next pass rewrites the record (A1), which
// is what makes it parseable again. Enqueueing it is never unsafe: a pass
// only adds holds. Records are unparseable only when something other than
// the core writes them, so these are few (N5 of the review of #109).
func (t *Tracker) enqueueHolders(id ID) {
	idx := t.holders.GetIndexer()
	for _, key := range []string{id.String(), unparseableKey} {
		objs, err := idx.ByIndex(refsIndex, key)
		if err != nil {
			continue
		}
		for _, o := range objs {
			if m, ok := o.(*metav1.PartialObjectMetadata); ok {
				t.enqueue(types.NamespacedName{Namespace: m.Namespace, Name: m.Name})
			}
		}
	}
}
