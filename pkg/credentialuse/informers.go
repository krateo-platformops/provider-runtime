package credentialuse

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	switch o := obj.(type) {
	case *metav1.PartialObjectMetadata:
		return o, true
	case *unstructured.Unstructured:
		// A content kind's object (§10.4): its metadata, trimmed as the
		// metadata informers' transform leaves theirs.
		return metadataOfContent(o), true
	}
	return nil, false
}

// metadataOfContent is u's metadata as a metadata informer would cache it.
func metadataOfContent(u *unstructured.Unstructured) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: u.GetAPIVersion(), Kind: u.GetKind()}}
	m.Name, m.Namespace, m.UID = u.GetName(), u.GetNamespace(), u.GetUID()
	m.ResourceVersion, m.Generation = u.GetResourceVersion(), u.GetGeneration()
	m.DeletionTimestamp = u.GetDeletionTimestamp()
	m.Finalizers = u.GetFinalizers()
	m.Labels = keepOnly(u.GetLabels(), isCredentialUseKey)
	m.Annotations = keepOnly(u.GetAnnotations(), isCredentialUseKey)
	return m
}

// foreignChange reports whether a content kind's object changed through
// someone else's write (§9.2, L2): the two versions differ once every
// credential-use. label, annotation and finalizer, the resourceVersion and
// the managedFields are set aside, so the tracker's own A2s and releases
// don't count. Either not being a content object counts as no change.
func foreignChange(oldObj, newObj any) bool {
	o, ok1 := contentOf(oldObj)
	n, ok2 := contentOf(newObj)
	if !ok1 || !ok2 {
		return false
	}
	return !equality.Semantic.DeepEqual(othersView(o), othersView(n))
}

func contentOf(obj any) (*unstructured.Unstructured, bool) {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok
}

// othersView is u without what the tracker writes and the server stamps.
func othersView(u *unstructured.Unstructured) map[string]any {
	c := u.DeepCopy()
	c.SetLabels(dropKeys(c.GetLabels(), isCredentialUseKey))
	// Consent is written by the target's owner, not by the tracker: a change
	// to it is someone else's (§11.3; the review of #122).
	c.SetAnnotations(dropKeys(c.GetAnnotations(), func(k string) bool {
		return isCredentialUseKey(k) && k != AnnotationAllowHoldersFrom
	}))
	fs := slices.DeleteFunc(c.GetFinalizers(), isCredentialUseKey)
	if len(fs) == 0 {
		fs = nil // absent and empty are one
	}
	c.SetFinalizers(fs)
	c.SetResourceVersion("")
	c.SetManagedFields(nil)
	return c.Object
}

// dropKeys returns a copy of m without the keys drop accepts, or nil when
// nothing is left, so that an empty map and an absent one compare equal.
func dropKeys(m map[string]string, drop func(string) bool) map[string]string {
	return keepOnly(m, func(k string) bool { return !drop(k) })
}

// contentTransform is a content kind's transform (§10.4): the whole object,
// without managedFields and status.
func contentTransform(obj any) (any, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj, nil
	}
	out := &unstructured.Unstructured{Object: maps.Clone(u.Object)}
	delete(out.Object, "status")
	if md, ok := out.Object["metadata"].(map[string]any); ok {
		md = maps.Clone(md)
		delete(md, "managedFields")
		out.Object["metadata"] = md
	}
	return out, nil
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
		// Deleted and created again under its name, in a watch's gap: no
		// deletion comes for the old UID, so it is forgotten here, as
		// OnDelete forgets a holder that is gone.
		h.t.ver.holderEvent(o.UID, nil, at)
		h.t.proto.written.forget(o.UID)
		h.t.proto.backoff.forget(o.UID)
		h.t.proto.gateTurns.forget(o.UID)
		h.t.proto.reported.forget(o.UID)
		h.t.unrecorded.forget(o.UID)
		h.t.degradedLogged.forget(o.UID)
		h.t.refused.forget(o.UID)
		h.t.noteRefused(o.UID, false)
		h.t.misplaced.forget(o.UID)
		h.t.proto.conflicts.forget(o.UID)
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
	h.t.proto.written.forget(m.UID)
	// A holder gone without a Forget, as one that was never established,
	// leaves nothing behind either: its UID never comes back, and whoever can
	// edit one names the refs these are kept for (§11.3). OnUpdate forgets the
	// same of a UID that a new one replaced.
	h.t.proto.backoff.forget(m.UID)
	h.t.proto.gateTurns.forget(m.UID)
	h.t.proto.reported.forget(m.UID)
	h.t.unrecorded.forget(m.UID)
	h.t.degradedLogged.forget(m.UID)
	h.t.refused.forget(m.UID)
	h.t.noteRefused(m.UID, false)
	h.t.misplaced.forget(m.UID)
	h.t.proto.conflicts.forget(m.UID)
	h.enqueue(m, nil)
	// A holder that is gone waits for no Missing ref (§10.3). One whose
	// deletion isn't delivered, while its CRD is gone, is probed for on, with
	// the prober's backoff.
	h.t.missing.set(types.NamespacedName{Namespace: m.Namespace, Name: m.Name}, nil, h.now(), h.t.o.ProbeInterval)
}

// enqueue adds one batch, of every ID in old's and cur's records.
func (h holderHandler) enqueue(old, cur *metav1.PartialObjectMetadata) {
	// In one Add: a worker may take what one queues before the next, and a
	// batch of its own would read the names in both records again.
	var k batchKey
	var ids []ID
	for _, m := range []*metav1.PartialObjectMetadata{old, cur} {
		if m != nil {
			k = batchKey{h.t.gk, m.Namespace}
			ids = append(ids, ReadRecord(m).IDs...)
		}
	}
	if len(ids) > 0 { // with none, an Add is of the whole namespace
		h.t.s.queue.Add(k, ids...)
	}
}

// --- the protected informers: one per protected kind, shared (§10.1) ---

func heldSelector() string { return LabelHeld + "=true" }

// protectedInformer is one protected kind's label-scoped cache, shared by
// every tracker that may hold that kind. Each such tracker has its own
// handler on it (protectedHandler).
type protectedInformer struct {
	gk schema.GroupKind
	ns string // the namespace it is restricted to (§10.5), or ""
	// name is the one object a per-name informer watches (§13 shape 1), or
	// "" for a label-scoped one.
	name string
	gvr  schema.GroupVersionResource
	// content: it caches whole objects, without managedFields and status,
	// through the dynamic client (§10.4); otherwise metadata only.
	content bool
	lw      *listWatch
	inf     cache.SharedIndexInformer
	// Under Shared.mu: claims counts the Tracks and trackers that use the
	// informer; runOwned, that RunInformers starts it; starting, that a
	// Track after Run is starting it, or has; stop ends it then.
	claims   int
	runOwned bool
	starting bool
	stop     context.CancelFunc
	// failed is closed when a Track after Run's start of it fails, with
	// startErr.
	failed   chan struct{}
	failOnce sync.Once
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

// cached returns the cached object namespace/name, if there is one: only a name
// that is a single path segment is a part of a key as it is.
func (p *protectedInformer) cached(namespace, name string) (any, bool) {
	if !pathSegment(namespace) || !pathSegment(name) {
		return nil, false
	}
	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}
	obj, ok, err := p.inf.GetStore().GetByKey(key)
	return obj, ok && err == nil
}

func (p *protectedInformer) get(namespace, name string) (*metav1.PartialObjectMetadata, bool) {
	obj, ok := p.cached(namespace, name)
	if !ok {
		return nil, false
	}
	return metadataOf(obj)
}

// getContent returns the cached object namespace/name whole, for a content
// kind (§10.4); a metadata-only informer has none.
func (p *protectedInformer) getContent(namespace, name string) (*unstructured.Unstructured, bool) {
	if !p.content {
		return nil, false
	}
	obj, ok := p.cached(namespace, name)
	if !ok {
		return nil, false
	}
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok
}

// inNamespace returns the cached objects in namespace.
func (p *protectedInformer) inNamespace(namespace string) []*metav1.PartialObjectMetadata {
	objs, err := p.inf.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil
	}
	out := make([]*metav1.PartialObjectMetadata, 0, len(objs))
	for _, o := range objs {
		if m, ok := metadataOf(o); ok {
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

	held    int // t's (object, finalizer) pairs in p's cache
	waiting int // p's cached objects being deleted that carry one of t's finalizers
	// w is the per-name watch p is, or nil for a label-scoped informer: once
	// it stops, its counts leave the gauges.
	w *nameWatch
}

func (h *protectedHandler) id(m *metav1.PartialObjectMetadata) ID {
	return ID{Group: h.p.gk.Group, Kind: h.p.gk.Kind, Namespace: m.Namespace, Name: m.Name}
}

func (h *protectedHandler) observe(m *metav1.PartialObjectMetadata) {
	if f := h.s.hooks.protectedEvent; f != nil {
		f(h.t, m)
	}
}

// count moves t's gauges: d held pairs, and w waiting objects.
func (h *protectedHandler) count(d, w int) {
	if d == 0 && w == 0 {
		return
	}
	h.held += d
	h.waiting += w
	h.t.setCounts(h.p.countKey(), h.held, h.waiting, h.w)
}

// waits reports whether m counts as waiting for t's users: it is being
// deleted and carries one of t's finalizers.
func waits(m *metav1.PartialObjectMetadata, held map[string]string) int {
	if m.DeletionTimestamp != nil && len(held) > 0 {
		return 1
	}
	return 0
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
	h.count(len(held), waits(m, held))
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
	consentChanged := o.Annotations[AnnotationAllowHoldersFrom] != n.Annotations[AnnotationAllowHoldersFrom]
	if lost || consentChanged || h.p.content && (o.Generation != n.Generation || foreignChange(oldObj, newObj)) {
		// A change to the object's consent reaches its holders too, of any
		// kind: their next passes read it live before dropping or adding a
		// hold (§11.3).
		// The holds that were there are gone, as when the object leaves the
		// cache: its holders' next passes write A2 again (§6.5, G4 of the
		// review of #109). And a content kind's object, which leads on to
		// other refs, changed through someone else's write or its spec: what
		// it names may have changed, so its holders' next passes follow it
		// again (§9.2, MED-5).
		t.enqueueHolders(id)
	}
	if replaced {
		h.s.seen.forget(o.UID)
	}
	if n.DeletionTimestamp != nil && (newlyDeleting || replaced) {
		h.graceTimers(n, id)
	}
	h.count(len(after)-len(before), waits(n, after)-waits(o, before))
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
	h.count(-len(held), -waits(m, held))
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

func (h namespaceHandler) OnAdd(obj any, _ bool) { h.wildcard(obj) }

// wildcard warns on a Namespace whose allow-holders-from names "*", which is
// ignored there (§11.3).
func (h namespaceHandler) wildcard(obj any) {
	m, ok := metadataOf(obj)
	if !ok || !hasWildcard(m.Annotations[AnnotationAllowHoldersFrom]) {
		return
	}
	ref := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: m.Name, UID: m.UID}}
	ref.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
	h.s.events.record(ref, "Warning", EventConsentWildcardIgnored,
		`"*" in allow-holders-from is ignored on a Namespace: name the holder namespaces, or set "*" on each object meant for every namespace`)
}
func (namespaceHandler) OnDelete(any) {}

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
	if o.Annotations[AnnotationAllowHoldersFrom] != n.Annotations[AnnotationAllowHoldersFrom] {
		h.wildcard(n)
	}
	if newlyDeleting {
		// Outside the caches no informer delivers the deletion of the
		// namespace's objects (§10.5).
		h.s.uncachedDeleting(n.Name)
	}
	for _, p := range h.s.allProtected() {
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

// protectedFor returns the informer whose cache covers id for t: its
// per-name watch of id for a PerName kind (§13 shape 1), or the shared
// label-scoped one; nil when none does.
func (t *Tracker) protectedFor(id ID) *protectedInformer {
	if t.isPerName(id.GroupKind()) {
		return t.names.get(id)
	}
	return t.s.protectedFor(id)
}

// cacheLacks reports whether the cache that covers id has synced, and doesn't
// have it: it isn't held, or is gone, or has just been held, which the cache
// will show, or its held label was stripped. A PerName kind's cache is the
// watch of id's index line, whatever the object's labels: with the index
// synced and no such watch, t holds no id, or its line is missing, until the
// tamper check or a rebuild restores it (§10.6.6). A kind, or a namespace,
// that no cache covers is never lacking.
func (t *Tracker) cacheLacks(id ID) bool {
	p := t.protectedFor(id)
	if p == nil {
		return t.isPerName(id.GroupKind()) && t.idx.inf.HasSynced()
	}
	if !p.inf.HasSynced() {
		return false
	}
	_, ok := p.get(id.Namespace, id.Name)
	return !ok
}

// trackerViews are tracker t's views over the shared caches.
type trackerViews struct{ t *Tracker }

var _ views = trackerViews{}

// Content returns content kind id's cached object (§10.4).
func (v trackerViews) Content(id ID) (*unstructured.Unstructured, bool) {
	p := v.t.protectedFor(id)
	if p == nil || !p.inf.HasSynced() {
		return nil, false
	}
	return p.getContent(id.Namespace, id.Name)
}

// NamespaceConsent returns namespace's cached allow-holders-from, and whether
// the cache knows it (§11.3): an unsynced cache, or one without it, doesn't.
func (v trackerViews) NamespaceConsent(namespace string) (string, bool) {
	if !v.t.s.namespaces.HasSynced() {
		return "", false
	}
	obj, ok, err := v.t.s.namespaces.GetStore().GetByKey(namespace)
	if err != nil || !ok {
		return "", false
	}
	m, ok := metadataOf(obj)
	if !ok {
		return "", false
	}
	return m.Annotations[AnnotationAllowHoldersFrom], true
}

func (v trackerViews) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	p := v.t.protectedFor(id)
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
