package credentialuse

import (
	"context"
	"maps"
	"reflect"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// What a runtime's adapter needs beyond Acquire, BeforeEstablish and Forget
// (§7.2, §7.5, §11.9): a kind's settings as one value, whether a tracker's
// handlers have their initial state, the holder event filter, the check of an
// unrecorded read, and the summary of a holder's unheld list for its failure
// message (§12).

// KindOptions are a holder kind's settings that a runtime's adapter takes from
// its consumer (§7.5): provider-runtime's reconciler.WithCredentialUse. The
// adapter fills in the rest of Options itself (the holder, the owner
// finalizers, Refs, the sinks, PollInterval and TimeoutBound). A zero field
// takes Options' default.
type KindOptions struct {
	// Protected are the kinds Refs may name; the default is Secret,
	// LabelScoped. A custom resource that serves several versions must
	// convert without losing a field, a label or an annotation (§6.2).
	Protected []ProtectedKind
	// MaxDepth bounds the levels of Next followed (§9.1).
	MaxDepth int
	// DeletionGrace is §5's grace.
	DeletionGrace time.Duration
	// ProbeInterval is the missing-ref prober's tick (§10.3).
	ProbeInterval time.Duration
	// DisownInterval and DisownPasses are Disown's pass interval and its
	// consecutive zero passes (§7.4). An unset DisownInterval is derived
	// from TimeoutBound, as Options.DisownInterval says, so a long reconcile
	// timeout needs no setting. One set here is validated as given.
	DisownInterval time.Duration
	DisownPasses   int
}

// HasSynced reports whether t's informers have synced and every one of t's
// handlers on the protected informers has had their initial state:
// provider-runtime's CredentialUseSource waits on it before the controller's
// workers start (§7.5, L1). Call it only on a tracker Track returned.
func (t *Tracker) HasSynced() bool {
	if !t.holders.HasSynced() || !t.crd.HasSynced() {
		return false
	}
	if t.idx != nil && !t.idx.inf.HasSynced() {
		return false
	}
	for _, reg := range t.regs {
		if !reg.HasSynced() {
			return false
		}
	}
	return true
}

// A recordWrite is what an A1 of this process last left on a holder: the
// values of its record and unheld annotations in the response, and whether
// each was present.
type recordWrite struct {
	refs, unheld       string
	hasRefs, hasUnheld bool
}

func recordWriteOf(annotations map[string]string) recordWrite {
	refs, hasRefs := annotations[AnnotationRefs]
	unheld, hasUnheld := annotations[AnnotationUnheld]
	return recordWrite{refs: refs, unheld: unheld, hasRefs: hasRefs, hasUnheld: hasUnheld}
}

// recordWrites remembers, per holder, what this process's last A1 wrote, for
// IsOwnRecordWrite (§7.5, M6, L8). An entry goes when the holder is forgotten
// or deleted.
type recordWrites struct {
	mu sync.Mutex
	by map[types.UID]recordWrite
}

func newRecordWrites() *recordWrites { return &recordWrites{by: map[types.UID]recordWrite{}} }

func (w *recordWrites) set(uid types.UID, annotations map[string]string) {
	if w == nil || uid == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.by[uid] = recordWriteOf(annotations)
}

func (w *recordWrites) get(uid types.UID) (recordWrite, bool) {
	if w == nil {
		return recordWrite{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	rw, ok := w.by[uid]
	return rw, ok
}

func (w *recordWrites) forget(uid types.UID) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.by, uid)
}

// IsOwnRecordWrite reports whether the only change from old to new, two
// versions of one holder, is to the record or the unheld annotation, setting
// aside resourceVersion and managedFields, and both now equal what this
// process's last A1 wrote for that holder (§7.5, M6, L8). A runtime drops
// such a holder update event: the pass that wrote it carries on with the
// object it got back, so the event would only cause an extra pass. A hand
// edit of the record, another process's write, or any other change returns
// false, and so does a pair that isn't a runtime.Object, which can't be
// compared whole.
func (t *Tracker) IsOwnRecordWrite(oldObj, newObj metav1.Object) bool {
	if oldObj == nil || newObj == nil || oldObj.GetUID() == "" || oldObj.GetUID() != newObj.GetUID() {
		return false
	}
	before, after := recordWriteOf(oldObj.GetAnnotations()), recordWriteOf(newObj.GetAnnotations())
	if before == after {
		return false // the record and the unheld list didn't change
	}
	if last, ok := t.proto.written.get(newObj.GetUID()); !ok || last != after {
		return false
	}
	return sameButRecord(oldObj, newObj)
}

// sameButRecord reports whether a and b are equal once their record and unheld
// annotations, resourceVersion and managedFields are set aside.
func sameButRecord(a, b metav1.Object) bool {
	ma, ok1 := strippedContent(a)
	mb, ok2 := strippedContent(b)
	return ok1 && ok2 && reflect.DeepEqual(ma, mb)
}

func strippedContent(o metav1.Object) (map[string]any, bool) {
	ro, ok := o.(runtime.Object)
	if !ok {
		return nil, false
	}
	var m map[string]any
	if u, ok := ro.(*unstructured.Unstructured); ok {
		m = runtime.DeepCopyJSON(u.Object)
	} else {
		var err error
		if m, err = runtime.DefaultUnstructuredConverter.ToUnstructured(ro); err != nil {
			return nil, false
		}
	}
	md, _ := m["metadata"].(map[string]any)
	if md == nil {
		return m, true
	}
	delete(md, "resourceVersion")
	delete(md, "managedFields")
	if an, ok := md["annotations"].(map[string]any); ok {
		an = maps.Clone(an)
		delete(an, AnnotationRefs)
		delete(an, AnnotationUnheld)
		if len(an) == 0 {
			delete(md, "annotations")
		} else {
			md["annotations"] = an
		}
	}
	return m, true
}

// CheckRead reports a read of protected object gk namespace/name, made for
// holder, that the holder's refs don't name (§11.9): a provider defect,
// since nothing holds that object. Runtimes call it from a wrapper around
// their clients (provider-runtime's CredentialUseCheckingReader). A read is
// named when the holder's record or its unheld list names the object, or
// when one of the refs Refs returns for the holder, Guaranteed ones
// included, does; a read of a kind the tracker doesn't protect, or that the
// Disown switch covers, is never reported. An unnamed read is logged once
// per holder and object, and every one is counted in
// credentialuse_unrecorded_reads_total.
func (t *Tracker) CheckRead(holder metav1.Object, gk schema.GroupKind, namespace, name string) {
	if holder == nil || !t.protects(gk) || t.disownSwitch().covers(gk) {
		return
	}
	id := ID{Group: gk.Group, Kind: gk.Kind, Namespace: namespace, Name: name}
	if ReadRecord(holder).Names(id) {
		return
	}
	for _, e := range ReadUnheld(holder.GetAnnotations()) {
		if e.Ref == unheldRef(id) {
			return
		}
	}
	if refs, err := t.o.Refs(holder); err == nil {
		for _, r := range refs {
			if r.ID() == id {
				return
			}
		}
	}
	sinkMetrics{t}.UnrecordedReads(kindLabel(t.gk))
	if t.unrecorded.first(consentKey{holder.GetUID(), id}) {
		t.warn("credential-use: a protected object was read that the holder's refs don't name, so nothing holds it (§11.9): name it in CredentialRefs",
			"holder", holder.GetNamespace()+"/"+holder.GetName(), "kind", t.gk.String(), "object", id.String())
	}
}

// protects reports whether gk is one of t's protected kinds.
func (t *Tracker) protects(gk schema.GroupKind) bool {
	for _, pk := range t.o.Protected {
		if pk.GVK.GroupKind() == gk {
			return true
		}
	}
	return false
}

// An UnheldSummary is what §12's failure message says of a holder's unheld
// list: how many refs it lists, counting those folded into its More entry,
// and the reason and time of the most recent entry.
type UnheldSummary struct {
	Count  int
	Reason string
	Since  time.Time
}

// SummarizeUnheld summarizes a holder's unheld annotation (§12). It reports
// false when the list is absent or empty.
func SummarizeUnheld(annotations map[string]string) (UnheldSummary, bool) {
	var s UnheldSummary
	found := false
	for _, e := range ReadUnheld(annotations) {
		if e.Reason == unheldMore {
			s.Count += e.Count
			continue
		}
		s.Count++
		if !found || e.Since.After(s.Since) {
			s.Reason, s.Since, found = e.Reason, e.Since, true
		}
	}
	return s, found
}

// WithResolvedObjects returns ctx carrying objs as the objects a pass read
// for the refs with a Next, keyed by their IDs, for Resolved: for a test or
// a runtime that resolves a chain itself, as reconcilertest's
// AssertCredentialRefsCover does. A runtime that called Acquire uses
// WithResolved.
func WithResolvedObjects(ctx context.Context, objs map[ID]*unstructured.Unstructured) context.Context {
	m := make(map[ID]*unstructured.Unstructured, len(objs))
	for id, o := range objs {
		m[id] = o.DeepCopy()
	}
	return context.WithValue(ctx, resolvedKey{}, m)
}
