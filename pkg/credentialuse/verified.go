package credentialuse

import (
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// The verified set (§6.3) lets A2 skip a write it has already made. It is
// per process and per holder kind K, and only ever reduces skips: a
// process skips only against its own write, made after its own observation of
// the record. All three maps are lost on a restart or a leader change, which
// costs one A2 per (P, K, N).
//
// Times are this process's clock readings. With the real clock they carry a
// monotonic reading, so they must never go through UTC, In, Local or
// Round(0), which strip it.

// writtenKey is a protected object incarnation and a holder namespace N: K is
// the verified set's own.
type writtenKey struct {
	uid       types.UID
	namespace string
}

// holderRef is a holder incarnation and a ref it names.
type holderRef struct {
	holder types.UID
	ref    ID
}

type verified struct {
	holder schema.GroupKind

	mu sync.Mutex
	// written[(P.uid, N)]: when this process issued a successful A2 for P,
	// K and N.
	written map[writtenKey]time.Time
	// recordSince[(H.uid, P)]: the informer delivery time of the first
	// holder event, in the current unbroken run of H's versions whose record
	// names P. Only the holder informer's handler writes it.
	recordSince map[holderRef]time.Time
	// observed[(H.uid, P)]: when the A1 that added P returned. Only passes
	// write it, synchronously.
	observed map[holderRef]time.Time
	// lastList: when the holder informer's latest list or relist completed.
	// A relist delivers only the final state, so a record that went away
	// and came back in a watch gap keeps its old recordSince; every t_obs is
	// therefore at least this (§6.5 L2, revision 16).
	lastList time.Time
}

func newVerified(holder schema.GroupKind) *verified {
	return &verified{
		holder:      holder,
		written:     map[writtenKey]time.Time{},
		recordSince: map[holderRef]time.Time{},
		observed:    map[holderRef]time.Time{},
	}
}

// tObs returns t_obs for (H, P): the later of observed and recordSince, and
// at least the last list's completion. If both observed and recordSince are
// absent, there is none, and the pass doesn't skip.
func (v *verified) tObs(h types.UID, p ID) (time.Time, bool) {
	k := holderRef{h, p}
	o, hasO := v.observed[k]
	r, hasR := v.recordSince[k]
	if !hasO && !hasR {
		return time.Time{}, false
	}
	t := o
	if hasR && (!hasO || r.After(t)) {
		t = r
	}
	if v.lastList.After(t) {
		t = v.lastList
	}
	return t, true
}

// canSkip reports whether A2 may skip for holder h, ref p and the cached
// incarnation pUID of P, in holder namespace n (§6.3). The caller has already
// excluded deletion passes and refs this pass's A1 added.
func (v *verified) canSkip(h types.UID, p ID, pUID types.UID, n string) bool {
	if pUID == "" {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	issued, ok := v.written[writtenKey{pUID, n}]
	if !ok {
		return false
	}
	obs, ok := v.tObs(h, p)
	return ok && obs.Before(issued)
}

// wrote records a successful A2 for P's incarnation pUID and namespace n,
// issued at t.
func (v *verified) wrote(pUID types.UID, n string, issued time.Time) {
	if pUID == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.written[writtenKey{pUID, n}] = issued
}

// released drops every written entry for P's incarnation pUID: a successful
// R3 does that at once, synchronously (§6.2 R3).
func (v *verified) released(pUID types.UID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k := range v.written {
		if k.uid == pUID {
			delete(v.written, k)
		}
	}
}

// announced updates observed after a successful A1 (§6.2 A1, M1): refs it
// added get the time A1's response returned, overwriting any earlier value,
// and refs it removed lose theirs.
func (v *verified) announced(h types.UID, added, removed []ID, at time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, p := range added {
		v.observed[holderRef{h, p}] = at
	}
	for _, p := range removed {
		delete(v.observed, holderRef{h, p})
	}
}

// forgot drops a holder's observed entries, when its record is removed.
func (v *verified) forgot(h types.UID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k := range v.observed {
		if k.holder == h {
			delete(v.observed, k)
		}
	}
}

// holderEvent is the holder informer handler's only work on the verified
// set (§6.3): it maintains recordSince for a holder version delivered at at.
// A version whose record names P starts or continues a run; one that doesn't
// name P ends it. An unparseable record ends every run: no skip rests on it,
// and the next A1 rewrites it. A nil h is a deletion.
func (v *verified) holderEvent(uid types.UID, h metav1.Object, at time.Time) {
	var names []ID
	if h != nil {
		if r := ReadRecord(h); !r.Unparseable {
			names = r.IDs
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	keep := map[ID]bool{}
	for _, p := range names {
		keep[p] = true
		k := holderRef{uid, p}
		if _, ok := v.recordSince[k]; !ok {
			v.recordSince[k] = at
		}
	}
	for k := range v.recordSince {
		if k.holder == uid && !keep[k.ref] {
			delete(v.recordSince, k)
		}
	}
	if h == nil {
		for k := range v.observed {
			if k.holder == uid {
				delete(v.observed, k)
			}
		}
	}
}

// listCompleted records that the holder informer's list or relist completed
// at at (§6.5 L2).
func (v *verified) listCompleted(at time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if at.After(v.lastList) {
		v.lastList = at
	}
}

// protectedEvent is the protected informer handler's only work on the
// verified set (§6.3): it drops written entries, and never creates or extends
// one. old is the version the cache had; current is nil when the object was
// deleted or left the label-scoped cache. An entry goes when P's incarnation
// changed or went, or when P lost the finalizer of K for that entry's N.
func (v *verified) protectedEvent(old, current metav1.Object) {
	if old == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for k := range v.written {
		if k.uid != old.GetUID() {
			continue
		}
		if current == nil || current.GetUID() != k.uid {
			delete(v.written, k)
			continue
		}
		f, err := FinalizerName(v.holder, k.namespace)
		if err != nil || !hasFinalizer(current, f) {
			delete(v.written, k)
		}
	}
}

func hasFinalizer(obj metav1.Object, f string) bool {
	for _, g := range obj.GetFinalizers() {
		if g == f {
			return true
		}
	}
	return false
}
