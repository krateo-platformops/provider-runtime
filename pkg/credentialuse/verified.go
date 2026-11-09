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
// the record. All of it is lost on a restart or a leader change, which costs
// one A2 per (P, K, N).
//
// A skip rests on one exact holder version (H1 of #59's review): the pass
// works from a holder at resourceVersion rv, and may skip A2 for P only if
// the core holder informer has delivered that very rv, that version names P,
// and this process wrote P after the run of versions naming P began. An
// observation of an earlier run of the record counts for nothing: a record
// that went away and came back, through a lost A1 response, another process's
// writes, or a manager cache running ahead of the core informer, is a new run
// whose start the informer hasn't delivered, so the pass writes.
//
// Times are this process's clock readings. With the real clock they carry a
// monotonic reading, so they must never go through UTC, In, Local or
// Round(0), which strip it.

type verified struct {
	holder schema.GroupKind

	mu sync.Mutex
	// written[P.uid][N]: when this process issued a successful A2 for P, K
	// and N.
	written map[types.UID]map[string]time.Time
	// delivered[H.uid]: the resourceVersion of the holder version the core
	// informer delivered last.
	delivered map[types.UID]string
	// recordSince[H.uid][P]: the informer delivery time of the first holder
	// event in the current unbroken run of H's versions whose record names P.
	// Only the holder informer's handler writes it.
	recordSince map[types.UID]map[ID]time.Time
	// observed[H.uid][P]: when the A1 that added P returned. Passes write it,
	// synchronously; the holder handler drops it when a delivered version
	// ends P's run.
	observed map[types.UID]map[ID]time.Time
	// lastList: when the holder informer's latest list or relist completed.
	// A relist delivers only the final state, so every t_obs is at least this
	// (§6.5 L2, revision 16).
	lastList time.Time
}

func newVerified(holder schema.GroupKind) *verified {
	return &verified{
		holder:      holder,
		written:     map[types.UID]map[string]time.Time{},
		delivered:   map[types.UID]string{},
		recordSince: map[types.UID]map[ID]time.Time{},
		observed:    map[types.UID]map[ID]time.Time{},
	}
}

// canSkip reports whether A2 may skip for holder h at resourceVersion hRV,
// ref p, and the cached incarnation pUID of P, in holder namespace n (§6.3).
// The caller has already excluded deletion passes and refs this pass's A1
// added. It holds only if:
//
//   - the informer has delivered exactly hRV, and that version names p;
//   - this process wrote P's incarnation pUID for n;
//   - t_obs, the later of the run's first delivery, the A1 that added p and
//     the last list, is before that write was issued.
func (v *verified) canSkip(h types.UID, hRV string, p ID, pUID types.UID, n string) bool {
	if pUID == "" || hRV == "" {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.delivered[h] != hRV {
		return false
	}
	since, named := v.recordSince[h][p]
	if !named {
		return false // the delivered version doesn't name p
	}
	issued, ok := v.written[pUID][n]
	if !ok {
		return false
	}
	obs := since
	if o, ok := v.observed[h][p]; ok && o.After(obs) {
		obs = o
	}
	if v.lastList.After(obs) {
		obs = v.lastList
	}
	return obs.Before(issued)
}

// wrote records a successful A2 for P's incarnation pUID and namespace n,
// issued at t.
func (v *verified) wrote(pUID types.UID, n string, issued time.Time) {
	if pUID == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.written[pUID] == nil {
		v.written[pUID] = map[string]time.Time{}
	}
	v.written[pUID][n] = issued
}

// hasWritten reports whether this process holds a written entry for P's
// incarnation pUID and holder namespace n.
func (v *verified) hasWritten(pUID types.UID, n string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.written[pUID][n]
	return ok
}

// released drops the written entry for P's incarnation pUID and holder
// namespace n, at once, synchronously: a successful R3 removed this kind's
// finalizer for n (§6.2 R3). Entries for other namespaces stand on their own
// finalizers, which that R3 tested and left in place.
func (v *verified) released(pUID types.UID, n string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.written[pUID], n)
	if len(v.written[pUID]) == 0 {
		delete(v.written, pUID)
	}
}

// announced updates observed after a successful A1 (§6.2 A1, M1): refs it
// added get the time A1's response returned, overwriting any earlier value,
// and refs it removed lose theirs.
func (v *verified) announced(h types.UID, added, removed []ID, at time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(added) > 0 && v.observed[h] == nil {
		v.observed[h] = map[ID]time.Time{}
	}
	for _, p := range added {
		v.observed[h][p] = at
	}
	for _, p := range removed {
		delete(v.observed[h], p)
	}
}

// forgot drops a holder's observed entries, when its record is removed.
func (v *verified) forgot(h types.UID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.observed, h)
}

// holderEvent is the holder informer handler's only work on the verified set
// (§6.3): for a holder version delivered at at, it records the version's
// resourceVersion, and maintains recordSince. A version whose record names P
// starts or continues P's run; one that doesn't name P ends it, and drops
// observed for P too. An unparseable record names nothing: no skip rests on
// it, and the next A1 rewrites it. A nil h is a deletion.
func (v *verified) holderEvent(uid types.UID, h metav1.Object, at time.Time) {
	names := map[ID]bool{}
	if h != nil {
		if r := ReadRecord(h); !r.Unparseable {
			for _, p := range r.IDs {
				names[p] = true
			}
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if h == nil {
		delete(v.delivered, uid)
		delete(v.recordSince, uid)
		delete(v.observed, uid)
		return
	}
	v.delivered[uid] = h.GetResourceVersion()
	runs := v.recordSince[uid]
	for p := range runs {
		if !names[p] {
			delete(runs, p)
			delete(v.observed[uid], p)
		}
	}
	for p := range names {
		if runs == nil {
			runs = map[ID]time.Time{}
			v.recordSince[uid] = runs
		}
		if _, ok := runs[p]; !ok {
			runs[p] = at
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
	entries, ok := v.written[old.GetUID()]
	if !ok {
		return
	}
	if current == nil || current.GetUID() != old.GetUID() {
		delete(v.written, old.GetUID())
		return
	}
	for n := range entries {
		f, err := FinalizerName(v.holder, n)
		if err != nil || !hasFinalizer(current, f) {
			delete(entries, n)
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
