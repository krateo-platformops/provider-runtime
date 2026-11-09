package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
)

// A Reason says why a ref is not held.
type Reason string

const (
	// ReasonMissing: the object doesn't exist (§11.2).
	ReasonMissing Reason = "Missing"
	// ReasonDeleting: the object is being deleted and lacks the finalizer,
	// so no hold can be added (§11.4).
	ReasonDeleting Reason = "Deleting"
	// ReasonNotConsented: the object is in another namespace, and its
	// target hasn't consented (§11.3).
	ReasonNotConsented Reason = "NotConsented"
)

// Unheld is a ref that isn't held, and why.
type Unheld struct {
	Ref    Ref
	Reason Reason
}

// Acquired is what one Acquire did for one pass of one holder (§7.1).
type Acquired struct {
	// Held are the refs held in this pass.
	Held []Ref
	// Unheld are the refs that aren't, each with a reason.
	Unheld []Unheld
	// MustNotConnect: the acquire failed for a holder that isn't
	// established, or a ref is Deleting for one (§8, §11.4). The holder
	// must not read credentials this pass.
	MustNotConnect bool
	// Degraded: the acquire failed for an established holder, which
	// proceeds as today (fail open, §8). The error is in Err.
	Degraded bool
	Err      error
	// Reread: A1's response showed a generation or labels other than the
	// holder had before the patch, so a mutating webhook or defaulting
	// changed it, and the caller's copy may be stale beyond its metadata.
	// The caller re-reads the holder before its next write (§6.2 A1, L2 of
	// the review of revision 16).
	Reread bool
}

// ErrHolderConflict is a conflict on the holder's own record write (A1). The
// caller requeues at once (§8 step 5).
var ErrHolderConflict = errors.New("credential-use: the holder changed while its record was written")

// errChainsNotYet refuses refs that need chains, which come with the next
// part of the build (plan, PR 4).
var errChainsNotYet = errors.New("credential-use: refs with Next or Guaranteed aren't supported yet")

// maxGuardedAttempts bounds A2's GET-and-patch on a custom resource (§6.2).
const maxGuardedAttempts = 3

// protocol runs §6's acquiring and releasing steps for one holder kind K on
// behalf of one actor. Several protocols of one process share its verified
// set and its first-sight times.
type protocol struct {
	holder          schema.GroupKind
	ownerFinalizers []string
	grace           time.Duration
	clock           clock.PassiveClock
	srv             server
	views           views
	ver             *verified
	seen            *firstSight
	reports         reports
	newEpoch        func() (string, error)
}

// firstSight remembers when this process first saw each protected object's
// deletionTimestamp, for the grace (§5, L4).
type firstSight struct {
	mu sync.Mutex
	at map[types.UID]time.Time
}

func newFirstSight() *firstSight { return &firstSight{at: map[types.UID]time.Time{}} }

// see returns when uid's deletion was first seen, recording now if never.
func (f *firstSight) see(uid types.UID, now time.Time) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.at[uid]; ok {
		return t
	}
	f.at[uid] = now
	return now
}

// isBuiltin reports whether a protected kind takes a strategic merge patch:
// the core kinds, such as Secret and ConfigMap. Custom resources don't
// (§2).
func isBuiltin(gk schema.GroupKind) bool { return gk.Group == "" }

// acquire runs A1 and A2 for holder obj, which the caller has just read, and
// refs, which RefsFunc returned for it (§6.2). It runs on every pass,
// deletion passes included, before anything reads a protected object.
//
// It returns ErrHolderConflict when A1 conflicts. Every other failure is in
// the Acquired: MustNotConnect for a holder that isn't established, Degraded
// for one that is.
func (p *protocol) acquire(ctx context.Context, obj metav1.Object, refs []Ref) (Acquired, error) {
	var a Acquired
	established := IsEstablished(obj, p.ownerFinalizers)
	deleting := obj.GetDeletionTimestamp() != nil
	namespace := obj.GetNamespace()
	fail := func(err error, reason string) (Acquired, error) {
		a.Err = err
		if established {
			a.Degraded = true
			p.reports.Degraded(p.holder, reason)
		} else {
			a.MustNotConnect = true
		}
		return a, nil
	}

	// Which refs are held: those in the holder's own namespace. A ref in
	// another namespace needs its target's consent, which comes with a later
	// part of the build (plan, PR 5); until then it is never held, as for a
	// target that hasn't consented (§11.3).
	byID := map[ID]Ref{}
	for _, r := range refs {
		if r.Next != nil || r.Guaranteed {
			return fail(errChainsNotYet, "Refs")
		}
		if r.GroupKind.Kind == "" || r.Name == "" {
			return fail(fmt.Errorf("credential-use: ref %s has no kind or no name", r.ID()), "Refs")
		}
		if r.Namespace != namespace {
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonNotConsented})
			continue
		}
		byID[r.ID()] = r
	}
	want := SortedIDs(slices.Collect(maps.Keys(byID)))

	// A1. Announce.
	added, err := p.announce(ctx, obj, want, &a)
	if err != nil {
		if errors.Is(err, ErrHolderConflict) {
			return a, err
		}
		return fail(err, "Announce")
	}

	// A2. Write P, for each ref.
	for _, id := range want {
		r := byID[id]
		if !deleting && !added[id] {
			if cached, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), id, cached.GetUID(), namespace) {
				a.Held = append(a.Held, r)
				continue
			}
		}
		switch class, err := p.write(ctx, id, namespace); class {
		case ClassNone:
			a.Held = append(a.Held, r)
		case ClassNotFound:
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})
		case ClassDeleting:
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonDeleting})
			if !established {
				return fail(fmt.Errorf("credential %s %s/%s is being deleted and can't be held; not connecting until it is replaced or the reference changes", id.Kind, id.Namespace, id.Name), "Deleting")
			}
			a.Degraded, a.Err = true, err
			p.reports.Degraded(p.holder, "Deleting")
		default:
			return fail(fmt.Errorf("credential-use: holding %s: %w", id, err), class.String())
		}
	}
	return a, nil
}

// announce is A1: when the holder's record differs from want, it writes want
// with a merge patch guarded by the holder's resourceVersion, and updates
// observed in the same pass, synchronously. It returns the refs it added. An
// absent record and an empty one are the same; a record that can't be parsed
// is rewritten.
func (p *protocol) announce(ctx context.Context, obj metav1.Object, want []ID, a *Acquired) (map[ID]bool, error) {
	rec := ReadRecord(obj)
	if !rec.Unparseable && slices.Equal(rec.IDs, want) || !rec.Present && len(want) == 0 {
		return nil, nil
	}
	var value any // null removes the annotation
	if len(want) > 0 {
		v, err := EncodeRecord(want)
		if err != nil {
			return nil, err
		}
		value = v
	}
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": obj.GetResourceVersion(),
		"annotations":     map[string]any{AnnotationRefs: value},
	}})
	if err != nil {
		return nil, err
	}
	generation, labels := obj.GetGeneration(), obj.GetLabels()
	out, err := p.srv.Patch(ctx, p.holder, obj.GetNamespace(), obj.GetName(), types.MergePatchType, data)
	if err != nil {
		if Classify(err) == ClassConflict {
			return nil, fmt.Errorf("%w: %w", ErrHolderConflict, err)
		}
		return nil, fmt.Errorf("credential-use: writing the record: %w", err)
	}
	at := p.clock.Now()
	copyMeta(obj, &out.ObjectMeta)
	if out.Generation != generation || !maps.Equal(out.Labels, labels) {
		a.Reread = true
	}

	before := map[ID]bool{}
	if !rec.Unparseable {
		for _, id := range rec.IDs {
			before[id] = true
		}
	}
	added := map[ID]bool{}
	var addedList, removed []ID
	for _, id := range want {
		if !before[id] {
			added[id] = true
			addedList = append(addedList, id)
		}
		delete(before, id)
	}
	for id := range before {
		removed = append(removed, id)
	}
	if rec.Unparseable {
		// Nothing was known to be named: every ref is new to this pass.
		for _, id := range want {
			added[id] = true
		}
		addedList = want
	}
	p.ver.announced(obj.GetUID(), addedList, removed, at)
	return added, nil
}

// write is A2 for one ref: it returns ClassNone when P now carries the
// finalizer and a fresh epoch, and otherwise the class of the failure:
// ClassNotFound (Missing), ClassDeleting, or anything else (Degraded, or fail
// closed). A successful write is recorded in written, keyed by the
// incarnation the response names, at the time the request was issued.
func (p *protocol) write(ctx context.Context, id ID, namespace string) (Class, error) {
	epoch, err := p.newEpoch()
	if err != nil {
		return ClassOther, err
	}
	gk := id.GroupKind()
	if isBuiltin(gk) {
		pt, err := AcquireStrategicPatch(p.holder, namespace, epoch)
		if err != nil {
			return ClassOther, err
		}
		issued := p.clock.Now()
		out, err := p.srv.Patch(ctx, gk, id.Namespace, id.Name, pt.Type, pt.Data)
		if err != nil {
			return Classify(err), err
		}
		p.ver.wrote(out.UID, namespace, issued)
		return ClassNone, nil
	}
	var last error
	for range maxGuardedAttempts {
		current, err := p.srv.Get(ctx, gk, id.Namespace, id.Name)
		if err != nil {
			return Classify(err), err
		}
		pt, err := AcquireGuardedPatch(current, p.holder, namespace, epoch)
		if err != nil {
			return ClassOther, err
		}
		issued := p.clock.Now()
		out, err := p.srv.Patch(ctx, gk, id.Namespace, id.Name, pt.Type, pt.Data)
		if err == nil {
			p.ver.wrote(out.UID, namespace, issued)
			return ClassNone, nil
		}
		if c := Classify(err); c != ClassConflict {
			return c, err
		}
		last = err
	}
	return ClassOther, fmt.Errorf("credential-use: %d conflicting writes in a row: %w", maxGuardedAttempts, last)
}

// beforeEstablish runs just before the caller adds its owner finalizer, and
// only when it is absent (§8 step 8, §6.7): it reads every ref that was
// Missing live, and holds any that now exists. On an error, the caller
// doesn't add its finalizer this pass.
func (p *protocol) beforeEstablish(ctx context.Context, obj metav1.Object, a Acquired) error {
	for _, u := range a.Unheld {
		if u.Reason != ReasonMissing {
			continue
		}
		id := u.Ref.ID()
		_, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
		switch Classify(err) {
		case ClassNotFound:
			continue
		case ClassNone:
		default:
			return fmt.Errorf("credential-use: checking %s before establishing: %w", id, err)
		}
		if class, err := p.write(ctx, id, obj.GetNamespace()); class != ClassNone {
			return fmt.Errorf("credential-use: holding %s before establishing (%s): %w", id, class, err)
		}
	}
	return nil
}

// forget removes the record, and the unheld list, from obj in memory, so that
// the caller's owner-finalizer removal writes it in the same write (§8). It
// reports whether anything changed.
func (p *protocol) forget(obj metav1.Object) bool {
	an := obj.GetAnnotations()
	_, hasRefs := an[AnnotationRefs]
	_, hasUnheld := an[AnnotationUnheld]
	if !hasRefs && !hasUnheld {
		return false
	}
	an = maps.Clone(an)
	delete(an, AnnotationRefs)
	delete(an, AnnotationUnheld)
	obj.SetAnnotations(an)
	p.ver.forgot(obj.GetUID())
	return true
}

// copyMeta copies everything the server may have set in src onto dst.
func copyMeta(dst metav1.Object, src *metav1.ObjectMeta) {
	dst.SetResourceVersion(src.ResourceVersion)
	dst.SetGeneration(src.Generation)
	dst.SetLabels(src.Labels)
	dst.SetAnnotations(src.Annotations)
	dst.SetFinalizers(src.Finalizers)
	dst.SetOwnerReferences(src.OwnerReferences)
	dst.SetManagedFields(src.ManagedFields)
	dst.SetDeletionTimestamp(src.DeletionTimestamp)
	dst.SetDeletionGracePeriodSeconds(src.DeletionGracePeriodSeconds)
}
