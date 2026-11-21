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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	// ReasonConsentWithdrawn: it was held, and its target withdrew consent:
	// this pass released it (§11.3).
	ReasonConsentWithdrawn Reason = "ConsentWithdrawn"
	// ReasonConsentUnreadable: it isn't held yet, and its consent couldn't
	// be read; RetryAfter asks for the next try (§11.3).
	ReasonConsentUnreadable Reason = "ConsentUnreadable"
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

	// RetryAfter, when set, asks the caller to requeue the holder after it:
	// a live consent read failed, so the pass kept the hold, or held
	// nothing new, without knowing (§11.3, §8 step 5). It backs off per
	// (holder, ref).
	RetryAfter time.Duration

	// resolved are the objects read for each ref with a Next that was
	// followed: held, unheld, or Guaranteed (§9.2). WithResolved hands them
	// on.
	resolved map[ID]*unstructured.Unstructured
}

// ErrHolderConflict is a conflict on the holder's own record write (A1). The
// caller requeues at once (§8 step 5).
var ErrHolderConflict = errors.New("credential-use: the holder changed while its record was written")

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
	// restoredEpochRequeue, if set, replaces the constant of that name
	// (epochRequeue).
	restoredEpochRequeue time.Duration
	// heldDeleting, if set, is told of every object an A2 held that its
	// response shows being deleted, with the holder namespace.
	heldDeleting func(id ID, namespace string)

	// Chains (§9): maxDepth bounds the levels; contentKinds are the
	// protected kinds with Content, read from contentCache or, live, through
	// contents, which also reads Guaranteed refs without a lister in
	// listers. A protocol with no chains needs none of them.
	maxDepth     int
	contentKinds map[schema.GroupKind]bool
	contentCache contentCache
	contents     contentReader
	listers      map[schema.GroupKind]Lister

	// Consent (§11.3): namespaceViews answers the gate's cached read of a
	// Namespace's consent; backoff paces the requeues of a failed live read;
	// unheldReads shares the live read of an unheld hop among holders for
	// 10s (§9.4); enqueueBatch adds the (K, N) release batch the acquirer
	// owes after each A2 of a cross-namespace ref (§10.2 L2). A protocol
	// without cross-namespace refs needs none of them.
	namespaceViews namespaceViews
	backoff        *consentBackoff
	unheldReads    *sharedReads
	enqueueBatch   func(id ID, holderNamespace string)
}

// firstSight remembers when this process first saw each protected object's
// deletionTimestamp, for the grace (§5, L4).
type firstSight struct {
	mu sync.Mutex
	at map[types.UID]time.Time

	// onAccess and onRead, nil in production: as verified's.
	onAccess func()
	onRead   func(any)
}

func (f *firstSight) read(r any) {
	if f.onRead != nil {
		f.onRead(r)
	}
}

func (f *firstSight) access() {
	if f.onAccess != nil {
		f.onAccess()
	}
}

func newFirstSight() *firstSight { return &firstSight{at: map[types.UID]time.Time{}} }

// forget drops uid, once its object is released or gone, so that the map
// holds only objects being deleted that this process still holds.
func (f *firstSight) forget(uid types.UID) {
	f.access()
	if uid == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.at, uid)
}

// see returns when uid's deletion was first seen, recording now if never.
func (f *firstSight) see(uid types.UID, now time.Time) time.Time {
	f.access()
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.at[uid]; ok {
		f.read(t)
		return t
	}
	f.at[uid] = now
	f.read(now)
	return now
}

// isBuiltin reports whether a protected kind takes a strategic merge patch:
// the core kinds, such as Secret and ConfigMap. Custom resources don't
// (§2).
func isBuiltin(gk schema.GroupKind) bool { return gk.Group == "" }

// acquire runs A1 and A2 for holder obj, which the caller has just read, and
// refs, which RefsFunc returned for it (§6.2), and follows their chains
// (§9.1). It runs on every pass, deletion passes included, before anything
// reads a protected object.
//
// The chain is followed level by level, and each hop is held before it is
// read: A1 for the level, A2 for each of its refs, then a read of each held
// ref with a Next, whose refs are the next level. An A1 between levels only
// adds to the record, so a ref of a deeper level that the record names isn't
// dropped from it, and so released, before the pass reaches it again; the
// last A1 writes exactly what the whole chain names. A Guaranteed ref is
// neither recorded nor held, and its Next is followed (§9.3).
//
// It returns ErrHolderConflict when an A1 conflicts. Every other failure is
// in the Acquired: MustNotConnect for a holder that isn't established,
// Degraded for one that is.
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
	maxDepth := p.maxDepth
	if maxDepth == 0 {
		maxDepth = DefaultMaxDepth
	}

	byID := map[ID]Ref{} // every held ref of the chain so far
	seen := map[ID]bool{}
	// unfollowed: a hop with a Next wasn't held this pass (Missing or
	// Deleting), so what lies behind it is unknown. The record then keeps
	// every ref it names: a ref behind that hop must stay held, as the
	// record was (§9.1, §8), while the holder may still read through it.
	// The record stores IDs, not which hop led to which, so a ref the
	// holder stopped naming for another reason is kept too, until a pass
	// follows the whole chain again: a longer hold, which §5 bounds.
	unfollowed := false
	rec0 := ReadRecord(obj)     // the record as the pass found it
	listed := map[string]bool{} // the unheld entries as the pass found them
	for _, e := range ReadUnheld(obj.GetAnnotations()) {
		listed[e.Ref] = true
	}
	// The cross-namespace refs, by what the gate found (§11.3): liveOK, held
	// on a live consent read this pass; notHeld, without consent, by
	// reason; keepEntry, those whose state this pass couldn't learn, which
	// keep their unheld entry as it was.
	liveOK := map[ID]bool{}
	cross := map[ID]bool{}
	notHeld := map[ID]string{}
	keepEntry := map[ID]bool{}
	// exact is what the pass's A1 writes: the chain, and with a hop
	// unfollowed the record's refs too, but never one dropped for consent:
	// the target's owner withdrew it (§11.3).
	exact := func() []ID {
		want := SortedIDs(slices.Collect(maps.Keys(byID)))
		if unfollowed {
			if rec := ReadRecord(obj); !rec.Unparseable {
				want = SortedIDs(append(want, rec.IDs...))
			}
		}
		return slices.DeleteFunc(want, func(id ID) bool { _, dropped := notHeld[id]; return dropped })
	}
	retry := func(id ID) {
		d := p.backoff.fail(consentKey{obj.GetUID(), id})
		if a.RetryAfter == 0 || d < a.RetryAfter {
			a.RetryAfter = d
		}
	}
	// drop records r as not held for want of consent, reporting a
	// withdrawal when it was recorded, and follows its Next through the
	// shared live read: an unheld hop doesn't stop the chain (§9.4).
	drop := func(r Ref) ([]Ref, error) {
		id := r.ID()
		delete(byID, id)
		reason := UnheldNotConsented
		switch {
		case rec0.Names(id):
			reason = UnheldConsentWithdrawn
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonConsentWithdrawn})
			p.reports.ConsentWithdrawn(p.holder, obj, id)
		default:
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonNotConsented})
			if !listed[unheldRef(id)] {
				// Reported once, when the ref first goes unheld.
				p.reports.RefNotHeld(p.holder, obj, id)
			}
		}
		notHeld[id] = reason
		if r.Next == nil {
			return nil, nil
		}
		o, err := p.unheldReads.get(ctx, p.contents, id)
		if Classify(err) == ClassNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("credential-use: reading the unheld hop %s: %w", id, err)
		}
		return p.resolve(&a, r, o)
	}
	level := refs
	for depth := 1; len(level) > 0; depth++ {
		if depth > maxDepth {
			return fail(fmt.Errorf("%w (%d)", errChainTooDeep, maxDepth), "Refs")
		}
		// Which refs of this level are held: those in the holder's own
		// namespace, and those in another whose target consents (§11.3).
		var toHold []ID
		var guaranteed []Ref
		var next []Ref
		chained := false
		for _, r := range level {
			if r.GroupKind.Kind == "" || r.Name == "" {
				return fail(fmt.Errorf("credential-use: ref %s has no kind or no name", r.ID()), "Refs")
			}
			id := r.ID()
			if seen[id] {
				continue
			}
			seen[id] = true
			if r.Next != nil {
				chained = true
			}
			if r.Guaranteed {
				guaranteed = append(guaranteed, r)
				continue
			}
			if r.Next != nil && !p.contentKinds[r.GroupKind] {
				// Its re-point would be invisible: the metadata caches keep
				// only credential-use. keys, and a metadata change moves no
				// generation, so nothing would enqueue its holders (§9.2,
				// §10.4; the user's decision, 2026-11-21).
				return fail(fmt.Errorf("%w: %s (set Content on its ProtectedKind)", errNextNeedsContent, id), "Refs")
			}
			if r.Namespace == namespace {
				byID[id] = r
				toHold = append(toHold, id)
				continue
			}
			// The gate, before A1: live in the adding direction, and from
			// the cache on a pass that may skip A2 (§11.3, M1).
			cross[id] = true
			switch g := p.gate(ctx, obj, id, rec0.Names(id) && !deleting); g {
			case gateHoldLive, gateHoldCached:
				liveOK[id] = g == gateHoldLive
				byID[id] = r
				toHold = append(toHold, id)
			case gateNotHeld:
				more, err := drop(r)
				if err != nil {
					return fail(err, "Refs")
				}
				next = append(next, more...)
			case gateMissing:
				// Nothing to hold; the record keeps it, as for any Missing
				// ref, and nothing is requeued for it (L1).
				byID[id] = r
				keepEntry[id] = true
				a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})
				unfollowed = unfollowed || r.Next != nil
			case gateUnknown:
				retry(id)
				keepEntry[id] = true
				if rec0.Names(id) {
					// A failed read keeps the hold (A5): it stays recorded,
					// and is held, without an A2.
					byID[id] = r
					a.Held = append(a.Held, r)
					unfollowed = unfollowed || r.Next != nil
				} else {
					a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonConsentUnreadable})
				}
			}
		}

		// A1. Announce: exactly the chain when no hop of this level leads on,
		// and otherwise the chain so far added to the record as it stands. An
		// unparseable record names nothing to keep, so a chain behind it is
		// exposed until the last A1, as for any ref of such a holder; only
		// someone else's write makes a record unparseable.
		want := exact()
		if chained {
			if rec := ReadRecord(obj); !rec.Unparseable {
				want = SortedIDs(append(want, rec.IDs...))
			}
		}
		added, err := p.announce(ctx, obj, want, nil, &a)
		if err != nil {
			if errors.Is(err, ErrHolderConflict) {
				return a, err
			}
			return fail(err, "Announce")
		}

		// A2. Write P, for each ref of this level. A skip rests on the exact
		// holder version this pass works from (§6.3; H1 of #59's review):
		// after an A1, that is A1's response, which the informer may not have
		// delivered yet.
		slices.SortFunc(toHold, compareIDs)
		for _, id := range toHold {
			r := byID[id]
			held := false
			if !deleting && !added[id] {
				if cached, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), obj.GetResourceVersion(), id, cached.GetUID(), namespace) {
					a.Held = append(a.Held, r)
					held = true
				}
			}
			if !held && cross[id] && !liveOK[id] {
				// The gate read consent from the cache, and the skip didn't
				// hold: every A2 of a cross-namespace ref follows a live
				// consent read in the same pass (§11.3, rule 1).
				switch p.liveConsent(ctx, obj, id) {
				case gateHoldLive:
					liveOK[id] = true
				case gateNotHeld:
					more, err := drop(r)
					if err != nil {
						return fail(err, "Refs")
					}
					next = append(next, more...)
					continue
				case gateMissing:
					keepEntry[id] = true
					a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})
					unfollowed = unfollowed || r.Next != nil
					continue
				default:
					retry(id)
					keepEntry[id] = true
					a.Held = append(a.Held, r)
					unfollowed = unfollowed || r.Next != nil
					continue
				}
			}
			if !held {
				switch class, err := p.write(ctx, id, namespace); class {
				case ClassNone:
					a.Held = append(a.Held, r)
					held = true
					if cross[id] && p.enqueueBatch != nil {
						// The backstop for an A2 that lands after a release
						// (§11.3 rule 3, §10.2 L2): this batch runs after it.
						p.enqueueBatch(id, namespace)
					}
				case ClassNotFound:
					a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonMissing})
					unfollowed = unfollowed || r.Next != nil
				case ClassDeleting:
					a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonDeleting})
					unfollowed = unfollowed || r.Next != nil
					if !established {
						return fail(fmt.Errorf("credential %s %s/%s is being deleted and can't be held; not connecting until it is replaced or the reference changes", id.Kind, id.Namespace, id.Name), "Deleting")
					}
					a.Degraded, a.Err = true, err
					p.reports.Degraded(p.holder, "Deleting")
				default:
					if class == ClassAdmissionDenied {
						p.reports.AdmissionDenied(p.holder)
					}
					return fail(fmt.Errorf("credential-use: holding %s: %w", id, err), class.String())
				}
			}
			// Read it only once it is held, and follow its Next (§9.1).
			if held && r.Next != nil {
				o, err := p.readHop(ctx, id)
				if err != nil {
					return fail(fmt.Errorf("credential-use: reading %s: %w", id, err), "Refs")
				}
				more, err := p.resolve(&a, r, o)
				if err != nil {
					return fail(err, "Refs")
				}
				next = append(next, more...)
			}
		}
		for _, r := range guaranteed {
			if r.Next == nil {
				continue
			}
			o, err := p.readGuaranteed(ctx, r)
			if err != nil {
				// A Guaranteed ref that can't be read breaks its guarantee; it
				// is never Missing, which BeforeEstablish would then hold.
				return fail(fmt.Errorf("credential-use: reading the guaranteed ref %s: %w", r.ID(), err), "Refs")
			}
			more, err := p.resolve(&a, r, o)
			if err != nil {
				return fail(err, "Refs")
			}
			next = append(next, more...)
		}
		// Refs already met end there, so a cycle ends, and isn't counted
		// against MaxDepth.
		level = slices.DeleteFunc(next, func(r Ref) bool { return seen[r.ID()] })
	}
	for id := range cross {
		if liveOK[id] {
			p.backoff.succeed(consentKey{obj.GetUID(), id})
		}
	}

	// The last A1: exactly the chain, and the unheld list with it, in one
	// patch (§11.3). It writes nothing when neither changed.
	unheld := p.unheldEntries(obj, notHeld, keepEntry)
	if _, err := p.announce(ctx, obj, exact(), &unheld, &a); err != nil {
		if errors.Is(err, ErrHolderConflict) {
			return a, err
		}
		return fail(err, "Announce")
	}
	return a, nil
}

// resolve keeps o as what was read for r (Resolved) and returns the refs r's
// Next finds in it.
func (p *protocol) resolve(a *Acquired, r Ref, o *unstructured.Unstructured) ([]Ref, error) {
	if a.resolved == nil {
		a.resolved = map[ID]*unstructured.Unstructured{}
	}
	a.resolved[r.ID()] = o
	return follow(r, o)
}

// announce is A1: when the holder's record differs from want, it writes want
// with a merge patch guarded by the holder's resourceVersion, and updates
// observed in the same pass, synchronously. It returns the refs it added. An
// absent record and an empty one are the same; a record that can't be parsed
// is rewritten.
//
// With unheld, it writes the holder's unheld list in the same patch (§11.3),
// and counts a change to it as a difference; with nil, it leaves the list as
// it is.
func (p *protocol) announce(ctx context.Context, obj metav1.Object, want []ID, unheld *[]UnheldEntry, a *Acquired) (map[ID]bool, error) {
	rec := ReadRecord(obj)
	sameRecord := !rec.Unparseable && slices.Equal(rec.IDs, want) || !rec.Present && len(want) == 0
	var unheldValue *string
	sameUnheld := true
	if unheld != nil {
		v, err := encodeUnheld(*unheld)
		if err != nil {
			return nil, err
		}
		unheldValue = v
		cur, has := obj.GetAnnotations()[AnnotationUnheld]
		sameUnheld = v == nil && !has || v != nil && has && cur == *v
	}
	if sameRecord && sameUnheld {
		return nil, nil
	}
	annotations := map[string]any{}
	if !sameRecord {
		var value any // null removes the annotation
		if len(want) > 0 {
			v, err := EncodeRecord(want)
			if err != nil {
				return nil, err
			}
			value = v
		}
		annotations[AnnotationRefs] = value
	}
	if !sameUnheld {
		var value any
		if unheldValue != nil {
			value = *unheldValue
		}
		annotations[AnnotationUnheld] = value
	}
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": obj.GetResourceVersion(),
		"annotations":     annotations,
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
	// An unparseable record names nothing (before is empty), so every ref
	// is new to this pass.
	for id := range before {
		removed = append(removed, id)
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
		p.wroteDeleting(id, namespace, out)
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
			p.wroteDeleting(id, namespace, out)
			return ClassNone, nil
		}
		if c := Classify(err); c != ClassConflict {
			return c, err
		}
		last = err
	}
	return ClassOther, fmt.Errorf("credential-use: %d conflicting writes in a row: %w", maxGuardedAttempts, last)
}

// wroteDeleting tells heldDeleting of an A2 response that shows its object
// being deleted.
func (p *protocol) wroteDeleting(id ID, namespace string, out *metav1.PartialObjectMetadata) {
	if p.heldDeleting != nil && out != nil && out.DeletionTimestamp != nil {
		p.heldDeleting(id, namespace)
	}
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
		if u.Ref.Next != nil {
			// It is held now, but what it leads to isn't: establishing now
			// would leave the end of the chain unprotected. The next pass
			// follows it (§9.1).
			return fmt.Errorf("credential-use: %s appeared before establishing, and its chain isn't held yet", id)
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
