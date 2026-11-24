package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
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
	// ReasonDisowned: the holder kind's Disown switch covers the ref's kind,
	// so nothing holds it, and the holder proceeds as today (§7.4, A13). It
	// is never written to the unheld annotation.
	ReasonDisowned Reason = "Disowned"
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
	// on. Production follows none in this release (ErrChainsNotSupported).
	resolved map[ID]*unstructured.Unstructured

	// gateErr is why the gate of recorded refs failed (§11.3), in a pass whose
	// chain was refused: Err joins the two.
	gateErr error
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
	// a2Bound bounds an A2 whose caller's context has no deadline: the
	// kind's TimeoutBound; 0 is no bound.
	a2Bound time.Duration
	// a2Grace and callGrace, if set, replace the constants of those names.
	a2Grace, callGrace time.Duration
	// restoredEpochRequeue, if set, replaces the constant of that name
	// (epochRequeue).
	restoredEpochRequeue time.Duration
	// mustWait, if set, reports whether the release of o, which is being
	// deleted, for holder namespace n must still wait (§6.4). The models have
	// none: their API server removes an object in one step.
	mustWait func(n string, o Waiting) bool
	// calls, if set, counts the calls in flight, from detach to their stop.
	calls *callsInFlight
	// The release's own (release.go): readRecord, if set, is how it reads a
	// holder's record, for tests to count; turnTime, if set, replaces the
	// constant of that name; readLimit, if set, is how long the read of an
	// object may take, in holder namespace n.
	readRecord func(metav1.Object) Record
	turnTime   time.Duration
	readLimit  func(n string, id ID) time.Duration
	// heldDeleting, if set, is told of every object an A2 held that its
	// response shows being deleted, with the holder namespace.
	heldDeleting func(id ID, namespace string)

	// followChains has acquire follow a Next, which it refuses otherwise
	// (refuseChains). Only the tests set it.
	followChains bool
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
	// The gate of recorded refs (§11.3): protects reports whether a kind is
	// protected, and cacheLacks whether an ID's cache covers it, has synced
	// and lacks it. Nil ones are every kind, and never. gateContext gives its
	// live reads their context; nil is the pass's.
	protects     func(schema.GroupKind) bool
	cacheLacks   func(ID) bool
	gateTurns    *gateTurns
	gateContext  func(context.Context) (context.Context, context.CancelFunc)
	backoff      *consentBackoff
	reported     *reportedSet
	unheldReads  *sharedReads
	enqueueBatch func(id ID, holderNamespace string)

	// conflicts remembers which refs were reported as conflicting (§9.1); nil
	// remembers nothing.
	conflicts *conflictSet

	// written remembers what each holder's last A1 left, for the holder
	// event filter (IsOwnRecordWrite, §7.5); nil remembers nothing.
	written *recordWrites

	// disowned reports whether the holder kind's Disown switch covers a
	// protected kind now (§7.4); nil is never. Acquire asks it for every
	// ref, and again just before each A2 (§10.6.8, interleaving 3).
	disowned func(schema.GroupKind) bool
	// disownLive reads the switch live, from the CRD, so that a stale CRD
	// informer can't let an A2 through after the switch: once per pass,
	// just before its first A2 (the user's decisions, 2026-11-22). nil is
	// never.
	disownLive func(context.Context) (disownSet, error)

	// noHolders answers for R2's list of holders that failed with listErr:
	// nil when the holder kind is gone, so that there are none (§6.4), and
	// otherwise the batch's error. It must read live, and is called after
	// R1's reads. nil knows of no kind that is gone.
	noHolders func(ctx context.Context, listErr error) error
	// releasedGone, if set, is told of an object that R3 released for want of
	// the holder kind.
	releasedGone func(id ID)
	// kindAbsent reports whether the holder kind's CRD is absent from its
	// informer, so that the holder cache may never catch up: R0 and R1's
	// grace shortcut then take nothing from it (§6.2). A hint: it decides
	// nothing. nil is never.
	kindAbsent func() bool

	// lines keeps the index's lines for the PerName kinds (§10.6.2): the
	// line before each A2, and the live check after it. Nil keeps none.
	lines lineKeeper
}

// lineKeeper is what the acquirer asks of the index (§10.6.2).
type lineKeeper interface {
	// covers reports whether refs of kind gk have lines.
	covers(gk schema.GroupKind) bool
	// ensure adds id's line unless the index's view shows it: before A2,
	// and on a pass that skips it, so that a finalizer is never written
	// without its line having been written first.
	ensure(ctx context.Context, id ID) error
	// postCheck reads the index live after a successful A2, and adds id's
	// line again if a concurrent prune removed it.
	postCheck(ctx context.Context, id ID) error
	// unknownA2 is told of an A2 for holder namespace n whose outcome is
	// unknown, so that it may have landed, or may yet: it checks id's line as
	// postCheck does, and checks id again once the landing margin is over.
	unknownA2(ctx context.Context, id ID, n string)
	// holding is told of an A2 of id from before its line is ensured until
	// the function it returns is called, once its outcome is known, or
	// unknownA2 told of it: no prune of this process takes its line then.
	holding(id ID) (done func())
}

const (
	// a2Grace is how long an A2 in flight goes on once its caller's context
	// has ended before its deadline, as a stop ends it: a write cut short
	// may have landed, or may yet, and nobody would know (§6.2).
	a2Grace = 10 * time.Second
	// callGrace is how long after its caller's context has ended one Acquire
	// or BeforeEstablish goes on at most, its last A2 and the line check
	// after it included, however many refs it holds (§7.2).
	callGrace = 15 * time.Second
)

// callContexts are one call's contexts for what mustn't be cut short: they
// keep the caller's values, and not its cancellation.
type callContexts struct {
	// write is an A2's: it ends at the caller's deadline, or at the
	// protocol's own bound without one, or a2Grace after the caller's
	// context ended.
	write context.Context
	// check is the line check's after it: it ends callGrace after the
	// caller's context ended.
	check context.Context
	stop  func()
}

// graces are a2Grace and callGrace, or a test's own.
func (p *protocol) graces() (a2, call time.Duration) {
	a2, call = a2Grace, callGrace
	if p.a2Grace > 0 {
		a2 = p.a2Grace
	}
	if p.callGrace > 0 {
		call = p.callGrace
	}
	return a2, call
}

// checkIn is cc's context for one line check, which may take callGrace even
// while the caller's context lives: an API server that hangs mustn't hold a
// pass without a deadline.
func (p *protocol) checkIn(cc callContexts) (context.Context, context.CancelFunc) {
	_, call := p.graces()
	return context.WithTimeout(cc.check, call)
}

// detach returns the contexts of one call under ctx.
func (p *protocol) detach(ctx context.Context) callContexts {
	ended := func() {}
	if p.calls != nil {
		ended = p.calls.begin()
	}
	a2, call := p.graces()
	base := context.WithoutCancel(ctx)
	check, endCheck := context.WithCancel(base)
	var write context.Context
	var endWrite context.CancelFunc
	if d, ok := ctx.Deadline(); ok {
		write, endWrite = context.WithDeadline(base, d)
	} else if p.a2Bound > 0 {
		write, endWrite = context.WithTimeout(base, p.a2Bound)
	} else {
		write, endWrite = context.WithCancel(base)
	}
	var mu sync.Mutex
	var timers []*time.Timer
	stopped := false
	unhook := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if !stopped {
			timers = append(timers, time.AfterFunc(a2, endWrite), time.AfterFunc(call, endCheck))
		}
	})
	return callContexts{write: write, check: check, stop: func() {
		unhook()
		mu.Lock()
		stopped = true
		for _, t := range timers {
			t.Stop()
		}
		mu.Unlock()
		endWrite()
		endCheck()
		ended()
	}}
}

// mayHaveLanded reports whether a write that failed with err may have
// landed, or may yet: anything but an answer of the API server that refuses
// it, a 4xx. A patch's 404 that no read confirmed is one (refusedPatch).
func mayHaveLanded(err error) bool {
	var refused refusedPatch
	if errors.As(err, &refused) {
		return false
	}
	st, ok := statusOf(err)
	return err != nil && (!ok || st.Code < 400 || st.Code >= 500 || st.Code == http.StatusRequestTimeout)
}

// keepsLine reports whether refs of kind gk have index lines.
func (p *protocol) keepsLine(gk schema.GroupKind) bool {
	return p.lines != nil && p.lines.covers(gk)
}

// liveDisown returns a pass's live read of the switch: made once, before the
// pass's first A2, and reused for the rest of the pass. A switch set later in
// the pass is the window of §10.6.8's interleaving 3, which the pass's own
// duration, under TimeoutBound, bounds.
func (p *protocol) liveDisown(ctx context.Context) func(schema.GroupKind) (bool, error) {
	var set *disownSet
	return func(gk schema.GroupKind) (bool, error) {
		if p.disownLive == nil {
			return false, nil
		}
		if set == nil {
			s, err := p.disownLive(ctx)
			if err != nil {
				return false, err
			}
			set = &s
		}
		return set.covers(gk), nil
	}
}

// isDisowned is disowned, false without one.
func (p *protocol) isDisowned(gk schema.GroupKind) bool {
	return p.disowned != nil && p.disowned(gk)
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
// (§9.1). Production refuses a list with a chain in it, whole, before it looks
// at the list (refuseChains); only tests set followChains. It runs on every
// pass, deletion passes included, before anything reads a protected object.
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
	cc := p.detach(ctx)
	defer cc.stop()
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

	// The gate of what the record names, first (§11.3). What follows reads
	// the record, and the unheld list, as it leaves them.
	recorded, err := p.gateRecorded(ctx, obj, &a)
	if errors.Is(err, ErrHolderConflict) {
		return a, err
	}
	// Then chains are refused (§9, "Not in this release"), before the list is
	// looked at: the pass reads and writes nothing more, so an established
	// holder's record, and its holds with it, stay as the gate leaves them.
	// Not before the gate: it looks at no ref, and a holder that could keep it
	// from running, by gaining a chain, would keep what its record names held
	// against the owner's withdrawal. Refs are found inside another only by
	// its Next, so a list that passes is the whole chain: one level, all
	// direct.
	//
	// A gate that failed is counted as well, and kept apart, so that it is
	// reported though the refusal was already (reportOutcome).
	if refused := p.refuseChains(refs); refused != nil {
		if a.gateErr = err; err != nil && established {
			p.reports.Degraded(p.holder, "Announce")
		}
		return fail(errors.Join(refused, err), reasonChainsRefused)
	}
	if err != nil {
		return fail(err, "Announce")
	}

	byID := map[ID]Ref{} // every held ref of the chain so far
	seen := map[ID]Ref{} // the first ref met of each object, which wins (§9.1)
	// asked is what later refs of an object asked for that the first doesn't.
	asked := map[ID]asks{}
	// met reports whether r's object was met. It reports what r asks for that
	// the first ref of the object doesn't, once per holder, object and thing
	// asked for, and a hold not of a Disowned kind (§9.1). It reads nothing.
	met := func(r Ref) bool {
		id := r.ID()
		first, ok := seen[id]
		if !ok {
			return false
		}
		hold, next := r.AsksMoreThan(first)
		before, told := asked[id], p.conflicts.told(obj.GetUID(), id)
		asked[id] = asks{before.hold || hold, before.next || next}
		hold = hold && !told.hold && !p.isDisowned(id.GroupKind())
		next = next && !told.next
		if hold || next {
			p.conflicts.tell(obj.GetUID(), id, asks{hold, next})
			p.reports.RefsConflicting(p.holder, obj, id, hold, next)
		}
		return true
	}
	// unfollowed: a hop with a Next wasn't held this pass (Missing or
	// Deleting), so what lies behind it is unknown. The record then keeps
	// every ref it names: a ref behind that hop must stay held, as the
	// record was (§9.1, §8), while the holder may still read through it.
	// The record stores IDs, not which hop led to which, so a ref the
	// holder stopped naming for another reason is kept too, until a pass
	// follows the whole chain again: a longer hold, which §5 bounds.
	unfollowed := false
	// metAll forgets the conflicts that are no more, once the pass has met all
	// the holder's refs. A pass that can't, such as one behind a Missing hop,
	// forgets none.
	metAll := func() {
		if !unfollowed {
			p.conflicts.keep(obj.GetUID(), asked)
		}
	}
	rec0 := ReadRecord(obj)                   // the record as the pass found it
	found := ReadUnheld(obj.GetAnnotations()) // the unheld list as the pass found it
	listed := map[string]bool{}
	for _, e := range found {
		listed[e.Ref] = true
	}
	// The cross-namespace refs, by what the gate found (§11.3): liveOK, held
	// on a live consent read this pass; notHeld, without consent, by
	// reason; keepEntry, those whose state this pass couldn't learn, which
	// keep their unheld entry as it was.
	liveOK := map[ID]bool{}
	// readOK: a live consent read of the ref succeeded this pass, whatever it
	// answered; its backoff goes back to 1s (§11.3).
	readOK := map[ID]bool{}
	var passNow time.Time // the pass's time for a new unheld entry
	cross := map[ID]bool{}
	notHeld := map[ID]string{}
	keepEntry := map[ID]bool{}
	// disowned: refs of a kind the Disown switch covers, which the pass
	// neither holds nor records (§7.4).
	disowned := map[ID]bool{}
	// unrecorded: a ref no A1 may write: one dropped for consent, whose
	// target's owner withdrew it (§11.3), or one of a kind the Disown switch
	// covers, which the Disown loop strips from every record (§7.4).
	unrecorded := func(id ID) bool {
		_, dropped := notHeld[id]
		return dropped || disowned[id] || p.isDisowned(id.GroupKind())
	}
	// exact is what the pass's A1 writes: the chain, and with a hop
	// unfollowed the record's refs too, but never an unrecorded one.
	exact := func() []ID {
		want := SortedIDs(slices.Collect(maps.Keys(byID)))
		if unfollowed {
			if rec := ReadRecord(obj); !rec.Unparseable {
				want = SortedIDs(append(want, rec.IDs...))
			}
		}
		return slices.DeleteFunc(want, unrecorded)
	}
	retry := func(id ID) { a.retryIn(p.backoff.fail(consentKey{obj.GetUID(), id})) }
	// lastUnheld is the unheld list that the pass's last A1 writes.
	lastUnheld := func() []UnheldEntry {
		u := p.unheldEntries(found, notHeld, keepEntry, &passNow)
		if unfollowed {
			u = append(u, keptEntries(found, seen, recorded.folded)...)
		}
		return u
	}
	// drop records r as not held for want of consent, reporting a
	// withdrawal when it was recorded, and follows its Next through the
	// shared live read: an unheld hop doesn't stop the chain (§9.4).
	drop := func(r Ref) ([]Ref, error) {
		id := r.ID()
		delete(byID, id)
		reason := UnheldNotConsented
		switch {
		case recorded.withdrawn[id]: // reported already, and in a.Unheld
			reason = UnheldConsentWithdrawn
			for i := range a.Unheld {
				if a.Unheld[i].Ref.ID() == id {
					a.Unheld[i].Ref = r
				}
			}
		case rec0.Names(id):
			reason = UnheldConsentWithdrawn
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonConsentWithdrawn})
			p.reports.ConsentWithdrawn(p.holder, obj, id)
		default:
			a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonNotConsented})
			if !listed[unheldRef(id)] && p.reported.first(consentKey{obj.GetUID(), id}) {
				// Reported once, when the ref first goes unheld: neither its
				// unheld entry, nor this process, has seen it.
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
	// disown records r as Disowned, and follows its Next as drop does: the
	// refs behind it stay protected unless the switch covers them too (the
	// user's decision, 2026-11-22).
	disown := func(r Ref) ([]Ref, error) {
		id := r.ID()
		delete(byID, id)
		disowned[id] = true
		a.Unheld = append(a.Unheld, Unheld{Ref: r, Reason: ReasonDisowned})
		if r.Next == nil {
			return nil, nil
		}
		o, err := p.unheldReads.get(ctx, p.contents, id)
		if Classify(err) == ClassNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("credential-use: reading the disowned hop %s: %w", id, err)
		}
		return p.resolve(&a, r, o)
	}
	liveDisowned := p.liveDisown(ctx)
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
			if met(r) {
				continue
			}
			seen[id] = r
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
			if p.isDisowned(r.GroupKind) {
				more, err := disown(r)
				if err != nil {
					return fail(err, "Refs")
				}
				next = append(next, more...)
				continue
			}
			if r.Namespace == namespace {
				byID[id] = r
				toHold = append(toHold, id)
				continue
			}
			// The gate, before A1: live in the adding direction, and from
			// the cache on a pass that may skip A2 (§11.3, M1).
			cross[id] = true
			g, read := recorded.live[id]
			if !read {
				g = p.gate(ctx, obj, id, rec0.Names(id) && !deleting)
			}
			readOK[id] = g == gateHoldLive || g == gateNotHeld || g == gateMissing
			switch g {
			case gateHoldLive, gateHoldCached:
				p.reported.clear(consentKey{obj.GetUID(), id})
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
		// A level that leads nowhere further is the last: its A1 writes the
		// chain and the unheld list in one patch (§11.3). Any other only adds.
		want := exact()
		var unheldNow *[]UnheldEntry
		if last := !chained && len(next) == 0; last {
			metAll()
			u := lastUnheld()
			unheldNow = &u
		} else if rec := ReadRecord(obj); !rec.Unparseable {
			want = SortedIDs(append(want, slices.DeleteFunc(slices.Clone(rec.IDs), unrecorded)...))
		}
		added, err := p.announce(ctx, obj, want, unheldNow, &a)
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
			if p.isDisowned(id.GroupKind()) {
				// The switch was set after this level's check: the pass
				// stops holding, and its refs go unheld (§10.6.8,
				// interleaving 3). What it already wrote, the Disown loop
				// strips.
				more, err := disown(r)
				if err != nil {
					return fail(err, "Refs")
				}
				next = append(next, more...)
				continue
			}
			held := false
			if !deleting && !added[id] {
				if cached, ok := p.views.Protected(id); ok && p.ver.canSkip(obj.GetUID(), obj.GetResourceVersion(), id, cached.GetUID(), namespace) {
					a.Held = append(a.Held, r)
					held = true
				}
			}
			if held && p.keepsLine(id.GroupKind()) {
				// A skip still checks the line (§10.6.2): one a prune lost
				// while this process's write was the last comes back.
				if err := p.lines.ensure(ctx, id); err != nil {
					return fail(fmt.Errorf("credential-use: keeping the index line of %s: %w", quotedLine(id), err), "Index")
				}
			}
			if !held && cross[id] && !liveOK[id] {
				// The gate read consent from the cache, and the skip didn't
				// hold: every A2 of a cross-namespace ref follows a live
				// consent read in the same pass (§11.3, rule 1).
				g := p.liveConsent(ctx, obj, id)
				readOK[id] = readOK[id] || g != gateUnknown
				switch g {
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
				// The switch, read live before the pass's first A2 (§10.6.8,
				// interleaving 3).
				switch d, err := liveDisowned(id.GroupKind()); {
				case err != nil:
					return fail(fmt.Errorf("credential-use: reading the Disown switch before holding %s: %w", id, err), "Disown")
				case d:
					more, err := disown(r)
					if err != nil {
						return fail(err, "Refs")
					}
					next = append(next, more...)
					continue
				}
				done := p.holding(id)
				if p.keepsLine(id.GroupKind()) {
					// The line before A2 (§10.6.2): an add that fails, as on
					// a full index, stops before A2.
					if err := p.lines.ensure(ctx, id); err != nil {
						done()
						return fail(fmt.Errorf("credential-use: adding the index line of %s: %w", quotedLine(id), err), "Index")
					}
				}
				class, uid, err := p.write(ctx, cc, id, namespace)
				var checkErr error
				if class == ClassNone {
					checkErr = p.checkLine(cc, id, uid, namespace)
				}
				done()
				switch class {
				case ClassNone:
					if checkErr != nil {
						return fail(checkErr, "Index")
					}
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
		level = slices.DeleteFunc(next, met)
	}
	metAll()
	for id := range readOK {
		if readOK[id] {
			p.backoff.succeed(consentKey{obj.GetUID(), id})
		}
	}

	// The last A1: exactly the chain, and the unheld list with it, in one
	// patch (§11.3). It writes nothing when neither changed.
	unheld := lastUnheld()
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
	p.written.set(obj.GetUID(), out.Annotations)
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

// write is A2 for one ref: it returns ClassNone, with the incarnation it
// wrote, when P now carries the finalizer and a fresh epoch, and otherwise
// the class of the failure: ClassNotFound (Missing), ClassDeleting, or
// anything else (Degraded, or fail closed). A successful write is recorded
// in written, keyed by the incarnation the response names, at the time the
// request was issued.
//
// A write that may have landed with its outcome unknown, or may yet, gets
// its line checked here, as a successful one does after it, and again once
// the landing margin is over; and the release batch it owes, should its
// holder be gone by now (§10.6.2). It isn't cut short by a stop (detach);
// none begins once ctx has ended.
func (p *protocol) write(ctx context.Context, cc callContexts, id ID, namespace string) (class Class, uid types.UID, err error) {
	if err := ctx.Err(); err != nil {
		return ClassOther, "", err
	}
	issuedOne := false
	defer func() {
		if issuedOne && mayHaveLanded(err) && p.keepsLine(id.GroupKind()) {
			check, cancel := p.checkIn(cc)
			defer cancel()
			p.lines.unknownA2(check, id, namespace)
			if p.enqueueBatch != nil {
				p.enqueueBatch(id, namespace)
			}
		}
	}()
	epoch, err := p.newEpoch()
	if err != nil {
		return ClassOther, "", err
	}
	gk := id.GroupKind()
	if isBuiltin(gk) {
		pt, err := AcquireStrategicPatch(p.holder, namespace, epoch)
		if err != nil {
			return ClassOther, "", err
		}
		issued := p.clock.Now()
		issuedOne = true
		out, err := p.srv.Patch(cc.write, gk, id.Namespace, id.Name, pt.Type, pt.Data)
		if err != nil {
			return Classify(err), "", err
		}
		p.ver.wrote(out.UID, namespace, issued)
		p.wroteDeleting(id, namespace, out)
		return ClassNone, out.UID, nil
	}
	var last error
	for range maxGuardedAttempts {
		issuedOne = false // a conflict didn't land, and this read isn't a write
		current, err := p.srv.Get(ctx, gk, id.Namespace, id.Name)
		if err != nil {
			return Classify(err), "", err
		}
		pt, err := AcquireGuardedPatch(current, p.holder, namespace, epoch)
		if err != nil {
			return ClassOther, "", err
		}
		issued := p.clock.Now()
		issuedOne = true
		out, err := p.srv.Patch(cc.write, gk, id.Namespace, id.Name, pt.Type, pt.Data)
		if err == nil {
			p.ver.wrote(out.UID, namespace, issued)
			p.wroteDeleting(id, namespace, out)
			return ClassNone, out.UID, nil
		}
		if c := Classify(err); c != ClassConflict {
			return c, "", err
		}
		last = err
	}
	issuedOne = false // every one conflicted
	return ClassOther, "", fmt.Errorf("credential-use: %d conflicting writes in a row: %w", maxGuardedAttempts, last)
}

// checkLine is the acquirer's live check of id's index line after a
// successful A2 for holder namespace n (§10.6.2). When it fails, whether the
// line survived a concurrent prune is unknown, so the A2's written entry
// goes: the holder's next pass writes A2 again, and checks again, rather
// than skipping.
func (p *protocol) checkLine(cc callContexts, id ID, uid types.UID, n string) error {
	if !p.keepsLine(id.GroupKind()) {
		return nil
	}
	ctx, cancel := p.checkIn(cc)
	defer cancel()
	err := p.lines.postCheck(ctx, id)
	if err == nil {
		return nil
	}
	p.ver.released(uid, n)
	return fmt.Errorf("credential-use: checking the index line of %s after holding it: %w", id, err)
}

// holding tells the index of an A2 of id that is about to be made
// (lineKeeper), and of its end when the function it returns is called.
func (p *protocol) holding(id ID) (done func()) {
	if !p.keepsLine(id.GroupKind()) {
		return func() {}
	}
	return p.lines.holding(id)
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
	// A pass refused for a chain held nothing, so it never establishes: a
	// Degraded one gets here if its holder lost its finalizer meanwhile. And
	// no ref with a Next is held here either, in an Acquired built by hand.
	if errors.Is(a.Err, ErrChainsNotSupported) {
		return a.Err
	}
	for _, u := range a.Unheld {
		if err := p.refuseChains([]Ref{u.Ref}); err != nil {
			return err
		}
	}
	cc := p.detach(ctx)
	defer cc.stop()
	liveDisowned := p.liveDisown(ctx)
	for _, u := range a.Unheld {
		if u.Reason != ReasonMissing {
			continue
		}
		id := u.Ref.ID()
		if p.isDisowned(id.GroupKind()) {
			continue // the switch was set since the pass: no A2 (§7.4)
		}
		cross := id.Namespace != obj.GetNamespace()
		if cross {
			// A ref in another namespace is held only on a live yes, here as
			// in every pass (§11.3): a Missing one that appeared may still
			// not consent. Without one it stays unheld, as the next pass
			// will find it; the holder may establish without it.
			if p.liveConsent(ctx, obj, id) != gateHoldLive {
				continue
			}
		} else {
			_, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
			switch Classify(err) {
			case ClassNotFound:
				continue
			case ClassNone:
			default:
				return fmt.Errorf("credential-use: checking %s before establishing: %w", id, err)
			}
		}
		switch d, err := liveDisowned(id.GroupKind()); {
		case err != nil:
			return fmt.Errorf("credential-use: reading the Disown switch before holding %s: %w", id, err)
		case d:
			continue
		}
		done := p.holding(id)
		if p.keepsLine(id.GroupKind()) {
			if err := p.lines.ensure(ctx, id); err != nil {
				done()
				return fmt.Errorf("credential-use: adding the index line of %s before establishing: %w", quotedLine(id), err)
			}
		}
		class, uid, err := p.write(ctx, cc, id, obj.GetNamespace())
		if class != ClassNone {
			done()
			return fmt.Errorf("credential-use: holding %s before establishing (%s): %w", id, class, err)
		}
		err = p.checkLine(cc, id, uid, obj.GetNamespace())
		done()
		if err != nil {
			return err
		}
		if cross && p.enqueueBatch != nil {
			p.enqueueBatch(id, obj.GetNamespace()) // §11.3 rule 3, as after any A2
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
	// First: a holder whose refs are all Guaranteed may have no record.
	p.conflicts.forget(obj.GetUID())
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
	p.backoff.forget(obj.GetUID())
	p.gateTurns.forget(obj.GetUID())
	p.reported.forget(obj.GetUID())
	p.written.forget(obj.GetUID())
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
