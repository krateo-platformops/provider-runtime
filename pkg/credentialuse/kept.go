package credentialuse

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// maxGateReads is how many recorded refs one pass reads live, at most (§11.3).
// Whoever can edit a holder writes its record, so it may name anything, as
// many times as an annotation holds: each read is a GET of the object, and of
// each Namespace one, once.
const maxGateReads = 16

// gateReadTimeout is how long those reads may take together, in a tracker
// (§11.3).
const gateReadTimeout = 5 * time.Second

// recordedGate is what gateRecorded found.
type recordedGate struct {
	// live are the answers read live, for the pass to use rather than read
	// them again.
	live map[ID]gateResult
	// withdrawn are the refs it dropped, and reported.
	withdrawn map[ID]bool
	// folded are the released holds' entries that its A1 trimmed from the
	// unheld list into More, by their refs.
	folded []string
}

// gateRecorded runs the consent gate for every ref in another namespace that
// obj's record names, first in a pass, whatever the pass comes to (§11.3): a
// record keeps refs that the pass doesn't name, behind a hop it can't follow
// (§9.1) and when it fails (§8), and the gate of the refs it does name never
// reaches those. A cached yes serves, on a deleting pass too, where the refs
// that the pass names are then read live before their A2. A ref whose cache
// has synced and lacks its object isn't held, or is gone, or its held label
// was stripped (LabelScoped), or its index line is missing (PerName): it is
// kept, and not read. Anything else is read live, up to maxGateReads refs a
// pass and within gateReadTimeout, those that have waited the longest first;
// the rest are kept, with RetryAfter.
//
// A withdrawn ref is dropped from the record, and listed as ConsentWithdrawn,
// in one A1, under ctx; a ref that can't be read is kept, with RetryAfter; one
// that is gone is kept, as any Missing ref. Not asked about are the refs of the
// holder's own namespace, cluster-scoped objects, kinds the tracker doesn't
// protect, and kinds that the Disown switch covers, which the Disown loop
// strips (§7.4). Nothing is dropped once ctx has ended. It returns
// ErrHolderConflict when the A1 conflicts.
func (p *protocol) gateRecorded(ctx context.Context, obj metav1.Object, a *Acquired) (recordedGate, error) {
	g := recordedGate{live: map[ID]gateResult{}, withdrawn: map[ID]bool{}}
	rec := ReadRecord(obj) // an unparseable one names nothing to ask about
	var ask []ID
	for _, id := range rec.IDs {
		gk := id.GroupKind()
		if id.Namespace == "" || id.Namespace == obj.GetNamespace() || p.isDisowned(gk) || p.protects != nil && !p.protects(gk) {
			continue
		}
		if p.cachedYes(obj, id) || p.cacheLacks != nil && p.cacheLacks(id) {
			continue
		}
		ask = append(ask, id)
	}
	uid := obj.GetUID()
	var withdrawn []ID
	known := 0 // how many of ask this pass knows about now
	namespaces := map[string]namespaceRead{}
	// The reads have a deadline of their own, so that one that hangs takes
	// neither the pass, nor the A1 of what the others found.
	reads, cancel := ctx, context.CancelFunc(func() {})
	if p.gateContext != nil {
		reads, cancel = p.gateContext(ctx)
	}
	defer cancel()
	for i, id := range p.gateTurns.next(uid, ask, maxGateReads) {
		if reads.Err() != nil {
			break
		}
		found := p.liveConsentIn(reads, obj, id, namespaces)
		if reads.Err() != nil {
			// Whatever it found, it may be for want of time, so it keeps its
			// turn, and is the first next time; unless it was the first, and
			// so had all the time there is.
			if i == 0 {
				p.gateTurns.read(uid, id)
			}
			break
		}
		g.live[id] = found
		switch found {
		case gateNotHeld:
			// First again next pass, unless its A1 drops it now.
			withdrawn = append(withdrawn, id)
			known++
			continue
		case gateUnknown:
		default:
			known++
		}
		p.gateTurns.read(uid, id)
	}
	if err := ctx.Err(); err != nil {
		if len(withdrawn) == 0 {
			err = nil // the pass has nothing to drop, and finds out itself
		}
		return g, err
	}
	// One backoff, the gate's own, for all that this pass asked about and
	// doesn't know: what it couldn't read, and what it didn't get to.
	if rest := (consentKey{holder: uid}); known < len(ask) {
		a.retryIn(p.backoff.fail(rest))
	} else {
		p.backoff.succeed(rest)
	}
	if len(withdrawn) == 0 {
		return g, nil
	}
	withdrawn = SortedIDs(withdrawn)
	for _, id := range withdrawn {
		g.withdrawn[id] = true
	}
	want := slices.DeleteFunc(slices.Clone(rec.IDs), func(id ID) bool { return g.withdrawn[id] || p.isDisowned(id.GroupKind()) })
	unheld := p.listWithdrawn(ReadUnheld(obj.GetAnnotations()), withdrawn)
	_, err := p.announce(ctx, obj, want, &unheld, a)
	if tooLarge(err) {
		// The entry is about as long as what it replaces in the record, and
		// the annotations are near their cap. Without it, they only shrink.
		_, err = p.announce(ctx, obj, want, nil, a)
		unheld = nil
	}
	if err != nil {
		g.withdrawn = map[ID]bool{}
		return g, err
	}
	g.folded = foldedReleases(unheld, ReadUnheld(obj.GetAnnotations()))
	for _, id := range withdrawn {
		a.Unheld = append(a.Unheld, Unheld{Ref: Ref{GroupKind: id.GroupKind(), Namespace: id.Namespace, Name: id.Name}, Reason: ReasonConsentWithdrawn})
		p.reports.ConsentWithdrawn(p.holder, obj, id)
	}
	return g, nil
}

// tooLarge reports whether a write may have been refused for its size: a 422,
// which an API server gives annotations over their cap, or a 413. A 422 may
// have another cause, which a smaller write then meets too.
func tooLarge(err error) bool {
	return err != nil && !errors.Is(err, ErrHolderConflict) && (apierrors.IsInvalid(err) || apierrors.IsRequestEntityTooLargeError(err))
}

// retryIn asks the caller to requeue the holder after d, or sooner.
func (a *Acquired) retryIn(d time.Duration) {
	if a.RetryAfter == 0 || d < a.RetryAfter {
		a.RetryAfter = d
	}
}

// namespaceRead is one live read of a Namespace's consent.
type namespaceRead struct {
	consent string
	result  gateResult // gateMissing or gateUnknown, when it has no consent to give
	ok      bool
}

// gateTurns is what the gate of recorded refs remembers of a holder, in memory
// (§11.3): how long each ref has waited to be read, so that each gets its turn,
// whether a pass reads no more than maxGateReads or runs out of time. A nil
// one remembers nothing.
type gateTurns struct {
	mu sync.Mutex
	// n counts what happens, and at is when each ref was last read, or first
	// asked about.
	n  uint64
	at map[types.UID]map[ID]uint64
}

func newGateTurns() *gateTurns { return &gateTurns{at: map[types.UID]map[ID]uint64{}} }

// next returns the n of ids, at most, that have waited the longest, in that
// order. One that wasn't asked about on the holder's last pass starts to wait
// now, behind all that were: whoever edits the record can't put others ahead
// of a ref in it.
func (t *gateTurns) next(holder types.UID, ids []ID, n int) []ID {
	n = min(n, len(ids))
	if t == nil {
		return ids[:n]
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(ids) == 0 {
		delete(t.at, holder)
		return nil
	}
	t.n++
	was, now := t.at[holder], map[ID]uint64{}
	for _, id := range ids { // and those no longer asked about are forgotten
		at, ok := was[id]
		if !ok {
			at = t.n
		}
		now[id] = at
	}
	t.at[holder] = now
	out := slices.Clone(ids)
	slices.SortStableFunc(out, func(a, b ID) int { return cmp.Compare(now[a], now[b]) })
	return out[:n]
}

// read moves id behind all that holder asks about.
func (t *gateTurns) read(holder types.UID, id ID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if at := t.at[holder]; at != nil {
		t.n++
		at[id] = t.n
	}
}

// forget drops holder's turns.
func (t *gateTurns) forget(holder types.UID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.at, holder)
}

// listWithdrawn is the unheld list found, with withdrawn as ConsentWithdrawn.
// The pass hasn't met the refs of the other entries yet, so they stay, and
// those of released holds count as ones no pass meets until it does.
func (p *protocol) listWithdrawn(found []UnheldEntry, withdrawn []ID) []UnheldEntry {
	out := slices.Clone(found)
	now := p.clock.Now().UTC().Truncate(time.Second)
	for _, id := range withdrawn {
		ref := unheldRef(id)
		out = slices.DeleteFunc(out, func(e UnheldEntry) bool { return e.Ref == ref })
		out = append(out, UnheldEntry{Ref: ref, Reason: UnheldConsentWithdrawn, Since: now})
	}
	for i := range out {
		out[i].unmet = released(out[i])
	}
	return out
}

// released reports whether e is of a hold that was released.
func released(e UnheldEntry) bool {
	return e.Reason == UnheldConsentWithdrawn || e.Reason == UnheldReadDenied
}

// foldedReleases are the released holds' entries of list that written, the
// list as it was written, doesn't show: the trim folded them into More.
func foldedReleases(list, written []UnheldEntry) []string {
	shown := map[string]bool{}
	for _, e := range written {
		shown[e.Ref] = true
	}
	var out []string
	for _, e := range list {
		if released(e) && !shown[e.Ref] {
			out = append(out, e.Ref)
		}
	}
	return out
}

// keptEntries are the entries of found that a pass which couldn't follow the
// whole chain leaves in the unheld list: those of holds that were released,
// for refs the pass didn't meet, and the count of those a trim folded into
// More. The holder may still read through to one, and the entry explains why
// it then finds nothing (§11.3). They go once a pass follows the whole chain.
//
// folded are the entries that this pass's first A1 folded, which counted them
// as unmet: those of refs the pass has met since aren't.
func keptEntries(found []UnheldEntry, met map[ID]Ref, folded []string) []UnheldEntry {
	refs := map[string]bool{}
	for id := range met {
		refs[unheldRef(id)] = true
	}
	var out []UnheldEntry
	for _, e := range found {
		switch {
		case e.Reason == unheldMore:
			n := e.Unmet
			for _, ref := range folded {
				if refs[ref] {
					n--
				}
			}
			if n > 0 {
				out = append(out, UnheldEntry{Ref: e.Ref, Reason: unheldMore, Count: n, Unmet: n})
			}
		case released(e) && !refs[e.Ref]:
			e.unmet = true
			out = append(out, e)
		}
	}
	return out
}
