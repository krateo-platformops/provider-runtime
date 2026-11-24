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

// recordedGate is what gateRecorded found.
type recordedGate struct {
	// live are the answers read live, for the pass to use rather than read
	// them again.
	live map[ID]gateResult
	// withdrawn are the refs it dropped, and reported.
	withdrawn map[ID]bool
	// retried are the refs whose read failed, and that the pass has asked a
	// retry for.
	retried map[ID]bool
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
// pass, those that have waited the longest first; the rest are kept, with
// RetryAfter.
//
// A withdrawn ref is dropped from the record, and listed as ConsentWithdrawn,
// in one A1; a ref that can't be read is kept, with RetryAfter; one that is
// gone is kept, as any Missing ref. Not asked about are the refs of the
// holder's own namespace, cluster-scoped objects, kinds the tracker doesn't
// protect, and kinds that the Disown switch covers, which the Disown loop
// strips (§7.4). Nothing is dropped once ctx has ended. It returns
// ErrHolderConflict when the A1 conflicts.
func (p *protocol) gateRecorded(ctx context.Context, obj metav1.Object, a *Acquired) (recordedGate, error) {
	g := recordedGate{live: map[ID]gateResult{}, withdrawn: map[ID]bool{}, retried: map[ID]bool{}}
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
	rest := consentKey{holder: uid}
	over := len(ask) > maxGateReads
	if over {
		ask = p.gateTurns.next(uid, ask, maxGateReads)
		a.retryIn(p.backoff.fail(rest))
	} else {
		p.gateTurns.clear(uid)
		p.backoff.succeed(rest)
	}
	var withdrawn []ID
	failed := map[ID]bool{}
	namespaces := map[string]namespaceRead{}
	for i, id := range ask {
		if ctx.Err() != nil {
			break
		}
		found := p.liveConsentIn(ctx, obj, id, namespaces)
		if ctx.Err() != nil {
			// Whatever it found, it may be for want of ctx, so it keeps its
			// turn; but not if the pass got to no other that moves back, or
			// one whose read takes all that ctx has would keep the others
			// from theirs.
			if i == len(withdrawn) {
				p.gateTurns.read(uid, id)
			}
			break
		}
		g.live[id] = found
		if found == gateNotHeld {
			// First again next pass, unless its A1 drops it now.
			withdrawn = append(withdrawn, id)
		} else {
			p.gateTurns.read(uid, id)
		}
		switch key := (consentKey{uid, id}); {
		case found != gateUnknown:
			p.backoff.succeed(key)
		case !over:
			failed[id] = true
			g.retried[id] = true
			a.retryIn(p.backoff.fail(key))
		}
		// Past the bound, one that fails is read again at its turn, which
		// the retry for the rest asks for: a retry of its own wouldn't bring
		// it sooner.
	}
	p.forgetFailed(uid, ask, over, failed, g.live)
	if len(withdrawn) == 0 {
		return g, nil
	}
	if err := ctx.Err(); err != nil {
		return g, err
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

// forgetFailed drops the backoff of the refs whose read by the gate failed in
// an earlier pass of holder, and which it no longer asks about: whoever edits
// the holder names them, and could name others on every pass. asked is what
// this pass asks about, failed what it couldn't read, and answered what it
// got to; one it didn't get to keeps its backoff. A pass with more to ask
// about than it may read keeps none (over).
func (p *protocol) forgetFailed(holder types.UID, asked []ID, over bool, failed map[ID]bool, answered map[ID]gateResult) {
	was := p.gateTurns.failing(holder)
	if !over {
		for _, id := range asked {
			if _, got := answered[id]; !got && was[id] {
				failed[id] = true
			}
		}
	}
	for id := range was {
		if !failed[id] {
			p.backoff.succeed(consentKey{holder, id})
		}
	}
	p.gateTurns.setFailing(holder, failed)
}

// gateTurns is what the gate of recorded refs remembers of a holder, in memory
// (§11.3): how long each ref has waited to be read, for a holder with more to
// read than one pass may, so that each gets its turn; and the refs whose
// backoff it keeps. A nil one remembers nothing.
type gateTurns struct {
	mu sync.Mutex
	// n counts what happens, and at is when each ref was last read, or first
	// asked about.
	n    uint64
	at   map[types.UID]map[ID]uint64
	fail map[types.UID]map[ID]bool
}

func newGateTurns() *gateTurns {
	return &gateTurns{at: map[types.UID]map[ID]uint64{}, fail: map[types.UID]map[ID]bool{}}
}

// next returns the n of ids that have waited the longest. One that wasn't
// asked about on the holder's last pass starts to wait now, behind all that
// were: whoever edits the record can't put others ahead of a ref in it.
func (t *gateTurns) next(holder types.UID, ids []ID, n int) []ID {
	if t == nil {
		return ids[:n]
	}
	t.mu.Lock()
	defer t.mu.Unlock()
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

// clear drops holder's turns, once a pass reads all that it asks about.
func (t *gateTurns) clear(holder types.UID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.at, holder)
}

// failing returns the refs of holder whose backoff the gate keeps.
func (t *gateTurns) failing(holder types.UID) map[ID]bool {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fail[holder]
}

// setFailing replaces them with ids.
func (t *gateTurns) setFailing(holder types.UID, ids map[ID]bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(ids) == 0 {
		delete(t.fail, holder)
		return
	}
	t.fail[holder] = ids
}

// forget drops all that is remembered of holder.
func (t *gateTurns) forget(holder types.UID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.at, holder)
	delete(t.fail, holder)
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
