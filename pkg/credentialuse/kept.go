package credentialuse

import (
	"context"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
}

// gateRecorded runs the consent gate for every ref in another namespace that
// obj's record names, first in a pass, whatever the pass comes to (§11.3): a
// record keeps refs that the pass doesn't name, behind a hop it can't follow
// (§9.1) and when it fails (§8), and the gate of the refs it does name never
// reaches those. A cached yes serves, on a deleting pass too, where the refs
// that the pass names are then read live before their A2; anything else is
// read live.
//
// A withdrawn ref is dropped from the record, and listed as ConsentWithdrawn,
// in one A1; a ref that can't be read is kept, with RetryAfter; one that is
// gone is kept, as any Missing ref. A kind that the Disown switch covers isn't
// asked about: the Disown loop strips it (§7.4). It returns ErrHolderConflict
// when the A1 conflicts.
func (p *protocol) gateRecorded(ctx context.Context, obj metav1.Object, a *Acquired) (recordedGate, error) {
	g := recordedGate{live: map[ID]gateResult{}, withdrawn: map[ID]bool{}, retried: map[ID]bool{}}
	rec := ReadRecord(obj) // an unparseable one names nothing to ask about
	var withdrawn []ID     // g.withdrawn, in the record's order
	for _, id := range rec.IDs {
		if id.Namespace == obj.GetNamespace() || p.isDisowned(id.GroupKind()) {
			continue
		}
		found := p.gate(ctx, obj, id, true)
		if found == gateHoldCached {
			continue
		}
		g.live[id] = found
		key := consentKey{obj.GetUID(), id}
		if found == gateUnknown {
			g.retried[id] = true
			a.retryIn(p.backoff.fail(key))
			continue
		}
		p.backoff.succeed(key)
		if found == gateNotHeld {
			g.withdrawn[id] = true
			withdrawn = append(withdrawn, id)
		}
	}
	if len(withdrawn) == 0 {
		return g, nil
	}
	want := slices.DeleteFunc(slices.Clone(rec.IDs), func(id ID) bool { return g.withdrawn[id] || p.isDisowned(id.GroupKind()) })
	unheld := p.listWithdrawn(ReadUnheld(obj.GetAnnotations()), withdrawn)
	if _, err := p.announce(ctx, obj, want, &unheld, a); err != nil {
		return g, err
	}
	for _, id := range withdrawn {
		p.reports.ConsentWithdrawn(p.holder, obj, id)
	}
	return g, nil
}

// retryIn asks the caller to requeue the holder after d, or sooner.
func (a *Acquired) retryIn(d time.Duration) {
	if a.RetryAfter == 0 || d < a.RetryAfter {
		a.RetryAfter = d
	}
}

// listWithdrawn is the unheld list found, with withdrawn as ConsentWithdrawn.
// The pass hasn't met the refs of the other entries yet, so they stay.
func (p *protocol) listWithdrawn(found []UnheldEntry, withdrawn []ID) []UnheldEntry {
	out := slices.Clone(found)
	now := p.clock.Now().UTC().Truncate(time.Second)
	for _, id := range withdrawn {
		ref := unheldRef(id)
		out = slices.DeleteFunc(out, func(e UnheldEntry) bool { return e.Ref == ref })
		out = append(out, UnheldEntry{Ref: ref, Reason: UnheldConsentWithdrawn, Since: now})
	}
	return out
}

// keptEntries are the entries of found that a pass which couldn't follow the
// whole chain leaves in the unheld list: those of holds that were released,
// for refs the pass didn't meet. The holder may still read through to one, and
// the entry explains why it then finds nothing (§11.3). They go once a pass
// follows the whole chain.
func keptEntries(found []UnheldEntry, met map[ID]Ref) []UnheldEntry {
	refs := map[string]bool{}
	for id := range met {
		refs[unheldRef(id)] = true
	}
	var out []UnheldEntry
	for _, e := range found {
		if (e.Reason == UnheldConsentWithdrawn || e.Reason == UnheldReadDenied) && !refs[e.Ref] {
			out = append(out, e)
		}
	}
	return out
}
