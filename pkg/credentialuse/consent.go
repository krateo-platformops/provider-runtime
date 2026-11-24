package credentialuse

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// consentList parses an allow-holders-from value: a comma-separated list of
// holder namespaces (§11.3). Blanks are dropped.
func consentList(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// consents reports whether a target with allow-holders-from objectList, in a
// Namespace with nsList, lets holders from holderNS hold it (§11.3): the
// union of the two lists. "*" counts on the object only; on a Namespace it is
// ignored (and reported by the Namespace handler).
func consents(objectList, nsList, holderNS string) bool {
	obj := consentList(objectList)
	if slices.Contains(obj, "*") || slices.Contains(obj, holderNS) {
		return true
	}
	return slices.Contains(consentList(nsList), holderNS)
}

// hasWildcard reports whether an allow-holders-from value names "*".
func hasWildcard(v string) bool { return slices.Contains(consentList(v), "*") }

// The consent-read backoff (§11.3, M2 of the review of revision 16): a live
// consent read that fails keeps the hold and asks for a requeue, 1s, then
// doubling up to the holder's poll interval, per (holder, ref), back to 1s
// after a read that succeeds.
const consentRetryMin = time.Second

type consentKey struct {
	holder types.UID
	ref    ID
}

type consentBackoff struct {
	mu  sync.Mutex
	max time.Duration
	at  map[consentKey]time.Duration
}

func newConsentBackoff(max time.Duration) *consentBackoff {
	if max < consentRetryMin {
		max = consentRetryMin
	}
	return &consentBackoff{max: max, at: map[consentKey]time.Duration{}}
}

// fail returns the requeue after a failed read, and doubles the next.
func (b *consentBackoff) fail(k consentKey) time.Duration {
	if b == nil {
		return consentRetryMin
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	d := b.at[k]
	if d == 0 {
		d = consentRetryMin
	}
	next := 2 * d
	if next > b.max {
		next = b.max
	}
	b.at[k] = next
	return d
}

// succeed resets k's backoff.
func (b *consentBackoff) succeed(k consentKey) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.at, k)
}

// forget drops every entry of holder, when it is forgotten.
func (b *consentBackoff) forget(holder types.UID) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range b.at {
		if k.holder == holder {
			delete(b.at, k)
		}
	}
}

// namespaceViews is the Namespace informer's view for the gate's cached
// read (§11.3): a Namespace's allow-holders-from, and whether the cache knows
// the Namespace at all.
type namespaceViews interface {
	NamespaceConsent(namespace string) (value string, known bool)
}

// What the gate found for one cross-namespace ref (§11.3).
type gateResult int

const (
	gateHoldLive   gateResult = iota // consent, read live
	gateHoldCached                   // consent, read from the cache
	gateNotHeld                      // no consent, read live
	gateMissing                      // the object or its Namespace is gone
	gateUnknown                      // the live read failed
)

// gate decides whether holder obj may hold cross-namespace ref id. With
// cachedOK, for a ref the record names on a pass that isn't deleting, a
// cached yes serves; anything else, a cached no included, is read live: a
// hold is dropped only on a positive, live-confirmed withdrawal (M1).
func (p *protocol) gate(ctx context.Context, obj metav1.Object, id ID, cachedOK bool) gateResult {
	if cachedOK && p.namespaceViews != nil {
		if m, ok := p.views.Protected(id); ok {
			if nsv, known := p.namespaceViews.NamespaceConsent(id.Namespace); known &&
				consents(m.Annotations[AnnotationAllowHoldersFrom], nsv, obj.GetNamespace()) {
				return gateHoldCached
			}
		}
	}
	return p.liveConsent(ctx, obj, id)
}

// liveConsent reads id's consent live, for holder obj (§11.3): its
// Namespace's metadata and its own. A NotFound of either is Missing; any
// other failure is unknown.
func (p *protocol) liveConsent(ctx context.Context, obj metav1.Object, id ID) gateResult {
	ns, err := p.srv.Get(ctx, namespaceGroupKind, "", id.Namespace)
	switch Classify(err) {
	case ClassNone:
	case ClassNotFound:
		return gateMissing
	default:
		return gateUnknown
	}
	o, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
	switch Classify(err) {
	case ClassNone:
	case ClassNotFound:
		return gateMissing
	default:
		return gateUnknown
	}
	if consents(o.Annotations[AnnotationAllowHoldersFrom], ns.Annotations[AnnotationAllowHoldersFrom], obj.GetNamespace()) {
		return gateHoldLive
	}
	return gateNotHeld
}

// unheldEntries is the unheld list the pass's last A1 writes (§11.3): an
// entry for each ref dropped for want of consent, keeping its since while
// its state lasts; the entries of refs whose state this pass couldn't learn,
// as they were; and nothing for a ref held or no longer named.
//
// now is the pass's time for a new entry, read once, on the first new entry:
// in the model a clock read is a scheduled input, and a pass with no new
// entry must not add one; and both of a pass's A1s must compute one list.
//
// found is the unheld list as the pass found it, before its first A1: both of
// a pass's A1s compute from it, so they compute one list.
func (p *protocol) unheldEntries(found []UnheldEntry, notHeld map[ID]string, keep map[ID]bool, now *time.Time) []UnheldEntry {
	prev := map[string]UnheldEntry{}
	for _, e := range found {
		if e.Reason != unheldMore {
			e.listed = true
			prev[e.Ref] = e
		}
	}
	var out []UnheldEntry
	for id, reason := range notHeld {
		ref := unheldRef(id)
		e, had := prev[ref]
		switch {
		case had && (e.Reason == reason || reason == UnheldNotConsented):
			// The same state, or still unheld after a withdrawal: the
			// entry and its since stay.
		default:
			if now.IsZero() {
				*now = p.clock.Now().UTC().Truncate(time.Second)
			}
			e = UnheldEntry{Ref: ref, Reason: reason, Since: *now}
		}
		out = append(out, e)
	}
	for id := range keep {
		if e, had := prev[unheldRef(id)]; had {
			out = append(out, e)
		}
	}
	return out
}

// reportedSet remembers, in memory, which (holder, ref) pairs have been
// reported not held, so CredentialRefNotHeld goes once per ref even when the
// unheld list is trimmed past maxUnheld (§11.3). A nil set remembers nothing.
type reportedSet struct {
	mu sync.Mutex
	m  map[consentKey]bool
}

func newReportedSet() *reportedSet { return &reportedSet{m: map[consentKey]bool{}} }

// first reports whether k is new, and remembers it.
func (r *reportedSet) first(k consentKey) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[k] {
		return false
	}
	r.m[k] = true
	return true
}

// clear forgets k, once its ref is held again.
func (r *reportedSet) clear(k consentKey) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, k)
}

// forget drops every pair of holder.
func (r *reportedSet) forget(holder types.UID) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.m {
		if k.holder == holder {
			delete(r.m, k)
		}
	}
}
