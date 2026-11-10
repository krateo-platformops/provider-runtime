package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// maxReleaseRounds bounds how often one batch goes back to R1 after a
// release patch found the state changed (§6.2, "JSON patches are classified
// apart").
const maxReleaseRounds = 5

// restoredEpochRequeue is how soon a batch looks again after R1 restored a
// missing epoch.
const restoredEpochRequeue = time.Second

// ErrReleaseRejected is a release patch that failed against the very
// resourceVersion it was built from: not contention but a defect, reported
// and not retried (§6.2, L1 of the review of revision 16).
var ErrReleaseRejected = errors.New("credential-use: a release patch was rejected by an unchanged object")

// A ReleaseDecision is what one batch decided for one protected object, with
// what it read: for diagnostics, and for the model's safety checks.
type ReleaseDecision struct {
	P        ID
	Input    ReleaseInput
	Decision Decision
	// NamespaceReadAt is when R2's live GET of P's Namespace returned, if it
	// made one.
	NamespaceReadAt time.Time
	// GraceEnd is when P's grace ran out, if P is being deleted.
	GraceEnd time.Time
}

// ReleaseResult is what one batch did.
type ReleaseResult struct {
	// Released are the objects whose finalizer for (K, N) R3 removed.
	Released []ID
	// Decisions are §5's decisions, one per object that reached R2.
	Decisions []ReleaseDecision
	// RequeueAt, when set, is when the batch must run again: the end of a
	// grace, or a restored epoch's recheck. It is absolute, the deadline
	// itself rather than a duration from some reading of the clock, so the
	// requeue lands exactly on it (docs/credential-use-model-reduction.md
	// §10.2). A time already past means at once: it is the batch's now.
	RequeueAt time.Time
}

func (r *ReleaseResult) requeueAt(now, at time.Time) {
	if at.Before(now) {
		at = now
	}
	if r.RequeueAt.IsZero() || at.Before(r.RequeueAt) {
		r.RequeueAt = at
	}
}

// r1Read is one protected object as R1 read it.
type r1Read struct {
	id       ID
	meta     *metav1.PartialObjectMetadata
	graceEnd time.Time
}

// release runs one batch for holder kind K and holder namespace n, over the
// protected objects pending there (§6.2 R0 to R3). It returns an error when
// some object couldn't be decided; the caller requeues the batch with
// backoff. Every object it decided is in the result either way.
func (p *protocol) release(ctx context.Context, n string, pending []ID) (ReleaseResult, error) {
	var res ReleaseResult
	f, err := FinalizerName(p.holder, n)
	if err != nil {
		return res, err
	}
	var errs []error

	// R0. The fast path, from the caches.
	cachedUsers := map[ID]int{}
	var todo []ID
	for _, id := range SortedIDs(pending) {
		users, keep := p.fastKeep(id, n)
		cachedUsers[id] = users
		if !keep {
			todo = append(todo, id)
		}
	}

	builtFrom := map[ID]string{} // the resourceVersion each failed release was built from
	for round := 0; len(todo) > 0; round++ {
		if round == maxReleaseRounds {
			errs = append(errs, fmt.Errorf("credential-use: the state of %v kept changing under %d releases", todo, maxReleaseRounds))
			break
		}

		// R1. Each pending object, live.
		var live []r1Read
		for _, id := range todo {
			m, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
			switch Classify(err) {
			case ClassNone:
			case ClassNotFound:
				p.seen.forget(uidOf(p.views, id))
				continue // gone: nothing to release
			default:
				errs = append(errs, fmt.Errorf("credential-use: reading %s: %w", id, err))
				continue
			}
			if rv, ok := builtFrom[id]; ok && rv == m.ResourceVersion {
				p.reports.ReleaseRejected(p.holder, id, rv)
				errs = append(errs, fmt.Errorf("%w: %s at resourceVersion %s", ErrReleaseRejected, id, rv))
				continue
			}
			if !hasFinalizer(m, f) {
				p.seen.forget(m.UID)
				continue // done
			}
			if m.Annotations[f] == "" {
				if err := p.restoreEpoch(ctx, id, m, n); err != nil {
					errs = append(errs, err)
				}
				now := p.clock.Now()
				res.requeueAt(now, now.Add(restoredEpochRequeue))
				continue
			}
			r := r1Read{id: id, meta: m}
			if m.DeletionTimestamp != nil {
				now := p.clock.Now()
				r.graceEnd = GraceEnd(m.DeletionTimestamp.Time, p.seen.see(m.UID, now), p.grace)
				// Inside the grace, in a namespace last seen live, with users:
				// nothing to decide until the grace ends (§6.2 R1).
				if now.Before(r.graceEnd) && cachedUsers[id] > 0 && !p.namespaceDeletingCached(id.Namespace) {
					res.requeueAt(now, r.graceEnd)
					continue
				}
			}
			live = append(live, r)
		}
		if len(live) == 0 {
			break
		}

		// R2. The last reads, after every R1: each deleting object's
		// Namespace, then one consistent list of K in n. Whether the grace
		// is over is measured before the first of them (M4 of #59's review):
		// measured after, a namespace deletion that began inside the grace,
		// after the Namespace GET, could be released over as if it began
		// after the grace (§6.4).
		now := p.clock.Now()
		nsDeleting := map[string]bool{}
		nsReadAt := map[string]time.Time{}
		var decidable []r1Read
		for _, r := range live {
			ns := r.id.Namespace
			if r.meta.DeletionTimestamp == nil || ns == "" {
				decidable = append(decidable, r)
				continue
			}
			if _, done := nsReadAt[ns]; !done {
				m, err := p.srv.Get(ctx, namespaceGroupKind, "", ns)
				switch Classify(err) {
				case ClassNone:
					nsDeleting[ns] = m.DeletionTimestamp != nil
				case ClassNotFound:
					// A namespace that is gone can't be recreated in time to
					// matter: treat it as deleting.
					nsDeleting[ns] = true
				default:
					errs = append(errs, fmt.Errorf("credential-use: reading namespace %s: %w", ns, err))
					continue
				}
				nsReadAt[ns] = p.clock.Now()
			}
			decidable = append(decidable, r)
		}
		if len(decidable) == 0 {
			break
		}
		holders, err := p.srv.List(ctx, p.holder, n)
		if err != nil {
			errs = append(errs, fmt.Errorf("credential-use: listing %s in %s: %w", p.holder, n, err))
			break
		}

		// Decide, then R3.
		var next []ID
		for _, r := range decidable {
			in := ReleaseInput{
				Deleting:          r.meta.DeletionTimestamp != nil,
				ClusterScoped:     r.id.Namespace == "",
				NamespaceDeleting: nsDeleting[r.id.Namespace],
				GraceOver:         r.meta.DeletionTimestamp != nil && !now.Before(r.graceEnd),
			}
			for i := range holders {
				if IsUser(&holders[i], r.id, p.ownerFinalizers) {
					in.Users++
				}
				if IsDeletingUser(&holders[i], r.id, p.ownerFinalizers) {
					in.DeletingUsers++
				}
			}
			d := Decide(in)
			res.Decisions = append(res.Decisions, ReleaseDecision{P: r.id, Input: in, Decision: d, NamespaceReadAt: nsReadAt[r.id.Namespace], GraceEnd: r.graceEnd})
			switch d {
			case Keep:
				continue
			case WaitForGrace:
				res.requeueAt(now, r.graceEnd)
				continue
			}
			pt, err := ReleasePatch(r.meta, p.holder, n)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			_, err = p.srv.Patch(ctx, r.id.GroupKind(), r.id.Namespace, r.id.Name, pt.Type, pt.Data)
			switch ClassifyJSONPatch(err) {
			case ClassNone:
				p.ver.released(r.meta.UID, n)
				p.seen.forget(r.meta.UID)
				res.Released = append(res.Released, r.id)
			case ClassStateChanged:
				builtFrom[r.id] = r.meta.ResourceVersion
				next = append(next, r.id) // back to R1
			case ClassNotFound:
				// gone
			default:
				errs = append(errs, fmt.Errorf("credential-use: releasing %s: %w", r.id, err))
			}
		}
		todo = next
	}
	return res, errors.Join(errs...)
}

// fastKeep is R0 for one object (§6.2): it reports the cached users of P in
// n, and whether the cache already settles the batch for P. A stale cache can
// only keep a hold longer.
func (p *protocol) fastKeep(id ID, n string) (users int, keep bool) {
	cached, ok := p.views.Protected(id)
	deletingUsers := 0
	for _, h := range p.views.HoldersNaming(id, n) {
		if IsUser(h, id, p.ownerFinalizers) {
			users++
		}
		if IsDeletingUser(h, id, p.ownerFinalizers) {
			deletingUsers++
		}
	}
	if !ok {
		return users, false
	}
	if cached.DeletionTimestamp == nil {
		return users, users > 0
	}
	return users, deletingUsers > 0
}

func (p *protocol) namespaceDeletingCached(ns string) bool {
	if ns == "" {
		return false
	}
	deleting, known := p.views.NamespaceDeleting(ns)
	return known && deleting
}

// restoreEpoch is R1's repair of a finalizer without its epoch.
func (p *protocol) restoreEpoch(ctx context.Context, id ID, m *metav1.PartialObjectMetadata, n string) error {
	epoch, err := p.newEpoch()
	if err != nil {
		return err
	}
	pt, err := RestoreEpochPatch(m, p.holder, n, epoch)
	if err != nil {
		return err
	}
	_, err = p.srv.Patch(ctx, id.GroupKind(), id.Namespace, id.Name, pt.Type, pt.Data)
	if c := ClassifyJSONPatch(err); c != ClassNone && c != ClassStateChanged && c != ClassNotFound {
		return fmt.Errorf("credential-use: restoring the epoch of %s: %w", id, err)
	}
	return nil
}

// uidOf returns the cached incarnation of id, if the views have it, for
// pruning what this process remembers about an object that is gone.
func uidOf(v views, id ID) types.UID {
	if m, ok := v.Protected(id); ok {
		return m.UID
	}
	return ""
}
