package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
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

// epochRequeue is how soon a batch looks again after R1 restored a missing
// epoch: restoredEpochRequeue, unless the protocol sets its own (the model
// does, a multiple of its time unit, docs/credential-use-model-reduction.md
// §10.2).
func (p *protocol) epochRequeue() time.Duration {
	if p.restoredEpochRequeue != 0 {
		return p.restoredEpochRequeue
	}
	return restoredEpochRequeue
}

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

// A KeptReason is why a batch kept a protected object that is being
// deleted (§12).
type KeptReason string

const (
	// KeptGrace: its grace runs (§5's live-namespace row).
	KeptGrace KeptReason = "its grace is running"
	// KeptNamespaceDeleting: its namespace is being deleted, and it has
	// users (§5).
	KeptNamespaceDeleting KeptReason = "its namespace is being deleted and it has users"
	// KeptUserDeleting: a user of it is being deleted and still needs it
	// (§5, U5).
	KeptUserDeleting KeptReason = "a user of it is being deleted and still needs it"
)

// A Kept is a protected object being deleted that a batch kept for its
// users, with the users as the batch saw them: the caches for R0 and R1's
// grace, R2's live list after it. The release loop reports it (§12).
type Kept struct {
	P             ID
	Why           KeptReason
	GraceEnd      time.Time
	Users         []types.NamespacedName
	DeletingUsers []types.NamespacedName
}

// ReleaseResult is what one batch did.
type ReleaseResult struct {
	// Released are the objects whose finalizer for (K, N) R3 removed.
	Released []ID
	// Decisions are §5's decisions, one per object that reached R2.
	Decisions []ReleaseDecision
	// Kept are the objects being deleted that the batch kept, wherever it
	// decided to keep them: R0, R1's grace, or R2.
	Kept []Kept
	// Bare are the objects the batch read, live, carrying no finalizer of
	// the holder kind at all, or gone: in R1, or in R3's response. In
	// PerName mode their index lines may be pruned (§10.6.2).
	Bare []ID
	// Failed are the objects the batch couldn't decide, each for an error it
	// returns: the caller retries them, and only them, with backoff.
	Failed []ID
	// Rest are the objects the batch didn't get to: neither decided nor
	// failed. The caller puts them back where they were in the queue.
	Rest []ID
	// Asked are the objects that must be looked at again, and when each: at the
	// end of its grace, or for its restored epoch's recheck. Nothing else of
	// the batch is.
	Asked []Asked
	// Waiting are the objects being deleted that the batch didn't decide, since
	// their release must wait (§6.4): the caller queues each when it may go on.
	// None is in Failed or Asked.
	Waiting []Waiting
	// RequeueAt, when set, is the soonest of Asked's times. They are absolute,
	// the deadline itself rather than a duration from some reading of the
	// clock, so the requeue lands exactly on it
	// (docs/credential-use-model-reduction.md §10.2). A time already past means
	// at once: it is the batch's now.
	RequeueAt time.Time
}

// Asked is an object a batch must look at again, and when.
type Asked struct {
	ID ID
	At time.Time
}

// Waiting is an object whose release must wait, as R1 read it.
type Waiting struct {
	ID  ID
	UID types.UID
}

func (r *ReleaseResult) requeueAt(now, at time.Time, id ID) {
	if at.Before(now) {
		at = now
	}
	r.Asked = append(r.Asked, Asked{id, at})
	if r.RequeueAt.IsZero() || at.Before(r.RequeueAt) {
		r.RequeueAt = at
	}
}

// What one batch, a turn of its key, covers of the objects that R0 leaves (§6.6):
// maxTurnReads at most, of which at least minTurnOthers, when there are so many,
// are ones not known to carry the finalizer. And none of R1's reads
// begins once turnReadTime is over, but the first of each sort (turnOf): a client
// of few requests a second reads fewer than 64, and what it read is decided.
const (
	maxTurnReads  = 64
	minTurnOthers = 16
	turnReadTime  = 10 * time.Second
)

// Each read that begins has a time of its own, whatever its turn's: a read cut
// short by others' would be refused unread, as client-go's rate limiter does, or
// cut at its every try. It is releaseReadLimit, doubled at each failure of its
// object in a row, to maxReadLimit, the API server's own for a request: so an
// object whose reads take long is released late, not never.
const (
	releaseReadLimit = 10 * time.Second
	maxReadLimit     = 60 * time.Second
)

// readLimitAfter is the time of the read of an object that failed n times.
func readLimitAfter(n int) time.Duration {
	return min(releaseReadLimit<<min(n, 3), maxReadLimit)
}

// read is R1's GET of id, for holder namespace n, unless its kind couldn't be
// mapped just before (missedKinds).
func (p *protocol) read(ctx context.Context, n string, id ID) (*metav1.PartialObjectMetadata, error) {
	limit := releaseReadLimit
	if p.readLimit != nil {
		limit = p.readLimit(n, id)
	}
	if err := p.missed.known(id.GroupKind(), p.clock); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	m, err := p.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
	if meta.IsNoMatchError(err) {
		p.missed.add(id.GroupKind(), err, p.clock.Now())
	}
	return m, err
}

// readNamespace is R2's GET of Namespace ns.
func (p *protocol) readNamespace(ctx context.Context, ns string) (*metav1.PartialObjectMetadata, error) {
	limit := releaseReadLimit
	if p.namespaceLimit != 0 {
		limit = p.namespaceLimit
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	return p.srv.Get(ctx, namespaceGroupKind, "", ns)
}

// readTime is turnReadTime, unless a test sets its own.
func (p *protocol) readTime() time.Duration {
	if p.turnTime != 0 {
		return p.turnTime
	}
	return turnReadTime
}

// turnOf splits todo into what this turn reads, in the order it does, and the
// rest. Those that shown says are known to carry the finalizer, from how they were
// queued or the caches, are read first, since they are the likely releases; but
// the others may be real holds too, as outside the caches, so they have a part in
// every turn that no number of the former can take. Each sort is in todo's order,
// that of waiting; first are the first of each, which the turn reads however long
// its reads take. So every turn decides the one of each sort that has waited
// longest, or fails it: an object waits for no more turns than objects were queued
// before it, whatever is queued since, and whichever sort the caches say it is of.
func turnOf(todo []ID, shown func(ID) bool) (now, rest []ID, first map[ID]bool) {
	var carry, others []ID
	for _, id := range todo {
		if shown(id) {
			carry = append(carry, id)
		} else {
			others = append(others, id)
		}
	}
	o := min(len(others), max(minTurnOthers, maxTurnReads-len(carry)))
	c := min(len(carry), maxTurnReads-o)
	first = map[ID]bool{}
	for _, sort := range [][]ID{carry[:c], others[:o]} {
		if len(sort) > 0 {
			first[sort[0]] = true
		}
	}
	return slices.Concat(carry[:c], others[:o]), slices.Concat(carry[c:], others[o:]), first
}

// r1Read is one protected object as R1 read it.
type r1Read struct {
	id       ID
	meta     *metav1.PartialObjectMetadata
	graceEnd time.Time
}

// release runs one batch for holder kind K and holder namespace n, over the
// protected objects pending there (§6.2 R0 to R3), in the order they have waited.
// R0 covers them all, and what it leaves is bounded (turnOf): the rest, and what
// the end of the turn's time, or of ctx, left unread, is Rest. It returns an error
// when some object couldn't be decided: Failed, which the caller requeues with
// backoff. Every object it decided is in the result either way, and none of the
// rest is.
func (p *protocol) release(ctx context.Context, n string, pending []ID) (ReleaseResult, error) {
	var res ReleaseResult
	f, err := FinalizerName(p.holder, n)
	if err != nil {
		res.Failed = pending
		return res, err
	}
	prefix, err := FinalizerPrefix(p.holder)
	if err != nil {
		res.Failed = pending
		return res, err
	}
	var errs []error
	fail := func(err error, ids ...ID) {
		errs = append(errs, err)
		res.Failed = append(res.Failed, ids...)
	}

	// R0. The fast path, from the caches: but nothing from the holder cache
	// of a kind whose CRD is absent, which may show holders that are gone for
	// as long as it is.
	records := p.newBatchRecords(pending)
	cachedUsers := map[ID]cachedUse{}
	absent := p.kindAbsent != nil && p.kindAbsent()
	var todo []ID
	for _, id := range pending {
		if _, again := cachedUsers[id]; again {
			continue
		}
		if ctx.Err() != nil {
			res.Rest = append(res.Rest, id)
			continue
		}
		var u cachedUse
		if !absent {
			u = p.fastKeep(id, n, f, records)
		}
		cachedUsers[id] = u
		if !u.keep {
			todo = append(todo, id)
		} else if u.deleting {
			res.Kept = append(res.Kept, Kept{P: id, Why: KeptUserDeleting, Users: u.users, DeletingUsers: u.deletingUsers})
		}
	}
	todo, rest, first := turnOf(todo, func(id ID) bool {
		return cachedUsers[id].carries || p.known != nil && p.known(n, id)
	})
	res.Rest = append(res.Rest, rest...)

	builtFrom := map[ID]string{} // the resourceVersion each failed release was built from
	for round := 0; len(todo) > 0; round++ {
		if round == maxReleaseRounds {
			fail(fmt.Errorf("credential-use: the state of %v kept changing under %d releases", todo, maxReleaseRounds), todo...)
			break
		}

		// R1. Each pending object, live.
		var live []r1Read
		window, closeWindow := context.WithTimeout(ctx, p.readTime())
		for _, id := range todo {
			if ctx.Err() != nil || !first[id] && window.Err() != nil {
				res.Rest = append(res.Rest, id)
				continue
			}
			m, err := p.read(ctx, n, id)
			switch Classify(err) {
			case ClassNone:
			case ClassNotFound:
				p.seen.forget(uidOf(p.views, id))
				res.Bare = append(res.Bare, id)
				continue // gone: nothing to release
			default:
				fail(fmt.Errorf("credential-use: reading %s: %w", id, err), id)
				continue
			}
			if rv, ok := builtFrom[id]; ok && rv == m.ResourceVersion {
				p.reports.ReleaseRejected(p.holder, id, rv)
				fail(fmt.Errorf("%w: %s at resourceVersion %s", ErrReleaseRejected, id, rv), id)
				continue
			}
			if !hasFinalizer(m, f) {
				p.seen.forget(m.UID)
				if countPrefix(m.Finalizers, prefix) == 0 {
					res.Bare = append(res.Bare, id)
				}
				continue // done
			}
			if m.Annotations[f] == "" {
				if err := p.restoreEpoch(ctx, id, m, n); err != nil {
					fail(err, id)
					continue
				}
				now := p.clock.Now()
				res.requeueAt(now, now.Add(p.epochRequeue()), id)
				continue
			}
			r := r1Read{id: id, meta: m}
			if m.DeletionTimestamp != nil {
				now := p.clock.Now()
				r.graceEnd = GraceEnd(m.DeletionTimestamp.Time, p.seen.see(m.UID, now), p.grace)
				// Inside the grace, in a namespace last seen live, with users:
				// nothing to decide until the grace ends (§6.2 R1).
				if u := cachedUsers[id]; now.Before(r.graceEnd) && len(u.users) > 0 && !p.namespaceDeletingCached(id.Namespace) {
					res.requeueAt(now, r.graceEnd, id)
					res.Kept = append(res.Kept, Kept{P: id, Why: KeptGrace, GraceEnd: r.graceEnd, Users: u.users, DeletingUsers: u.deletingUsers})
					continue
				}
				// The wait before it may be released (§6.4), except in a
				// namespace being deleted, where it can't be created again.
				if w := (Waiting{id, m.UID}); p.mustWait != nil && !p.namespaceDeletingCached(id.Namespace) && p.mustWait(n, w) {
					res.Waiting = append(res.Waiting, w)
					continue
				}
			}
			live = append(live, r)
		}
		closeWindow()
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
		nsFailed := map[string]bool{} // the error is one for all that are in it
		window, closeWindow = context.WithTimeout(ctx, p.readTime())
		var decidable []r1Read
		for _, r := range live {
			ns := r.id.Namespace
			if r.meta.DeletionTimestamp == nil || ns == "" {
				decidable = append(decidable, r)
				continue
			}
			if nsFailed[ns] {
				res.Failed = append(res.Failed, r.id)
				continue
			}
			if _, done := nsReadAt[ns]; !done {
				// As R1's reads: none begins once the time is over, but the first.
				if len(nsReadAt)+len(nsFailed) > 0 && window.Err() != nil {
					res.Rest = append(res.Rest, r.id)
					continue
				}
				m, err := p.readNamespace(ctx, ns)
				switch Classify(err) {
				case ClassNone:
					nsDeleting[ns] = m.DeletionTimestamp != nil
				case ClassNotFound:
					// A namespace that is gone can't be recreated in time to
					// matter: treat it as deleting.
					nsDeleting[ns] = true
				default:
					nsFailed[ns] = true
					fail(fmt.Errorf("credential-use: reading namespace %s: %w", ns, err), r.id)
					continue
				}
				nsReadAt[ns] = p.clock.Now()
			}
			decidable = append(decidable, r)
		}
		closeWindow()
		if len(decidable) == 0 {
			break
		}
		holders, gone, err := p.listHolders(ctx, n)
		if err != nil {
			fail(fmt.Errorf("credential-use: listing %s in %s: %w", p.holder, n, err))
			for _, r := range decidable {
				res.Failed = append(res.Failed, r.id)
			}
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
			var users, deletingUsers []types.NamespacedName
			for i := range holders {
				nn := types.NamespacedName{Namespace: holders[i].Namespace, Name: holders[i].Name}
				if records.isUser(&holders[i], r.id) {
					in.Users++
					users = append(users, nn)
				}
				if records.isDeletingUser(&holders[i], r.id) {
					in.DeletingUsers++
					deletingUsers = append(deletingUsers, nn)
				}
			}
			d := Decide(in)
			res.Decisions = append(res.Decisions, ReleaseDecision{P: r.id, Input: in, Decision: d, NamespaceReadAt: nsReadAt[r.id.Namespace], GraceEnd: r.graceEnd})
			kept := Kept{P: r.id, GraceEnd: r.graceEnd, Users: users, DeletingUsers: deletingUsers}
			switch d {
			case Keep:
				if in.Deleting {
					kept.Why = KeptUserDeleting
					if in.NamespaceDeleting {
						kept.Why = KeptNamespaceDeleting
					}
					res.Kept = append(res.Kept, kept)
				}
				continue
			case WaitForGrace:
				res.requeueAt(now, r.graceEnd, r.id)
				kept.Why = KeptGrace
				res.Kept = append(res.Kept, kept)
				continue
			}
			pt, err := ReleasePatch(r.meta, p.holder, n)
			if err != nil {
				fail(err, r.id)
				continue
			}
			out, err := p.srv.Patch(ctx, r.id.GroupKind(), r.id.Namespace, r.id.Name, pt.Type, pt.Data)
			switch ClassifyJSONPatch(err) {
			case ClassNone:
				p.ver.released(r.meta.UID, n)
				p.seen.forget(r.meta.UID)
				res.Released = append(res.Released, r.id)
				if gone && p.releasedGone != nil {
					p.releasedGone(r.id)
				}
				if out != nil && countPrefix(out.Finalizers, prefix) == 0 {
					// R3 removed the kind's last finalizer, and its marker.
					res.Bare = append(res.Bare, r.id)
				}
			case ClassStateChanged:
				builtFrom[r.id] = r.meta.ResourceVersion
				next = append(next, r.id) // back to R1
			case ClassNotFound:
				res.Bare = append(res.Bare, r.id) // gone
			default:
				fail(fmt.Errorf("credential-use: releasing %s: %w", r.id, err), r.id)
			}
		}
		todo = next
		if len(todo) > 0 {
			first = map[ID]bool{todo[0]: true}
		}
	}
	return res, errors.Join(errs...)
}

// listHolders is R2's list of the holder kind in n, or an empty one when it
// fails because the kind is gone, and its holders with it (noHolders, §6.4):
// it reports which.
func (p *protocol) listHolders(ctx context.Context, n string) (holders []metav1.PartialObjectMetadata, gone bool, err error) {
	holders, err = p.srv.List(ctx, p.holder, n)
	if err != nil && p.noHolders != nil {
		err = p.noHolders(ctx, err)
		return nil, err == nil, err
	}
	return holders, false, err
}

// cachedUse is R0's reading of one object's users in the caches.
type cachedUse struct {
	users, deletingUsers []types.NamespacedName
	// deleting: the cached object is being deleted.
	deleting bool
	// carries: the cached object carries the finalizer.
	carries bool
	// keep: the cache already settles the batch for the object.
	keep bool
}

// fastKeep is R0 for one object (§6.2): it reports the cached users of P in
// n, whether the cached P carries finalizer f, K's for n, and whether the cache
// already settles the batch for P. A stale cache can only keep a hold longer.
func (p *protocol) fastKeep(id ID, n, f string, records *batchRecords) cachedUse {
	var u cachedUse
	cached, ok := p.views.Protected(id)
	for _, h := range p.views.HoldersNaming(id, n) {
		nn := types.NamespacedName{Namespace: h.Namespace, Name: h.Name}
		if records.isUser(h, id) {
			u.users = append(u.users, nn)
		}
		if records.isDeletingUser(h, id) {
			u.deletingUsers = append(u.deletingUsers, nn)
		}
	}
	if !ok {
		return u
	}
	u.deleting = cached.DeletionTimestamp != nil
	u.carries = hasFinalizer(cached, f)
	if !u.deleting {
		u.keep = len(u.users) > 0
	} else {
		u.keep = len(u.deletingUsers) > 0
	}
	return u
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
