package credentialuse

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// Hooks for the envtests of the external test package.

// OnHolderEvent sets a function that sees every holder version a handler of
// any tracker gets, before the handler touches the verified set.
func (s *Shared) OnHolderEvent(f func(t *Tracker, m *metav1.PartialObjectMetadata)) {
	s.hooks.holderEvent = f
}

// OnProtectedEvent sets a function that sees every protected version a
// tracker's protected handler gets, before the handler acts on it.
func (s *Shared) OnProtectedEvent(f func(t *Tracker, m *metav1.PartialObjectMetadata)) {
	s.hooks.protectedEvent = f
}

// OnNamespaceAct sets a function that sees every Namespace change the
// Namespace handler acts on, as the handler has it.
func (s *Shared) OnNamespaceAct(f func(n *metav1.PartialObjectMetadata)) { s.hooks.namespaceAct = f }

// BeforeListRecorded sets a function that runs when a holder informer's
// list or relist has completed, just before the completion is recorded.
func (s *Shared) BeforeListRecorded(f func(t *Tracker)) { s.hooks.beforeListRecorded = f }

// FollowChains has every tracker a later Track builds follow chains, which
// Acquire otherwise refuses (ErrChainsNotSupported): the chain code's own
// tests, which keep it working for the release that supports it (§9).
//
// Every test whose refs have a Next must call it, or set its protocol's
// followChains. A refused pass looks like a failed one (MustNotConnect, or
// Degraded with the record kept), so a test of a chain that fails would pass
// without following it.
func (s *Shared) FollowChains() { s.hooks.followChains = true }

// SetSyncTimeout bounds each informer's first sync.
func (s *Shared) SetSyncTimeout(d time.Duration) { s.syncTimeout.Store(int64(d)) }

// SetReleaseWait sets what draws the wait before the release of an object being
// deleted (§6.4); nil is no wait. Set it before Track.
func (s *Shared) SetReleaseWait(f func() time.Duration) { s.releaseWait = f }

// WaitEnd is when the wait of id, in the batch of gk's holders in ns, ends or
// ended, while it is kept: until a batch decides id.
func (s *Shared) WaitEnd(gk schema.GroupKind, ns string, id ID) (time.Time, bool) {
	s.waits.mu.Lock()
	defer s.waits.mu.Unlock()
	e, ok := s.waits.ends[batchKey{gk, ns}][id]
	return e.end, ok
}

// QueueLoad is how many batches wait for a release worker, and how many are
// worked on.
func (s *Shared) QueueLoad() (ready, working int) {
	s.queue.mu.Lock()
	defer s.queue.mu.Unlock()
	return len(s.queue.ready), len(s.queue.working)
}

// SetLandingMargin replaces LandingMargin, so that Disown's passes can be
// short. Set it before Track.
func (s *Shared) SetLandingMargin(d time.Duration) { s.hooks.landingMargin = &d }

// SetPruneQuiet replaces how long the prunes are kept back once the write
// loops have started. Set it before they do.
func (s *Shared) SetPruneQuiet(d time.Duration) { s.hooks.pruneQuiet = &d }

// QueuedBatch reports whether the batch of holder kind gk in namespace ns is
// waiting, and the protected objects it covers.
func (s *Shared) QueuedBatch(gk schema.GroupKind, ns string) (bool, []ID) {
	q := s.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	k := batchKey{gk, ns}
	return q.queued[k], SortedIDs(keysOf(q.pending[k]))
}

// DrainQueue takes every batch waiting in the queue, as a release loop with
// nothing to do would, and returns how many it took.
func (s *Shared) DrainQueue() int {
	n := 0
	for s.queue.Len() > 0 {
		k, _, ok := s.queue.Get(context.Background())
		if !ok {
			break
		}
		s.queue.Done(k)
		n++
	}
	return n
}

// LastList is when the holder informer's latest list or relist completed.
func (t *Tracker) LastList() time.Time {
	t.ver.mu.Lock()
	defer t.ver.mu.Unlock()
	return t.ver.lastList
}

// HolderSynced reports whether the holder informer has synced.
func (t *Tracker) HolderSynced() bool { return t.holders.HasSynced() }

func (t *Tracker) testLW() *testListWatch { return t.holderLW.(*testListWatch) }

// ExpireHolderWatch ends the holder informer's watches with a 410, so that
// its reflector relists.
func (t *Tracker) ExpireHolderWatch() { t.testLW().expire() }

// RefuseWatchList makes the holder informer's watch-list requests fail, so
// that its reflector falls back to a List.
func (t *Tracker) RefuseWatchList(refuse bool) { t.testLW().refuseWatchList.Store(refuse) }

// PauseHolderWatch holds the holder informer's watch events, so that its
// cache lags behind the API server, until ResumeHolderWatch.
func (t *Tracker) PauseHolderWatch() { t.testLW().pause() }

// ResumeHolderWatch lets the held holder events go.
func (t *Tracker) ResumeHolderWatch() { t.testLW().resume() }

// HolderLists counts the holder informer's completed lists: by List, and by
// watch-list stream.
func (t *Tracker) HolderLists() (lists, streams int64) {
	return t.testLW().lists.Load(), t.testLW().streams.Load()
}

// HookCalls sets functions that run before and after every API call the
// trackers' protocols and the janitor make: an error from before fails the
// call. Set it before Track.
//
// The index's calls are hooked too (§10.6), as Ops "index-get",
// "index-update" and "index-create" on the ConfigMap; a write's Call carries
// the lines it leaves.
func (s *Shared) HookCalls(before func(ctx context.Context, c Call) error, after func(c Call, err error)) {
	s.hooks.wrapServer = func(srv server) server { return hookServer{server: srv, before: before, after: after} }
	s.hooks.wrapIndex = func(st indexStore) indexStore { return hookIndexStore{st: st, before: before, after: after} }
}

// KindUnserved and CRDNotFound are what R2 takes for a kind that isn't
// served, and for a CRD that is gone (§6.4).
func KindUnserved(err error) bool             { return kindUnserved(err) }
func CRDNotFound(err error, name string) bool { return crdNotFound(err, name) }

// PolicyParamsMissing reports whether err is the index namespace's policy
// failing for want of its parameters, as the tracker matches it (§10.6.1).
func PolicyParamsMissing(err error) bool { return policyParamsMissing(err) }

// Index is the tracker's index ConfigMap, or false without one.
func (t *Tracker) Index() (types.NamespacedName, bool) {
	if t.idx == nil {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: t.idx.namespace, Name: t.idx.name}, true
}

// PerNameSynced reports whether the per-name watch of id has synced.
func (t *Tracker) PerNameSynced(id ID) bool {
	p := t.names.get(id)
	return p != nil && p.inf.HasSynced()
}

// PerNameWatches returns the IDs the tracker watches by name.
func (t *Tracker) PerNameWatches() []ID {
	var out []ID
	for _, p := range t.names.all() {
		out = append(out, ID{Group: p.gk.Group, Kind: p.gk.Kind, Namespace: p.ns, Name: p.name})
	}
	return SortedIDs(out)
}

// PendingIndexChecks returns the objects waiting for a live check of their
// index line, and whether a rebuild is pending.
func (t *Tracker) PendingIndexChecks() ([]ID, bool) {
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	return SortedIDs(keysOf(t.idx.pending)), t.idx.lost
}

// hookIndexStore is an index store whose calls a test hooks.
type hookIndexStore struct {
	st     indexStore
	before func(ctx context.Context, c Call) error
	after  func(c Call, err error)
}

func (h hookIndexStore) call(ctx context.Context, op string, lines []string, f func() error) error {
	c := Call{Op: op, GK: configMapGroupKind, Lines: lines}
	if hs, ok := h.st.(configMapStore); ok {
		c.Namespace, c.Name = hs.namespace, hs.name
	}
	if h.before != nil {
		if err := h.before(ctx, c); err != nil {
			return err
		}
	}
	err := f()
	if h.after != nil {
		h.after(c, err)
	}
	return err
}

func (h hookIndexStore) get(ctx context.Context) (v indexVersion, err error) {
	err = h.call(ctx, "index-get", nil, func() error { v, err = h.st.get(ctx); return err })
	return v, err
}

func (h hookIndexStore) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (v indexVersion, err error) {
	err = h.call(ctx, "index-update", lines, func() error { v, err = h.st.update(ctx, cur, lines, mark); return err })
	return v, err
}

func (h hookIndexStore) create(ctx context.Context, lines []string) (v indexVersion, err error) {
	err = h.call(ctx, "index-create", lines, func() error { v, err = h.st.create(ctx, lines); return err })
	return v, err
}
