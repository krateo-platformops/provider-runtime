package credentialuse

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Hooks for the envtests of the external test package.

// OnHolderEvent sets a function that sees every holder version a handler of
// any tracker gets, before the handler touches the verified set.
func (s *Shared) OnHolderEvent(f func(t *Tracker, m *metav1.PartialObjectMetadata)) {
	s.onHolderEvent = f
}

// OnNamespaceAct sets a function that sees every Namespace change the
// Namespace handler acts on.
func (s *Shared) OnNamespaceAct(f func(namespace string)) { s.onNamespaceAct = f }

// SetSyncTimeout bounds each informer's first sync.
func (s *Shared) SetSyncTimeout(d time.Duration) { s.syncTimeout = d }

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

// ExpireHolderWatch ends the holder informer's watches with a 410, so that
// its reflector relists.
func (t *Tracker) ExpireHolderWatch() { t.lw.expire() }

// RefuseWatchList makes the holder informer's watch-list requests fail, so
// that its reflector falls back to a List.
func (t *Tracker) RefuseWatchList(refuse bool) { t.lw.refuseWatchList.Store(refuse) }

// HolderLists counts the holder informer's completed lists: by List, and by
// watch-list stream.
func (t *Tracker) HolderLists() (lists, streams int64) { return t.lw.lists.Load(), t.lw.streams.Load() }
