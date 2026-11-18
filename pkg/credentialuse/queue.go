package credentialuse

import (
	"context"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"
)

// A batchKey is one release batch: holder kind K's holds in namespace N
// (§6.2, §10.2). Every protected object a batch covers carries K's
// finalizer for N.
type batchKey struct {
	Holder    schema.GroupKind
	Namespace string
}

// batchQueue is the (K, N) batch queue (§10.2, §7.5). NewShared creates it
// before any informer starts, so that it buffers and deduplicates from the
// first event, the grace's timers included, while the write loops wait for
// leader election (M1 of the review of revision 16). The release loop drains
// it once it runs.
//
// A key is queued once however often it is added, and the protected objects
// added with it accumulate until the batch is taken. A key added while its
// batch is being worked on is queued again when the work is done, with what
// was added meanwhile. Timers run on the queue's clock, so tests drive them
// with a fake one.
//
// No lock of the queue is held while a timer's add or a caller runs.
type batchQueue struct {
	clock clock.WithDelayedExecution

	mu      sync.Mutex
	cond    *sync.Cond
	pending map[batchKey]map[ID]bool // what each key's next batch covers
	ready   []batchKey               // keys waiting to be taken, in order
	queued  map[batchKey]bool        // in ready
	working map[batchKey]bool        // taken, not yet done
	timers  map[clock.Timer]bool     // AddAfter timers not yet fired
	stopped bool
}

func newBatchQueue(c clock.WithDelayedExecution) *batchQueue {
	q := &batchQueue{
		clock:   c,
		pending: map[batchKey]map[ID]bool{},
		queued:  map[batchKey]bool{},
		working: map[batchKey]bool{},
		timers:  map[clock.Timer]bool{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Add queues k's batch, covering ids as well as whatever it covered already.
// With no ids it still queues the batch, which then lists the namespace's
// held objects itself. After ShutDown it does nothing.
func (q *batchQueue) Add(k batchKey, ids ...ID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	set := q.pending[k]
	if set == nil {
		set = map[ID]bool{}
		q.pending[k] = set
	}
	for _, id := range ids {
		set[id] = true
	}
	if !q.queued[k] && !q.working[k] {
		q.queued[k] = true
		q.ready = append(q.ready, k)
		q.cond.Signal()
	}
}

// AddAfter adds k's batch, covering ids, once d has passed on the queue's
// clock: the end of a grace (§5, §10.2). A d of zero or less adds at once.
func (q *batchQueue) AddAfter(k batchKey, d time.Duration, ids ...ID) {
	if d <= 0 {
		q.Add(k, ids...)
		return
	}
	ids = slices.Clone(ids)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	var t clock.Timer
	t = q.clock.AfterFunc(d, func() {
		q.mu.Lock()
		delete(q.timers, t)
		q.mu.Unlock()
		q.Add(k, ids...)
	})
	q.timers[t] = true
}

// Get takes the next batch, waiting for one, and marks it as being worked
// on: the caller calls Done when it has finished. It returns false once the
// queue is shut down, or ctx is done.
func (q *batchQueue) Get(ctx context.Context) (batchKey, []ID, bool) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.cond.Broadcast()
	})
	defer stop()
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.ready) == 0 && !q.stopped && ctx.Err() == nil {
		q.cond.Wait()
	}
	if q.stopped || ctx.Err() != nil {
		return batchKey{}, nil, false
	}
	k := q.ready[0]
	q.ready = q.ready[1:]
	delete(q.queued, k)
	q.working[k] = true
	ids := SortedIDs(keysOf(q.pending[k]))
	delete(q.pending, k)
	return k, ids, true
}

// Done ends the work on k's batch. If k was added meanwhile, it is queued
// again with what was added.
func (q *batchQueue) Done(k batchKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.working, k)
	if _, again := q.pending[k]; again && !q.queued[k] && !q.stopped {
		q.queued[k] = true
		q.ready = append(q.ready, k)
		q.cond.Signal()
	}
}

// Len is the number of batches waiting to be taken.
func (q *batchQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready)
}

// ShutDown stops the timers, drops what is waiting, and wakes every Get.
func (q *batchQueue) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	for t := range q.timers {
		t.Stop()
	}
	clear(q.timers)
	q.ready, q.pending = nil, map[batchKey]map[ID]bool{}
	clear(q.queued)
	q.cond.Broadcast()
}

func keysOf[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
