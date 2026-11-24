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
// The queue never calls its clock while holding mu. A clock may run a
// timer's function while holding a lock of its own, as the fake clock of
// k8s.io/utils does under Step, and that function takes mu; so calling the
// clock under mu would order the two locks both ways (D1 of the review of
// #109). AddAfter therefore registers a timer in two steps, and ShutDown
// stops the timers after releasing mu.
type batchQueue struct {
	clock clock.WithDelayedExecution

	mu      sync.Mutex
	cond    *sync.Cond
	pending map[batchKey]map[ID]bool // what each key's next batch covers
	ready   []batchKey               // keys waiting to be taken, in order
	queued  map[batchKey]bool        // in ready
	working map[batchKey]bool        // taken, not yet done
	timers  map[*queueTimer]bool     // AddAfter's and Retry's timers neither fired nor stopped
	retries map[batchKey]*queueTimer // each key's pending retry, in timers too
	stopped bool
}

func newBatchQueue(c clock.WithDelayedExecution) *batchQueue {
	q := &batchQueue{
		clock:   c,
		pending: map[batchKey]map[ID]bool{},
		queued:  map[batchKey]bool{},
		working: map[batchKey]bool{},
		timers:  map[*queueTimer]bool{},
		retries: map[batchKey]*queueTimer{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// fillAll is never an object: it stands in a batch for "every object of
// the namespace that holds K's finalizer for N", which an Add with no ids
// asks for. It is kept with the batch's objects, so it survives the merge
// of later additions, and the release loop replaces it with the held
// objects it lists (the review of #114: an empty batch merged with an
// object's lost the namespace's list).
var fillAll = ID{Kind: "\x00all"}

// Add queues k's batch, covering ids as well as whatever it covered already.
// With no ids it covers every held object of the namespace too (fillAll),
// which the release loop lists itself. After ShutDown it does nothing.
func (q *batchQueue) Add(k batchKey, ids ...ID) {
	if len(ids) == 0 {
		ids = []ID{fillAll}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.addLocked(k, ids)
}

func (q *batchQueue) addLocked(k batchKey, ids []ID) {
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

// A queueTimer is one AddAfter, or a key's pending retry. It is in the queue's
// timers from before the clock is asked for a timer until the timer fires, or
// ShutDown or a sooner retry takes it; t is set once the clock has returned
// it.
type queueTimer struct {
	t   clock.Timer
	ids []ID
	at  time.Time // a retry's: when it is due
}

// AddAfter adds k's batch, covering ids, once d has passed on the queue's
// clock: the end of a grace (§5, §10.2). A d of zero or less adds at once.
// After ShutDown it does nothing.
func (q *batchQueue) AddAfter(k batchKey, d time.Duration, ids ...ID) {
	if d <= 0 {
		q.Add(k, ids...)
		return
	}
	e := &queueTimer{ids: slices.Clone(ids)}
	if len(ids) == 0 {
		e.ids = []ID{fillAll}
	}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.timers[e] = true
	q.mu.Unlock()
	q.arm(k, d, e)
}

// Retry adds k's batch, covering ids, once d has passed, as AddAfter does,
// except that a key has one pending retry: ids join the one it has, which
// keeps the sooner of the two times. So a key whose batches keep failing,
// however often events queue them, is retried once per backoff. A pending
// retry isn't AddAfter's: a grace's end is its own. A d of zero or less adds
// at once.
func (q *batchQueue) Retry(k batchKey, d time.Duration, ids ...ID) {
	if d <= 0 {
		q.Add(k, ids...)
		return
	}
	if len(ids) == 0 {
		ids = []ID{fillAll}
	}
	e := &queueTimer{ids: slices.Clone(ids), at: q.clock.Now().Add(d)}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	var later clock.Timer
	if old := q.retries[k]; old != nil {
		e.ids = slices.Compact(SortedIDs(append(e.ids, old.ids...)))
		if !e.at.Before(old.at) {
			old.ids = e.ids
			q.mu.Unlock()
			return
		}
		delete(q.timers, old)
		later = old.t
	}
	q.timers[e] = true
	q.retries[k] = e
	q.mu.Unlock()
	if later != nil {
		later.Stop()
	}
	q.arm(k, d, e)
}

// arm asks the clock for e's timer, which adds k's batch. e is in timers.
func (q *batchQueue) arm(k batchKey, d time.Duration, e *queueTimer) {
	t := q.clock.AfterFunc(d, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if !q.timers[e] {
			return
		}
		delete(q.timers, e)
		if q.retries[k] == e {
			delete(q.retries, k)
		}
		q.addLocked(k, e.ids)
	})

	q.mu.Lock()
	live := q.timers[e]
	if live {
		e.t = t
	}
	q.mu.Unlock()
	if !live {
		// It fired already, or ShutDown or a sooner retry took it before it
		// had a timer to stop: either way it is no longer the queue's, and
		// stopping it is harmless if it fired.
		t.Stop()
	}
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
// A timer that fires while ShutDown runs adds nothing.
func (q *batchQueue) ShutDown() {
	q.mu.Lock()
	q.stopped = true
	var stop []clock.Timer
	for e := range q.timers {
		if e.t != nil {
			stop = append(stop, e.t)
		}
	}
	clear(q.timers)
	clear(q.retries)
	q.ready, q.pending = nil, map[batchKey]map[ID]bool{}
	clear(q.queued)
	q.cond.Broadcast()
	q.mu.Unlock()
	for _, t := range stop {
		t.Stop()
	}
}

func keysOf[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
