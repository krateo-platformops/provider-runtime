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
// #109). AddAfter and Fail therefore register a timer in two steps, and Settle
// and ShutDown stop timers after releasing mu.
type batchQueue struct {
	clock clock.WithDelayedExecution

	mu      sync.Mutex
	cond    *sync.Cond
	pending map[batchKey]map[ID]bool // what each key's next batch covers
	ready   []batchKey               // keys waiting to be taken, in order
	queued  map[batchKey]bool        // in ready
	working map[batchKey]bool        // taken, not yet done
	timers  map[*queueTimer]bool     // AddAfter's, AddAt's and Fail's, neither fired nor stopped
	failing map[batchKey]map[ID]*failure
	asked   map[askedFor]bool                // what AddAt's timers wait for
	backoff func(failures int) time.Duration // backoffAfter, but for tests
	stopped bool

	// afterAdd, if set, runs after each Add, outside mu, and not after what a
	// timer adds: a test's, as of a worker that takes the batch at once.
	afterAdd func(k batchKey)
}

func newBatchQueue(c clock.WithDelayedExecution) *batchQueue {
	q := &batchQueue{
		clock:   c,
		pending: map[batchKey]map[ID]bool{},
		queued:  map[batchKey]bool{},
		working: map[batchKey]bool{},
		timers:  map[*queueTimer]bool{},
		failing: map[batchKey]map[ID]*failure{},
		asked:   map[askedFor]bool{},
		backoff: backoffAfter,
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
	if q.afterAdd != nil {
		defer q.afterAdd(k)
	}
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

// A queueTimer is one AddAfter, or AddAt, or the pending retry of the objects
// that one Fail gave the same delay. It is in the queue's timers from before the clock
// is asked for a timer until the timer fires, ShutDown takes it, or everything
// it would retry is decided; t is set once the clock has returned it.
type queueTimer struct {
	t     clock.Timer
	ids   []ID
	retry bool
	left  int   // a retry's objects that still wait for it
	at    int64 // an AddAt's time
}

// askedFor is one object's time that an AddAt's timer waits for.
type askedFor struct {
	k  batchKey
	id ID
	at int64 // Unix nanoseconds
}

// A failure is an object that a batch of a key failed at, and none decided
// since: how often in a row its retries failed, and the one it waits for. It is
// the name's: one deleted and created again while no batch decided it inherits
// the count, until one does.
type failure struct {
	n     int
	waits *queueTimer
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

// AddAt adds k's batch, covering ids, at the time at, as AddAfter does, but an
// object's time has one timer: one that a batch asks for again, as every batch
// that finds an object in its grace does, is there already. Unlike AddAfter,
// with no ids it does nothing.
func (q *batchQueue) AddAt(k batchKey, at time.Time, ids ...ID) {
	if len(ids) == 0 {
		return
	}
	d := at.Sub(q.clock.Now())
	if d <= 0 {
		q.Add(k, ids...)
		return
	}
	e := &queueTimer{at: at.UnixNano()}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	for _, id := range ids {
		if f := (askedFor{k, id, e.at}); !q.asked[f] {
			q.asked[f] = true
			e.ids = append(e.ids, id)
		}
	}
	if len(e.ids) == 0 {
		q.mu.Unlock()
		return
	}
	q.timers[e] = true
	q.mu.Unlock()
	q.arm(k, d, e)
}

// Fail schedules the retry of ids, which k's batch failed at: each object on a
// backoff of its own, from releaseBackoffBase, doubling at each retry that
// fails, to releaseBackoffMax. So one that fails for good puts off no other.
// An object that already waits for its retry keeps it, and isn't counted
// again: what failed was a run an event queued. Objects of one Fail that get
// the same delay share its timer, so there are never more timers than objects.
// It returns the soonest delay it gave, or 0 if all of them were waiting. After
// ShutDown it does nothing. Fail and Settle are for the worker of k's batch,
// one at a time: a Settle forgets a Fail, so what one decided mustn't race what
// another failed at.
func (q *batchQueue) Fail(k batchKey, ids ...ID) (soonest time.Duration) {
	q.mu.Lock()
	if q.stopped || len(ids) == 0 {
		q.mu.Unlock()
		return 0
	}
	m := q.failing[k]
	if m == nil {
		m = map[ID]*failure{}
		q.failing[k] = m
	}
	timers := map[time.Duration]*queueTimer{}
	for _, id := range ids {
		f := m[id]
		if f == nil {
			f = &failure{}
			m[id] = f
		}
		if f.waits != nil {
			continue
		}
		d := q.backoff(f.n)
		f.n++
		e := timers[d]
		if e == nil {
			e = &queueTimer{retry: true}
			timers[d] = e
			q.timers[e] = true
		}
		e.ids = append(e.ids, id)
		e.left++
		f.waits = e
	}
	q.mu.Unlock()
	for d, e := range timers {
		if soonest == 0 || d < soonest {
			soonest = d
		}
		q.arm(k, d, e)
	}
	return soonest
}

// Settle records that ids of k's batch were decided: what they waited for, and
// how often they had failed, is forgotten. R0's keep, from the caches alone,
// decides too (§6.2): the user it saw is in the way of any release, and its own
// event runs the batch again when it goes, but for a user whose record can't be
// parsed, and one of a kind whose CRD is gone (§6.2).
func (q *batchQueue) Settle(k batchKey, ids ...ID) {
	var stop []clock.Timer
	q.mu.Lock()
	m := q.failing[k]
	for _, id := range ids {
		f := m[id]
		if f == nil {
			continue
		}
		delete(m, id)
		if e := f.waits; e != nil {
			if e.left--; e.left == 0 {
				delete(q.timers, e)
				if e.t != nil {
					stop = append(stop, e.t)
				}
			}
		}
	}
	if m != nil && len(m) == 0 {
		delete(q.failing, k)
	}
	q.mu.Unlock()
	for _, t := range stop {
		t.Stop()
	}
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
		ids := e.ids
		if e.at != 0 {
			for _, id := range ids {
				delete(q.asked, askedFor{k, id, e.at})
			}
		}
		if e.retry {
			ids = nil
			for _, id := range e.ids {
				// Not one decided since, which may have failed again.
				if f := q.failing[k][id]; f != nil && f.waits == e {
					f.waits = nil
					ids = append(ids, id)
				}
			}
		}
		q.addLocked(k, ids)
	})

	q.mu.Lock()
	live := q.timers[e]
	if live {
		e.t = t
	}
	q.mu.Unlock()
	if !live {
		// It fired already, or ShutDown or a Settle took it before it had a
		// timer to stop: either way it is no longer the queue's, and stopping
		// it is harmless if it fired.
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
	clear(q.failing)
	clear(q.asked)
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
