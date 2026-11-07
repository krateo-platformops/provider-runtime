package ratelimit

import (
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/util/workqueue"
	utilclock "k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
)

// NewGatedQueue returns a controller queue whose workers take a slot in g's
// local capacity before they pop an item, and give it back when they call Done
// (docs/design-local-slot-before-pop.md). Low local capacity then behaves like
// fewer workers: the queue's order is the order work runs in, and nothing is
// popped that can't run, so no popped item sleeps through a freed slot. The slot
// covers the whole reconcile, because controller-runtime calls Done when the
// reconcile returns, on every path including a panic.
//
// A worker asks for a slot only when the queue has an item it can hand out, so
// an idle worker holds nothing, and one controller's idle workers can't take
// capacity from another controller's backlog on the same Governor. When several
// queues have work, the Governor grants freed slots round-robin.
//
// It keeps controller-runtime's priority queue inside for ordering and
// de-duplication, and owns the delays and the rate limiting itself, so it always
// knows how many items the inner queue can hand out without asking it. One
// difference from that queue: a delayed item is ordered among items of its
// priority by when it became ready, not by when it was added.
//
// controller.Options.ForControllerRuntime builds one for each controller when
// saturation is on. rateLimiter is used for AddRateLimited; nil means
// controller-runtime's default for its priority queue.
func NewGatedQueue[T comparable](g *Governor, name string, rateLimiter workqueue.TypedRateLimiter[T]) priorityqueue.PriorityQueue[T] {
	return newGatedQueue(g, name, rateLimiter, utilclock.RealClock{})
}

// gatedQueueRef is how the Governor grants a queue a slot.
type gatedQueueRef interface {
	// tryGrant hands one local slot to one of the queue's idle workers, with an
	// item reserved for it, and reports whether it did. The Governor calls it
	// holding g.mu; it takes the queue's lock.
	tryGrant() bool
}

type waitingItem struct {
	priority int
	at       time.Time
	timer    utilclock.Timer
	// gen tells a timer that fired late from the current one.
	gen uint64
}

type gatedQueue[T comparable] struct {
	gov         *Governor
	inner       priorityqueue.PriorityQueue[T]
	rateLimiter workqueue.TypedRateLimiter[T]
	clock       utilclock.WithDelayedExecution

	mu   sync.Mutex
	cond *sync.Cond
	// ready holds the keys in inner, and processing the keys a worker holds. The
	// keys inner can hand out are ready minus processing: it skips a key that is
	// being processed. free counts them.
	ready      map[T]struct{}
	processing map[T]struct{}
	free       int
	// waiting holds delayed keys until they are due.
	waiting map[T]*waitingItem
	// idle counts workers waiting for a slot; granted counts slots handed to
	// this queue whose worker hasn't popped yet; reserved counts the items held
	// for those slots, so free-reserved is what a new grant may take.
	idle, granted, reserved int
	shutdown                bool
}

func newGatedQueue[T comparable](g *Governor, name string, rateLimiter workqueue.TypedRateLimiter[T], clk utilclock.WithDelayedExecution) *gatedQueue[T] {
	if rateLimiter == nil {
		// The priority queue's own default.
		rateLimiter = workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second)
	}
	q := &gatedQueue[T]{
		gov:         g,
		inner:       priorityqueue.New(name, func(o *priorityqueue.Opts[T]) { o.Log = logr.Discard() }),
		rateLimiter: rateLimiter,
		clock:       clk,
		ready:       map[T]struct{}{},
		processing:  map[T]struct{}{},
		waiting:     map[T]*waitingItem{},
	}
	q.cond = sync.NewCond(&q.mu)
	g.addQueue(q)
	return q
}

// AddWithOpts adds items with the priority queue's rules: an item already
// queued keeps the higher of its priorities and the earlier of its ready times.
func (q *gatedQueue[T]) AddWithOpts(o priorityqueue.AddOpts, items ...T) {
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return
	}
	more := false
	for _, key := range items {
		after := o.After
		if o.RateLimited {
			if rl := q.rateLimiter.When(key); after == 0 || rl < after {
				after = rl
			}
		}
		priority := ptr.Deref(o.Priority, 0)
		if after > 0 {
			q.addWaitingLocked(key, after, priority)
		} else if q.addReadyLocked(key, priority) {
			more = true
		}
	}
	q.mu.Unlock()
	if more {
		q.gov.dispatch()
	}
}

// addReadyLocked puts key in inner now, and reports whether that made one more
// item available to hand out. A delayed entry for key is folded in.
func (q *gatedQueue[T]) addReadyLocked(key T, priority int) bool {
	if w, ok := q.waiting[key]; ok {
		priority = max(priority, w.priority)
		w.timer.Stop()
		delete(q.waiting, key)
	}
	q.inner.AddWithOpts(priorityqueue.AddOpts{Priority: &priority}, key)
	if _, ok := q.ready[key]; ok {
		return false
	}
	q.ready[key] = struct{}{}
	if _, busy := q.processing[key]; busy {
		return false
	}
	q.free++
	return true
}

// addWaitingLocked schedules key to become ready after a delay. A key already
// in inner stays ready, as in the priority queue, and only takes the priority.
func (q *gatedQueue[T]) addWaitingLocked(key T, after time.Duration, priority int) {
	if _, ok := q.ready[key]; ok {
		q.inner.AddWithOpts(priorityqueue.AddOpts{Priority: &priority}, key)
		return
	}
	at := q.clock.Now().Add(after)
	w, ok := q.waiting[key]
	if !ok {
		w = &waitingItem{priority: priority, at: at}
		q.waiting[key] = w
		q.scheduleLocked(key, w, after)
		return
	}
	w.priority = max(w.priority, priority)
	if at.Before(w.at) {
		w.timer.Stop()
		w.at = at
		q.scheduleLocked(key, w, after)
	}
}

func (q *gatedQueue[T]) scheduleLocked(key T, w *waitingItem, after time.Duration) {
	w.gen++
	gen := w.gen
	w.timer = q.clock.AfterFunc(after, func() { q.due(key, w, gen) })
}

// due makes every delayed key that is due ready at once, as the priority queue
// does, so items due together are handed out by priority, not in the order their
// timers ran. It does nothing when the entry that fired was replaced or
// rescheduled. The fired entry's time stands for now: due must not call the
// clock, because a test clock runs this under its own lock. Timers of the other
// keys it moves still fire later, and find no entry.
func (q *gatedQueue[T]) due(key T, w *waitingItem, gen uint64) {
	q.mu.Lock()
	if q.shutdown || q.waiting[key] != w || w.gen != gen {
		q.mu.Unlock()
		return
	}
	more := false
	for k, wi := range q.waiting {
		if !wi.at.After(w.at) {
			delete(q.waiting, k)
			if q.addReadyLocked(k, wi.priority) {
				more = true
			}
		}
	}
	q.mu.Unlock()
	if more {
		q.gov.dispatch()
	}
}

func (q *gatedQueue[T]) Add(item T) { q.AddWithOpts(priorityqueue.AddOpts{}, item) }

func (q *gatedQueue[T]) AddAfter(item T, after time.Duration) {
	q.AddWithOpts(priorityqueue.AddOpts{After: after}, item)
}

func (q *gatedQueue[T]) AddRateLimited(item T) {
	q.AddWithOpts(priorityqueue.AddOpts{RateLimited: true}, item)
}

// GetWithPriority waits for a local slot with an item reserved for it, then
// pops the highest-priority item the queue can hand out. It returns
// shutdown=true once the queue is shut down.
func (q *gatedQueue[T]) GetWithPriority() (_ T, priority int, shutdown bool) {
	var zero T
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return zero, 0, true
	}
	q.idle++
	q.mu.Unlock()
	q.gov.dispatch()

	q.mu.Lock()
	for q.granted == 0 && !q.shutdown {
		q.cond.Wait()
	}
	if !q.shutdown {
		q.granted--
		q.mu.Unlock()
		return q.pop()
	}
	// Shut down while waiting. Each waiting worker is either idle or granted.
	release := q.granted > 0
	if release {
		q.granted--
		q.reserved--
	} else {
		q.idle--
	}
	q.mu.Unlock()
	if release {
		q.gov.releaseLocal()
	}
	return zero, 0, true
}

// pop takes the reserved item from inner. It doesn't block: an item was
// available when the slot was granted, and only this queue's workers pop it.
func (q *gatedQueue[T]) pop() (T, int, bool) {
	key, priority, shutdown := q.inner.GetWithPriority()
	q.mu.Lock()
	q.reserved--
	if shutdown {
		q.mu.Unlock()
		q.gov.releaseLocal()
		var zero T
		return zero, 0, true
	}
	delete(q.ready, key)
	q.processing[key] = struct{}{}
	q.free--
	q.mu.Unlock()
	return key, priority, false
}

func (q *gatedQueue[T]) Get() (item T, shutdown bool) {
	item, _, shutdown = q.GetWithPriority()
	return item, shutdown
}

// Done marks key processed and gives back its local slot. A key re-added while
// it was processed becomes available again. Done for a key no worker holds does
// nothing.
func (q *gatedQueue[T]) Done(key T) {
	q.inner.Done(key)
	q.mu.Lock()
	if _, ok := q.processing[key]; !ok {
		q.mu.Unlock()
		return
	}
	delete(q.processing, key)
	if _, ok := q.ready[key]; ok {
		q.free++
	}
	q.mu.Unlock()
	q.gov.releaseLocal()
}

// tryGrant implements gatedQueueRef.
func (q *gatedQueue[T]) tryGrant() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown || q.idle == 0 || q.free-q.reserved <= 0 {
		return false
	}
	q.idle--
	q.granted++
	q.reserved++
	q.cond.Signal()
	return true
}

func (q *gatedQueue[T]) Forget(item T)          { q.rateLimiter.Forget(item) }
func (q *gatedQueue[T]) NumRequeues(item T) int { return q.rateLimiter.NumRequeues(item) }

// Len returns the number of items in inner, as the priority queue counts them:
// ready ones, including those being processed again, and not delayed ones.
func (q *gatedQueue[T]) Len() int { return q.inner.Len() }

func (q *gatedQueue[T]) ShuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shutdown
}

// ShutDown wakes every waiting worker, which returns shutdown=true, and gives
// back any slot granted to one of them. Running reconciles keep their slots
// until their Done.
func (q *gatedQueue[T]) ShutDown() {
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return
	}
	q.shutdown = true
	for key, w := range q.waiting {
		w.timer.Stop()
		delete(q.waiting, key)
	}
	q.cond.Broadcast()
	q.mu.Unlock()
	q.gov.removeQueue(q)
	q.inner.ShutDown()
}

// ShutDownWithDrain is ShutDown, as for the priority queue: controller-runtime
// doesn't use draining.
func (q *gatedQueue[T]) ShutDownWithDrain() { q.ShutDown() }
