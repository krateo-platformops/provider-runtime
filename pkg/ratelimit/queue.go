package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/util/workqueue"
	utilclock "k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
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
// de-duplication, and owns the delays and the rate limiting itself. Every call
// into the inner queue is made under the queue's lock, in the same critical
// section that updates its own record of what the inner queue holds, so that
// record is exact: the inner queue changes only through those calls. One
// difference from the priority queue: a delayed item is ordered among items of
// its priority by when it became ready, not by when it was added.
//
// The inner queue's own log, at V(5), is discarded, as newControllerQueue in
// pkg/controller already does for a queue it builds.
//
// controller.Options.ForControllerRuntime builds one for each controller when
// saturation is on. rateLimiter is used for AddRateLimited; nil means
// controller-runtime's default for its priority queue.
func NewGatedQueue[T comparable](g *Governor, name string, rateLimiter workqueue.TypedRateLimiter[T]) priorityqueue.PriorityQueue[T] {
	return newGatedQueue(g, name, rateLimiter, utilclock.RealClock{})
}

// gatedQueueRef is how the Governor grants a queue a slot.
type gatedQueueRef interface {
	// tryGrant hands one local slot to one of the queue's idle workers, for an
	// item the queue can hand out, and reports whether it did. The Governor
	// calls it holding g.mu; it takes the queue's lock.
	tryGrant() bool
}

// popStuckAfter is how long a pop may wait inside the inner queue before it is
// reported. A pop waits there only when the record counts an item inner doesn't
// have, which is a defect: inner hands a counted item out at once. A variable so
// tests can lower it.
var popStuckAfter = 10 * time.Second

// brokenBooks counts the times the gated queues' or the Governor's own records
// were found to disagree with what happened, which must never be. Each one is
// logged as an error; the package's tests fail when it is not zero.
var brokenBooks atomic.Int64

// reportBrokenBooks counts and logs one disagreement in the records.
func reportBrokenBooks(log logging.Logger, msg string, keysAndValues ...any) {
	brokenBooks.Add(1)
	log.Error(errors.New(msg), "The local capacity's records disagree with what happened. This is a provider-runtime defect: please report it", keysAndValues...)
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
	name        string
	inner       priorityqueue.PriorityQueue[T]
	rateLimiter workqueue.TypedRateLimiter[T]
	clock       utilclock.WithDelayedExecution

	// mu guards everything below, and every call into inner is made holding
	// it. Lock order: the Governor's lock, then this one. A clock callback
	// never takes it while the clock holds its own lock (see scheduleLocked).
	mu   sync.Mutex
	cond *sync.Cond
	// ready holds the keys in inner, and processing the keys a worker holds:
	// inner skips a key that is processing, so it can hand out ready minus
	// processing. free counts those.
	ready      map[T]struct{}
	processing map[T]struct{}
	free       int
	// waiting holds delayed keys until they are due.
	waiting map[T]*waitingItem
	// idle counts workers waiting for a slot. granted counts slots handed to
	// this queue whose worker hasn't popped yet; each is for one of the free
	// items, so free-granted is what a new grant may take.
	idle, granted int
	shutdown      bool

	// popping is set from a grant until its pop has finished: the queue's lock
	// may then be held for as long as inner takes to hand the item out. While
	// it is set, tryGrant skips the queue instead of waiting for its lock, so a
	// Governor dispatch never waits on another queue's pop, and records the skip
	// in skipped; the pop dispatches again when it is done. They are read
	// without the lock. See design §4, "Containment".
	popping, skipped atomic.Bool
}

func newGatedQueue[T comparable](g *Governor, name string, rateLimiter workqueue.TypedRateLimiter[T], clk utilclock.WithDelayedExecution) *gatedQueue[T] {
	if rateLimiter == nil {
		// The priority queue's own default.
		rateLimiter = workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second)
	}
	q := &gatedQueue[T]{
		gov:         g,
		name:        name,
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

// scheduleLocked arms w's timer. The callback hands off to a goroutine, so it
// never waits for the queue's lock while a clock holds its own: a test clock
// runs callbacks under its lock, and the queue calls the clock under its own.
func (q *gatedQueue[T]) scheduleLocked(key T, w *waitingItem, after time.Duration) {
	w.gen++
	gen := w.gen
	w.timer = q.clock.AfterFunc(after, func() { go q.due(key, w, gen) })
}

// due makes every delayed key that is due ready at once, as the priority queue
// does, so items due together are handed out by priority, not in the order their
// timers ran. It stops the timers of the other keys it moves. It does nothing
// when the entry that fired was replaced or rescheduled.
func (q *gatedQueue[T]) due(key T, w *waitingItem, gen uint64) {
	q.mu.Lock()
	if q.shutdown || q.waiting[key] != w || w.gen != gen {
		q.mu.Unlock()
		return
	}
	now := q.clock.Now()
	more := false
	for k, wi := range q.waiting {
		if wi.at.After(now) && wi != w {
			continue
		}
		if wi != w {
			wi.timer.Stop()
		}
		delete(q.waiting, k)
		if q.addReadyLocked(k, wi.priority) {
			more = true
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

// GetWithPriority waits for a local slot granted for an item this queue can
// hand out, then pops the highest-priority one. It returns shutdown=true once
// the queue is shut down.
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
	if q.shutdown {
		// Each waiting worker is either idle or granted. A grant that is
		// handed back ends its pop before it starts.
		release := q.granted > 0
		if release {
			q.granted--
			q.popping.Store(false)
		} else {
			q.idle--
		}
		q.mu.Unlock()
		if release {
			q.gov.releaseLocal()
		}
		return zero, 0, true
	}
	q.granted--
	key, priority, ok := q.popLocked()
	// In this order: clear popping, unlock, then dispatch if a grant was
	// skipped meanwhile (design §4, "Containment").
	q.popping.Store(false)
	q.mu.Unlock()
	if q.skipped.Swap(false) {
		q.gov.dispatch()
	}
	if !ok {
		q.gov.releaseLocal()
		return zero, 0, true
	}
	return key, priority, false
}

// popLocked pops the item a grant was for. It holds q.mu across inner's
// GetWithPriority, so no add, pop or Done can change inner between the record
// saying an item is free and inner handing it out. That record is exact, so
// inner has an item to hand out at once: one that is ready and not
// processing. inner is shut down only by ShutDown, under q.mu, which the
// caller checked, so a shutdown here is a broken record too; it reports false.
//
// A watchdog on the real clock reports a pop that is still waiting after
// popStuckAfter. Its callback takes no lock: the pop holds this queue's lock.
func (q *gatedQueue[T]) popLocked() (T, int, bool) {
	watchdog := time.AfterFunc(popStuckAfter, func() {
		reportBrokenBooks(q.gov.log, "a pop has waited in the inner queue longer than it can: the record counts an item the inner queue doesn't hold, and this controller's queue is stuck",
			"queue", q.name, "after", popStuckAfter.String())
	})
	key, priority, shutdown := q.inner.GetWithPriority()
	watchdog.Stop()
	if shutdown {
		reportBrokenBooks(q.gov.log, "the inner queue was shut down while the gated queue was not")
		var zero T
		return zero, 0, false
	}
	_, isReady := q.ready[key]
	_, isProcessing := q.processing[key]
	if !isReady || isProcessing {
		reportBrokenBooks(q.gov.log, "the inner queue handed out a key the record didn't hold as free",
			"key", fmt.Sprint(key), "ready", isReady, "processing", isProcessing)
	} else {
		q.free--
	}
	delete(q.ready, key)
	q.processing[key] = struct{}{}
	return key, priority, true
}

func (q *gatedQueue[T]) Get() (item T, shutdown bool) {
	item, _, shutdown = q.GetWithPriority()
	return item, shutdown
}

// Done marks key processed and gives back its local slot. A key re-added while
// it was processed becomes available again. Done for a key no worker holds does
// nothing. inner learns of it in the same critical section as the record, so no
// worker can pop key between the two.
func (q *gatedQueue[T]) Done(key T) {
	q.mu.Lock()
	if _, ok := q.processing[key]; !ok {
		q.mu.Unlock()
		return
	}
	q.inner.Done(key)
	delete(q.processing, key)
	if _, ok := q.ready[key]; ok {
		q.free++
	}
	q.mu.Unlock()
	q.gov.releaseLocal()
}

// tryGrant implements gatedQueueRef. A queue with a grant whose pop hasn't
// finished is skipped without taking its lock, so the Governor never waits on
// it; the skip is recorded, and that pop dispatches again when it is done. One
// grant at a time per queue: pops are one at a time under the queue's lock
// anyway.
func (q *gatedQueue[T]) tryGrant() bool {
	if q.popping.Load() {
		q.skipped.Store(true)
		if q.popping.Load() {
			return false
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown || q.idle == 0 || q.free-q.granted <= 0 {
		return false
	}
	q.idle--
	q.granted++
	q.popping.Store(true)
	q.cond.Signal()
	return true
}

func (q *gatedQueue[T]) Forget(item T)          { q.rateLimiter.Forget(item) }
func (q *gatedQueue[T]) NumRequeues(item T) int { return q.rateLimiter.NumRequeues(item) }

// Len returns the number of items in inner, as the priority queue counts them:
// ready ones, including those being processed again, and not delayed ones.
func (q *gatedQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inner.Len()
}

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
	q.inner.ShutDown()
	q.cond.Broadcast()
	q.mu.Unlock()
	q.gov.removeQueue(q)
}

// ShutDownWithDrain is ShutDown, as for the priority queue: controller-runtime
// doesn't use draining.
func (q *gatedQueue[T]) ShutDownWithDrain() { q.ShutDown() }
