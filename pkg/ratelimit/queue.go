package ratelimit

import (
	"fmt"
	"math"
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
// queues have work, the Governor grants a freed slot first to a queue that holds
// less than its guaranteed share of the local capacity, and otherwise round-robin
// (docs/design-capacity-model.md §6).
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
	// tryGrantOwed is tryGrant for a queue that holds fewer than share slots:
	// it grants nothing to a queue that already holds its guaranteed share.
	tryGrantOwed(share int) bool
	// counts returns what the queue reports for metrics.
	counts() *queueCounts
}

// queueCounts is what a gated queue reports to the Governor, for metrics. The
// queue writes it under its own lock and anything reads it without one, so a
// reader never waits on a pop.
type queueCounts struct {
	// name is the queue's name: the controller's.
	name string
	// held is the local slots the queue holds: granted and not yet popped, or
	// being processed. waiting is the items it could hand out that have no
	// slot yet.
	held, waiting atomic.Int64
	// arrivals counts items that became ready to run, and completions
	// reconciles that gave their slot back.
	arrivals, completions atomic.Int64
}

// popStuckAfter is how long a pop may wait inside the inner queue before it is
// reported. A pop waits there only when the record counts an item inner doesn't
// have, which is a defect: inner hands a counted item out at once. A variable so
// tests can lower it. Tests change it without a lock, which is safe only because
// no test in this package calls t.Parallel; a test that does must not touch it.
var popStuckAfter = 10 * time.Second

// skipRaceHook, when a test sets it, runs in tryGrant between seeing a pop in
// progress and recording the skip: the one interleaving the re-check of
// popping exists for, which no test could otherwise reach. Unset in production.
var skipRaceHook atomic.Pointer[func()]

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

	// stopping is set when ShutDown begins, before it shuts inner down and
	// before it takes mu: a pop waiting inside inner holds mu, and only inner's
	// ShutDown wakes it. Once it is set, inner hands nothing out, nothing calls
	// inner's Done, and a pop that inner ends with shutdown is no broken record.
	// ShuttingDown reads it. See design §4, "Shutdown".
	stopping atomic.Bool
	// readyLen is len(ready), stored with every change to ready, so Len never
	// waits for mu.
	readyLen atomic.Int64
	// stats is what the queue reports for metrics; see publishLocked.
	stats *queueCounts

	// mu guards everything below, and every call into inner is made holding
	// it, except ShutDown's (see stopping). Lock order: the Governor's lock,
	// then this one. A clock callback never takes it while the clock holds its
	// own lock (see scheduleLocked).
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
		stats:       &queueCounts{name: name},
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
	q.readyLen.Store(int64(len(q.ready)))
	if _, busy := q.processing[key]; busy {
		return false
	}
	q.arrivedLocked()
	return true
}

// arrivedLocked records that one more item can be handed out: it became ready
// to run. An item added again while it waits was counted when it first became
// ready; one added while it is processed is counted when its Done makes it
// ready.
func (q *gatedQueue[T]) arrivedLocked() {
	q.free++
	q.stats.arrivals.Add(1)
	q.gov.arrivals.Add(1)
	q.publishLocked()
}

// heldLocked is the local slots the queue holds: those granted to a worker
// that hasn't popped yet, and those of the items being processed.
func (q *gatedQueue[T]) heldLocked() int { return q.granted + len(q.processing) }

// publishLocked stores, for readers without the lock, the slots the queue
// holds and the items waiting for one. It is called after every change to
// granted, processing or free.
func (q *gatedQueue[T]) publishLocked() {
	q.stats.held.Store(int64(q.heldLocked()))
	q.stats.waiting.Store(int64(q.free - q.granted))
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
			q.publishLocked()
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
	q.publishLocked()
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
// processing. It reports false when inner ends the pop with shutdown: inner
// may then have handed the item out too, but the queue is going away and the
// record no longer has to match. Only ShutDown shuts inner down, and it sets
// stopping first, so a shutdown without stopping is a broken record.
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
		if !q.stopping.Load() {
			reportBrokenBooks(q.gov.log, "the inner queue was shut down while the gated queue was not")
		}
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
	q.readyLen.Store(int64(len(q.ready)))
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
// worker can pop key between the two. Once ShutDown has begun inner doesn't:
// it hands nothing out by then, so its record no longer matters. On
// controller-runtime v0.23.3 its Done could also wait forever on a lock its own
// hand-out was left holding; v0.25.0 fixed that (#3540), and the skip stays
// because it is still correct (design §4, "Shutdown"). The slot is given back
// either way.
func (q *gatedQueue[T]) Done(key T) {
	q.mu.Lock()
	if _, ok := q.processing[key]; !ok {
		q.mu.Unlock()
		return
	}
	if !q.stopping.Load() {
		q.inner.Done(key)
	}
	delete(q.processing, key)
	q.stats.completions.Add(1)
	q.gov.completions.Add(1)
	if _, ok := q.ready[key]; ok {
		q.arrivedLocked()
	}
	q.publishLocked()
	q.mu.Unlock()
	q.gov.releaseLocal()
}

// tryGrant implements gatedQueueRef: a grant whatever the queue already holds.
func (q *gatedQueue[T]) tryGrant() bool { return q.grant(math.MaxInt) }

// tryGrantOwed implements gatedQueueRef.
func (q *gatedQueue[T]) tryGrantOwed(share int) bool { return q.grant(share) }

// counts implements gatedQueueRef.
func (q *gatedQueue[T]) counts() *queueCounts { return q.stats }

// grant hands one local slot to an idle worker, for an item the queue can hand
// out, if the queue holds fewer than under slots. A queue with a grant whose
// pop hasn't finished is skipped without taking its lock, so the Governor never
// waits on it; the skip is recorded, and that pop dispatches again when it is
// done. One grant at a time per queue: pops are one at a time under the queue's
// lock anyway.
func (q *gatedQueue[T]) grant(under int) bool {
	if q.popping.Load() {
		if hook := skipRaceHook.Load(); hook != nil {
			(*hook)()
		}
		q.skipped.Store(true)
		if q.popping.Load() {
			return false
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown || q.idle == 0 || q.free-q.granted <= 0 || q.heldLocked() >= under {
		return false
	}
	q.idle--
	q.granted++
	q.publishLocked()
	q.popping.Store(true)
	q.cond.Signal()
	return true
}

func (q *gatedQueue[T]) Forget(item T)          { q.rateLimiter.Forget(item) }
func (q *gatedQueue[T]) NumRequeues(item T) int { return q.rateLimiter.NumRequeues(item) }

// Len returns the number of items the priority queue would count: ready ones,
// including those being processed again, and not delayed ones. It reads them
// from the record, which holds exactly the keys in inner, and takes no lock, so
// it never waits on a pop. It doesn't ask inner, which would mean calling it
// without mu or waiting for mu behind a pop (design §4, "Shutdown"). Nothing
// in provider-runtime or controller-runtime calls it.
func (q *gatedQueue[T]) Len() int {
	return int(q.readyLen.Load())
}

// ShuttingDown reports whether ShutDown has begun. It takes no lock.
func (q *gatedQueue[T]) ShuttingDown() bool {
	return q.stopping.Load()
}

// ShutDown wakes every waiting worker, which returns shutdown=true, and gives
// back any slot granted to one of them. Running reconciles keep their slots
// until their Done.
//
// It shuts inner down first, without mu: a pop waiting inside inner holds mu,
// and only inner's ShutDown ends that pop. Normally that wait is short, but
// when the record is broken it never ends, and graceful shutdown would hang on
// mu. stopping, set first, makes inner's ShutDown happen once (a second one
// panics) and tells the pop it ends that it isn't a broken record.
func (q *gatedQueue[T]) ShutDown() {
	if q.stopping.CompareAndSwap(false, true) {
		q.inner.ShutDown()
	}
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
}

// ShutDownWithDrain is ShutDown, as for the priority queue: controller-runtime
// doesn't use draining.
func (q *gatedQueue[T]) ShutDownWithDrain() { q.ShutDown() }
