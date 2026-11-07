package controller

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	sigspq "sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type fakeQueueWaitRecorder struct {
	lock          sync.Mutex
	waitDurations []time.Duration
	depthDeltas   []int64
	oldestAges    []time.Duration
	workDurations []time.Duration
}

func (r *fakeQueueWaitRecorder) RecordQueueWaitDuration(_ context.Context, d time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.waitDurations = append(r.waitDurations, d)
}

func (r *fakeQueueWaitRecorder) AddQueueDepth(_ context.Context, delta int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.depthDeltas = append(r.depthDeltas, delta)
}

func (r *fakeQueueWaitRecorder) RecordQueueOldestItemAge(_ context.Context, d time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.oldestAges = append(r.oldestAges, d)
}

func (r *fakeQueueWaitRecorder) RecordQueueWorkDuration(_ context.Context, d time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.workDurations = append(r.workDurations, d)
}

func TestQueueWaitInstrumentedRecordsQueueState(t *testing.T) {
	recorder := &fakeQueueWaitRecorder{}
	queue := &queueWaitInstrumented[string]{
		recorder:   recorder,
		enqueuedAt: map[string]time.Time{},
		startedAt:  map[string]time.Time{},
	}

	started := time.Unix(100, 0)
	inserted, oldestAge := queue.recordEnqueueAt("foo", started)
	if !inserted {
		t.Fatal("expected first enqueue to be recorded")
	}
	if oldestAge != 0 {
		t.Fatalf("expected oldest age after first enqueue = 0, got %v", oldestAge)
	}
	inserted, _ = queue.recordEnqueueAt("foo", started.Add(10*time.Second))
	if inserted {
		t.Fatal("expected duplicate enqueue to be ignored")
	}
	waitDuration, oldestAge, removed := queue.recordDequeueAt("foo", started.Add(25*time.Second))
	if !removed {
		t.Fatal("expected dequeue to remove pending item")
	}
	if waitDuration != 25*time.Second {
		t.Fatalf("recorded queue wait = %v, want %v", waitDuration, 25*time.Second)
	}
	if oldestAge != 0 {
		t.Fatalf("expected oldest age after dequeue = 0, got %v", oldestAge)
	}
	workDuration, ok := queue.recordDoneAt("foo", started.Add(40*time.Second))
	if !ok {
		t.Fatal("expected processing duration to be recorded")
	}
	if workDuration != 15*time.Second {
		t.Fatalf("recorded work duration = %v, want %v", workDuration, 15*time.Second)
	}

	if len(recorder.waitDurations) != 1 {
		t.Fatalf("expected 1 queue wait record, got %d", len(recorder.waitDurations))
	}
	if got, want := recorder.waitDurations[0], 25*time.Second; got != want {
		t.Fatalf("recorded queue wait = %v, want %v", got, want)
	}
	if got, want := recorder.depthDeltas, []int64{1, -1}; len(got) != len(want) {
		t.Fatalf("expected %d depth updates, got %d", len(want), len(got))
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("depth delta[%d] = %d, want %d", i, got[i], want[i])
			}
		}
	}
	if len(recorder.oldestAges) != 2 {
		t.Fatalf("expected 2 oldest-age samples, got %d", len(recorder.oldestAges))
	}
	if recorder.oldestAges[0] != 0 || recorder.oldestAges[1] != 0 {
		t.Fatalf("expected oldest ages to remain at zero, got %v", recorder.oldestAges)
	}
	if len(recorder.workDurations) != 1 {
		t.Fatalf("expected 1 work duration record, got %d", len(recorder.workDurations))
	}
	if got, want := recorder.workDurations[0], 15*time.Second; got != want {
		t.Fatalf("recorded work duration = %v, want %v", got, want)
	}
}

func TestQueueWaitInstrumentedIgnoresMissingEnqueue(t *testing.T) {
	recorder := &fakeQueueWaitRecorder{}
	queue := &queueWaitInstrumented[string]{
		recorder:   recorder,
		enqueuedAt: map[string]time.Time{},
		startedAt:  map[string]time.Time{},
	}

	_, _, removed := queue.recordDequeueAt("foo", time.Unix(100, 0))
	if removed {
		t.Fatal("expected missing enqueue to be ignored")
	}
	if workDuration, ok := queue.recordDoneAt("foo", time.Unix(125, 0)); ok || workDuration != 0 {
		t.Fatalf("expected missing processing entry to be ignored, got duration=%v ok=%v", workDuration, ok)
	}

	if len(recorder.waitDurations) != 0 {
		t.Fatalf("expected no queue wait records, got %d", len(recorder.waitDurations))
	}
	if len(recorder.depthDeltas) != 0 {
		t.Fatalf("expected no depth updates, got %d", len(recorder.depthDeltas))
	}
	if len(recorder.oldestAges) != 0 {
		t.Fatalf("expected no oldest-age samples, got %d", len(recorder.oldestAges))
	}
	if len(recorder.workDurations) != 0 {
		t.Fatalf("expected no work duration records, got %d", len(recorder.workDurations))
	}
}

func TestOptionsForControllerRuntimeQueueWaitRecorder(t *testing.T) {
	tests := map[string]struct {
		opts         Options
		wantNewQueue bool
	}{
		"recorder disabled": {
			opts:         Options{},
			wantNewQueue: false,
		},
		"recorder enabled": {
			opts: Options{
				QueueWaitRecorder: &fakeQueueWaitRecorder{},
			},
			wantNewQueue: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runtimeOpts := tc.opts.ForControllerRuntime()
			if tc.wantNewQueue && runtimeOpts.NewQueue == nil {
				t.Fatal("expected custom NewQueue to be configured")
			}
			if !tc.wantNewQueue && runtimeOpts.NewQueue != nil {
				t.Fatal("did not expect a custom NewQueue to be configured")
			}
		})
	}
}

func newTestRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "test", Name: name}}
}

func newRecordedQueue(t *testing.T, opts Options) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	runtimeOpts := opts.ForControllerRuntime()
	if runtimeOpts.NewQueue == nil {
		t.Fatal("expected custom NewQueue to be configured")
	}
	queue := runtimeOpts.NewQueue("test", runtimeOpts.RateLimiter)
	t.Cleanup(queue.ShutDown)
	return queue
}

// requirePriorityQueue asserts the queue controller-runtime gets is a
// priority queue: the type assertion its controller makes before wrapping a
// plain queue at priority 0.
func requirePriorityQueue(t *testing.T, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) sigspq.PriorityQueue[reconcile.Request] {
	t.Helper()
	pq, ok := queue.(sigspq.PriorityQueue[reconcile.Request])
	if !ok {
		t.Fatalf("queue %T is not a priorityqueue.PriorityQueue: controller-runtime would run every item at priority 0", queue)
	}
	return pq
}

func getWithPriorityWithin(t *testing.T, pq sigspq.PriorityQueue[reconcile.Request], timeout time.Duration) (reconcile.Request, int) {
	t.Helper()
	type result struct {
		item     reconcile.Request
		priority int
		shutdown bool
	}
	got := make(chan result, 1)
	go func() {
		item, priority, shutdown := pq.GetWithPriority()
		got <- result{item, priority, shutdown}
	}()
	select {
	case r := <-got:
		if r.shutdown {
			t.Fatal("GetWithPriority reported shutdown")
		}
		return r.item, r.priority
	case <-time.After(timeout):
		t.Fatalf("GetWithPriority returned nothing within %v", timeout)
		return reconcile.Request{}, 0
	}
}

func waitForLen(t *testing.T, queue workqueue.TypedRateLimitingInterface[reconcile.Request], want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for queue.Len() != want {
		if time.Now().After(deadline) {
			t.Fatalf("queue length = %d, want %d", queue.Len(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQueueWaitRecorderQueueKind(t *testing.T) {
	off := false
	tests := map[string]struct {
		opts         Options
		wantPriority bool
	}{
		"priority queue by default": {
			opts:         Options{QueueWaitRecorder: &fakeQueueWaitRecorder{}},
			wantPriority: true,
		},
		"plain workqueue when priority is off": {
			opts:         Options{QueueWaitRecorder: &fakeQueueWaitRecorder{}, UsePriorityQueue: &off},
			wantPriority: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			queue := newRecordedQueue(t, tc.opts)
			if _, ok := queue.(sigspq.PriorityQueue[reconcile.Request]); ok != tc.wantPriority {
				t.Fatalf("queue %T is a priorityqueue.PriorityQueue = %v, want %v", queue, ok, tc.wantPriority)
			}
		})
	}
}

func TestQueueWaitRecorderHandsOutHighPriorityFirst(t *testing.T) {
	queue := newRecordedQueue(t, Options{QueueWaitRecorder: &fakeQueueWaitRecorder{}})
	pq := requirePriorityQueue(t, queue)

	low, high := 0, 10
	pq.AddWithOpts(sigspq.AddOpts{Priority: &low}, newTestRequest("resync"))
	pq.AddWithOpts(sigspq.AddOpts{Priority: &high}, newTestRequest("spec-change"))
	waitForLen(t, queue, 2)

	item, priority := getWithPriorityWithin(t, pq, 5*time.Second)
	if item != newTestRequest("spec-change") || priority != high {
		t.Fatalf("first item = %v at priority %d, want spec-change at priority %d", item, priority, high)
	}
	item, priority = getWithPriorityWithin(t, pq, 5*time.Second)
	if item != newTestRequest("resync") || priority != low {
		t.Fatalf("second item = %v at priority %d, want resync at priority %d", item, priority, low)
	}
}

func TestQueueWaitRecorderRecordsPriorityMethods(t *testing.T) {
	recorder := &fakeQueueWaitRecorder{}
	queue := newRecordedQueue(t, Options{QueueWaitRecorder: recorder})
	pq := requirePriorityQueue(t, queue)

	const gap = 20 * time.Millisecond
	a, b := newTestRequest("a"), newTestRequest("b")
	low, high := 0, 5

	pq.AddWithOpts(sigspq.AddOpts{Priority: &low}, a)
	time.Sleep(gap)
	// a is a duplicate here and must not count again; b is new.
	pq.AddWithOpts(sigspq.AddOpts{Priority: &high}, a, b)
	waitForLen(t, queue, 2)

	item, _ := getWithPriorityWithin(t, pq, 5*time.Second)
	if item != a {
		t.Fatalf("first item = %v, want %v (raised to priority %d, added first)", item, a, high)
	}
	time.Sleep(gap)
	pq.Done(item)
	item, _ = getWithPriorityWithin(t, pq, 5*time.Second)
	if item != b {
		t.Fatalf("second item = %v, want %v", item, b)
	}
	pq.Done(item)

	recorder.lock.Lock()
	defer recorder.lock.Unlock()
	if got, want := recorder.depthDeltas, []int64{1, 1, -1, -1}; !slices.Equal(got, want) {
		t.Fatalf("depth deltas = %v, want %v", got, want)
	}
	if len(recorder.waitDurations) != 2 {
		t.Fatalf("expected 2 queue wait records, got %v", recorder.waitDurations)
	}
	if recorder.waitDurations[0] < gap {
		t.Fatalf("queue wait of a = %v, want at least %v", recorder.waitDurations[0], gap)
	}
	// Samples at: enqueue of a, enqueue of b (a pending), dequeue of a
	// (b pending), dequeue of b (nothing pending).
	if len(recorder.oldestAges) != 4 {
		t.Fatalf("expected 4 oldest-age samples, got %v", recorder.oldestAges)
	}
	if recorder.oldestAges[0] != 0 || recorder.oldestAges[1] < gap || recorder.oldestAges[2] <= 0 || recorder.oldestAges[3] != 0 {
		t.Fatalf("oldest ages = %v, want [0, >=%v, >0, 0]", recorder.oldestAges, gap)
	}
	if len(recorder.workDurations) != 2 {
		t.Fatalf("expected 2 work duration records, got %v", recorder.workDurations)
	}
	if recorder.workDurations[0] < gap {
		t.Fatalf("work duration of a = %v, want at least %v", recorder.workDurations[0], gap)
	}
}

// TestQueueWaitRecorderPriorityDelaysCountAsWait checks AddWithOpts records at
// call time even with After or RateLimited, as AddAfter and AddRateLimited do,
// so the delay counts as queue wait.
func TestQueueWaitRecorderPriorityDelaysCountAsWait(t *testing.T) {
	const delay = 30 * time.Millisecond
	tests := map[string]sigspq.AddOpts{
		"after":        {After: delay},
		"rate limited": {RateLimited: true},
	}

	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			recorder := &fakeQueueWaitRecorder{}
			rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](delay, time.Second)
			queue := newQueueWithWaitMetrics("test", rateLimiter, true, recorder)
			t.Cleanup(queue.ShutDown)
			pq := requirePriorityQueue(t, queue)

			pq.AddWithOpts(opts, newTestRequest("delayed"))
			item, _ := getWithPriorityWithin(t, pq, 5*time.Second)
			pq.Done(item)

			recorder.lock.Lock()
			defer recorder.lock.Unlock()
			if len(recorder.waitDurations) != 1 || recorder.waitDurations[0] < delay {
				t.Fatalf("queue waits = %v, want one of at least %v", recorder.waitDurations, delay)
			}
		})
	}
}

// TestQueueWaitRecorderPlainQueueUnchanged checks the plain workqueue path
// when priority is off: FIFO order, duplicates merged, the same records.
func TestQueueWaitRecorderPlainQueueUnchanged(t *testing.T) {
	off := false
	recorder := &fakeQueueWaitRecorder{}
	queue := newRecordedQueue(t, Options{QueueWaitRecorder: recorder, UsePriorityQueue: &off})
	if _, ok := queue.(*queueWaitInstrumented[reconcile.Request]); !ok {
		t.Fatalf("queue %T is not the plain instrumented wrapper with UsePriorityQueue=false", queue)
	}

	a, b := newTestRequest("a"), newTestRequest("b")
	queue.Add(a)
	queue.Add(b)
	queue.Add(a)
	for _, want := range []reconcile.Request{a, b} {
		item, shutdown := queue.Get()
		if shutdown || item != want {
			t.Fatalf("Get = %v (shutdown %v), want %v", item, shutdown, want)
		}
		queue.Done(item)
	}

	recorder.lock.Lock()
	defer recorder.lock.Unlock()
	if got, want := recorder.depthDeltas, []int64{1, 1, -1, -1}; !slices.Equal(got, want) {
		t.Fatalf("depth deltas = %v, want %v", got, want)
	}
	if len(recorder.waitDurations) != 2 || len(recorder.oldestAges) != 4 || len(recorder.workDurations) != 2 {
		t.Fatalf("records: waits %v, oldest ages %v, work %v; want 2, 4 and 2", recorder.waitDurations, recorder.oldestAges, recorder.workDurations)
	}
}
