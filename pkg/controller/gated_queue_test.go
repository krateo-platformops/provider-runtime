package controller

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sigspq "sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

func saturationOptions() Options {
	o := DefaultOptions()
	o.MaxConcurrentReconciles = 4
	o.RateLimit.Saturation = ratelimit.SaturationOptions{Enabled: true, CgroupDir: filepath.Join("..", "ratelimit", "testdata", "healthy")}
	return o
}

// The gated queue is a priority queue, so saturation with UsePriorityQueue
// turned off is a conflict, and setup says so instead of choosing one.
func TestSaturationWithUsePriorityQueueOffIsAnError(t *testing.T) {
	off := false
	o := saturationOptions()
	o.UsePriorityQueue = &off
	err := o.SetupRateLimitGovernor(&runnables{})
	if err == nil || !strings.Contains(err.Error(), "UsePriorityQueue=false conflicts with RateLimit.Saturation") {
		t.Fatalf("err = %v, want one naming the conflict", err)
	}
	if o.RateLimitGovernor != nil {
		t.Error("a governor was built despite the error")
	}

	on := true
	o = saturationOptions()
	o.UsePriorityQueue = &on
	if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
		t.Errorf("UsePriorityQueue=true with saturation: %v", err)
	}
	o = DefaultOptions()
	o.UsePriorityQueue = &off
	o.RateLimit.Enabled = true
	if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
		t.Errorf("UsePriorityQueue=false with the rate-limit input alone: %v", err)
	}
}

type handedOut struct {
	item     reconcile.Request
	priority int
}

// worker pops from pq until it shuts down, the way one controller-runtime
// worker does, without calling Done: the test does.
func worker(pq sigspq.PriorityQueue[reconcile.Request]) <-chan handedOut {
	ch := make(chan handedOut, 16)
	go func() {
		for {
			item, priority, shutdown := pq.GetWithPriority()
			if shutdown {
				return
			}
			ch <- handedOut{item, priority}
		}
	}()
	return ch
}

// gotWithin returns what the worker took within d, or reports none.
func gotWithin(ch <-chan handedOut, d time.Duration) (reconcile.Request, int, bool) {
	select {
	case r := <-ch:
		return r.item, r.priority, true
	case <-time.After(d):
		return reconcile.Request{}, 0, false
	}
}

// With saturation on, ForControllerRuntime gates the queue on the governor's
// local capacity, with or without a QueueWaitRecorder, and keeps priorities. A
// fresh governor's local capacity is 1: one item out, the next only after Done.
func TestForControllerRuntimeGatesTheQueueWithSaturation(t *testing.T) {
	for name, recorder := range map[string]QueueWaitRecorder{"no recorder": nil, "with a QueueWaitRecorder": &fakeQueueWaitRecorder{}} {
		t.Run(name, func(t *testing.T) {
			o := saturationOptions()
			o.QueueWaitRecorder = recorder
			if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
				t.Fatal(err)
			}
			queue := newRecordedQueue(t, o)
			pq := requirePriorityQueue(t, queue)

			got := worker(pq)
			low, high := -100, 100
			pq.AddWithOpts(sigspq.AddOpts{Priority: &low}, newTestRequest("resync"))
			pq.AddWithOpts(sigspq.AddOpts{Priority: &high}, newTestRequest("spec-change"))
			first, priority, ok := gotWithin(got, 5*time.Second)
			if !ok || first != newTestRequest("spec-change") || priority != high {
				t.Fatalf("first item = %v at %d (got one: %v), want spec-change at %d", first, priority, ok, high)
			}
			if second, _, ok := gotWithin(got, 100*time.Millisecond); ok {
				t.Fatalf("%v was handed out with the only local slot taken", second)
			}
			pq.Done(first)
			if second, _, ok := gotWithin(got, 5*time.Second); !ok || second != newTestRequest("resync") {
				t.Errorf("after Done, got %v (one: %v), want resync", second, ok)
			}
			if r, ok := recorder.(*fakeQueueWaitRecorder); ok {
				r.lock.Lock()
				deltas := slices.Clone(r.depthDeltas)
				r.lock.Unlock()
				if len(deltas) == 0 {
					t.Error("the QueueWaitRecorder saw nothing: the gated queue must still be wrapped")
				}
			}
		})
	}
}

// errorLog counts Error lines.
type errorLog struct {
	mu     sync.Mutex
	errors int
}

func (l *errorLog) Info(string, ...any)  {}
func (l *errorLog) Debug(string, ...any) {}
func (l *errorLog) Warn(string, ...any)  {}
func (l *errorLog) Error(error, string, ...any) {
	l.mu.Lock()
	l.errors++
	l.mu.Unlock()
}
func (l *errorLog) WithName(string) logging.Logger   { return l }
func (l *errorLog) WithValues(...any) logging.Logger { return l }

// ForControllerRuntime registers the controller it gates, so the governor
// doesn't take its reconciler for one built without it. A reconciler with no
// gated controller is reported.
func TestForControllerRuntimeRegistersTheGatedController(t *testing.T) {
	for name, gate := range map[string]bool{"gated": true, "built without ForControllerRuntime": false} {
		t.Run(name, func(t *testing.T) {
			logs := &errorLog{}
			o := saturationOptions()
			o.Logger = logs
			if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
				t.Fatal(err)
			}
			g := o.RateLimitGovernor
			g.RegisterReconciler() // what reconciler.WithRateLimitGovernor does
			if gate {
				_ = o.ForControllerRuntime()
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { _ = g.Start(ctx); close(done) }()
			for deadline := time.Now().Add(5 * time.Second); !g.Snapshot().Inputs[0].Enabled; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the sampler never ran")
				}
			}
			cancel()
			<-done
			want := 1
			if gate {
				want = 0
			}
			logs.mu.Lock()
			defer logs.mu.Unlock()
			if logs.errors != want {
				t.Errorf("logged %d errors, want %d", logs.errors, want)
			}
		})
	}
}

// Without saturation the queue is controller-runtime's own: nothing is gated,
// and with no recorder ForControllerRuntime doesn't set NewQueue at all.
func TestForControllerRuntimeLeavesTheQueueAloneWithoutSaturation(t *testing.T) {
	o := DefaultOptions()
	o.RateLimit.Enabled = true
	if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
		t.Fatal(err)
	}
	if o.ForControllerRuntime().NewQueue != nil {
		t.Error("NewQueue is set with saturation off and no recorder")
	}
}
