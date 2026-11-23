package credentialuse

import (
	"context"
	"sync"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// In the tests, every holder informer lists and watches through a
// testListWatch: the tests' knobs live here, not in the core's listWatch
// (N5 of the review of #109).
func init() {
	holderListWatchSeam = func(lw *listWatch) cache.ListerWatcher {
		return &testListWatch{inner: lw, watches: map[*testWatch]bool{}}
	}
}

// testListWatch wraps the core's listWatch. It can refuse watch-list
// requests, so that the reflector falls back to a List; it counts the lists
// that completed each way; and it can end every open watch with a 410, as a
// compacted resourceVersion does, so that the reflector relists.
type testListWatch struct {
	inner *listWatch

	refuseWatchList atomic.Bool
	lists, streams  atomic.Int64

	mu      sync.Mutex
	watches map[*testWatch]bool
	// paused, when set, holds every watch event until it is closed: the
	// informer's cache lags behind the API server meanwhile.
	paused chan struct{}
}

// pause holds the watches' events until resume.
func (l *testListWatch) pause() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused == nil {
		l.paused = make(chan struct{})
	}
}

// resume lets the held events go, in order.
func (l *testListWatch) resume() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused != nil {
		close(l.paused)
		l.paused = nil
	}
}

func (l *testListWatch) gate() chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.paused
}

var _ cache.ListerWatcherWithContext = (*testListWatch)(nil)

func (l *testListWatch) IsWatchListSemanticsUnSupported() bool {
	return l.inner.IsWatchListSemanticsUnSupported()
}

func (l *testListWatch) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	out, err := l.inner.ListWithContext(ctx, options)
	if err == nil {
		if li, ok := out.(metav1.ListInterface); ok && li.GetContinue() == "" {
			l.lists.Add(1)
		}
	}
	return out, err
}

func (l *testListWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	initial := options.SendInitialEvents != nil && *options.SendInitialEvents
	if initial && l.refuseWatchList.Load() {
		return nil, apierrors.NewBadRequest("credential-use test: watch-list refused")
	}
	w, err := l.inner.WatchWithContext(ctx, options)
	if err != nil {
		return nil, err
	}
	tw := &testWatch{inner: w, out: make(chan watch.Event), done: make(chan struct{}), inject: make(chan watch.Event), initial: initial, lw: l}
	l.mu.Lock()
	l.watches[tw] = true
	l.mu.Unlock()
	go tw.forward()
	return tw, nil
}

func (l *testListWatch) List(options metav1.ListOptions) (runtime.Object, error) {
	return l.ListWithContext(context.Background(), options)
}

func (l *testListWatch) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return l.WatchWithContext(context.Background(), options)
}

// expire ends every open watch with a 410.
func (l *testListWatch) expire() {
	l.mu.Lock()
	ws := keysOf(l.watches)
	l.mu.Unlock()
	for _, w := range ws {
		w.expire()
	}
}

// testWatch passes the core's watch on, counting a watch-list stream at its
// initial-events-end bookmark, and carries a test's 410 into the stream.
type testWatch struct {
	inner   watch.Interface
	out     chan watch.Event
	done    chan struct{}
	once    sync.Once
	initial bool
	lw      *testListWatch
	// inject carries the 410, which forward passes on and then ends: only
	// forward sends on out, and only forward closes it.
	inject chan watch.Event
}

func (w *testWatch) ResultChan() <-chan watch.Event { return w.out }

func (w *testWatch) Stop() {
	w.once.Do(func() {
		close(w.done)
		w.inner.Stop()
		w.lw.mu.Lock()
		delete(w.lw.watches, w)
		w.lw.mu.Unlock()
	})
}

func (w *testWatch) forward() {
	defer close(w.out)
	in := w.inner.ResultChan()
	for {
		select {
		case <-w.done:
			return
		case ev := <-w.inject:
			select {
			case w.out <- ev:
			case <-w.done:
			}
			go w.Stop()
			return
		case ev, ok := <-in:
			if !ok {
				return
			}
			if w.initial && ev.Type == watch.Bookmark && isInitialEventsEnd(ev.Object) {
				w.lw.streams.Add(1)
			}
			if g := w.lw.gate(); g != nil {
				select {
				case <-g:
				case <-w.done:
					return
				}
			}
			select {
			case w.out <- ev:
			case <-w.done:
				return
			}
		}
	}
}

func (w *testWatch) expire() {
	ev := watch.Event{Type: watch.Error, Object: &metav1.Status{
		Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired,
		Message: "credential-use test: resourceVersion too old",
	}}
	select {
	case w.inject <- ev:
	case <-w.done:
	}
}
