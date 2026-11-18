package credentialuse

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

// listWatch is the ListerWatcher the core builds its informers on (§6.5 L2,
// revision 16; plan A20). An event handler can't see a relist: client-go
// turns its Replaced deltas into ordinary OnAdd, OnUpdate and OnDelete calls.
// So the ListerWatcher records when each list completes, and the holder
// informer's verified set takes that as a floor under every observation
// time: t_obs = max(observed, recordSince, lastList).
//
// The obligation the exact-version skip rule rests on (the re-review of #59):
// lastList is recorded no later than the relisted versions reach the
// handler. Both ways a reflector lists keep it:
//   - a List: the last page's return is recorded before the reflector gets
//     it, and the reflector replaces its store only after that;
//   - a watch-list stream: the k8s.io/initial-events-end bookmark is
//     recorded before it is passed on, and the reflector replaces its store
//     from the streamed objects only once it has read that bookmark
//     (client-go tools/cache/reflector.go, watchList).
type listWatch struct {
	client   metadata.ResourceInterface
	selector string // a label selector, or empty
	clock    clock.PassiveClock
	// listed, if set, receives each list's completion time.
	listed func(time.Time)

	// For tests: refuseWatchList makes a watch-list request fail, so that
	// the reflector falls back to a List; lists and streams count the lists
	// that completed each way.
	refuseWatchList atomic.Bool
	lists, streams  atomic.Int64

	mu      sync.Mutex
	watches map[*recordingWatch]bool
}

var _ cache.ListerWatcherWithContext = (*listWatch)(nil)

func newListWatch(c metadata.ResourceInterface, selector string, clk clock.PassiveClock, listed func(time.Time)) *listWatch {
	return &listWatch{client: c, selector: selector, clock: clk, listed: listed, watches: map[*recordingWatch]bool{}}
}

func (l *listWatch) completed() {
	if l.listed != nil {
		l.listed(l.clock.Now())
	}
}

func (l *listWatch) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	if l.selector != "" {
		options.LabelSelector = l.selector
	}
	out, err := l.client.List(ctx, options)
	if err != nil {
		return nil, err
	}
	if out.Continue == "" {
		l.lists.Add(1)
		l.completed()
	}
	return out, nil
}

func (l *listWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	if l.selector != "" {
		options.LabelSelector = l.selector
	}
	initial := options.SendInitialEvents != nil && *options.SendInitialEvents
	if initial && l.refuseWatchList.Load() {
		return nil, apierrors.NewBadRequest("credential-use test: watch-list refused")
	}
	w, err := l.client.Watch(ctx, options)
	if err != nil {
		return nil, err
	}
	rw := &recordingWatch{inner: w, out: make(chan watch.Event), done: make(chan struct{}), inject: make(chan watch.Event), initial: initial, lw: l}
	l.mu.Lock()
	l.watches[rw] = true
	l.mu.Unlock()
	go rw.forward()
	return rw, nil
}

// List and Watch serve informers built with the context-free interface.
func (l *listWatch) List(options metav1.ListOptions) (runtime.Object, error) {
	return l.ListWithContext(context.Background(), options)
}

func (l *listWatch) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return l.WatchWithContext(context.Background(), options)
}

// expire ends every open watch with a 410, as a compacted resourceVersion
// does, so that the reflector relists: for tests (plan, PR 3).
func (l *listWatch) expire() {
	l.mu.Lock()
	ws := keysOf(l.watches)
	l.mu.Unlock()
	for _, w := range ws {
		w.expire()
	}
}

// recordingWatch passes a watch's events on, recording the end of a
// watch-list stream's initial events before it passes that bookmark on.
type recordingWatch struct {
	inner   watch.Interface
	out     chan watch.Event
	done    chan struct{}
	once    sync.Once
	initial bool
	lw      *listWatch
	// inject carries a test's 410 into the stream, which forward passes on
	// and then ends: only forward sends on out, and only forward closes it.
	inject chan watch.Event
}

func (w *recordingWatch) ResultChan() <-chan watch.Event { return w.out }

func (w *recordingWatch) Stop() {
	w.once.Do(func() {
		close(w.done)
		w.inner.Stop()
		w.lw.mu.Lock()
		delete(w.lw.watches, w)
		w.lw.mu.Unlock()
	})
}

func (w *recordingWatch) forward() {
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
				w.lw.completed()
			}
			select {
			case w.out <- ev:
			case <-w.done:
				return
			}
		}
	}
}

// expire sends a 410 Gone error event, then ends the stream.
func (w *recordingWatch) expire() {
	ev := watch.Event{Type: watch.Error, Object: &metav1.Status{
		Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired,
		Message: "credential-use test: resourceVersion too old",
	}}
	select {
	case w.inject <- ev:
	case <-w.done:
	}
}

func isInitialEventsEnd(obj runtime.Object) bool {
	m, err := meta.Accessor(obj)
	return err == nil && m.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true"
}
