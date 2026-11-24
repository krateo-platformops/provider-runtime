package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// An informer whose lists or watches fail says so, once (§12). The tests that
// wait out the reflectors' backoffs, which run on the real clock, are
// parallel.

// sinkLines records what a tracker's sink logs.
type sinkLines struct {
	mu    sync.Mutex
	warns []string
	infos []string
}

func (l *sinkLines) Debug(string, ...any)        {}
func (l *sinkLines) Error(error, string, ...any) {}
func (l *sinkLines) Info(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprintf("%s %q", msg, kv))
}
func (l *sinkLines) Warn(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf("%s %q", msg, kv))
}
func (l *sinkLines) WithValues(...any) logging.Logger { return l }
func (l *sinkLines) WithName(string) logging.Logger   { return l }

// countLines is how many of *lines, under mu, have every one of subs.
func countLines(mu *sync.Mutex, lines *[]string, subs ...string) int {
	mu.Lock()
	defer mu.Unlock()
	n := 0
	for _, s := range *lines {
		ok := true
		for _, sub := range subs {
			ok = ok && strings.Contains(s, sub)
		}
		if ok {
			n++
		}
	}
	return n
}

// warned is how many warnings have every one of subs; informed, how many
// Info lines.
func (l *sinkLines) warned(subs ...string) int   { return countLines(&l.mu, &l.warns, subs...) }
func (l *sinkLines) informed(subs ...string) int { return countLines(&l.mu, &l.infos, subs...) }

// klogLines records what client-go logs through the context it is given.
type klogLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *klogLines) logger() logr.Logger {
	return funcr.New(func(_, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, args)
	}, funcr.Options{})
}

func (l *klogLines) logged(subs ...string) int { return countLines(&l.mu, &l.lines, subs...) }

// An outage makes one resource's lists and watches fail in the fake API
// server.
type outage struct {
	mu      sync.Mutex
	err     error
	listOK  bool // only the watches fail
	watches []*watch.RaceFreeFakeWatcher
	tries   int // failed lists and watches
}

// breakable lets the test fail resource.
func (r *fakeRig) breakable(resource string) *outage { return breakableIn(&r.md.Fake, resource) }

// breakableIn is breakable, of the API server that f is a client of.
func breakableIn(f *clienttesting.Fake, resource string) *outage {
	o := &outage{}
	f.PrependReactor("list", resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.err == nil || o.listOK {
			return false, nil, nil
		}
		o.tries++
		return true, nil, o.err
	})
	f.PrependWatchReactor(resource, func(clienttesting.Action) (bool, watch.Interface, error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.err != nil {
			o.tries++
			return true, nil, o.err
		}
		w := watch.NewRaceFreeFake()
		o.watches = append(o.watches, w)
		return true, w, nil
	})
	return o
}

// fail ends the watches, and makes every list and watch fail with err.
func (o *outage) fail(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err = err
	for _, w := range o.watches {
		w.Stop()
	}
	o.watches = nil
}

// failWatches is fail, with the lists working.
func (o *outage) failWatches(err error) {
	o.mu.Lock()
	o.listOK = true
	o.mu.Unlock()
	o.fail(err)
}

// heal makes them work.
func (o *outage) heal() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err = nil
}

func (o *outage) failures() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.tries
}

// watching reports whether n watches are open.
func (o *outage) watching(n int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.watches) >= n
}

// slowly is eventually with two minutes, for what waits out the reflector's
// backoffs beside the other parallel tests.
func slowly(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// runLogged is run, with client-go's log going to l.
func (r *fakeRig) runLogged(t *testing.T, l *klogLines) {
	t.Helper()
	ctx, cancel := context.WithCancel(klog.NewContext(context.Background(), l.logger()))
	done := make(chan error, 1)
	go func() { done <- r.s.RunInformers(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	slowly(t, "RunInformers to sync", r.s.HasSynced)
}

func (r *fakeRig) trackLogged(t *testing.T, sink logging.Logger, protected ...ProtectedKind) *Tracker {
	t.Helper()
	opts := r.options(widgetGK, protected...)
	opts.Logger = sink
	tr, err := r.s.Track(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

var (
	errTest404 = apierrors.NewGenericServerResponse(404, "list", schema.GroupResource{Resource: "x"}, "", "404 page not found", 0, true)
	errTest403 = apierrors.NewForbidden(schema.GroupResource{Resource: "x"}, "", errors.New("test: no"))
)

// The holder and CRD informers' failures after the first sync are logged on
// the tracker's sink: once while they last, however often the reflector
// retries, and again after a recovery. The ones that belong to no tracker log
// where client-go does.
func TestAnInformersFailureIsLoggedOnce(t *testing.T) {
	for resource, what := range map[string]string{
		"widgets":                   "the holder informer of " + widgetGK.String(),
		"customresourcedefinitions": "the CRD informer of " + widgetGK.String(),
		"secrets":                   "", // shared
		"namespaces":                "",
		"configmaps":                "", // shared, and started by a Track after Run
	} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			r := newFakeRig(t)
			o, sink, kl := r.breakable(resource), &sinkLines{}, &klogLines{}
			if resource == "configmaps" {
				r.runLogged(t, kl)
				r.trackLogged(t, sink, ProtectedKind{GVK: configMapGVK})
			} else {
				r.trackLogged(t, sink)
				r.runLogged(t, kl)
			}
			logged := func() (here, there int) {
				here, there = sink.warned(what+" can't list or watch", `"its kind isn't served"`), kl.logged("Failed to watch", "could not find")
				if what == "" {
					here, there = there, sink.warned()
				}
				return here, there
			}
			for round := 1; round <= 2; round++ {
				slowly(t, "a watch", func() bool { return o.watching(1) })
				tries := o.failures()
				o.fail(errTest404)
				// A second failure comes after the first was handled.
				slowly(t, "two failed tries", func() bool { return o.failures() >= tries+2 })
				if here, there := logged(); here != round || there != 0 {
					t.Errorf("round %d: logged %d times, and %d where it shouldn't be; want %d and 0\n%q\n%q", round, here, there, round, sink.warns, kl.lines)
				}
				o.heal()
			}
		})
	}
}

// So is the index informer's, on the tracker's sink.
func TestTheIndexInformersFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()
	var o *outage
	r, sink, _ := perNameFakeRig(t, nil, func(r *fakeRig) { o = breakableIn(&r.dyn.Fake, "configmaps") })
	what := r.s.trackers[widgetGK].indexInformer().what
	for round := 1; round <= 2; round++ {
		slowly(t, "a watch", func() bool { return o.watching(1) })
		tries := o.failures()
		o.fail(errTest404)
		slowly(t, "two failed tries", func() bool { return o.failures() >= tries+2 })
		if n := sink.warned(what+" can't list or watch", `"its kind isn't served"`); n != round {
			t.Errorf("round %d: logged %d times, want %d: %q", round, n, round, sink.warns)
		}
		o.heal()
	}
}

// A watch that is refused while its lists work isn't recovered by a list:
// the reflector lists at every retry, and would log each time.
func TestAListAloneIsNoRecovery(t *testing.T) {
	t.Parallel()
	r := newFakeRig(t)
	o, sink := r.breakable("widgets"), &sinkLines{}
	r.trackLogged(t, sink)
	r.runLogged(t, &klogLines{})
	slowly(t, "the first watch", func() bool { return o.watching(1) })
	o.failWatches(errTest403)
	slowly(t, "three failed watches", func() bool { return o.failures() >= 3 })
	if n := sink.warned("forbidden"); n != 1 {
		t.Errorf("warned %d times, want once: %q", n, sink.warns)
	}
}

// A watch that streams its list is a recovery once the stream's initial
// events end; one after a List, as soon as it opens.
func TestAStreamsInitialListIsARecovery(t *testing.T) {
	for _, stream := range []bool{true, false} {
		w := watch.NewRaceFreeFake()
		recovered := make(chan struct{}, 10)
		lw := &listWatch{client: fakeLW{w}, recovered: func() { recovered <- struct{}{} }}
		opts := metav1.ListOptions{}
		if stream {
			opts.SendInitialEvents = &stream
		}
		rw, err := lw.Watch(opts)
		if err != nil {
			t.Fatal(err)
		}
		if n := len(recovered); (n == 1) == stream {
			t.Errorf("stream %v: %d recoveries as it opened", stream, n)
		}
		w.Add(&metav1.PartialObjectMetadata{})
		<-rw.ResultChan()
		if n := len(recovered); stream && n != 0 {
			t.Errorf("%d recoveries before the initial events ended", n)
		}
		end := &metav1.PartialObjectMetadata{}
		end.Annotations = map[string]string{metav1.InitialEventsAnnotationKey: "true"}
		w.Action(watch.Bookmark, end)
		<-rw.ResultChan()
		if n := len(recovered); n != 1 {
			t.Errorf("stream %v: %d recoveries in all, want one", stream, n)
		}
		rw.Stop()
	}
}

// fakeLW lists nothing, and watches w.
type fakeLW struct{ w watch.Interface }

func (fakeLW) list(context.Context, metav1.ListOptions) (runtime.Object, string, error) {
	return &metav1.PartialObjectMetadataList{}, "", nil
}
func (f fakeLW) watch(context.Context, metav1.ListOptions) (watch.Interface, error) { return f.w, nil }

// perNameWatches is a tracker's per-name watches of ids, on the fake API
// server, with sink its tracker's.
func perNameWatches(t *testing.T, r *fakeRig, sink logging.Logger, ids ...ID) *perNames {
	t.Helper()
	n := newPerNames(r.trackLogged(t, sink))
	n.infos[secretGroupKind] = perNameKind{gvr: secretsGVR, namespaced: true}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	n.start(ctx)
	n.follow(ids)
	return n
}

// A per-name watch that the API server forbids is warned of once per name,
// and again once the watch had been back. What every per-name watch of a
// kind meets, as a kind that isn't served, is warned of once for the kind.
func TestAPerNameWatchsFailureIsLoggedOnce(t *testing.T) {
	a, b := secretID("ns", "a"), secretID("ns", "b")
	for name, c := range map[string]struct {
		err  error
		subs [][]string // each logged once per round
	}{
		"forbidden, per name":   {errTest403, [][]string{{"is forbidden", a.String()}, {"is forbidden", b.String()}}},
		"not served, per kind":  {errTest404, [][]string{{"a per-name informer can't list or watch", `"its kind isn't served"`}}},
		"unreachable, per kind": {errors.New("connection refused"), [][]string{{"a per-name informer can't list or watch", "connection refused"}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newFakeRig(t)
			o, sink := r.breakable("secrets"), &sinkLines{}
			perNameWatches(t, r, sink, a, b)
			for round := 1; round <= 2; round++ {
				slowly(t, "both watches", func() bool { return o.watching(2) })
				tries := o.failures()
				o.fail(c.err)
				slowly(t, "each watch to fail", func() bool { return o.failures() >= tries+4 })
				for _, subs := range c.subs {
					slowly(t, "the warning", func() bool { return sink.warned(subs...) >= round })
					if n := sink.warned(subs...); n != round {
						t.Errorf("round %d: warned of %v %d times in all, want %d: %q", round, subs, n, round, sink.warns)
					}
				}
				o.heal()
			}
		})
	}
}

// A watch that stopped, its line gone, neither keeps the kind's failure
// pending nor records one.
func TestAStoppedPerNameWatchForgetsItsFailure(t *testing.T) {
	r := newFakeRig(t)
	o, sink := r.breakable("secrets"), &sinkLines{}
	a, b := secretID("ns", "a"), secretID("ns", "b")
	n := perNameWatches(t, r, sink, a, b)
	eventually(t, "both watches", func() bool { return o.watching(2) })
	ctx := context.Background()
	wa, wb := n.by[a], n.by[b]
	n.watchFailed(ctx, nil, a, wa, errTest404)
	n.watchFailed(ctx, nil, b, wb, errTest404)
	n.follow([]ID{b})
	n.watchFailed(ctx, nil, a, wa, errTest404)
	n.watchFailed(ctx, nil, a, wa, errTest403)
	n.watching(b, wb)
	if len(n.failures.by) != 0 || n.forbidden[a] {
		t.Errorf("the stopped watch's failures stay: %v, forbidden %v", n.failures.by, n.forbidden)
	}
	n.watchFailed(ctx, nil, b, wb, errTest404)
	if got := sink.warned("a per-name informer can't list or watch"); got != 2 {
		t.Errorf("warned %d times, want once before and once after the recovery: %q", got, sink.warns)
	}
	// So does a refusal: the line that comes back is warned of again.
	n.watchFailed(ctx, nil, b, wb, errTest403)
	n.follow(nil)
	if len(n.forbidden) != 0 || len(n.failures.by) != 0 {
		t.Errorf("with every watch stopped: forbidden %v, failures %v", n.forbidden, n.failures.by)
	}
	n.follow([]ID{b})
	n.watchFailed(ctx, nil, b, n.by[b], errTest403)
	if got := sink.warned("is forbidden"); got != 2 {
		t.Errorf("the refusal was warned of %d times, want once for each watch: %q", got, sink.warns)
	}
}

// A Track that fails forgets what its informers failed with, so that the next
// says why it fails too, and not only that it didn't sync in time.
func TestAFailedTracksFailuresAreLoggedByTheNext(t *testing.T) {
	r, kl := newFakeRig(t), &klogLines{}
	r.runLogged(t, kl)
	r.s.syncTimeout.Store(int64(2 * time.Second))
	try := func(protected ...ProtectedKind) *sinkLines {
		t.Helper()
		sink := &sinkLines{}
		opts := r.options(gadgetGK, protected...)
		opts.Logger = sink
		if _, err := r.s.Track(context.Background(), opts); err == nil {
			t.Fatal("Track succeeded")
		}
		return sink
	}
	gadgets := r.breakable("gadgets")
	gadgets.fail(errTest404)
	for n := 1; n <= 2; n++ {
		if sink := try(); sink.warned("the holder informer of", `"its kind isn't served"`) != 1 {
			t.Errorf("Track %d logged %q, want why its holder informer fails, once", n, sink.warns)
		}
	}
	gadgets.heal()
	crds := r.breakable("customresourcedefinitions")
	crds.fail(errTest404)
	for n := 1; n <= 2; n++ {
		if sink := try(); sink.warned("the CRD informer of", `"its kind isn't served"`) != 1 {
			t.Errorf("Track %d logged %q, want why its CRD informer fails, once", n, sink.warns)
		}
	}
	crds.heal()
	r.breakable("configmaps").fail(errTest404)
	for n := 1; n <= 2; n++ {
		try(ProtectedKind{GVK: configMapGVK})
		if got := kl.logged("Failed to watch", "could not find"); got != n {
			t.Errorf("after Track %d, a protected informer it started logged %d times, want %d: %q", n, got, n, kl.lines)
		}
	}
	if len(r.s.failures.by) != 0 {
		t.Errorf("failures that nothing has any more: %v", r.s.failures.by)
	}
}

// So does a Track whose index informer fails.
func TestAFailedTracksIndexFailureIsLoggedByTheNext(t *testing.T) {
	r := newFakeRigWith(t, SharedOptions{IndexNamespace: "idx"}, namespaceObject("idx"))
	r.run(t)
	r.s.syncTimeout.Store(int64(2 * time.Second))
	breakableIn(&r.dyn.Fake, "configmaps").fail(errTest404)
	for n := 1; n <= 2; n++ {
		sink := &sinkLines{}
		opts := r.options(gadgetGK, ProtectedKind{GVK: secretGVK, Watch: PerName})
		opts.Logger = sink
		if _, err := r.s.Track(context.Background(), opts); err == nil {
			t.Fatal("Track succeeded")
		}
		if sink.warned("the index informer of", `"its kind isn't served"`) != 1 {
			t.Errorf("Track %d logged %q, want why its index informer fails, once", n, sink.warns)
		}
	}
	if len(r.s.failures.by) != 0 {
		t.Errorf("failures that nothing has any more: %v", r.s.failures.by)
	}
}

// Each cause is logged once, also when causes alternate, until a recovery.
// How a watch ends in the normal run of things isn't, nor what it fails with
// as the process stops; and a 403 before the first sync is the start's error.
func TestWhatAFailureLogs(t *testing.T) {
	r := newFakeRig(t)
	sink := &sinkLines{}
	tr := r.trackLogged(t, sink)
	ctx := context.Background()
	for range 3 {
		r.s.watchFailed(ctx, nil, tr, "it", true, errTest404)
		r.s.watchFailed(ctx, nil, tr, "it", true, errTest403)
		r.s.watchFailed(ctx, nil, tr, "it", true, errors.New("connection refused"))
		r.s.watchFailed(ctx, nil, tr, "it", true, errors.New("connection reset"))
	}
	if n := sink.warned(); n != 3 {
		t.Errorf("warned %d times of three causes: %q", n, sink.warns)
	}
	r.s.watchRecovered("another")()
	r.s.watchFailed(ctx, nil, tr, "it", true, errTest404)
	if n := sink.warned(); n != 3 {
		t.Errorf("another informer's recovery was this one's: %q", sink.warns)
	}
	r.s.watchRecovered("it")()
	r.s.watchFailed(ctx, nil, tr, "it", true, errTest403)
	if n := sink.warned(); n != 4 || len(r.s.failures.by) != 1 {
		t.Errorf("warned %d times after the recovery, want 4; pending %v", n, r.s.failures.by)
	}
	r.s.watchRecovered("it")()

	stopped, cancel := context.WithCancel(ctx)
	cancel()
	r.s.watchFailed(stopped, nil, tr, "it", true, errTest404)
	r.s.watchFailed(ctx, nil, tr, "it", false, errTest403)
	r.s.watchFailed(ctx, nil, tr, "it", true, io.EOF)
	if n := sink.warned(); n != 4 {
		t.Errorf("a stop, the start's error or a watch's end was logged: %q", sink.warns)
	}
	r.s.watchFailed(ctx, nil, tr, "it", false, errTest404) // says why the start waits
	if n := sink.warned(); n != 5 {
		t.Errorf("warned %d times, want 5: %q", n, sink.warns)
	}

	for _, err := range []error{apierrors.NewResourceExpired("test"), apierrors.NewGone("test"), io.EOF, io.ErrUnexpectedEOF} {
		if !routineWatchEnd(err) {
			t.Errorf("%v isn't routine", err)
		}
	}
	for _, err := range []error{errTest404, errTest403, errors.New("connection refused"), &url.Error{Op: "Get", URL: "https://x", Err: io.EOF}} {
		if routineWatchEnd(err) {
			t.Errorf("%v is routine", err)
		}
	}
}
