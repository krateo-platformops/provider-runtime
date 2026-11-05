package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/controller"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

// keyedConnector is an ExternalConnecter that implements RateLimitKeyer.
type keyedConnector struct {
	key     func(mg resource.Managed) (string, error)
	connect func(ctx context.Context, mg resource.Managed) (ExternalClient, error)

	keyCalls     atomic.Int64
	connectCalls atomic.Int64
}

func (c *keyedConnector) RateLimitKey(mg resource.Managed) (string, error) {
	c.keyCalls.Add(1)
	return c.key(mg)
}

func (c *keyedConnector) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	c.connectCalls.Add(1)
	return c.connect(ctx, mg)
}

// keyedClient is an ExternalClient that implements RateLimitKeyer.
type keyedClient struct {
	ExternalClientFns
	key func(mg resource.Managed) (string, error)
}

func (c *keyedClient) RateLimitKey(mg resource.Managed) (string, error) { return c.key(mg) }

// keyAnnotation carries an object's rate-limit key in these tests.
const keyAnnotation = "test.krateo.io/rate-limit-key"

func keyFromAnnotation(mg resource.Managed) (string, error) {
	return mg.GetAnnotations()[keyAnnotation], nil
}

// clientCalls counts calls to an ExternalClient's methods.
type clientCalls struct{ observe, create, update, delete atomic.Int64 }

func (c *clientCalls) total() int64 {
	return c.observe.Load() + c.create.Load() + c.update.Load() + c.delete.Load()
}

// countingClient returns an ExternalClient whose Observe answers obs.
func countingClient(calls *clientCalls, observe func(ctx context.Context, mg resource.Managed) (ExternalObservation, error)) ExternalClientFns {
	return ExternalClientFns{
		ObserveFn: func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
			calls.observe.Add(1)
			return observe(ctx, mg)
		},
		CreateFn: func(context.Context, resource.Managed) error { calls.create.Add(1); return nil },
		UpdateFn: func(context.Context, resource.Managed) error { calls.update.Add(1); return nil },
		DeleteFn: func(context.Context, resource.Managed) error { calls.delete.Add(1); return nil },
	}
}

func upToDate(_ context.Context, mg resource.Managed) (ExternalObservation, error) {
	mg.SetConditions(prv1.Available())
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

// fixedReady is a Ready condition with a fixed transition time, so "left
// byte-identical" can be checked with DeepEqual.
var fixedReady = prv1.Condition{
	Type:               prv1.TypeReady,
	Status:             metav1.ConditionTrue,
	Reason:             prv1.ReasonAvailable,
	Message:            "available since the last observe",
	LastTransitionTime: metav1.NewTime(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)),
}

// stored gives c the whole-second transition time of a condition read from the
// API server. metav1.Now() has nanoseconds, which a JSON round trip drops, so
// without this a fixture would differ from its own deep copy.
func stored(c prv1.Condition) prv1.Condition {
	c.LastTransitionTime = fixedReady.LastTransitionTime
	return c
}

// apiServer is a mock API server: Get returns the object named in the request
// with keyAnnotation set from keys, and status updates are counted and kept.
type apiServer struct {
	mu            sync.Mutex
	keys          map[string]string
	start         []prv1.Condition
	statusUpdates int
	updates       int
	persisted     map[string]*fake.Managed
	onGet         func(name string)
	// sticky makes Get return the conditions last written, like a real API
	// server, instead of start.
	sticky bool
}

func newAPIServer(keys map[string]string, start ...prv1.Condition) *apiServer {
	return &apiServer{keys: keys, start: start, persisted: map[string]*fake.Managed{}}
}

func (s *apiServer) client() *test.MockClient {
	return &test.MockClient{
		MockGet: func(_ context.Context, key client.ObjectKey, obj client.Object) error {
			if s.onGet != nil {
				s.onGet(key.Name)
			}
			mg := obj.(*fake.Managed)
			mg.SetName(key.Name)
			mg.SetNamespace(key.Namespace)
			mg.SetUID(types.UID("uid-" + key.Name))
			mg.SetAnnotations(map[string]string{keyAnnotation: s.keys[key.Name], meta.AnnotationKeyExternalName: key.Name})
			s.mu.Lock()
			last, sticky := s.persisted[key.Name], s.sticky
			s.mu.Unlock()
			if sticky && last != nil {
				mg.SetConditions(last.Conditions...)
			} else {
				mg.SetConditions(s.start...)
			}
			return nil
		},
		MockUpdate: func(context.Context, client.Object, ...client.UpdateOption) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.updates++
			return nil
		},
		MockStatusUpdate: func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.statusUpdates++
			mg := obj.(*fake.Managed)
			s.persisted[mg.GetName()] = mg.DeepCopyObject().(*fake.Managed)
			return nil
		},
	}
}

func (s *apiServer) statusUpdateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusUpdates
}

func (s *apiServer) persistedObject(name string) *fake.Managed {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persisted[name]
}

func newGovernor(t *testing.T, o ratelimit.Options) *ratelimit.Governor {
	t.Helper()
	g, err := ratelimit.New(o)
	if err != nil {
		t.Fatalf("ratelimit.New: %v", err)
	}
	return g
}

func newRateLimitedReconciler(s *apiServer, o ...ReconcilerOption) *Reconciler {
	mgr := &fake.Manager{Client: s.client(), Scheme: fake.SchemeWith(&fake.Managed{})}
	return NewReconciler(mgr, resource.ManagedKind(fake.GVK(&fake.Managed{})), append([]ReconcilerOption{
		WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
		}),
	}, o...)...)
}

func request(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}}
}

// deadlineIn parses the RFC 3339 time after "next attempt " in msg.
func deadlineIn(t *testing.T, msg string) time.Time {
	t.Helper()
	_, after, ok := strings.Cut(msg, "next attempt ")
	if !ok {
		t.Fatalf("message %q names no next attempt", msg)
	}
	at, err := time.Parse(time.RFC3339, strings.Fields(after)[0])
	if err != nil {
		t.Fatalf("message %q: the next attempt is not RFC 3339: %v", msg, err)
	}
	return at
}

// A deferral writes no assessment: a deferred pass leaves Ready
// byte-identical, writes Synced=False/ReconcileDeferred, and calls no
// ExternalClient method. And it is visible and dated: the message parses to the
// deadline, with one status update as the denominator.
func TestDeferredPassWritesNoAssessmentAndIsDated(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	throttledAt := time.Now()
	g.Observe(ratelimit.Signal{Key: "ghe.example.com", Throttled: true, RetryAfter: 30 * time.Second})

	srv := newAPIServer(map[string]string{"repo": "ghe.example.com"}, fixedReady, prv1.ReconcileSuccess())
	calls := &clientCalls{}
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(calls, upToDate)
		return &c, nil
	}}
	events := &eventCapture{}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g), WithThrottledRecorder(events))

	got, err := r.Reconcile(context.Background(), request("repo"))
	if err != nil {
		t.Fatalf("Reconcile returned %v, want a nil error: a deferral is not a failure", err)
	}
	if conn.connectCalls.Load() != 0 || calls.total() != 0 {
		t.Errorf("a deferred pass called Connect %d times and the client %d times, want none", conn.connectCalls.Load(), calls.total())
	}
	if n := srv.statusUpdateCount(); n != 1 {
		t.Fatalf("status updates = %d, want exactly 1", n)
	}
	persisted := srv.persistedObject("repo")
	// Byte-identical: compared as the API server stores it, in JSON.
	gotReady, _ := json.Marshal(persisted.GetCondition(prv1.TypeReady))
	wantReady, _ := json.Marshal(fixedReady)
	if !bytes.Equal(gotReady, wantReady) {
		t.Errorf("Ready changed on a deferred pass:\n got %s\nwant %s", gotReady, wantReady)
	}
	synced := persisted.GetCondition(prv1.TypeSynced)
	if synced.Status != metav1.ConditionFalse || synced.Reason != prv1.ReasonReconcileDeferred {
		t.Fatalf("Synced = %s/%s, want False/ReconcileDeferred", synced.Status, synced.Reason)
	}
	if !strings.Contains(synced.Message, "ghe.example.com") {
		t.Errorf("message %q does not name the key", synced.Message)
	}
	deadline := throttledAt.Add(30 * time.Second).Truncate(time.Second)
	if at := deadlineIn(t, synced.Message); at.Before(deadline) || at.After(deadline.Add(2*time.Second)) {
		t.Errorf("message deadline %v, want the server's deadline %v (never earlier)", at, deadline)
	}
	if got.RequeueAfter < 29*time.Second || got.RequeueAfter > 30*time.Second {
		t.Errorf("RequeueAfter = %v, want the ~30s left until the server's deadline", got.RequeueAfter)
	}

	deferredEvents := events.withReason(event.Reason(prv1.ReasonReconcileDeferred))
	if len(deferredEvents) != 1 || deferredEvents[0].Type != event.TypeNormal {
		t.Fatalf("events = %+v, want one Normal ReconcileDeferred event", events.events)
	}

	// The same window again, on an object that already reads deferred: no
	// second event.
	srv.start = []prv1.Condition{fixedReady, synced}
	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if n := len(events.withReason(event.Reason(prv1.ReasonReconcileDeferred))); n != 1 {
		t.Errorf("%d ReconcileDeferred events after two passes in one throttle window, want 1", n)
	}
}

// A deferral on the client's own key happens after Connect and before Observe.
func TestClientKeyGatesObserve(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	g.Observe(ratelimit.Signal{Key: "dev.azure.com", Throttled: true, RetryAfter: time.Minute})

	srv := newAPIServer(map[string]string{"pipe": "dev.azure.com"}, fixedReady)
	calls := &clientCalls{}
	connects := 0
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			connects++
			return &keyedClient{ExternalClientFns: countingClient(calls, upToDate), key: keyFromAnnotation}, nil
		})))

	got, err := r.Reconcile(context.Background(), request("pipe"))
	if err != nil || got.RequeueAfter <= 0 {
		t.Fatalf("Reconcile = %+v, %v; want a RequeueAfter with a nil error", got, err)
	}
	if connects != 1 || calls.observe.Load() != 0 {
		t.Errorf("Connect %d, Observe %d; want Connect once (the connecter has no key) and Observe never", connects, calls.observe.Load())
	}
}

// A signal the provider reports during Observe reaches the governor through the
// context, and the Create that follows is deferred before the create-pending
// annotation is written: a deferral after it would leave an unresolved create
// handshake for a create that was never attempted.
func TestSignalFromObserveDefersCreateBeforeThePendingAnnotation(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	srv := newAPIServer(map[string]string{"repo": "api.github.com"})
	calls := &clientCalls{}
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(calls, func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
			ratelimit.Report(ctx, ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: time.Minute})
			mg.SetConditions(prv1.Unavailable())
			return ExternalObservation{ResourceExists: false}, nil
		})
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	got, err := r.Reconcile(context.Background(), request("repo"))
	if err != nil || got.RequeueAfter < 59*time.Second {
		t.Fatalf("Reconcile = %+v, %v; want a ~60s RequeueAfter with a nil error", got, err)
	}
	if calls.create.Load() != 0 {
		t.Error("Create ran on a throttled key")
	}
	if srv.updates != 0 {
		t.Errorf("the managed resource was updated %d times: the create-pending annotation was written for a deferred create", srv.updates)
	}
	if synced := srv.persistedObject("repo").GetCondition(prv1.TypeSynced); synced.Reason != prv1.ReasonReconcileDeferred {
		t.Errorf("Synced reason = %s, want ReconcileDeferred", synced.Reason)
	}
}

// A deferral doesn't consume backoff: the pass returns RequeueAfter with a nil
// error, and in a real controller-runtime controller, with the queue
// ForControllerRuntime configures, the item's NumRequeues stays 0 across two
// deferrals.
func TestDeferralDoesNotConsumeBackoff(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(map[string]string{"repo": "api.github.com"})
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, upToDate)
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	req := request("repo")
	var queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	var passes atomic.Int64
	requeuesAtThirdPass := make(chan int, 1)
	srv.onGet = func(string) {
		switch passes.Add(1) {
		case 1, 2:
			// The server keeps saying "wait": both passes are deferred.
			g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: 100 * time.Millisecond})
		case 3:
			// The third pass starts only after controller-runtime has handled
			// the second's result, so NumRequeues is settled here.
			requeuesAtThirdPass <- queue.NumRequeues(req)
		}
	}

	opts := controller.DefaultOptions().ForControllerRuntime()
	opts.Reconciler = r
	opts.SkipNameValidation = func() *bool { b := true; return &b }()
	opts.NewQueue = func(name string, rl workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
		queue = workqueue.NewTypedRateLimitingQueueWithConfig(rl, workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{Name: name})
		return queue
	}
	c, err := ctrlcontroller.NewTypedUnmanaged("deferral-backoff", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Watch(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		q.Add(req)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	select {
	case n := <-requeuesAtThirdPass:
		if n != 0 {
			t.Errorf("NumRequeues = %d after two deferrals, want 0: the deferral consumed the item's failure backoff", n)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the third pass never ran")
	}
}

// No worker is parked: with MaxConcurrentReconciles=N workers and one throttled
// key queued first, N reconciles of a second key still run concurrently.
func TestNoWorkerIsParkedOnAThrottledKey(t *testing.T) {
	const n = 3
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: n})
	g.Observe(ratelimit.Signal{Key: "throttled.example.com", Throttled: true, RetryAfter: time.Hour})

	keys := map[string]string{}
	var queue []reconcile.Request
	for i := range n {
		name := "busy-" + string(rune('a'+i))
		keys[name] = "throttled.example.com"
		queue = append(queue, request(name))
	}
	for i := range n {
		name := "healthy-" + string(rune('a'+i))
		keys[name] = "healthy.example.com"
		queue = append(queue, request(name))
	}

	var inObserve atomic.Int64
	allIn := make(chan struct{})
	var once sync.Once
	srv := newAPIServer(keys)
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
			if inObserve.Add(1) == n {
				once.Do(func() { close(allIn) })
			}
			select {
			case <-allIn:
			case <-ctx.Done():
			}
			return upToDate(ctx, mg)
		})
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	work := make(chan reconcile.Request, len(queue))
	for _, q := range queue {
		work <- q
	}
	close(work)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for req := range work {
				_, _ = r.Reconcile(ctx, req)
			}
		})
	}

	select {
	case <-allIn:
	case <-time.After(5 * time.Second):
		t.Errorf("only %d of %d healthy reconciles were ever in Observe at once: workers are parked on the throttled key", inObserve.Load(), n)
		cancel()
	}
	wg.Wait()
}

// Keys are independent across reconciles: one object's throttled key doesn't
// defer another object's call on a different key.
func TestThrottledKeyDoesNotDeferAnotherKey(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	g.Observe(ratelimit.Signal{Key: "ghe.example.com", Throttled: true, RetryAfter: time.Hour})

	srv := newAPIServer(map[string]string{"a": "ghe.example.com", "b": "dev.azure.com"})
	calls := &clientCalls{}
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(calls, upToDate)
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	if _, err := r.Reconcile(context.Background(), request("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request("b")); err != nil {
		t.Fatal(err)
	}
	if calls.observe.Load() != 1 {
		t.Errorf("Observe ran %d times, want once: for b, whose key is not throttled", calls.observe.Load())
	}
	if synced := srv.persistedObject("b").GetCondition(prv1.TypeSynced); synced.Reason != prv1.ReasonReconcileSuccess {
		t.Errorf("b's Synced reason = %s, want ReconcileSuccess", synced.Reason)
	}
}

// Off by default: with no governor, a keyer is never asked for a key, nothing is
// deferred, and the pass ends exactly as it does for a provider without a keyer.
func TestOffByDefault(t *testing.T) {
	type outcome struct {
		result   reconcile.Result
		obj      *fake.Managed
		keyCalls int64
		err      error
	}
	run := func(withKeyer bool) outcome {
		srv := newAPIServer(map[string]string{"repo": "api.github.com"}, fixedReady)
		connect := func(context.Context, resource.Managed) (ExternalClient, error) {
			c := countingClient(&clientCalls{}, upToDate)
			return &c, nil
		}
		conn := &keyedConnector{key: keyFromAnnotation, connect: connect}
		opt := WithExternalConnecter(ExternalConnectorFn(connect))
		if withKeyer {
			opt = WithExternalConnecter(conn)
		}
		res, err := newRateLimitedReconciler(srv, opt).Reconcile(context.Background(), request("repo"))
		return outcome{result: res, obj: srv.persistedObject("repo"), keyCalls: conn.keyCalls.Load(), err: err}
	}

	keyed, plain := run(true), run(false)
	if keyed.keyCalls != 0 {
		t.Errorf("RateLimitKey was called %d times with no governor configured", keyed.keyCalls)
	}
	if keyed.result != plain.result || (keyed.err == nil) != (plain.err == nil) {
		t.Errorf("with a keyer and no governor: %+v, %v; without a keyer: %+v, %v", keyed.result, keyed.err, plain.result, plain.err)
	}
	for _, ct := range []prv1.ConditionType{prv1.TypeReady, prv1.TypeSynced} {
		if a, b := keyed.obj.GetCondition(ct), plain.obj.GetCondition(ct); !a.Equal(b) {
			t.Errorf("%s differs with a keyer and no governor: %+v vs %+v", ct, a, b)
		}
	}
	objKeyed := keyed.obj
	if objKeyed.GetCondition(prv1.TypeSynced).Reason == prv1.ReasonReconcileDeferred {
		t.Error("a pass was deferred with no governor configured")
	}
}

// A RateLimitKey error is a provider defect: the call proceeds ungated, and the
// runtime logs it once per object, not once per pass.
func TestKeyerErrorProceedsUngatedAndLogsOncePerObject(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(map[string]string{"a": "", "b": ""})
	calls := &clientCalls{}
	conn := &keyedConnector{
		key: func(resource.Managed) (string, error) { return "", errors.New("no server URL resolved yet") },
		connect: func(context.Context, resource.Managed) (ExternalClient, error) {
			c := countingClient(calls, upToDate)
			return &c, nil
		},
	}
	log := newErrorLogCapture()
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g), WithLogger(log))

	for _, name := range []string{"a", "a", "a", "b"} {
		if _, err := r.Reconcile(context.Background(), request(name)); err != nil {
			t.Fatal(err)
		}
	}
	if calls.observe.Load() != 4 {
		t.Errorf("Observe ran %d times, want 4: a keyer error must not block the call", calls.observe.Load())
	}
	logged := 0
	for _, e := range *log.errors {
		if strings.Contains(e.msg, "rate-limit key") {
			logged++
		}
	}
	if logged != 2 {
		t.Errorf("logged the keyer error %d times for two objects, want 2 (once per object)", logged)
	}
}

// saturatedGovernor returns a governor with saturation on whose sampler ran
// once and stopped, so local capacity stays at its starting value of 1 and the
// governor is on its startup ramp.
func saturatedGovernor(t *testing.T, workers int) *ratelimit.Governor {
	t.Helper()
	g := unstartedSaturatedGovernor(t, workers)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Start(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if s := g.Snapshot(); len(s.Inputs) > 0 && s.Inputs[0].Enabled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sampler never took its first sample")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return g
}

// unstartedSaturatedGovernor returns a governor with saturation on whose
// sampler never started: what a provider gets from ratelimit.New without adding
// it to the manager.
func unstartedSaturatedGovernor(t *testing.T, workers int) *ratelimit.Governor {
	t.Helper()
	return newGovernor(t, ratelimit.Options{
		MaxConcurrentReconciles: workers,
		Saturation: ratelimit.SaturationOptions{
			Enabled:   true,
			CgroupDir: filepath.Join("..", "ratelimit", "testdata", "healthy"),
		},
	})
}

// A governor whose sampler never started stays at capacity 1. That is a provider
// defect, so it must show on the object instead of passing for the quiet ramp.
func TestUnstartedSamplerShowsOnTheObject(t *testing.T) {
	g := unstartedSaturatedGovernor(t, 16)
	srv := newAPIServer(map[string]string{"repo": ""}, fixedReady)
	conn := ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, upToDate)
		return &c, nil
	})
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))
	held, d := g.Acquire(context.Background(), "")
	if d != nil {
		t.Fatal(d)
	}
	defer held()
	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if n := srv.statusUpdateCount(); n != 1 {
		t.Fatalf("status updates = %d, want 1: this deferral must not be quiet", n)
	}
	synced := srv.persistedObject("repo").GetCondition(prv1.TypeSynced)
	if synced.Reason != prv1.ReasonReconcileDeferred || !strings.Contains(synced.Message, "saturation sampler is not running") {
		t.Errorf("Synced = %s %q, want ReconcileDeferred naming the stopped sampler", synced.Reason, synced.Message)
	}
}

// boundedObserve runs one reconcile per object concurrently against conn's
// clients, whose Observe holds its slot until every pass has either entered
// Observe or ended deferred. It returns the most Observe bodies seen at once,
// and how many passes were deferred by the local capacity.
func boundedObserve(t *testing.T, ctx context.Context, g *ratelimit.Governor, objects int, keyed bool) (maxInObserve, localDeferrals int64) {
	t.Helper()
	keys := map[string]string{}
	for i := range objects {
		name := "obj-" + string(rune('a'+i))
		keys[name] = ""
		if keyed {
			keys[name] = "host-" + string(rune('a'+i)) + ".example.com" // one key each: only local can bind
		}
	}
	var inObserve, maxIn, settled, deferredLocal atomic.Int64
	releaseObserve := make(chan struct{})
	observe := func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
		now := inObserve.Add(1)
		for m := maxIn.Load(); now > m && !maxIn.CompareAndSwap(m, now); m = maxIn.Load() {
		}
		settled.Add(1)
		// Stand-in for expensive work: hold the slot until released.
		<-releaseObserve
		inObserve.Add(-1)
		return upToDate(ctx, mg)
	}
	srv := newAPIServer(keys)
	connect := func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, observe)
		return &c, nil
	}
	var opt ReconcilerOption
	if keyed {
		opt = WithExternalConnecter(&keyedConnector{key: keyFromAnnotation, connect: connect})
	} else {
		opt = WithExternalConnecter(ExternalConnectorFn(connect)) // no RateLimitKeyer anywhere
	}
	r := newRateLimitedReconciler(srv, opt, WithRateLimitGovernor(g))

	var wg sync.WaitGroup
	for name := range keys {
		wg.Go(func() {
			res, err := r.Reconcile(ctx, request(name))
			if err != nil {
				t.Errorf("Reconcile(%s): %v", name, err)
			}
			// On the startup ramp a deferred pass writes nothing; a pass that
			// ran Observe writes its result.
			if res.RequeueAfter > 0 && srv.persistedObject(name) == nil {
				deferredLocal.Add(1)
				settled.Add(1)
			}
		})
	}
	for deadline := time.Now().Add(5 * time.Second); settled.Load() < int64(objects) && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	close(releaseObserve)
	wg.Wait()
	return maxIn.Load(), deferredLocal.Load()
}

// local bounds the work inside gated calls: with local capacity 1 and 8 objects
// on 8 different keys, one Observe body runs at a time, and the rest are
// deferred by the local capacity. This is why a provider's expensive work must happen inside Connect and the ExternalClient
// methods: work done before them, in the provider's own code, is not bounded by
// local.
func TestLocalCapacityBoundsConcurrentObserveBodies(t *testing.T) {
	maxIn, deferred := boundedObserve(t, context.Background(), saturatedGovernor(t, 8), 8, true)
	if maxIn != 1 {
		t.Errorf("at most %d Observe bodies ran at once, want 1: local capacity must bound concurrent gated calls", maxIn)
	}
	if deferred != 7 {
		t.Errorf("%d passes were deferred by the local capacity, want 7", deferred)
	}
}

// Saturation applies to every gated call, keyer or not: a provider that
// implements no RateLimitKeyer is still bounded by local.
func TestProviderWithoutKeyerIsBoundedByLocal(t *testing.T) {
	maxIn, deferred := boundedObserve(t, context.Background(), saturatedGovernor(t, 8), 8, false)
	if maxIn != 1 {
		t.Errorf("at most %d Observe bodies of a keyless provider ran at once, want 1: saturation must bound calls without a keyer", maxIn)
	}
	if deferred != 7 {
		t.Errorf("%d keyless passes were deferred by the local capacity, want 7", deferred)
	}
}

// Without saturation, a provider without a keyer is not gated at all.
func TestProviderWithoutKeyerIsUngatedWithoutSaturation(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(map[string]string{"a": "", "b": ""})
	calls := &clientCalls{}
	held := make(chan struct{})
	entered := make(chan struct{}, 2)
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			c := countingClient(calls, func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
				entered <- struct{}{}
				<-held
				return upToDate(ctx, mg)
			})
			return &c, nil
		})))
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		wg.Go(func() { _, _ = r.Reconcile(context.Background(), request(name)) })
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("both keyless Observes should run at once with saturation off and a ceiling of 1")
		}
	}
	close(held)
	wg.Wait()
}

// A deferral that would write the object back unchanged skips the status
// update: two consecutive at-capacity deferrals produce one write. A throttled
// deferral that names a new deadline still writes.
func TestIdenticalDeferralSkipsTheStatusWrite(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(map[string]string{"repo": "api.github.com"}, fixedReady)
	srv.sticky = true
	calls := &clientCalls{}
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(calls, upToDate)
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	// At capacity: the key's only slot is taken.
	held, d := g.Acquire(context.Background(), "api.github.com")
	if d != nil {
		t.Fatal(d)
	}
	for pass := 1; pass <= 2; pass++ {
		got, err := r.Reconcile(context.Background(), request("repo"))
		if err != nil || got.RequeueAfter <= 0 {
			t.Fatalf("pass %d: Reconcile = %+v, %v; want a RequeueAfter with a nil error", pass, got, err)
		}
	}
	if n := srv.statusUpdateCount(); n != 1 {
		t.Fatalf("two identical at-capacity deferrals made %d status updates, want 1", n)
	}
	held()

	// Throttled: a deadline the object doesn't show yet writes; the same
	// deadline again doesn't; a later deadline does.
	g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: 30 * time.Second})
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
			t.Fatal(err)
		}
	}
	if n := srv.statusUpdateCount(); n != 2 {
		t.Fatalf("a throttled deferral with a new deadline, then the same again: %d status updates in all, want 2", n)
	}
	g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: 10 * time.Minute})
	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if n := srv.statusUpdateCount(); n != 3 {
		t.Errorf("a throttled deferral with a later deadline made %d status updates in all, want 3: a new deadline must be written", n)
	}
	if calls.total() != 0 {
		t.Errorf("deferred passes called the client %d times", calls.total())
	}
}

// A restart is quiet: with a fresh governor, on its startup ramp, and the one
// local slot taken, 200 healthy objects get no status update, no event and no
// provider call, keep their Synced, and each pass requeues with a nil error.
// Before this was quiet, the same restart made 200 status updates and left all
// 200 at ReconcileDeferred.
func TestRestartOnTheStartupRampIsQuiet(t *testing.T) {
	const objects = 200
	for name, keyed := range map[string]bool{"with a keyer": true, "without a keyer": false} {
		t.Run(name, func(t *testing.T) {
			keys := map[string]string{}
			for i := range objects {
				keys[fmt.Sprintf("obj-%03d", i)] = "api.github.com"
			}
			srv := newAPIServer(keys, fixedReady, stored(prv1.ReconcileSuccess()))
			srv.sticky = true
			calls := &clientCalls{}
			connects := &atomic.Int64{}
			connect := func(context.Context, resource.Managed) (ExternalClient, error) {
				connects.Add(1)
				c := countingClient(calls, upToDate)
				return &c, nil
			}
			opt := WithExternalConnecter(ExternalConnectorFn(connect))
			if keyed {
				opt = WithExternalConnecter(&keyedConnector{key: keyFromAnnotation, connect: connect})
			}
			g := saturatedGovernor(t, 16)
			events := &eventCapture{}
			r := newRateLimitedReconciler(srv, opt, WithRateLimitGovernor(g), WithRecorder(events), WithThrottledRecorder(events))

			held, d := g.Acquire(context.Background(), "")
			if d != nil {
				t.Fatal(d)
			}
			for name := range keys {
				got, err := r.Reconcile(context.Background(), request(name))
				if err != nil || got.RequeueAfter <= 0 {
					t.Fatalf("Reconcile(%s) = %+v, %v; want a RequeueAfter with a nil error", name, got, err)
				}
			}
			if n := srv.statusUpdateCount(); n != 0 {
				t.Errorf("%d status updates for %d objects on the startup ramp, want 0", n, objects)
			}
			if len(events.events) != 0 {
				t.Errorf("%d events on the startup ramp, want 0: %+v", len(events.events), events.events[0])
			}
			if connects.Load() != 0 || calls.total() != 0 {
				t.Errorf("Connect ran %d times and the client %d, want no provider call", connects.Load(), calls.total())
			}

			// The queue drains once a slot frees: the object goes straight to
			// Synced=True, and was never reported as deferred.
			held()
			if _, err := r.Reconcile(context.Background(), request("obj-000")); err != nil {
				t.Fatal(err)
			}
			if synced := srv.persistedObject("obj-000").GetCondition(prv1.TypeSynced); synced.Reason != prv1.ReasonReconcileSuccess {
				t.Errorf("after the slot freed, Synced reason = %s, want ReconcileSuccess", synced.Reason)
			}
		})
	}
}

// cutGovernor returns a governor whose local capacity has been cut by a real
// saturated sample, with the one local slot taken, so the next call is deferred
// by the local capacity. With clean set, a clean sample has followed the cut.
func cutGovernor(t *testing.T, ctx context.Context, clean bool) (*ratelimit.Governor, func()) {
	t.Helper()
	dir := t.TempDir()
	write := func(avg10 string) {
		t.Helper()
		line := "some avg10=" + avg10 + " avg60=0.00 avg300=0.00 total=0\n"
		if err := os.WriteFile(filepath.Join(dir, "cpu.pressure"), []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("90.00")
	g := newGovernor(t, ratelimit.Options{
		MaxConcurrentReconciles: 16,
		Saturation:              ratelimit.SaturationOptions{Enabled: true, CgroupDir: dir, SamplePeriod: 5 * time.Millisecond},
	})
	held, d := g.Acquire(ctx, "")
	if d != nil {
		t.Fatal(d)
	}
	go func() { _ = g.Start(ctx) }()
	waitFor := func(want ratelimit.DeferReason) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if _, d := g.Acquire(ctx, ""); d != nil && d.Reason == want {
				return
			}
		}
		t.Fatalf("no %s deferral within 5s", want)
	}
	waitFor(ratelimit.DeferSaturated)
	if clean {
		write("1.00")
		waitFor(ratelimit.DeferAtCapacity)
	}
	return g, held
}

// Quiet applies only to the startup ramp. After a cut the same deferral writes,
// and a saturated, throttled or per-key at-capacity deferral writes whenever it
// happens, also while the ramp is running.
func TestOnlyTheStartupRampIsQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cases := map[string]struct {
		setup       func(t *testing.T) *ratelimit.Governor
		wantMessage string
	}{
		"at capacity after a cut": {
			setup: func(t *testing.T) *ratelimit.Governor {
				g, held := cutGovernor(t, ctx, true)
				t.Cleanup(held)
				return g
			},
			wantMessage: "controller recovering its local concurrency limit",
		},
		"saturated during the ramp": {
			setup: func(t *testing.T) *ratelimit.Governor {
				g, held := cutGovernor(t, ctx, false)
				t.Cleanup(held)
				return g
			},
			wantMessage: "controller saturated: CPU pressure 90%",
		},
		"throttled during the ramp": {
			setup: func(t *testing.T) *ratelimit.Governor {
				g := saturatedGovernor(t, 16)
				g.Observe(ratelimit.Signal{Key: "api.github.com", Throttled: true, RetryAfter: time.Minute})
				return g
			},
			wantMessage: "api.github.com is rate-limited by the server; next attempt",
		},
		"key at capacity during the ramp": {
			setup: func(t *testing.T) *ratelimit.Governor {
				g := saturatedGovernor(t, 1)
				held, d := g.Acquire(ctx, "api.github.com")
				if d != nil {
					t.Fatal(d)
				}
				t.Cleanup(held)
				return g
			},
			wantMessage: "api.github.com is at its concurrency limit of 1 call(s)",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := tc.setup(t)
			srv := newAPIServer(map[string]string{"repo": "api.github.com"}, fixedReady, prv1.ReconcileSuccess())
			calls := &clientCalls{}
			conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
				c := countingClient(calls, upToDate)
				return &c, nil
			}}
			events := &eventCapture{}
			r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g), WithThrottledRecorder(events))

			got, err := r.Reconcile(ctx, request("repo"))
			if err != nil || got.RequeueAfter <= 0 {
				t.Fatalf("Reconcile = %+v, %v; want a RequeueAfter with a nil error", got, err)
			}
			if n := srv.statusUpdateCount(); n != 1 {
				t.Fatalf("status updates = %d, want 1: only the startup ramp is quiet", n)
			}
			synced := srv.persistedObject("repo").GetCondition(prv1.TypeSynced)
			if synced.Status != metav1.ConditionFalse || synced.Reason != prv1.ReasonReconcileDeferred || !strings.Contains(synced.Message, tc.wantMessage) {
				t.Errorf("Synced = %s/%s %q, want False/ReconcileDeferred naming %q", synced.Status, synced.Reason, synced.Message, tc.wantMessage)
			}
			if n := len(events.withReason(event.Reason(prv1.ReasonReconcileDeferred))); n != 1 {
				t.Errorf("%d ReconcileDeferred events, want 1", n)
			}
			if calls.total() != 0 {
				t.Errorf("the client was called %d times, want none", calls.total())
			}
		})
	}
}

// A pass deferred after Observe, on the startup ramp, must not lose what
// Observe learned: the fresh Ready is written, and Synced is not set to
// deferred. With nothing new, nothing is written.
func TestQuietDeferralAfterObserveKeepsWhatObserveLearned(t *testing.T) {
	cases := map[string]struct {
		start       prv1.Condition
		wantWrites  int
		wantReady   prv1.ConditionReason
		wantPersist bool
	}{
		"Observe changed Ready":   {start: stored(prv1.Unavailable()), wantWrites: 1, wantReady: prv1.ReasonAvailable, wantPersist: true},
		"Observe changed nothing": {start: stored(prv1.Available()), wantWrites: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := saturatedGovernor(t, 16)
			srv := newAPIServer(map[string]string{"repo": "api.github.com"}, tc.start, stored(prv1.ReconcileSuccess()))
			calls := &clientCalls{}

			// The client's key is asked before Observe and again before Update.
			// Taking the one local slot on the second ask defers Update, after
			// Observe has run.
			var gates atomic.Int64
			var held func()
			key := func(resource.Managed) (string, error) {
				if gates.Add(1) == 2 {
					held, _ = g.Acquire(context.Background(), "other")
				}
				return "api.github.com", nil
			}
			observe := func(_ context.Context, mg resource.Managed) (ExternalObservation, error) {
				mg.SetConditions(prv1.Available())
				return ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, nil
			}
			events := &eventCapture{}
			r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g), WithRecorder(events), WithThrottledRecorder(events),
				WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
					return &keyedClient{ExternalClientFns: countingClient(calls, observe), key: key}, nil
				})))

			got, err := r.Reconcile(context.Background(), request("repo"))
			if err != nil || got.RequeueAfter <= 0 {
				t.Fatalf("Reconcile = %+v, %v; want a RequeueAfter with a nil error", got, err)
			}
			if held == nil {
				t.Fatal("the local slot was never taken, so no deferral after Observe was tested")
			}
			defer held()
			if calls.observe.Load() != 1 || calls.update.Load() != 0 {
				t.Fatalf("Observe ran %d times and Update %d, want 1 and 0", calls.observe.Load(), calls.update.Load())
			}
			if n := srv.statusUpdateCount(); n != tc.wantWrites {
				t.Fatalf("status updates = %d, want %d", n, tc.wantWrites)
			}
			if len(events.events) != 0 {
				t.Errorf("%d events on a quiet deferral, want 0", len(events.events))
			}
			if tc.wantPersist {
				persisted := srv.persistedObject("repo")
				if ready := persisted.GetCondition(prv1.TypeReady); ready.Reason != tc.wantReady {
					t.Errorf("persisted Ready reason = %s, want %s: what Observe learned was lost", ready.Reason, tc.wantReady)
				}
				if synced := persisted.GetCondition(prv1.TypeSynced); synced.Reason != prv1.ReasonReconcileSuccess {
					t.Errorf("persisted Synced reason = %s, want it left at ReconcileSuccess", synced.Reason)
				}
			}
		})
	}
}

// A provider call that panics still gives its slot back: controller-runtime
// recovers the panic, and a leaked slot would lower the key's capacity for the
// life of the process.
func TestPanickingCallReleasesItsSlot(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(map[string]string{"repo": "api.github.com"})
	conn := &keyedConnector{key: keyFromAnnotation, connect: func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, func(context.Context, resource.Managed) (ExternalObservation, error) {
			panic("provider bug")
		})
		return &c, nil
	}}
	r := newRateLimitedReconciler(srv, WithExternalConnecter(conn), WithRateLimitGovernor(g))

	func() {
		defer func() { _ = recover() }()
		_, _ = r.Reconcile(context.Background(), request("repo"))
	}()
	release, d := g.Acquire(context.Background(), "api.github.com")
	if d != nil {
		t.Fatalf("after a panicking Observe, the only slot is still taken: %+v", d)
	}
	release()
}
