package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
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

// multiKeyedConnector is an ExternalConnecter that implements
// RateLimitKeysProvider.
type multiKeyedConnector struct {
	keys    func(mg resource.Managed) ([]string, error)
	connect func(ctx context.Context, mg resource.Managed) (ExternalClient, error)
}

func (c *multiKeyedConnector) RateLimitKeys(mg resource.Managed) ([]string, error) { return c.keys(mg) }

func (c *multiKeyedConnector) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	return c.connect(ctx, mg)
}

// multiKeyedClient is an ExternalClient that implements RateLimitKeysProvider.
type multiKeyedClient struct {
	ExternalClientFns
	keys func(mg resource.Managed) ([]string, error)
}

func (c *multiKeyedClient) RateLimitKeys(mg resource.Managed) ([]string, error) { return c.keys(mg) }

// bothKeyers implements RateLimitKeyer and RateLimitKeysProvider.
type bothKeyers struct{ multiKeyedClient }

func (*bothKeyers) RateLimitKey(resource.Managed) (string, error) { return "single", nil }

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

// fromAndTo is the keyer of a sync between two endpoints.
func fromAndTo(resource.Managed) ([]string, error) {
	return []string{"source.example.com", "github.com"}, nil
}

// A sync between two endpoints stops when either one refuses it, and the object
// says which. This is why RateLimitKeysProvider exists: gated on one key, it
// would keep calling the other endpoint through a 429.
func TestEitherEndpointOfAMultiKeyObjectDefersIt(t *testing.T) {
	for _, throttled := range []string{"source.example.com", "github.com"} {
		t.Run(throttled, func(t *testing.T) {
			g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
			g.Observe(ratelimit.Signal{Key: throttled, Throttled: true, RetryAfter: time.Minute})

			srv := newAPIServer(nil, fixedReady)
			calls := &clientCalls{}
			r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
				WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
					return &multiKeyedClient{ExternalClientFns: countingClient(calls, upToDate), keys: fromAndTo}, nil
				})))

			got, err := r.Reconcile(context.Background(), request("repo"))
			if err != nil || got.RequeueAfter <= 0 {
				t.Fatalf("Reconcile = %+v, %v; want a RequeueAfter with a nil error", got, err)
			}
			if calls.total() != 0 {
				t.Errorf("%d external calls were made, want 0: %s refused the last one", calls.total(), throttled)
			}
			synced := srv.persistedObject("repo").GetCondition(prv1.TypeSynced)
			if synced.Reason != prv1.ReasonReconcileDeferred || !strings.Contains(synced.Message, throttled) {
				t.Errorf("Synced = %s %q, want ReconcileDeferred naming %s", synced.Reason, synced.Message, throttled)
			}
		})
	}
}

// A healthy multi-key object runs, and holds nothing when the pass ends.
func TestMultiKeyObjectReleasesEveryKey(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	srv := newAPIServer(nil)
	calls := &clientCalls{}
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return &multiKeyedClient{ExternalClientFns: countingClient(calls, upToDate), keys: fromAndTo}, nil
		})))

	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if calls.observe.Load() != 1 {
		t.Fatalf("Observe ran %d times, want once", calls.observe.Load())
	}
	release, d := g.AcquireAll(context.Background(), []string{"source.example.com", "github.com"})
	if d != nil {
		t.Fatalf("after the pass the keys are still held: %+v", d)
	}
	release()
}

// The connecter's keys gate Connect, and the client's calls when the client has
// no keyer of its own, the same as for RateLimitKeyer.
func TestConnecterKeysGateConnectAndTheClient(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	g.Observe(ratelimit.Signal{Key: "github.com", Throttled: true, RetryAfter: time.Minute})

	srv := newAPIServer(nil)
	connects := 0
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g), WithExternalConnecter(&multiKeyedConnector{
		keys: fromAndTo,
		connect: func(context.Context, resource.Managed) (ExternalClient, error) {
			connects++
			c := countingClient(&clientCalls{}, upToDate)
			return &c, nil
		},
	}))

	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if connects != 0 {
		t.Errorf("Connect ran %d times with one of its keys held, want 0", connects)
	}
}

// RateLimitKeys wins on a type that implements both, so a provider can add it
// without removing the single key older callers use.
func TestRateLimitKeysWinsOverRateLimitKey(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	g.Observe(ratelimit.Signal{Key: "github.com", Throttled: true, RetryAfter: time.Minute})

	calls := &clientCalls{}
	srv := newAPIServer(nil)
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return &bothKeyers{multiKeyedClient{ExternalClientFns: countingClient(calls, upToDate), keys: fromAndTo}}, nil
		})))

	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if calls.total() != 0 {
		t.Errorf("%d external calls were made: the single key was used instead of RateLimitKeys", calls.total())
	}
}

// No key means not gated, the same as an empty RateLimitKey: whether the
// provider returned nothing, or only empty strings. With saturation on too: the
// local slot is the queue's, taken before the reconcile, not the call's.
func TestNoRateLimitKeysAreUngated(t *testing.T) {
	for name, keys := range map[string][]string{"nil": nil, "empty": {}, "only empty strings": {"", ""}} {
		t.Run(name, func(t *testing.T) {
			testNoRateLimitKeysAreUngated(t, newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1}), keys)
		})
		t.Run(name+", saturation on", func(t *testing.T) {
			testNoRateLimitKeysAreUngated(t, saturatedGovernor(t, 1), keys)
		})
	}
}

func testNoRateLimitKeysAreUngated(t *testing.T, g *ratelimit.Governor, keys []string) {
	t.Helper()
	// The one slot of a key is taken, so a call that went through the governor
	// on any key would be deferred: only an ungated call gets to Observe.
	held, d := g.Acquire(context.Background(), "other")
	if d != nil {
		t.Fatalf("Acquire was deferred: %+v", d)
	}
	defer held()

	srv := newAPIServer(nil)
	calls := &clientCalls{}
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return &multiKeyedClient{
				ExternalClientFns: countingClient(calls, upToDate),
				keys:              func(resource.Managed) ([]string, error) { return keys, nil },
			}, nil
		})))
	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if calls.observe.Load() != 1 {
		t.Errorf("Observe ran %d times, want once", calls.observe.Load())
	}
}

// An error from RateLimitKeys is the same provider defect as one from
// RateLimitKey: the call proceeds without per-endpoint gating.
func TestRateLimitKeysErrorProceedsUngated(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 2})
	g.Observe(ratelimit.Signal{Key: "github.com", Throttled: true, RetryAfter: time.Minute})

	srv := newAPIServer(nil)
	calls := &clientCalls{}
	r := newRateLimitedReconciler(srv, WithRateLimitGovernor(g),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return &multiKeyedClient{
				ExternalClientFns: countingClient(calls, upToDate),
				keys: func(resource.Managed) ([]string, error) {
					return []string{"github.com"}, errors.New("cannot name the keys")
				},
			}, nil
		})))

	if _, err := r.Reconcile(context.Background(), request("repo")); err != nil {
		t.Fatal(err)
	}
	if calls.observe.Load() != 1 {
		t.Errorf("Observe ran %d times, want once: the keys that came with the error must not be used", calls.observe.Load())
	}
}
