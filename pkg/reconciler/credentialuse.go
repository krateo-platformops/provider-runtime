package reconciler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	pkgerrors "github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// The credential-use adapter (docs/design-credential-use-finalizer.md §7.5):
// a connecter that names the Secrets, and the objects that lead to them, that
// Connect reads gets them held for as long as losing them would be
// unrecoverable. Section numbers (§) here are that design's.

// A CredentialRefsProvider names what Connect reads to reach the external
// system for mg: the Secrets, and the objects that lead to them (§7.5, §9).
//
// In this release no ref it returns may have a Next: name each object as a
// ref of its own. A list with a Next is refused whole, with
// credentialuse.ErrChainsNotSupported, which is an error like any other below:
// a new object doesn't connect, and an established one proceeds and keeps the
// holds it has, but nothing new is held, its direct refs included (§9, "Not in
// this release").
//
// It is discovered on the connecter given to WithExternalConnecter or
// WithExternalConnectDisconnecter, as the rate-limit keyers are. It lives on
// the connecter because refs are needed before Connect, when there is no
// client yet.
//
// It must make no API calls; it may use configuration held in memory. It is
// called to acquire, never to release, and an error fails the pass closed for
// a new object and open for an established one (§8). (Once chains are
// supported, a ref with a Next is followed after the object it names is held;
// Connect then reads that object with credentialuse.Resolved, not from a
// cache (§9.2).)
//
// A connecter that implements it also needs the controller's builder line:
//
//	r := reconciler.NewReconciler(mgr, kind, reconciler.WithExternalConnecter(c), ...)
//	return ctrl.NewControllerManagedBy(mgr).
//		Named("myresource").
//		For(&v1alpha1.MyResource{}, builder.WithPredicates(r.IgnoreCredentialUseRecord())).
//		WatchesRawSource(r.CredentialUseSource()).
//		WithOptions(o.ForControllerRuntime()).
//		Complete(r)
//
// Without the source the manager refuses to start (§7.5, M4). A connecter
// that doesn't implement it changes nothing (§11.10): no informer, no
// annotation, no write, and the source and the predicate are no-ops.
//
// Before implementing it: if mg's CRD, or a protected custom resource's, serves
// several versions, check its conversions (§6.2).
type CredentialRefsProvider interface {
	CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error)
}

// credentialRefsOf returns v's CredentialRefsProvider, or nil.
func credentialRefsOf(v any) CredentialRefsProvider {
	p, _ := v.(CredentialRefsProvider)
	return p
}

// WithCredentialUse sets the holder kind's credential-use settings: the
// protected kinds, their watch mode and Content, the grace and the intervals
// (§7.5). Without it, Secrets are protected, LabelScoped, with the core's
// defaults. It does nothing for a connecter that doesn't implement
// CredentialRefsProvider.
func WithCredentialUse(o credentialuse.KindOptions) ReconcilerOption {
	return func(r *Reconciler) {
		r.cu.kind = o
	}
}

// WithCredentialUseShared has the Reconciler track its kind in s, instead of
// the Shared NewReconciler builds once per manager with the default
// SharedOptions (§7.5). A provider that needs other SharedOptions
// (Namespaces, Listers, IndexNamespace for PerName) builds its own and passes it to every Reconciler of
// the manager, in production as in tests. With a manager that has a config,
// NewReconciler adds s's two runnables to it, once per Shared; with a test
// manager's nil config, the test runs s itself (Run, or RunInformers and
// RunWriteLoops).
func WithCredentialUseShared(s *credentialuse.Shared) ReconcilerOption {
	return func(r *Reconciler) {
		r.cu.shared = s
	}
}

// sourceStartTimeout is how long after leader election, or after its
// NewReconciler when that comes later, a Reconciler's CredentialUseSource may
// stay unstarted before the write-loop runnable fails the manager (§7.5, M4).
// A variable, so that the tests can shorten it.
var sourceStartTimeout = 2 * time.Minute

// credentialUseRecorderName names the events the core records (§12): the
// reportingController of their events.k8s.io/v1 Events.
const credentialUseRecorderName = "credential-use" // #nosec G101 -- an event recorder name, not a credential

// credentialUse is a Reconciler's credential-use state.
type credentialUse struct {
	// refs is the connecter's CredentialRefsProvider, or nil (§11.10).
	refs CredentialRefsProvider
	// kind and shared are WithCredentialUse's and WithCredentialUseShared's.
	kind   credentialuse.KindOptions
	shared *credentialuse.Shared

	// enabled: the connecter names refs and the Reconciler has a Shared, so
	// every pass needs the tracker; tracker is it, once Track succeeded,
	// and setupErr is why it didn't.
	enabled  bool
	tracker  *credentialuse.Tracker
	setupErr error
	// owners are the owner finalizers: the NamedFinalizer's name, the one
	// RemoveFinalizer removes (§7.5).
	owners []string
	src    *credentialUseSource
	wiring *sharedWiring
}

// active reports whether the passes acquire.
func (c *credentialUse) active() bool { return c.tracker != nil }

// fail records err as the setup error: the manager refuses to start, and
// every pass returns it before the Get.
func (c *credentialUse) fail(err error) {
	c.enabled = true
	c.setupErr = err
	if c.wiring != nil {
		c.wiring.fail(err)
	}
}

// ready is §8's step 1, before the Get: a pass of a kind whose credential-use
// setup failed, or whose CredentialUseSource was never started, does nothing
// (no Acquire, no Connect) and fails the manager (M4). The process is about to
// exit, so nothing is lost by waiting for the next one.
func (c *credentialUse) ready(kind string) error {
	if !c.enabled {
		return nil
	}
	if c.setupErr != nil {
		return c.setupErr
	}
	if !c.src.started.Load() {
		err := sourceNotStarted(kind)
		if c.wiring != nil {
			c.wiring.fail(err)
		}
		return err
	}
	if c.wiring != nil {
		return c.wiring.notWriting(kind, time.Now())
	}
	return nil
}

func sourceNotStarted(kind string) error {
	return fmt.Errorf("credential-use source for %s was not started: add .WatchesRawSource(r.CredentialUseSource()) to the controller", kind)
}

// setupCredentialUse wires credential-use for the Reconciler's kind, once
// every option is applied (§7.5).
func (r *Reconciler) setupCredentialUse(m manager.Manager, gvk schema.GroupVersionKind) {
	c := &r.cu
	if c.refs == nil {
		return // §11.10: nothing is created, and the source is a no-op
	}
	s := c.shared
	if s == nil && m.GetConfig() == nil {
		// A test manager (pkg/resource/fake.Manager) has no config.
		logCredentialUseOff(m, r.log, r.kind)
		return
	}
	c.enabled = true
	if m.GetConfig() != nil {
		var err error
		if s, c.wiring, err = wiringFor(m, s); err != nil {
			c.fail(fmt.Errorf("credential-use: %s: %w", r.kind, err))
			return
		}
	}
	c.src = &credentialUseSource{kind: r.kind, wiring: c.wiring, registered: time.Now()}
	if c.wiring != nil {
		c.wiring.register(c.src)
	}
	nf, ok := r.managed.Finalizer.(resource.NamedFinalizer)
	if !ok || nf.FinalizerName() == "" {
		c.fail(fmt.Errorf("credential-use: %s: the Finalizer given with WithFinalizer doesn't implement resource.NamedFinalizer, "+
			"so objects that carry it can't be told from new ones (§7.5): give it a FinalizerName", r.kind))
		return
	}
	c.owners = []string{nf.FinalizerName()}
	metrics := credentialuse.MetricsRecorder(credentialuse.NopMetrics{})
	if mr, ok := r.metrics.(credentialuse.MetricsRecorder); ok {
		metrics = mr
	} else if r.metrics != nil {
		// It may lack a method the interface grew, or embed two recorders.
		r.log.Warn("credential-use: the recorder given to WithMetrics isn't a credentialuse.MetricsRecorder, so no credentialuse_ metric is recorded: "+
			"embed credentialuse.NopMetrics, or *telemetry.Metrics, not both", "kind", r.kind, "recorder", fmt.Sprintf("%T", r.metrics))
	}
	refs := c.refs
	o := credentialuse.Options{
		Holder:          gvk,
		OwnerFinalizers: c.owners,
		Refs: func(obj metav1.Object) ([]credentialuse.Ref, error) {
			mg, ok := obj.(resource.Managed)
			if !ok {
				return nil, fmt.Errorf("credential-use: %T is not a managed resource", obj)
			}
			return refs.CredentialRefs(mg)
		},
		Protected:      c.kind.Protected,
		MaxDepth:       c.kind.MaxDepth,
		DeletionGrace:  c.kind.DeletionGrace,
		ProbeInterval:  c.kind.ProbeInterval,
		DisownInterval: c.kind.DisownInterval,
		DisownPasses:   c.kind.DisownPasses,
		PollInterval:   r.pollInterval,
		Enqueue:        c.src.enqueue,
		// The adapter's bound on a pass: its timeout plus the grace for
		// the status write after it (§7.3, A6).
		TimeoutBound: r.timeout + reconcileGracePeriod,
		Logger:       r.log,
		Metrics:      metrics,
	}
	t, err := s.Track(context.Background(), o)
	if err != nil {
		c.fail(err)
		return
	}
	c.tracker = t
	c.src.tracker = t
}

// CredentialUseSource is the source the controller must watch when the
// connecter implements CredentialRefsProvider:
// WatchesRawSource(r.CredentialUseSource()) (§7.5). It enqueues a holder when
// an object it holds changes, is re-pointed, or appears (§10.2, §10.3). It is
// a syncing source: the controller's workers start only once the protected
// objects' initial state has reached the tracker (L1). Its Start also starts
// the informers, when the informer runnable hasn't yet, so that
// EnableWarmup, which starts sources before election, works (IP4).
//
// For a connecter that doesn't implement CredentialRefsProvider, or with a
// test manager's nil config, it is a no-op source, so the builder line is
// harmless there.
func (r *Reconciler) CredentialUseSource() source.TypedSyncingSource[reconcile.Request] {
	if !r.cu.enabled || r.cu.src == nil {
		return nopSource{}
	}
	return r.cu.src
}

// IgnoreCredentialUseRecord is the predicate the controller's For must take
// when the connecter implements CredentialRefsProvider (§7.5, IP3): it drops a
// holder update event whose only change is the credential-use record or the
// unheld annotation, setting aside resourceVersion and managedFields, when
// both equal what this process last wrote for that holder (M6, L8). That
// removes the extra pass each record write would cause, and nothing else: a
// hand edit of the record, or another process's write, still triggers a pass.
// Without credential-use it passes every event.
func (r *Reconciler) IgnoreCredentialUseRecord() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		t := r.cu.tracker
		if t == nil || e.ObjectOld == nil || e.ObjectNew == nil {
			return true
		}
		return !t.IsOwnRecordWrite(e.ObjectOld, e.ObjectNew)
	}}
}

// nopSource is CredentialUseSource without credential-use.
type nopSource struct{}

func (nopSource) Start(context.Context, workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	return nil
}
func (nopSource) WaitForSync(context.Context) error { return nil }
func (nopSource) String() string                    { return "credential-use source (off)" }

// credentialUseSource is a Reconciler's CredentialUseSource.
type credentialUseSource struct {
	kind       string
	wiring     *sharedWiring
	registered time.Time
	// tracker is set once Track succeeded, before NewReconciler returns.
	tracker *credentialuse.Tracker

	started atomic.Bool
	queue   atomic.Pointer[workqueue.TypedRateLimitingInterface[reconcile.Request]]
}

// Start keeps the controller's queue, for the tracker's Enqueue, and starts
// the informers if nothing has yet.
func (s *credentialUseSource) Start(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	if q == nil {
		return errors.New("credential-use: the source was started without a queue")
	}
	s.queue.Store(&q)
	s.started.Store(true)
	if s.wiring != nil {
		s.wiring.startInformers()
	}
	return nil
}

// sourceSyncPoll is how often WaitForSync looks at the tracker's handlers.
const sourceSyncPoll = 50 * time.Millisecond

// WaitForSync waits until the tracker's informers have synced and its
// handlers on the protected informers have had their initial state (§7.5,
// L1). It fails at once when the informers fail, or the setup did.
func (s *credentialUseSource) WaitForSync(ctx context.Context) error {
	t := s.tracker
	var failed, ended <-chan struct{}
	if s.wiring != nil {
		failed, ended = s.wiring.failed, s.wiring.runDone
	}
	if t == nil {
		if s.wiring != nil {
			select {
			case <-failed:
				return s.wiring.failErr
			default:
			}
		}
		return fmt.Errorf("credential-use: %s isn't tracked", s.kind)
	}
	tick := time.NewTicker(sourceSyncPoll)
	defer tick.Stop()
	for !t.HasSynced() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-failed:
			return s.wiring.failErr
		case <-ended:
			return s.wiring.runError()
		case <-tick.C:
		}
	}
	return nil
}

func (s *credentialUseSource) String() string { return "credential-use source for " + s.kind }

// enqueue is the tracker's Enqueue: it adds holder nn to the controller's
// queue once the source has started, and drops it before then. Nothing is
// lost by that: no Reconcile runs before the source has started and synced,
// and the controller's For source delivers every holder once at start (A22).
func (s *credentialUseSource) enqueue(nn types.NamespacedName) {
	if q := s.queue.Load(); q != nil {
		(*q).Add(reconcile.Request{NamespacedName: nn})
	}
}

// The per-manager registry (§7.5, A11): the Shared NewReconciler builds for a
// manager, once, and the runnables of every Shared a manager runs, added once
// each, so that concurrent NewReconcilers on one manager share them.
var credentialUseRegistry = struct {
	mu       sync.Mutex
	defaults map[manager.Manager]*credentialuse.Shared
	wirings  map[*credentialuse.Shared]*sharedWiring
	offLog   map[manager.Manager]bool
}{
	defaults: map[manager.Manager]*credentialuse.Shared{},
	wirings:  map[*credentialuse.Shared]*sharedWiring{},
	offLog:   map[manager.Manager]bool{},
}

// logCredentialUseOff logs, once per manager, that credential-use is off for
// want of a config.
func logCredentialUseOff(m manager.Manager, log logging.Logger, kind string) {
	if !comparableKey(m) {
		return
	}
	reg := &credentialUseRegistry
	reg.mu.Lock()
	first := !reg.offLog[m]
	reg.offLog[m] = true
	reg.mu.Unlock()
	if first {
		log.Info("credential-use is off: the manager has no config (a test manager); the connecter's CredentialRefs are not held", "kind", kind)
	}
}

func comparableKey(m manager.Manager) bool {
	return m != nil && reflect.TypeOf(m).Comparable()
}

// wiringFor returns s, or the manager's default Shared when s is nil, and its
// runnables, adding them to m on first use.
func wiringFor(m manager.Manager, s *credentialuse.Shared) (*credentialuse.Shared, *sharedWiring, error) {
	if !comparableKey(m) {
		return nil, nil, fmt.Errorf("the manager, a %T, can't key the per-manager registry", m)
	}
	reg := &credentialUseRegistry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if s == nil {
		s = reg.defaults[m]
		if s == nil {
			var err error
			if s, err = newDefaultShared(m); err != nil {
				return nil, nil, err
			}
			reg.defaults[m] = s
		}
	}
	if w := reg.wirings[s]; w != nil {
		return s, w, nil
	}
	w := newSharedWiring(s)
	if err := m.Add(informerRunnable{w}); err != nil {
		return nil, nil, fmt.Errorf("adding the informer runnable: %w", err)
	}
	if err := m.Add(writeLoopsRunnable{w}); err != nil {
		return nil, nil, fmt.Errorf("adding the write-loop runnable: %w", err)
	}
	reg.wirings[s] = w
	return s, w, nil
}

// newDefaultShared builds a manager's Shared from its config and RESTMapper,
// with the default SharedOptions (§7.5).
func newDefaultShared(m manager.Manager) (*credentialuse.Shared, error) {
	cfg := m.GetConfig()
	md, err := metadata.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the metadata client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the dynamic client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the discovery client: %w", err)
	}
	return credentialuse.NewShared(credentialuse.Clients{
		Metadata:  md,
		Dynamic:   dyn,
		Mapper:    m.GetRESTMapper(),
		Discovery: disc.RESTClient(),
		Events:    m.GetEventRecorder(credentialUseRecorderName),
	}, credentialuse.SharedOptions{}), nil
}

// sharedWiring is a Shared as a manager runs it: two runnables (§7.5, IP4),
// the informers, which start at the first of the informer runnable's Start
// and a CredentialUseSource's, and the write loops, which need leader
// election; and the setup failures that make both refuse to run.
type sharedWiring struct {
	s *credentialuse.Shared

	// runCtx is the informers' context. The informer runnable ends it when
	// its own context ends; a source that starts them first doesn't own them.
	runCtx    context.Context
	cancelRun context.CancelFunc
	startOnce sync.Once
	runDone   chan struct{}
	runErr    error

	failOnce sync.Once
	failed   chan struct{}
	failErr  error

	// writing: the write loops have started in this process.
	writing atomic.Bool

	mu      sync.Mutex
	sources []*credentialUseSource
	// waiting is when the first pass ran without the write loops.
	waiting time.Time
}

func newSharedWiring(s *credentialuse.Shared) *sharedWiring {
	ctx, cancel := context.WithCancel(context.Background())
	return &sharedWiring{s: s, runCtx: ctx, cancelRun: cancel, runDone: make(chan struct{}), failed: make(chan struct{})}
}

// startInformers starts the Shared's informers, once.
func (w *sharedWiring) startInformers() {
	w.startOnce.Do(func() {
		go func() {
			w.runErr = w.s.RunInformers(w.runCtx)
			close(w.runDone)
		}()
	})
}

// runError is RunInformers' error, once it returned.
func (w *sharedWiring) runError() error {
	<-w.runDone
	if w.runErr != nil {
		return w.runErr
	}
	return errors.New("credential-use: the informers stopped")
}

// fail makes both runnables return err, once.
func (w *sharedWiring) fail(err error) {
	w.failOnce.Do(func() {
		w.failErr = err
		close(w.failed)
	})
}

// notWriting is the error of a pass in a process whose write loops haven't
// started, as on a standby whose controller doesn't need leader election:
// what a hold owes after it (the check of an A2 whose outcome is unknown, the
// re-adds, the releases) would wait for an election that may never come. The
// pass does nothing. A leader's passes may start just before its write loops;
// passes still without them sourceStartTimeout later fail the manager (§7.5;
// the user's decision, 2026-11-23).
func (w *sharedWiring) notWriting(kind string, now time.Time) error {
	if w.writing.Load() {
		return nil
	}
	w.mu.Lock()
	if w.waiting.IsZero() {
		w.waiting = now
	}
	waited := now.Sub(w.waiting)
	w.mu.Unlock()
	err := fmt.Errorf("credential-use: %s was reconciled in a process whose write loops haven't started: they need leader election, and so must its controller (leave NeedLeaderElection unset, or true)", kind)
	if waited >= sourceStartTimeout {
		w.fail(err)
	}
	return err
}

// register adds a Reconciler's source to those the write-loop runnable
// checks are started (M4).
func (w *sharedWiring) register(src *credentialUseSource) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sources = append(w.sources, src)
}

// unstartedSource returns the error of a source still unstarted
// sourceStartTimeout after elected, or after its registration when that
// came later.
func (w *sharedWiring) unstartedSource(elected, now time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, src := range w.sources {
		from := elected
		if src.registered.After(from) {
			from = src.registered
		}
		if !src.started.Load() && now.Sub(from) >= sourceStartTimeout {
			return sourceNotStarted(src.kind)
		}
	}
	return nil
}

// informerRunnable runs the Shared's informers. They only read, so they need
// no leader election, as the manager's own cache doesn't (§7.5, IP4): a
// standby's informers see holder deletions before its write loops run (M1 of
// the review of revision 16).
type informerRunnable struct{ w *sharedWiring }

var (
	_ manager.Runnable               = informerRunnable{}
	_ manager.LeaderElectionRunnable = informerRunnable{}
)

func (informerRunnable) NeedLeaderElection() bool { return false }

// Start runs the informers until ctx ends. A setup error of any Reconciler
// on the Shared, the informers failing to start (missing RBAC, a Namespace
// informer that can't sync), or an unstarted source makes it return an
// error, and so the manager refuses to start.
func (i informerRunnable) Start(ctx context.Context) error {
	w := i.w
	select {
	case <-w.failed:
		w.cancelRun()
		return w.failErr
	default:
	}
	w.startInformers()
	select {
	case <-ctx.Done():
		w.cancelRun()
		<-w.runDone
		return nil
	case <-w.runDone:
		return w.runError()
	case <-w.failed:
		w.cancelRun()
		<-w.runDone
		return w.failErr
	}
}

// writeLoopsRunnable runs the Shared's write loops (the release loop, the
// prober, Disown and the label janitor) once elected (§7.5, IP4), and checks
// that every Reconciler's source has started (M4).
type writeLoopsRunnable struct{ w *sharedWiring }

var (
	_ manager.Runnable               = writeLoopsRunnable{}
	_ manager.LeaderElectionRunnable = writeLoopsRunnable{}
)

func (writeLoopsRunnable) NeedLeaderElection() bool { return true }

// Start runs the write loops until ctx ends. It fails when a Reconciler's
// CredentialUseSource is still unstarted sourceStartTimeout after election
// (M4), or when the informer runnable fails.
func (l writeLoopsRunnable) Start(ctx context.Context) error {
	return l.run(ctx, l.w.s.RunWriteLoops)
}

func (l writeLoopsRunnable) run(ctx context.Context, writeLoops func(context.Context) error) error {
	w := l.w
	elected := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- writeLoops(ctx) }()
	w.writing.Store(true)
	check := max(sourceStartTimeout/8, 10*time.Millisecond)
	tick := time.NewTicker(check)
	defer tick.Stop()
	for {
		select {
		case err := <-errc:
			return err
		case <-w.failed:
			cancel()
			// The write loops end with ctx; anything else they report joins.
			return errors.Join(w.failErr, <-errc)
		case now := <-tick.C:
			if err := w.unstartedSource(elected, now); err != nil {
				w.fail(err)
				cancel()
				return errors.Join(err, <-errc)
			}
		}
	}
}

// The holder of a pass, on the context Connect and the ExternalClient's
// methods get, for CredentialUseCheckingReader.
type credentialUseHolderKey struct{}

type credentialUseHolder struct {
	t      *credentialuse.Tracker
	holder resource.Managed
}

func withCredentialUseHolder(ctx context.Context, t *credentialuse.Tracker, mg resource.Managed) context.Context {
	return context.WithValue(ctx, credentialUseHolderKey{}, credentialUseHolder{t: t, holder: mg})
}

// CredentialUseCheckingReader wraps the reader a provider's Connect and
// ExternalClient read Secrets and protected objects with (§11.9, A10). A read
// made during a pass, of a protected kind that the managed resource's refs
// don't name, is a provider defect, since nothing holds that object: it is
// logged once per object and counted in credentialuse_unrecorded_reads_total.
// It checks nothing outside a pass, or for a kind without credential-use, and
// it never changes what the reader returns. s maps Go types to kinds.
func CredentialUseCheckingReader(rd client.Reader, s *runtime.Scheme) client.Reader {
	return checkingReader{Reader: rd, scheme: s}
}

type checkingReader struct {
	client.Reader
	scheme *runtime.Scheme
}

func (c checkingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Reader.Get(ctx, key, obj, opts...)
	c.check(ctx, obj, key.Namespace, key.Name)
	return err
}

func (c checkingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if _, ok := ctx.Value(credentialUseHolderKey{}).(credentialUseHolder); !ok {
		return nil
	}
	return meta.EachListItem(list, func(o runtime.Object) error {
		if co, ok := o.(client.Object); ok {
			c.check(ctx, co, co.GetNamespace(), co.GetName())
		}
		return nil
	})
}

func (c checkingReader) check(ctx context.Context, obj client.Object, namespace, name string) {
	h, ok := ctx.Value(credentialUseHolderKey{}).(credentialUseHolder)
	if !ok || h.t == nil {
		return
	}
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return
	}
	h.t.CheckRead(h.holder, gvk.GroupKind(), namespace, name)
}

// unheldSuffixReasonMax bounds unheldSuffix's reason, so the suffix stays within
// 160 characters even for a hand-edited annotation (§12).
const unheldSuffixReasonMax = 16

// unheldSuffix is §12's pointer to mg's unheld refs, appended to the message
// of a pass that failed in Connect or Observe; "" when the list is empty.
func unheldSuffix(mg resource.Managed) string {
	sum, ok := credentialuse.SummarizeUnheld(mg.GetAnnotations())
	if !ok {
		return ""
	}
	n := min(sum.Count, 9999)
	plural, more := "s", fmt.Sprintf(", +%d more", n-1)
	if n == 1 {
		plural, more = "", ""
	}
	reason := sum.Reason
	if len(reason) > unheldSuffixReasonMax {
		reason = reason[:unheldSuffixReasonMax]
	}
	return fmt.Sprintf("; credential-use: %d ref%s not held (%s since %s%s), see annotation %s",
		n, plural, reason, sum.Since.UTC().Format(time.RFC3339), more, credentialuse.AnnotationUnheld)
}

// withUnheldSuffix appends unheldSuffix to err, keeping err in its chain.
func (r *Reconciler) withUnheldSuffix(err error, mg resource.Managed) error {
	if !r.cu.active() {
		return err
	}
	if s := unheldSuffix(mg); s != "" {
		return fmt.Errorf("%w%s", err, s)
	}
	return err
}

// credentialUsePass is what a pass's Acquire left for the rest of Reconcile.
type credentialUsePass struct {
	acquired credentialuse.Acquired
	// retryAfter is Acquired.RetryAfter: a live consent read failed, the pass
	// kept its hold and proceeded, and asks to come back sooner (§8 step 5,
	// IP2). deferred: the governor deferred the pass, which keeps the
	// governor's requeue.
	retryAfter time.Duration
	deferred   bool
	// refused is the refusal of an established holder's chain, which Synced's
	// message carries while it lasts (refusedSynced), or "".
	refused string
}

// refusedPrefix starts Synced's message while an established holder's refs are
// refused (§9, §12; decided 2026-11-25).
const refusedPrefix = "credential-use: refs refused, not held: "

// refusedSynced puts a refused chain on Synced, just before each status write:
// an established holder's pass proceeds, Synced=True/ReconcileSuccess, and the
// refusal lasts until the provider or the object changes, while its event
// doesn't (§9, §12; decided 2026-11-25). Only that Synced takes it; any other,
// a policy report included, is left as it is. It keeps the condition's stored
// transition time when nothing changed, so it costs no write of its own.
func refusedSynced(managed resource.Managed, start passConditions, refused string) {
	if refused == "" {
		return
	}
	c := managed.GetCondition(prv1.TypeSynced)
	if c.Status != metav1.ConditionTrue || c.Reason != prv1.ReasonReconcileSuccess {
		return
	}
	managed.SetConditions(start.keep(c.WithMessage(refusedPrefix + refused + " (§9)")))
}

// refusalOf is the refusal of an established holder's pass, on one line, or "":
// the first line of its error, which errors.Join puts first, before a failed
// gate's (credentialuse.ErrChainsNotSupported).
func refusalOf(a credentialuse.Acquired) string {
	if !a.Degraded || !errors.Is(a.Err, credentialuse.ErrChainsNotSupported) {
		return ""
	}
	line, _, _ := strings.Cut(a.Err.Error(), "\n")
	return line
}

// requeue applies retryAfter to the pass's result: a pass that ends without
// an error, a rate-limited requeue or a deferral requeues at the smaller of
// retryAfter and the time it asked for, its poll interval on most paths. It
// backs off per (holder, ref), from 1s doubling to the poll interval (§11.3).
func (p *credentialUsePass) requeue(res reconcile.Result, err error) reconcile.Result {
	if p.retryAfter <= 0 || err != nil || p.deferred || isRateLimitedRequeue(res) {
		return res
	}
	if res.RequeueAfter == 0 || res.RequeueAfter > p.retryAfter {
		res.RequeueAfter = p.retryAfter
	}
	return res
}

// errCredentialUseUnheld is the message of a pass that must not connect
// without one from the core.
const errCredentialUseUnheld = "credential-use: the object's credentials can't be held; not connecting" // #nosec G101 -- an error message, not a credential

// acquireCredentials is §8's step 5. It returns the pass's object, which an
// A1 that a webhook changed has it re-read, the context Connect and the
// ExternalClient get, carrying the resolved chain and the holder, and
// whether the pass stops there, with its result:
//   - a conflict on the record write requeues, as every conflict on the
//     object does;
//   - a new object whose acquire failed, or one of whose refs is being
//     deleted, must not connect: Synced=False/ReconcileError, Ready
//     NotObserved, and the error, so backoff applies (§11.4, §12);
//   - an established one fails open: the core logs, records
//     CredentialUseDegraded and counts it, and the pass proceeds (R4).
func (r *Reconciler) acquireCredentials(ctx, externalCtx context.Context, log logging.Logger, managed resource.Managed, pass *tracePass, cup *credentialUsePass,
	writeStatus func(error) error) (resource.Managed, context.Context, reconcile.Result, bool, error) {
	a, err := r.cu.tracker.Acquire(ctx, managed)
	if err != nil && !errors.Is(err, credentialuse.ErrHolderConflict) {
		// Acquire reports every other failure in a; this is one all the same.
		a.Err = err
		if credentialuse.IsEstablished(managed, r.cu.owners) {
			a.Degraded = true
		} else {
			a.MustNotConnect = true
		}
	}
	pass.event(tracing.EventCredentialUseAcquire, acquireAttributes(a, err)...)
	if errors.Is(err, credentialuse.ErrHolderConflict) {
		log.Debug("The credential-use record write conflicted; requeueing", "error", err)
		pass.conflict(tracing.StepCredentialUse)
		return managed, externalCtx, rateLimitedRequeue(), true, nil
	}
	if a.MustNotConnect {
		passErr := a.Err
		if passErr == nil {
			passErr = errors.New(errCredentialUseUnheld)
		}
		log.Debug("Not connecting: the object's credentials can't be held", "error", passErr)
		markNotObserved(managed, passErr)
		managed.SetConditions(prv1.ReconcileError(passErr))
		if err := writeStatus(passErr); err != nil {
			return managed, externalCtx, reconcile.Result{}, true, err
		}
		return managed, externalCtx, reconcile.Result{}, true, passErr
	}
	if a.Reread {
		// A mutating webhook or defaulting changed the object when A1 wrote
		// the record: it is read again before the pass's next write (§6.2
		// A1, L2), live when the Reconciler has a live reader. A stale read
		// only makes that write conflict.
		fresh := r.newManaged()
		rd := client.Reader(r.client)
		if r.apiReader != nil {
			rd = r.apiReader
		}
		if err := rd.Get(ctx, client.ObjectKeyFromObject(managed), fresh); err != nil {
			if kerrors.IsNotFound(err) {
				return managed, externalCtx, reconcile.Result{}, true, nil
			}
			log.Debug("Cannot read the managed resource again after the credential-use record write", "error", err)
			return managed, externalCtx, reconcile.Result{}, true, pkgerrors.Wrap(err, errGetManaged)
		}
		managed = fresh
	}
	cup.acquired = a
	cup.retryAfter = a.RetryAfter
	cup.refused = refusalOf(a)
	externalCtx = credentialuse.WithResolved(externalCtx, a)
	externalCtx = withCredentialUseHolder(externalCtx, r.cu.tracker, managed)
	return managed, externalCtx, reconcile.Result{}, false, nil
}

// removeFinalizer removes the owner finalizer with the credential-use record
// in the same write (§8 steps 4 and 7): Forget removes the record in memory
// just before RemoveFinalizer's Update. When the finalizer is already gone, so
// RemoveFinalizer would write nothing, and the record is present, the
// record's removal is an Update of its own (R12).
func (r *Reconciler) removeFinalizer(ctx context.Context, mg resource.Managed) error {
	if !r.cu.active() || !r.cu.tracker.Forget(mg) || credentialuse.IsEstablished(mg, r.cu.owners) {
		return r.managed.RemoveFinalizer(ctx, mg)
	}
	return pkgerrors.Wrap(resource.IgnoreNotFound(r.client.Update(ctx, mg)), errUpdateManaged)
}

// acquireAttributes are the credentialuse.acquire event's attributes for one
// Acquire: counts and the outcome, never a ref's name. refs is held plus
// unheld, chains followed, so a Guaranteed ref, which is neither, isn't
// counted.
func acquireAttributes(a credentialuse.Acquired, err error) []attribute.KeyValue {
	outcome := tracing.CredentialUseOK
	switch {
	case errors.Is(err, credentialuse.ErrHolderConflict):
		outcome = tracing.CredentialUseConflict
	case a.MustNotConnect:
		outcome = tracing.CredentialUseMustNotConnect
	case a.Degraded:
		outcome = tracing.CredentialUseDegraded
	}
	attrs := []attribute.KeyValue{
		tracing.AttrCredentialUseRefs.Int(len(a.Held) + len(a.Unheld)),
		tracing.AttrCredentialUseHeld.Int(len(a.Held)),
		tracing.AttrCredentialUseOutcome.String(outcome),
	}
	byReason := map[credentialuse.Reason]int{}
	for _, u := range a.Unheld {
		byReason[u.Reason]++
	}
	reasons := make([]string, 0, len(byReason))
	for r := range byReason {
		reasons = append(reasons, string(r))
	}
	slices.Sort(reasons)
	for _, r := range reasons {
		attrs = append(attrs, attribute.Int(tracing.AttrCredentialUseUnheldPrefix+r, byReason[credentialuse.Reason(r)]))
	}
	return attrs
}
