package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// Clients are what the core talks to the API server with (§7.1). It builds
// its own informers from them: it has no manager.
type Clients struct {
	// Metadata lists and watches what the informers cache, and makes the
	// protocol's reads and writes. Required.
	Metadata metadata.Interface
	// Dynamic checks this process's access before each informer starts
	// (SelfSubjectAccessReviews), and publishes the shared settings on the
	// holder kind's CRD (§7.3). Required.
	Dynamic dynamic.Interface
	// Mapper maps the holder and protected kinds to resources. Required.
	Mapper meta.RESTMapper
	// Events records §12's events on holders, protected objects and the
	// index (k8s.io/client-go/tools/events). Nil records none. The core
	// throttles them itself.
	Events events.EventRecorder
}

// SharedOptions configure the process-wide state. A later part of the build
// adds the index (§10.6).
type SharedOptions struct {
	// Listers read Guaranteed refs from the caller's cache, by kind (§9.3).
	// A Guaranteed ref of a kind without one is read live.
	Listers map[schema.GroupKind]Lister
	// Namespaces restricts the protected-object caches to these namespaces
	// (§10.5); the default is every namespace. A ref outside them is read
	// live on every pass, and its deletion is noticed by the next pass of a
	// holder that reads it (see Tracker.Acquire). The holder informers and
	// the Namespace informer stay cluster-wide: the release batches start
	// from holder events, and §5's namespace rows read every namespace.
	Namespaces []string
}

// A WatchMode is how a protected kind's holds are found (§10).
type WatchMode int

const (
	// LabelScoped: a label-scoped informer on the held label (§10.1).
	LabelScoped WatchMode = iota
	// PerName: the index ConfigMap (§10.6), for resourceNames RBAC.
	PerName
	// PerNamePoll: the index, polled (§10.7).
	PerNamePoll
)

// A ProtectedKind is a kind that Refs may name (§7.1).
type ProtectedKind struct {
	GVK   schema.GroupVersionKind
	Watch WatchMode
	// Content keeps whole objects, for refs with a Next (§10.4).
	Content bool
}

// Options configure one holder kind's tracker (§7.1).
type Options struct {
	// Holder is the kind the caller reconciles.
	Holder schema.GroupVersionKind
	// OwnerFinalizers are the accepted owner finalizers on holders: a holder
	// carrying any of them is established (§6.1). Two during a rename.
	OwnerFinalizers []string
	// Refs names what a holder reads to reach its external system.
	Refs RefsFunc
	// Protected are the kinds Refs may name; the default is Secret.
	Protected []ProtectedKind
	// MaxDepth bounds the levels of Next followed (§9.1); the default is 3.
	MaxDepth int
	// Enqueue adds a holder to the caller's queue (§10.2). Required. It
	// must not block or call Track: a Track with equal Options rebinds it,
	// and the rebind waits for the calls in flight, so that none reaches
	// the old one after Track returns (§7.3). An unbounded queue's Add, such
	// as a workqueue's, is fine.
	Enqueue func(holder types.NamespacedName)
	// DeletionGrace is §5's grace; the default is 60s.
	DeletionGrace time.Duration
	// ProbeInterval is the missing-ref prober's tick (§10.3); the default
	// is 15s.
	ProbeInterval time.Duration
	// DisownInterval and DisownPasses are Disown's pass interval and its
	// consecutive zero passes (§7.4); the defaults are 60s and 5.
	DisownInterval time.Duration
	DisownPasses   int
	// TimeoutBound is the kind's largest reconcile or request timeout.
	// Required: setup checks DisownPasses × DisownInterval against twice it,
	// and publishes it (§7.3). provider-runtime passes its timeout plus
	// grace, 90s.
	TimeoutBound time.Duration
	// Logger and Metrics are rebound with Enqueue, and the same rule holds:
	// neither may block or call Track.
	Logger  logging.Logger
	Metrics MetricsRecorder
}

// RefsFunc names what obj reads to reach its external system. It must make
// no API calls, and is called only to acquire, never to release (§7.1).
type RefsFunc func(obj metav1.Object) ([]Ref, error)

// The defaults of Options (§7.1).
const (
	DefaultDeletionGrace  = 60 * time.Second
	DefaultProbeInterval  = 15 * time.Second
	DefaultDisownInterval = 60 * time.Second
	DefaultDisownPasses   = 5
	DefaultMaxDepth       = 3
)

// secretGVK is the default protected kind.
var secretGVK = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}

// namespaceGVR is the core Namespace resource.
var namespaceGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

// errNamespaceSync is Run's error when the Namespace informer can't sync
// (§7.5, L3): the consent gate and §5's namespace rows depend on it.
var errNamespaceSync = errors.New("credential-use: cannot sync the Namespace informer: grant get, list and watch on namespaces")

// Shared is the process-wide state: one informer per protected kind, one per
// holder kind, the Namespace informer, and the (K, N) batch queue, shared by
// every tracker (§7.1). Build it once per process with NewShared. Leader
// election isn't needed for safety (§6.8).
//
// It runs in two parts, as provider-runtime's two runnables do (§7.5, IP4):
// RunInformers, read-only, needs no leader election; RunWriteLoops, the
// release loop, the prober and the label janitor, needs it (plan, PR 3b).
// Run runs both.
type Shared struct {
	c      Clients
	o      SharedOptions
	clock  sharedClock
	queue  *batchQueue
	seen   *firstSight
	events *eventSink
	// janitor queues protected objects whose labels the janitor may clean
	// (§10.1); janitorState and backoff are the janitor's and the release
	// loop's memory across attempts.
	janitor      workqueue.TypedRateLimitingInterface[ID]
	janitorState janitorState
	backoff      batchBackoff

	namespaces cache.SharedIndexInformer
	nsLW       *listWatch

	// trackWaiting counts Tracks waiting for another Track of their kind,
	// for tests.
	trackWaiting atomic.Int32

	mu sync.Mutex
	// trackers are the kinds whose Track succeeded; tracking, those a Track
	// is adding (D2 of the review of #109, and its re-review).
	trackers  map[schema.GroupKind]*Tracker
	tracking  map[schema.GroupKind]*inflight
	protected map[protKey]*protectedInformer
	setupErr  error
	// informerCtx is RunInformers' context once it runs, for Track after
	// start; nil before.
	informerCtx context.Context
	// writeCtx is RunWriteLoops' context while the write loops run, for the
	// recheck outside the caches after a Track (§10.5); nil otherwise.
	// writeWG counts those rechecks, so the write loops wait for them.
	writeCtx context.Context
	writeWG  sync.WaitGroup
	// runDone is closed when a RunInformers that started returns, so that
	// a Track after start stops waiting for informers it will never sync.
	runDone chan struct{}

	// syncTimeout, a time.Duration, bounds each informer's first sync, and
	// each of a Track's two budgets (see Track). Atomic, so that tests can
	// change it between Tracks.
	syncTimeout atomic.Int64
	// synced: RunInformers' informers have all synced; syncedCh is closed
	// then.
	synced   atomic.Bool
	syncedCh chan struct{}

	hooks hooks
}

// hooks are the tests' ways in (export_test.go). All are nil in production,
// and set before anything runs.
type hooks struct {
	// holderEvent sees every holder version a holder handler gets, before
	// it touches the verified set; protectedEvent, every protected version
	// a tracker's protected handler gets, before it acts.
	holderEvent    func(t *Tracker, m *metav1.PartialObjectMetadata)
	protectedEvent func(t *Tracker, m *metav1.PartialObjectMetadata)
	// namespaceAct sees every Namespace change the Namespace handler acts
	// on.
	namespaceAct func(n *metav1.PartialObjectMetadata)
	// beforeListRecorded runs when a holder informer's list or relist has
	// completed, just before the completion is recorded.
	beforeListRecorded func(t *Tracker)
	// wrapServer wraps the server every tracker and the janitor use.
	wrapServer func(server) server
}

// NewShared builds the process-wide state. It creates the batch queue before
// any informer starts, so that it buffers and deduplicates from the first
// event (M1 of the review of revision 16).
func NewShared(c Clients, o SharedOptions) *Shared {
	return newShared(c, o, clock.RealClock{})
}

// sharedClock is the core's clock: readings, tickers for the write loops,
// and the queue's timers. clock.RealClock is one, and so is a fake clock in
// tests.
type sharedClock interface {
	clock.WithTicker
	AfterFunc(d time.Duration, f func()) clock.Timer
}

func newShared(c Clients, o SharedOptions, clk sharedClock) *Shared {
	s := &Shared{
		c:         c,
		o:         o,
		clock:     clk,
		queue:     newBatchQueue(clk),
		janitor:   newJanitorQueue(clk),
		syncedCh:  make(chan struct{}),
		seen:      newFirstSight(),
		events:    newEventSink(c.Events, c.Mapper, clk),
		trackers:  map[schema.GroupKind]*Tracker{},
		tracking:  map[schema.GroupKind]*inflight{},
		runDone:   make(chan struct{}),
		protected: map[protKey]*protectedInformer{},
	}
	s.syncTimeout.Store(int64(2 * time.Minute))
	if c.Metadata != nil {
		s.nsLW = newListWatch(c.Metadata, namespaceGVR, "", "", clk, nil)
		s.namespaces = cache.NewSharedIndexInformerWithOptions(s.nsLW, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
			ObjectDescription: "credential-use namespaces",
		})
		_ = s.namespaces.SetTransform(transform(func(string) bool { return false }, isCredentialUseKey))
		_, _ = s.namespaces.AddEventHandler(namespaceHandler{s})
	}
	return s
}

// A protKey names one protected informer: a protected kind, and the
// namespace it is restricted to, or "" for every namespace.
type protKey struct {
	gk schema.GroupKind
	ns string
}

// protectedFor returns the protected informer whose cache covers id, or nil
// when none does: id's namespace isn't among SharedOptions.Namespaces
// (§10.5), or no tracker protects id's kind.
func (s *Shared) protectedFor(id ID) *protectedInformer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.protected[protKey{id.GroupKind(), id.Namespace}]; p != nil {
		return p
	}
	return s.protected[protKey{id.GroupKind(), ""}]
}

// cached reports whether id's namespace is covered by a protected cache,
// whether or not any tracker protects its kind yet.
func (s *Shared) cached(id ID) bool {
	return s.namespaceCached(id.Namespace)
}

// newServer is the server the protocol and the janitor call the API
// server through.
func (s *Shared) newServer() server {
	var srv server = clientServer{md: s.c.Metadata, mapper: s.c.Mapper}
	if f := s.hooks.wrapServer; f != nil {
		srv = f(srv)
	}
	return srv
}

// trackerList returns every tracker.
func (s *Shared) trackerList() []*Tracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Tracker, 0, len(s.trackers))
	for _, t := range s.trackers {
		out = append(out, t)
	}
	return out
}

func (s *Shared) protectedInformers() []*protectedInformer {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protectedInformer, 0, len(s.protected))
	for _, p := range s.protected {
		out = append(out, p)
	}
	return out
}

// Run runs the informers and, once they have synced, the write loops, until
// ctx is done: RunInformers and RunWriteLoops together, for a process that
// needs no leader election (§6.8) or holds it already. It fails at once on a
// setup error of a Track before it, and when the Namespace informer can't
// sync (§7.3, §7.5): the process then refuses to start. When either part
// fails, it stops the other, and returns the informers' error first.
func (s *Shared) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	informers, writes := make(chan error, 1), make(chan error, 1)
	go func() { informers <- s.RunInformers(ctx) }()
	go func() { writes <- s.RunWriteLoops(ctx) }()
	select {
	case err := <-informers:
		cancel()
		// The write loops end because the informers did: errInformersEnded,
		// or nil when ctx ended first, adds nothing. Anything else follows
		// the informers' error.
		if werr := <-writes; werr != nil && !errors.Is(werr, errInformersEnded) {
			return errors.Join(err, werr)
		}
		return err
	case err := <-writes:
		cancel()
		if ierr := <-informers; ierr != nil {
			return ierr
		}
		return err
	}
}

// RunInformers starts the Namespace informer and waits for it to sync, then
// starts every holder and protected informer and waits for them, and runs
// them until ctx is done. Informers a later Track adds start there and then.
// It needs no leader election: it only reads (§7.5). Before it starts an
// informer it checks that this process may list and watch what it caches.
// When ctx is done, or it fails, it shuts the batch queue down, which stops
// the grace timers (G6 of the review of #109).
func (s *Shared) RunInformers(ctx context.Context) error {
	s.mu.Lock()
	if s.setupErr != nil {
		err := s.setupErr
		s.mu.Unlock()
		return err
	}
	if s.informerCtx != nil {
		s.mu.Unlock()
		return errors.New("credential-use: the informers are already running")
	}
	if s.namespaces == nil {
		s.mu.Unlock()
		return errors.New("credential-use: Clients.Metadata is required")
	}
	s.informerCtx = ctx
	var infs []informer
	for _, t := range s.trackers {
		infs = append(infs, t.informer())
	}
	for _, p := range s.protected {
		p.runOwned = true
		infs = append(infs, p.informer())
	}
	s.mu.Unlock()
	defer close(s.runDone)
	defer s.queue.ShutDown()
	defer s.janitor.ShutDown()

	if err := s.canAccess(ctx, namespaceGVR, "", "get", "list", "watch"); err != nil {
		return fmt.Errorf("%w (%w)", errNamespaceSync, err)
	}
	for _, inf := range infs {
		if err := inf.preflight(ctx, s); err != nil {
			return err
		}
	}
	if err := s.startAndSync(ctx, ctx, s.namespaces, "the Namespace informer"); err != nil {
		if errors.Is(err, errForbidden) {
			return errNamespaceSync
		}
		return err
	}
	for _, inf := range infs {
		if err := s.startAndSync(ctx, ctx, inf.inf, inf.what); err != nil {
			return err
		}
	}
	s.synced.Store(true)
	close(s.syncedCh)
	<-ctx.Done()
	return nil
}

// HasSynced reports whether RunInformers has started every informer and
// seen each sync. A Track after that syncs its own before it returns.
func (s *Shared) HasSynced() bool { return s.synced.Load() }

// An informer is one the core starts, with the resource it lists and
// watches, for the RBAC check before it starts.
type informer struct {
	inf  cache.SharedIndexInformer
	gvr  schema.GroupVersionResource
	ns   string // the namespace it is restricted to, or ""
	what string
	// clusterList is set for a protected informer restricted to a
	// namespace (SharedOptions.Namespaces): the objects of its kind
	// outside the caches are rechecked with a cluster-wide list (§10.5).
	clusterList bool
}

// preflight checks that this process may list and watch the informer's
// resource, in its namespace or in every namespace (§7.3: missing RBAC is a
// setup error). A protected informer restricted to a namespace also needs
// list in every namespace, for §10.5's rechecks outside the caches.
func (i informer) preflight(ctx context.Context, s *Shared) error {
	if err := s.canAccess(ctx, i.gvr, i.ns, "list", "watch"); err != nil {
		return fmt.Errorf("credential-use: cannot run %s: grant list and watch on %s: %w", i.what, i.gvr.GroupResource(), err)
	}
	if i.clusterList {
		if err := s.canAccess(ctx, i.gvr, "", "list"); err != nil {
			return fmt.Errorf("credential-use: cannot run %s: SharedOptions.Namespaces restricts its cache, and the held objects outside it are rechecked with a list in every namespace (§10.5): grant list on %s in every namespace: %w", i.what, i.gvr.GroupResource(), err)
		}
	}
	return nil
}

// ssarGVR is the SelfSubjectAccessReview resource.
var ssarGVR = schema.GroupVersionResource{Group: "authorization.k8s.io", Version: "v1", Resource: "selfsubjectaccessreviews"}

// canAccess checks, before an informer starts, that this process may use
// each of verbs on gvr in every namespace, with SelfSubjectAccessReviews
// (§7.5, L3). A reflector that streams its list needs no list verb, but one
// that falls back to a List does, so a missing verb fails start either way,
// rather than on the day the fallback runs. Without Clients.Dynamic it
// can't check, and fails (N2 of the review of #109).
func (s *Shared) canAccess(ctx context.Context, gvr schema.GroupVersionResource, namespace string, verbs ...string) error {
	if s.c.Dynamic == nil {
		return errors.New("credential-use: Clients.Dynamic is required")
	}
	for _, verb := range verbs {
		review := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "authorization.k8s.io/v1",
			"kind":       "SelfSubjectAccessReview",
			"spec": map[string]any{"resourceAttributes": map[string]any{
				"verb": verb, "group": gvr.Group, "resource": gvr.Resource, "namespace": namespace,
			}},
		}}
		out, err := s.c.Dynamic.Resource(ssarGVR).Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("checking %s on %s: %w", verb, gvr.GroupResource(), err)
		}
		if allowed, _, _ := unstructured.NestedBool(out.Object, "status", "allowed"); !allowed {
			return fmt.Errorf("%s on %s isn't allowed", verb, gvr.GroupResource())
		}
	}
	return nil
}

// errForbidden marks a list or watch the API server forbade.
var errForbidden = errors.New("credential-use: forbidden")

// startAndSync starts inf under run and waits for its first sync for as
// long as wait allows, at most syncTimeout. A list or watch the server
// forbids fails it at once, rather than retrying until the timeout: missing
// RBAC is a setup error (§7.3).
func (s *Shared) startAndSync(run, wait context.Context, inf cache.SharedIndexInformer, what string) error {
	forbidden := make(chan error, 1)
	_ = inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		if apierrors.IsForbidden(err) {
			select {
			case forbidden <- err:
			default:
			}
		}
	})
	go inf.RunWithContext(run)
	return s.waitSynced(wait, inf.HasSynced, what, forbidden)
}

// timeout is syncTimeout.
func (s *Shared) timeout() time.Duration { return time.Duration(s.syncTimeout.Load()) }

// waitSynced waits until synced reports true, for at most syncTimeout. An
// error on forbidden fails it at once.
func (s *Shared) waitSynced(ctx context.Context, synced func() bool, what string, forbidden <-chan error) error {
	limit := s.timeout()
	syncCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- cache.WaitForCacheSync(syncCtx.Done(), synced) }()
	select {
	case ok := <-done:
		if !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("credential-use: %s didn't sync within %s", what, limit)
		}
		return nil
	case err := <-forbidden:
		cancel()
		return fmt.Errorf("%w: %s: %w", errForbidden, what, err)
	}
}

// setupFailed records err, so that a Run after it fails (§7.3), and returns
// it.
func (s *Shared) setupFailed(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.informerCtx == nil {
		s.setupErr = errors.Join(s.setupErr, err)
	}
	return err
}

// Track adds holder kind o.Holder (§7.3). It can be called before or after
// Run; after, it starts the kind's informers and waits for their sync
// before it returns. A setup error before Run also makes Run fail, so that
// the process refuses to start.
//
// It is idempotent per holder GroupKind. Options are equal when every field
// that isn't a function or a sink is (equalOptions). A Track with Options
// equal to the existing tracker's returns that tracker and rebinds its
// sinks, Enqueue, Logger and Metrics, to this call's: once it returns, no
// event reaches the old ones. Different Options are an error. Refs stays
// bound to the first Track: it is part of the kind's identity, since
// changing it would mean re-indexing every holder; a second Track with
// another Refs is the caller's mistake, and isn't detected. This is the
// user's decision of 2026-11-18: rediscovery rebuilds a kind's controller
// and its queue, and the latest registration's queue must get the events.
//
// Tracks of one kind run one at a time: a Track for a kind another Track is
// adding waits for it to end, then gets that tracker or, if that Track
// failed, adds the kind itself, since a failed Track leaves nothing behind
// (D2 and D3 of the review of #109). No lock is held across an API call, so
// a slow Track of one kind never delays another's.
//
// Deadlines. ctx is the caller's: when it ends, as when a manager shuts
// down, a pending Track stops at once and returns an error that wraps
// ctx.Err(). Beyond that, Track bounds itself, with two budgets of two
// minutes each. The first covers waiting for another Track of the
// same kind to end. The second covers this Track's own work: publishing the
// settings, the RBAC checks, and waiting for each informer the kind needs to
// sync. So a Track returns within about four minutes, and at once when a
// RunInformers that started has returned. A protected informer that a Track
// after Run creates is shared: it runs under RunInformers' context, not the
// Track's, and every Track that needs it waits for it on its own budget. A
// Track therefore fails only on its own budget or on a real start error,
// never because another Track ran out of time (the re-review of #109).
//
// Errors, and what to do about each:
//   - "already tracked with other options": this call's Options differ from
//     the tracker's; the caller's mistake, and retrying won't help.
//   - a setup refusal before any API call (validate's messages), or settings
//     that disagree with those published on the CRD: configuration; fix it
//     and restart. Before Run, it also makes Run fail.
//   - "grant ... on ..." or "forbidden": missing RBAC; grant it, then Track
//     again.
//   - "waiting for another Track of the kind", "didn't sync within": a
//     budget ran out, with the API server slow or unreachable; Track again
//     later.
//   - "RunInformers has ended": the informers stopped, or failed to start;
//     the process is stopping, and a Track can't succeed.
//   - "abandoned", wrapping ctx.Err() (errors.Is(err, context.Canceled) or
//     context.DeadlineExceeded): the caller's ctx ended. Nothing is
//     recorded against Run, so a Track cancelled before Run doesn't stop
//     the process from starting.
//
// A failed Track leaves nothing behind, so Tracking again is always safe.
func (s *Shared) Track(ctx context.Context, o Options) (*Tracker, error) {
	o, err := s.validate(o)
	if err != nil {
		return nil, s.setupFailed(err)
	}
	gk := o.Holder.GroupKind()
	if err := ctx.Err(); err != nil {
		return nil, s.abandonedOr(ctx, gk, err)
	}

	wait, cancelWait := s.trackCtx(ctx)
	defer cancelWait()

	var mine *inflight
	for mine == nil {
		s.mu.Lock()
		if t := s.trackers[gk]; t != nil {
			s.mu.Unlock()
			if !equalOptions(t.o, o) {
				return nil, fmt.Errorf("credential-use: %s is already tracked with other options", gk)
			}
			t.rebind(o)
			return t, nil
		}
		other := s.tracking[gk]
		if other == nil {
			mine = &inflight{done: make(chan struct{})}
			s.tracking[gk] = mine
		}
		s.mu.Unlock()
		if other != nil {
			s.trackWaiting.Add(1)
			select {
			case <-other.done:
				s.trackWaiting.Add(-1)
			case <-wait.Done():
				s.trackWaiting.Add(-1)
				return nil, s.abandonedOr(ctx, gk, s.trackFailed(gk, fmt.Errorf("waiting for another Track of the kind: %w", wait.Err())))
			}
		}
	}
	defer s.endTracking(gk, mine)
	work, cancel := s.trackCtx(ctx)
	defer cancel()
	t, err := s.track(ctx, work, o, mine)
	if err != nil {
		return nil, s.abandonedOr(ctx, gk, err)
	}
	return t, nil
}

// abandonedOr is err, or, when the caller's ctx has ended, the error that
// says so and wraps ctx.Err().
func (s *Shared) abandonedOr(ctx context.Context, gk schema.GroupKind, err error) error {
	if ctx.Err() == nil {
		return err
	}
	return fmt.Errorf("credential-use: tracking %s was abandoned: %w (%v)", gk, ctx.Err(), err)
}

// trackCtx is one of a Track's budgets: syncTimeout under the caller's
// parent, ended at once when a RunInformers that started returns.
func (s *Shared) trackCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, s.timeout())
	go func() {
		select {
		case <-s.runDone:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// An inflight is a Track adding its kind: done is closed when it ends, and
// t is its tracker once built, for tests. parked holds the release batches
// of its kind that a release worker took while it ran (the review of #114):
// a Track after start replays the shared caches to its handlers, and those
// batches, the recheck of every hold already cached, reach the queue before
// the kind is in s.trackers.
type inflight struct {
	done   chan struct{}
	t      *Tracker
	parked []parkedBatch
}

// A parkedBatch is a release batch held while its kind's Track runs.
type parkedBatch struct {
	key batchKey
	ids []ID
}

// endTracking ends the Track of gk that mine records: the release batches
// parked while it ran go back to the queue if it tracked the kind, and are
// dropped if it failed, which leaves nothing behind (§7.3); a later Track
// replays the caches again. Only then can another Track of gk start.
func (s *Shared) endTracking(gk schema.GroupKind, mine *inflight) {
	s.mu.Lock()
	delete(s.tracking, gk)
	parked := mine.parked
	mine.parked = nil
	t := s.trackers[gk]
	// While the write loops run, a kind tracked now needs the recheck
	// outside the caches that their start gave the kinds tracked before
	// (§10.5); the replay of the caches covers the cached objects.
	wctx := s.writeCtx
	if wctx != nil && wctx.Err() != nil {
		wctx = nil // the write loops are stopping
	}
	if t != nil && wctx != nil {
		s.writeWG.Add(1)
	}
	s.mu.Unlock()
	if t != nil {
		for _, b := range parked {
			s.queue.Add(b.key, b.ids...)
		}
		if wctx != nil {
			go func() {
				defer s.writeWG.Done()
				s.recheckUncached(wctx, []*Tracker{t})
			}()
		}
	}
	close(mine.done)
}

// trackerOrPark returns the tracker of k's holder kind. While a Track of
// that kind runs, it parks the batch (k, ids) with that Track instead and
// returns nil, as it does for a kind nobody tracks or is tracking.
func (s *Shared) trackerOrPark(k batchKey, ids []ID) *Tracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.trackers[k.Holder]; t != nil {
		return t
	}
	if in := s.tracking[k.Holder]; in != nil {
		in.parked = append(in.parked, parkedBatch{key: k, ids: append([]ID(nil), ids...)})
	}
	return nil
}

// trackFailed wraps err, the failure of a Track of gk, naming the end of
// RunInformers when that is what stopped it.
func (s *Shared) trackFailed(gk schema.GroupKind, err error) error {
	select {
	case <-s.runDone:
		return fmt.Errorf("credential-use: tracking %s: RunInformers has ended: %w", gk, err)
	default:
		return fmt.Errorf("credential-use: tracking %s: %w", gk, err)
	}
}

// track adds kind o.Holder, which no other Track is adding, within ctx, its
// budget under the caller's caller.
func (s *Shared) track(caller, ctx context.Context, o Options, mine *inflight) (*Tracker, error) {
	gk := o.Holder.GroupKind()
	// setupFailed records a failure against Run, unless the caller gave up.
	setupFailed := func(err error) error {
		if caller.Err() != nil {
			return err
		}
		return s.setupFailed(err)
	}
	if err := s.publishConfig(ctx, o); err != nil {
		return nil, setupFailed(err)
	}
	t, err := s.newTracker(o)
	if err != nil {
		return nil, setupFailed(err)
	}
	t.uncached = s.computeUncachedKinds(t)
	// Build the protected informers that may be missing before taking mu:
	// the mapper may call the API server.
	var keys []protKey
	built := map[protKey]*protectedInformer{}
	for _, pk := range o.Protected {
		ps, err := s.newProtectedInformers(pk)
		if err != nil {
			return nil, setupFailed(err)
		}
		for _, p := range ps {
			keys = append(keys, p.key())
			built[p.key()] = p
		}
	}

	// Claim each protected informer, installing the missing ones. A claimed
	// informer stays while any Track or tracker claims it.
	s.mu.Lock()
	mine.t = t
	var ps []*protectedInformer
	for _, k := range keys {
		p := s.protected[k]
		if p == nil {
			p = built[k]
			s.protected[k] = p
		}
		if p.content != built[k].content {
			// One informer caches a kind for every tracker, so they must
			// agree on whether it keeps content (§10.4).
			s.releaseLocked(ps)
			s.mu.Unlock()
			return nil, setupFailed(fmt.Errorf("credential-use: %s: protected kind %s has Content %v, but another holder kind tracks it with Content %v",
				gk, k.gk, built[k].content, p.content))
		}
		p.claims++
		ps = append(ps, p)
	}
	ictx := s.informerCtx
	if ictx == nil {
		// RunInformers starts every informer, and each initial list reaches
		// t's handlers.
		for _, p := range ps {
			if err := t.join(p); err != nil {
				t.leaveAll()
				s.releaseLocked(ps)
				s.mu.Unlock()
				return nil, setupFailed(err)
			}
		}
		s.trackers[gk] = t
		s.mu.Unlock()
		return t, nil
	}
	s.mu.Unlock()

	if err := s.startTracker(ctx, ictx, t, ps); err != nil {
		// Roll back every piece of state this call created (D3 of the
		// review of #109): the informers it started stop, its handlers
		// leave the informers it shares, and it gives up its claims; an
		// informer nobody claims any more goes.
		t.stopInformers()
		t.leaveAll()
		s.mu.Lock()
		stops := s.releaseLocked(ps)
		s.mu.Unlock()
		for _, stop := range stops {
			stop()
		}
		t.dropHeld()
		return nil, s.trackFailed(gk, err)
	}
	s.mu.Lock()
	s.trackers[gk] = t
	s.mu.Unlock()
	return t, nil
}

// releaseLocked gives up one claim on each of ps, removes those nobody
// claims any more and RunInformers doesn't run, and returns their stops.
// s.mu is held.
func (s *Shared) releaseLocked(ps []*protectedInformer) []context.CancelFunc {
	var stops []context.CancelFunc
	for _, p := range ps {
		p.claims--
		if p.claims > 0 || p.runOwned {
			continue
		}
		if s.protected[p.key()] == p {
			delete(s.protected, p.key())
		}
		if p.stop != nil {
			stops = append(stops, p.stop)
		}
	}
	return stops
}

// startTracker, under the running RunInformers' context ictx: starts t's
// holder informer, after checking it may list and watch holders, and waits
// for its sync; then, for each of t's protected informers, starts it if
// nobody has, joins t's handler to it, the running ones replaying their
// caches to it (D4 of the review of #109), and waits for that. t joins
// only once its holder index is full, so that an object leaving a cache
// enqueues its holders from a full index (the re-review of #109). It
// gives up when ctx ends.
func (s *Shared) startTracker(ctx, ictx context.Context, t *Tracker, ps []*protectedInformer) error {
	if err := t.informer().preflight(ctx, s); err != nil {
		return err
	}
	hctx, stop := context.WithCancel(ictx)
	t.stopHolders = stop
	if err := s.startAndSync(hctx, ctx, t.holders, "the holder informer of "+t.gk.String()); err != nil {
		return err
	}
	for _, p := range ps {
		if err := p.ensureStarted(ctx, ictx, s); err != nil {
			return err
		}
		if err := t.join(p); err != nil {
			return err
		}
	}
	for _, p := range ps {
		if err := s.waitSynced(ctx, t.regs[p].HasSynced, "the handler of "+t.gk.String()+" on "+p.what(), nil); err != nil {
			return err
		}
	}
	return nil
}

// setStop records stop as what ends p: releaseLocked calls it once nobody
// claims p, and the end of RunInformers' context ends p anyway.
func (s *Shared) setStop(p *protectedInformer, stop context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p.stop = stop
}

// ensureStarted has p started, if neither RunInformers nor another Track
// has started it, and waits for its first sync on ctx, this Track's budget.
// Every Track that needs p gets the result of p's start, and none the
// deadline of another.
func (p *protectedInformer) ensureStarted(ctx, ictx context.Context, s *Shared) error {
	s.mu.Lock()
	if p.runOwned {
		s.mu.Unlock()
		return nil
	}
	starter := !p.starting
	p.starting = true
	s.mu.Unlock()
	if starter {
		s.startShared(ictx, p)
	}
	return s.waitShared(ctx, p)
}

// startShared starts p for every Track that needs it, under RunInformers'
// context ictx rather than a Track's (the re-review of #109). Its RBAC check
// has syncTimeout of its own. A refused check, or a list or watch the server
// forbids before p's first sync, is p's start error: every Track that
// claimed p fails with it, and p is removed and stopped when the last of
// them lets go, so that a Track after the grant starts a fresh informer. A
// 403 after the first sync isn't a start error: the reflector retries, as
// for any watch error, and p recovers once the grant is back (the final
// review of #109).
func (s *Shared) startShared(ictx context.Context, p *protectedInformer) {
	pctx, pstop := context.WithCancel(ictx)
	s.setStop(p, pstop)
	_ = p.inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		if apierrors.IsForbidden(err) && !p.inf.HasSynced() {
			p.fail(fmt.Errorf("%w: %s: %w", errForbidden, p.what(), err))
		}
	})
	go func() {
		check, cancel := context.WithTimeout(pctx, s.timeout())
		err := p.informer().preflight(check, s)
		cancel()
		if err != nil {
			p.fail(err)
			return
		}
		p.inf.RunWithContext(pctx)
	}()
}

// fail records err as p's start error, once.
func (p *protectedInformer) fail(err error) {
	p.failOnce.Do(func() {
		p.startErr = err
		close(p.failed)
	})
}

// waitShared waits for p's first sync for as long as ctx allows, or until
// p's start fails.
func (s *Shared) waitShared(ctx context.Context, p *protectedInformer) error {
	done := make(chan bool, 1)
	go func() { done <- cache.WaitForCacheSync(ctx.Done(), p.inf.HasSynced) }()
	select {
	case ok := <-done:
		if ok {
			return nil
		}
		select {
		case <-p.failed:
			return p.startErr
		default:
		}
		return fmt.Errorf("credential-use: %s didn't sync within this Track's budget: %w", p.what(), ctx.Err())
	case <-p.failed:
		return p.startErr
	}
}

// validate checks o as §7.3 says, except for the PerName items, which come
// with that mode, and fills in the defaults.
func (s *Shared) validate(o Options) (Options, error) {
	gk := o.Holder.GroupKind()
	if _, err := FinalizerPrefix(gk); err != nil {
		return o, err
	}
	if s.c.Mapper == nil || s.c.Metadata == nil || s.c.Dynamic == nil {
		return o, errors.New("credential-use: Clients.Metadata, Clients.Dynamic and Clients.Mapper are required")
	}
	if _, err := s.c.Mapper.RESTMapping(gk, o.Holder.Version); err != nil {
		return o, fmt.Errorf("credential-use: the holder kind %s can't be mapped: %w", o.Holder, err)
	}
	if len(o.OwnerFinalizers) == 0 {
		return o, fmt.Errorf("credential-use: %s: OwnerFinalizers is empty", gk)
	}
	if o.Refs == nil {
		return o, fmt.Errorf("credential-use: %s: Refs is required", gk)
	}
	if o.Enqueue == nil {
		return o, fmt.Errorf("credential-use: %s: Enqueue is required (§7.2)", gk)
	}
	if o.TimeoutBound <= 0 {
		return o, fmt.Errorf("credential-use: %s: TimeoutBound is required (§7.3)", gk)
	}
	if o.DeletionGrace == 0 {
		o.DeletionGrace = DefaultDeletionGrace
	}
	if o.ProbeInterval == 0 {
		o.ProbeInterval = DefaultProbeInterval
	}
	if o.DisownInterval == 0 {
		o.DisownInterval = DefaultDisownInterval
	}
	if o.DisownPasses == 0 {
		o.DisownPasses = DefaultDisownPasses
	}
	if o.MaxDepth == 0 {
		o.MaxDepth = DefaultMaxDepth
	}
	if o.DeletionGrace < 0 || o.ProbeInterval < 0 || o.DisownInterval < 0 || o.DisownPasses < 0 || o.MaxDepth < 0 {
		return o, fmt.Errorf("credential-use: %s: a negative option", gk)
	}
	if time.Duration(o.DisownPasses)*o.DisownInterval < 2*o.TimeoutBound {
		return o, fmt.Errorf("credential-use: %s: DisownPasses × DisownInterval (%d × %s) is below twice TimeoutBound (%s) (§7.4)",
			gk, o.DisownPasses, o.DisownInterval, o.TimeoutBound)
	}
	if len(o.Protected) == 0 {
		o.Protected = []ProtectedKind{{GVK: secretGVK}}
	}
	kinds := map[schema.GroupKind]bool{}
	for _, pk := range o.Protected {
		if kinds[pk.GVK.GroupKind()] {
			return o, fmt.Errorf("credential-use: %s: protected kind %s is listed twice", gk, pk.GVK.GroupKind())
		}
		kinds[pk.GVK.GroupKind()] = true
		switch {
		case pk.Watch != LabelScoped:
			return o, fmt.Errorf("credential-use: %s: protected kind %s: only LabelScoped is supported so far", gk, pk.GVK)
		}
		if _, err := s.c.Mapper.RESTMapping(pk.GVK.GroupKind(), pk.GVK.Version); err != nil {
			return o, fmt.Errorf("credential-use: %s: the protected kind %s can't be mapped: %w", gk, pk.GVK, err)
		}
	}
	if o.Logger == nil {
		o.Logger = logging.NewNopLogger()
	}
	if o.Metrics == nil {
		o.Metrics = NopMetrics{}
	}
	return o, nil
}

// equalOptions reports whether a and b, both validated, configure the same
// kind: every field that isn't a function or a sink is equal, with
// OwnerFinalizers and Protected compared as sets (N1 of the review of #109,
// the user's decision of 2026-11-18). Refs, Enqueue, Logger and Metrics
// aren't compared: see Track.
func equalOptions(a, b Options) bool {
	return a.Holder == b.Holder &&
		sameSet(a.OwnerFinalizers, b.OwnerFinalizers) &&
		sameSet(a.Protected, b.Protected) &&
		a.MaxDepth == b.MaxDepth && a.DeletionGrace == b.DeletionGrace &&
		a.ProbeInterval == b.ProbeInterval && a.DisownInterval == b.DisownInterval &&
		a.DisownPasses == b.DisownPasses && a.TimeoutBound == b.TimeoutBound
}

// sameSet reports whether a and b hold the same elements, ignoring order
// and repetition.
func sameSet[T comparable](a, b []T) bool {
	as, bs := map[T]bool{}, map[T]bool{}
	for _, x := range a {
		as[x] = true
	}
	for _, x := range b {
		bs[x] = true
	}
	return maps.Equal(as, bs)
}

// newProtectedInformers builds kind gvk's protected informers: one per
// namespace of SharedOptions.Namespaces for a namespaced kind, when that is
// set (§10.5), and otherwise one for every namespace.
func (s *Shared) newProtectedInformers(pk ProtectedKind) ([]*protectedInformer, error) {
	gvk := pk.GVK
	m, err := s.c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	namespaces := []string{""}
	if len(s.o.Namespaces) > 0 && m.Scope.Name() == meta.RESTScopeNameNamespace {
		namespaces = s.o.Namespaces
	}
	var out []*protectedInformer
	for _, ns := range namespaces {
		p := &protectedInformer{gk: gvk.GroupKind(), ns: ns, gvr: m.Resource, content: pk.Content, failed: make(chan struct{})}
		var example runtime.Object = &metav1.PartialObjectMetadata{}
		tf := transform(isCredentialUseKey, isCredentialUseKey)
		if pk.Content {
			p.lw = newContentListWatch(s.c.Dynamic, m.Resource, ns, heldSelector(), s.clock)
			example, tf = &unstructured.Unstructured{}, contentTransform
		} else {
			p.lw = newListWatch(s.c.Metadata, m.Resource, ns, heldSelector(), s.clock, nil)
		}
		p.inf = cache.NewSharedIndexInformerWithOptions(p.lw, example, cache.SharedIndexInformerOptions{
			Indexers:          cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			ObjectDescription: "credential-use " + p.what(),
		})
		if err := p.inf.SetTransform(tf); err != nil {
			return nil, err
		}
		_, _ = p.inf.AddEventHandler(janitorHandler{s, p})
		out = append(out, p)
	}
	return out, nil
}

func (p *protectedInformer) key() protKey { return protKey{p.gk, p.ns} }

// what names p in messages.
func (p *protectedInformer) what() string {
	if p.ns == "" {
		return "the protected informer of " + p.gk.String()
	}
	return "the protected informer of " + p.gk.String() + " in namespace " + p.ns
}

func (p *protectedInformer) informer() informer {
	return informer{inf: p.inf, gvr: p.gvr, ns: p.ns, what: p.what(), clusterList: p.ns != ""}
}
