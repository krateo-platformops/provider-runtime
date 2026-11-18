package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
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

// SharedOptions configure the process-wide state. Later parts of the build
// add the namespace restriction (§10.5), the listers for Guaranteed refs
// (§9.3) and the index (§10.6).
type SharedOptions struct{}

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
	// Enqueue adds a holder to the caller's queue (§10.2). Required.
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
	Logger       logging.Logger
	Metrics      MetricsRecorder
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

	namespaces cache.SharedIndexInformer
	nsLW       *listWatch

	// trackMu runs one Track at a time (D2 of the review of #109). Only
	// Track changes trackers and protected, so a Track sees them stay as it
	// read them; it takes mu inside trackMu to change them.
	trackMu sync.Mutex
	// trackWaiting counts Tracks waiting for trackMu, for tests.
	trackWaiting atomic.Int32

	mu        sync.Mutex
	trackers  map[schema.GroupKind]*Tracker
	protected map[schema.GroupKind]*protectedInformer
	setupErr  error
	// informerCtx is RunInformers' context once it runs, for Track after
	// start; nil before.
	informerCtx context.Context

	// syncTimeout bounds each informer's first sync.
	syncTimeout time.Duration
	// synced: RunInformers' informers have all synced.
	synced atomic.Bool

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
		c:           c,
		o:           o,
		clock:       clk,
		queue:       newBatchQueue(clk),
		seen:        newFirstSight(),
		events:      newEventSink(c.Events, c.Mapper, clk),
		trackers:    map[schema.GroupKind]*Tracker{},
		protected:   map[schema.GroupKind]*protectedInformer{},
		syncTimeout: 2 * time.Minute,
	}
	if c.Metadata != nil {
		s.nsLW = newListWatch(c.Metadata, namespaceGVR, "", clk, nil)
		s.namespaces = cache.NewSharedIndexInformerWithOptions(s.nsLW, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
			ObjectDescription: "credential-use namespaces",
		})
		_ = s.namespaces.SetTransform(transform(func(string) bool { return false }, isCredentialUseKey))
		_, _ = s.namespaces.AddEventHandler(namespaceHandler{s})
	}
	return s
}

func (s *Shared) protectedInformer(gk schema.GroupKind) *protectedInformer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.protected[gk]
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
// ctx is done. It fails at once on a setup error of a Track before it, and
// when the Namespace informer can't sync (§7.3, §7.5): the process then
// refuses to start.
//
// The write loops come with the next part of the build (plan, PR 3b); until
// then Run runs the informers.
func (s *Shared) Run(ctx context.Context) error { return s.RunInformers(ctx) }

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
		infs = append(infs, p.informer())
	}
	s.mu.Unlock()
	defer s.queue.ShutDown()

	if err := s.canAccess(ctx, namespaceGVR, "get", "list", "watch"); err != nil {
		return fmt.Errorf("%w (%w)", errNamespaceSync, err)
	}
	for _, inf := range infs {
		if err := inf.preflight(ctx, s); err != nil {
			return err
		}
	}
	if err := s.startAndSync(ctx, s.namespaces, "the Namespace informer"); err != nil {
		if errors.Is(err, errForbidden) {
			return errNamespaceSync
		}
		return err
	}
	for _, inf := range infs {
		if err := s.startAndSync(ctx, inf.inf, inf.what); err != nil {
			return err
		}
	}
	s.synced.Store(true)
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
	what string
}

// preflight checks that this process may list and watch the informer's
// resource in every namespace (§7.3: missing RBAC is a setup error).
func (i informer) preflight(ctx context.Context, s *Shared) error {
	if err := s.canAccess(ctx, i.gvr, "list", "watch"); err != nil {
		return fmt.Errorf("credential-use: cannot run %s: grant list and watch on %s: %w", i.what, i.gvr.GroupResource(), err)
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
func (s *Shared) canAccess(ctx context.Context, gvr schema.GroupVersionResource, verbs ...string) error {
	if s.c.Dynamic == nil {
		return errors.New("credential-use: Clients.Dynamic is required")
	}
	for _, verb := range verbs {
		review := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "authorization.k8s.io/v1",
			"kind":       "SelfSubjectAccessReview",
			"spec": map[string]any{"resourceAttributes": map[string]any{
				"verb": verb, "group": gvr.Group, "resource": gvr.Resource,
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

// startAndSync starts inf under ctx and waits for its first sync. A list or
// watch the server forbids fails it at once, rather than retrying until the
// timeout: missing RBAC is a setup error (§7.3).
func (s *Shared) startAndSync(ctx context.Context, inf cache.SharedIndexInformer, what string) error {
	forbidden := make(chan error, 1)
	_ = inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		if apierrors.IsForbidden(err) {
			select {
			case forbidden <- err:
			default:
			}
		}
	})
	go inf.RunWithContext(ctx)
	return s.waitSynced(ctx, inf.HasSynced, what, forbidden)
}

// waitSynced waits until synced reports true, for at most syncTimeout. An
// error on forbidden fails it at once.
func (s *Shared) waitSynced(ctx context.Context, synced func() bool, what string, forbidden <-chan error) error {
	syncCtx, cancel := context.WithTimeout(ctx, s.syncTimeout)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- cache.WaitForCacheSync(syncCtx.Done(), synced) }()
	select {
	case ok := <-done:
		if !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("credential-use: %s didn't sync within %s", what, s.syncTimeout)
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

// Track adds holder kind o.Holder (§7.3). It is idempotent per holder
// GroupKind: equal Options return the existing tracker, and different ones
// an error. It can be called before or after Run; after, it starts the
// kind's informers and waits for their sync before it returns. A setup error
// before Run also makes Run fail, so that the process refuses to start.
//
// Tracks run one at a time. A Track for a kind that another Track is adding
// waits for it to end: it then gets that tracker, once it has synced, or,
// if that Track failed, adds the kind itself, since a failed Track leaves
// nothing behind (D2 and D3 of the review of #109).
func (s *Shared) Track(o Options) (*Tracker, error) {
	o, err := s.validate(o)
	if err != nil {
		return nil, s.setupFailed(err)
	}
	gk := o.Holder.GroupKind()

	s.trackWaiting.Add(1)
	s.trackMu.Lock()
	s.trackWaiting.Add(-1)
	defer s.trackMu.Unlock()

	s.mu.Lock()
	prev := s.trackers[gk]
	existing := maps.Clone(s.protected)
	s.mu.Unlock()
	if prev != nil {
		if !equalOptions(prev.o, o) {
			return nil, fmt.Errorf("credential-use: %s is already tracked with other options", gk)
		}
		return prev, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.syncTimeout)
	defer cancel()
	if err := s.publishConfig(ctx, o); err != nil {
		return nil, s.setupFailed(err)
	}

	// Build everything new before changing what other goroutines see, so
	// that a failure here leaves nothing behind.
	t, err := s.newTracker(o)
	if err != nil {
		return nil, s.setupFailed(err)
	}
	fresh := map[schema.GroupKind]*protectedInformer{}
	var joined []*protectedInformer // every protected informer of t's, once
	for _, pk := range o.Protected {
		pgk := pk.GVK.GroupKind()
		p := existing[pgk]
		if p == nil {
			if p, err = s.newProtectedInformer(pk.GVK); err != nil {
				return nil, s.setupFailed(err)
			}
			fresh[pgk] = p
		}
		joined = append(joined, p)
	}

	s.mu.Lock()
	ictx := s.informerCtx
	if ictx == nil {
		// RunInformers starts every informer, and each initial list reaches
		// t's handlers.
		for _, p := range joined {
			if err := t.join(p); err != nil {
				t.leaveAll()
				s.mu.Unlock()
				return nil, s.setupFailed(err)
			}
		}
		s.trackers[gk] = t
		maps.Copy(s.protected, fresh)
		s.mu.Unlock()
		return t, nil
	}
	s.trackers[gk] = t
	maps.Copy(s.protected, fresh)
	s.mu.Unlock()

	if err := s.startTracker(ictx, t, fresh, joined); err != nil {
		// Roll back every piece of state this call created (D3 of the
		// review of #109): the informers it started stop, its handlers
		// leave the informers it shares, and the maps lose its entries.
		t.stopInformers()
		for _, p := range fresh {
			p.stop()
		}
		t.leaveAll()
		s.mu.Lock()
		delete(s.trackers, gk)
		for pgk := range fresh {
			delete(s.protected, pgk)
		}
		s.mu.Unlock()
		t.dropHeld()
		return nil, err
	}
	return t, nil
}

// startTracker starts, under the running RunInformers' context ictx, t's
// holder informer and the protected informers this Track created (fresh),
// after checking it may list and watch what they cache; joins t's handlers
// to every protected informer of t's (joined), the shared ones replaying
// their caches to it (D4 of the review of #109); and waits for all of it to
// sync.
func (s *Shared) startTracker(ictx context.Context, t *Tracker, fresh map[schema.GroupKind]*protectedInformer, joined []*protectedInformer) error {
	starting := []informer{t.informer()}
	for _, p := range fresh {
		starting = append(starting, p.informer())
	}
	for _, inf := range starting {
		if err := inf.preflight(ictx, s); err != nil {
			return err
		}
	}
	for _, p := range joined {
		if err := t.join(p); err != nil {
			return err
		}
	}
	hctx, stop := context.WithCancel(ictx)
	t.stopHolders = stop
	if err := s.startAndSync(hctx, t.holders, "the holder informer of "+t.gk.String()); err != nil {
		return err
	}
	for _, p := range fresh {
		pctx, pstop := context.WithCancel(ictx)
		p.stop = pstop
		if err := s.startAndSync(pctx, p.inf, "the protected informer of "+p.gk.String()); err != nil {
			return err
		}
	}
	for _, p := range joined {
		if err := s.waitSynced(ictx, t.regs[p].HasSynced, "the handler of "+t.gk.String()+" on "+p.gk.String(), nil); err != nil {
			return err
		}
	}
	return nil
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
		case pk.Content:
			return o, fmt.Errorf("credential-use: %s: protected kind %s: Content comes with chains (plan, PR 4)", gk, pk.GVK)
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

// equalOptions reports whether a and b, both validated, are the same
// configuration: the same values, and the same functions.
func equalOptions(a, b Options) bool {
	same := func(f, g any) bool { return fmt.Sprintf("%p", f) == fmt.Sprintf("%p", g) }
	return a.Holder == b.Holder &&
		slices.Equal(a.OwnerFinalizers, b.OwnerFinalizers) &&
		slices.Equal(a.Protected, b.Protected) &&
		a.MaxDepth == b.MaxDepth && a.DeletionGrace == b.DeletionGrace &&
		a.ProbeInterval == b.ProbeInterval && a.DisownInterval == b.DisownInterval &&
		a.DisownPasses == b.DisownPasses && a.TimeoutBound == b.TimeoutBound &&
		same(a.Refs, b.Refs) && same(a.Enqueue, b.Enqueue)
}

func (s *Shared) newProtectedInformer(gvk schema.GroupVersionKind) (*protectedInformer, error) {
	m, err := s.c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	p := &protectedInformer{gk: gvk.GroupKind(), gvr: m.Resource, stop: func() {}}
	p.lw = newListWatch(s.c.Metadata, m.Resource, heldSelector(), s.clock, nil)
	p.inf = cache.NewSharedIndexInformerWithOptions(p.lw, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
		Indexers:          cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		ObjectDescription: "credential-use protected " + gvk.GroupKind().String(),
	})
	if err := p.inf.SetTransform(transform(isCredentialUseKey, isCredentialUseKey)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *protectedInformer) informer() informer {
	return informer{inf: p.inf, gvr: p.gvr, what: "the protected informer of " + p.gk.String()}
}
