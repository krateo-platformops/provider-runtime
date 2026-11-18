package credentialuse

import (
	"context"
	"errors"
	"fmt"
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
	Metadata metadata.Interface
	Dynamic  dynamic.Interface
	Mapper   meta.RESTMapper
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

	// For tests: onHolderEvent sees every holder version a handler gets,
	// before it touches the verified set; onNamespaceAct, every Namespace
	// change the handler acts on.
	onHolderEvent  func(t *Tracker, m *metav1.PartialObjectMetadata)
	onNamespaceAct func(namespace string)
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
		s.nsLW = newListWatch(c.Metadata.Resource(namespaceGVR), "", clk, nil)
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
// It needs no leader election: it only reads (§7.5).
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
	var infs []cache.SharedIndexInformer
	for _, t := range s.trackers {
		infs = append(infs, t.holders)
	}
	for _, p := range s.protected {
		infs = append(infs, p.inf)
	}
	s.mu.Unlock()

	if err := s.canWatchNamespaces(ctx); err != nil {
		return err
	}
	if err := s.startAndSync(ctx, s.namespaces, "the Namespace informer"); err != nil {
		if errors.Is(err, errForbidden) {
			return errNamespaceSync
		}
		return err
	}
	for _, inf := range infs {
		if err := s.startAndSync(ctx, inf, "an informer"); err != nil {
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

// ssarGVR is the SelfSubjectAccessReview resource.
var ssarGVR = schema.GroupVersionResource{Group: "authorization.k8s.io", Version: "v1", Resource: "selfsubjectaccessreviews"}

// canWatchNamespaces checks, before the Namespace informer starts, that this
// process may get, list and watch namespaces (§7.5, L3). A reflector that
// streams its list needs no list verb, but one that falls back to a List
// does, so a missing verb fails start either way, rather than on the day the
// fallback runs.
func (s *Shared) canWatchNamespaces(ctx context.Context) error {
	if s.c.Dynamic == nil {
		return nil
	}
	for _, verb := range []string{"get", "list", "watch"} {
		review := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "authorization.k8s.io/v1",
			"kind":       "SelfSubjectAccessReview",
			"spec": map[string]any{"resourceAttributes": map[string]any{
				"verb": verb, "resource": "namespaces",
			}},
		}}
		out, err := s.c.Dynamic.Resource(ssarGVR).Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("%w (checking %s: %w)", errNamespaceSync, verb, err)
		}
		if allowed, _, _ := unstructured.NestedBool(out.Object, "status", "allowed"); !allowed {
			return fmt.Errorf("%w (%s isn't allowed)", errNamespaceSync, verb)
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
	syncCtx, cancel := context.WithTimeout(ctx, s.syncTimeout)
	defer cancel()
	synced := make(chan bool, 1)
	go func() { synced <- cache.WaitForCacheSync(syncCtx.Done(), inf.HasSynced) }()
	select {
	case ok := <-synced:
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
func (s *Shared) Track(o Options) (*Tracker, error) {
	o, err := s.validate(o)
	if err != nil {
		return nil, s.setupFailed(err)
	}
	gk := o.Holder.GroupKind()

	s.mu.Lock()
	if t, ok := s.trackers[gk]; ok {
		s.mu.Unlock()
		if !equalOptions(t.o, o) {
			return nil, fmt.Errorf("credential-use: %s is already tracked with other options", gk)
		}
		return t, nil
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.syncTimeout)
	defer cancel()
	if err := s.publishConfig(ctx, o); err != nil {
		return nil, s.setupFailed(err)
	}

	t, err := s.newTracker(o)
	if err != nil {
		return nil, s.setupFailed(err)
	}

	s.mu.Lock()
	if prev, ok := s.trackers[gk]; ok { // a concurrent Track won
		s.mu.Unlock()
		if !equalOptions(prev.o, o) {
			return nil, fmt.Errorf("credential-use: %s is already tracked with other options", gk)
		}
		return prev, nil
	}
	s.trackers[gk] = t
	var fresh []cache.SharedIndexInformer
	fresh = append(fresh, t.holders)
	for _, pk := range o.Protected {
		pgk := pk.GVK.GroupKind()
		p, ok := s.protected[pgk]
		if !ok {
			p, err = s.newProtectedInformer(pk.GVK)
			if err != nil {
				delete(s.trackers, gk)
				s.mu.Unlock()
				return nil, s.setupFailed(err)
			}
			s.protected[pgk] = p
			fresh = append(fresh, p.inf)
		}
		p.add(t)
	}
	ictx := s.informerCtx
	s.mu.Unlock()

	if ictx != nil {
		for _, inf := range fresh {
			if err := s.startAndSync(ictx, inf, "an informer of "+gk.String()); err != nil {
				return nil, err
			}
		}
	}
	return t, nil
}

// validate checks o as §7.3 says, except for the PerName items, which come
// with that mode, and fills in the defaults.
func (s *Shared) validate(o Options) (Options, error) {
	gk := o.Holder.GroupKind()
	if _, err := FinalizerPrefix(gk); err != nil {
		return o, err
	}
	if s.c.Mapper == nil || s.c.Metadata == nil {
		return o, errors.New("credential-use: Clients.Metadata and Clients.Mapper are required")
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
	for _, pk := range o.Protected {
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
	p := &protectedInformer{gk: gvk.GroupKind()}
	p.lw = newListWatch(s.c.Metadata.Resource(m.Resource), heldSelector(), s.clock, nil)
	p.inf = cache.NewSharedIndexInformerWithOptions(p.lw, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
		Indexers:          cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		ObjectDescription: "credential-use protected " + gvk.GroupKind().String(),
	})
	if err := p.inf.SetTransform(transform(isCredentialUseKey, isCredentialUseKey)); err != nil {
		return nil, err
	}
	if _, err := p.inf.AddEventHandler(protectedHandler{s, p}); err != nil {
		return nil, err
	}
	return p, nil
}
