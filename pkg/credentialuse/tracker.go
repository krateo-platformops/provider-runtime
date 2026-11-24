package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// A Tracker holds the protected objects one holder kind reads, for as long
// as its holders need them (§7.1). Shared.Track builds it.
type Tracker struct {
	s      *Shared
	o      Options
	gk     schema.GroupKind
	gvr    schema.GroupVersionResource
	prefix string

	ver     *verified
	proto   *protocol
	holders cache.SharedIndexInformer
	// holderLW is what the holder informer lists and watches with: the
	// core's listWatch, or the tests' wrapper of it.
	holderLW cache.ListerWatcher
	// stopHolders ends the holder informer when a Track after Run started
	// it; RunInformers' context ends the others. Nil until then.
	stopHolders context.CancelFunc

	// regs are t's handlers on the protected informers, which only Track
	// changes.
	regs map[*protectedInformer]cache.ResourceEventHandlerRegistration

	// heldMu guards the gauges' counts, by protected informer: held
	// (object, finalizer) pairs, and waiting objects (§12).
	heldMu    sync.Mutex
	heldBy    map[countKey]int
	waitingBy map[countKey]int
	// waitingUncached are the objects outside the caches that the release
	// loop last found kept for t's users (§10.5, keptUncached).
	waitingUncached map[ID]bool
	// uncached are t's protected kinds with objects outside the caches
	// (§10.5), set when Track builds t.
	uncached []schema.GroupKind
	gone     bool // a failed Track dropped t

	// missing are the refs a holder's pass last found Missing, for the
	// prober (§10.3).
	missing *missingRefs
	// unrecorded are the (holder, object) pairs CheckRead has logged
	// (§11.9).
	unrecorded *reportedSet
	// degradedLogged are the holders whose degraded pass was logged, keyed
	// with an empty ref: §8 logs it once per object, until a pass that
	// isn't degraded, or Forget, clears it.
	degradedLogged *reportedSet
	// refused are the holders whose refused chain was reported, event and log
	// line both, keyed and cleared as degradedLogged is. It is apart, so that
	// neither that nor another failure hides the other, the failed gate of a
	// refused pass included.
	refused *reportedSet
	// refusedNow are the holders whose last pass was refused for a chain, new
	// or established, for credentialuse_refused_holders (§9, §12).
	refusedNow refusedHolders
	// misplaced are the holders in the index namespace whose refusal was
	// logged, once each (§10.6.1).
	misplaced *reportedSet

	// sinkMu guards sink. Every call to a sink holds it for reading, so
	// that rebind, which holds it for writing, returns only once no call to
	// the old sinks is running, and none starts after (N1 of the review of
	// #109). A sink must not call Track.
	sinkMu sync.RWMutex
	sink   sinks

	// Disown (§7.4): crd is the informer on the holder kind's own CRD,
	// crdName, that carries the switch; disown is this process's progress
	// towards disown-settled.
	crd     cache.SharedIndexInformer
	crdName string
	disown  disownState

	// idx is the kind's index, for its PerName kinds (§10.6), or nil
	// without any; names are the per-name watches that follow its lines
	// (§13 shape 1).
	idx   *index
	names *perNames
}

// sinks are where a tracker's events go: the kind's queue, its logger and
// its metrics. A Track with equal Options rebinds them.
type sinks struct {
	enqueue func(types.NamespacedName)
	logger  logging.Logger
	metrics MetricsRecorder
}

func sinksOf(o Options) sinks { return sinks{o.Enqueue, o.Logger, o.Metrics} }

// rebind makes o's sinks t's, and reports the held gauge to the new
// metrics.
func (t *Tracker) rebind(o Options) {
	t.sinkMu.Lock()
	t.sink = sinksOf(o)
	t.sinkMu.Unlock()
	t.reportHeld()
	t.reportRefused()
}

// enqueue adds holder nn to the kind's queue.
func (t *Tracker) enqueue(nn types.NamespacedName) {
	t.sinkMu.RLock()
	defer t.sinkMu.RUnlock()
	t.sink.enqueue(nn)
}

func (t *Tracker) sinkInfo(msg string, keysAndValues ...any) {
	t.sinkMu.RLock()
	defer t.sinkMu.RUnlock()
	t.sink.logger.Info(msg, keysAndValues...)
}

func (t *Tracker) warn(msg string, keysAndValues ...any) {
	t.sinkMu.RLock()
	defer t.sinkMu.RUnlock()
	t.sink.logger.Warn(msg, keysAndValues...)
}

func (t *Tracker) withMetrics(f func(MetricsRecorder)) {
	t.sinkMu.RLock()
	defer t.sinkMu.RUnlock()
	f(t.sink.metrics)
}

// sinkMetrics is a tracker's MetricsRecorder: whichever its sinks hold.
type sinkMetrics struct{ t *Tracker }

var _ MetricsRecorder = sinkMetrics{}

func (m sinkMetrics) HeldObjects(k string, n int) {
	m.t.withMetrics(func(r MetricsRecorder) { r.HeldObjects(k, n) })
}
func (m sinkMetrics) WaitingObjects(k string, n int) {
	m.t.withMetrics(func(r MetricsRecorder) { r.WaitingObjects(k, n) })
}
func (m sinkMetrics) IndexFillRatio(k string, x float64) {
	m.t.withMetrics(func(r MetricsRecorder) { r.IndexFillRatio(k, x) })
}
func (m sinkMetrics) Degraded(k, reason string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.Degraded(k, reason) })
}
func (m sinkMetrics) RefsNotHeld(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.RefsNotHeld(k) })
}
func (m sinkMetrics) ConsentWithdrawn(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.ConsentWithdrawn(k) })
}
func (m sinkMetrics) RefsReadDenied(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.RefsReadDenied(k) })
}
func (m sinkMetrics) IndexTampered(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.IndexTampered(k) })
}
func (m sinkMetrics) UnrecordedReads(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.UnrecordedReads(k) })
}
func (m sinkMetrics) AdmissionDenied(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.AdmissionDenied(k) })
}
func (m sinkMetrics) ReleaseRejected(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.ReleaseRejected(k) })
}
func (m sinkMetrics) RefusedHolders(k string, n int) {
	m.t.withMetrics(func(r MetricsRecorder) { r.RefusedHolders(k, n) })
}
func (m sinkMetrics) DisownUnreadable(k string, n int) {
	m.t.withMetrics(func(r MetricsRecorder) { r.DisownUnreadable(k, n) })
}
func (m sinkMetrics) IndexReadDenied(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.IndexReadDenied(k) })
}
func (m sinkMetrics) RefsConflicting(k, asked string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.RefsConflicting(k, asked) })
}
func (m sinkMetrics) DisownRestarts(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.DisownRestarts(k) })
}
func (m sinkMetrics) CRDWriteStarved(k string) {
	m.t.withMetrics(func(r MetricsRecorder) { r.CRDWriteStarved(k) })
}

func (s *Shared) newTracker(o Options) (*Tracker, error) {
	gk := o.Holder.GroupKind()
	prefix, err := FinalizerPrefix(gk)
	if err != nil {
		return nil, err
	}
	m, err := s.c.Mapper.RESTMapping(gk, o.Holder.Version)
	if err != nil {
		return nil, err
	}
	t := &Tracker{
		s: s, o: o, gk: gk, gvr: m.Resource, prefix: prefix, ver: newVerified(gk),
		regs:       map[*protectedInformer]cache.ResourceEventHandlerRegistration{},
		heldBy:     map[countKey]int{},
		waitingBy:  map[countKey]int{},
		missing:    newMissingRefs(),
		unrecorded: newReportedSet(),

		degradedLogged: newReportedSet(),
		refused:        newReportedSet(),
		misplaced:      newReportedSet(),
		sink:           sinksOf(o),
	}
	t.proto = &protocol{
		holder:          gk,
		ownerFinalizers: o.OwnerFinalizers,
		grace:           o.DeletionGrace,
		clock:           s.clock,
		srv:             s.newServer(o.Holder),
		views:           trackerViews{t},
		ver:             t.ver,
		seen:            s.seen,
		reports:         coreReports{metrics: sinkMetrics{t}, events: s.events, warn: t.warn},
		newEpoch:        NewEpoch,
		a2Bound:         o.TimeoutBound,
		heldDeleting:    t.heldDeleting,
		mustWait:        func(n string, o Waiting) bool { return s.mustWait(batchKey{gk, n}, o) },
		followChains:    s.hooks.followChains,
		maxDepth:        o.MaxDepth,
		contentKinds:    map[schema.GroupKind]bool{},
		contentCache:    trackerViews{t},
		contents:        dynamicContent{dyn: s.c.Dynamic, mapper: s.c.Mapper},
		listers:         s.o.Listers,
		namespaceViews:  trackerViews{t},
		protects:        t.protects,
		cacheLacks:      t.cacheLacks,
		gateTurns:       newGateTurns(),
		gateContext:     gateDeadline,
		backoff:         newConsentBackoff(o.PollInterval),
		reported:        newReportedSet(),
		conflicts:       newConflictSet(),
		written:         newRecordWrites(),
		unheldReads:     s.unheldReads,
		enqueueBatch: func(id ID, ns string) {
			s.queue.Add(batchKey{gk, ns}, id)
		},
		disowned:   func(pk schema.GroupKind) bool { return t.disownSwitch().covers(pk) },
		disownLive: t.disownForHold,
		noHolders:  t.noHolders,
	}
	for _, pk := range o.Protected {
		if pk.Content {
			t.proto.contentKinds[pk.GVK.GroupKind()] = true
		}
	}
	t.names = newPerNames(t)
	if err := t.names.mapKinds(s.c.Mapper, o.Protected); err != nil {
		return nil, err
	}
	if err := t.newTrackerIndex(); err != nil {
		return nil, err
	}
	// The holder informer's list or relist completion is a floor under
	// every observation of the record (§6.5 L2).
	lw := newListWatch(s.c.Metadata, m.Resource, "", "", s.clock, t.listCompleted)
	lw.recovered = s.watchRecovered(t.informer().what)
	t.holderLW = lw
	if wrap := holderListWatchSeam; wrap != nil {
		t.holderLW = wrap(lw)
	}
	t.holders = cache.NewSharedIndexInformerWithOptions(t.holderLW, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
		Indexers:          cache.Indexers{refsIndex: holderIndexer},
		ObjectDescription: "credential-use holders " + gk.String(),
	})
	if err := t.holders.SetTransform(transform(func(string) bool { return false }, isCredentialUseKey)); err != nil {
		return nil, err
	}
	if _, err := t.holders.AddEventHandler(holderHandler{t}); err != nil {
		return nil, err
	}
	t.crdName = m.Resource.Resource + "." + m.Resource.Group
	if t.crd, err = newCRDInformer(s.c.Metadata, t.crdName, s.clock, s.watchRecovered(t.crdInformer().what)); err != nil {
		return nil, err
	}
	if _, err := t.crd.AddEventHandler(crdHandler{t}); err != nil {
		return nil, err
	}
	return t, nil
}

// holderListWatchSeam, set only by the tests (export_test.go), wraps each
// holder informer's listWatch: to refuse watch-list requests, count lists,
// and end watches with a 410 (N5 of the review of #109).
var holderListWatchSeam func(lw *listWatch) cache.ListerWatcher

// listCompleted records a list or relist of the holder informer (§6.5 L2).
func (t *Tracker) listCompleted(at time.Time) {
	if f := t.s.hooks.beforeListRecorded; f != nil {
		f(t)
	}
	t.ver.listCompleted(at)
}

func (t *Tracker) informer() informer {
	return informer{t: t, inf: t.holders, gvr: t.gvr, what: "the holder informer of " + t.gk.String()}
}

// crdInformer is the informer on t's own CRD, which carries the Disown
// switch (§7.4); it needs list and watch on that CRD's name alone.
func (t *Tracker) crdInformer() informer {
	return informer{t: t, inf: t.crd, gvr: crdGVR, name: t.crdName, what: "the CRD informer of " + t.gk.String()}
}

// join registers t's handler on protected informer p. On an informer that
// is already running, client-go replays p's cache to the new handler as
// adds before any later event.
func (t *Tracker) join(p *protectedInformer) error {
	reg, err := p.inf.AddEventHandler(&protectedHandler{s: t.s, p: p, t: t})
	if err != nil {
		return fmt.Errorf("credential-use: joining %s: %w", p.what(), err)
	}
	t.regs[p] = reg
	p.add(t)
	return nil
}

// leaveAll removes t's handlers from the protected informers.
func (t *Tracker) leaveAll() {
	for p, reg := range t.regs {
		_ = p.inf.RemoveEventHandler(reg)
		p.remove(t)
		delete(t.regs, p)
	}
}

// stopInformers ends the holder informer, if a Track after Run started it.
func (t *Tracker) stopInformers() {
	if t.stopHolders != nil {
		t.stopHolders()
	}
}

// setCounts sets what t holds in protected informer pk's cache, and the
// gauges to their sums over t's protected informers (§12): held, the
// (object, finalizer) pairs with the kind's prefix (L4); waiting, the
// objects being deleted that carry one of the kind's finalizers. The
// releaser removes a finalizer as soon as §5 lets it, so a deleting object
// that still carries one is waiting for the kind's users, or for the grace.
// Counting from the caches keeps the gauge right on every replica, not only
// on the one whose write loops run.
//
// w is the per-name watch the counts come from, or nil: a watch that has
// stopped counts no more.
func (t *Tracker) setCounts(k countKey, held, waiting int, w *nameWatch) {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if t.gone || w != nil && w.dead {
		return
	}
	t.heldBy[k], t.waitingBy[k] = held, waiting
	t.reportHeldLocked()
}

// dropCounts takes a stopped per-name watch's counts off the gauges: its
// handler gets no deletion for what it counted.
func (t *Tracker) dropCounts(w *nameWatch) {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	w.dead = true
	k := w.p.countKey()
	if _, ok := t.heldBy[k]; !ok {
		return
	}
	delete(t.heldBy, k)
	delete(t.waitingBy, k)
	if !t.gone {
		t.reportHeldLocked()
	}
}

// reportHeld reports the held gauge again, as after a rebind.
func (t *Tracker) reportHeld() {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if !t.gone {
		t.reportHeldLocked()
	}
}

func (t *Tracker) reportHeldLocked() {
	held, waiting := 0, 0
	for _, c := range t.heldBy {
		held += c
	}
	for _, c := range t.waitingBy {
		waiting += c
	}
	waiting += len(t.waitingUncached)
	sinkMetrics{t}.HeldObjects(kindLabel(t.gk), held)
	sinkMetrics{t}.WaitingObjects(kindLabel(t.gk), waiting)
}

// dropHeld clears the gauges of a tracker a failed Track dropped; a handler
// that still runs then changes them no more.
func (t *Tracker) dropHeld() {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if !t.gone && len(t.heldBy) > 0 {
		sinkMetrics{t}.HeldObjects(kindLabel(t.gk), 0)
		sinkMetrics{t}.WaitingObjects(kindLabel(t.gk), 0)
	}
	t.gone = true
}

// Acquire runs on every pass of holder obj, deletion passes included, after
// the caller has read obj and before anything reads a protected object
// (§7.2 item 2, §6.2 A1 and A2). It returns ErrHolderConflict when the
// record write conflicts: the caller requeues. Every other failure is in the
// Acquired: MustNotConnect for a holder that isn't established, Degraded for
// one that is. In this release, a ref with a Next is such a failure
// (ErrChainsNotSupported).
func (t *Tracker) Acquire(ctx context.Context, obj metav1.Object) (Acquired, error) {
	// failed is a pass that ends before it names its refs. The gate of what
	// the record names runs first in every pass (§11.3), in these as in
	// proto.acquire; what comes before it here calls nothing.
	failed := func(err error, reason string) (Acquired, error) {
		var a Acquired
		if _, gerr := t.proto.gateRecorded(ctx, obj, &a); errors.Is(gerr, ErrHolderConflict) {
			return a, gerr
		} else if gerr != nil {
			err = errors.Join(err, gerr)
		}
		a.Err = err
		if IsEstablished(obj, t.o.OwnerFinalizers) {
			a.Degraded = true
			t.proto.reports.Degraded(t.gk, reason)
		} else {
			a.MustNotConnect = true
		}
		t.reportOutcome(obj, a)
		return a, nil
	}
	if t.idx != nil && obj.GetNamespace() != "" && obj.GetNamespace() == t.idx.namespace {
		// The index must outlive every holder, so a holder in its namespace
		// gets no hold: the start check's backstop, which the namespace's
		// admission policy makes unreachable (§10.6.1).
		if t.misplaced.first(consentKey{holder: obj.GetUID()}) {
			t.warn("credential-use: a holder lives in the index namespace, so it gets no hold: move it, or the index (§10.6.1)",
				"holder", obj.GetNamespace()+"/"+obj.GetName(), "kind", t.gk.String(), "index", t.idx.ref())
		}
		return failed(fmt.Errorf("credential-use: %s/%s lives in the index namespace %s, where no holder may live (§10.6.1)",
			obj.GetNamespace(), obj.GetName(), t.idx.namespace), "IndexNamespace")
	}
	refs, err := t.o.Refs(obj)
	if err != nil {
		return failed(fmt.Errorf("credential-use: naming the refs of %s/%s: %w", obj.GetNamespace(), obj.GetName(), err), "Refs")
	}
	a, err := t.proto.acquire(ctx, obj, refs)
	if err != nil {
		return a, err
	}
	t.noteMissing(obj, a)
	t.reportOutcome(obj, a)
	return a, nil
}

// noteMissing keeps the prober's refs (§10.3): a ref this pass found Missing
// is due again at the shortest backoff, and the holder no longer waits for
// any other ref it was missing.
func (t *Tracker) noteMissing(obj metav1.Object, a Acquired) {
	holder := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	var missing []ID
	for _, u := range a.Unheld {
		if u.Reason == ReasonMissing {
			missing = append(missing, u.Ref.ID())
		}
	}
	t.missing.set(holder, missing, t.s.clock.Now(), t.o.ProbeInterval)
}

// heldDeleting is told of an object an A2 held whose response shows it being
// deleted. Outside the protected caches (SharedOptions.Namespaces, §10.5) no
// informer delivers that deletion, so the object's batch is enqueued here:
// R1 then measures the grace from this first sight. Inside them, the
// informer does it.
func (t *Tracker) heldDeleting(id ID, namespace string) {
	if !t.cachedID(id) {
		t.s.queue.Add(batchKey{t.gk, namespace}, id)
	}
}

// reportOutcome records §12's event for a degraded pass, on the holder,
// throttled by the event sink, and logs it once per holder while it stays
// degraded (§8): a pass that isn't degraded, or Forget, clears that, so the
// next degraded pass logs again.
//
// A refused chain's event is recorded once too, with its log line, and cleared
// the same way: it lasts until the provider or the object changes, so a
// throttle alone would record one per holder every eventThrottle, for ever.
// The counter counts every pass. That is of a refusal alone: one whose gate
// failed too is reported as any failure, since a Secret may stay held against
// its owner meanwhile.
func (t *Tracker) reportOutcome(obj metav1.Object, a Acquired) {
	t.noteRefused(obj.GetUID(), errors.Is(a.Err, ErrChainsNotSupported))
	key := consentKey{holder: obj.GetUID()}
	if !a.Degraded {
		t.degradedLogged.clear(key)
		t.refused.clear(key)
		return
	}
	if a.Err == nil {
		return
	}
	refused := errors.Is(a.Err, ErrChainsNotSupported) && a.gateErr == nil
	if refused && !t.refused.first(key) {
		return
	}
	if ro, ok := obj.(runtime.Object); ok {
		t.s.events.record(ro, "Warning", EventDegraded, a.Err.Error())
	}
	if !refused && !t.degradedLogged.first(key) {
		return
	}
	kv := []any{"holder", obj.GetNamespace() + "/" + obj.GetName(), "kind", t.gk.String(), "error", a.Err.Error()}
	var chain chainRefused
	if errors.As(a.Err, &chain) {
		kv = append(kv, "ref", chain.id.String()) // whole: the error cuts it
	}
	t.warn("credential-use: the acquire failed for an established holder, which proceeds", kv...)
}

// refusedHolders are a tracker's refused holders, by UID.
type refusedHolders struct {
	mu sync.Mutex
	by map[types.UID]bool
}

// noteRefused records whether holder uid's last pass was refused for a chain,
// and sets credentialuse_refused_holders when that changes its count. It is
// called with no other lock of the tracker's held, and holds its own across
// the sink, so that the gauge's values come in the order of the changes.
func (t *Tracker) noteRefused(uid types.UID, refused bool) {
	r := &t.refusedNow
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.by[uid] == refused {
		return
	}
	if r.by == nil {
		r.by = map[types.UID]bool{}
	}
	if refused {
		r.by[uid] = true
	} else {
		delete(r.by, uid)
	}
	sinkMetrics{t}.RefusedHolders(kindLabel(t.gk), len(r.by))
}

// reportRefused sets the gauge again, as after a rebind.
func (t *Tracker) reportRefused() {
	r := &t.refusedNow
	r.mu.Lock()
	defer r.mu.Unlock()
	sinkMetrics{t}.RefusedHolders(kindLabel(t.gk), len(r.by))
}

// BeforeEstablish runs just before the caller adds its owner finalizer
// (§7.2 item 3): it live-reads every ref that was Missing, and holds any
// that now exists. On an error, the caller doesn't add the finalizer this
// pass.
func (t *Tracker) BeforeEstablish(ctx context.Context, obj metav1.Object, a Acquired) error {
	return t.proto.beforeEstablish(ctx, obj, a)
}

// Forget removes the record from obj in memory, just before the caller's
// write that removes its owner finalizer, so that both go in one write
// (§7.2 item 4). It is mandatory. It reports whether anything changed.
func (t *Tracker) Forget(obj metav1.Object) bool {
	t.degradedLogged.forget(obj.GetUID())
	t.refused.forget(obj.GetUID())
	t.noteRefused(obj.GetUID(), false)
	t.missing.set(types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, nil, t.s.clock.Now(), t.o.ProbeInterval)
	return t.proto.forget(obj)
}

// Users returns the holders of this kind in namespace that use p, read live
// (§6.1): for diagnostics and tests. The release loop decides from its own
// consistent list.
func (t *Tracker) Users(ctx context.Context, p Ref, namespace string) ([]types.NamespacedName, error) {
	if p.GroupKind.Kind == "" || p.Name == "" {
		return nil, errors.New("credential-use: Users needs a ref with a kind and a name")
	}
	list, err := t.proto.srv.List(ctx, t.gk, namespace)
	if err != nil {
		return nil, err
	}
	var out []types.NamespacedName
	for i := range list {
		if IsUser(&list[i], p.ID(), t.o.OwnerFinalizers) {
			out = append(out, types.NamespacedName{Namespace: list[i].Namespace, Name: list[i].Name})
		}
	}
	return out, nil
}

// HolderKind is the GroupKind the tracker holds for.
func (t *Tracker) HolderKind() schema.GroupKind { return t.gk }
