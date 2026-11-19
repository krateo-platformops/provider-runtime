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

	heldMu sync.Mutex
	heldBy map[schema.GroupKind]int // the held gauge, by protected kind
	gone   bool                     // a failed Track dropped t

	// sinkMu guards sink. Every call to a sink holds it for reading, so
	// that rebind, which holds it for writing, returns only once no call to
	// the old sinks is running, and none starts after (N1 of the review of
	// #109). A sink must not call Track.
	sinkMu sync.RWMutex
	sink   sinks
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
}

// enqueue adds holder nn to the kind's queue.
func (t *Tracker) enqueue(nn types.NamespacedName) {
	t.sinkMu.RLock()
	defer t.sinkMu.RUnlock()
	t.sink.enqueue(nn)
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
		regs:   map[*protectedInformer]cache.ResourceEventHandlerRegistration{},
		heldBy: map[schema.GroupKind]int{},
		sink:   sinksOf(o),
	}
	t.proto = &protocol{
		holder:          gk,
		ownerFinalizers: o.OwnerFinalizers,
		grace:           o.DeletionGrace,
		clock:           s.clock,
		srv:             clientServer{md: s.c.Metadata, mapper: s.c.Mapper},
		views:           trackerViews{t},
		ver:             t.ver,
		seen:            s.seen,
		reports:         coreReports{metrics: sinkMetrics{t}, events: s.events},
		newEpoch:        NewEpoch,
	}
	// The holder informer's list or relist completion is a floor under
	// every observation of the record (§6.5 L2).
	lw := newListWatch(s.c.Metadata, m.Resource, "", s.clock, t.listCompleted)
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
	return informer{inf: t.holders, gvr: t.gvr, what: "the holder informer of " + t.gk.String()}
}

// join registers t's handler on protected informer p. On an informer that
// is already running, client-go replays p's cache to the new handler as
// adds before any later event.
func (t *Tracker) join(p *protectedInformer) error {
	reg, err := p.inf.AddEventHandler(&protectedHandler{s: t.s, p: p, t: t})
	if err != nil {
		return fmt.Errorf("credential-use: joining the protected informer of %s: %w", p.gk, err)
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

// setHeld sets t's held pairs in protected kind pgk's cache to n, and the
// held gauge to their sum over t's protected kinds (§12, L4): (protected
// object, finalizer) pairs with the kind's prefix in this process's cache.
func (t *Tracker) setHeld(pgk schema.GroupKind, n int) {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if t.gone {
		return
	}
	t.heldBy[pgk] = n
	t.reportHeldLocked()
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
	sum := 0
	for _, c := range t.heldBy {
		sum += c
	}
	sinkMetrics{t}.HeldObjects(kindLabel(t.gk), sum)
}

// dropHeld clears the held gauge of a tracker a failed Track dropped; a
// handler that still runs then changes it no more.
func (t *Tracker) dropHeld() {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if !t.gone && len(t.heldBy) > 0 {
		sinkMetrics{t}.HeldObjects(kindLabel(t.gk), 0)
	}
	t.gone = true
}

// Acquire runs on every pass of holder obj, deletion passes included, after
// the caller has read obj and before anything reads a protected object
// (§7.2 item 2, §6.2 A1 and A2). It returns ErrHolderConflict when the
// record write conflicts: the caller requeues. Every other failure is in the
// Acquired: MustNotConnect for a holder that isn't established, Degraded for
// one that is.
func (t *Tracker) Acquire(ctx context.Context, obj metav1.Object) (Acquired, error) {
	refs, err := t.o.Refs(obj)
	if err != nil {
		var a Acquired
		a.Err = fmt.Errorf("credential-use: naming the refs of %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		if IsEstablished(obj, t.o.OwnerFinalizers) {
			a.Degraded = true
			t.proto.reports.Degraded(t.gk, "Refs")
		} else {
			a.MustNotConnect = true
		}
		t.reportOutcome(obj, a)
		return a, nil
	}
	a, err := t.proto.acquire(ctx, obj, refs)
	if err != nil {
		return a, err
	}
	t.reportOutcome(obj, a)
	return a, nil
}

// reportOutcome records §12's event for a degraded pass, on the holder.
func (t *Tracker) reportOutcome(obj metav1.Object, a Acquired) {
	if !a.Degraded || a.Err == nil {
		return
	}
	if ro, ok := obj.(runtime.Object); ok {
		t.s.events.record(ro, "Warning", EventDegraded, a.Err.Error())
	}
	t.warn("credential-use: the acquire failed for an established holder, which proceeds",
		"holder", obj.GetNamespace()+"/"+obj.GetName(), "kind", t.gk.String(), "error", a.Err.Error())
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
func (t *Tracker) Forget(obj metav1.Object) bool { return t.proto.forget(obj) }

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
