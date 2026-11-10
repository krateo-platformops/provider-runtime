package reconciler

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// defaultTraceContinueWindow is how long a new krateo.io/traceparent value keeps
// making Parent passes that don't act (docs/design-tracing.md §6.2, "The
// window"). Every default poll fits in it at least three times (1m for
// provider-runtime, incident- and alert-provider; 3m for git- and core-provider),
// and the controller's failure backoff, capped at 60s, saturates well inside it.
const defaultTraceContinueWindow = 10 * time.Minute

// WithTracerProvider sets the TracerProvider the Reconciler's spans go to,
// instead of the one tracing.SetTracerProvider set (which telemetry.Setup sets
// with tracing on). It exists for tests, and for a reconciler that should use a
// different provider. nil means: use tracing.TracerProvider().
func WithTracerProvider(tp trace.TracerProvider) ReconcilerOption {
	return func(r *Reconciler) {
		r.tracerProv = tp
	}
}

// WithTraceContinueWindow sets how long a new krateo.io/traceparent value keeps
// making Parent passes that don't act, before the marker is written without an
// action and later passes link (docs/design-tracing.md §6.2). The default is
// 10 minutes. A provider polling much more slowly than that needs a window
// longer than its poll. 0 means: continue on the first pass, the first-sight
// rule.
func WithTraceContinueWindow(d time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		if d < 0 {
			d = 0
		}
		r.traceWindow = d
	}
}

// tracerProvider is the provider for this pass, or nil when tracing is off.
func (r *Reconciler) tracerProvider() trace.TracerProvider {
	if r.tracerProv != nil {
		return r.tracerProv
	}
	return tracing.TracerProvider()
}

// memoState is what this process knows about the value a memo entry holds.
type memoState int

const (
	// memoSeen: the value is new and not continued yet.
	memoSeen memoState = iota
	// memoMarked: this process wrote the marker; it holds until a read shows it.
	memoMarked
	// memoFailed: the marker write failed with something other than a conflict.
	memoFailed
)

// memoEntry is the memo's record for one object (docs/design-tracing.md §6.2,
// "The memo").
type memoEntry struct {
	uid       types.UID
	value     string    // the canonical traceparent last seen
	firstSeen time.Time // when this process first saw value, by the monotonic clock
	state     memoState
	// failureLogged: a marker write failure for this entry was logged.
	failureLogged bool
}

// traceMemo is the reconciler's in-memory record, by object, of the
// traceparent values it has seen and marked. One mutex guards the map; it is
// held only to read or update an entry, never across an API call.
// controller-runtime never runs two passes of one key at once, so two passes
// never race on one entry.
//
// It holds at most one entry per object this reconciler has reconciled that
// carries a traceparent whose marker isn't confirmed yet: seen entries end
// within a window and a pass, marked entries at the next read that shows the
// marker, and failed entries while the object lives. NotFound, finalizer
// removal, a UID change, and the annotation going away drop an entry. With
// tracing off nothing is ever added.
//
// Turning tracing off while the process runs (tracing.SetTracerProvider(nil))
// leaves the entries it holds in place: a pass with tracing off doesn't touch
// the memo, so it costs nothing. They stay bounded by the live annotated
// objects. They're dropped when such an object is deleted, because the
// NotFound pass forgets its entry whether tracing is on or off. If tracing
// comes back on, the next pass of each object reconciles its entry with what
// the object shows.
type traceMemo struct {
	mu      sync.Mutex
	entries map[types.NamespacedName]memoEntry
}

// decide computes the pass's Decision from the object and the memo. With tp
// nil it keeps nothing and only parses the annotation, so StartReconcile can
// carry it for Inject.
func (m *traceMemo) decide(tp trace.TracerProvider, key types.NamespacedName, obj resource.Managed, now time.Time, window time.Duration) tracing.Decision {
	ann := obj.GetAnnotations()
	if tp == nil {
		return tracing.Decide(ann, "", time.Time{}, now, window)
	}
	// What the object alone says: Link here means its own marker continues R.
	plain := tracing.Decide(ann, "", time.Time{}, now, window)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[types.NamespacedName]memoEntry{}
	}
	e, ok := m.entries[key]
	if ok && e.uid != obj.GetUID() {
		delete(m.entries, key)
		ok = false
	}
	if !plain.Remote.IsValid() {
		delete(m.entries, key)
		return plain
	}
	cur := tracing.Canonical(plain.Remote)
	if ok && e.value != cur {
		delete(m.entries, key)
		ok = false
	}
	if plain.Mode == tracing.Link && plain.WriteMarker == tracing.Never {
		// The object shows the marker for R: whatever this process remembers
		// about it is confirmed.
		delete(m.entries, key)
		return plain
	}

	remembered, firstSeen := "", time.Time{}
	if ok {
		switch e.state {
		case memoMarked, memoFailed:
			remembered = cur
		case memoSeen:
			firstSeen = e.firstSeen
		}
	}
	d := tracing.Decide(ann, remembered, firstSeen, now, window)
	if !ok && d.WriteMarker != tracing.Never {
		m.entries[key] = memoEntry{uid: obj.GetUID(), value: cur, firstSeen: now, state: memoSeen}
	}
	return d
}

// set records the outcome of a marker write for value.
func (m *traceMemo) set(key types.NamespacedName, uid types.UID, value string, state memoState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[types.NamespacedName]memoEntry{}
	}
	e := m.entries[key]
	if e.uid != uid || e.value != value {
		e = memoEntry{uid: uid, value: value, firstSeen: time.Now()}
	}
	e.state = state
	m.entries[key] = e
}

// loggedFailure reports whether a marker write failure for key should be
// logged: once per entry, so a failing object doesn't log on every pass.
func (m *traceMemo) loggedFailure(key types.NamespacedName) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok || e.failureLogged {
		return false
	}
	e.failureLogged = true
	m.entries[key] = e
	return true
}

// forget drops key's entry.
func (m *traceMemo) forget(key types.NamespacedName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
}

// len reports the number of entries, for tests.
func (m *traceMemo) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// beginTrace decides the pass's mode and opens its root span. With tracing off
// it returns offPass and allocates nothing, unless the object carries a
// krateo.io/traceparent, which StartReconcile then carries for Inject
// (docs/design-tracing.md §3.4). read is false when the Get failed: the span
// then has no parent and no memo is consulted.
func (r *Reconciler) beginTrace(ctx context.Context, tp trace.TracerProvider, req reconcile.Request, managed resource.Managed, read bool, started time.Time) (context.Context, *tracePass) {
	var d tracing.Decision
	if read {
		d = r.traces.decide(tp, req.NamespacedName, managed, time.Now(), r.traceWindow)
	}
	spanCtx, span := tracing.StartReconcile(ctx, tp, tracing.Object{Kind: r.objectKind, Name: req.Name, Namespace: req.Namespace}, d, started)
	if tp == nil {
		return spanCtx, offPass
	}
	return spanCtx, &tracePass{tp: tp, span: span, d: d, key: req.NamespacedName, uid: managed.GetUID(), memo: &r.traces}
}

// offPass is every pass's tracing state with tracing off. Every method returns
// before it writes anything when tp is nil, so one shared value serves every
// pass, concurrently.
var offPass = &tracePass{}

// tracePass is one reconcile's tracing state. With tracing off tp is nil and
// every method does nothing.
type tracePass struct {
	tp     trace.TracerProvider
	span   trace.Span
	d      tracing.Decision
	key    types.NamespacedName
	uid    types.UID
	memo   *traceMemo
	marked bool // the marker was written, or the write failed, this pass
	// actJoined: this pass's first act step for a new value has been parented
	// on the upstream context (D8). Later act steps aren't.
	actJoined bool
}

// step opens a child span under the root.
func (p *tracePass) step(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracing.StartStep(ctx, p.tp, name)
}

// actStep opens an act step (create, update or delete). The pass's first
// act step for a new value is parented on the upstream context, with a link
// back to the root (D8); every other is an ordinary child of the root.
func (p *tracePass) actStep(ctx context.Context, name string) (context.Context, trace.Span) {
	if p.tp == nil {
		return tracing.StartStep(ctx, nil, name)
	}
	if p.d.Mode == tracing.Parent && !p.actJoined {
		p.actJoined = true
		return tracing.StartActStep(ctx, p.tp, name, p.d.Remote)
	}
	return tracing.StartStep(ctx, p.tp, name)
}

// endStep records err on a child and ends it.
func (p *tracePass) endStep(span trace.Span, err error) {
	if p.tp == nil {
		return
	}
	tracing.RecordError(span, err)
	span.End()
}

// event adds a fixed-name event to the root.
func (p *tracePass) event(name string, attrs ...attribute.KeyValue) {
	if p.tp == nil {
		return
	}
	p.span.AddEvent(name, trace.WithAttributes(attrs...))
}

// conflict records that the pass requeues because a write at step conflicted.
func (p *tracePass) conflict(step tracing.Step) {
	if p.tp == nil {
		return
	}
	p.event(tracing.EventConflict, tracing.AttrStep.String(string(step)))
}

// deferred records the governor's deferral of the pass's next call.
func (p *tracePass) deferred(d *ratelimit.Deferral) {
	if p.tp == nil {
		return
	}
	attrs := []attribute.KeyValue{
		tracing.AttrRateLimitKey.String(d.Key),
		tracing.AttrRateLimitCause.String(d.Reason.Cause()),
		tracing.AttrRateLimitRequeueAfter.Float64(d.RequeueAfter.Seconds()),
	}
	if d.Reason == ratelimit.DeferThrottled && !d.Until.IsZero() {
		attrs = append(attrs, tracing.AttrRateLimitUntil.String(d.Until.UTC().Format(time.RFC3339)))
	}
	p.event(tracing.EventRateLimitDeferred, attrs...)
}

// markError marks the root Error for a pass that returns nil but wrote
// Synced=False/ReconcileError.
func (p *tracePass) markError(err error) {
	if p.tp == nil {
		return
	}
	tracing.RecordError(p.span, err)
}

// forget drops the object's memo entry: its finalizer was removed.
func (p *tracePass) forget() {
	if p.tp == nil {
		return
	}
	p.memo.forget(p.key)
}

// end ends the root: Error "panic" when the pass panicked, Error with err when
// it returned one, Unset otherwise. It never sets Ok.
func (p *tracePass) end(err error, panicked bool) {
	if p.tp == nil {
		return
	}
	if panicked {
		p.span.SetStatus(codes.Error, "panic")
	} else {
		tracing.RecordError(p.span, err)
	}
	p.span.End()
}

// writeMarkerIf writes the marker when the pass's decision says to write it
// at when, and it hasn't been written this pass. It returns stop with a
// rateLimitedRequeue when the locked patch conflicted: the pass must end
// before it acts, and the next pass reads the object afresh.
//
// Any other failure is recorded as trace.marker_failed, the memo remembers the
// value as failed so later passes in this process link, and the pass goes on.
func (r *Reconciler) writeMarkerIf(ctx context.Context, log logging.Logger, managed resource.Managed, p *tracePass, when tracing.When) (reconcile.Result, bool) {
	if p.tp == nil || p.marked || p.d.WriteMarker != when || p.d.Marker == "" {
		return reconcile.Result{}, false
	}
	err := r.writeMarker(ctx, managed, p.d.Marker)
	switch {
	case err == nil || kerrors.IsNotFound(err):
		p.marked = true
		p.memo.set(p.key, p.uid, p.d.Marker, memoMarked)
		return reconcile.Result{}, false
	case kerrors.IsConflict(err):
		log.Debug("The object changed since this pass read it; requeueing before acting", "annotation", meta.AnnotationKeyTraceparentContinued)
		p.conflict(tracing.StepMarker)
		return rateLimitedRequeue(), true
	default:
		p.marked = true
		p.memo.set(p.key, p.uid, p.d.Marker, memoFailed)
		p.event(tracing.EventMarkerFailed, attribute.String("exception.message", tracing.Truncate(err.Error())))
		if p.memo.loggedFailure(p.key) {
			log.Info("Cannot write the trace marker; later passes in this process link to the trace", "annotation", meta.AnnotationKeyTraceparentContinued, "error", err.Error())
		}
		return reconcile.Result{}, false
	}
}

// writeMarker writes value as krateo.io/traceparent-continued, with a JSON
// merge patch guarded by the resourceVersion the pass read
// (client.MergeFromWithOptimisticLock), sent on a copy of managed: decoding the
// response into managed would throw away what Observe computed in memory. On
// success it copies only the new resourceVersion and the marker onto managed,
// so the pass's later writes stay version-checked against the object it read.
func (r *Reconciler) writeMarker(ctx context.Context, managed resource.Managed, value string) error {
	cp, ok := managed.DeepCopyObject().(client.Object)
	if !ok {
		return nil
	}
	base, _ := cp.DeepCopyObject().(client.Object)
	meta.AddAnnotations(cp, map[string]string{meta.AnnotationKeyTraceparentContinued: value})
	if err := r.client.Patch(ctx, cp, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	managed.SetResourceVersion(cp.GetResourceVersion())
	meta.AddAnnotations(managed, map[string]string{meta.AnnotationKeyTraceparentContinued: value})
	return nil
}
