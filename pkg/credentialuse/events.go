package credentialuse

import (
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
)

// eventThrottle is how long an event stays suppressed after it was recorded,
// for the same object, reason and note (§12: "throttled, once per state
// change"). A new note, which a new state brings, goes through at once.
const eventThrottle = 10 * time.Minute

// eventCap bounds the throttle's memory: past it, entries older than the
// throttle are dropped.
const eventCap = 4096

// The events of §12.
const (
	EventDegraded        = "CredentialUseDegraded"
	EventReleaseRejected = "CredentialReleaseRejected"
	// EventRefNotHeld (Warning, on the holder): a cross-namespace ref isn't
	// held, for want of consent; Connect reads it as today (§11.3).
	EventRefNotHeld = "CredentialRefNotHeld"
	// EventConsentWithdrawn (Warning, on the target and the holder): a hold
	// was released because its target withdrew consent (§11.3).
	EventConsentWithdrawn = "CredentialConsentWithdrawn"
	// EventConsentWildcardIgnored (Warning, on a Namespace): "*" in a
	// Namespace's allow-holders-from is ignored (§11.3).
	EventConsentWildcardIgnored = "CredentialConsentWildcardIgnored"
)

// eventSink records the core's events through Clients.Events, throttled.
// A nil recorder records nothing.
type eventSink struct {
	rec    events.EventRecorder
	mapper meta.RESTMapper
	clock  clock.PassiveClock

	mu   sync.Mutex
	last map[string]time.Time
}

func newEventSink(rec events.EventRecorder, mapper meta.RESTMapper, c clock.PassiveClock) *eventSink {
	return &eventSink{rec: rec, mapper: mapper, clock: c, last: map[string]time.Time{}}
}

// record records an event regarding obj, unless the same one was recorded
// less than eventThrottle ago.
func (e *eventSink) record(obj runtime.Object, eventtype, reason, note string) {
	if e == nil || e.rec == nil || obj == nil {
		return
	}
	m, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	key := strings.Join([]string{string(m.GetUID()), m.GetNamespace(), m.GetName(), reason, note}, "\x00")
	now := e.clock.Now()
	e.mu.Lock()
	if t, ok := e.last[key]; ok && now.Sub(t) < eventThrottle {
		e.mu.Unlock()
		return
	}
	if len(e.last) >= eventCap {
		for k, t := range e.last {
			if now.Sub(t) >= eventThrottle {
				delete(e.last, k)
			}
		}
	}
	e.last[key] = now
	e.mu.Unlock()
	e.rec.Eventf(obj, nil, eventtype, reason, reason, "%s", note)
}

// regarding builds an object for an event on protected object id, of which
// the core holds only an ID: its kind and version come from the mapper.
func (e *eventSink) regarding(id ID) runtime.Object {
	o := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: id.Namespace, Name: id.Name}}
	gk := id.GroupKind()
	if e.mapper != nil {
		if m, err := e.mapper.RESTMapping(gk); err == nil {
			o.SetGroupVersionKind(m.GroupVersionKind)
			return o
		}
	}
	o.SetGroupVersionKind(gk.WithVersion("v1"))
	return o
}

// kindLabel is a holder kind as the metrics label it: "<kind>.<group>".
func kindLabel(gk schema.GroupKind) string {
	if gk.Group == "" {
		return gk.Kind
	}
	return gk.Kind + "." + gk.Group
}

// coreReports are the protocol's reports on the core's metrics and events
// (§12).
type coreReports struct {
	metrics MetricsRecorder
	events  *eventSink
	warn    func(msg string, keysAndValues ...any) // nil logs nothing
}

var _ reports = coreReports{}

func (r coreReports) Degraded(holder schema.GroupKind, reason string) {
	r.metrics.Degraded(kindLabel(holder), reason)
}

func (r coreReports) AdmissionDenied(holder schema.GroupKind) {
	r.metrics.AdmissionDenied(kindLabel(holder))
}

// holderRef is holder h, of kind gk, as an event's object.
func (e *eventSink) holderRef(gk schema.GroupKind, h metav1.Object) runtime.Object {
	o := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: h.GetNamespace(), Name: h.GetName(), UID: h.GetUID()}}
	if e.mapper != nil {
		if m, err := e.mapper.RESTMapping(gk); err == nil {
			o.SetGroupVersionKind(m.GroupVersionKind)
			return o
		}
	}
	o.SetGroupVersionKind(gk.WithVersion("v1"))
	return o
}

func (r coreReports) RefNotHeld(holder schema.GroupKind, h metav1.Object, ref ID) {
	r.metrics.RefsNotHeld(kindLabel(holder))
	if r.events == nil {
		return
	}
	r.events.record(r.events.holderRef(holder, h), "Warning", EventRefNotHeld,
		"ref "+unheldRef(ref)+" is not held: allow-holders-from on it or on Namespace "+ref.Namespace+
			" doesn't name "+h.GetNamespace()+"; it is read as before, without protection (credential-use.krateo.io/unheld)")
}

func (r coreReports) ConsentWithdrawn(holder schema.GroupKind, h metav1.Object, ref ID) {
	r.metrics.ConsentWithdrawn(kindLabel(holder))
	if r.events == nil {
		return
	}
	note := "hold on " + unheldRef(ref) + " released: holders from " + h.GetNamespace() +
		" are no longer allowed (allow-holders-from on it and on Namespace " + ref.Namespace + ")"
	r.events.record(r.events.holderRef(holder, h), "Warning", EventConsentWithdrawn, note)
	r.events.record(r.events.regarding(ref), "Warning", EventConsentWithdrawn, note)
}

// What a conflicting ref asked for, as credentialuse_refs_conflicting_total
// is labelled (§9.1).
const (
	askedHold = "hold"
	askedNext = "next"
)

func (r coreReports) RefsConflicting(holder schema.GroupKind, h metav1.Object, ref ID, hold, next bool) {
	var asked []string
	if hold {
		asked = append(asked, askedHold)
	}
	if next {
		asked = append(asked, askedNext)
	}
	for _, a := range asked {
		r.metrics.RefsConflicting(kindLabel(holder), a)
	}
	if r.warn == nil {
		return
	}
	r.warn("credential-use: a holder's refs name one object more than once, and a later one asks for more than the first, which wins: what it asks for isn't done (§9.1)",
		"kind", holder.String(), "holder", h.GetNamespace()+"/"+h.GetName(), "object", ref.String(), "asked", strings.Join(asked, ","))
}

func (r coreReports) ReleaseRejected(holder schema.GroupKind, p ID, resourceVersion string) {
	r.metrics.ReleaseRejected(kindLabel(holder))
	if r.events == nil {
		return
	}
	r.events.record(r.events.regarding(p), "Warning", EventReleaseRejected,
		"the release patch for "+kindLabel(holder)+" failed against an unchanged object (resourceVersion "+resourceVersion+"): it is reported, not retried as contention")
}
