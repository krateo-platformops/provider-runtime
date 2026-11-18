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
}

var _ reports = coreReports{}

func (r coreReports) Degraded(holder schema.GroupKind, reason string) {
	r.metrics.Degraded(kindLabel(holder), reason)
}

func (r coreReports) AdmissionDenied(holder schema.GroupKind) {
	r.metrics.AdmissionDenied(kindLabel(holder))
}

func (r coreReports) ReleaseRejected(holder schema.GroupKind, p ID, resourceVersion string) {
	r.metrics.ReleaseRejected(kindLabel(holder))
	if r.events == nil {
		return
	}
	r.events.record(r.events.regarding(p), "Warning", EventReleaseRejected,
		"the release patch for "+kindLabel(holder)+" failed against an unchanged object (resourceVersion "+resourceVersion+"): it is reported, not retried as contention")
}
