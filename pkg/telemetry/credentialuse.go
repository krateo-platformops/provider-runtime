package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// The credential-use metrics (docs/design-credential-use-finalizer.md §12).
// The instrument names are the design's, with a dot after the prefix, so the
// Prometheus exporter gives exactly the design's names: credentialuse_held_objects
// for a gauge, and credentialuse_degraded_total, its _total added, for a
// counter. Every one is labelled holder_kind, as "<kind>.<group>".

// AttrHolderKind labels every credential-use metric with its holder kind.
const AttrHolderKind = attribute.Key("holder_kind")

// AttrDegradedReason labels credentialuse_degraded_total with the class of
// the fail-open (§8).
const AttrDegradedReason = attribute.Key("reason")

// AttrAsked labels credentialuse_refs_conflicting_total with what the skipped
// ref asked for: "hold" or "next" (§9.1).
const AttrAsked = attribute.Key("asked")

var _ credentialuse.MetricsRecorder = (*Metrics)(nil)

type credentialUseMetrics struct {
	heldObjects      metric.Int64Gauge
	waitingObjects   metric.Int64Gauge
	indexFillRatio   metric.Float64Gauge
	disownUnreadable metric.Int64Gauge
	degraded         metric.Int64Counter
	refsNotHeld      metric.Int64Counter
	refsConflicting  metric.Int64Counter
	consentWithdrawn metric.Int64Counter
	refsReadDenied   metric.Int64Counter
	indexTampered    metric.Int64Counter
	unrecordedReads  metric.Int64Counter
	admissionDenied  metric.Int64Counter
	releaseRejected  metric.Int64Counter
	crdWriteStarved  metric.Int64Counter
	indexReadDenied  metric.Int64Counter
	disownRestarts   metric.Int64Counter
}

func newCredentialUseMetrics(meter metric.Meter) (credentialUseMetrics, error) {
	var m credentialUseMetrics
	var err error
	if m.heldObjects, err = meter.Int64Gauge("credentialuse.held_objects",
		metric.WithDescription("The (protected object, finalizer) pairs with the holder kind's prefix in this process's cache.")); err != nil {
		return m, err
	}
	if m.waitingObjects, err = meter.Int64Gauge("credentialuse.waiting_objects",
		metric.WithDescription("Protected objects being deleted that wait for the holder kind's users, or for the grace.")); err != nil {
		return m, err
	}
	if m.indexFillRatio, err = meter.Float64Gauge("credentialuse.index_fill_ratio",
		metric.WithDescription("How full the holder kind's index ConfigMap is (PerName and PerNamePoll).")); err != nil {
		return m, err
	}
	if m.disownUnreadable, err = meter.Int64Gauge("credentialuse.disown_unreadable",
		metric.WithDescription("Objects in the index or a record that Disown's last pass couldn't read, keeping its count unknown (PerName).")); err != nil {
		return m, err
	}
	counters := []struct {
		dst  *metric.Int64Counter
		name string
		desc string
	}{
		{&m.degraded, "credentialuse.degraded", "Acquires that failed for an established holder, which proceeded (fail open), by reason."},
		{&m.refsNotHeld, "credentialuse.refs_not_held", "Cross-namespace refs not held for want of consent, counted when each first goes unheld."},
		{&m.refsConflicting, "credentialuse.refs_conflicting", "Refs skipped though they asked for more than the first ref of the same object, by what they asked for, once per holder, object and thing asked for."},
		{&m.consentWithdrawn, "credentialuse.consent_withdrawn", "Holds released because their target withdrew consent."},
		{&m.refsReadDenied, "credentialuse.refs_read_denied", "Holds released after a persistent 403 on the consent read (PerNamePoll)."},
		{&m.indexTampered, "credentialuse.index_tampered", "Index ConfigMaps found changed by someone else, and rebuilt."},
		{&m.unrecordedReads, "credentialuse.unrecorded_reads", "Reads of a protected object that the holder's refs don't name, so nothing holds it."},
		{&m.admissionDenied, "credentialuse.admission_denied", "Acquiring writes refused by admission."},
		{&m.releaseRejected, "credentialuse.release_rejected", "Release patches that failed against an unchanged object."},
		{&m.crdWriteStarved, "credentialuse.crd_write_starved", "CRD writes that conflicted on three ticks in a row."},
		{&m.indexReadDenied, "credentialuse.index_read_denied", "Objects the index names that a check couldn't read for a 403, counted once each; their lines are kept (PerName)."},
		{&m.disownRestarts, "credentialuse.disown_restarts", "Disown's runs of zero passes that started over because the CRD changed."},
	}
	for _, c := range counters {
		if *c.dst, err = meter.Int64Counter(c.name, metric.WithDescription(c.desc)); err != nil {
			return m, err
		}
	}
	return m, nil
}

func holderKind(k string) metric.MeasurementOption {
	return metric.WithAttributes(AttrHolderKind.String(k))
}

// HeldObjects implements credentialuse.MetricsRecorder.
func (m *Metrics) HeldObjects(holderKindLabel string, n int) {
	m.cu.heldObjects.Record(context.Background(), int64(n), holderKind(holderKindLabel))
}

// WaitingObjects implements credentialuse.MetricsRecorder.
func (m *Metrics) WaitingObjects(holderKindLabel string, n int) {
	m.cu.waitingObjects.Record(context.Background(), int64(n), holderKind(holderKindLabel))
}

// IndexFillRatio implements credentialuse.MetricsRecorder.
func (m *Metrics) IndexFillRatio(holderKindLabel string, ratio float64) {
	m.cu.indexFillRatio.Record(context.Background(), ratio, holderKind(holderKindLabel))
}

// Degraded implements credentialuse.MetricsRecorder.
func (m *Metrics) Degraded(holderKindLabel, reason string) {
	m.cu.degraded.Add(context.Background(), 1, metric.WithAttributes(AttrHolderKind.String(holderKindLabel), AttrDegradedReason.String(reason)))
}

// RefsNotHeld implements credentialuse.MetricsRecorder.
func (m *Metrics) RefsNotHeld(holderKindLabel string) {
	m.cu.refsNotHeld.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// RefsConflicting implements credentialuse.MetricsRecorder.
func (m *Metrics) RefsConflicting(holderKindLabel, asked string) {
	m.cu.refsConflicting.Add(context.Background(), 1, metric.WithAttributes(AttrHolderKind.String(holderKindLabel), AttrAsked.String(asked)))
}

// ConsentWithdrawn implements credentialuse.MetricsRecorder.
func (m *Metrics) ConsentWithdrawn(holderKindLabel string) {
	m.cu.consentWithdrawn.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// RefsReadDenied implements credentialuse.MetricsRecorder.
func (m *Metrics) RefsReadDenied(holderKindLabel string) {
	m.cu.refsReadDenied.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// IndexTampered implements credentialuse.MetricsRecorder.
func (m *Metrics) IndexTampered(holderKindLabel string) {
	m.cu.indexTampered.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// UnrecordedReads implements credentialuse.MetricsRecorder.
func (m *Metrics) UnrecordedReads(holderKindLabel string) {
	m.cu.unrecordedReads.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// AdmissionDenied implements credentialuse.MetricsRecorder.
func (m *Metrics) AdmissionDenied(holderKindLabel string) {
	m.cu.admissionDenied.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// ReleaseRejected implements credentialuse.MetricsRecorder.
func (m *Metrics) ReleaseRejected(holderKindLabel string) {
	m.cu.releaseRejected.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// DisownUnreadable implements credentialuse.MetricsRecorder.
func (m *Metrics) DisownUnreadable(holderKindLabel string, n int) {
	m.cu.disownUnreadable.Record(context.Background(), int64(n), holderKind(holderKindLabel))
}

// IndexReadDenied implements credentialuse.MetricsRecorder.
func (m *Metrics) IndexReadDenied(holderKindLabel string) {
	m.cu.indexReadDenied.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// DisownRestarts implements credentialuse.MetricsRecorder.
func (m *Metrics) DisownRestarts(holderKindLabel string) {
	m.cu.disownRestarts.Add(context.Background(), 1, holderKind(holderKindLabel))
}

// CRDWriteStarved implements credentialuse.MetricsRecorder.
func (m *Metrics) CRDWriteStarved(holderKindLabel string) {
	m.cu.crdWriteStarved.Add(context.Background(), 1, holderKind(holderKindLabel))
}
