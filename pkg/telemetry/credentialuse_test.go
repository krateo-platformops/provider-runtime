package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// §12: the credential-use metrics are recorded under the design's names,
// each labelled holder_kind, and credentialuse_degraded_total also reason.
func TestCredentialUseMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	ctx := context.Background()
	t.Cleanup(func() { _ = provider.Shutdown(ctx) })
	m, err := newMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	const kind = "Repo.git.krateo.io"
	m.HeldObjects(kind, 3)
	m.WaitingObjects(kind, 1)
	m.IndexFillRatio(kind, 0.5)
	m.DisownUnreadable(kind, 1)
	m.Degraded(kind, "Deleting")
	for _, f := range []func(string){m.RefsNotHeld, m.ConsentWithdrawn, m.RefsReadDenied, m.IndexTampered,
		m.UnrecordedReads, m.AdmissionDenied, m.ReleaseRejected, m.CRDWriteStarved, m.IndexReadDenied} {
		f(kind)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]attribute.Set{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			switch d := md.Data.(type) {
			case metricdata.Gauge[int64]:
				got[md.Name] = d.DataPoints[0].Attributes
			case metricdata.Gauge[float64]:
				got[md.Name] = d.DataPoints[0].Attributes
			case metricdata.Sum[int64]:
				got[md.Name] = d.DataPoints[0].Attributes
			}
		}
	}
	for _, name := range []string{"credentialuse.held_objects", "credentialuse.waiting_objects", "credentialuse.index_fill_ratio",
		"credentialuse.degraded", "credentialuse.refs_not_held", "credentialuse.consent_withdrawn", "credentialuse.refs_read_denied",
		"credentialuse.index_tampered", "credentialuse.unrecorded_reads", "credentialuse.admission_denied",
		"credentialuse.release_rejected", "credentialuse.crd_write_starved",
		"credentialuse.disown_unreadable", "credentialuse.index_read_denied"} {
		attrs, ok := got[name]
		if !ok {
			t.Errorf("%s wasn't recorded", name)
			continue
		}
		if v, ok := attrs.Value(AttrHolderKind); !ok || v.AsString() != kind {
			t.Errorf("%s: holder_kind = %v, want %s", name, v, kind)
		}
	}
	degraded := got["credentialuse.degraded"]
	if v, ok := degraded.Value(AttrDegradedReason); !ok || v.AsString() != "Deleting" {
		t.Errorf("credentialuse.degraded: reason = %v, want Deleting", v)
	}
}
