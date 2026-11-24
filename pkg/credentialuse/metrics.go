package credentialuse

// MetricsRecorder receives the core's metrics (§12). Every metric is
// labelled with the holder kind, as "<kind>.<group>". pkg/telemetry
// implements it with OpenTelemetry (plan, PR 7); NopMetrics records nothing.
// Implementations must be safe for concurrent use, and must not block. The
// interface grows with the metrics: an implementation outside this module
// embeds NopMetrics.
type MetricsRecorder interface {
	// HeldObjects sets credentialuse_held_objects: the (protected object,
	// finalizer) pairs with the kind's prefix in this process's cache.
	HeldObjects(holderKind string, n int)
	// WaitingObjects sets credentialuse_waiting_objects: protected objects
	// whose deletion waits for this kind's users.
	WaitingObjects(holderKind string, n int)
	// IndexFillRatio sets credentialuse_index_fill_ratio (PerName and
	// PerNamePoll, §10.6.4).
	IndexFillRatio(holderKind string, ratio float64)
	// Degraded counts credentialuse_degraded_total{reason}: every fail-open
	// of an established holder (§8).
	Degraded(holderKind, reason string)
	// RefsNotHeld counts credentialuse_refs_not_held_total.
	RefsNotHeld(holderKind string)
	// ConsentWithdrawn counts credentialuse_consent_withdrawn_total (§11.3).
	ConsentWithdrawn(holderKind string)
	// RefsConflicting counts credentialuse_refs_conflicting_total{asked}:
	// refs skipped though they asked for more than the first ref of the same
	// object (§9.1), once per holder, object and thing asked for. asked is
	// "hold" or "next", and a ref that asked for both counts in both.
	RefsConflicting(holderKind, asked string)
	// RefsReadDenied counts credentialuse_refs_read_denied_total (§11.3).
	RefsReadDenied(holderKind string)
	// IndexTampered counts credentialuse_index_tampered_total (§10.6.6).
	IndexTampered(holderKind string)
	// UnrecordedReads counts credentialuse_unrecorded_reads_total (§11.9).
	UnrecordedReads(holderKind string)
	// AdmissionDenied counts credentialuse_admission_denied_total: class (e)
	// of §6.2 (L3 of the review of revision 16).
	AdmissionDenied(holderKind string)
	// ReleaseRejected counts credentialuse_release_rejected_total: a release
	// patch that failed against an unchanged object (§6.2).
	ReleaseRejected(holderKind string)
	// CRDWriteStarved counts credentialuse_crd_write_starved_total (§7.3).
	CRDWriteStarved(holderKind string)
	// DisownUnreadable sets credentialuse_disown_unreadable: the objects in
	// the index or a record that Disown's last pass couldn't read, which
	// keep its count "unknown" (§7.4).
	DisownUnreadable(holderKind string, n int)
	// IndexReadDenied counts credentialuse_index_read_denied_total: objects
	// the index names that a check couldn't read for a 403, once each; their
	// lines are kept (§10.6.5).
	IndexReadDenied(holderKind string)
}

// NopMetrics records nothing.
type NopMetrics struct{}

var _ MetricsRecorder = NopMetrics{}

func (NopMetrics) HeldObjects(string, int)        {}
func (NopMetrics) WaitingObjects(string, int)     {}
func (NopMetrics) IndexFillRatio(string, float64) {}
func (NopMetrics) Degraded(string, string)        {}
func (NopMetrics) RefsNotHeld(string)             {}
func (NopMetrics) RefsConflicting(_, _ string)    {}
func (NopMetrics) ConsentWithdrawn(string)        {}
func (NopMetrics) RefsReadDenied(string)          {}
func (NopMetrics) IndexTampered(string)           {}
func (NopMetrics) UnrecordedReads(string)         {}
func (NopMetrics) AdmissionDenied(string)         {}
func (NopMetrics) ReleaseRejected(string)         {}
func (NopMetrics) CRDWriteStarved(string)         {}
func (NopMetrics) DisownUnreadable(string, int)   {}
func (NopMetrics) IndexReadDenied(string)         {}
