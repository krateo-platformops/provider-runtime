package credentialuse

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// namespaceGroupKind is the GroupKind of a core Namespace.
var namespaceGroupKind = schema.GroupKind{Kind: "Namespace"}

// server is everything the protocol asks of the API server. Every call is a
// live, consistent read or a write: no decision rests on a cache (§6.4,
// "What the proof assumes"). The client-go implementation maps GroupKinds to
// resources with a RESTMapper (plan, PR 3); the in-memory model implements it
// in the tests, so that the schedules of §14 run this package's own code.
//
// An implementation must not hold a lock of the protocol's across a call, and
// the protocol holds none across one.
type server interface {
	// Get reads one object's metadata. A Namespace is GroupKind{Kind:
	// "Namespace"} with an empty namespace.
	Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error)
	// Patch writes one patch and returns the object's metadata after it.
	Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error)
	// List reads every object of gk in namespace, metadata only, with no
	// label selector and no resourceVersion, so that it is one consistent
	// read; it pages internally.
	List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error)
}

// views are the caches the protocol may read for its fast paths only: R0 and
// the written skip (§6.4). Both are safe when stale: a stale view can only
// keep a hold longer, or make the protocol write when it could have skipped.
type views interface {
	// Protected returns the cached metadata of protected object id, if the
	// cache has it.
	Protected(id ID) (*metav1.PartialObjectMetadata, bool)
	// HoldersNaming returns the cached holders of the tracker's kind in
	// namespace whose record names id or can't be parsed.
	HoldersNaming(id ID, namespace string) []*metav1.PartialObjectMetadata
	// NamespaceDeleting reports whether the cache has seen a
	// deletionTimestamp on namespace, and whether it knows the namespace
	// at all.
	NamespaceDeleting(namespace string) (deleting, known bool)
}

// reports are the protocol's events and counters. coreReports implements
// them with the metrics and events of §12; the model's protocols use
// nopReports.
type reports interface {
	// Degraded counts a fail-open of an established holder (§8), by class.
	Degraded(holder schema.GroupKind, reason string)
	// AdmissionDenied counts an A2 refused with class (e) of §6.2, for any
	// holder (L3 of the review of revision 16).
	AdmissionDenied(holder schema.GroupKind)
	// ReleaseRejected reports a release patch that failed against an
	// unchanged object (§6.2, L1 of the review of revision 16).
	ReleaseRejected(holder schema.GroupKind, p ID, resourceVersion string)
	// RefNotHeld reports a cross-namespace ref of holder h that isn't held,
	// for want of consent, once, when it first goes unheld (§11.3).
	RefNotHeld(holder schema.GroupKind, h metav1.Object, ref ID)
	// ConsentWithdrawn reports a hold of h released because its target
	// withdrew consent (§11.3).
	ConsentWithdrawn(holder schema.GroupKind, h metav1.Object, ref ID)
	// RefsConflicting reports that a ref of holder h was skipped for an earlier
	// one of the same object, ref, and asked for more: to hold it, or to
	// follow a Next (§9.1). It is called once per holder, object and thing
	// asked for, while the conflict lasts. A refused chain's pass reports none,
	// though its list may have one, so next is never asked for in production
	// in this release (§9).
	RefsConflicting(holder schema.GroupKind, h metav1.Object, ref ID, hold, next bool)
}

type nopReports struct{}

func (nopReports) Degraded(schema.GroupKind, string)                    {}
func (nopReports) AdmissionDenied(schema.GroupKind)                     {}
func (nopReports) ReleaseRejected(schema.GroupKind, ID, string)         {}
func (nopReports) RefNotHeld(schema.GroupKind, metav1.Object, ID)       {}
func (nopReports) ConsentWithdrawn(schema.GroupKind, metav1.Object, ID) {}

func (nopReports) RefsConflicting(schema.GroupKind, metav1.Object, ID, bool, bool) {}
