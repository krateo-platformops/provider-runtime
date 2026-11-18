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

// reports are the protocol's events and counters. PR 3 implements them
// with the metrics and events of §12; until then the protocol uses
// nopReports.
type reports interface {
	// Degraded counts a fail-open of an established holder (§8), by class.
	Degraded(holder schema.GroupKind, reason string)
	// ReleaseRejected reports a release patch that failed against an
	// unchanged object (§6.2, L1 of the review of revision 16).
	ReleaseRejected(holder schema.GroupKind, p ID, resourceVersion string)
}

type nopReports struct{}

func (nopReports) Degraded(schema.GroupKind, string)            {}
func (nopReports) ReleaseRejected(schema.GroupKind, ID, string) {}
