package scaletozero

import (
	"context"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
)

// Held objects (docs/design-credential-use-finalizer.md §11.6, §10.6.5): a
// Kind that holds credentials stays up while it holds any, or while its
// Disown switch hasn't settled. See "Held objects" in the package
// documentation.

// An Option is an option of ReconcileWithOptions and ExistsWithOptions.
type Option func(*options)

// options are the options a call was given.
type options struct {
	mapper meta.RESTMapper
	// withHeld: WithHeld was given; protected are its kinds.
	withHeld  bool
	protected []schema.GroupVersionKind
	// index is WithHeldIndex's ConfigMap, nil without it.
	index *types.NamespacedName
}

// WithHeld counts as an instance every object of the protected kinds that
// carries a credential-use finalizer of the Kind's (§11.6), and a Disown
// switch on the Kind's CRD that hasn't settled. It lists each kind by the
// Kind's marker label, so it is for LabelScoped kinds: the PerName ones are
// WithHeldIndex's. At least one kind is required; each needs a version and a
// kind.
func WithHeld(protected ...schema.GroupVersionKind) Option {
	p := slices.Clone(protected)
	return func(o *options) {
		o.withHeld = true
		o.protected = append(o.protected, p...)
	}
}

// WithHeldIndex counts as an instance every object that the Kind's index
// ConfigMap, at index, names and that still carries a credential-use
// finalizer of the Kind's, and a Disown switch on the Kind's CRD that hasn't
// settled (§10.6.5). It is for the PerName and PerNamePoll kinds, whose holds
// the index names. A missing or unreadable index is an error, never "nothing
// held".
func WithHeldIndex(index types.NamespacedName) Option {
	return func(o *options) { o.index = &index }
}

// WithRESTMapper gives ExistsWithOptions the RESTMapper that WithHeld and
// WithHeldIndex need: a client.Reader carries none. ReconcileWithOptions
// takes its client's, and one given here takes its place.
func WithRESTMapper(m meta.RESTMapper) Option {
	return func(o *options) { o.mapper = m }
}

// newOptions applies opts and checks them.
func newOptions(opts []Option) (options, error) {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.withHeld && len(o.protected) == 0 {
		return o, errors.New("scaletozero: WithHeld needs at least one protected kind")
	}
	for _, p := range o.protected {
		if p.Version == "" || p.Kind == "" {
			return o, errors.Errorf("scaletozero: WithHeld needs a version and a kind for each protected kind, got %q", p)
		}
	}
	if o.index != nil && (o.index.Namespace == "" || o.index.Name == "") {
		return o, errors.Errorf("scaletozero: WithHeldIndex needs a namespace and a name, got %q", o.index.String())
	}
	if o.held() && o.mapper == nil {
		return o, errors.New("scaletozero: WithHeld and WithHeldIndex need WithRESTMapper, since a client.Reader carries no RESTMapper")
	}
	return o, nil
}

// held reports whether held objects count.
func (o options) held() bool { return o.withHeld || o.index != nil }

// crdGVK is the CustomResourceDefinition kind.
var crdGVK = schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}

// configMapGVK is the ConfigMap kind.
var configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}

// heldExists reports whether holder kind gvk's Disown switch hasn't settled,
// or an object still carries one of its finalizers: of WithHeld's kinds, by
// the marker label, or one that WithHeldIndex's index names.
func (o options) heldExists(ctx context.Context, r client.Reader, gvk schema.GroupVersionKind) (bool, error) {
	holder := gvk.GroupKind()
	prefix, err := credentialuse.FinalizerPrefix(holder)
	if err != nil {
		return false, fmt.Errorf("scaletozero: %w", err)
	}
	unsettled, err := disownUnsettled(ctx, r, o.mapper, gvk)
	if err != nil || unsettled {
		return unsettled, err
	}
	marked, err := labels.NewRequirement(credentialuse.MarkerLabel(holder), selection.Exists, nil)
	if err != nil {
		return false, fmt.Errorf("scaletozero: selecting by the marker label of %s: %w", holder, err)
	}
	sel := labels.NewSelector().Add(*marked)
	carries := func(m *metav1.PartialObjectMetadata) bool { return hasPrefix(m.Finalizers, prefix) }
	for _, p := range o.protected {
		found, err := first(ctx, r, p, sel, carries)
		if err != nil || found {
			return found, err
		}
	}
	if o.index != nil {
		return indexHeld(ctx, r, o.mapper, *o.index, prefix)
	}
	return false, nil
}

// disownUnsettled reports whether holder kind gvk's CRD has a Disown switch
// that is set and hasn't settled, by credentialuse.DisownSettled (§7.4; decided
// 2026-11-23). The CRD's name is the kind's plural and group, which only the
// mapper knows. A CRD that can't be read is an error.
func disownUnsettled(ctx context.Context, r client.Reader, mapper meta.RESTMapper, gvk schema.GroupVersionKind) (bool, error) {
	m, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return false, fmt.Errorf("scaletozero: mapping %s for the name of its CRD: %w", gvk, err)
	}
	name := m.Resource.Resource + "." + gvk.Group
	crd := &metav1.PartialObjectMetadata{}
	crd.SetGroupVersionKind(crdGVK)
	if err := r.Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
		return false, fmt.Errorf("scaletozero: reading CRD %s for its Disown switch: %w", name, err)
	}
	ann := crd.GetAnnotations()
	return ann[credentialuse.AnnotationDisown] != "" && !credentialuse.DisownSettled(ann), nil
}

// indexHeld reports whether an object that the index ConfigMap at index names
// still carries a finalizer with prefix. It GETs the objects one by one and
// stops at the first that does. A missing or unreadable index is an error
// (§10.6.5, L4), and so is any failed GET but a NotFound.
func indexHeld(ctx context.Context, r client.Reader, mapper meta.RESTMapper, index types.NamespacedName, prefix string) (bool, error) {
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	if err := r.Get(ctx, client.ObjectKey(index), cm); err != nil {
		return false, fmt.Errorf("scaletozero: reading the index %s: %w", index, err)
	}
	data, _, err := unstructured.NestedStringMap(cm.Object, "data")
	if err != nil {
		return false, fmt.Errorf("scaletozero: reading the index %s: %w", index, err)
	}
	for _, id := range credentialuse.IndexIDs(data) {
		held, err := lineHeld(ctx, r, mapper, id, prefix)
		if err != nil || held {
			return held, err
		}
	}
	return false, nil
}

// lineHeld reports whether the object an index line names carries a finalizer
// with prefix. One that is gone, or can't exist, holds nothing. A kind the
// mapper can't map is an error (decided 2026-11-25).
func lineHeld(ctx context.Context, r client.Reader, mapper meta.RESTMapper, id credentialuse.ID, prefix string) (bool, error) {
	m, err := mapper.RESTMapping(id.GroupKind())
	if err != nil {
		return false, fmt.Errorf("scaletozero: mapping %s, of the index line %q: %w", id.GroupKind(), id, err)
	}
	if !mayExist(m.Scope.Name() == meta.RESTScopeNameNamespace, id.Namespace, id.Name) {
		return false, nil
	}
	obj := &metav1.PartialObjectMetadata{}
	obj.SetGroupVersionKind(m.GroupVersionKind)
	err = r.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, obj)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("scaletozero: reading %q, of the index: %w", id, err)
	}
	return hasPrefix(obj.Finalizers, prefix), nil
}

// mayExist reports whether there may be an object namespace/name of a kind:
// a namespaced kind's has a namespace, a cluster-scoped kind's none, and the
// API server admits no name, nor namespace, that isn't a single path segment.
// A hand-edited line may name one that can't, which holds nothing.
func mayExist(namespaced bool, namespace, name string) bool {
	return (namespace != "") == namespaced && len(rest.IsValidPathSegmentName(name)) == 0 &&
		len(rest.IsValidPathSegmentName(namespace)) == 0
}

// hasPrefix reports whether a finalizer starts with prefix.
func hasPrefix(finalizers []string, prefix string) bool {
	for _, f := range finalizers {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}
