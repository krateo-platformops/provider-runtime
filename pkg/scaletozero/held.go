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
	// filters are WithHeldFilter's: a hold counts only if each accepts it.
	filters []func(credentialuse.ID) bool
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

// WithHeldFilter counts only the holds that accept accepts, of WithHeld's
// lists and of WithHeldIndex's lines alike, and the check still stops at the
// first hold it accepts (decided 2026-11-26). An object that carries the
// prefix but that accept refuses is passed over as a collision is, and a line
// it refuses isn't read at all, so its object needs no grant.
//
// So accept is asked about different things on the two paths. On the lists,
// only about holds: objects that carry the prefix. On the index, about every
// line before its GET, so also about objects that are gone or carry no prefix.
// A filter with a cost or a side effect sees more calls on the index path.
//
// A call checks the holds of one holder kind, the gvk it is given, so accept
// may capture that kind. core-provider's Undeploy, for one, counts only the
// holds that no other live definition's RBAC covers (§16.3):
//
//	scaletozero.WithHeldFilter(func(id credentialuse.ID) bool {
//		return !coveredBySurvivorOf(holderGVK.GroupKind(), id)
//	})
//
// The filter doesn't apply to the Disown switch: a switch that hasn't settled
// counts whatever the filter, since it isn't the hold of a name. With
// several filters a hold counts only if each accepts it. A nil filter is
// ignored, and a non-nil one without WithHeld or WithHeldIndex is an error.
func WithHeldFilter(accept func(credentialuse.ID) bool) Option {
	return func(o *options) {
		if accept != nil {
			o.filters = append(o.filters, accept)
		}
	}
}

// WithRESTMapper gives ExistsWithOptions the RESTMapper that WithHeld and
// WithHeldIndex need: a client.Reader carries none. ReconcileWithOptions
// takes its client's only when none is given here.
func WithRESTMapper(m meta.RESTMapper) Option {
	return func(o *options) { o.mapper = m }
}

// collect applies opts, without checking them.
func collect(opts []Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// newOptions applies opts and checks them.
func newOptions(opts []Option) (options, error) {
	o := collect(opts)
	if len(o.filters) > 0 && !o.held() {
		return o, errors.New("scaletozero: WithHeldFilter needs WithHeld or WithHeldIndex")
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
		return o, errors.New("scaletozero: WithHeld and WithHeldIndex need a RESTMapper: pass WithRESTMapper, since a client.Reader carries none (ReconcileWithOptions takes its client's)")
	}
	return o, nil
}

// held reports whether held objects count.
func (o options) held() bool { return o.withHeld || o.index != nil }

// accepts reports whether every WithHeldFilter accepts the hold of id.
func (o options) accepts(id credentialuse.ID) bool {
	for _, f := range o.filters {
		if !f(id) {
			return false
		}
	}
	return true
}

// crdGVK is the CustomResourceDefinition kind.
var crdGVK = schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}

// configMapGVK is the ConfigMap kind.
var configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}

// HeldWithOptions reports whether holder kind gvk holds anything: it checks
// only what ExistsWithOptions checks after the instances, the holds and the
// Disown switch, with the same options, filter and errors, and never lists
// the instances (decided 2026-11-26). It is for an owner whose instances may
// rightly remain, as core-provider's Undeploy on its SkipCRD path, where a
// surviving definition keeps the kind's instances (§16.3). labels.Nothing()
// can't stand in for it: controller-runtime sends it as an empty selector,
// which matches every instance.
//
// It needs WithHeld or WithHeldIndex, without which it would have nothing to
// check, and WithRESTMapper. Every failed read is an error, never false.
func HeldWithOptions(ctx context.Context, r client.Reader, gvk schema.GroupVersionKind, opts ...Option) (bool, error) {
	if r == nil {
		return false, errors.New("scaletozero: HeldWithOptions needs a reader")
	}
	if gvk.Version == "" || gvk.Kind == "" {
		return false, errors.Errorf("scaletozero: HeldWithOptions needs a version and a kind, got %q", gvk)
	}
	o, err := newOptions(opts)
	if err != nil {
		return false, err
	}
	if !o.held() {
		return false, errors.New("scaletozero: HeldWithOptions needs WithHeld or WithHeldIndex")
	}
	return o.heldExists(ctx, r, gvk)
}

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
	for _, p := range o.protected {
		gk := p.GroupKind()
		carries := func(m *metav1.PartialObjectMetadata) bool {
			return hasPrefix(m.Finalizers, prefix) && o.accepts(credentialuse.ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name})
		}
		found, err := first(ctx, r, p, sel, carries)
		if err != nil || found {
			return found, err
		}
	}
	if o.index != nil {
		return o.indexHeld(ctx, r, prefix)
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

// indexHeld reports whether an object that WithHeldIndex's ConfigMap names,
// and the filters accept, still carries a finalizer with prefix. It GETs the
// objects one by one and stops at the first that does; a line the filters
// refuse isn't read. A missing or unreadable index is an error (§10.6.5, L4),
// and so is any failed GET but a NotFound.
func (o options) indexHeld(ctx context.Context, r client.Reader, prefix string) (bool, error) {
	index := *o.index
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
		if !o.accepts(id) {
			continue
		}
		held, err := lineHeld(ctx, r, o.mapper, id, prefix)
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
