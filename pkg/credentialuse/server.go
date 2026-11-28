package credentialuse

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// listPageSize bounds each page of a consistent list (R2, the prober).
const listPageSize = 500

// listPageTime bounds how long the answer to each page of one takes (answered): the
// API server's own time for a request, by default, so that a page it would still have
// answered isn't cut. A list has as many pages as it needs.
const listPageTime = time.Minute

// clientServer is the protocol's server on client-go (plan, PR 3): the
// metadata client for every read and write, and the RESTMapper to find each
// GroupKind's resource. Every read is live: Get and List carry no
// resourceVersion, so they are served from etcd, consistent with every write
// before them.
type clientServer struct {
	md     metadata.Interface
	mapper meta.RESTMapper
	// holder is a tracker's holder kind, at its declared version (§6.2).
	holder schema.GroupVersionKind
	// pageTime, if set, replaces listPageTime.
	pageTime time.Duration
}

var _ server = clientServer{}

// resource returns gk's resource, and whether it is namespaced: the holder
// kind's at its declared version, and any other's at the mapper's version for
// the kind alone (§6.2).
func (s clientServer) resource(gk schema.GroupKind) (schema.GroupVersionResource, bool, error) {
	var versions []string
	if gk == s.holder.GroupKind() && s.holder.Version != "" {
		versions = []string{s.holder.Version}
	}
	m, err := s.mapper.RESTMapping(gk, versions...)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("credential-use: mapping %s: %w", gk, err)
	}
	return m.Resource, m.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// pathSegment reports whether an object, or a namespace, may have name: the
// API server's own registry admits none that isn't a single path segment.
func pathSegment(name string) bool { return len(rest.IsValidPathSegmentName(name)) == 0 }

// mayExist reports whether there may be an object namespace/name of a kind,
// namespaced or not: a namespaced kind's has a namespace, a cluster-scoped
// kind's none, and each is a single path segment.
func mayExist(namespaced bool, namespace, name string) bool {
	return (namespace != "") == namespaced && pathSegment(namespace) && pathSegment(name)
}

// object returns the client for object namespace/name of gk. One that can't
// exist (mayExist), as a holder's data may name one, names no object: it is
// NotFound, without a call, whose 404 would name nothing (Classify). A list
// without a namespace is of every one, and isn't this.
func (s clientServer) object(gk schema.GroupKind, namespace, name string) (metadata.ResourceInterface, error) {
	gvr, namespaced, err := s.resource(gk)
	if err != nil {
		return nil, err
	}
	if !mayExist(namespaced, namespace, name) {
		return nil, apierrors.NewNotFound(gvr.GroupResource(), name)
	}
	return s.client(gvr, namespaced, namespace), nil
}

// client returns the client for gvr, namespaced or not, as resource answered:
// in namespace, or in every one.
func (s clientServer) client(gvr schema.GroupVersionResource, namespaced bool, namespace string) metadata.ResourceInterface {
	r := s.md.Resource(gvr)
	if namespaced {
		return r.Namespace(namespace)
	}
	return r
}

func (s clientServer) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	c, err := s.object(gk, namespace, name)
	if err != nil {
		return nil, err
	}
	return c.Get(ctx, name, metav1.GetOptions{})
}

// refusedPatch is the error of a patch that was answered with a 404 no read
// confirmed. The text has the answer and not the chain, which would lend it the
// 404's class, so this says what the status no longer does: the API server
// refused the patch, which didn't land (mayHaveLanded).
type refusedPatch struct{ err error }

func (e refusedPatch) Error() string { return e.err.Error() }
func (e refusedPatch) Unwrap() error { return e.err }

// Patch patches the object. A 404 of a patch isn't the API server's word that
// the object doesn't exist: admission may answer one for another object, as a
// webhook's namespaceSelector does for a Namespace that is gone, or a validator
// for whatever it looked up, this object included. Only a read's says so, which
// admission isn't asked about: a 404 is confirmed by a GET, and is NotFound if
// that GET finds nothing of this name. Anything else, the read's own failures
// included, is an ordinary error, never of the class NotFound, whose text has
// both answers; what isn't the API server's, as a timeout, stays in its chain,
// and it is a refusedPatch.
func (s clientServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (_ *metav1.PartialObjectMetadata, err error) {
	c, err := s.object(gk, namespace, name)
	if err != nil {
		return nil, err
	}
	out, err := c.Patch(ctx, name, pt, data, metav1.PatchOptions{})
	if Classify(err) != ClassNotFound {
		return out, err
	}
	defer func() {
		if Classify(err) != ClassNotFound {
			err = refusedPatch{err}
		}
	}()
	// The texts of the answers, not the errors, below: wrapped, either would
	// lend the error its class, and the patch's is a NotFound.
	_, read := c.Get(ctx, name, metav1.GetOptions{})
	if read == nil {
		return nil, fmt.Errorf("credential-use: the patch was refused with a 404, and the object is there: %s", err)
	}
	if st, ok := statusOf(read); ok && Classify(read) == ClassNotFound && st.Details.Name == name {
		return nil, read
	}
	const unconfirmed = "credential-use: the patch was refused with a 404 (%s), and a read can't tell whether the object is there: "
	if _, ok := statusOf(read); !ok {
		return nil, fmt.Errorf(unconfirmed+"%w", err, read)
	}
	return nil, fmt.Errorf(unconfirmed+"%s", err, read)
}

// List reads every object of gk in namespace, page by page, with no label
// selector and no resourceVersion: each page is a consistent read, and the
// continue token keeps the pages at the first page's revision.
// A namespace that can't exist has none, and any namespace none of a
// cluster-scoped kind: no call is made. But the holder kind's are always asked
// for: the mapping is kept while the process lives, so a kind it has as
// cluster-scoped may be namespaced by now, with holders in namespace, and R2
// releases what has none (§6.2). Its list is then of every one, of which those
// in namespace are kept. That is exact: a kind that is namespaced has them all
// on that path, each with its namespace, and one that is cluster-scoped has
// none with any, its holders' holds being of another finalizer (FinalizerName).
func (s clientServer) List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	gvr, namespaced, err := s.resource(gk)
	if err != nil {
		return nil, err
	}
	filter := false
	if namespace != "" {
		filter = !namespaced && gk == s.holder.GroupKind()
		if !filter && (!namespaced || !pathSegment(namespace)) {
			return nil, nil
		}
	}
	c := s.client(gvr, namespaced, namespace)
	var out []metav1.PartialObjectMetadata
	opts := metav1.ListOptions{Limit: listPageSize}
	limit := s.pageTime
	if limit == 0 {
		limit = listPageTime
	}
	for {
		l, err := listPage(ctx, limit, c, opts)
		if err != nil {
			return nil, err
		}
		for i := range l.Items {
			if !filter || l.Items[i].Namespace == namespace {
				out = append(out, l.Items[i])
			}
		}
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}

func listPage(ctx context.Context, limit time.Duration, c metadata.ResourceInterface, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	return answered(ctx, limit, func(ctx context.Context) (*metav1.PartialObjectMetadataList, error) { return c.List(ctx, opts) })
}
