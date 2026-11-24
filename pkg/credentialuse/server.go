package credentialuse

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
)

// listPageSize bounds each page of a consistent list (R2, the prober).
const listPageSize = 500

// clientServer is the protocol's server on client-go (plan, PR 3): the
// metadata client for every read and write, and the RESTMapper to find each
// GroupKind's resource. Every read is live: Get and List carry no
// resourceVersion, so they are served from etcd, consistent with every write
// before them.
type clientServer struct {
	md     metadata.Interface
	mapper meta.RESTMapper
}

var _ server = clientServer{}

// resource returns gk's resource, and whether it is namespaced.
func (s clientServer) resource(gk schema.GroupKind) (schema.GroupVersionResource, bool, error) {
	m, err := s.mapper.RESTMapping(gk)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("credential-use: mapping %s: %w", gk, err)
	}
	return m.Resource, m.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// object returns the client for object namespace/name of gk. An object of a
// namespaced kind named without a namespace, as a cluster-scoped holder's data
// may, can't exist: it is NotFound, without a call, whose 404 would name
// nothing (Classify). A list without a namespace is of every one, and isn't
// this.
func (s clientServer) object(gk schema.GroupKind, namespace, name string) (metadata.ResourceInterface, error) {
	gvr, namespaced, err := s.resource(gk)
	if err != nil {
		return nil, err
	}
	if namespaced && namespace == "" {
		return nil, apierrors.NewNotFound(gvr.GroupResource(), name)
	}
	return s.client(gk, namespace)
}

func (s clientServer) client(gk schema.GroupKind, namespace string) (metadata.ResourceInterface, error) {
	gvr, namespaced, err := s.resource(gk)
	if err != nil {
		return nil, err
	}
	r := s.md.Resource(gvr)
	if namespaced {
		return r.Namespace(namespace), nil
	}
	return r, nil
}

func (s clientServer) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	c, err := s.object(gk, namespace, name)
	if err != nil {
		return nil, err
	}
	return c.Get(ctx, name, metav1.GetOptions{})
}

// Patch patches the object. A 404 of a patch isn't the API server's word that
// the object doesn't exist: admission may answer one for another object, as a
// webhook's namespaceSelector does for a Namespace that is gone, or a validator
// for whatever it looked up, this object included. Only a read's says so, which
// admission isn't asked about: a 404 is confirmed by a GET, and is NotFound if
// that GET finds nothing of this name. Anything else, the read's own failures
// included, is an ordinary error, never of the class NotFound, whose text has
// both answers; what isn't the API server's, as a timeout, stays in its chain.
func (s clientServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	c, err := s.object(gk, namespace, name)
	if err != nil {
		return nil, err
	}
	out, err := c.Patch(ctx, name, pt, data, metav1.PatchOptions{})
	if Classify(err) != ClassNotFound {
		return out, err
	}
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
func (s clientServer) List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	c, err := s.client(gk, namespace)
	if err != nil {
		return nil, err
	}
	var out []metav1.PartialObjectMetadata
	opts := metav1.ListOptions{Limit: listPageSize}
	for {
		l, err := c.List(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, l.Items...)
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}
