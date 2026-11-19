package credentialuse

import (
	"context"
	"fmt"

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
	c, err := s.client(gk, namespace)
	if err != nil {
		return nil, err
	}
	return c.Get(ctx, name, metav1.GetOptions{})
}

func (s clientServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	c, err := s.client(gk, namespace)
	if err != nil {
		return nil, err
	}
	return c.Patch(ctx, name, pt, data, metav1.PatchOptions{})
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
