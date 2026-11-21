package credentialuse

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// A Lister reads one object of a kind from the caller's cache, for a
// Guaranteed ref (§9.3). A missing object is a NotFound error.
type Lister func(namespace, name string) (*unstructured.Unstructured, error)

// contentReader reads one object whole, live (§9.1 step 2, §9.3). It is apart
// from server, which is metadata only, so that the model's server needn't
// carry content.
type contentReader interface {
	Content(ctx context.Context, gk schema.GroupKind, namespace, name string) (*unstructured.Unstructured, error)
}

// contentCache is the content caches' view (§10.4): the cached object, if a
// content kind's informer has it.
type contentCache interface {
	Content(id ID) (*unstructured.Unstructured, bool)
}

// dynamicContent is contentReader on the dynamic client.
type dynamicContent struct {
	dyn    dynamic.Interface
	mapper meta.RESTMapper
}

func (d dynamicContent) Content(ctx context.Context, gk schema.GroupKind, namespace, name string) (*unstructured.Unstructured, error) {
	if d.dyn == nil {
		return nil, errors.New("credential-use: reading content needs Clients.Dynamic")
	}
	m, err := d.mapper.RESTMapping(gk)
	if err != nil {
		return nil, fmt.Errorf("credential-use: mapping %s: %w", gk, err)
	}
	r := d.dyn.Resource(m.Resource)
	if m.Scope.Name() == meta.RESTScopeNameNamespace {
		return r.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	return r.Get(ctx, name, metav1.GetOptions{})
}

var (
	// errChainTooDeep: a Next named refs below MaxDepth (§9.1).
	errChainTooDeep = errors.New("credential-use: the chain is deeper than MaxDepth")
	// errUnheldChainNotYet: a ref in another namespace has a Next. Following
	// an unheld hop needs the shared live read, which comes with consent
	// (§9.4; plan, PR 5).
	errUnheldChainNotYet = errors.New("credential-use: a ref in another namespace with a Next isn't supported yet")
	// errNextNeedsContent: a held ref has a Next, but its kind isn't
	// Content, so a re-point of it would go unseen (§10.4).
	errNextNeedsContent = errors.New("credential-use: a ref with a Next must be of a protected kind with Content")
)

// resolvedKey keys the objects an Acquire read in a context (WithResolved).
type resolvedKey struct{}

// WithResolved returns ctx carrying the intermediate objects a's pass held
// and read (§9.2), for Resolved.
func WithResolved(ctx context.Context, a Acquired) context.Context {
	return context.WithValue(ctx, resolvedKey{}, a.resolved)
}

// Resolved returns the object ref named, exactly as this pass's Acquire read
// it after holding it (§9.2): not a newer cached copy, which could name an
// object this pass never held. It has one for each ref with a Next that was
// held, or Guaranteed, and was followed. The object is the caller's to keep.
func Resolved(ctx context.Context, ref Ref) (*unstructured.Unstructured, bool) {
	m, _ := ctx.Value(resolvedKey{}).(map[ID]*unstructured.Unstructured)
	u, ok := m[ref.ID()]
	if !ok {
		return nil, false
	}
	return u.DeepCopy(), true
}

// readHop reads held hop id, of a content kind, after its A2, for its Next
// (§9.1 step 2): from its content cache, or live if the cache doesn't have it
// yet.
func (p *protocol) readHop(ctx context.Context, id ID) (*unstructured.Unstructured, error) {
	if p.contentCache != nil {
		if u, ok := p.contentCache.Content(id); ok {
			return u.DeepCopy(), nil
		}
	}
	u, err := p.contents.Content(ctx, id.GroupKind(), id.Namespace, id.Name)
	if err != nil {
		return nil, err
	}
	return asCached(u), nil
}

// asCached is u as a content cache keeps it, without managedFields and
// status, so a Next sees the same shape however the hop was read.
func asCached(u *unstructured.Unstructured) *unstructured.Unstructured {
	out, _ := contentTransform(u)
	return out.(*unstructured.Unstructured)
}

// readGuaranteed reads Guaranteed ref r (§9.3): through the caller's lister
// for its kind, or live.
func (p *protocol) readGuaranteed(ctx context.Context, r Ref) (*unstructured.Unstructured, error) {
	if l := p.listers[r.GroupKind]; l != nil {
		u, err := l(r.Namespace, r.Name)
		if err != nil {
			return nil, err
		}
		return asCached(u), nil
	}
	u, err := p.contents.Content(ctx, r.GroupKind, r.Namespace, r.Name)
	if err != nil {
		return nil, err
	}
	return asCached(u), nil
}

// follow calls r's Next on obj, the object read for it.
func follow(r Ref, obj *unstructured.Unstructured) ([]Ref, error) {
	refs, err := r.Next(obj.DeepCopy())
	if err != nil {
		return nil, fmt.Errorf("credential-use: following %s: %w", r.ID(), err)
	}
	return refs, nil
}
