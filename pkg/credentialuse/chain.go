package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/clock"
)

// A Lister reads one object of a kind from the caller's cache, for a
// Guaranteed ref (§9.3). A missing object is a NotFound error. The object must
// hold JSON types only, as an unstructured one read from the API server does:
// a DeepCopy of any other, such as an int32 or a []string, panics. The tracker
// doesn't change it.
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
	namespaced := m.Scope.Name() == meta.RESTScopeNameNamespace
	if !mayExist(namespaced, namespace, name) {
		return nil, apierrors.NewNotFound(m.Resource.GroupResource(), name) // as clientServer.object
	}
	r := d.dyn.Resource(m.Resource)
	if namespaced {
		return r.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	return r.Get(ctx, name, metav1.GetOptions{})
}

// ErrChainsNotSupported is the Acquired.Err of a pass one of whose refs has a
// Next: this release holds only the objects the refs name themselves, and
// refuses a chain (§9, "Not in this release"; the user's decision of
// 2026-11-24). The whole list is refused, its direct refs with it, before the
// pass reads or writes anything, so a holder that isn't established must not
// connect, and an established one is Degraded and keeps the record, and so
// the holds, it has. A later release follows chains.
var ErrChainsNotSupported = errors.New("credential-use: chains (a Ref with a Next) aren't supported in this release: " +
	"name each object Connect reads as a ref of its own, without a Next; one that can't be named so stays unheld")

var (
	// errChainTooDeep: a Next named refs below MaxDepth (§9.1).
	errChainTooDeep = errors.New("credential-use: the chain is deeper than MaxDepth")
	// errNextNeedsContent: a held ref has a Next, but its kind isn't
	// Content, so a re-point of it would go unseen (§10.4).
	errNextNeedsContent = errors.New("credential-use: a ref with a Next must be of a protected kind with Content")
)

// refuseChains returns ErrChainsNotSupported, naming the first of refs with a
// Next, unless the tests have p follow chains. It looks at every ref,
// Guaranteed ones and second refs to one object included.
func (p *protocol) refuseChains(refs []Ref) error {
	if p.followChains {
		return nil
	}
	for _, r := range refs {
		if r.Next != nil {
			return fmt.Errorf("%w: ref %s has a Next", ErrChainsNotSupported, r.ID())
		}
	}
	return nil
}

// resolvedKey keys the objects an Acquire read in a context (WithResolved).
type resolvedKey struct{}

// WithResolved returns ctx carrying the intermediate objects a's pass held
// and read (§9.2), for Resolved.
func WithResolved(ctx context.Context, a Acquired) context.Context {
	return context.WithValue(ctx, resolvedKey{}, a.resolved)
}

// Resolved returns the object ref named, exactly as this pass's Acquire read
// it (§9.2), after holding it if it holds it: not a newer cached copy, which
// could name an object this pass never held. It has one for each ref with a
// Next that the pass followed: held, Guaranteed, or unheld for want of
// consent or Disowned, which may be the shared read of up to 10s ago. Each is
// as its Next saw it, without managedFields and status. The object is the
// caller's to keep.
//
// In this release no ref is followed (ErrChainsNotSupported), so on a pass's
// context it finds nothing.
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

// NextObject is u, an object read from the API server, as an Acquire gives it
// to a Next and to Resolved: without managedFields and status (§10.4). It is
// for a test, or a runtime, that calls a Next itself. u isn't changed, and
// doesn't change with it.
func NextObject(u *unstructured.Unstructured) *unstructured.Unstructured {
	return asCached(u.DeepCopy())
}

// readGuaranteed reads Guaranteed ref r (§9.3): through the caller's lister
// for its kind, or live.
func (p *protocol) readGuaranteed(ctx context.Context, r Ref) (*unstructured.Unstructured, error) {
	if l := p.listers[r.GroupKind]; l != nil {
		u, err := l(r.Namespace, r.Name)
		if err != nil {
			return nil, err
		}
		if u == nil {
			return nil, fmt.Errorf("credential-use: the lister for %s returned neither an object nor an error", r.GroupKind)
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

// unheldReadTTL is how long a live read of an unheld hop is shared (§9.4,
// L6).
const unheldReadTTL = 10 * time.Second

// sharedReads shares the live read of unheld hops among a process's holders
// (§9.4): a hop isn't labelled, so it isn't cached, and many holders may name
// one. A read is reused for unheldReadTTL; a NotFound is shared too, any
// other failure isn't. No release decision reads it: it only decides which
// refs lie behind the hop, each then gated and held on its own.
type sharedReads struct {
	clock clock.PassiveClock
	mu    sync.Mutex
	m     map[ID]sharedRead
}

type sharedRead struct {
	obj *unstructured.Unstructured
	err error
	at  time.Time
}

func newSharedReads(c clock.PassiveClock) *sharedReads {
	return &sharedReads{clock: c, m: map[ID]sharedRead{}}
}

// get returns hop id, read through c, from a read shared for unheldReadTTL.
// A nil sharedReads reads every time.
func (s *sharedReads) get(ctx context.Context, c contentReader, id ID) (*unstructured.Unstructured, error) {
	if s == nil {
		u, err := c.Content(ctx, id.GroupKind(), id.Namespace, id.Name)
		if err != nil {
			return nil, err
		}
		return asCached(u), nil
	}
	now := s.clock.Now()
	s.mu.Lock()
	if e, ok := s.m[id]; ok && now.Sub(e.at) < unheldReadTTL {
		s.mu.Unlock()
		if e.err != nil {
			return nil, e.err
		}
		return e.obj.DeepCopy(), nil
	}
	s.mu.Unlock()
	u, err := c.Content(ctx, id.GroupKind(), id.Namespace, id.Name)
	if err != nil && Classify(err) != ClassNotFound {
		return nil, err
	}
	e := sharedRead{err: err, at: now}
	if err == nil {
		e.obj = asCached(u)
	}
	s.mu.Lock()
	for k, old := range s.m {
		if now.Sub(old.at) >= unheldReadTTL {
			delete(s.m, k)
		}
	}
	s.m[id] = e
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return e.obj.DeepCopy(), nil
}
