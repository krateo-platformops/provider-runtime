package credentialuse

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The harness binds the protocol to the in-memory model: a process is a
// cache with the handlers of §6.3 on it, a verified set and first-sight
// times; a server is the model, reached through one actor's yield points, so
// that every API call the protocol makes is a step of the schedule.

var widgetGK = schema.GroupKind{Group: "credentialuse.test.krateo.io", Kind: "Widget"}

const ownerFinalizer = "finalizer.managedresource.krateo.io"

var ownerFinalizers = []string{ownerFinalizer}

func widgetKey(ns, name string) apimodel.Key {
	return apimodel.Key{GK: widgetGK, Namespace: ns, Name: name}
}
func secretKey(ns, name string) apimodel.Key {
	return apimodel.Key{GK: secretGroupKind, Namespace: ns, Name: name}
}
func nsKey(name string) apimodel.Key { return apimodel.Key{GK: apimodel.NamespaceKind, Name: name} }
func keyOf(id ID) apimodel.Key {
	return apimodel.Key{GK: id.GroupKind(), Namespace: id.Namespace, Name: id.Name}
}

// modelServer is the protocol's server over the model. With an actor, every
// call is a step of its schedule; without one, calls run directly.
type modelServer struct {
	m *apimodel.Model
	a *apimodel.Actor
}

func (s modelServer) call(label string, op func() error) error {
	if s.a == nil {
		return op()
	}
	return s.a.Call(label, op)
}

func (s modelServer) Get(_ context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	var out *metav1.PartialObjectMetadata
	err := s.call("get "+gk.Kind+"/"+name, func() error {
		var err error
		out, err = s.m.Get(apimodel.Key{GK: gk, Namespace: namespace, Name: name})
		return err
	})
	return out, err
}

func (s modelServer) Patch(_ context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	var out *metav1.PartialObjectMetadata
	err := s.call("patch "+gk.Kind+"/"+name, func() error {
		var err error
		out, err = s.m.Patch(apimodel.Key{GK: gk, Namespace: namespace, Name: name}, pt, data)
		return err
	})
	return out, err
}

func (s modelServer) List(_ context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	var out []metav1.PartialObjectMetadata
	err := s.call("list "+gk.Kind, func() error {
		out = s.m.List(gk, namespace)
		return nil
	})
	return out, err
}

// process is one controller process: what a restart or a leader change
// loses.
type process struct {
	name  string
	cache *apimodel.Cache
	ver   *verified
	seen  *firstSight
}

// heldOnly is the protected caches' selector: credential-use.krateo.io/held
// (§10.1). Holders and namespaces are cached whole.
func heldOnly(k apimodel.Key, o *metav1.PartialObjectMetadata) bool {
	if k.GK == widgetGK || k.GK == apimodel.NamespaceKind {
		return true
	}
	return o.Labels[LabelHeld] == "true"
}

// newProcess starts a process: its caches list the model at now.
func newProcess(name string, m *apimodel.Model, now time.Time) *process {
	p := &process{name: name, cache: apimodel.NewCache(m, heldOnly), ver: newVerified(widgetGK), seen: newFirstSight()}
	p.relist(now)
	return p
}

// deliver runs §6.3's handlers for one cache change, delivered at at.
func (p *process) deliver(ch apimodel.Change, at time.Time) {
	switch ch.Key.GK {
	case widgetGK:
		var uid types.UID
		var h metav1.Object
		if ch.New != nil {
			uid, h = ch.New.UID, ch.New
		} else {
			uid = ch.Old.UID
		}
		if ch.Old != nil && ch.New != nil && ch.Old.UID != ch.New.UID {
			p.ver.holderEvent(ch.Old.UID, nil, at)
		}
		p.ver.holderEvent(uid, h, at)
	case apimodel.NamespaceKind:
	default:
		if ch.Old != nil {
			var cur metav1.Object
			if ch.New != nil {
				cur = ch.New
			}
			p.ver.protectedEvent(ch.Old, cur)
		}
	}
}

// relist is a list or a relist of every cache, at now (§6.5 L2).
func (p *process) relist(now time.Time) {
	for _, ch := range p.cache.List() {
		p.deliver(ch, now)
	}
	p.ver.listCompleted(now)
}

// cacheViews are the protocol's views over a process's cache. The process
// may be replaced by a restart, so the views follow a pointer to it.
type cacheViews struct{ p **process }

func (v cacheViews) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	return (*v.p).cache.Get(keyOf(id))
}

func (v cacheViews) HoldersNaming(id ID, namespace string) []*metav1.PartialObjectMetadata {
	var out []*metav1.PartialObjectMetadata
	for _, h := range (*v.p).cache.In(widgetGK, namespace) {
		if ReadRecord(h).Names(id) {
			out = append(out, h)
		}
	}
	return out
}

func (v cacheViews) NamespaceDeleting(namespace string) (bool, bool) {
	o, ok := (*v.p).cache.Get(nsKey(namespace))
	if !ok {
		return false, false
	}
	return o.DeletionTimestamp != nil, true
}

// noViews are empty caches: every fast path misses.
type noViews struct{}

func (noViews) Protected(ID) (*metav1.PartialObjectMetadata, bool)       { return nil, false }
func (noViews) HoldersNaming(ID, string) []*metav1.PartialObjectMetadata { return nil }
func (noViews) NamespaceDeleting(string) (bool, bool)                    { return false, false }

var epochSeq atomic.Int64

// testEpoch is a fresh, readable epoch.
func testEpoch() (string, error) { return fmt.Sprintf("epoch-%d", epochSeq.Add(1)), nil }

func newProtocol(srv server, v views, ver *verified, seen *firstSight, c clock.PassiveClock, grace time.Duration) *protocol {
	return &protocol{
		holder:          widgetGK,
		ownerFinalizers: ownerFinalizers,
		grace:           grace,
		clock:           c,
		srv:             srv,
		views:           v,
		ver:             ver,
		seen:            seen,
		reports:         nopReports{},
		newEpoch:        testEpoch,
	}
}

// finalizerPatch adds or removes the owner finalizer, guarded by the
// holder's resourceVersion, and with remove also removes the record: the one
// write of §8 steps 4 and 7.
func finalizerPatch(obj metav1.Object, add bool) []byte {
	fs := slices.DeleteFunc(slices.Clone(obj.GetFinalizers()), func(f string) bool { return f == ownerFinalizer })
	meta := map[string]any{"resourceVersion": obj.GetResourceVersion()}
	if add {
		fs = append(fs, ownerFinalizer)
	} else {
		// Forget removed the record in memory; a merge patch removes it
		// with a null, in the same write as the finalizer.
		meta["annotations"] = map[string]any{AnnotationRefs: nil, AnnotationUnheld: nil}
	}
	if fs == nil {
		fs = []string{}
	}
	meta["finalizers"] = fs
	b, _ := json.Marshal(map[string]any{"metadata": meta})
	return b
}

// seed creates a namespace, and in it a Secret and holders. held lists the
// holders whose record names the Secret and that carry the owner finalizer;
// the Secret then carries the finalizer, its epoch and both labels, as after
// their acquire.
func seed(m *apimodel.Model, ns, secret string, holders []string, held bool) {
	must(m.Create(nsKey(ns), metav1.ObjectMeta{}))
	f, _ := FinalizerName(widgetGK, ns)
	sm := metav1.ObjectMeta{}
	if held {
		sm = metav1.ObjectMeta{
			Finalizers:  []string{f},
			Annotations: map[string]string{f: "epoch-seed"},
			Labels:      map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"},
		}
	}
	must(m.Create(secretKey(ns, secret), sm))
	rec, _ := EncodeRecord([]ID{SecretRef(ns, secret).ID()})
	for _, h := range holders {
		hm := metav1.ObjectMeta{}
		if held {
			hm = metav1.ObjectMeta{Finalizers: []string{ownerFinalizer}, Annotations: map[string]string{AnnotationRefs: rec}}
		}
		must(m.Create(widgetKey(ns, h), hm))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
