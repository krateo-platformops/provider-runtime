package credentialuse

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// A Tracker holds the protected objects one holder kind reads, for as long
// as its holders need them (§7.1). Shared.Track builds it.
type Tracker struct {
	s      *Shared
	o      Options
	gk     schema.GroupKind
	prefix string

	ver     *verified
	proto   *protocol
	holders cache.SharedIndexInformer
	lw      *listWatch
}

func (s *Shared) newTracker(o Options) (*Tracker, error) {
	gk := o.Holder.GroupKind()
	prefix, err := FinalizerPrefix(gk)
	if err != nil {
		return nil, err
	}
	m, err := s.c.Mapper.RESTMapping(gk, o.Holder.Version)
	if err != nil {
		return nil, err
	}
	t := &Tracker{s: s, o: o, gk: gk, prefix: prefix, ver: newVerified(gk)}
	t.proto = &protocol{
		holder:          gk,
		ownerFinalizers: o.OwnerFinalizers,
		grace:           o.DeletionGrace,
		clock:           s.clock,
		srv:             clientServer{md: s.c.Metadata, mapper: s.c.Mapper},
		views:           trackerViews{t},
		ver:             t.ver,
		seen:            s.seen,
		reports:         coreReports{metrics: o.Metrics, events: s.events},
		newEpoch:        NewEpoch,
	}
	// The holder informer's list or relist completion is a floor under
	// every observation of the record (§6.5 L2).
	t.lw = newListWatch(s.c.Metadata.Resource(m.Resource), "", s.clock, t.ver.listCompleted)
	t.holders = cache.NewSharedIndexInformerWithOptions(t.lw, &metav1.PartialObjectMetadata{}, cache.SharedIndexInformerOptions{
		Indexers:          cache.Indexers{refsIndex: holderIndexer},
		ObjectDescription: "credential-use holders " + gk.String(),
	})
	if err := t.holders.SetTransform(transform(func(string) bool { return false }, isCredentialUseKey)); err != nil {
		return nil, err
	}
	if _, err := t.holders.AddEventHandler(holderHandler{t}); err != nil {
		return nil, err
	}
	return t, nil
}

// Acquire runs on every pass of holder obj, deletion passes included, after
// the caller has read obj and before anything reads a protected object
// (§7.2 item 2, §6.2 A1 and A2). It returns ErrHolderConflict when the
// record write conflicts: the caller requeues. Every other failure is in the
// Acquired: MustNotConnect for a holder that isn't established, Degraded for
// one that is.
func (t *Tracker) Acquire(ctx context.Context, obj metav1.Object) (Acquired, error) {
	refs, err := t.o.Refs(obj)
	if err != nil {
		var a Acquired
		a.Err = fmt.Errorf("credential-use: naming the refs of %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		if IsEstablished(obj, t.o.OwnerFinalizers) {
			a.Degraded = true
			t.proto.reports.Degraded(t.gk, "Refs")
		} else {
			a.MustNotConnect = true
		}
		t.reportOutcome(obj, a)
		return a, nil
	}
	a, err := t.proto.acquire(ctx, obj, refs)
	if err != nil {
		return a, err
	}
	t.reportOutcome(obj, a)
	return a, nil
}

// reportOutcome records §12's event for a degraded pass, on the holder.
func (t *Tracker) reportOutcome(obj metav1.Object, a Acquired) {
	if !a.Degraded || a.Err == nil {
		return
	}
	if ro, ok := obj.(runtime.Object); ok {
		t.s.events.record(ro, "Warning", EventDegraded, a.Err.Error())
	}
	t.o.Logger.Warn("credential-use: the acquire failed for an established holder, which proceeds",
		"holder", obj.GetNamespace()+"/"+obj.GetName(), "kind", t.gk.String(), "error", a.Err.Error())
}

// BeforeEstablish runs just before the caller adds its owner finalizer
// (§7.2 item 3): it live-reads every ref that was Missing, and holds any
// that now exists. On an error, the caller doesn't add the finalizer this
// pass.
func (t *Tracker) BeforeEstablish(ctx context.Context, obj metav1.Object, a Acquired) error {
	return t.proto.beforeEstablish(ctx, obj, a)
}

// Forget removes the record from obj in memory, just before the caller's
// write that removes its owner finalizer, so that both go in one write
// (§7.2 item 4). It is mandatory. It reports whether anything changed.
func (t *Tracker) Forget(obj metav1.Object) bool { return t.proto.forget(obj) }

// Users returns the holders of this kind in namespace that use p, read live
// (§6.1): for diagnostics and tests. The release loop decides from its own
// consistent list.
func (t *Tracker) Users(ctx context.Context, p Ref, namespace string) ([]types.NamespacedName, error) {
	if p.GroupKind.Kind == "" || p.Name == "" {
		return nil, errors.New("credential-use: Users needs a ref with a kind and a name")
	}
	list, err := t.proto.srv.List(ctx, t.gk, namespace)
	if err != nil {
		return nil, err
	}
	var out []types.NamespacedName
	for i := range list {
		if IsUser(&list[i], p.ID(), t.o.OwnerFinalizers) {
			out = append(out, types.NamespacedName{Namespace: list[i].Namespace, Name: list[i].Name})
		}
	}
	return out, nil
}

// HolderKind is the GroupKind the tracker holds for.
func (t *Tracker) HolderKind() schema.GroupKind { return t.gk }
