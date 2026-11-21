package credentialuse

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
)

// Held objects outside the protected caches (§10.5, the user's decision of
// 2026-11-19). A process whose caches SharedOptions.Namespaces restricts
// still holds the objects its holders name outside them, reading them live.
// No informer delivers their changes, so three triggers release them:
//
//   - a holder's pass whose A2 finds such an object being deleted
//     (Tracker.heldDeleting);
//   - a namespace outside the caches starting to delete
//     (namespaceHandler): each tracker's batch for that namespace, which a
//     live list fills (heldIn);
//   - the write loops' start, and a Track after it (recheckUncached): one
//     live list of the held objects outside the caches, in every
//     namespace, enqueuing their batches. That covers a holder deleted
//     while the process was down, and a leader that died between a
//     holder's Forget and R3, which no holder pass reaches any more.
//
// The lists select by the held label, in every namespace, so a restricted
// process needs list on each protected kind cluster-wide (informer's
// preflight). They only find what to recheck: R1 to R3 read and write each
// object as for any batch.

// namespaceCached reports whether namespace's objects are in the protected
// caches.
func (s *Shared) namespaceCached(namespace string) bool {
	return len(s.o.Namespaces) == 0 || namespace == "" || slices.Contains(s.o.Namespaces, namespace)
}

// uncachedKinds returns t's protected kinds with objects outside the caches:
// the namespaced ones, when SharedOptions.Namespaces restricts the caches.
func (s *Shared) uncachedKinds(t *Tracker) []schema.GroupKind {
	if len(s.o.Namespaces) == 0 {
		return nil
	}
	var out []schema.GroupKind
	for _, pk := range t.o.Protected {
		gk := pk.GVK.GroupKind()
		m, err := s.c.Mapper.RESTMapping(gk)
		if err != nil || m.Scope.Name() != meta.RESTScopeNameNamespace {
			// Track mapped every protected kind; a cluster-scoped kind is
			// cached whatever the namespaces.
			continue
		}
		if !slices.Contains(out, gk) {
			out = append(out, gk)
		}
	}
	return out
}

// sweepMarkers queues for the janitor every object of ts's protected kinds,
// in every namespace, that carries one of their marker labels but not the
// held label.
func (s *Shared) sweepMarkers(ctx context.Context, ts []*Tracker) error {
	for _, t := range ts {
		var kinds []schema.GroupKind
		for _, pk := range t.o.Protected {
			if gk := pk.GVK.GroupKind(); !slices.Contains(kinds, gk) {
				kinds = append(kinds, gk)
			}
		}
		for _, gk := range kinds {
			items, err := s.liveList(ctx, gk, "", MarkerLabel(t.gk)+",!"+LabelHeld)
			if err != nil {
				return err
			}
			for i := range items {
				if m := &items[i]; s.needsJanitor(m) {
					s.janitor.Add(ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name})
				}
			}
		}
	}
	return nil
}

// liveHeld lists, live, the objects of gk that carry the held label, in
// namespace, or in every namespace when namespace is "": it finds what to
// recheck.
func (s *Shared) liveHeld(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	return s.liveList(ctx, gk, namespace, heldSelector())
}

// liveList lists, live and page by page, the metadata of the objects of gk
// that selector selects, in namespace, or in every namespace when namespace
// is "".
func (s *Shared) liveList(ctx context.Context, gk schema.GroupKind, namespace, selector string) ([]metav1.PartialObjectMetadata, error) {
	m, err := s.c.Mapper.RESTMapping(gk)
	if err != nil {
		return nil, fmt.Errorf("credential-use: mapping %s: %w", gk, err)
	}
	var c metadata.ResourceInterface = s.c.Metadata.Resource(m.Resource)
	if namespace != "" {
		c = s.c.Metadata.Resource(m.Resource).Namespace(namespace)
	}
	var out []metav1.PartialObjectMetadata
	opts := metav1.ListOptions{LabelSelector: selector, Limit: listPageSize}
	for {
		l, err := c.List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("credential-use: listing %s with %q outside the caches: %w", gk, selector, err)
		}
		out = append(out, l.Items...)
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}

// liveHeldIn returns the objects outside the caches that carry t's
// finalizer for holder namespace n, read live: what a batch added with no
// objects covers in a namespace the caches don't.
func (s *Shared) liveHeldIn(ctx context.Context, t *Tracker, n string) ([]ID, error) {
	var out []ID
	for _, gk := range s.uncachedKinds(t) {
		items, err := s.liveHeld(ctx, gk, n)
		if err != nil {
			return nil, err
		}
		for i := range items {
			m := &items[i]
			if _, held := holds(t, m)[n]; held {
				out = append(out, ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name})
			}
		}
	}
	return out, nil
}

// recheckUncached is the write loops' recheck of what the caches can't
// show, for the kinds ts, run at their start and after a Track while they
// run:
//
//   - the batch of every (K, N) finalizer of ts on every held object
//     outside the caches (§10.5), read with one live list per protected
//     kind;
//   - every object that carries a kind's marker label but not the held
//     label, for the janitor (§10.1): the caches select by the held label,
//     so a third party that strips it and leaves a marker leaves an object
//     no informer delivers, and scaletozero selects on the marker (§11.6).
//
// It retries a failed list with the release loop's backoff until it
// succeeds or ctx ends: a recheck that is skipped can strand a hold for
// good.
func (s *Shared) recheckUncached(ctx context.Context, ts []*Tracker) {
	for attempt := 0; ; attempt++ {
		err := s.recheckUncachedOnce(ctx, ts)
		if err == nil || ctx.Err() != nil {
			return
		}
		d := releaseBackoffBase << min(attempt, 8)
		d = min(d, releaseBackoffMax)
		for _, t := range ts {
			t.warn("credential-use: the recheck of the held objects outside the caches failed; it is retried",
				"kind", t.gk.String(), "retryIn", d.String(), "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(d):
		}
	}
}

func (s *Shared) recheckUncachedOnce(ctx context.Context, ts []*Tracker) error {
	if err := s.sweepMarkers(ctx, ts); err != nil {
		return err
	}
	byKind := map[schema.GroupKind][]*Tracker{}
	var kinds []schema.GroupKind
	for _, t := range ts {
		for _, gk := range s.uncachedKinds(t) {
			if byKind[gk] == nil {
				kinds = append(kinds, gk)
			}
			byKind[gk] = append(byKind[gk], t)
		}
	}
	for _, gk := range kinds {
		items, err := s.liveHeld(ctx, gk, "")
		if err != nil {
			return err
		}
		for i := range items {
			m := &items[i]
			if s.namespaceCached(m.Namespace) {
				continue
			}
			id := ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name}
			for _, t := range byKind[gk] {
				for n := range holds(t, m) {
					s.queue.Add(batchKey{t.gk, n}, id)
				}
			}
		}
	}
	return nil
}

// uncachedDeleting enqueues, for a namespace outside the caches that starts
// deleting, each tracker's batch for it, with no objects: heldIn fills it
// with a live list. No informer delivers the deletion of those objects.
func (s *Shared) uncachedDeleting(namespace string) {
	if s.namespaceCached(namespace) {
		return
	}
	for _, t := range s.trackerList() {
		if len(s.uncachedKinds(t)) > 0 {
			s.queue.Add(batchKey{t.gk, namespace})
		}
	}
}

// keptUncached updates t's count of the objects outside the caches that
// wait for its users, from a batch's result: the waiting gauge counts the
// cached ones from the caches on every replica, and these from what this
// process's release loop last found, so only the process that runs the
// write loops reports them.
func (t *Tracker) keptUncached(ids []ID, kept []Kept) {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if t.gone {
		return
	}
	if t.waitingUncached == nil {
		t.waitingUncached = map[ID]bool{}
	}
	changed := false
	for _, id := range ids {
		if !t.s.namespaceCached(id.Namespace) && t.waitingUncached[id] {
			delete(t.waitingUncached, id)
			changed = true
		}
	}
	for _, kp := range kept {
		if !t.s.namespaceCached(kp.P.Namespace) && !t.waitingUncached[kp.P] {
			t.waitingUncached[kp.P] = true
			changed = true
		}
	}
	if changed {
		t.reportHeldLocked()
	}
}
