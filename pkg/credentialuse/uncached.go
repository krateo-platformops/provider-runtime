package credentialuse

import (
	"context"
	"errors"
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

// uncachedKinds returns t's protected kinds with objects outside the caches,
// worked out once, when Track builds t (computeUncachedKinds): the
// Namespace informer's handler asks, and mustn't wait on the mapper.
func (s *Shared) uncachedKinds(t *Tracker) []schema.GroupKind { return t.uncached }

// computeUncachedKinds returns t's protected kinds with objects outside the
// caches: the namespaced ones, when SharedOptions.Namespaces restricts the
// caches.
func (s *Shared) computeUncachedKinds(t *Tracker) []schema.GroupKind {
	if len(s.o.Namespaces) == 0 {
		return nil
	}
	var out []schema.GroupKind
	for _, pk := range t.o.Protected {
		if pk.Watch == PerName {
			// Its per-name watches cover every namespace, and the lists
			// would be forbidden under resourceNames RBAC (§13 shape 1).
			continue
		}
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

// sweepKinds returns t's protected kinds that sweepMarkers lists. A PerName
// kind is left out: the list would be forbidden under resourceNames RBAC, and
// its per-name watches show the janitor every object its index names, labelled
// or not (§13 shape 1).
func sweepKinds(t *Tracker) []schema.GroupKind {
	var kinds []schema.GroupKind
	for _, pk := range t.o.Protected {
		if pk.Watch == PerName {
			continue
		}
		if gk := pk.GVK.GroupKind(); !slices.Contains(kinds, gk) {
			kinds = append(kinds, gk)
		}
	}
	return kinds
}

// sweepMarkers queues for the janitor every object of gk, in every namespace,
// that carries t's marker label but not the held label.
func (s *Shared) sweepMarkers(ctx context.Context, t *Tracker, gk schema.GroupKind) error {
	items, err := s.liveList(ctx, gk, "", MarkerLabel(t.gk)+",!"+LabelHeld)
	if err != nil {
		return err
	}
	for i := range items {
		if m := &items[i]; s.needsJanitor(m) {
			s.janitor.Add(ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name})
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
	return listLive(ctx, s.c, gk, namespace, selector)
}

// listHolders lists, live and page by page, the metadata of t's holders in
// every namespace, at their declared version (§6.2).
func (s *Shared) listHolders(ctx context.Context, t *Tracker) ([]metav1.PartialObjectMetadata, error) {
	return listResource(ctx, s.c, t.gk, t.gvr, "", "")
}

// listLive is liveList on clients cl, for DisownPrefix, which has no Shared.
func listLive(ctx context.Context, cl Clients, gk schema.GroupKind, namespace, selector string) ([]metav1.PartialObjectMetadata, error) {
	m, err := cl.Mapper.RESTMapping(gk)
	if err != nil {
		return nil, fmt.Errorf("credential-use: mapping %s: %w", gk, err)
	}
	return listResource(ctx, cl, gk, m.Resource, namespace, selector)
}

// listResource is listLive of gvr, gk's resource.
func listResource(ctx context.Context, cl Clients, gk schema.GroupKind, gvr schema.GroupVersionResource, namespace, selector string) ([]metav1.PartialObjectMetadata, error) {
	if !pathSegment(namespace) {
		return nil, nil // it can't exist
	}
	var c metadata.ResourceInterface = cl.Metadata.Resource(gvr)
	if namespace != "" {
		c = cl.Metadata.Resource(gvr).Namespace(namespace)
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
// objects covers in a namespace the caches don't. A kind it can't list doesn't
// stop the others: it returns theirs, with the error.
func (s *Shared) liveHeldIn(ctx context.Context, t *Tracker, n string) ([]ID, error) {
	var out []ID
	var errs []error
	for _, gk := range s.uncachedKinds(t) {
		items, err := s.liveHeld(ctx, gk, n)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for i := range items {
			m := &items[i]
			if _, held := holds(t, m)[n]; held {
				out = append(out, ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name})
			}
		}
	}
	return out, errors.Join(errs...)
}

// recheckUncached is the write loops' recheck of what the caches can't
// show, for the kinds ts, run at their start and after a Track while they
// run:
//
//   - the batch of every (K, N) finalizer of ts on every held object
//     outside the caches (§10.5), read with one live list per protected
//     kind;
//   - for the PerName kinds, the batch of every (K, N) finalizer on every
//     object the index names, read with a GET each, and the prune of the
//     lines that name objects without one (recheckIndex, §10.6.5);
//   - every object that carries a kind's marker label but not the held
//     label, for the janitor (§10.1): the caches select by the held label,
//     so a third party that strips it and leaves a marker leaves an object
//     no informer delivers, and scaletozero selects on the marker (§11.6).
//
// It retries what failed of it, and only that, with the release loop's backoff
// until it succeeds or ctx ends: a recheck that is skipped can strand a hold
// for good.
func (s *Shared) recheckUncached(ctx context.Context, ts []*Tracker) {
	left := s.recheckParts(ts)
	for attempt := 0; ; attempt++ {
		d := min(releaseBackoffBase<<min(attempt, 8), releaseBackoffMax)
		var failed []recheckPart
		for _, p := range left {
			err := p.run(ctx)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				continue
			}
			failed = append(failed, p)
			for _, t := range p.ts {
				t.warn("credential-use: the recheck of the held objects outside the caches failed; it is retried",
					"kind", t.gk.String(), "retryIn", d.String(), "error", err.Error())
			}
		}
		if left = failed; len(left) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(d):
		}
	}
}

// A recheckPart is one part of recheckUncached, for trackers ts. Each runs
// whatever fails before it, and is retried alone: an object, or a kind, one
// tracker can't read must stop neither another's recheck nor §10.5's lists,
// and must not repeat them.
type recheckPart struct {
	ts  []*Tracker
	run func(context.Context) error
}

func (s *Shared) recheckParts(ts []*Tracker) []recheckPart {
	var parts []recheckPart
	for _, t := range ts {
		for _, gk := range sweepKinds(t) {
			parts = append(parts, recheckPart{[]*Tracker{t}, func(ctx context.Context) error { return s.sweepMarkers(ctx, t, gk) }})
		}
	}
	for _, t := range ts {
		if t.idx != nil {
			done := map[ID]bool{}
			parts = append(parts, recheckPart{[]*Tracker{t}, func(ctx context.Context) error { return s.recheckIndex(ctx, t, done) }})
		}
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
		parts = append(parts, recheckPart{byKind[gk], func(ctx context.Context) error { return s.recheckHeld(ctx, gk, byKind[gk]) }})
	}
	return parts
}

// recheckHeld enqueues the batch of every (K, N) finalizer of ts on every held
// object of gk outside the caches (§10.5).
func (s *Shared) recheckHeld(ctx context.Context, gk schema.GroupKind, ts []*Tracker) error {
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
		for _, t := range ts {
			for n := range holds(t, m) {
				s.queue.Add(batchKey{t.gk, n}, id)
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
		if !t.cachedID(id) && t.waitingUncached[id] {
			delete(t.waitingUncached, id)
			changed = true
		}
	}
	for _, kp := range kept {
		if !t.cachedID(kp.P) && !t.waitingUncached[kp.P] {
			t.waitingUncached[kp.P] = true
			changed = true
		}
	}
	if changed {
		t.reportHeldLocked()
	}
}

// clearUncached forgets what the release loop counted outside the caches,
// when the write loops stop.
func (t *Tracker) clearUncached() {
	t.heldMu.Lock()
	defer t.heldMu.Unlock()
	if t.gone || len(t.waitingUncached) == 0 {
		return
	}
	t.waitingUncached = nil
	t.reportHeldLocked()
}
