package credentialuse

import (
	"context"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// The missing-ref prober (§10.3, R6). A recreated object, such as a Secret
// deleted and created again for rotation, isn't labelled, so no
// label-scoped informer sees it. The tracker keeps the refs its holders'
// passes last found Missing, and the prober checks them live, each on its
// own backoff, and enqueues the holders of every ref it finds. Their next
// pass holds the new incarnation (U5).

const (
	// probeTick is how often the prober looks for refs that are due. Each
	// ref's own backoff, from Options.ProbeInterval, decides when it is.
	probeTick = time.Second
	// probeBackoffMax caps a ref's backoff (L9).
	probeBackoffMax = 5 * time.Minute
	// probeGetLimit is the most due names of one (kind, namespace) that are
	// probed by GET; more take one LIST (§10.3).
	probeGetLimit = 3
)

// missingRefs are a tracker's Missing refs, with the holders that found
// each one Missing, and its backoff.
type missingRefs struct {
	mu       sync.Mutex
	refs     map[ID]*missingRef
	byHolder map[types.NamespacedName][]ID
}

type missingRef struct {
	holders map[types.NamespacedName]bool
	backoff time.Duration
	due     time.Time
}

func newMissingRefs() *missingRefs {
	return &missingRefs{refs: map[ID]*missingRef{}, byHolder: map[types.NamespacedName][]ID{}}
}

// set records that holder's latest pass found ids Missing: each is due
// after base, the shortest backoff, so that a ref under active use is
// probed often. The holder stops waiting for any ref it no longer finds
// Missing, and a ref no holder waits for is dropped.
func (m *missingRefs) set(holder types.NamespacedName, ids []ID, now time.Time, base time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.byHolder[holder] {
		if slices.Contains(ids, id) {
			continue
		}
		if r := m.refs[id]; r != nil {
			delete(r.holders, holder)
			if len(r.holders) == 0 {
				delete(m.refs, id)
			}
		}
	}
	for _, id := range ids {
		r := m.refs[id]
		if r == nil {
			r = &missingRef{holders: map[types.NamespacedName]bool{}}
			m.refs[id] = r
		}
		r.holders[holder] = true
		r.backoff, r.due = base, now.Add(base)
	}
	if len(ids) == 0 {
		delete(m.byHolder, holder)
	} else {
		m.byHolder[holder] = slices.Clone(ids)
	}
}

// A probeGroup is one protected kind in one namespace.
type probeGroup struct {
	gk schema.GroupKind
	ns string
}

// due returns the refs due at now, by kind and namespace.
func (m *missingRefs) due(now time.Time) map[probeGroup][]ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[probeGroup][]ID{}
	for id, r := range m.refs {
		if !now.Before(r.due) {
			g := probeGroup{id.GroupKind(), id.Namespace}
			out[g] = append(out[g], id)
		}
	}
	for g := range out {
		out[g] = SortedIDs(out[g])
	}
	return out
}

// backOff records a probe that found id still missing: its backoff
// doubles, up to probeBackoffMax.
func (m *missingRefs) backOff(id ID, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.refs[id]; r != nil {
		r.backoff = min(2*r.backoff, probeBackoffMax)
		r.due = now.Add(r.backoff)
	}
}

// retry records a probe that couldn't tell: id is due again after its
// current backoff, unchanged.
func (m *missingRefs) retry(id ID, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.refs[id]; r != nil {
		r.due = now.Add(r.backoff)
	}
}

// found records a probe that found id, and returns its holders. It stays
// until their passes hold it, due again after base in case none does.
func (m *missingRefs) found(id ID, now time.Time, base time.Duration) []types.NamespacedName {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.refs[id]
	if r == nil {
		return nil
	}
	r.backoff, r.due = base, now.Add(base)
	out := make([]types.NamespacedName, 0, len(r.holders))
	for h := range r.holders {
		out = append(out, h)
	}
	slices.SortFunc(out, func(a, b types.NamespacedName) int {
		if a.Namespace != b.Namespace {
			if a.Namespace < b.Namespace {
				return -1
			}
			return 1
		}
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	})
	return out
}

// runProber probes every tracker's due refs every probeTick, until ctx ends.
func (s *Shared) runProber(ctx context.Context) {
	tick := s.clock.NewTicker(probeTick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			for _, t := range s.trackerList() {
				t.probe(ctx)
			}
		}
	}
}

// probe checks t's due Missing refs live (§10.3). Per (kind, namespace),
// probeGetLimit or fewer names are a GET each; more are one paginated,
// metadata-only LIST, and a LIST that RBAC refuses, as under resourceNames
// RBAC, falls back to GETs. A PerName kind takes GETs only. It enqueues the holders of every ref it finds.
func (t *Tracker) probe(ctx context.Context) {
	now := t.s.clock.Now()
	srv := t.proto.srv
	for g, ids := range t.missing.due(now) {
		var names map[string]bool
		// A PerName kind is probed with GETs only: the list would be
		// forbidden under resourceNames RBAC (§13 shape 1).
		// Nor is a namespaced kind's group without a namespace listed: the
		// list is of every one, and would find other objects of those names.
		if len(ids) > probeGetLimit && !t.isPerName(g.gk) && (g.ns != "" || !t.namespaced(g.gk)) {
			list, err := srv.List(ctx, g.gk, g.ns)
			switch {
			case err == nil:
				names = map[string]bool{}
				for i := range list {
					names[list[i].Name] = true
				}
			case Classify(err) == ClassRBAC:
				// GETs below.
			default:
				for _, id := range ids {
					t.missing.retry(id, now)
				}
				continue
			}
		}
		for _, id := range ids {
			var exists bool
			if names != nil {
				exists = names[id.Name]
			} else {
				_, err := srv.Get(ctx, g.gk, g.ns, id.Name)
				switch Classify(err) {
				case ClassNone:
					exists = true
				case ClassNotFound:
				default:
					t.missing.retry(id, now)
					continue
				}
			}
			if !exists {
				t.missing.backOff(id, now)
				continue
			}
			for _, h := range t.missing.found(id, now, t.o.ProbeInterval) {
				t.enqueue(h)
			}
		}
	}
}

// namespaced reports whether gk is a namespaced kind, by the mapper.
func (t *Tracker) namespaced(gk schema.GroupKind) bool {
	if t.s.c.Mapper == nil {
		return false
	}
	m, err := t.s.c.Mapper.RESTMapping(gk)
	return err == nil && m.Scope.Name() == meta.RESTScopeNameNamespace
}
