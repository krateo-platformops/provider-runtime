package credentialuse

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// A kind that can't be mapped (§6.6): a mapper looks for one again at every
// miss, which costs a discovery. A release remembers the miss for a minute.

// countingMapper counts the mappings asked for, by kind; the kinds of back are
// mapped.
type countingMapper struct {
	meta.RESTMapper
	mu   sync.Mutex
	n    map[schema.GroupKind]int
	back map[schema.GroupKind]bool
}

func (m *countingMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n[gk]++
	if m.back[gk] {
		return &meta.RESTMapping{Resource: schema.GroupVersionResource{Group: gk.Group, Version: "v1", Resource: "gones"},
			GroupVersionKind: gk.WithVersion("v1"), Scope: meta.RESTScopeNamespace}, nil
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

func (m *countingMapper) count(gk schema.GroupKind) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n[gk]
}

func (m *countingMapper) restore(gk schema.GroupKind) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.back[gk] = true
}

var goneGK = schema.GroupKind{Group: "gone.test.krateo.io", Kind: "Gone"}

func goneIDs(n int) []ID {
	ids := make([]ID, n)
	for i := range ids {
		ids[i] = ID{Group: goneGK.Group, Kind: goneGK.Kind, Namespace: "out", Name: fmt.Sprintf("g%03d", i)}
	}
	return ids
}

func unmapped(t *testing.T) (*fakeRig, *Tracker, *countingMapper) {
	t.Helper()
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"))
	m := &countingMapper{RESTMapper: r.s.c.Mapper, n: map[schema.GroupKind]int{}, back: map[schema.GroupKind]bool{}}
	r.s.c.Mapper = m
	return r, r.track(t, widgetGK), m
}

// One mapping for the objects of a kind that can't be mapped, of any holder
// kind's batches, however many, until a minute after the miss, whatever asked
// meanwhile; another kind has its own. Each fails as it would have.
func TestAKindThatCantBeMappedIsLookedForOnce(t *testing.T) {
	r, _, m := unmapped(t)
	r.track(t, gadgetGK)
	k, other := batchKey{widgetGK, "out"}, ID{Group: goneGK.Group, Kind: "Other", Namespace: "out", Name: "o"}
	ids := goneIDs(3)
	r.s.queue.Add(k, append(slices.Clone(ids), other)...)
	r.s.queue.Add(batchKey{gadgetGK, "out"}, ids...)
	for range 2 {
		r.turn(context.Background())
	}
	if got := m.count(goneGK); got != 1 {
		t.Errorf("%d mappings for 6 objects of a kind that can't be mapped, want one", got)
	}
	if got := m.count(other.GroupKind()); got != 1 {
		t.Errorf("%d mappings for an object of another kind, want its own", got)
	}
	f := failuresOf(r.s.queue, k)
	for _, id := range append(slices.Clone(ids), other) {
		if f[id] != 1 {
			t.Errorf("%s failed %d times, want once", id, f[id])
		}
	}
	// The retries of the next minute ask again, and find the miss remembered: at
	// 1s, 3s, 7s, 15s and 31s. That of 63s looks again.
	for _, c := range []struct {
		step time.Duration
		want int
	}{{time.Second, 1}, {2 * time.Second, 1}, {4 * time.Second, 1}, {8 * time.Second, 1}, {16 * time.Second, 1}, {32 * time.Second, 2}} {
		r.clk.Step(c.step)
		for range 2 {
			r.turn(context.Background())
		}
		if got := m.count(goneGK); got != c.want {
			t.Errorf("%s later, %d mappings, want %d", c.step, got, c.want)
		}
	}
	if f := failuresOf(r.s.queue, k); f[ids[0]] != 7 {
		t.Errorf("%d failures, want 7: a remembered miss fails as another does", f[ids[0]])
	}
}

// Each object of a kind that can't be mapped counts among the turn's 64.
func TestAMissCountsInTheTurn(t *testing.T) {
	r, _, _ := unmapped(t)
	k, ids := batchKey{widgetGK, "out"}, goneIDs(65)
	r.s.queue.Add(k, ids...)
	r.turn(context.Background())
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, ids[64:]) {
		t.Errorf("after a turn %v is queued, want the 65th", got)
	}
}

// A kind that is mapped again is read once the minute is over; and what doesn't
// go through a batch, as Acquire, has no such memory, and finds it at once.
func TestAKindThatIsBack(t *testing.T) {
	r, tr, m := unmapped(t)
	k, ids := batchKey{widgetGK, "out"}, goneIDs(1)
	r.s.queue.Add(k, ids...)
	r.turn(context.Background())
	m.restore(goneGK)
	before := m.count(goneGK)
	if _, err := tr.proto.srv.Get(context.Background(), goneGK, "out", "g000"); Classify(err) != ClassNotFound || m.count(goneGK) == before {
		t.Errorf("a read outside a batch = %v, with %d mappings; want it looked for, and not found", err, m.count(goneGK)-before)
	}
	for n := range 6 {
		r.clk.Step(backoffAfter(n))
		r.turn(context.Background())
	}
	if f := failuresOf(r.s.queue, k); len(f) != 0 {
		t.Errorf("a minute after the miss, failures %v; want the object read, and gone", f)
	}
}

// A protected object of a kind that its tracker no longer lists in Protected is
// read, and released: only such batches release what is left of one.
func TestAKindOutsideProtectedIsStillReleased(t *testing.T) {
	r, _, l := quiet(t, heldObject(configMapGVK, "out", "cm", "u1", widgetGK, "out"))
	id := ID{Kind: "ConfigMap", Namespace: "out", Name: "cm"}
	r.s.queue.Add(batchKey{widgetGK, "out"}, id)
	r.turn(context.Background())
	if got := l.take(); !slices.Equal(got, []ID{id}) {
		t.Errorf("read %v, want the ConfigMap", got)
	}
	o, err := r.md.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "out", "cm")
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := metadataOf(o); !ok || hasFinalizer(m, mustFinalizer(widgetGK, "out")) {
		t.Errorf("the ConfigMap has the finalizer still: %+v", o)
	}
}
