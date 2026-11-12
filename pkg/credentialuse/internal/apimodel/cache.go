package apimodel

import (
	"maps"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// A Cache is one process's informer over the model: it lags the model by
// whatever events haven't been delivered yet. Match is its selector: an
// object that stops matching leaves the cache as a deletion, and one that
// starts matching enters it as an addition, as a label-selected watch does
// (§2).
type Cache struct {
	m     *Model
	match func(Key, *metav1.PartialObjectMetadata) bool
	seq   int
	objs  map[Key]*metav1.PartialObjectMetadata
	// sorted is objs' keys in order, rebuilt only when they change (Each).
	sorted   []Key
	sortedOK bool

	// OnAccess, if set, runs on every read and every change of the cache:
	// the harness's check that no lost unit of work touches it.
	OnAccess func()
}

// A Change is what one delivery did to the cache: Old is the version it had,
// New the version it has now, nil for a deletion.
type Change struct {
	Key      Key
	Old, New *metav1.PartialObjectMetadata
}

// NewCache returns an empty cache over m; List fills it.
func NewCache(m *Model, match func(Key, *metav1.PartialObjectMetadata) bool) *Cache {
	if match == nil {
		match = func(Key, *metav1.PartialObjectMetadata) bool { return true }
	}
	return &Cache{m: m, match: match, objs: map[Key]*metav1.PartialObjectMetadata{}}
}

// List is a list or a relist: the cache becomes the model's current state,
// and its watch resumes after it, skipping every event in between. It returns
// the changes, as a relist delivers them: only the final state of each object.
func (c *Cache) List() []Change {
	c.access()
	snap, seq := c.m.Snapshot()
	next := map[Key]*metav1.PartialObjectMetadata{}
	for i := range snap {
		if c.match(snap[i].Key, &snap[i].Obj) {
			next[snap[i].Key] = &snap[i].Obj
		}
	}
	keys := slices.Collect(maps.Keys(c.objs))
	for k := range next {
		if _, ok := c.objs[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, func(a, b Key) int { return strings.Compare(a.String(), b.String()) })
	var changes []Change
	for _, k := range keys {
		changes = append(changes, Change{Key: k, Old: c.objs[k], New: next[k]})
	}
	c.objs, c.seq = next, seq
	c.sortedOK = false
	return changes
}

// Pending reports whether the model has an event the cache hasn't delivered.
func (c *Cache) Pending() bool { c.access(); return c.m.LastSeq() > c.seq }

func (c *Cache) access() {
	if c.OnAccess != nil {
		c.OnAccess()
	}
}

// Next delivers the next event. ok is false when the event changed nothing
// the cache holds.
func (c *Cache) Next() (Change, bool) {
	c.access()
	evs := c.m.EventsAfter(c.seq)
	if len(evs) == 0 {
		return Change{}, false
	}
	ev := evs[0]
	c.seq = ev.Seq
	old := c.objs[ev.Key]
	obj := ev.Obj
	if ev.Type == watch.Deleted || !c.match(ev.Key, &obj) {
		if old == nil {
			return Change{}, false
		}
		delete(c.objs, ev.Key)
		c.sortedOK = false
		return Change{Key: ev.Key, Old: old}, true
	}
	if old == nil {
		c.sortedOK = false
	}
	c.objs[ev.Key] = &obj
	return Change{Key: ev.Key, Old: old, New: &obj}, true
}

// Objects returns the cached objects, sorted by key, and the log position
// the cache stands at: for the state hash, without the access hook.
func (c *Cache) Objects() ([]Event, int) {
	keys := slices.Collect(maps.Keys(c.objs))
	sortKeys(keys)
	out := make([]Event, 0, len(keys))
	for _, k := range keys {
		out = append(out, Event{Key: k, Obj: *c.objs[k]})
	}
	return out, c.seq
}

// Each calls fn on every cached object, sorted by key, without copying it,
// and returns the log position the cache stands at: Objects for the
// instrumentation, without the access hook. fn must neither change nor keep
// what it is given.
func (c *Cache) Each(fn func(k Key, o *metav1.PartialObjectMetadata)) int {
	if !c.sortedOK {
		c.sorted = slices.Collect(maps.Keys(c.objs))
		sortKeys(c.sorted)
		c.sortedOK = true
	}
	for _, k := range c.sorted {
		fn(k, c.objs[k])
	}
	return c.seq
}

// Seq returns the log position the cache stands at, without the access hook.
func (c *Cache) Seq() int { return c.seq }

// Get returns the cached version of k.
func (c *Cache) Get(k Key) (*metav1.PartialObjectMetadata, bool) {
	c.access()
	o, ok := c.objs[k]
	return o, ok
}

// In returns the cached objects of gk in namespace, sorted by name.
func (c *Cache) In(gk schema.GroupKind, namespace string) []*metav1.PartialObjectMetadata {
	c.access()
	var out []*metav1.PartialObjectMetadata
	for k, o := range c.objs {
		if k.GK == gk && k.Namespace == namespace {
			out = append(out, o)
		}
	}
	slices.SortFunc(out, func(a, b *metav1.PartialObjectMetadata) int { return strings.Compare(a.Name, b.Name) })
	return out
}
