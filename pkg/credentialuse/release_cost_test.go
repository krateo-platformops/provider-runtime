package credentialuse

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// What one release batch costs (§6.6): whoever can edit a holder's record
// decides how many objects a batch covers, and so must not decide how long it
// runs. Counts, not times.

// costViews are caches of a batch: the protected objects given, and every
// holder given, whatever its record names.
type costViews struct {
	protected map[ID]*metav1.PartialObjectMetadata
	holders   []*metav1.PartialObjectMetadata
	onNaming  func() // called for each object R0 reads
}

func (v costViews) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	m, ok := v.protected[id]
	return m, ok
}

func (v costViews) HoldersNaming(_ ID, ns string) []*metav1.PartialObjectMetadata {
	if v.onNaming != nil {
		v.onNaming()
	}
	var out []*metav1.PartialObjectMetadata
	for _, h := range v.holders {
		if h.Namespace == ns {
			out = append(out, h)
		}
	}
	return out
}

func (costViews) NamespaceDeleting(string) (bool, bool) { return false, true }

// someIDs are n Secrets of namespace ns, whose names begin with prefix.
func someIDs(ns, prefix string, n int) []ID {
	ids := make([]ID, n)
	for i := range ids {
		ids[i] = secretID(ns, fmt.Sprintf("%s%03d", prefix, i))
	}
	return ids
}

// addHolder adds holder ns/name to the model, with record as its record's text
// ("-" for none), and returns it as a cache has it.
func addHolder(r *rig, name, record string, deleting bool) *metav1.PartialObjectMetadata {
	m := metav1.ObjectMeta{Finalizers: []string{ownerFinalizer}}
	if record != "-" {
		m.Annotations = map[string]string{AnnotationRefs: record}
	}
	must(r.m.Create(widgetKey("ns", name), m))
	if deleting {
		must(r.m.Delete(widgetKey("ns", name)))
	}
	h, err := r.m.Get(widgetKey("ns", name))
	must(err)
	return h
}

// parsesOf counts, by holder, the records p's releases read.
func parsesOf(p *protocol) map[string]int {
	parses := map[string]int{}
	p.readRecord = func(h metav1.Object) Record {
		parses[h.GetName()]++
		return ReadRecord(h)
	}
	return parses
}

func addHeld(r *rig, id ID) {
	must(r.m.Create(keyOf(id), metav1.ObjectMeta{
		Finalizers:  []string{fNS},
		Annotations: map[string]string{fNS: "e0"},
		Labels:      map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"},
	}))
}

// A batch reads each holder's record once, however many objects it covers,
// whether R0 or R2 asks, and whether it can be parsed.
func TestABatchParsesARecordOnce(t *testing.T) {
	const n = 50
	ids := someIDs("ns", "forged-", n)
	same, err := EncodeRecord(ids)
	if err != nil {
		t.Fatal(err)
	}
	other := func(i int) string {
		rec, _ := EncodeRecord(append(slices.Clone(ids), secretID("ns", fmt.Sprintf("z%03d", i))))
		return rec
	}
	for what, c := range map[string]struct {
		records  []string
		deleting bool
		held     bool // the objects are there, so R2 is reached
	}{
		"one holder":                 {[]string{same}, false, false},
		"a deleting holder":          {[]string{same}, true, false},
		"holders that name the same": {[]string{same, same, same}, false, false},
		"other records":              {[]string{other(0), other(1), other(2)}, false, false},
		"unparseable":                {[]string{"[", "{"}, false, false},
		"R0 and R2":                  {[]string{other(0), other(1)}, false, true},
		"R0 and R2, deleting":        {[]string{other(0), other(1)}, true, true},
		"R0 and R2, unparseable":     {[]string{"["}, false, true},
	} {
		r := newRig(t)
		v := costViews{}
		for i, rec := range c.records {
			v.holders = append(v.holders, addHolder(r, fmt.Sprintf("h%d", i), rec, c.deleting))
		}
		if c.held {
			for _, id := range ids {
				addHeld(r, id)
			}
		}
		r.views = v
		parses := parsesOf(r.p)
		res, err := r.p.release(context.Background(), "ns", ids)
		if err != nil || len(res.Released) != 0 {
			t.Errorf("%s: %d released, %v", what, len(res.Released), err)
		}
		if c.held && len(res.Decisions) != n {
			t.Errorf("%s: R2 decided %d, want %d", what, len(res.Decisions), n)
		}
		if len(parses) == 0 {
			t.Errorf("%s: no record was read", what)
		}
		for h, got := range parses {
			if got > 1 {
				t.Errorf("%s: the record of %s was parsed %d times, want once", what, h, got)
			}
		}
	}
}

// What one record names is never read as another's, in R0 or in R2: a holder
// counts for the objects its own record names, none without a record, and all
// with one that can't be parsed, as an empty one can't.
func TestABatchKeepsRecordsApart(t *testing.T) {
	a, b, c := secretID("ns", "a"), secretID("ns", "b"), secretID("ns", "c")
	names := func(ids ...ID) string {
		rec, _ := EncodeRecord(ids)
		return rec
	}
	r := newRig(t)
	v := costViews{protected: map[ID]*metav1.PartialObjectMetadata{}}
	for _, h := range []struct {
		name, record string
		deleting     bool
	}{
		{"ab", names(a, b), false},
		{"b", names(b), false},
		{"b2", names(b), true},
		{"none", "-", false},
		{"nothing", names(), false},
		{"elsewhere", names(secretID("ns", "d")), false},
		{"empty", "", false},
	} {
		v.holders = append(v.holders, addHolder(r, h.name, h.record, h.deleting))
	}
	want := map[ID][2]int{a: {2, 0}, b: {4, 1}, c: {1, 0}}
	for _, id := range []ID{a, b, c} {
		addHeld(r, id)
	}
	r.views = v
	// R2's reading: the caches don't have the objects, so R0 settles nothing.
	res, err := r.p.release(context.Background(), "ns", []ID{a, b, c})
	if err != nil || len(res.Decisions) != 3 {
		t.Fatalf("release = %+v, %v", res, err)
	}
	for _, d := range res.Decisions {
		if got := [2]int{d.Input.Users, d.Input.DeletingUsers}; got != want[d.P] {
			t.Errorf("R2 of %s: %v users and deleting users, want %v", d.P, got, want[d.P])
		}
	}
	// R0's: they are being deleted, in their grace, and cached, so R0 keeps b
	// for its deleting user, and R1 the others for the grace.
	for _, id := range []ID{a, b, c} {
		must(r.m.Delete(keyOf(id)))
		m := r.get(t, keyOf(id))
		v.protected[id] = m
	}
	res, err = r.p.release(context.Background(), "ns", []ID{a, b, c})
	if err != nil || len(res.Kept) != 3 {
		t.Fatalf("release = %+v, %v", res, err)
	}
	for _, k := range res.Kept {
		if got := [2]int{len(k.Users), len(k.DeletingUsers)}; got != want[k.P] {
			t.Errorf("R0 of %s: %v users and deleting users, want %v", k.P, got, want[k.P])
		}
	}
}

// A costLog is what batches on a fakeRig asked the API server for.
type costLog struct {
	mu     sync.Mutex
	gets   []ID // R1's
	lists  int  // R2's
	before func(ctx context.Context, c Call) error
}

// log counts r's calls from now on: call it before Track.
func (r *fakeRig) log() *costLog {
	l := &costLog{}
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(ctx context.Context, c Call) error {
			l.mu.Lock()
			switch {
			case c.Op == "get" && c.GK != namespaceGroupKind:
				l.gets = append(l.gets, ID{Group: c.GK.Group, Kind: c.GK.Kind, Namespace: c.Namespace, Name: c.Name})
			case c.Op == "list":
				l.lists++
			}
			before := l.before
			l.mu.Unlock()
			if before != nil {
				return before(ctx, c)
			}
			return nil
		}}
	}
	return l
}

// take returns the GETs since the last take.
func (l *costLog) take() []ID {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.gets
	l.gets = nil
	return out
}

func (l *costLog) setBefore(f func(ctx context.Context, c Call) error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.before = f
}

// turn takes the next batch, and runs it as a worker does.
func (r *fakeRig) turn(ctx context.Context) (batchKey, bool) {
	if r.s.queue.Len() == 0 {
		return batchKey{}, false
	}
	k, ids, ok := r.s.queue.Get(ctx)
	if !ok {
		return k, false
	}
	r.s.releaseBatch(ctx, k, ids)
	r.s.queue.Done(k)
	return k, true
}
