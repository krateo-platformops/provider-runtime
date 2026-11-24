package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// The time a turn's reads take (§6.6): none begins once the turn's time is over,
// and each that began has a time of its own, so that one that doesn't answer
// holds a worker for that long, and delays nothing else for good.

// hang makes the GETs of the names hangs says wait for the end of their context.
func hang(hangs func(c Call) bool) func(ctx context.Context, c Call) error {
	return func(ctx context.Context, c Call) error {
		if c.Op == "get" && hangs(c) {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
}

func named(name string) func(Call) bool { return func(c Call) bool { return c.Name == name } }

// limitsRig is a rig whose reads may take 30ms each, and whose turns' reads
// begin within turnTime; the context it returns ends what nothing else does.
func limitsRig(t *testing.T, turnTime time.Duration, hangs func(Call) bool) (r *rig, ctx context.Context) {
	t.Helper()
	r = newRig(t)
	r.p.srv = hookServer{server: r.srv, before: hang(hangs)}
	r.p.turnTime = turnTime
	r.p.readLimit = func(string, ID) time.Duration { return 30 * time.Millisecond }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return r, ctx
}

// A read that doesn't answer fails, alone, once its own time is over; what the
// turn's time left unread is the rest, and the next turn reads it.
func TestAReadThatDoesntAnswer(t *testing.T) {
	r, ctx := limitsRig(t, 10*time.Millisecond, named("s000"))
	ids := someIDs("ns", "s", 5)
	res, err := r.p.release(ctx, "ns", ids)
	if err == nil || !slices.Equal(res.Failed, ids[:1]) || !slices.Equal(res.Rest, ids[1:]) || len(res.Bare) != 0 {
		t.Errorf("failed %v, rest %v, bare %v, %v; want the one that didn't answer failed, and the others the rest", res.Failed, res.Rest, res.Bare, err)
	}
	res, err = r.p.release(ctx, "ns", ids[1:])
	if err != nil || !slices.Equal(res.Bare, ids[1:]) {
		t.Errorf("the next turn: %+v, %v; want the others read", res, err)
	}
	// In the middle of a turn, whose time isn't over, those after it are read.
	r, ctx = limitsRig(t, 2*time.Second, named("s002"))
	res, err = r.p.release(ctx, "ns", ids)
	if err == nil || !slices.Equal(res.Failed, ids[2:3]) || len(res.Rest) != 0 || len(res.Bare) != 4 {
		t.Errorf("failed %v, rest %v, bare %v, %v; want the one that didn't answer failed, and the others read", res.Failed, res.Rest, res.Bare, err)
	}
}

// The first that the caches show, and the first that they don't, are read in
// every turn, however long the others of it took.
func TestEachSortsFirstIsRead(t *testing.T) {
	for _, hanging := range []string{"s000", "o000"} {
		r, ctx := limitsRig(t, 10*time.Millisecond, named(hanging))
		shown, others := someIDs("ns", "s", 2), someIDs("ns", "o", 2)
		v := costViews{protected: map[ID]*metav1.PartialObjectMetadata{}}
		for _, id := range shown {
			v.protected[id] = heldObject(secretGVK, "ns", id.Name, "u", widgetGK, "ns")
		}
		r.views = v
		// Others were queued first.
		res, err := r.p.release(ctx, "ns", slices.Concat(others, shown))
		if err == nil {
			t.Errorf("%s: no error", hanging)
		}
		read := SortedIDs(slices.Concat(res.Failed, res.Bare))
		if !slices.Contains(read, shown[0]) || !slices.Contains(read, others[0]) {
			t.Errorf("%s doesn't answer: read %v, rest %v; want the first of each sort read", hanging, read, res.Rest)
		}
		if hanging == "s000" && !slices.Equal(res.Rest, []ID{shown[1], others[1]}) {
			t.Errorf("rest %v, want the second of each sort", res.Rest)
		}
	}
}

// Two keys whose reads don't answer hold both workers for no longer than a
// read's time: the rest of each, and another key, are read.
func TestTheWorkersGoOnBesideReadsThatDontAnswer(t *testing.T) {
	r, tr, l := quiet(t)
	tr.proto.turnTime = 10 * time.Millisecond
	tr.proto.readLimit = func(string, ID) time.Duration { return 30 * time.Millisecond }
	l.setBefore(hang(named("a-hangs")))
	want := map[ID]bool{}
	for _, ns := range []string{"out", "out2"} {
		ids := append([]ID{secretID(ns, "a-hangs")}, someIDs(ns, "s", 3)...)
		r.s.queue.Add(batchKey{widgetGK, ns}, ids...)
		for _, id := range ids[1:] {
			want[id] = true
		}
	}
	r.s.queue.Add(batchKey{widgetGK, "out3"}, secretID("out3", "s"))
	want[secretID("out3", "s")] = true

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range releaseWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.s.releaseWorker(ctx)
		}()
	}
	for end := time.Now().Add(10 * time.Second); len(want) > 0 && time.Now().Before(end); time.Sleep(time.Millisecond) {
		for _, id := range l.take() {
			delete(want, id)
		}
	}
	cancel()
	wg.Wait()
	if len(want) > 0 {
		t.Errorf("not read in 10s: %v", SortedIDs(keysOf(want)))
	}
}

// A read's time is 10s, and twice that after each of its object's failures, to
// 60s, the API server's own: an object whose reads are slow, as one behind a slow
// conversion webhook, is delayed, never lost. One that was decided begins again.
// The patches and the lists have no such time.
func TestAReadsTimeGrows(t *testing.T) {
	r, _, l := quiet(t, heldObject(secretGVK, "out", "free", "u1", widgetGK, "out"))
	k, slow := batchKey{widgetGK, "out"}, secretID("out", "slow")
	var mu sync.Mutex
	var limits []time.Duration
	fails := true
	l.setBefore(func(ctx context.Context, c Call) error {
		mu.Lock()
		defer mu.Unlock()
		end, limited := ctx.Deadline()
		switch {
		case c.Op != "get":
			if limited {
				t.Errorf("a %s has a deadline, in %s", c.Op, time.Until(end))
			}
		case c.Name == "slow" && !limited:
			limits = append(limits, -1)
		case c.Name == "slow":
			limits = append(limits, time.Until(end))
			if fails {
				return unservedError()
			}
		}
		return nil
	})
	step := func(d time.Duration) {
		r.clk.Step(d)
		r.turn(context.Background())
	}
	r.s.queue.Add(k, slow)
	step(0)
	for n := range 4 {
		step(backoffAfter(n))
	}
	fails = false
	step(backoffAfter(4))
	r.s.queue.Add(k, slow, secretID("out", "free"))
	step(0)
	if r.heldBy(t, widgetGK, "out", "free") {
		t.Error("the Secret beside it wasn't released")
	}
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second, 60 * time.Second, 60 * time.Second, 10 * time.Second}
	if len(limits) != len(want) {
		t.Fatalf("%d reads: %v, want %d", len(limits), limits, len(want))
	}
	for i, got := range limits {
		if got > want[i] || got < want[i]-5*time.Second {
			t.Errorf("read %d had %s, want %s", i+1, got, want[i])
		}
	}
}

// A Namespace that doesn't answer fails the objects in it, once for all of them;
// the turn's time over, the other Namespaces' are the rest, and what needs no
// Namespace is decided.
func TestANamespaceThatDoesntAnswer(t *testing.T) {
	r, ctx := limitsRig(t, 10*time.Millisecond, func(c Call) bool { return c.GK == namespaceGroupKind && c.Name == "a" })
	a1, a2, b, live := secretID("a", "s1"), secretID("a", "s2"), secretID("b", "s"), secretID("ns", "s")
	for _, ns := range []string{"a", "b"} {
		must(r.m.Create(nsKey(ns), metav1.ObjectMeta{}))
	}
	for _, id := range []ID{a1, a2, b, live} {
		addHeld(r, id)
		if id != live {
			must(r.m.Delete(keyOf(id)))
		}
	}
	r.p.readLimit = func(string, ID) time.Duration { return 5 * time.Second }
	r.p.namespaceLimit = 30 * time.Millisecond
	var got []string
	r.p.srv = hookServer{server: r.p.srv, before: func(_ context.Context, c Call) error {
		if c.Op == "get" && c.GK == namespaceGroupKind {
			got = append(got, c.Name)
		}
		return nil
	}}
	res, err := r.p.release(ctx, "ns", []ID{a1, a2, b, live})
	if err == nil || !slices.Equal(res.Failed, []ID{a1, a2}) || !slices.Equal(res.Rest, []ID{b}) || !slices.Equal(res.Released, []ID{live}) {
		t.Errorf("failed %v, rest %v, released %v, %v; want those of a failed, b the rest, and the live one released", res.Failed, res.Rest, res.Released, err)
	}
	if !slices.Equal(got, []string{"a"}) {
		t.Errorf("the Namespaces read are %v, want a once", got)
	}
	if err != nil && strings.Contains(err.Error(), "\n") {
		t.Errorf("the error has several lines, want one for the Namespace: %v", err)
	}
	if res, err := r.p.release(ctx, "ns", []ID{b}); err != nil || !slices.Equal(res.Released, []ID{b}) {
		t.Errorf("the next turn: %+v, %v; want b released", res, err)
	}
}

// clientAt is a real metadata client of qps requests a second, with no burst,
// against an API server that has nothing, and notes the paths it was asked for.
func clientAt(t *testing.T, qps float32) (srv clientServer, asked func() []string) {
	t.Helper()
	var mu sync.Mutex
	var paths []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		paths = append(paths, req.URL.Path)
		mu.Unlock()
		st := apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, path.Base(req.URL.Path)).Status()
		st.APIVersion, st.Kind = "v1", "Status"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(st)
	}))
	t.Cleanup(api.Close)
	md, err := metadata.NewForConfig(&rest.Config{Host: api.URL, QPS: qps, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	return clientServer{md: md, mapper: newFakeRig(t).s.c.Mapper}, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(paths)
	}
}

// The premise of a turn's time (§6.6): a client's own rate limiter refuses a read
// whose wait would pass its deadline at once, unsent, with an error that is
// neither the context's nor the API server's. So reads that shared a deadline
// would fail unread, and nothing in the error would say so.
func TestPremiseAClientRefusesAReadItCantWaitFor(t *testing.T) {
	srv, asked := clientAt(t, 0.2)
	if _, err := srv.Get(context.Background(), secretGroupKind, "ns", "first"); Classify(err) != ClassNotFound {
		t.Fatalf("the first read: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // the next token is 5s away
	defer cancel()
	_, err := srv.Get(ctx, secretGroupKind, "ns", "second")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Errorf("the refused read: %v, its context %v; want another error, and the context live", err, ctx.Err())
	}
	if _, status := statusOf(err); status {
		t.Errorf("the refused read has an API status: %v", err)
	}
	if got := asked(); len(got) != 1 {
		t.Errorf("the API server was asked for %v, want the first alone", got)
	}
}

// With a real client of few requests a second, no read of a turn fails unread:
// what its time leaves is the rest, and the next turns read it, each object once.
func TestARealSlowClientsTurns(t *testing.T) {
	srv, asked := clientAt(t, 20)
	r := newRig(t)
	r.p.srv = srv
	r.p.turnTime = 100 * time.Millisecond
	ids := someIDs("ns", "s", 10)
	turns := 0
	for left := ids; len(left) > 0; turns++ {
		res, err := r.p.release(context.Background(), "ns", left)
		if err != nil || len(res.Failed) != 0 || len(res.Bare) == 0 || !slices.Equal(slices.Concat(res.Bare, res.Rest), left) {
			t.Fatalf("turn %d of %v: read %v, failed %v, rest %v, %v", turns, left, res.Bare, res.Failed, res.Rest, err)
		}
		left = res.Rest
	}
	if turns < 2 {
		t.Errorf("%d turn, want several: ten reads take 450ms", turns)
	}
	got := asked()
	for i, p := range got {
		got[i] = path.Base(p)
	}
	var want []string
	for _, id := range ids {
		want = append(want, id.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("the API server was asked for %v, want each once, in order: %v", got, want)
	}
}

// pagedMetadata is a metadata client whose lists have pages pages, and whose
// page at hangs waits for the end of its context; it notes each page's deadline.
type pagedMetadata struct {
	metadata.Interface
	pages, hangs int
	ends         []time.Time
}

type pagedLists struct {
	metadata.Getter
	m *pagedMetadata
}

func (m *pagedMetadata) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return pagedLists{m.Interface.Resource(gvr), m}
}

func (p pagedLists) Namespace(string) metadata.ResourceInterface { return p }

func (p pagedLists) List(ctx context.Context, _ metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	end, _ := ctx.Deadline()
	p.m.ends = append(p.m.ends, end)
	n := len(p.m.ends)
	if n == p.m.hangs {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	l := &metav1.PartialObjectMetadataList{Items: make([]metav1.PartialObjectMetadata, 1)}
	if n < p.m.pages {
		l.Continue = "more"
	}
	return l, nil
}

// Each page of a list has a minute, and a list has as many as it needs: one that
// takes long is delayed by no more than the page that doesn't answer.
func TestAListsPagesHaveATime(t *testing.T) {
	r := newFakeRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	m := &pagedMetadata{Interface: r.md, pages: 3}
	now := time.Now()
	items, err := clientServer{md: m, mapper: r.s.c.Mapper}.List(ctx, secretGroupKind, "ns")
	if err != nil || len(items) != 3 || len(m.ends) != 3 {
		t.Fatalf("%d items of %d pages, %v; want 3 of each", len(items), len(m.ends), err)
	}
	for i, end := range m.ends {
		if d := end.Sub(now); d <= 0 || d > 61*time.Second {
			t.Errorf("page %d ends in %s, want a minute", i+1, d)
		}
		if i > 0 && !end.After(m.ends[i-1]) {
			t.Errorf("page %d ends at %s, no later than the one before it: the minute is the list's", i+1, end)
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m = &pagedMetadata{Interface: r.md, pages: 3, hangs: 2}
	_, err = clientServer{md: m, mapper: r.s.c.Mapper, pageTime: 20 * time.Millisecond}.List(ctx, secretGroupKind, "ns")
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Errorf("a page that doesn't answer: %v, with the caller's context %v; want the page's deadline alone", err, ctx.Err())
	}
}
