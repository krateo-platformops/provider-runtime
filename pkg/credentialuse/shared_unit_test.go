package credentialuse

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"
	testingclock "k8s.io/utils/clock/testing"
)

var batchNS = batchKey{Holder: widgetGK, Namespace: "ns"}

func idOf(name string) ID { return SecretRef("ns", name).ID() }

// The (K, N) queue buffers from its creation: adds before anyone takes are
// kept, deduplicated by key, and accumulate the objects they cover.
func TestBatchQueueBuffersAndDeduplicates(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	q.Add(batchNS, idOf("a"))
	q.Add(batchNS, idOf("b"))
	q.Add(batchNS, idOf("a"))
	q.Add(batchKey{widgetGK, "other"})
	if q.Len() != 2 {
		t.Fatalf("Len = %d, want 2: one per key", q.Len())
	}
	k, ids, ok := q.Get(context.Background())
	if !ok || k != batchNS || !slices.Equal(ids, []ID{idOf("a"), idOf("b")}) {
		t.Fatalf("Get = %v, %v, %v; want the first key with a and b", k, ids, ok)
	}
}

// A key added while its batch is worked on runs again once it is done, with
// only what was added meanwhile; it is never taken twice at once.
func TestBatchQueueRequeuesWhatArrivesDuringWork(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	q.Add(batchNS, idOf("a"))
	k, _, _ := q.Get(context.Background())
	q.Add(batchNS, idOf("b"))
	if q.Len() != 0 {
		t.Fatalf("Len = %d while the key is worked on, want 0", q.Len())
	}
	q.Done(k)
	if q.Len() != 1 {
		t.Fatalf("Len = %d after Done, want the key queued again", q.Len())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, ids, ok := q.Get(ctx)
	if !ok || !slices.Equal(ids, []ID{idOf("b")}) {
		t.Fatalf("after Done, Get = %v, %v; want b only", ids, ok)
	}
	q.Done(k)
	if q.Len() != 0 {
		t.Fatalf("Len = %d after a Done with nothing added, want 0", q.Len())
	}
}

// AddAfter adds when the queue's clock reaches the time, not before; and
// ShutDown stops the timers and wakes a waiting Get.
func TestBatchQueueTimersAndShutDown(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(clk)
	q.AddAfter(batchNS, time.Minute, idOf("a"))
	clk.Step(59 * time.Second)
	if q.Len() != 0 {
		t.Fatalf("Len = %d before the timer, want 0", q.Len())
	}
	clk.Step(time.Second)
	if q.Len() != 1 {
		t.Fatalf("Len = %d at the timer, want 1", q.Len())
	}
	q.AddAfter(batchKey{widgetGK, "later"}, time.Hour, idOf("b"))

	var wg sync.WaitGroup
	got := make(chan bool, 2)
	_, _, _ = q.Get(context.Background()) // take the ready one
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, ok := q.Get(context.Background())
		got <- ok
	}()
	q.ShutDown()
	wg.Wait()
	if <-got {
		t.Error("a Get woken by ShutDown returned a batch")
	}
	if clk.HasWaiters() {
		t.Error("a timer is still waiting after ShutDown")
	}
	clk.Step(2 * time.Hour)
	if q.Len() != 0 {
		t.Errorf("Len = %d after ShutDown and the timer's time, want 0", q.Len())
	}
	q.Add(batchNS, idOf("c"))
	if q.Len() != 0 {
		t.Error("an Add after ShutDown queued a batch")
	}
}

// The fake clock runs a timer's function while holding its own lock, and
// that function takes the queue's; so the queue must never call the clock
// while holding its lock (D1 of the review of #109). These two probes race
// what would order the locks the other way: AddAfter against timers firing,
// and ShutDown against timers firing. Each fails on a deadline rather than
// hanging.
func TestBatchQueueNeverCallsTheClockUnderItsLock(t *testing.T) {
	within := func(t *testing.T, what string, f func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { defer close(done); f() }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("%s deadlocked", what)
		}
	}
	t.Run("AddAfter against Step", func(t *testing.T) {
		clk := testingclock.NewFakeClock(time.Now())
		q := newBatchQueue(clk)
		within(t, "AddAfter against Step", func() {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := range 2000 {
					q.AddAfter(batchKey{widgetGK, "ns"}, time.Millisecond, idOf(string(rune('a'+i%26))))
				}
			}()
			go func() {
				defer wg.Done()
				for range 2000 {
					clk.Step(time.Millisecond)
				}
			}()
			wg.Wait()
		})
	})
	t.Run("ShutDown against Step", func(t *testing.T) {
		for range 200 {
			clk := testingclock.NewFakeClock(time.Now())
			q := newBatchQueue(clk)
			for i := range 20 {
				q.AddAfter(batchKey{widgetGK, "ns"}, time.Duration(i+1)*time.Millisecond)
			}
			within(t, "ShutDown against Step", func() {
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for range 20 {
						clk.Step(time.Millisecond)
					}
				}()
				go func() { defer wg.Done(); q.ShutDown() }()
				wg.Wait()
			})
			if clk.HasWaiters() {
				t.Fatal("a timer is still waiting after ShutDown")
			}
		}
	})
}

// A Get returns false when its context ends.
func TestBatchQueueGetHonoursContext(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() { _, _, ok := q.Get(ctx); done <- ok }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Error("Get returned a batch after its context ended")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Get didn't return after its context ended")
	}
}

// countingMetrics counts what the core records.
type countingMetrics struct {
	NopMetrics
	mu      sync.Mutex
	counts  map[string]int
	reasons []string
}

func (m *countingMetrics) inc(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[name]++
}

func (m *countingMetrics) Degraded(kind, reason string) {
	m.inc("degraded " + kind)
	m.mu.Lock()
	m.reasons = append(m.reasons, reason)
	m.mu.Unlock()
}
func (m *countingMetrics) AdmissionDenied(kind string) { m.inc("admission_denied " + kind) }
func (m *countingMetrics) ReleaseRejected(kind string) { m.inc("release_rejected " + kind) }

// Class (e) of §6.2 is counted in credentialuse_admission_denied_total, for a
// new holder and an established one alike (L3 of the review of revision 16):
// an RBAC message the classifier stops matching then shows up.
func TestAdmissionDenialIsCounted(t *testing.T) {
	for _, established := range []bool{false, true} {
		r := newRig(t)
		m := &countingMetrics{}
		r.p.reports = coreReports{metrics: m}
		h := r.holder(t, "h", established)
		r.srv.before = func(op, kind, _ string) error {
			if op == "patch" && kind == "Secret" {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "s",
					errors.New(`admission webhook "v.example.com" denied the request: no`))
			}
			return nil
		}
		a, err := r.p.acquire(context.Background(), h, []Ref{SecretRef("ns", "s")})
		if err != nil {
			t.Fatal(err)
		}
		if got := m.counts["admission_denied "+kindLabel(widgetGK)]; got != 1 {
			t.Errorf("established=%v: admission_denied counted %d times, want 1 (acquire %+v)", established, got, a)
		}
		if established != a.Degraded || established == a.MustNotConnect {
			t.Errorf("established=%v: Degraded=%v MustNotConnect=%v", established, a.Degraded, a.MustNotConnect)
		}
	}
}

// The published settings compare as values: "60s" and "1m0s" agree, and any
// difference is named.
func TestPublishedSettingsCompareAsValues(t *testing.T) {
	want := publishedOf(Options{DeletionGrace: time.Minute, DisownPasses: 5, DisownInterval: time.Minute, TimeoutBound: 90 * time.Second})
	if want != (published{"60s", 5, "60s", "90s"}) {
		t.Fatalf("published = %+v, want the design's form", want)
	}
	if d := disagreement(want, published{"1m0s", 5, "60s", "1m30s"}); d != "" {
		t.Errorf("equal values disagree: %s", d)
	}
	d := disagreement(want, published{"30s", 4, "60s", "90s"})
	if !strings.Contains(d, "deletionGrace") || !strings.Contains(d, "disownPasses") || strings.Contains(d, "timeoutBound") {
		t.Errorf("disagreement = %q, want deletionGrace and disownPasses only", d)
	}
	if d := disagreement(want, published{"soon", 5, "60s", "90s"}); !strings.Contains(d, "deletionGrace") {
		t.Errorf("an unreadable duration agrees: %q", d)
	}
}

// Setup refuses what §7.3 lists, before any API call, and a Run after a
// refused Track fails: the process refuses to start.
func TestTrackValidates(t *testing.T) {
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(widgetGK.WithVersion("v1"), meta.RESTScopeNamespace)
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	ok := Options{
		Holder:          widgetGK.WithVersion("v1"),
		OwnerFinalizers: []string{"owner"},
		Refs:            func(metav1.Object) ([]Ref, error) { return nil, nil },
		Enqueue:         func(types.NamespacedName) {},
		TimeoutBound:    90 * time.Second,
	}
	cases := map[string]struct {
		change func(o *Options)
		want   string
	}{
		"no owner finalizers": {func(o *Options) { o.OwnerFinalizers = nil }, "OwnerFinalizers"},
		"no Refs":             {func(o *Options) { o.Refs = nil }, "Refs is required"},
		"no Enqueue":          {func(o *Options) { o.Enqueue = nil }, "Enqueue is required"},
		"no TimeoutBound":     {func(o *Options) { o.TimeoutBound = 0 }, "TimeoutBound is required"},
		"disown too short":    {func(o *Options) { o.DisownPasses, o.DisownInterval = 1, time.Minute }, "twice TimeoutBound"},
		"a bad prefix":        {func(o *Options) { o.Holder = schema.GroupVersionKind{Kind: "Widget", Version: "v1"} }, "needs a kind and a group"},
		"an unmapped holder":  {func(o *Options) { o.Holder = schema.GroupVersionKind{Group: "x.io", Version: "v1", Kind: "Nope"} }, "can't be mapped"},
		"an unmapped kind": {func(o *Options) {
			o.Protected = []ProtectedKind{{GVK: schema.GroupVersionKind{Version: "v1", Kind: "Nope"}}}
		}, "can't be mapped"},
		"PerName": {func(o *Options) { o.Protected = []ProtectedKind{{GVK: secretGVK, Watch: PerName}} }, "only LabelScoped"},
		"Content": {func(o *Options) { o.Protected = []ProtectedKind{{GVK: secretGVK, Content: true}} }, "Content"},
	}
	md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	s := newShared(Clients{Metadata: md, Mapper: mapper}, SharedOptions{}, testingclock.NewFakeClock(time.Now()))
	for name, c := range cases {
		o := ok
		c.change(&o)
		_, err := s.Track(o)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Track = %v, want an error containing %q", name, err, c.want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.RunInformers(ctx); err == nil || !strings.Contains(err.Error(), "OwnerFinalizers") {
		t.Errorf("Run after refused Tracks = %v, want their errors", err)
	}
}
