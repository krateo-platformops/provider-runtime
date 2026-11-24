package credentialuse_test

// What the wait does to two processes (§6.4, H1), measured, and why: opt-in, not a
// check of CI's.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

func h1Rounds(t *testing.T) int {
	t.Helper()
	v := os.Getenv("CREDENTIALUSE_H1_ROUNDS")
	if v == "" {
		t.Skip("CREDENTIALUSE_H1_ROUNDS is not set")
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// h1Duration is environment variable name's duration, or def.
func h1Duration(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// An h1Release is one R3 of one process: when its object's wait ended (zero
// without one), when the last R1, Namespace read and list before it began, and when
// it began and was answered; with the batches ready and worked on in its process at
// R1 and R3.
type h1Release struct {
	waitEnd, r1, ns, list, r3, done time.Time
	readyR1, workingR1              int
	readyR3, workingR3              int
	err                             string
}

// late is how long after its wait's end R1 began: what it waited for a worker, or
// for the clock.
func (r h1Release) late() time.Duration { return r.r1.Sub(r.waitEnd) }

// h1Process records one process's releases.
type h1Process struct {
	// holder is the holder kind of the namespace ns: the Widget, or a kind of its
	// own.
	holder func(ns string) schema.GroupKind

	mu    sync.Mutex
	open  map[string]*h1Release // by object, the one its batch works on
	ns    map[string]time.Time  // by name, the last read of a Namespace
	lists map[string]time.Time  // by namespace, the last list of the holders
	done  map[string][]h1Release
}

func newH1Process(holder func(ns string) schema.GroupKind) *h1Process {
	return &h1Process{
		holder: holder, open: map[string]*h1Release{}, ns: map[string]time.Time{},
		lists: map[string]time.Time{}, done: map[string][]h1Release{},
	}
}

// hook records s's calls. Before each R3 it sleeps a random time below jitter, if
// that is set: what a delay between R2 and R3 would do, measured without one.
func (p *h1Process) hook(s *cu.Shared, jitter time.Duration) {
	isR3 := func(c cu.Call) bool {
		return c.Op == "patch" && c.GK.Kind == "Secret" && c.PatchType == types.JSONPatchType
	}
	s.HookCalls(func(_ context.Context, c cu.Call) error {
		if jitter > 0 && isR3(c) {
			time.Sleep(rand.N(jitter))
		}
		now := time.Now()
		key := c.Namespace + "/" + c.Name
		switch {
		case c.Op == "get" && c.GK.Kind == "Secret":
			ready, working := s.QueueLoad()
			p.mu.Lock()
			p.open[key] = &h1Release{r1: now, readyR1: ready, workingR1: working}
			p.mu.Unlock()
		case c.Op == "get" && c.GK.Kind == "Namespace":
			p.mu.Lock()
			p.ns[c.Name] = now
			p.mu.Unlock()
		case c.Op == "list" && c.GK == p.holder(c.Namespace):
			p.mu.Lock()
			p.lists[c.Namespace] = now
			p.mu.Unlock()
		case isR3(c):
			end, _ := s.WaitEnd(p.holder(c.Namespace), c.Namespace, cu.SecretRef(c.Namespace, c.Name).ID())
			ready, working := s.QueueLoad()
			p.mu.Lock()
			if r := p.open[key]; r != nil {
				r.waitEnd, r.ns, r.list, r.r3 = end, p.ns[c.Namespace], p.lists[c.Namespace], now
				r.readyR3, r.workingR3 = ready, working
			}
			p.mu.Unlock()
		}
		return nil
	}, func(c cu.Call, err error) {
		if !isR3(c) {
			return
		}
		key := c.Namespace + "/" + c.Name
		p.mu.Lock()
		defer p.mu.Unlock()
		if r := p.open[key]; r != nil {
			r.done = time.Now()
			if err != nil {
				r.err = string(apierrors.ReasonForError(err))
			}
			p.done[key] = append(p.done[key], *r)
		}
	})
}

// h1Stalls samples the test process's clock: a gap well over the sleep is the
// machine, or this process, standing still.
type h1Stalls struct {
	mu   sync.Mutex
	gaps []h1Gap
	stop chan struct{}
	wg   sync.WaitGroup
}

type h1Gap struct{ from, to time.Time }

func startH1Stalls() *h1Stalls {
	s := &h1Stalls{stop: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		last := time.Now()
		for {
			select {
			case <-s.stop:
				return
			default:
			}
			time.Sleep(2 * time.Millisecond)
			now := time.Now()
			if now.Sub(last) > 12*time.Millisecond {
				s.mu.Lock()
				s.gaps = append(s.gaps, h1Gap{last, now})
				s.mu.Unlock()
			}
			last = now
		}
	}()
	return s
}

// within is the longest gap that overlaps from and to.
func (s *h1Stalls) within(from, to time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var most time.Duration
	for _, g := range s.gaps {
		if g.from.Before(to) && from.Before(g.to) {
			most = max(most, g.to.Sub(g.from))
		}
	}
	return most
}

func quantiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "none"
	}
	ds = slices.Clone(ds)
	slices.Sort(ds)
	at := func(q float64) time.Duration {
		return ds[min(len(ds)-1, int(q*float64(len(ds))))].Round(100 * time.Microsecond)
	}
	return fmt.Sprintf("p10 %s p50 %s p90 %s p99 %s max %s (%d)", at(0.1), at(0.5), at(0.9), at(0.99), ds[len(ds)-1].Round(100*time.Microsecond), len(ds))
}

// An h1Result is what one measurement found.
type h1Result struct {
	n, overlapped, recreatedGone int
	meanR3                       time.Duration
}

// An h1Kind is a holder kind.
type h1Kind struct {
	gvk schema.GroupVersionKind
	gvr schema.GroupVersionResource
}

var h1Widget = h1Kind{cutest.WidgetGVK, cutest.WidgetGVR}

// h1Kinds creates n holder kinds. Each has a watch cache, and an etcd client, of its
// own in the API server.
func h1Kinds(t *testing.T, n int) []h1Kind {
	t.Helper()
	ctx := context.Background()
	var kinds []h1Kind
	for i := range n {
		k := h1Kind{
			schema.GroupVersionKind{Group: cutest.Group, Version: "v1", Kind: fmt.Sprintf("Lane%d", i)},
			schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: fmt.Sprintf("lane%ds", i)},
		}
		if err := env.Client.Create(ctx, cutest.CRD(k.gvk.Kind, k.gvr.Resource)); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
		kinds = append(kinds, k)
	}
	for _, k := range kinds {
		cutest.Eventually(t, time.Minute, func() error {
			_, err := env.Metadata.Resource(k.gvr).List(ctx, metav1.ListOptions{})
			return err
		})
	}
	return kinds
}

func h1Create(t *testing.T, k h1Kind, ns, name string, finalizers ...string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": "unused"}}}
	u.SetGroupVersionKind(k.gvk)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetFinalizers(finalizers)
	if _, err := env.Dynamic.Resource(k.gvr).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// h1Caches are the API server's counters of the watch caches of kinds, summed: how
// long the reads that waited to be fresh did, and the consistent reads.
func h1Caches(t *testing.T, kinds []h1Kind) map[string]float64 {
	t.Helper()
	raw, err := env.Kube.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for line := range strings.Lines(string(raw)) {
		name, rest, _ := strings.Cut(line, "{")
		if !strings.HasPrefix(name, "apiserver_watch_cache_read_wait_seconds_") && name != "apiserver_watch_cache_consistent_read_total" {
			continue
		}
		labels, value, ok := strings.Cut(rest, "} ")
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if !ok || err != nil {
			continue
		}
		var group, resource string
		var other []string
		for _, l := range strings.Split(labels, ",") {
			switch k, v, _ := strings.Cut(l, "="); k {
			case "group":
				group = strings.Trim(v, `"`)
			case "resource":
				resource = strings.Trim(v, `"`)
			default:
				other = append(other, l)
			}
		}
		for _, k := range kinds {
			if k.gvr.Group == group && k.gvr.Resource == resource {
				got[strings.TrimPrefix(name, "apiserver_watch_cache_")+"{"+strings.Join(other, ",")+"}"] += v
			}
		}
	}
	return got
}

// h1CacheLine says what changed from before to after; the buckets are cumulative.
func h1CacheLine(before, after map[string]float64) string {
	var parts []string
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			parts = append(parts, fmt.Sprintf("%s %.4g", k, d))
		}
	}
	slices.Sort(parts)
	if len(parts) == 0 {
		return "no counter of these caches changed"
	}
	return strings.Join(parts, "; ")
}

// h1Write patches an object of kind k that holds nothing, in a namespace of its own,
// every period, until stop is called; it says how many writes were answered.
func h1Write(t *testing.T, k h1Kind, period time.Duration) (stop func() int64) {
	t.Helper()
	ns := env.Namespace(t, "h1write")
	h1Create(t, k, ns, "busy")
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var writes atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(period)
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := env.Metadata.Resource(k.gvr).Namespace(ns).Patch(ctx, "busy", types.MergePatchType,
					[]byte(fmt.Sprintf(`{"metadata":{"annotations":{"n":"%d"}}}`, i)), metav1.PatchOptions{}); err == nil {
					writes.Add(1)
				}
			}()
		}
	}()
	return func() int64 { cancel(); wg.Wait(); return writes.Load() }
}

// h1Measure deletes rounds of namespaces times each Secrets that a live user
// holds, of the kinds in turn by namespace; their graces end together, and two
// processes release them, while a creator tries each again every poll. It counts the
// Secrets whose two R3s overlapped, and the recreated ones that then went, which
// nobody deletes; and it prints, for the releases that overlapped and the rest, how
// late each began after its wait, and what else was going on.
func h1Measure(t *testing.T, name string, kinds []h1Kind, rounds, namespaces, each int, wait bool, poll time.Duration) h1Result {
	t.Helper()
	ctx := context.Background()
	for _, k := range kinds {
		resetPublished(t, k.gvr)
		t.Cleanup(func() { resetPublished(t, k.gvr) })
	}
	var holders sync.Map // by namespace, its h1Kind
	holder := func(ns string) schema.GroupKind {
		if k, ok := holders.Load(ns); ok {
			return k.(h1Kind).gvk.GroupKind()
		}
		return schema.GroupKind{}
	}
	stalls := startH1Stalls()
	longest := h1Duration(t, "CREDENTIALUSE_H1_WAIT", 0)
	jitter := h1Duration(t, "CREDENTIALUSE_H1_JITTER", 0)
	var procs [2]*h1Process
	for i := range procs {
		p := newH1Process(holder)
		procs[i] = p
		s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
		switch {
		case !wait:
			s.SetReleaseWait(nil)
		case longest > 0:
			s.SetReleaseWait(func() time.Duration { return rand.N(longest) })
		}
		p.hook(s, jitter)
		for _, k := range kinds {
			o := widgetOptions(func(types.NamespacedName) {})
			o.Holder = k.gvk
			o.DeletionGrace = testGrace
			if _, err := s.Track(ctx, o); err != nil {
				t.Fatal(err)
			}
		}
		running(t, s)
	}
	if period := h1Duration(t, "CREDENTIALUSE_H1_WRITES", 0); period > 0 {
		for _, k := range kinds {
			began, stop := time.Now(), h1Write(t, k, period)
			defer func() {
				n := stop()
				t.Logf("H1WRITES %s: %d writes of %s, %.0f a second", name, n, k.gvk.Kind, float64(n)/time.Since(began).Seconds())
			}()
		}
	}
	caches := h1Caches(t, kinds)
	var res h1Result
	type target struct{ ns, name string }
	for range rounds {
		var wg sync.WaitGroup
		var lost atomic.Int64
		var targets []target
		for i := range namespaces {
			k := kinds[i%len(kinds)]
			ns := env.Namespace(t, "h1wait")
			holders.Store(ns, k)
			var ids []cu.ID
			for i := range each {
				ids = append(ids, cu.SecretRef(ns, fmt.Sprintf("s%d", i)).ID())
				targets = append(targets, target{ns, fmt.Sprintf("s%d", i)})
			}
			rec, err := cu.EncodeRecord(ids)
			if err != nil {
				t.Fatal(err)
			}
			h1Create(t, k, ns, "w", ownerFinalizer)
			if _, err := env.Metadata.Resource(k.gvr).Namespace(ns).Patch(ctx, "w", types.MergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, rec)), metav1.PatchOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		for _, x := range targets {
			gk := holder(x.ns)
			f, err := cu.FinalizerName(gk, x.ns)
			if err != nil {
				t.Fatal(err)
			}
			s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: x.name, Namespace: x.ns, Finalizers: []string{f}, Annotations: map[string]string{f: "e0"},
				Labels: map[string]string{cu.LabelHeld: "true", cu.MarkerLabel(gk): "true"},
			}}
			if _, err := env.Kube.CoreV1().Secrets(x.ns).Create(ctx, s, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		for _, x := range targets {
			res.n++
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := secrets(env.Metadata, x.ns).Delete(ctx, x.name, metav1.DeleteOptions{}); err != nil {
					t.Error(err)
					return
				}
				again := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: x.name, Namespace: x.ns}}
				for end := time.Now().Add(time.Minute); time.Now().Before(end); time.Sleep(poll) {
					if _, err := env.Kube.CoreV1().Secrets(x.ns).Create(ctx, again, metav1.CreateOptions{}); err == nil {
						time.Sleep(3 * time.Second)
						if _, err := secrets(env.Metadata, x.ns).Get(ctx, x.name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
							lost.Add(1)
						}
						return
					}
				}
				t.Errorf("%s/%s was never released", x.ns, x.name)
			}()
		}
		wg.Wait()
		res.recreatedGone += int(lost.Load())
	}
	t.Logf("H1CACHE %s: %s", name, h1CacheLine(caches, h1Caches(t, kinds)))
	close(stalls.stop)
	stalls.wg.Wait()
	h1Lists(t, name, procs)
	h1Report(t, name, &res, procs, stalls, wait)
	return res
}

// h1Lists prints how long before an R3 its list, and its Namespace read, began: for
// the first R3 of each object, and for the others.
func h1Lists(t *testing.T, name string, procs [2]*h1Process) {
	t.Helper()
	byKey := map[string][]h1Release{}
	for _, p := range procs {
		p.mu.Lock()
		for key, rs := range p.done {
			byKey[key] = append(byKey[key], rs...)
		}
		p.mu.Unlock()
	}
	var lists, namespaces [2][]time.Duration
	for _, rs := range byKey {
		slices.SortFunc(rs, func(a, b h1Release) int { return a.r3.Compare(b.r3) })
		for i, r := range rs {
			i = min(i, 1)
			lists[i] = append(lists[i], r.r3.Sub(r.list))
			if !r.ns.IsZero() {
				namespaces[i] = append(namespaces[i], r.r3.Sub(r.ns))
			}
		}
	}
	for i, what := range []string{"the first R3 of an object", "a later one"} {
		t.Logf("H1LISTS %s, %s: the list to R3: %s; the Namespace's read to R3: %s", name, what, quantiles(lists[i]), quantiles(namespaces[i]))
	}
}

// h1Report counts the overlaps, and prints what went with them.
func h1Report(t *testing.T, name string, res *h1Result, procs [2]*h1Process, stalls *h1Stalls, wait bool) {
	t.Helper()
	type group struct {
		late, r1ToR3, r3, stall    []time.Duration
		readyR1, readyR3, r1s, big int
	}
	var over, rest group
	var endDiffs, r3Diffs []time.Duration // of the overlapping pairs
	var pairs []string
	closeEnds, withBoth := 0, 0
	var total time.Duration
	calls := 0
	for i := range procs {
		procs[i].mu.Lock()
		defer procs[i].mu.Unlock()
	}
	overlapping := map[string]bool{}
	for key, as := range procs[0].done {
		for _, a := range as {
			for _, b := range procs[1].done[key] {
				if wait && !a.waitEnd.IsZero() && !b.waitEnd.IsZero() {
					withBoth++
					if a.waitEnd.Sub(b.waitEnd).Abs() < a.done.Sub(a.r3) {
						closeEnds++
					}
				}
				if !a.r3.Before(b.done) || !b.r3.Before(a.done) {
					continue
				}
				overlapping[key] = true
				r3Diffs = append(r3Diffs, a.r3.Sub(b.r3).Abs())
				if !wait {
					continue
				}
				endDiffs = append(endDiffs, a.waitEnd.Sub(b.waitEnd).Abs())
				if len(pairs) < 60 {
					one := func(r h1Release) string {
						return fmt.Sprintf("late %s, R1 to R2 %s, R2 to R3 %s, R3 %s %s, ready %d/%d, worked on %d/%d, longest stall %s",
							r.late().Round(100*time.Microsecond), r.list.Sub(r.r1).Round(100*time.Microsecond), r.r3.Sub(r.list).Round(100*time.Microsecond),
							r.done.Sub(r.r3).Round(100*time.Microsecond), r.err, r.readyR1, r.readyR3, r.workingR1, r.workingR3, stalls.within(r.waitEnd, r.done).Round(time.Millisecond))
					}
					pairs = append(pairs, fmt.Sprintf("waits ended %s apart, R3s began %s apart | p0: %s | p1: %s",
						a.waitEnd.Sub(b.waitEnd).Abs().Round(100*time.Microsecond), a.r3.Sub(b.r3).Abs().Round(100*time.Microsecond), one(a), one(b)))
				}
			}
		}
	}
	res.overlapped = len(overlapping)
	for _, p := range procs {
		for key, rs := range p.done {
			g := &rest
			if overlapping[key] {
				g = &over
			}
			for _, r := range rs {
				total += r.done.Sub(r.r3)
				calls++
				g.r3 = append(g.r3, r.done.Sub(r.r3))
				g.r1ToR3 = append(g.r1ToR3, r.r3.Sub(r.r1))
				g.r1s++
				if r.readyR1 > 0 {
					g.readyR1++
				}
				if r.readyR3 > 0 {
					g.readyR3++
				}
				if wait && !r.waitEnd.IsZero() {
					g.late = append(g.late, r.late())
					s := stalls.within(r.waitEnd, r.done)
					g.stall = append(g.stall, s)
					if s > 0 {
						g.big++
					}
				}
			}
		}
	}
	if calls > 0 {
		res.meanR3 = total / time.Duration(calls)
	}
	for _, g := range []struct {
		what string
		g    group
	}{{"overlapped", over}, {"the rest", rest}} {
		if g.g.r1s == 0 {
			continue
		}
		t.Logf("H1DELAY %s, %s: %d R3s; R1 after the wait's end: %s; R1 to R3: %s; an R3: %s; batches ready at R1 in %.0f%%, at R3 in %.0f%%; a stall over 12ms between the wait's end and the answer in %.0f%%, the longest: %s",
			name, g.what, g.g.r1s, quantiles(g.g.late), quantiles(g.g.r1ToR3), quantiles(g.g.r3),
			100*float64(g.g.readyR1)/float64(g.g.r1s), 100*float64(g.g.readyR3)/float64(g.g.r1s),
			100*float64(g.g.big)/float64(max(1, len(g.g.stall))), quantiles(g.g.stall))
	}
	if wait {
		t.Logf("H1DELAY %s: of the pairs that overlapped, the waits ended %s apart, and the R3s began %s apart; waits that both made an R3 %d, of which ended within an R3 of each other %d",
			name, quantiles(endDiffs), quantiles(r3Diffs), withBoth, closeEnds)
	}
	for _, p := range pairs {
		t.Logf("H1PAIR %s: %s", name, p)
	}
}

// Set CREDENTIALUSE_H1_ROUNDS to the rounds, about 8s each, for each of four
// settings: two shapes (40 Secrets, each in a namespace of its own, where a batch is
// of one; and 200 in one namespace, where batches are large), without the wait and
// with it. CREDENTIALUSE_H1_POLL sets the creators' poll, 1ms in the first shape and
// 20ms in the second.
// CREDENTIALUSE_H1_ONLY runs only the settings whose name it is in. Two more say
// what another wait would do: CREDENTIALUSE_H1_WAIT is a longer wait than 2s, and
// CREDENTIALUSE_H1_JITTER a random delay before each R3. CREDENTIALUSE_H1_WRITES is
// the period at which something else writes the holder kind.
// CREDENTIALUSE_H1_ALONE=n is another shape, of n Secrets a round, each held by a kind
// of its own, so that no other list of the kind is made then; with 1, nothing else is
// released.
func TestReleaseWaitSpreadsTwoProcesses(t *testing.T) {
	cutest.Require(t, env)
	rounds := h1Rounds(t)
	if v := os.Getenv("CREDENTIALUSE_H1_ALONE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("CREDENTIALUSE_H1_ALONE is %q", v)
		}
		name := fmt.Sprintf("%d kinds, one Secret of each, the wait", n)
		poll := h1Duration(t, "CREDENTIALUSE_H1_POLL", time.Millisecond)
		h1Line(t, name, poll, h1Measure(t, name, h1Kinds(t, n), rounds, n, 1, true, poll))
		return
	}
	for _, shape := range []struct {
		name             string
		namespaces, each int
		poll             time.Duration
	}{
		{"40 namespaces, one Secret in each", 40, 1, time.Millisecond},
		{"one namespace, 200 Secrets in it", 1, 200, 20 * time.Millisecond},
	} {
		overlaps := map[string]h1Result{}
		poll := h1Duration(t, "CREDENTIALUSE_H1_POLL", shape.poll)
		for _, c := range []struct {
			name string
			wait bool
		}{{"no wait", false}, {"the wait", true}} {
			name := shape.name + ", " + c.name
			if !strings.Contains(name, os.Getenv("CREDENTIALUSE_H1_ONLY")) {
				continue
			}
			t.Run(name, func(t *testing.T) {
				overlaps[c.name] = h1Measure(t, name, []h1Kind{h1Widget}, rounds, shape.namespaces, shape.each, c.wait, poll)
				h1Line(t, name, poll, overlaps[c.name])
			})
		}
		if x, y := overlaps["no wait"], overlaps["the wait"]; x.n > 0 && y.n > 0 && y.overlapped > 0 {
			t.Logf("H1RATIO %s: no wait %d of %d, the wait %d of %d: %.1f times", shape.name, x.overlapped, x.n, y.overlapped, y.n,
				float64(x.overlapped)/float64(x.n)/(float64(y.overlapped)/float64(y.n)))
		}
	}
}

// h1Line prints what one measurement counted.
func h1Line(t *testing.T, name string, poll time.Duration, r h1Result) {
	t.Helper()
	longest := h1Duration(t, "CREDENTIALUSE_H1_WAIT", 2*time.Second)
	t.Logf("H1WAIT %s below %s, polled every %s: %d releases; the two processes' R3s of one object overlapped in %d (%.2f%%); a recreated Secret went in %d; an R3 took %s: 2w/L is %.2f%%",
		name, longest, poll, r.n, r.overlapped, 100*float64(r.overlapped)/float64(r.n), r.recreatedGone, r.meanR3, 200*r.meanR3.Seconds()/longest.Seconds())
}

// A list as R2 makes it, without a resourceVersion and with a limit, against the
// writes of its kind answered before it began and before it ended: §6.4's windows
// rest on its not being behind a write answered before it began. Set
// CREDENTIALUSE_H1_LISTS to the lists of each setting.
func TestAListIsNotBehindAnAnsweredWrite(t *testing.T) {
	cutest.Require(t, env)
	v := os.Getenv("CREDENTIALUSE_H1_LISTS")
	if v == "" {
		t.Skip("CREDENTIALUSE_H1_LISTS is not set")
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name         string
		listers      int
		gap, writing time.Duration // the means of random times: between a lister's lists, and between writes of the kind (0 is none)
	}{
		{"40 listers, no write of the kind", 40, time.Second, 0},
		{"40 listers, a write of the kind each second", 40, time.Second, time.Second},
		{"40 listers, a write of the kind each 100ms", 40, time.Second, 100 * time.Millisecond},
		{"40 listers, a write of the kind each 10ms", 40, time.Second, 10 * time.Millisecond},
		{"4 listers, a write of the kind each 100ms", 4, time.Second, 100 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) { h1Freshness(t, c.name, c.listers, n, c.gap, c.writing) })
	}
}

// h1Freshness makes n lists. Writes of another kind go on, which put etcd's revision
// ahead of the watch cache's: without them a list doesn't wait.
func h1Freshness(t *testing.T, name string, listers, n int, gap, writing time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := env.Namespace(t, "h1lists")
	h1Create(t, h1Widget, ns, "w")
	widgets := env.Metadata.Resource(cutest.WidgetGVR).Namespace(ns)
	if _, err := env.Kube.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	others := env.Metadata.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(ns)
	rv := func(s string) uint64 {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Errorf("a resourceVersion %q: %v", s, err)
		}
		return v
	}
	type write struct {
		rv       uint64
		answered time.Time
	}
	type list struct {
		begin, end time.Time
		rv, item   uint64
	}
	var mu sync.Mutex
	var writes []write
	var lists []list
	var background, wg sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		for i := 0; ctx.Err() == nil; i++ {
			_, err := others.Patch(ctx, "other", types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"annotations":{"n":"%d"}}}`, i)), metav1.PatchOptions{})
			if err != nil && ctx.Err() == nil {
				t.Errorf("a write of another kind: %v", err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if writing > 0 {
		background.Add(1)
		go func() {
			defer background.Done()
			for i := 0; ctx.Err() == nil; i++ {
				m, err := widgets.Patch(ctx, "w", types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"annotations":{"n":"%d"}}}`, i)), metav1.PatchOptions{})
				now := time.Now()
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("a write of the kind: %v", err)
					}
					return
				}
				mu.Lock()
				writes = append(writes, write{rv(m.ResourceVersion), now})
				mu.Unlock()
				time.Sleep(rand.N(2 * writing))
			}
		}()
	}
	before := h1Caches(t, []h1Kind{h1Widget})
	var taken atomic.Int64
	for range listers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for taken.Add(1) <= int64(n) {
				time.Sleep(rand.N(2 * gap))
				begin := time.Now()
				l, err := widgets.List(ctx, metav1.ListOptions{Limit: 500})
				end := time.Now()
				if err != nil || len(l.Items) != 1 {
					t.Errorf("a list: %v", err)
					return
				}
				mu.Lock()
				lists = append(lists, list{begin, end, rv(l.ResourceVersion), rv(l.Items[0].ResourceVersion)})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	after := h1Caches(t, []h1Kind{h1Widget})
	cancel()
	background.Wait()
	var took, behindBegin, behindEnd []time.Duration
	during, olderItem := 0, 0
	for _, l := range lists {
		took = append(took, l.end.Sub(l.begin))
		var atBegin, atEnd time.Duration // since the first answered write that the list lacks
		for _, w := range writes {
			if w.answered.After(l.end) {
				break // in the order they were answered: one writer
			}
			if w.answered.After(l.begin) {
				during++
			}
			if w.rv > l.item && w.rv <= l.rv {
				olderItem++
			}
			if w.rv <= l.rv && w.rv <= l.item {
				continue
			}
			if !w.answered.After(l.begin) {
				atBegin = max(atBegin, l.begin.Sub(w.answered)+1)
			}
			atEnd = max(atEnd, l.end.Sub(w.answered)+1)
		}
		if atBegin > 0 {
			behindBegin = append(behindBegin, atBegin)
		}
		if atEnd > 0 {
			behindEnd = append(behindEnd, atEnd)
		}
	}
	t.Logf("H1FRESH %s: %d lists, %d writes of the kind; a list took %s; lists behind a write answered before they began: %d, by %s; writes answered during a list: %d; lists that ended without a write answered by then: %d, answered before the end by %s; writes that a list's version covers and its item doesn't: %d",
		name, len(lists), len(writes), quantiles(took), len(behindBegin), quantiles(behindBegin), during, len(behindEnd), quantiles(behindEnd), olderItem)
	t.Logf("H1CACHE %s: %s", name, h1CacheLine(before, after))
	if len(behindBegin) > 0 {
		t.Errorf("%d lists were behind a write answered before they began", len(behindBegin))
	}
}
