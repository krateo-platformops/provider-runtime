package credentialuse_test

// PerName mode and the index ConfigMap against envtest (plan, PR 8; §10.6,
// §13 shape 1, §14).

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// widgetIndex is the Widget kind's index name.
var widgetIndex = cu.IndexName(widgetGK)

// perName protects Secrets in PerName mode.
func perName(o *cu.Options) {
	o.Protected = []cu.ProtectedKind{{GVK: secretGVK, Watch: cu.PerName}}
}

// resetPointer removes the index pointer from the Widget CRD, so that the
// next Track writes its own: every test uses an index namespace of its own.
func resetPointer(t *testing.T) {
	t.Helper()
	setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationIndex: nil})
}

// indexNamespace creates a namespace for a test's index, with the pointer
// cleared before, and again after the test, once the rigs created later have
// stopped.
func indexNamespace(t *testing.T) string {
	t.Helper()
	ns := env.Namespace(t, "cu-idx")
	resetPointer(t)
	t.Cleanup(func() { resetPointer(t) })
	return ns
}

// indexLines reads the index in namespace idx live, and reports whether it
// exists.
func indexLines(t *testing.T, idx string) ([]string, bool) {
	t.Helper()
	cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(context.Background(), widgetIndex, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(cm.Data["entries"], "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out, true
}

// secretLine is Secret ns/name's index line.
func secretLine(ns, name string) string { return cu.SecretRef(ns, name).ID().String() }

// lined waits until the index in idx has line, or, with want false, hasn't.
func lined(t *testing.T, idx, line string, want bool) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		lines, _ := indexLines(t, idx)
		if slices.Contains(lines, line) != want {
			return fmt.Errorf("index %v, line %s wanted %v: %w", lines, line, want, cutest.ErrNotYet)
		}
		return nil
	})
}

// setIndexLines writes the index in idx by hand, as an operator would,
// retrying on conflict.
func setIndexLines(t *testing.T, idx string, edit func([]string) []string) {
	t.Helper()
	cutest.Eventually(t, 30*time.Second, func() error {
		cms := env.Kube.CoreV1().ConfigMaps(idx)
		cm, err := cms.Get(context.Background(), widgetIndex, metav1.GetOptions{})
		if err != nil {
			return err
		}
		lines, _ := indexLines(t, idx)
		lines = edit(lines)
		slices.Sort(lines)
		data := ""
		if len(lines) > 0 {
			data = strings.Join(lines, "\n") + "\n"
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["entries"] = data
		_, err = cms.Update(context.Background(), cm, metav1.UpdateOptions{})
		return err
	})
}

// perNameRig is a writeRig in PerName mode, its index in namespace idx.
func perNameRig(t *testing.T, idx string, change func(o *cu.Options), before func(*cu.Shared)) *writeRig {
	t.Helper()
	return newWriteRig(t, env.Config, cu.SharedOptions{IndexNamespace: idx}, func(o *cu.Options) {
		perName(o)
		if change != nil {
			change(o)
		}
	}, before)
}

// The pointer is written once on the CRD, by whichever of two first
// processes wins; a later process with another index refuses to start
// (§10.6.1, §7.3). The index is created at the first start (§10.6.1).
func TestIndexPointerWrittenOnce(t *testing.T) {
	cutest.Require(t, env)
	idx, other := indexNamespace(t), env.Namespace(t, "cu-idx-other")
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	track := func(ns string) error {
		s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: ns})
		o := widgetOptions(func(types.NamespacedName) {})
		perName(&o)
		_, err := s.Track(context.Background(), o)
		return err
	}
	// Two first processes race; both start, and one pointer stands.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = track(idx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("first process %d: %v", i, err)
		}
	}
	if got := widgetCRDAnnotations(t)[cu.AnnotationIndex]; got != idx+"/"+widgetIndex {
		t.Fatalf("pointer = %q, want %s/%s", got, idx, widgetIndex)
	}
	if _, ok := indexLines(t, idx); !ok {
		t.Fatal("the index wasn't created at start")
	}
	err := track(other)
	if err == nil || !strings.Contains(err.Error(), "points at the index "+idx+"/"+widgetIndex) {
		t.Fatalf("a process with another index = %v, want a refusal naming the pointer", err)
	}
	if got := widgetCRDAnnotations(t)[cu.AnnotationIndex]; got != idx+"/"+widgetIndex {
		t.Errorf("the refused process changed the pointer to %q", got)
	}
	if _, ok := indexLines(t, other); ok {
		t.Error("the refused process created an index")
	}
}

// The line is added before A2 and pruned after R3 (§10.6.2); the per-name
// watch follows it (§13 shape 1); a Missing ref's line stays while a record
// names it, and goes once none does (§10.6.2, A14).
func TestIndexLinesBeforeA2AndPrunedAfterR3(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-lines")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	var mu sync.Mutex
	var atA2 []bool // whether the index had the line at each A2 of s
	r := perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.Name == "s" && c.PatchType == types.StrategicMergePatchType {
				lines, _ := indexLines(t, idx)
				mu.Lock()
				atA2 = append(atA2, slices.Contains(lines, secretLine(ns, "s")))
				mu.Unlock()
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, secretLine(ns, "s"), true)
	mu.Lock()
	if len(atA2) == 0 || slices.Contains(atA2, false) {
		t.Errorf("the line at each A2: %v; want it there before every one", atA2)
	}
	mu.Unlock()
	sid := cu.SecretRef(ns, "s").ID()
	cutest.Eventually(t, time.Minute, func() error {
		if !r.tr.PerNameSynced(sid) {
			return fmt.Errorf("no synced per-name watch of %s: %v: %w", sid, r.tr.PerNameWatches(), cutest.ErrNotYet)
		}
		return nil
	})

	// A Missing ref: its line is added before the A2 that finds NotFound,
	// and stays while the record names it.
	createWidget(t, ns, "m", "absent")
	r.c.enqueue(widgetNN(ns, "m"))
	lined(t, idx, secretLine(ns, "absent"), true)
	time.Sleep(2 * time.Second) // a few release batches and ticks
	lined(t, idx, secretLine(ns, "absent"), true)

	// Re-pointed: no record names it, and its line goes.
	repoint(t, ns, "m", "s")
	r.c.enqueue(widgetNN(ns, "m"))
	lined(t, idx, secretLine(ns, "absent"), false)

	// Released: the line goes after R3, and the watch with it.
	deleteWidget(t, ns, "w")
	deleteWidget(t, ns, "m")
	r.c.enqueue(widgetNN(ns, "w"))
	r.c.enqueue(widgetNN(ns, "m"))
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
	cutest.Eventually(t, time.Minute, func() error {
		if slices.Contains(r.tr.PerNameWatches(), sid) {
			return fmt.Errorf("the per-name watch of %s stays: %w", sid, cutest.ErrNotYet)
		}
		return nil
	})
}

// repoint sets Widget ns/name's spec.secret.
func repoint(t *testing.T, ns, name, secret string) {
	t.Helper()
	cutest.Eventually(t, 30*time.Second, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := unstructured.SetNestedField(w.Object, secret, "spec", "secret"); err != nil {
			return err
		}
		_, err = env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Update(context.Background(), w, metav1.UpdateOptions{})
		return err
	})
}

// A hand-deleted index is recreated and rebuilt from the records (§10.6.6);
// an object whose last record was gone before its R3, the residual, isn't in
// it, and its marker label finds it.
func TestIndexRebuiltAfterDeletion(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-rebuild")
	for _, n := range []string{"s1", "s2"} {
		createSecret(t, ns, n)
		createWidget(t, ns, "w-"+n, n)
	}
	r := perNameRig(t, idx, nil, nil)
	for _, n := range []string{"s1", "s2"} {
		r.c.enqueue(widgetNN(ns, "w-"+n))
		established(t, ns, n, "w-"+n)
	}
	// The residual: s3 carries the kind's finalizer and marker, no record
	// names it, and its line is gone.
	createSecret(t, ns, "s3")
	acquireAs(t, ns, "s3")

	if err := env.Kube.CoreV1().ConfigMaps(idx).Delete(context.Background(), widgetIndex, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	lined(t, idx, secretLine(ns, "s1"), true)
	lined(t, idx, secretLine(ns, "s2"), true)
	if lines, _ := indexLines(t, idx); slices.Contains(lines, secretLine(ns, "s3")) {
		t.Fatalf("the rebuild found the residual: %v", lines)
	}
	l, err := env.Kube.CoreV1().Secrets("").List(context.Background(), metav1.ListOptions{LabelSelector: cu.MarkerLabel(widgetGK) + "=true"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(l.Items, func(s corev1.Secret) bool { return s.Namespace == ns && s.Name == "s3" }) {
		t.Errorf("the marker label doesn't find the residual")
	}
}

// acquireAs writes the Widget kind's A2 on Secret ns/name for holder
// namespace ns, as a hold whose record is gone.
func acquireAs(t *testing.T, ns, name string) {
	t.Helper()
	epoch, _ := cu.NewEpoch()
	p, err := cu.AcquireStrategicPatch(widgetGK, ns, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := send(context.Background(), secrets(env.Metadata, ns), name, p); err != nil {
		t.Fatal(err)
	}
}

// §10.6.8, interleaving 1: a restart between Forget and R3. The holder's
// last pass removes its record with its owner finalizer, and the process
// dies before R3: the Secret keeps the finalizer, and its line. The next
// process's startup recheck reads the index, finds the finalizer, and
// releases it.
func TestIndexRestartBetweenForgetAndR3(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-restart")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	var armed, killed atomic.Bool
	var r *writeRig
	r = perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if armed.Load() && c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.JSONPatchType {
				if killed.CompareAndSwap(false, true) {
					r.kill()
				}
				return errors.New("the process died before R3")
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	armed.Store(true)
	deleteWidget(t, ns, "w")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, time.Minute, "widget w", widgetGet(ns, "w"))
	cutest.Eventually(t, time.Minute, func() error {
		if !killed.Load() {
			return fmt.Errorf("R3 not reached: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if err := heldFor(ns, "s", ns)(); err != nil {
		t.Fatalf("after the crash: %v", err)
	}
	lined(t, idx, secretLine(ns, "s"), true)

	perNameRig(t, idx, nil, nil)
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
}

// failingConnect fails every Connect, as a provider whose external system is
// unreachable: the holder never gets its owner finalizer.
func failingConnect(context.Context, types.NamespacedName, cu.Ref) error {
	return errors.New("the external system is unreachable")
}

// perNameProcess is a process in PerName mode whose driver uses connect.
func perNameProcess(t *testing.T, cfg *rest.Config, idx string, connect func(context.Context, types.NamespacedName, cu.Ref) error) (*cu.Shared, *controller, context.CancelFunc) {
	t.Helper()
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	c := newController(t)
	clients := clientsFor(t, cfg)
	rec := events.NewFakeRecorder(100000)
	go func() {
		for range rec.Events {
		}
	}()
	clients.Events = rec
	s := cu.NewShared(clients, cu.SharedOptions{IndexNamespace: idx})
	o := widgetOptions(c.enqueue)
	o.DeletionGrace, o.ProbeInterval = testGrace, time.Second
	perName(&o)
	tr, err := s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	stop := running(t, s)
	d := newDriver(tr)
	d.Connect = connect
	c.start(d)
	return s, c, func() { c.stop(); stop() }
}

// §10.6.8, interleaving 2: a never-established holder deleted while no
// process runs. Its pass held the Secret (A1, the line, A2), then Connect
// failed, so it never got its owner finalizer, and it vanishes at once. The
// next process's startup recheck finds the Secret through its line, and R2
// finds no record naming it.
func TestIndexNeverEstablishedHolderDeletedWhileDown(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-never")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	_, c, stop := perNameProcess(t, env.Config, idx, failingConnect)
	c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), true)
	stop()
	w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), "w", metav1.GetOptions{})
	if err != nil || hasString(w.GetFinalizers(), ownerFinalizer) {
		t.Fatalf("the holder is established, or gone: %v, %v", w, err)
	}
	deleteWidget(t, ns, "w")
	gone(t, time.Minute, "widget w", widgetGet(ns, "w"))

	perNameRig(t, idx, nil, nil)
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
}

// §10.6.8, interleaving 3, in PerName mode: a pass's A1 and line land before
// the Disown switch, and its A2 after the loop has stripped the record, the
// Secret and the index. The A2's check after it puts the line back, so the
// loop's next pass counts the finalizer through the index: the settling
// resets, and the loop strips it again.
func TestIndexA1BeforeTheDisownSwitch(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-late")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	var armed, fired atomic.Bool
	reached, release, landed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	theA2 := func(c cu.Call) bool {
		return c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.StrategicMergePatchType
	}
	r := perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			if !armed.Load() || !theA2(c) || !fired.CompareAndSwap(false, true) {
				return nil
			}
			close(reached)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(c cu.Call, err error) {
			if fired.Load() && theA2(c) {
				select {
				case landed <- err:
				default:
				}
			}
		})
	})
	armed.Store(true)
	r.c.enqueue(widgetNN(ns, "w"))
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("the pass never reached its A2")
	}
	lined(t, idx, secretLine(ns, "s"), true)
	set("all")
	// The loop strips the record and empties the index.
	cutest.Eventually(t, time.Minute, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cu.ReadRecord(w).Present {
			return cutest.ErrNotYet
		}
		return nil
	})
	lined(t, idx, secretLine(ns, "s"), false)

	counts := watchHeldCount(t)
	close(release)
	select {
	case err := <-landed:
		if err != nil {
			t.Fatalf("the late A2: %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("the late A2 never returned")
	}
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	seen := counts()
	stripped(t, ns, "s")
	if !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" }) {
		t.Errorf("held-count from the late A2 on: %v; want a non-zero count before settling", seen)
	}
	lined(t, idx, secretLine(ns, "s"), false)
}

// watchHeldCount records every value of the CRD's held-count from now on,
// until the returned function is called, which returns them.
func watchHeldCount(t *testing.T) func() []string {
	var mu sync.Mutex
	var seen []string
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			crd, err := env.Dynamic.Resource(crdGVR).Get(context.Background(), widgetCRD, metav1.GetOptions{})
			if err != nil {
				continue
			}
			mu.Lock()
			if v := crd.GetAnnotations()[cu.AnnotationHeldCount]; len(seen) == 0 || seen[len(seen)-1] != v {
				seen = append(seen, v)
			}
			mu.Unlock()
		}
	}()
	var once sync.Once
	finish := func() []string {
		once.Do(func() {
			close(stop)
			<-done
		})
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(seen)
	}
	t.Cleanup(func() { finish() })
	return finish
}

// roleKey tags a context with the actor it is: "rel" for the write loops,
// "acq1" and "acq2" for the passes of holders h1 and h2.
type roleKey struct{}

func roleOf(ctx context.Context) string {
	r, _ := ctx.Value(roleKey{}).(string)
	return r
}

func withRole(ctx context.Context, role string) context.Context {
	return context.WithValue(ctx, roleKey{}, role)
}

// gates hold the steps of a forced schedule: a step reaches its gate and
// waits until the test opens it, or its context ends.
type gates struct {
	mu      sync.Mutex
	reached map[string]chan struct{}
	opened  map[string]chan struct{}
}

func newGates() *gates {
	return &gates{reached: map[string]chan struct{}{}, opened: map[string]chan struct{}{}}
}

func (g *gates) chans(name string) (reached, opened chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reached[name] == nil {
		g.reached[name], g.opened[name] = make(chan struct{}), make(chan struct{})
	}
	return g.reached[name], g.opened[name]
}

// hold is a step at gate name.
func (g *gates) hold(ctx context.Context, name string) error {
	reached, opened := g.chans(name)
	g.mu.Lock()
	select {
	case <-reached:
	default:
		close(reached)
	}
	g.mu.Unlock()
	select {
	case <-opened:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wait waits until a step reaches gate name.
func (g *gates) wait(t *testing.T, name string) {
	t.Helper()
	reached, _ := g.chans(name)
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatalf("no step reached gate %s", name)
	}
}

// open lets the steps at gate name, and every later one, go.
func (g *gates) open(name string) {
	_, opened := g.chans(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-opened:
	default:
		close(opened)
	}
}

// A supersetCase is one forced schedule of §10.6.3's interleaving.
type supersetCase struct {
	// startup: the prune is the write loops' startup recheck's, over a stale
	// line of a bare Secret; otherwise a release batch's, after R3.
	startup bool
	// pruneFirst: the prune's write lands before H2's index read after its
	// A2 (tP < tI); otherwise after it.
	pruneFirst bool
	// crashAt, if set, kills the process when a step reaches that gate:
	// "post", H2's read; "check", the pruner's read of P. The next process
	// must then restore the line.
	crashAt string
}

// runSuperset forces sc's schedule with interceptors holding each step
// (§14): R2 sees no users of P; H2 announces, its view showing P's line;
// R3; H2's A2; then H2's index read and the prune, in sc's order. Either
// way P's line is back, from H2's read when the prune came first, and from
// the pruner's read of P otherwise.
func runSuperset(t *testing.T, sc supersetCase) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-superset")
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	createSecret(t, ns, "s")
	line := secretLine(ns, "s")
	isS := func(c cu.Call) bool { return c.GK.Kind == "Secret" && c.Namespace == ns && c.Name == "s" }

	g := newGates()
	var armed, r3Issued, a2Issued, pruneIssued atomic.Bool
	var relIndexGets, acqUpdates, relUpdates atomic.Int32
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx})
	s.HookCalls(func(ctx context.Context, c cu.Call) error {
		role := roleOf(ctx)
		if role == "rel" && c.Op == "index-get" {
			if n := relIndexGets.Add(1); armed.Load() && sc.startup && n == 2 {
				return g.hold(ctx, "pruneget") // the prune's read: the recheck read P bare
			}
		}
		if !armed.Load() {
			return nil
		}
		switch {
		case !sc.startup && role == "rel" && c.Op == "patch" && c.PatchType == types.JSONPatchType && isS(c):
			err := g.hold(ctx, "r3")
			r3Issued.Store(true)
			return err
		case !sc.startup && role == "acq2" && c.Op == "patch" && c.PatchType == types.StrategicMergePatchType && isS(c):
			err := g.hold(ctx, "a2")
			a2Issued.Store(true)
			return err
		case role == "acq2" && c.Op == "patch" && c.PatchType == types.StrategicMergePatchType && isS(c):
			a2Issued.Store(true)
		case role == "acq2" && c.Op == "index-get" && a2Issued.Load():
			return g.hold(ctx, "post")
		case role == "acq2" && c.Op == "index-update" && a2Issued.Load():
			acqUpdates.Add(1)
		case role == "rel" && c.Op == "index-update" && (sc.startup || r3Issued.Load()):
			relUpdates.Add(1)
			var err error
			if !sc.startup {
				err = g.hold(ctx, "prune")
			}
			pruneIssued.Store(true)
			return err
		case role == "rel" && c.Op == "get" && isS(c) && pruneIssued.Load():
			return g.hold(ctx, "check")
		}
		return nil
	}, nil)
	o := widgetOptions(func(types.NamespacedName) {})
	o.DeletionGrace = testGrace
	perName(&o)
	tr, err := s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(tr)
	runCtx, kill := context.WithCancel(withRole(context.Background(), "rel"))
	var loops sync.WaitGroup
	t.Cleanup(func() {
		kill()
		loops.Wait()
	})
	stopInformers := runningInformers(t, s)
	t.Cleanup(stopInformers)
	startLoops := func() {
		loops.Add(1)
		go func() {
			defer loops.Done()
			_ = s.RunWriteLoops(runCtx)
		}()
	}
	passCtx, killPasses := context.WithCancel(context.Background())
	t.Cleanup(killPasses)
	passOf := func(role, name string) error {
		_, err := d.Reconcile(withRole(passCtx, role), widgetNN(ns, name))
		return err
	}

	if sc.startup {
		// A stale line for a bare Secret, which the startup recheck prunes.
		setIndexLines(t, idx, func(l []string) []string { return append(l, line) })
		cutest.Eventually(t, time.Minute, func() error {
			if !tr.PerNameSynced(cu.SecretRef(ns, "s").ID()) {
				return cutest.ErrNotYet
			}
			return nil
		})
		armed.Store(true)
		startLoops()
		g.wait(t, "pruneget")
	} else {
		startLoops()
		createWidget(t, ns, "h1", "s")
		if err := passOf("acq1", "h1"); err != nil {
			t.Fatal(err)
		}
		established(t, ns, "s", "h1")
		lined(t, idx, line, true)
		cutest.Eventually(t, time.Minute, func() error {
			if relIndexGets.Load() == 0 {
				return fmt.Errorf("the startup recheck hasn't run: %w", cutest.ErrNotYet)
			}
			return nil
		})
		armed.Store(true)
		deleteWidget(t, ns, "h1")
		if err := passOf("acq1", "h1"); err != nil {
			t.Fatal(err)
		}
		gone(t, time.Minute, "widget h1", widgetGet(ns, "h1"))
		g.wait(t, "r3") // R2 saw no users
		// The pruner's view of the records lags: H2's announce isn't in
		// its holder cache when it decides what to prune. With a current
		// cache, H2's record keeps the line (§10.6.2), and the race below
		// can't happen; a lagging cache is its window.
		tr.PauseHolderWatch()
		t.Cleanup(tr.ResumeHolderWatch)
	}

	// H2 announces, and its view shows P's line.
	createWidget(t, ns, "h2", "s")
	passDone := make(chan error, 1)
	go func() { passDone <- passOf("acq2", "h2") }()
	var passOnce sync.Once
	var passErr error
	passResult := func() error {
		passOnce.Do(func() { passErr = <-passDone })
		return passErr
	}
	if !sc.startup {
		g.wait(t, "a2")
		g.open("r3")
		g.wait(t, "prune") // R3 landed; the prune, decided, waits
		tr.ResumeHolderWatch()
		g.open("a2")
	}
	g.wait(t, "post") // H2's A2 landed; its read waits

	crash := func() {
		kill()
		killPasses()
		loops.Wait()
		stopInformers()
		_ = passResult()
	}
	prune := "prune"
	if sc.startup {
		prune = "pruneget"
	}
	if sc.pruneFirst {
		g.open(prune)
		g.wait(t, "check") // the prune landed; the pruner's read of P waits
		if sc.crashAt == "post" {
			crash()
		} else {
			g.open("post")
			if err := passResult(); err != nil {
				t.Fatalf("H2's pass: %v", err)
			}
			if acqUpdates.Load() == 0 {
				t.Error("H2's read after the prune didn't add the line back")
			}
			g.open("check")
		}
	} else {
		g.open("post")
		if err := passResult(); err != nil {
			t.Fatalf("H2's pass: %v", err)
		}
		if acqUpdates.Load() != 0 {
			t.Errorf("H2 wrote the index %d times after its A2, reading before the prune", acqUpdates.Load())
		}
		g.open(prune)
		g.wait(t, "check")
		if sc.crashAt == "check" {
			crash()
		} else {
			g.open("check")
		}
	}

	if sc.crashAt != "" {
		// The process died between a write and its check: P carries the
		// finalizer without its line, until H2's next pass, whose written
		// set is empty, adds it before its A2 (§10.6.3, L2).
		if err := heldFor(ns, "s", ns)(); err != nil {
			t.Fatal(err)
		}
		if lines, _ := indexLines(t, idx); slices.Contains(lines, line) {
			t.Fatalf("the line is there after the crash: %v; the case tests nothing", lines)
		}
		r := perNameRig(t, idx, nil, nil)
		r.c.enqueue(widgetNN(ns, "h2"))
		lined(t, idx, line, true)
		if v := r.c.violations(); len(v) > 0 {
			t.Errorf("violations: %v", v)
		}
		return
	}
	lined(t, idx, line, true)
	if err := heldFor(ns, "s", ns)(); err != nil {
		t.Fatal(err)
	}
	if !sc.pruneFirst && relUpdates.Load() < 2 {
		t.Errorf("the pruner wrote the index %d times; want the prune and the re-add after its read of P", relUpdates.Load())
	}
	// It stays: nothing prunes a line whose object carries the finalizer.
	time.Sleep(time.Second)
	lined(t, idx, line, true)
}

// §10.6.3's interleaving, with a release batch's prune, in both orders of
// H2's index read and the prune.
func TestIndexSupersetInterleaving(t *testing.T) {
	t.Run("prune before H2's read", func(t *testing.T) { runSuperset(t, supersetCase{pruneFirst: true}) })
	t.Run("prune after H2's read", func(t *testing.T) { runSuperset(t, supersetCase{}) })
}

// The same with the startup recheck's prune against an A2.
func TestIndexSupersetStartupPrune(t *testing.T) {
	t.Run("prune before H2's read", func(t *testing.T) { runSuperset(t, supersetCase{startup: true, pruneFirst: true}) })
	t.Run("prune after H2's read", func(t *testing.T) { runSuperset(t, supersetCase{startup: true}) })
}

// Drain (§14): the process dies between H2's A2 and its index check, after
// a prune; and between the prune and the pruner's check. The next process
// converges, with the line back.
func TestIndexDrain(t *testing.T) {
	t.Run("between the A2 and its check", func(t *testing.T) { runSuperset(t, supersetCase{pruneFirst: true, crashAt: "post"}) })
	t.Run("between the prune and its check", func(t *testing.T) { runSuperset(t, supersetCase{crashAt: "check"}) })
}

// indexMetrics records the index's metrics.
type indexMetrics struct {
	cu.NopMetrics
	mu       sync.Mutex
	tampered int
	fill     float64
}

func (m *indexMetrics) IndexTampered(string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tampered++
}

func (m *indexMetrics) IndexFillRatio(_ string, r float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fill = r
}

func (m *indexMetrics) get() (int, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tampered, m.fill
}

// eventSeen waits until the rig has recorded an event containing all of
// parts.
func eventSeen(t *testing.T, r *writeRig, parts ...string) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, e := range r.seen {
			all := true
			for _, p := range parts {
				all = all && strings.Contains(e, p)
			}
			if all {
				return nil
			}
		}
		return fmt.Errorf("no event with %q: %w", parts, cutest.ErrNotYet)
	})
}

// A hand edit that drops a held line is restored, with the event and the
// tampering metric; a dropped line whose R3 the per-name watch has shown,
// a bare Secret's, is never read (§10.6.6, L5).
func TestIndexTampering(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-tamper")
	createSecret(t, ns, "s")
	createSecret(t, ns, "bare")
	createWidget(t, ns, "w", "s")
	m := &indexMetrics{}
	var bareGets atomic.Int32
	var counting atomic.Bool
	r := perNameRig(t, idx, func(o *cu.Options) { o.Metrics = m }, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if counting.Load() && c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == ns && c.Name == "bare" {
				bareGets.Add(1)
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, secretLine(ns, "s"), true)

	// A stale line for the bare Secret, watched by name.
	setIndexLines(t, idx, func(l []string) []string { return append(l, secretLine(ns, "bare")) })
	cutest.Eventually(t, time.Minute, func() error {
		if !r.tr.PerNameSynced(cu.SecretRef(ns, "bare").ID()) {
			return cutest.ErrNotYet
		}
		return nil
	})
	counting.Store(true)
	setIndexLines(t, idx, func(l []string) []string {
		return slices.DeleteFunc(l, func(x string) bool { return x == secretLine(ns, "s") || x == secretLine(ns, "bare") })
	})
	lined(t, idx, secretLine(ns, "s"), true)
	eventSeen(t, r, cu.EventIndexTampered, ns+"/Secret/s")
	cutest.Eventually(t, time.Minute, func() error {
		if n, _ := m.get(); n != 1 {
			return fmt.Errorf("tampered %d: %w", n, cutest.ErrNotYet)
		}
		return nil
	})
	time.Sleep(2 * time.Second) // the write loops' ticks
	if n := bareGets.Load(); n != 0 {
		t.Errorf("the bare Secret's dropped line was read %d times; its R3 was in the per-name cache", n)
	}
	if lines, _ := indexLines(t, idx); slices.Contains(lines, secretLine(ns, "bare")) {
		t.Errorf("the bare Secret's line came back: %v", lines)
	}
	if pending, _ := r.tr.PendingIndexChecks(); len(pending) != 0 {
		t.Errorf("checks still pending: %v", pending)
	}
}

// The rebuild after a deletion merges with an add another writer makes
// meanwhile (§10.6.6, L5): its create finds the index there, and it merges
// into it.
func TestIndexRebuildMergesWithAConcurrentAdd(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-merge")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	foreign := secretLine(ns, "foreign")
	var armed, fired atomic.Bool
	r := perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			if armed.Load() && c.Op == "index-create" && fired.CompareAndSwap(false, true) {
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx}, Data: map[string]string{"entries": foreign + "\n"}}
				if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
					t.Errorf("the concurrent create: %v", err)
				}
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, secretLine(ns, "s"), true)
	armed.Store(true)
	if err := env.Kube.CoreV1().ConfigMaps(idx).Delete(context.Background(), widgetIndex, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	lined(t, idx, secretLine(ns, "s"), true)
	lined(t, idx, foreign, true)
	if !fired.Load() {
		t.Error("the rebuild never created the index")
	}
}

// A missing or unreadable index keeps Disown's count "unknown", never "0"
// (§10.6.5, L4); once it can be read again, Disown settles.
func TestIndexUnreadableKeepsTheCountUnknown(t *testing.T) {
	cutest.Require(t, env)
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			set := disowning(t)
			idx, ns := indexNamespace(t), env.Namespace(t, "idx-unknown")
			createSecret(t, ns, "s")
			createWidget(t, ns, "w", "s")
			var failing atomic.Bool
			r := perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
				s.HookCalls(func(_ context.Context, c cu.Call) error {
					if failing.Load() && (c.Op == "index-get" && !missing || c.Op == "index-create" && missing) {
						return errors.New("the index can't be read")
					}
					return nil
				}, nil)
			})
			r.c.enqueue(widgetNN(ns, "w"))
			established(t, ns, "s", "w")
			lined(t, idx, secretLine(ns, "s"), true)
			failing.Store(true)
			if missing {
				if err := env.Kube.CoreV1().ConfigMaps(idx).Delete(context.Background(), widgetIndex, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			counts := watchHeldCount(t)
			set("all")
			cutest.Eventually(t, time.Minute, func() error {
				if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" {
					return fmt.Errorf("held-count %q: %w", a[cu.AnnotationHeldCount], cutest.ErrNotYet)
				}
				return nil
			})
			time.Sleep(4 * disownInterval)
			if a := widgetCRDAnnotations(t); a[cu.AnnotationDisownSettled] != "" || a[cu.AnnotationHeldCount] != "unknown" {
				t.Errorf("with the index unreadable: held-count %q, disown-settled %q", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled])
			}
			if seen := counts(); slices.Contains(seen, "0") {
				t.Errorf("held-count was 0 with the index unreadable: %v", seen)
			}
			failing.Store(false)
			cutest.Eventually(t, time.Minute, settledAtZero(t))
		})
	}
}

// At 80% of the cap the tracker warns, with the event and the gauge; a full
// index fails closed for a new holder, and is Degraded for an established
// one (§10.6.4, L2).
func TestIndexFillsAndFails(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-full")
	m := &indexMetrics{}
	r := newWriteRig(t, env.Config, cu.SharedOptions{IndexNamespace: idx, IndexMaxLines: 5}, func(o *cu.Options) {
		perName(o)
		o.Metrics = m
	}, nil)
	for i := 1; i <= 6; i++ {
		createSecret(t, ns, fmt.Sprintf("s%d", i))
	}
	for i := 1; i <= 4; i++ {
		createWidget(t, ns, fmt.Sprintf("w%d", i), fmt.Sprintf("s%d", i))
		r.c.enqueue(widgetNN(ns, fmt.Sprintf("w%d", i)))
		established(t, ns, fmt.Sprintf("s%d", i), fmt.Sprintf("w%d", i))
	}
	eventSeen(t, r, cu.EventIndexFilling, "80%")
	if _, fill := m.get(); fill < 0.8 {
		t.Errorf("fill ratio %v, want 0.8", fill)
	}
	createWidget(t, ns, "w5", "s5")
	r.c.enqueue(widgetNN(ns, "w5"))
	established(t, ns, "s5", "w5")

	// Full: a new holder fails closed, and its Secret isn't held.
	createWidget(t, ns, "w6", "s6")
	r.c.enqueue(widgetNN(ns, "w6"))
	cutest.Eventually(t, time.Minute, func() error {
		a := r.c.lastResult(widgetNN(ns, "w6")).Acquired
		if !a.MustNotConnect || a.Err == nil || !strings.Contains(a.Err.Error(), "index is full") {
			return fmt.Errorf("w6: %+v: %w", a, cutest.ErrNotYet)
		}
		return nil
	})
	if err := notHeldFor(ns, "s6", ns)(); err != nil {
		t.Error(err)
	}
	deleteWidget(t, ns, "w6")
	gone(t, time.Minute, "widget w6", widgetGet(ns, "w6"))
	// An established holder re-pointed at s6 is Degraded, and proceeds. Its
	// A1 drops s1, whose release frees a line afterwards; the pass itself
	// meets the full index.
	repoint(t, ns, "w1", "s6")
	r.c.enqueue(widgetNN(ns, "w1"))
	cutest.Eventually(t, time.Minute, func() error {
		res := r.c.lastResult(widgetNN(ns, "w1"))
		if !res.Acquired.Degraded || res.Acquired.Err == nil || !strings.Contains(res.Acquired.Err.Error(), "index is full") || !res.Connected {
			return fmt.Errorf("w1: %+v: %w", res, cutest.ErrNotYet)
		}
		return nil
	})
}

// Start fails clearly when the index namespace is absent (§7.3, §13 shape
// 1), and when holders of the kind live in it; a holder created there later
// gets no hold (§10.6.1).
func TestIndexNamespaceChecks(t *testing.T) {
	cutest.Require(t, env)
	resetPointer(t)
	t.Cleanup(func() { resetPointer(t) })
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	track := func(idx string) (*cu.Tracker, error) {
		s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx})
		o := widgetOptions(func(types.NamespacedName) {})
		perName(&o)
		return s.Track(context.Background(), o)
	}
	_, err := track("cu-idx-absent")
	if want := "credential-use: index namespace cu-idx-absent not found: install it (installer release) before enabling PerName"; err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("an absent namespace: %v, want %q", err, want)
	}
	if got := widgetCRDAnnotations(t)[cu.AnnotationIndex]; got != "" {
		t.Errorf("the refused start wrote the pointer %q", got)
	}

	crowded := env.Namespace(t, "cu-idx-crowded")
	createSecret(t, crowded, "s")
	createWidget(t, crowded, "w", "s")
	if _, err := track(crowded); err == nil || !strings.Contains(err.Error(), "holds holders of the kind") {
		t.Errorf("a namespace with holders: %v", err)
	}

	idx := env.Namespace(t, "cu-idx-later")
	tr, err := track(idx)
	if err != nil {
		t.Fatal(err)
	}
	createSecret(t, idx, "s")
	createWidget(t, idx, "new", "s")
	createWidget(t, idx, "old", "s", ownerFinalizer)
	for name, check := range map[string]func(a cu.Acquired) bool{
		"new": func(a cu.Acquired) bool { return a.MustNotConnect },
		"old": func(a cu.Acquired) bool { return a.Degraded },
	} {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(idx).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		a, err := tr.Acquire(context.Background(), w)
		if err != nil || !check(a) || a.Err == nil || !strings.Contains(a.Err.Error(), "index namespace") || len(a.Held) != 0 {
			t.Errorf("%s holder in the index namespace: %+v, %v", name, a, err)
		}
	}
	if err := notHeldFor(idx, "s", idx)(); err != nil {
		t.Error(err)
	}
}

// The index is created at the first start, and rebuilt from the records
// then: a kind moved from LabelScoped to PerName starts with a superset
// (§10.6.1, §10.6.6).
func TestIndexCreatedAtStartRebuildsFromRecords(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-created")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	labelScoped := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	labelScoped.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	labelScoped.stop()
	if _, ok := indexLines(t, idx); ok {
		t.Fatal("an index exists before the first PerName start")
	}
	perNameRig(t, idx, nil, nil)
	lines, ok := indexLines(t, idx)
	if !ok || !slices.Contains(lines, secretLine(ns, "s")) {
		t.Fatalf("the index after the first start: %v, %v; want the held Secret's line", lines, ok)
	}
}

// DisownPrefix takes the names from an index given explicitly, for a
// deleted PerName kind whose CRD can no longer point at it (§7.4): a dry
// run reports, a run strips, and the index is only read.
func TestDisownPrefixFromTheIndex(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns := env.Namespace(t, "cu-idx-prefix"), env.Namespace(t, "disown-prefix-index")
	createSecret(t, ns, "s")
	epoch, _ := cu.NewEpoch()
	p, err := cu.AcquireStrategicPatch(gadgetGK, ns, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := send(ctx, secrets(env.Metadata, ns), "s", p); err != nil {
		t.Fatal(err)
	}
	name := cu.IndexName(gadgetGK)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: idx}, Data: map[string]string{"entries": secretLine(ns, "s") + "\n" + secretLine(ns, "gone") + "\n"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	o := cu.DisownPrefixOptions{Holder: gadgetGK, Index: &types.NamespacedName{Namespace: idx, Name: name}, DryRun: true}
	rep, err := cu.DisownPrefix(ctx, clientsFor(t, env.Config), o)
	if err != nil || len(rep.Objects) != 1 || rep.Finalizers != 1 || len(rep.Stripped) != 0 {
		t.Fatalf("dry run = %+v, %v", rep, err)
	}
	f, _ := cu.FinalizerName(gadgetGK, ns)
	if err := heldForKind(ns, "s", f); err != nil {
		t.Fatalf("the dry run stripped: %v", err)
	}
	o.DryRun = false
	rep, err = cu.DisownPrefix(ctx, clientsFor(t, env.Config), o)
	if err != nil || len(rep.Stripped) != 1 {
		t.Fatalf("run = %+v, %v", rep, err)
	}
	if err := heldForKind(ns, "s", f); err == nil {
		t.Error("the run left the finalizer")
	}
	after, err := env.Kube.CoreV1().ConfigMaps(idx).Get(ctx, name, metav1.GetOptions{})
	if err != nil || after.Data["entries"] != cm.Data["entries"] {
		t.Errorf("the index changed: %v, %v", after, err)
	}
	o.Index.Name = "cu-index-absent"
	if _, err := cu.DisownPrefix(ctx, clientsFor(t, env.Config), o); err == nil {
		t.Error("a missing index is no error")
	}
}

// heldForKind reports nil when Secret ns/name carries finalizer f.
func heldForKind(ns, name, f string) error {
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !hasString(s.Finalizers, f) {
		return fmt.Errorf("%s/%s lacks %s", ns, name, f)
	}
	return nil
}

// A rebuild that fails right after the start created the index is left to
// the write loops, which retry it: the holds made before the index get their
// lines without any holder's pass (§10.6.2, §10.6.6). A later start would
// find the index present, and never rebuild it.
func TestIndexSetupRebuildFailureIsRetried(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-setup-rebuild")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	labelScoped := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	labelScoped.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	labelScoped.stop()

	var failed atomic.Bool
	perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			// The rebuild's list of every holder, once.
			if c.Op == "list" && c.GK == widgetGK && c.Namespace == "" && failed.CompareAndSwap(false, true) {
				return errors.New("the API server is busy")
			}
			return nil
		}, nil)
	})
	if !failed.Load() {
		t.Fatal("the start's rebuild never listed the holders")
	}
	lined(t, idx, secretLine(ns, "s"), true)
}

// indexRebuilt reports whether the index in idx carries the marker of a
// completed rebuild (§10.6.6).
func indexRebuilt(t *testing.T, idx string) bool {
	t.Helper()
	cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(context.Background(), widgetIndex, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cm.Annotations[cu.AnnotationRebuilt] == "true"
}

// heldBeforeTheIndex has a LabelScoped process hold Secret ns/s for Widget
// ns/w, and stop: a hold the PerName index doesn't know.
func heldBeforeTheIndex(t *testing.T, ns string) {
	t.Helper()
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	r.stop()
}

// A process that creates the index and dies before its rebuild leaves it
// without the marker; the next start rebuilds it, and the hold made before
// the index gets its line (§10.6.6, the user's decision of 2026-11-22). A
// rebuild that fails never sets the marker.
func TestIndexCrashBeforeTheRebuildIsRebuiltAtRestart(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-crash-rebuild")
	heldBeforeTheIndex(t, ns)
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })

	// The process creates the index, and dies before its rebuild: every
	// list of the holders fails, and it never runs.
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx})
	created := false
	s.HookCalls(func(_ context.Context, c cu.Call) error {
		if c.Op == "index-create" {
			created = true
		}
		if c.Op == "list" && c.GK == widgetGK && c.Namespace == "" {
			return errors.New("the process died")
		}
		return nil
	}, nil)
	o := widgetOptions(func(types.NamespacedName) {})
	perName(&o)
	if _, err := s.Track(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("the dying process didn't create the index")
	}
	if lines, _ := indexLines(t, idx); slices.Contains(lines, secretLine(ns, "s")) || indexRebuilt(t, idx) {
		t.Fatalf("after the crash: lines %v, marker %v; want neither", lines, indexRebuilt(t, idx))
	}

	perNameRig(t, idx, nil, nil)
	lined(t, idx, secretLine(ns, "s"), true)
	if !indexRebuilt(t, idx) {
		t.Error("the restart's rebuild didn't set the marker")
	}
}

// An index an acquirer recreated, after a deletion while no write loops
// ran, carries no marker, and its merges set none; the next start rebuilds
// it, and the holds it lacked get their lines (§10.6.6).
func TestIndexAcquirerCreatedIndexIsRebuiltAtStart(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-acquirer-created")
	heldBeforeTheIndex(t, ns)
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })

	// A process whose Track found the index rebuilt, and whose write loops
	// never run.
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx})
	o := widgetOptions(func(types.NamespacedName) {})
	perName(&o)
	tr, err := s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if !indexRebuilt(t, idx) {
		t.Fatal("Track's rebuild didn't set the marker")
	}
	if err := env.Kube.CoreV1().ConfigMaps(idx).Delete(context.Background(), widgetIndex, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"s2", "s3"} {
		createSecret(t, ns, n)
		createWidget(t, ns, "w-"+n, n)
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), "w-"+n, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if a, err := tr.Acquire(context.Background(), w); err != nil || len(a.Held) != 1 {
			t.Fatalf("acquiring for w-%s: %+v, %v", n, a, err)
		}
		if indexRebuilt(t, idx) {
			t.Fatalf("an acquirer's create or merge set the marker (after w-%s)", n)
		}
	}
	if lines, _ := indexLines(t, idx); slices.Contains(lines, secretLine(ns, "s")) || !slices.Contains(lines, secretLine(ns, "s3")) {
		t.Fatalf("the acquirer's index: %v", lines)
	}

	perNameRig(t, idx, nil, nil)
	lined(t, idx, secretLine(ns, "s"), true)
	if !indexRebuilt(t, idx) {
		t.Error("the start's rebuild didn't set the marker")
	}
}

// warnLog records the warnings a tracker logs.
type warnLog struct {
	mu    sync.Mutex
	warns []string
}

func (l *warnLog) Info(string, ...any)         {}
func (l *warnLog) Debug(string, ...any)        {}
func (l *warnLog) Error(error, string, ...any) {}
func (l *warnLog) Warn(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprint(msg, kv))
}
func (l *warnLog) WithValues(...any) logging.Logger { return l }
func (l *warnLog) WithName(string) logging.Logger   { return l }

// matching returns the warnings that contain s.
func (l *warnLog) matching(s string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, w := range l.warns {
		if strings.Contains(w, s) {
			out = append(out, w)
		}
	}
	return out
}

// The startup recheck alone finds a hold whose per-name watch the server
// forbids: a process that may get and patch a Secret by name, but not list
// or watch it, has no cache of it, and its rescan sees nothing. The recheck
// reads the index, GETs the Secret, and enqueues its batch, which releases
// it, its holder being gone (§10.6.5).
func TestIndexStartupRecheckWithoutTheWatch(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-recheck-nowatch")
	const sa = "cu-nowatch"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch"})
	createSecret(t, ns, "s")
	acquireAs(t, ns, "s") // held for ns, by a holder that is gone
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx, Annotations: map[string]string{cu.AnnotationRebuilt: "true"}},
		Data: map[string]string{"entries": secretLine(ns, "s") + "\n"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	_, c, _ := perNameProcess(t, env.AsServiceAccount("default", sa), idx, nil)
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
	if v := c.violations(); len(v) > 0 {
		t.Errorf("violations: %v", v)
	}
}

// Disown counts, strips and prunes a hold that only the index knows (§7.4,
// A15): no record names the Secret, a PerName kind is never listed, and the
// release loop can't take it, its batch's list of holders failing. The
// count is non-zero before it settles, the Secret ends stripped, and the
// index empty.
func TestDisownCountsAndEmptiesTheIndex(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-index")
	createSecret(t, ns, "x")
	acquireAs(t, ns, "x")
	perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			// R2's consistent list of the holders in ns: the release loop
			// never decides. Disown lists every namespace.
			if c.Op == "list" && c.GK == widgetGK && c.Namespace == ns {
				return errors.New("the release loop can't list the holders")
			}
			return nil
		}, nil)
	})
	setIndexLines(t, idx, func(l []string) []string { return append(l, secretLine(ns, "x")) })
	if err := heldFor(ns, "x", ns)(); err != nil {
		t.Fatal(err)
	}
	counts := watchHeldCount(t)
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if seen := counts(); !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" && v != "unknown" }) {
		t.Errorf("held-count before settling: %v; want the index-only hold counted", seen)
	}
	stripped(t, ns, "x")
	lined(t, idx, secretLine(ns, "x"), false)
	if lines, _ := indexLines(t, idx); len(lines) != 0 {
		t.Errorf("the index after Disown: %v; want it empty", lines)
	}
}

// An object a record names that the process can't read, here because the
// API server keeps failing for it, stops neither the startup recheck nor the
// rest of the write loops' recheck (the reviewer's probe of the marker): the
// index's rebuild keeps its line and sets the marker; the recheck goes on
// past it, and returns the error for the retry,
// releasing a readable stale hold and pruning a bare line; and §10.5's list
// outside the caches still runs, for another holder kind, and releases a
// hold stranded there.
func TestIndexRecheckWithAnUnreadableRecordedObject(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns, cached := indexNamespace(t), env.Namespace(t, "idx-unreadable"), env.Namespace(t, "idx-unreadable-cached")
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	resetPublished(t, cutest.TargetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.TargetGVR) })

	// The Widget kind, PerName: a record names a-nogrant, which can't be
	// read, a transient error every time (a 403 is reported once and not
	// retried, TestIndexRecheckDoesntRetryADeniedLine); z-held is a stale hold; z-stale's line names a bare Secret. The
	// index has no marker.
	for _, n := range []string{"a-nogrant", "z-held", "z-stale"} {
		createSecret(t, ns, n)
	}
	acquireAs(t, ns, "z-held")
	createWidget(t, ns, "w", "a-nogrant")
	rec, err := cu.EncodeRecord([]cu.ID{cu.SecretRef(ns, "a-nogrant").ID()})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, rec))
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(ctx, "w", types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx},
		Data: map[string]string{"entries": strings.Join([]string{secretLine(ns, "a-nogrant"), secretLine(ns, "z-held"), secretLine(ns, "z-stale")}, "\n") + "\n"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// The Target kind, LabelScoped on ConfigMaps, its caches restricted to
	// another namespace: a hold stranded in ns, with no holder, is found
	// only by §10.5's list outside the caches.
	targetGK := cutest.TargetGVK.GroupKind()
	if _, err := env.Kube.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "stranded"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	epoch, _ := cu.NewEpoch()
	p, err := cu.AcquireStrategicPatch(targetGK, ns, epoch)
	if err != nil {
		t.Fatal(err)
	}
	configMaps := env.Metadata.Resource(corev1.SchemeGroupVersion.WithResource("configmaps")).Namespace(ns)
	if _, err := send(ctx, configMaps, "stranded", p); err != nil {
		t.Fatal(err)
	}
	fTarget, _ := cu.FinalizerName(targetGK, ns)

	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx, Namespaces: []string{cached}})
	s.HookCalls(func(_ context.Context, c cu.Call) error {
		if c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == ns && c.Name == "a-nogrant" {
			return apierrors.NewServiceUnavailable("the API server can't read a-nogrant")
		}
		return nil
	}, nil)
	ow := widgetOptions(func(types.NamespacedName) {})
	perName(&ow)
	if _, err := s.Track(ctx, ow); err != nil {
		t.Fatalf("Track with an unreadable recorded object: %v", err)
	}
	ot := cu.Options{
		Holder: cutest.TargetGVK, OwnerFinalizers: []string{ownerFinalizer}, TimeoutBound: 90 * time.Second,
		Refs:      func(metav1.Object) ([]cu.Ref, error) { return nil, nil },
		Enqueue:   func(types.NamespacedName) {},
		Protected: []cu.ProtectedKind{{GVK: corev1.SchemeGroupVersion.WithKind("ConfigMap")}},
	}
	if _, err := s.Track(ctx, ot); err != nil {
		t.Fatal(err)
	}
	if !indexRebuilt(t, idx) {
		t.Error("the rebuild at Track didn't complete and set the marker")
	}
	running(t, s)

	cutest.Eventually(t, time.Minute, notHeldFor(ns, "z-held", ns))
	lined(t, idx, secretLine(ns, "z-stale"), false)
	lined(t, idx, secretLine(ns, "a-nogrant"), true)
	cutest.Eventually(t, time.Minute, func() error {
		m, err := configMaps.Get(ctx, "stranded", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if hasString(m.Finalizers, fTarget) {
			return fmt.Errorf("the stranded hold outside the caches stays: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if !indexRebuilt(t, idx) {
		t.Error("the marker went")
	}
}

// deniedMetrics counts the index's refused reads, and keeps Disown's
// unreadable gauge.
type deniedMetrics struct {
	cu.NopMetrics
	mu         sync.Mutex
	denied     int
	unreadable int
}

func (m *deniedMetrics) IndexReadDenied(string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.denied++
}

func (m *deniedMetrics) DisownUnreadable(_ string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unreadable = n
}

func (m *deniedMetrics) get() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.denied, m.unreadable
}

// A line whose object the authorizer refuses to read (an RBAC 403, class
// (d)) doesn't make the write loops' recheck retry: retrying can't help until RBAC changes, and
// the line is kept, so nothing is stranded. The refusal is logged and
// counted once; a readable stale hold beside it is released (§10.6.5).
func TestIndexRecheckDoesntRetryADeniedLine(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-denied")
	createSecret(t, ns, "a-denied")
	createSecret(t, ns, "z-held")
	acquireAs(t, ns, "z-held")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx, Annotations: map[string]string{cu.AnnotationRebuilt: "true"}},
		Data: map[string]string{"entries": secretLine(ns, "a-denied") + "\n" + secretLine(ns, "z-held") + "\n"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	m := &deniedMetrics{}
	logs := &warnLog{}
	var deniedGets atomic.Int32
	perNameRig(t, idx, func(o *cu.Options) { o.Metrics, o.Logger = m, logs }, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == ns && c.Name == "a-denied" {
				deniedGets.Add(1)
				return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, c.Name, fmt.Errorf(
					`User "system:serviceaccount:default:provider" cannot get resource "secrets" in API group "" in the namespace %q`, ns))
			}
			return nil
		}, nil)
	})
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "z-held", ns))
	lined(t, idx, secretLine(ns, "z-held"), false)
	time.Sleep(4 * time.Second) // the recheck's backoff starts at 1s
	if n := deniedGets.Load(); n != 1 {
		t.Errorf("the denied Secret was read %d times; want once: the recheck retried", n)
	}
	if denied, _ := m.get(); denied != 1 {
		t.Errorf("index_read_denied counted %d, want 1", denied)
	}
	if w := logs.matching("outside the caches failed"); len(w) > 0 {
		t.Errorf("the recheck failed, and is retried: %v", w)
	}
	if len(logs.matching("can't be read (403)")) != 1 {
		t.Errorf("the refusal wasn't logged once: %v", logs.matching("403"))
	}
	lined(t, idx, secretLine(ns, "a-denied"), true)
}

// Disown under resourceNames RBAC with an index line for a Secret outside
// the grant, which no record names: it can't be read, so held-count stays
// "unknown", never zero, and a Warning event on the CRD names it, with the
// gauge; once the line is removed by hand, the documented remedy, Disown
// settles (§7.4, the user's decision of 2026-11-22).
func TestDisownWithAnUnreadableIndexLine(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-unreadable")
	const sa = "cu-disown-unreadable"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch", "list", "watch"})
	createSecret(t, ns, "nogrant")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx, Annotations: map[string]string{cu.AnnotationRebuilt: "true"}},
		Data: map[string]string{"entries": secretLine(ns, "nogrant") + "\n"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	m := &deniedMetrics{}
	r := newWriteRig(t, env.AsServiceAccount("default", sa), cu.SharedOptions{IndexNamespace: idx}, func(o *cu.Options) {
		perName(o)
		disownOptions(o)
		o.Metrics = m
	}, nil)
	set("all")
	cutest.Eventually(t, time.Minute, func() error {
		if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" {
			return fmt.Errorf("held-count %q: %w", a[cu.AnnotationHeldCount], cutest.ErrNotYet)
		}
		return nil
	})
	eventSeen(t, r, cu.EventDisownUnreadable, ns+"/Secret/nogrant: forbidden")
	if _, n := m.get(); n < 1 {
		t.Errorf("disown_unreadable = %d, want the unreadable Secret counted", n)
	}
	// The other tests' holders' records, which the first pass strips, may
	// name more; from the second pass the set is the one Secret's, and its
	// event isn't sent again on every pass.
	time.Sleep(2 * disownInterval)
	unreadableEvents := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		n := 0
		for _, e := range r.seen {
			if strings.Contains(e, cu.EventDisownUnreadable) {
				n++
			}
		}
		return n
	}
	before := unreadableEvents()
	time.Sleep(4 * disownInterval)
	if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" || a[cu.AnnotationDisownSettled] != "" {
		t.Fatalf("with the line unreadable: held-count %q, disown-settled %q", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled])
	}
	if after := unreadableEvents(); after != before {
		t.Errorf("the event was recorded %d more times over four passes for the same set: it isn't throttled", after-before)
	}
	setIndexLines(t, idx, func(l []string) []string {
		return slices.DeleteFunc(l, func(x string) bool { return x == secretLine(ns, "nogrant") })
	})
	cutest.Eventually(t, time.Minute, settledAtZero(t))
}
