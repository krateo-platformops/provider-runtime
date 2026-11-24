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
	"k8s.io/client-go/kubernetes"
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
// the Disown switch, and its A2 after the loop has stripped the record and
// counted zero. Disown keeps the line until it has settled, so its next pass
// counts the finalizer through the index, the record being gone: the
// settling resets, and the loop strips it again. So it does when the late
// A2's response is lost, and the pass never checks its line after it (§7.4).
//
// The line keeps the Secret's per-name watch too, whose event has the
// release loop take the late hold, nobody using it, before Disown's next
// pass: with it running the end is the same, whichever strips it first. With
// it held off, only Disown's count can find the hold.
func TestIndexA1BeforeTheDisownSwitch(t *testing.T) {
	cutest.Require(t, env)
	for _, tc := range []struct{ lost, releases bool }{{lost: false}, {lost: true}, {lost: true, releases: true}} {
		t.Run(fmt.Sprintf("response lost=%v, release loop=%v", tc.lost, tc.releases), func(t *testing.T) {
			lost := tc.lost
			ctx := context.Background()
			set := disowning(t)
			idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-late")
			createSecret(t, ns, "s")
			createWidget(t, ns, "w", "s")
			line := secretLine(ns, "s")
			var armed, fired atomic.Bool
			reached, release, landed := make(chan struct{}), make(chan struct{}), make(chan error, 2)
			theA2 := func(c cu.Call) bool {
				return c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.StrategicMergePatchType
			}
			// Index writes without the line, before Disown has settled.
			var early atomic.Int32
			unsettled := func() bool { return settledAtZero(t)() != nil }
			r := perNameRig(t, idx, func(o *cu.Options) {
				// Two intervals fit in TimeoutBound, which bounds the A2: it
				// lands after the first zero count, and before its end.
				o.DisownInterval, o.DisownPasses, o.TimeoutBound = 2*time.Second, 6, 5*time.Second
			}, func(s *cu.Shared) {
				s.HookCalls(func(_ context.Context, c cu.Call) error {
					if c.Op == "index-update" && armed.Load() && !slices.Contains(c.Lines, line) && unsettled() {
						early.Add(1)
					}
					// R2's list of the holders in ns: without it the release
					// loop never decides. Disown lists every namespace.
					if !tc.releases && c.Op == "list" && c.GK == widgetGK && c.Namespace == ns {
						return errors.New("the release loop can't list the holders")
					}
					if !armed.Load() || !theA2(c) || !fired.CompareAndSwap(false, true) {
						return nil
					}
					close(reached)
					select {
					case <-release:
					case <-time.After(time.Minute):
						return errors.New("never released")
					}
					if !lost {
						return nil
					}
					epoch, err := cu.NewEpoch()
					if err == nil {
						var p cu.Patch
						if p, err = cu.AcquireStrategicPatch(widgetGK, ns, epoch); err == nil {
							_, err = env.Kube.CoreV1().Secrets(ns).Patch(ctx, "s", p.Type, p.Data, metav1.PatchOptions{})
						}
					}
					landed <- err
					return apierrors.NewTimeoutError("the response was lost", 1)
				}, func(c cu.Call, err error) {
					if !lost && fired.Load() && theA2(c) {
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
			lined(t, idx, line, true)
			set("all")
			// The loop strips the record, and then counts zero: what it
			// counts from here on is the late hold, not the record.
			cutest.Eventually(t, time.Minute, func() error {
				w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if a := widgetCRDAnnotations(t); cu.ReadRecord(w).Present || a[cu.AnnotationHeldCount] != "0" {
					return fmt.Errorf("record %v, held-count %q: %w", cu.ReadRecord(w).Present, a[cu.AnnotationHeldCount], cutest.ErrNotYet)
				}
				return nil
			})
			lined(t, idx, line, true)
			counts := watchHeldCount(t)
			close(release)
			select {
			case err := <-landed:
				if err != nil {
					t.Fatalf("the late A2: %v", err)
				}
			case <-time.After(time.Minute):
				t.Fatal("the late A2 never landed")
			}
			if !unsettled() {
				t.Fatal("Disown settled before the late A2 landed; the test needs it to land before")
			}
			cutest.Eventually(t, 2*time.Minute, settledAtZero(t))
			seen := counts()
			stripped(t, ns, "s")
			if !tc.releases && !slices.Contains(seen, "1") {
				t.Errorf("held-count from the late A2 on: %v; want it counted before settling", seen)
			}
			if n := early.Load(); n != 0 {
				t.Errorf("the line was removed %d times before Disown settled", n)
			}
			lined(t, idx, line, false)
		})
	}
}

// Disown adds the lines of the objects a record names before it strips the
// record: a hold of one may still land (§7.4). Here the line was taken before
// the switch, as a prune may, and only the record names the Secret. The line
// goes once Disown has settled.
func TestDisownLinesWhatTheRecordsName(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-lines")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	rec, err := cu.EncodeRecord([]cu.ID{cu.SecretRef(ns, "s").ID()})
	if err != nil {
		t.Fatal(err)
	}
	r := perNameRig(t, idx, disownOptions, nil)
	r.c.stop() // no pass holds the Secret
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(ctx, "w", types.MergePatchType,
		[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, rec)), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	lined(t, idx, line, false)
	stripped := func() bool {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		return err == nil && !cu.ReadRecord(w).Present
	}
	set("all")
	for deadline := time.Now().Add(time.Minute); !stripped(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the record wasn't stripped")
		}
	}
	lines, ok := indexLines(t, idx)
	if !ok || !slices.Contains(lines, line) {
		t.Errorf("with the record stripped, the index has %v; want the Secret's line", lines)
	}
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	lined(t, idx, line, false)
}

// What empties the index when the process that settled Disown stops first
// (§7.4): the lines left name nothing held, and the next process counts
// none, settles by its own run, and empties it.
func TestDisownLinesLeftByTheSettler(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-left")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	var refuse atomic.Bool
	r := perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if refuse.Load() && c.Op == "index-update" && !slices.Contains(c.Lines, line) {
				return errors.New("the process stops before it empties the index")
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, line, true)
	refuse.Store(true)
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	r.stop()
	lined(t, idx, line, true)
	stripped(t, ns, "s")

	// The flag the first process left stays: it is of this switch, and
	// nothing has been held since.
	counts, settled := watchHeldCount(t), watchCRDAnnotation(t, cu.AnnotationDisownSettled)
	perNameRig(t, idx, disownOptions, nil)
	lined(t, idx, line, false)
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if seen := counts(); slices.ContainsFunc(seen, func(v string) bool { return v != "0" }) {
		t.Errorf("held-count with only a bare line left: %v; want 0 throughout", seen)
	}
	if seen := settled(); slices.ContainsFunc(seen, func(v string) bool { return v != "all" }) {
		t.Errorf("disown-settled in the second process: %v; want all throughout", seen)
	}
}

// A switch turned off and on again starts the settling over, and a hold made
// in between is counted and stripped (§7.4). The flag of the run before is
// gone before the hold is there, though the loop may not have looked yet. No
// pass runs after the switch is back, so that only Disown drops the ref.
func TestDisownTurnedOffAndOnAgain(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-again")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	r := perNameRig(t, idx, disownOptions, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	lined(t, idx, secretLine(ns, "s"), false)
	setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationDisown: nil})
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))
	if v := widgetCRDAnnotations(t)[cu.AnnotationDisownSettled]; v != "" {
		t.Errorf("held again, with disown-settled %q still there", v)
	}
	established(t, ns, "s", "w")
	lined(t, idx, secretLine(ns, "s"), true)
	r.c.stop()
	counts, again := watchHeldCount(t), time.Now()
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if got, min := time.Since(again), (disownPasses-1)*disownInterval-150*time.Millisecond; got < min {
		t.Errorf("settled %s after the switch was set again, want at least %s", got, min)
	}
	if seen := counts(); !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" && v != "unknown" }) {
		t.Errorf("held-count after the switch was set again: %v; want the hold counted", seen)
	}
	stripped(t, ns, "s")
	lined(t, idx, secretLine(ns, "s"), false)
}

// watchHeldCount records every value of the CRD's held-count from now on,
// until the returned function is called, which returns them.
func watchHeldCount(t *testing.T) func() []string {
	return watchCRDAnnotation(t, cu.AnnotationHeldCount)
}

// watchCRDAnnotation records every value of the Widget CRD's annotation key
// from now on, until the returned function is called, which returns them.
func watchCRDAnnotation(t *testing.T, key string) func() []string {
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
			if v := crd.GetAnnotations()[key]; len(seen) == 0 || seen[len(seen)-1] != v {
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
	dead    chan struct{} // closed when the process is killed
}

func newGates() *gates {
	return &gates{reached: map[string]chan struct{}{}, opened: map[string]chan struct{}{}, dead: make(chan struct{})}
}

// errKilled is a call of a killed process: it doesn't happen.
var errKilled = errors.New("the process was killed")

// killed is errKilled once the process is killed: a context that ends is a
// stop, which what mustn't be cut short outlives, and not a crash.
func (g *gates) killed() error {
	select {
	case <-g.dead:
		return errKilled
	default:
		return nil
	}
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
		return g.killed()
	case <-g.dead:
		return errKilled
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
		if err := g.killed(); err != nil {
			return err
		}
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
		close(g.dead) // every later call of the process fails
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
		var some []string
		for _, e := range r.seen {
			all := true
			for _, p := range parts {
				all = all && strings.Contains(e, p)
			}
			if all {
				return nil
			}
			if strings.Contains(e, parts[0]) {
				some = append(some, e)
			}
		}
		return fmt.Errorf("no event with %q; with the first of them: %q: %w", parts, some, cutest.ErrNotYet)
	})
}

// A hand edit that drops a held line is restored, with the event and the
// tampering metric; a bare Secret's dropped line is read once, though the
// per-name watch shows it bare, and stays dropped (§10.6.6).
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
	cutest.Eventually(t, time.Minute, func() error {
		if bareGets.Load() == 0 {
			return fmt.Errorf("the bare Secret's dropped line wasn't read: %w", cutest.ErrNotYet)
		}
		return nil
	})
	time.Sleep(3 * time.Second) // the write loops' ticks: no second read
	if n := bareGets.Load(); n != 1 {
		t.Errorf("the bare Secret's dropped line was read %d times, want once", n)
	}
	if lines, ok := indexLines(t, idx); !ok || slices.Contains(lines, secretLine(ns, "bare")) {
		t.Errorf("the bare Secret's line came back, or the index is gone: %v", lines)
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

// vouchIndex sets the operator's vouch for the index itself in idx, for uid,
// or for the ConfigMap that is there.
func vouchIndex(t *testing.T, idx, uid string) {
	t.Helper()
	editIndex(t, idx, func(cm *corev1.ConfigMap) {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		if cm.Annotations[cu.AnnotationVouchedIndex] = uid; uid == "" {
			cm.Annotations[cu.AnnotationVouchedIndex] = string(cm.UID)
		}
	})
}

// staysUnknown waits until Disown's count is "unknown", and checks that it
// stays so for longer than a run of zero passes.
func staysUnknown(t *testing.T) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" {
			return fmt.Errorf("held-count %q: %w", a[cu.AnnotationHeldCount], cutest.ErrNotYet)
		}
		return nil
	})
	counts, settled := watchHeldCount(t), watchCRDAnnotation(t, cu.AnnotationDisownSettled)
	time.Sleep((disownPasses + 2) * disownInterval)
	if seen := counts(); slices.ContainsFunc(seen, func(v string) bool { return v != "unknown" }) {
		t.Errorf("held-count while unknown: %v", seen)
	}
	if seen := settled(); slices.ContainsFunc(seen, func(v string) bool { return v != "" }) {
		t.Errorf("disown-settled while the count is unknown: %v", seen)
	}
}

// Whether a missing index is a lost one, with Disown on before the kind's
// first Track (§7.4): it is when the CRD shows that the kind was tracked
// before, by the pointer or the published settings; a kind never tracked
// settles; and an index that is there, rebuilt, is intact whatever the CRD
// shows, and its lines count.
func TestIndexLostUnderDisown(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	ref := func(idx string) string { return idx + "/" + widgetIndex }
	for name, tc := range map[string]struct {
		before func(t *testing.T, idx, ns string)
		lost   bool
	}{
		"never tracked": {before: func(*testing.T, string, string) {}},
		"a pointer, and no index": {before: func(t *testing.T, idx, _ string) {
			setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationIndex: ref(idx)})
		}, lost: true},
		"moved from LabelScoped": {before: func(t *testing.T, _, _ string) {
			s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
			s.SetLandingMargin(0)
			o := widgetOptions(func(types.NamespacedName) {})
			o.DeletionGrace, o.ProbeInterval = testGrace, time.Second
			disownOptions(&o)
			if _, err := s.Track(ctx, o); err != nil {
				t.Fatal(err)
			}
		}, lost: true},
		"the CRD recreated, the index kept": {before: func(t *testing.T, idx, ns string) {
			createSecret(t, ns, "leftover")
			acquireAs(t, ns, "leftover")
			createIndex(t, idx, secretLine(ns, "leftover"))
		}},
	} {
		t.Run(name, func(t *testing.T) {
			set := disowning(t)
			idx, ns := indexNamespace(t), env.Namespace(t, "idx-lost")
			set("all")
			r := perNameRig(t, idx, disownOptions, func(*cu.Shared) { tc.before(t, idx, ns) })
			if !tc.lost {
				cutest.Eventually(t, time.Minute, settledAtZero(t))
				if name != "never tracked" {
					stripped(t, ns, "leftover")
				}
				return
			}
			staysUnknown(t)
			eventSeen(t, r, cu.EventIndexLost, cu.AnnotationVouchedIndex)
			// Another ConfigMap's vouch isn't this one's.
			vouchIndex(t, idx, "00000000-0000-0000-0000-000000000000")
			staysUnknown(t)
			vouchIndex(t, idx, "")
			cutest.Eventually(t, time.Minute, settledAtZero(t))
		})
	}
}

// A missing or unreadable index keeps Disown's count "unknown", never "0"
// (§10.6.5, L4). Once an unreadable one can be read again, Disown settles.
// A missing one was lost: the rebuild under the switch doesn't settle it
// either, until an operator vouches for the new index (§7.4).
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
			if missing {
				cutest.Eventually(t, time.Minute, func() error {
					if _, ok := indexLines(t, idx); !ok {
						return fmt.Errorf("the index isn't recreated: %w", cutest.ErrNotYet)
					}
					return nil
				})
				staysUnknown(t)
				eventSeen(t, r, cu.EventIndexLost, cu.AnnotationVouchedIndex)
				vouchIndex(t, idx, "")
			}
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
	// Track alone, with neither informers nor write loops: the rebuild is
	// Track's own, not the startup recheck's.
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{IndexNamespace: idx})
	o := widgetOptions(func(types.NamespacedName) {})
	perName(&o)
	if _, err := s.Track(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	lines, ok := indexLines(t, idx)
	if !ok || !slices.Contains(lines, secretLine(ns, "s")) || !indexRebuilt(t, idx) {
		t.Fatalf("the index right after Track: %v, %v, marker %v; want the held Secret's line, rebuilt", lines, ok, indexRebuilt(t, idx))
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
	var tr *cu.Tracker
	perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			// The rebuild's list of every holder, once.
			if c.Op == "list" && c.GK == widgetGK && c.Namespace == "" && failed.CompareAndSwap(false, true) {
				return errors.New("the API server is busy")
			}
			return nil
		}, nil)
		// Track's own result, before any write loop runs: the rebuild that
		// failed is queued for the write loops, as the marker's absence
		// would also make the startup recheck do.
		resetPublished(t, cutest.WidgetGVR)
		o := widgetOptions(func(types.NamespacedName) {})
		perName(&o)
		o.DeletionGrace, o.ProbeInterval = testGrace, time.Second
		var err error
		if tr, err = s.Track(context.Background(), o); err != nil {
			t.Fatal(err)
		}
		if _, lost := tr.PendingIndexChecks(); !lost {
			t.Error("the failed rebuild isn't queued for the write loops when Track returns")
		}
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
	return cm.Annotations[cu.AnnotationRebuilt] == string(cm.UID)
}

// An index restored from a backup isn't complete, whatever it carries: it may
// lack the lines of holds made after the backup, whose records are gone.
// Disown over it doesn't settle, until an operator vouches for it (§7.4,
// §10.6.6).
func TestARestoredIndexIsALostOne(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-restored")
	cms := env.Kube.CoreV1().ConfigMaps(idx)
	r := perNameRig(t, idx, disownOptions, nil)
	if !indexRebuilt(t, idx) {
		t.Fatal("the index of a kind never tracked isn't marked")
	}
	backup, err := cms.Get(ctx, widgetIndex, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r.stop()
	// Held since the backup, its record gone and its release not done yet.
	createSecret(t, ns, "s")
	acquireAs(t, ns, "s")
	if err := cms.Delete(ctx, widgetIndex, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	backup.ResourceVersion, backup.UID, backup.ManagedFields = "", "", nil
	if _, err := cms.Create(ctx, backup, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	set("all")
	r = perNameRig(t, idx, disownOptions, nil)
	staysUnknown(t)
	if f := finalizersOf(t, ns, "s"); len(f) != 1 {
		t.Errorf("the Secret's finalizers are %v, want the hold", f)
	}
	eventSeen(t, r, cu.EventIndexLost, cu.AnnotationVouchedIndex)
	// The steps: the hold comes off by its marker label, and the vouch.
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, "s", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	vouchIndex(t, idx, "")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
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
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-recheck-nowatch")
	const sa = "cu-nowatch"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch"})
	createSecret(t, ns, "s")
	acquireAs(t, ns, "s") // held for ns, by a holder that is gone
	createIndex(t, idx, secretLine(ns, "s"))
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
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-denied")
	createSecret(t, ns, "a-denied")
	createSecret(t, ns, "z-held")
	acquireAs(t, ns, "z-held")
	createIndex(t, idx, secretLine(ns, "a-denied"), secretLine(ns, "z-held"))
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
// gauge; removing the line by hand doesn't help, and once an operator vouches
// for the Secret, the documented remedy, Disown settles (§7.4, the user's
// decision of 2026-11-22). No write removes the line before that (§7.4).
func TestDisownWithAnUnreadableIndexLine(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-unreadable")
	const sa = "cu-disown-unreadable"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch", "list", "watch"})
	stripUnreadableRecords(t, kubeAs(t, env.AsServiceAccount("default", sa)))
	createSecret(t, ns, "nogrant")
	createIndex(t, idx, secretLine(ns, "nogrant"))
	line := secretLine(ns, "nogrant")
	// No index write that would leave the index without the line gets
	// through until the test has seen Disown settle over it; tried counts
	// them.
	var mayGo atomic.Bool
	var tried atomic.Int32
	m := &deniedMetrics{}
	r := newWriteRig(t, env.AsServiceAccount("default", sa), cu.SharedOptions{IndexNamespace: idx}, func(o *cu.Options) {
		perName(o)
		disownOptions(o)
		o.Metrics = m
	}, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if (c.Op == "index-update" || c.Op == "index-create") && !mayGo.Load() && !slices.Contains(c.Lines, line) {
				tried.Add(1)
				return errors.New("the index can't lose the line")
			}
			return nil
		}, nil)
	})
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
	// The set is the one Secret's, and its event isn't sent again on every
	// pass.
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

	// A hand removal alone isn't a remedy: the line of an object this
	// process can't read may name a hold, so it comes back (§10.6.3).
	// A pass between the removal and the restore may count the empty index
	// as "0", which is a true count then; the restore makes the next pass
	// "unknown" again, so Disown never settles on the removal alone.
	settledSeen := watchCRDAnnotation(t, cu.AnnotationDisownSettled)
	setIndexLines(t, idx, func(l []string) []string { return slices.DeleteFunc(l, func(x string) bool { return x == line }) })
	lined(t, idx, line, true)
	cutest.Eventually(t, time.Minute, func() error {
		if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" {
			return fmt.Errorf("held-count %q after the restore: %w", a[cu.AnnotationHeldCount], cutest.ErrNotYet)
		}
		return nil
	})
	time.Sleep(disownPasses*disownInterval + 2*time.Second)
	if seen := settledSeen(); slices.ContainsFunc(seen, func(v string) bool { return v != "" }) {
		t.Fatalf("disown-settled after the hand removal alone: %v", seen)
	}

	// The remedy: the operator vouches for it, and leaves the line. Every
	// pass meets the vouched line it can't read, skips it, and settles; then
	// the emptying, which the hook holds back until the test has looked,
	// removes the line for good.
	if n := tried.Load(); n != 0 {
		t.Errorf("%d index writes tried to remove the line before Disown settled", n)
	}
	editIndex(t, idx, func(cm *corev1.ConfigMap) {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[cu.AnnotationVouched] = ns + "/Secret/nogrant"
	})
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if lines, ok := indexLines(t, idx); !ok {
		t.Fatal("the index went before Disown settled")
	} else if !slices.Contains(lines, line) {
		t.Fatalf("the vouched line went before Disown settled: %v; the test needs Disown to meet it", lines)
	}
	mayGo.Store(true)
	lined(t, idx, line, false)
	time.Sleep(2 * time.Second) // the write loops' ticks
	if lines, ok := indexLines(t, idx); !ok {
		t.Error("the index went after Disown's emptying")
	} else if slices.Contains(lines, line) {
		t.Errorf("the vouched line came back: %v", lines)
	}
	if v := indexAnnotations(t, idx)[cu.AnnotationVouched]; v != ns+"/Secret/nogrant" {
		t.Errorf("the vouched annotation after Disown's writes: %q", v)
	}
}

// stripUnreadableRecords removes the records, left by other tests, that name
// a Secret k can't read: Disown keeps a line for every object a record
// names, so a test of a restricted process would count them unknown. A
// record whose Secrets k can read, or that are gone for it, stays.
func stripUnreadableRecords(t *testing.T, k kubernetes.Interface) {
	t.Helper()
	ctx := context.Background()
	widgets, err := env.Dynamic.Resource(cutest.WidgetGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range widgets.Items {
		w := &widgets.Items[i]
		for _, id := range cu.ReadRecord(w).IDs {
			if id.Kind != "Secret" || id.Group != "" {
				continue
			}
			if _, err := k.CoreV1().Secrets(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{}); !apierrors.IsForbidden(err) {
				continue
			}
			if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(w.GetNamespace()).Patch(ctx, w.GetName(), types.MergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, cu.AnnotationRefs)), metav1.PatchOptions{}); err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			break
		}
	}
}

// editIndex edits the index in idx by hand, as an operator would, retrying
// on conflict.
func editIndex(t *testing.T, idx string, edit func(cm *corev1.ConfigMap)) {
	t.Helper()
	cutest.Eventually(t, 30*time.Second, func() error {
		cms := env.Kube.CoreV1().ConfigMaps(idx)
		cm, err := cms.Get(context.Background(), widgetIndex, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		edit(cm)
		_, err = cms.Update(context.Background(), cm, metav1.UpdateOptions{})
		return err
	})
}

// indexAnnotations reads the annotations of the index in idx.
func indexAnnotations(t *testing.T, idx string) map[string]string {
	t.Helper()
	cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(context.Background(), widgetIndex, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cm.Annotations
}

// A vouched object that can be read and carries a finalizer of the kind is
// counted and stripped as any other: the vouch applies only while it can't
// be read, and the wrong vouch is logged (§7.4).
func TestDisownCountsAVouchedObjectThatCarries(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-vouch-wrong")
	createSecret(t, ns, "x")
	acquireAs(t, ns, "x")
	logs := &warnLog{}
	perNameRig(t, idx, func(o *cu.Options) {
		disownOptions(o)
		o.Logger = logs
	}, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			// The release loop can't take x: its batch's list fails.
			if c.Op == "list" && c.GK == widgetGK && c.Namespace == ns {
				return errors.New("the release loop can't list the holders")
			}
			return nil
		}, nil)
	})
	editIndex(t, idx, func(cm *corev1.ConfigMap) {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[cu.AnnotationVouched] = ns + "/Secret/x"
		cm.Data["entries"] += secretLine(ns, "x") + "\n"
	})
	counts := watchHeldCount(t)
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if seen := counts(); !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" && v != "unknown" }) {
		t.Errorf("held-count before settling: %v; want the vouched, held Secret counted", seen)
	}
	stripped(t, ns, "x")
	if len(logs.matching("the vouch is wrong")) == 0 {
		t.Error("the wrong vouch wasn't logged")
	}
}

// Disown empties the index itself (§7.4): a hold that a record names, and
// the index lists, is stripped with the record, and its line pruned by
// Disown's emptying. Nothing else can prune it here: the holder cache is
// held at the version whose record names the Secret, so the release batch
// that follows the strip keeps the line (§10.6.2, a line a record names).
func TestDisownEmptiesTheIndexItself(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-disown-empties")
	createSecret(t, ns, "x")
	createWidget(t, ns, "w", "x")
	r := perNameRig(t, idx, disownOptions, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "x", "w")
	lined(t, idx, secretLine(ns, "x"), true)
	r.c.stop() // no more passes: the holder isn't held again after Disown
	r.tr.PauseHolderWatch()
	t.Cleanup(r.tr.ResumeHolderWatch)

	counts := watchHeldCount(t)
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if seen := counts(); !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" && v != "unknown" }) {
		t.Errorf("held-count before settling: %v; want the hold counted", seen)
	}
	stripped(t, ns, "x")
	// The emptying follows the write of the settled flag.
	lined(t, idx, secretLine(ns, "x"), false)
	if lines, ok := indexLines(t, idx); !ok || len(lines) != 0 {
		t.Errorf("the index after Disown: %v, %v; want it there, and empty", lines, ok)
	}
}
