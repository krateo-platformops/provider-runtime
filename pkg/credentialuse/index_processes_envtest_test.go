package credentialuse_test

// Several processes on one index, and Disown's edges with them (§6.8, §7.4,
// §10.6).

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// createIndex creates the index in idx with lines, rebuilt for Secrets.
func createIndex(t *testing.T, idx string, lines ...string) {
	t.Helper()
	createIndexFor(t, idx, "Secret", lines...)
}

// createIndexFor is createIndex for kinds, as the marker lists them.
func createIndexFor(t *testing.T, idx, kinds string, lines ...string) {
	t.Helper()
	data := ""
	for _, l := range lines {
		data += l + "\n"
	}
	ctx, cms := context.Background(), env.Kube.CoreV1().ConfigMaps(idx)
	cm, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx},
		Data: map[string]string{"entries": data}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The marker of a completed rebuild (§10.6.6).
	cm.Annotations = map[string]string{cu.AnnotationRebuilt: string(cm.UID) + "/" + kinds}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// isIndexWrite reports whether c writes the index.
func isIndexWrite(c cu.Call) bool { return c.Op == "index-update" || c.Op == "index-create" }

// Two processes of one kind Disown over one index that names a Secret
// neither can read, and which is really held: neither ever takes its line
// out, though their passes align, so neither counts zero while the other has
// it out, and it never settles (§7.4).
func TestTwoProcessesDisownAnUnreadableHold(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-two-unreadable")
	const sa = "cu-two-unreadable"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch", "list", "watch"})
	cfg := env.AsServiceAccount("default", sa)
	stripUnreadableRecords(t, kubeAs(t, cfg))
	createSecret(t, ns, "nogrant")
	acquireAs(t, ns, "nogrant")
	line := secretLine(ns, "nogrant")
	createIndex(t, idx, line)
	var without atomic.Int32
	for range 2 {
		newWriteRig(t, cfg, cu.SharedOptions{IndexNamespace: idx}, func(o *cu.Options) {
			perName(o)
			disownOptions(o)
		}, func(s *cu.Shared) {
			s.HookCalls(func(_ context.Context, c cu.Call) error {
				if isIndexWrite(c) && !slices.Contains(c.Lines, line) {
					without.Add(1)
				}
				return nil
			}, nil)
		})
	}
	set("all")
	staysUnknown(t)
	staysUnknown(t)
	if n := without.Load(); n != 0 {
		t.Errorf("%d index writes took the line out", n)
	}
	lined(t, idx, line, true)
	if err := heldFor(ns, "nogrant", ns)(); err != nil {
		t.Error(err)
	}
}

// A record that names a Secret the process can't read, so can't strip, stays,
// with the Secret's line, until the Secret can be read or an operator vouches
// for it (§7.4).
func TestDisownKeepsTheRecordOfAnUnreadableSecret(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-unreadable-record")
	const sa = "cu-unreadable-record"
	grantPerName(t, ns, idx, sa, []string{"s"}, []string{"get", "patch", "list", "watch"})
	cfg := env.AsServiceAccount("default", sa)
	stripUnreadableRecords(t, kubeAs(t, cfg))
	createSecret(t, ns, "nogrant")
	createWidget(t, ns, "w", "nogrant")
	line := secretLine(ns, "nogrant")
	createIndex(t, idx)
	r := newWriteRig(t, cfg, cu.SharedOptions{IndexNamespace: idx}, func(o *cu.Options) {
		perName(o)
		disownOptions(o)
	}, nil)
	r.c.stop() // no pass holds the Secret
	rec, err := cu.EncodeRecord([]cu.ID{cu.SecretRef(ns, "nogrant").ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(ctx, "w", types.MergePatchType,
		[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, rec)), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	set("all")
	staysUnknown(t)
	lined(t, idx, line, true)
	w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !cu.ReadRecord(w).Present {
		t.Error("the record of a Secret that couldn't be read was stripped")
	}
	editIndex(t, idx, func(cm *corev1.ConfigMap) {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[cu.AnnotationVouched] = ns + "/Secret/nogrant"
	})
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	unrecorded(t, ns, "w")
	lined(t, idx, line, false)
}

// Two processes settle at different times: one can't read the index for a
// while, so its count is unknown, and neither settles; once it can, both do,
// the index is emptied, and nothing writes it again: the one that settles
// later, and the live checks of the other's removals, don't put a line back
// (§7.4).
func TestTwoProcessesSettleApart(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-two-apart")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	var blind atomic.Bool
	var writes atomic.Int32
	rig := func(second bool) *writeRig {
		return perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
			s.HookCalls(func(_ context.Context, c cu.Call) error {
				if second && blind.Load() && c.Op == "index-get" {
					return errors.New("the index can't be read")
				}
				if isIndexWrite(c) {
					writes.Add(1)
				}
				return nil
			}, nil)
		})
	}
	r := rig(false)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, line, true)
	rig(true)
	blind.Store(true)
	settled := watchCRDAnnotation(t, cu.AnnotationDisownSettled)
	set("all")
	time.Sleep((disownPasses + 3) * disownInterval)
	if seen := settled(); slices.ContainsFunc(seen, func(v string) bool { return v != "" }) {
		t.Errorf("settled while one process couldn't read the index: %v", seen)
	}
	lined(t, idx, line, true)
	blind.Store(false)
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	lined(t, idx, line, false)
	stripped(t, ns, "s")
	// Both runs are done by then; from there, nothing writes.
	time.Sleep((disownPasses + 2) * disownInterval)
	before := writes.Load()
	time.Sleep((disownPasses + 2) * disownInterval)
	if n := writes.Load() - before; n != 0 {
		t.Errorf("the index was written %d times with both processes settled", n)
	}
	lined(t, idx, line, false)
	if err := settledAtZero(t)(); err != nil {
		t.Error(err)
	}
}

// A switch over some kinds keeps the lines of those kinds only: a Secret
// released meanwhile has its line pruned, as without the switch, and
// disown-settled is the switch's value (§7.4).
func TestDisownOfAnotherKindPrunesThisOne(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-partial")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	r := perNameRig(t, idx, func(o *cu.Options) {
		disownOptions(o)
		o.Protected = append(o.Protected, cu.ProtectedKind{GVK: schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, Watch: cu.PerName})
	}, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, line, true)
	set("ConfigMap")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if v := widgetCRDAnnotations(t)[cu.AnnotationDisownSettled]; v != "ConfigMap" {
		t.Errorf("disown-settled %q, want ConfigMap", v)
	}
	lined(t, idx, line, true)
	if err := heldFor(ns, "s", ns)(); err != nil {
		t.Errorf("a Secret's hold under a switch over ConfigMaps: %v", err)
	}
	// Before it has settled again, which a change of the switch makes it do.
	set("ConfigMap,Other.x.io")
	deleteWidget(t, ns, "w")
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, line, false)
}

// With a full index, Disown can't line what a record names (§7.4, E2): it
// leaves the record, counts unknown, and says how to go on; a process with a
// higher cap then finishes.
func TestDisownWithAFullIndex(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-full-disown")
	createSecret(t, ns, "s")
	createSecret(t, ns, "x")
	createWidget(t, ns, "w", "s")
	createIndex(t, idx, secretLine(ns, "x"))
	rec, err := cu.EncodeRecord([]cu.ID{cu.SecretRef(ns, "s").ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(ctx, "w", types.MergePatchType,
		[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, rec)), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	recorded := func() bool {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		return err == nil && cu.ReadRecord(w).Present
	}
	// On before the process starts, so that its startup recheck keeps the
	// line of x, which is bare, and the index stays full.
	set("all")
	r := newWriteRig(t, env.Config, cu.SharedOptions{IndexNamespace: idx, IndexMaxLines: 1}, func(o *cu.Options) {
		perName(o)
		disownOptions(o)
	}, nil)
	r.c.stop()
	staysUnknown(t)
	eventSeen(t, r, cu.EventDisownIndexFull, "IndexMaxLines")
	if !recorded() {
		t.Error("the record was stripped, with nowhere else naming its Secret")
	}
	lined(t, idx, secretLine(ns, "x"), true)
	r.stop()

	r2 := perNameRig(t, idx, disownOptions, nil)
	r2.c.stop()
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if recorded() {
		t.Error("the record wasn't stripped once the index had room")
	}
	lined(t, idx, secretLine(ns, "s"), false)
	lined(t, idx, secretLine(ns, "x"), false)
}

// A process loses its Lease with an A2 in flight: its caller's context ends,
// and the A2 goes on. The new owner finds the holder gone and the Secret
// bare, and prunes its line; then the old A2 lands. Its call finishes: its
// check puts the line back, and through it the new owner sees the hold, and
// releases it (§6.2, §10.6.2).
func TestALostLeaseWithAnA2InFlight(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-lost-lease")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	var fired, back atomic.Bool
	reached, release := make(chan struct{}), make(chan struct{})
	theA2 := func(c cu.Call) bool {
		return c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.StrategicMergePatchType
	}
	old := perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			if theA2(c) && fired.CompareAndSwap(false, true) {
				close(reached)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}, func(c cu.Call, err error) {
			if fired.Load() && isIndexWrite(c) && err == nil && slices.Contains(c.Lines, line) {
				back.Store(true)
			}
		})
	})
	old.c.enqueue(widgetNN(ns, "w"))
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("the pass never reached its A2")
	}
	lined(t, idx, line, true)
	// Its write loops and its passes end, and the A2 doesn't.
	old.c.cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		old.stopS()
	}()
	deleteWidget(t, ns, "w")
	gone(t, time.Minute, "widget w", widgetGet(ns, "w"))
	perNameRig(t, idx, nil, nil) // with no quiet time
	lined(t, idx, line, false)   // the new owner's startup recheck, or batch
	close(release)
	<-stopped
	old.c.stop() // waits for the pass
	cutest.Eventually(t, time.Minute, func() error {
		if !back.Load() {
			return fmt.Errorf("the old process's check didn't put the line back: %w", cutest.ErrNotYet)
		}
		return nil
	})
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, line, false)
	stripped(t, ns, "s")
}

// The write loops' start keeps the prunes back, as long as an A2 of the
// process that ran them before may land (§10.6.2). One that does land
// meanwhile, its holder gone, is found by the line that was kept, and
// released; the lines go once the time is over.
func TestThePrunesWaitForALandingAfterAStart(t *testing.T) {
	cutest.Require(t, env)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-quiet")
	const quiet = 15 * time.Second
	createSecret(t, ns, "s")
	createIndex(t, idx, secretLine(ns, "s"))
	began := time.Now()
	perNameRig(t, idx, nil, func(s *cu.Shared) { s.SetPruneQuiet(quiet) })
	time.Sleep(quiet / 3) // the startup recheck has run, and found it bare
	lined(t, idx, secretLine(ns, "s"), true)
	acquireAs(t, ns, "s")
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
	if d := time.Since(began); d < quiet {
		t.Errorf("the line went after %s, want %s", d, quiet)
	}
}

// restartMetrics counts Disown's restarted runs.
type restartMetrics struct {
	cu.NopMetrics
	restarts atomic.Int32
}

func (m *restartMetrics) DisownRestarts(string) { m.restarts.Add(1) }

// Two writers of the CRD that never agree keep Disown from settling, and say
// so: the flag never appears, the count stays right, and one event names
// both, though each takes the field, and its entry in managedFields, from the
// other. Once they stop, Disown settles (§7.4).
func TestDisownUnderTwoWritersThatNeverAgree(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	ns := env.Namespace(t, "disown-writers")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	m := &restartMetrics{}
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) {
		disownOptions(o)
		o.Metrics = m
	}, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	write := func(manager string, value any) error {
		data := fmt.Sprintf(`{"metadata":{"labels":{"credentialuse.test.krateo.io/writer":%q}}}`, value)
		if value == nil {
			data = `{"metadata":{"labels":{"credentialuse.test.krateo.io/writer":null}}}`
		}
		_, err := env.Dynamic.Resource(crdGVR).Patch(ctx, widgetCRD, types.MergePatchType, []byte(data), metav1.PatchOptions{FieldManager: manager})
		return err
	}
	stop, done := make(chan struct{}), make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- write("writer-a", nil)
				return
			case <-time.After(200 * time.Millisecond):
			}
			// Each has the restart after the other's: which one a restart
			// sees doesn't depend on the clock.
			w := []string{"writer-a", "writer-b"}[m.restarts.Load()%2]
			if err := write(w, fmt.Sprintf("%s-%d", w, i)); err != nil {
				done <- err
				return
			}
		}
	}()
	// Also when the test fails: writers left running would keep every later
	// test's Disown from settling.
	stopWriters := sync.OnceValue(func() error {
		close(stop)
		return <-done
	})
	t.Cleanup(func() { _ = stopWriters() })
	counts, settled := watchHeldCount(t), watchCRDAnnotation(t, cu.AnnotationDisownSettled)
	set("all")
	eventSeen(t, r, cu.EventDisownRestarted, "writer-a", "writer-b")
	time.Sleep(2 * (disownPasses + 1) * disownInterval)
	if seen := settled(); slices.ContainsFunc(seen, func(v string) bool { return v != "" }) {
		t.Errorf("settled while the CRD kept changing: %v", seen)
	}
	stripped(t, ns, "s")
	if seen := counts(); len(seen) == 0 || seen[len(seen)-1] != "0" || slices.Contains(seen, "unknown") {
		t.Errorf("held-count while the CRD kept changing: %v; want it to end at 0, never unknown", seen)
	}
	r.mu.Lock()
	n := 0
	for _, e := range r.seen {
		if strings.Contains(e, cu.EventDisownRestarted) {
			n++
		}
	}
	r.mu.Unlock()
	if n != 1 {
		t.Errorf("reported %d times within its throttle, want once", n)
	}
	if err := stopWriters(); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, settledAtZero(t))
}

// A dropped line whose live check keeps failing may name a hold: the count
// is unknown while the check is pending, though nothing else names the
// Secret, and the index can be read (§7.4).
func TestDisownWaitsForAPendingCheck(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-pending")
	createSecret(t, ns, "x")
	acquireAs(t, ns, "x")
	line := secretLine(ns, "x")
	createIndex(t, idx, line)
	var failing atomic.Bool
	failing.Store(true)
	r := perNameRig(t, idx, disownOptions, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if failing.Load() && c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == ns {
				return errors.New("the API server keeps failing for it")
			}
			return nil
		}, nil)
	})
	set("all")
	staysUnknown(t)
	setIndexLines(t, idx, func(l []string) []string { return slices.DeleteFunc(l, func(x string) bool { return x == line }) })
	cutest.Eventually(t, time.Minute, func() error {
		if p, _ := r.tr.PendingIndexChecks(); len(p) == 0 {
			return fmt.Errorf("no check is pending: %w", cutest.ErrNotYet)
		}
		return nil
	})
	staysUnknown(t)
	lined(t, idx, line, false)
	failing.Store(false)
	cutest.Eventually(t, 2*time.Minute, settledAtZero(t))
	stripped(t, ns, "x")
}

// An A2 lands after its response was lost, its holder was gone, and another
// process had pruned the line of a Secret it found bare, which the sender's own
// batch leaves: once the landing margin is over, the sender checks the Secret
// again, and releases it (§10.6.2).
func TestALateA2IsReleased(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns := indexNamespace(t), env.Namespace(t, "idx-late")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	line := secretLine(ns, "s")
	var lost atomic.Bool
	r := perNameRig(t, idx, nil, func(s *cu.Shared) {
		s.SetLandingMargin(25 * time.Second)
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			// Every A2 the process sends: the holder never gets its finalizer.
			if c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.StrategicMergePatchType {
				lost.Store(true)
				return errors.New("the response was lost")
			}
			return nil
		}, nil)
	})
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, func() error {
		if !lost.Load() {
			return fmt.Errorf("no A2 was sent: %w", cutest.ErrNotYet)
		}
		return nil
	})
	r.c.stop()
	deleteWidget(t, ns, "w")
	gone(t, time.Minute, "widget w", widgetGet(ns, "w"))
	time.Sleep(2 * time.Second) // the holder's batch runs, and leaves the line
	if lines, _ := indexLines(t, idx); !slices.Contains(lines, line) {
		t.Errorf("lines %q: the sender's own prune took the line of an A2 that may land yet", lines)
	}
	setIndexLines(t, idx, func(l []string) []string { return slices.DeleteFunc(l, func(x string) bool { return x == line }) })
	lined(t, idx, line, false)
	time.Sleep(3 * time.Second) // the check of the line's drop finds it bare, and is done
	epoch, err := cu.NewEpoch()
	if err != nil {
		t.Fatal(err)
	}
	p, err := cu.AcquireStrategicPatch(widgetGK, ns, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, "s", p.Type, p.Data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, line, false)
	stripped(t, ns, "s")
}
