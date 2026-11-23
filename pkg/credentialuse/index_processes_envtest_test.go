package credentialuse_test

// Several processes on one index, and Disown's edges with them (§6.8, §7.4,
// §10.6).

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// createIndex creates the rebuilt index in idx with lines.
func createIndex(t *testing.T, idx string, lines ...string) {
	t.Helper()
	data := ""
	for _, l := range lines {
		data += l + "\n"
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx, Annotations: map[string]string{cu.AnnotationRebuilt: "true"}},
		Data: map[string]string{"entries": data}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(context.Background(), cm, metav1.CreateOptions{}); err != nil {
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
	perNameRig(t, idx, nil, nil)
	lined(t, idx, line, false) // the new owner's startup recheck, or batch
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
