package credentialuse_test

// Disown and DisownPrefix against envtest (plan, PR 6; §7.4, §10.6.8).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

var crdGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

// widgetCRD is the Widget CRD's name.
var widgetCRD = cutest.WidgetGVR.Resource + "." + cutest.WidgetGVR.Group

const (
	disownInterval = time.Second
	disownPasses   = 3
)

// disownOptions shorten Disown's passes for the tests: 3 × 1s, over twice
// a TimeoutBound of 1s (§7.4).
func disownOptions(o *cu.Options) {
	o.DisownInterval, o.DisownPasses, o.TimeoutBound = disownInterval, disownPasses, time.Second
}

// setWidgetCRDAnnotations merge-patches the Widget CRD's annotations; nil
// removes one.
func setWidgetCRDAnnotations(t *testing.T, ann map[string]any) {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": ann}})
	if _, err := env.Dynamic.Resource(crdGVR).Patch(context.Background(), widgetCRD, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

func widgetCRDAnnotations(t *testing.T) map[string]string {
	t.Helper()
	crd, err := env.Dynamic.Resource(crdGVR).Get(context.Background(), widgetCRD, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return crd.GetAnnotations()
}

// disowning sets the switch on the Widget CRD, and removes it, with the
// count, when t ends, after the rigs created later have stopped.
func disowning(t *testing.T) func(value string) {
	t.Helper()
	clear := func() {
		setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationDisown: nil, cu.AnnotationHeldCount: nil, cu.AnnotationDisownSettled: nil})
	}
	clear()
	t.Cleanup(clear)
	return func(value string) { setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationDisown: value}) }
}

func settledAtZero(t *testing.T) func() error {
	return func() error {
		a := widgetCRDAnnotations(t)
		if a[cu.AnnotationHeldCount] != "0" || a[cu.AnnotationDisownSettled] != "true" {
			return fmt.Errorf("held-count %q, disown-settled %q: %w", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled], cutest.ErrNotYet)
		}
		return nil
	}
}

// stripped checks that Secret ns/name carries nothing of the Widget kind:
// no finalizer or epoch of it, no marker, and no held label.
func stripped(t *testing.T, ns, name string) {
	t.Helper()
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range s.Finalizers {
		if cu.IsCredentialUseFinalizer(f) {
			t.Errorf("%s/%s keeps %s", ns, name, f)
		}
	}
	for k := range s.Annotations {
		if cu.IsCredentialUseFinalizer(k) {
			t.Errorf("%s/%s keeps the epoch %s", ns, name, k)
		}
	}
	if _, ok := s.Labels[cu.MarkerLabel(widgetGK)]; ok {
		t.Errorf("%s/%s keeps the marker label", ns, name)
	}
	if _, ok := s.Labels[cu.LabelHeld]; ok {
		t.Errorf("%s/%s keeps the held label", ns, name)
	}
}

func unrecorded(t *testing.T, ns, name string) {
	t.Helper()
	w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cu.ReadRecord(w).Present {
		t.Errorf("%s/%s keeps its record %v", ns, name, w.GetAnnotations()[cu.AnnotationRefs])
	}
}

// Disown through the CRD drains two processes' holds to a live count of 0,
// including a namespace outside both processes' caches; the holders still
// connect, and removing the switch holds them again and clears the count.
func TestDisownDrainsTwoProcesses(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	cached, outside := env.Namespace(t, "disown-cached"), env.Namespace(t, "disown-outside")
	for _, ns := range []string{cached, outside} {
		createSecret(t, ns, "s")
		createWidget(t, ns, "w", "s")
	}
	so := cu.SharedOptions{Namespaces: []string{cached}}
	rigs := []*writeRig{newWriteRig(t, env.Config, so, disownOptions, nil), newWriteRig(t, env.Config, so, disownOptions, nil)}
	for _, r := range rigs {
		r.c.enqueue(widgetNN(cached, "w"))
		r.c.enqueue(widgetNN(outside, "w"))
	}
	for _, ns := range []string{cached, outside} {
		cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))
	}

	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	for _, ns := range []string{cached, outside} {
		stripped(t, ns, "s")
		unrecorded(t, ns, "w")
	}
	// A pass now holds nothing, and the holders connect, as today.
	for _, r := range rigs {
		before := r.c.passes.Load()
		r.c.enqueue(widgetNN(cached, "w"))
		r.c.enqueue(widgetNN(outside, "w"))
		cutest.Eventually(t, time.Minute, func() error {
			if r.c.passes.Load() < before+2 {
				return cutest.ErrNotYet
			}
			return nil
		})
	}
	for _, ns := range []string{cached, outside} {
		stripped(t, ns, "s")
		if !slices.Contains(rigs[0].c.d.Events(), "connect "+ns+"/w") {
			t.Errorf("%s/w never connected: %v", ns, rigs[0].c.d.Events())
		}
	}

	// Off again: the switch's change enqueues the holders, which hold again,
	// and the count leaves the CRD.
	setWidgetCRDAnnotations(t, map[string]any{cu.AnnotationDisown: nil})
	for _, ns := range []string{cached, outside} {
		cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))
	}
	cutest.Eventually(t, time.Minute, func() error {
		a := widgetCRDAnnotations(t)
		if _, ok := a[cu.AnnotationHeldCount]; ok {
			return fmt.Errorf("held-count %q: %w", a[cu.AnnotationHeldCount], cutest.ErrNotYet)
		}
		if _, ok := a[cu.AnnotationDisownSettled]; ok {
			return fmt.Errorf("disown-settled %q: %w", a[cu.AnnotationDisownSettled], cutest.ErrNotYet)
		}
		return nil
	})
}

// Settling takes DisownPasses consecutive zero passes, and a finalizer put
// back after it, as a late A2 would, resets it (§7.4, L4). The Secret is
// outside the process's caches, so no informer event brings the release
// loop to it: only Disown's live count finds the hold put back.
func TestDisownSettlesAfterConsecutiveZeroPasses(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	cached, ns := env.Namespace(t, "disown-settle-cached"), env.Namespace(t, "disown-settle")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	r := newWriteRig(t, env.Config, cu.SharedOptions{Namespaces: []string{cached}}, disownOptions, nil)
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))

	set("all")
	var firstZero time.Time
	cutest.Eventually(t, time.Minute, func() error {
		a := widgetCRDAnnotations(t)
		if a[cu.AnnotationHeldCount] == "0" && firstZero.IsZero() {
			firstZero = time.Now()
		}
		if a[cu.AnnotationDisownSettled] == "true" && firstZero.IsZero() {
			t.Fatal("settled before any zero count was seen")
		}
		return settledAtZero(t)()
	})
	// The first zero pass, then DisownPasses-1 more, an interval apart; the
	// poll reads every 100ms.
	if got, min := time.Since(firstZero), time.Duration(disownPasses-1)*disownInterval-150*time.Millisecond; got < min {
		t.Errorf("settled %s after the first zero count, want at least %s", got, min)
	}

	// A hold put back after settling: the next pass counts it, the flag
	// goes, the pass strips it, and settling starts over.
	f, _ := cu.FinalizerName(widgetGK, ns)
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{
		"finalizers":  []string{f},
		"annotations": map[string]string{f: "late"},
		"labels":      map[string]string{cu.MarkerLabel(widgetGK): "true", cu.LabelHeld: "true"},
	}})
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(context.Background(), "s", types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, func() error {
		if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "1" || a[cu.AnnotationDisownSettled] != "" {
			return fmt.Errorf("held-count %q, settled %q: %w", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled], cutest.ErrNotYet)
		}
		return nil
	})
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	stripped(t, ns, "s")
}

// Three processes with no leader election all run Disown: they agree on the
// count, through guarded writes, and settle.
func TestDisownWithThreeProcesses(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	var nss []string
	for i := range 3 {
		ns := env.Namespace(t, fmt.Sprintf("disown-three-%d", i))
		createSecret(t, ns, "s")
		createWidget(t, ns, "w", "s")
		nss = append(nss, ns)
	}
	var rigs []*writeRig
	for range 3 {
		rigs = append(rigs, newWriteRig(t, env.Config, cu.SharedOptions{}, disownOptions, nil))
	}
	for i, ns := range nss {
		rigs[i].c.enqueue(widgetNN(ns, "w"))
		cutest.Eventually(t, time.Minute, heldFor(ns, "s", ns))
	}
	set("all")
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	for _, ns := range nss {
		stripped(t, ns, "s")
		unrecorded(t, ns, "w")
	}
}

// §10.6.8, interleaving 3: a pass's A1 lands before the switch, and its A2
// after the Disown loop has stripped the record. The loop's next pass counts
// the finalizer the A2 put back, so the settling resets, and strips it. The
// Secret is outside the process's caches, as in the test above.
func TestALateA2AfterTheDisownStrip(t *testing.T) {
	cutest.Require(t, env)
	set := disowning(t)
	cached, ns := env.Namespace(t, "disown-late-cached"), env.Namespace(t, "disown-late")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")

	var armed, fired atomic.Bool
	reached, release, landed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	theA2 := func(c cu.Call) bool {
		return c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.StrategicMergePatchType
	}
	r := newWriteRig(t, env.Config, cu.SharedOptions{Namespaces: []string{cached}}, disownOptions, func(s *cu.Shared) {
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
	set("all")
	// The loop strips the record the pass's A1 wrote.
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

	// Watch the count from the A2 on.
	var mu sync.Mutex
	var seen []string
	stop := make(chan struct{})
	done := make(chan struct{})
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
	close(stop)
	<-done
	stripped(t, ns, "s")
	mu.Lock()
	defer mu.Unlock()
	if !slices.ContainsFunc(seen, func(v string) bool { return v != "0" && v != "" }) {
		t.Errorf("held-count from the late A2 on: %v; want a non-zero count before settling", seen)
	}
}

// gadgetGK is a holder kind whose CRD the test creates and deletes.
var gadgetGK = schema.GroupKind{Group: cutest.Group, Kind: "Gadget"}

// DisownPrefix refuses while the holder CRD exists, even while it is being
// deleted; once it is gone, a dry run reports and changes nothing, and a run
// strips every finalizer of the prefix, the epochs and the labels (§7.4).
func TestDisownPrefix(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	const hold = "credentialuse.test.krateo.io/hold"
	crd := cutest.CRD("Gadget", "gadgets")
	crd.Finalizers = []string{hold}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(crd)
	if err != nil {
		t.Fatal(err)
	}
	crds := env.Dynamic.Resource(crdGVR)
	if _, err := crds.Create(ctx, &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	unhold := func() {
		_, _ = crds.Patch(ctx, crd.Name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	}
	t.Cleanup(func() { unhold(); _ = crds.Delete(ctx, crd.Name, metav1.DeleteOptions{}) })

	ns := env.Namespace(t, "disown-prefix")
	fa, _ := cu.FinalizerName(gadgetGK, ns)
	fb, _ := cu.FinalizerName(gadgetGK, "elsewhere")
	marker := cu.MarkerLabel(gadgetGK)
	put := func(name string, finalizers []string, labels map[string]string) {
		t.Helper()
		createSecret(t, ns, name)
		ann := map[string]string{}
		for _, f := range finalizers {
			ann[f] = "epoch"
		}
		data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers, "annotations": ann, "labels": labels}})
		if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	both := map[string]string{marker: "true", cu.LabelHeld: "true"}
	put("one", []string{fa}, both)
	put("two", []string{fa, fb}, both)
	put("labels", nil, both)
	put("none", nil, nil)
	o := cu.DisownPrefixOptions{Holder: gadgetGK, Protected: []schema.GroupVersionKind{{Version: "v1", Kind: "Secret"}}}
	clients := clientsFor(t, env.Config)

	if _, err := cu.DisownPrefix(ctx, clients, o); !errors.Is(err, cu.ErrHolderCRDExists) {
		t.Fatalf("DisownPrefix with the CRD in place = %v, want ErrHolderCRDExists", err)
	}
	if err := crds.Delete(ctx, crd.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cu.DisownPrefix(ctx, clients, o); !errors.Is(err, cu.ErrHolderCRDExists) {
		t.Fatalf("DisownPrefix with the CRD being deleted = %v, want ErrHolderCRDExists", err)
	}
	unhold()
	cutest.Eventually(t, time.Minute, func() error {
		if _, err := crds.Get(ctx, crd.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the CRD: %v: %w", err, cutest.ErrNotYet)
		}
		return nil
	})

	id := func(name string) cu.ID { return cu.SecretRef(ns, name).ID() }
	ids := func(got []cu.ID) []cu.ID { return cu.SortedIDs(slices.Clone(got)) }
	want := cu.SortedIDs([]cu.ID{id("one"), id("two"), id("labels")})
	dry := o
	dry.DryRun = true
	rep, err := cu.DisownPrefix(ctx, clients, dry)
	if err != nil || !slices.Equal(ids(rep.Objects), want) || rep.Finalizers != 3 || len(rep.Stripped) != 0 {
		t.Fatalf("dry run = %+v, %v; want %v with 3 finalizers, none stripped", rep, err, want)
	}
	if s, _ := env.Kube.CoreV1().Secrets(ns).Get(ctx, "two", metav1.GetOptions{}); len(s.Finalizers) != 2 {
		t.Fatalf("the dry run changed two: %v", s.Finalizers)
	}

	rep, err = cu.DisownPrefix(ctx, clients, o)
	if err != nil || !slices.Equal(ids(rep.Stripped), want) {
		t.Fatalf("DisownPrefix = %+v, %v; want %v stripped", rep, err, want)
	}
	for _, name := range []string{"one", "two", "labels", "none"} {
		s, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, marked := s.Labels[marker]
		_, held := s.Labels[cu.LabelHeld]
		if len(s.Finalizers) != 0 || marked || held || s.Annotations[fa] != "" || s.Annotations[fb] != "" {
			t.Errorf("%s: finalizers %v, labels %v, annotations %v", name, s.Finalizers, s.Labels, s.Annotations)
		}
	}

	// By name, for resourceNames RBAC: no list; a name that is gone is
	// skipped.
	put("named", []string{fa}, nil)
	rep, err = cu.DisownPrefix(ctx, clients, cu.DisownPrefixOptions{Holder: gadgetGK, Names: []cu.Ref{cu.SecretRef(ns, "named"), cu.SecretRef(ns, "gone")}})
	if err != nil || !slices.Equal(rep.Stripped, []cu.ID{id("named")}) {
		t.Fatalf("DisownPrefix by name = %+v, %v", rep, err)
	}
	if s, _ := env.Kube.CoreV1().Secrets(ns).Get(ctx, "named", metav1.GetOptions{}); len(s.Finalizers) != 0 {
		t.Errorf("named keeps %v", s.Finalizers)
	}
}
