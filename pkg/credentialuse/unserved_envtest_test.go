package credentialuse_test

// A 404 for a resource the API server doesn't serve (§6.2): what the API
// server and client-go answer, and what a tracker does when a protected kind
// stops being served while it runs.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// Multis are served at v2 and v1, and stored at v1; the mapper prefers v2,
// which a test stops serving.
var (
	multiGVK = schema.GroupVersionKind{Group: cutest.Group, Version: "v2", Kind: "Multi"}
	multiV2  = schema.GroupVersionResource{Group: cutest.Group, Version: "v2", Resource: "multis"}
	multiV1  = schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "multis"}
)

// multiCRD creates the Multi CRD, and removes it when t ends. serve sets
// whether v2 is served, and waits until the API server does as it says.
func multiCRD(t *testing.T) (serve func(bool)) {
	t.Helper()
	ctx := context.Background()
	crd := cutest.CRD("Multi", "multis")
	v2 := *crd.Spec.Versions[0].DeepCopy()
	v2.Name, v2.Storage = "v2", false
	crd.Spec.Versions = append(crd.Spec.Versions, v2)
	cutest.Eventually(t, 30*time.Second, func() error { return env.Client.Create(ctx, crd.DeepCopy()) })
	t.Cleanup(func() {
		// A test that failed may leave a finalizer, which would keep the CRD.
		if l, err := env.Dynamic.Resource(multiV1).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range l.Items {
				_, _ = env.Dynamic.Resource(multiV1).Namespace(l.Items[i].GetNamespace()).Patch(ctx, l.Items[i].GetName(),
					types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
			}
		}
		_ = env.Client.Delete(ctx, crd)
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, crd.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("the Multi CRD is still there (%v): %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	})
	served := func(want bool) {
		t.Helper()
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := env.Metadata.Resource(multiV2).List(ctx, metav1.ListOptions{}); (err == nil) != want {
				return fmt.Errorf("listing Multis at v2: %v: %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	}
	served(true)
	return func(on bool) {
		t.Helper()
		p := fmt.Sprintf(`[{"op":"replace","path":"/spec/versions/1/served","value":%v}]`, on)
		if _, err := env.Dynamic.Resource(crdGVR).Patch(ctx, crd.Name, types.JSONPatchType, []byte(p), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		served(on)
	}
}

func createMulti(t *testing.T, ns, name string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(multiV1.GroupVersion().WithKind("Multi"))
	u.SetNamespace(ns)
	u.SetName(name)
	if _, err := env.Dynamic.Resource(multiV1).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// P19: a version the CRD no longer serves, though the object exists, and a
// CRD that is gone, are a 404 with client-go's UnexpectedServerResponse cause,
// for every call the core makes; a missing object's 404 has none.
func TestPremiseUnservedResource(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "unserved-premise")
	serve := multiCRD(t)
	createMulti(t, ns, "m")
	m, d := env.Metadata.Resource(multiV2).Namespace(ns), env.Dynamic.Resource(multiV2).Namespace(ns)
	calls := map[string]func(name string) error{
		"get": func(n string) error { _, err := m.Get(ctx, n, metav1.GetOptions{}); return err },
		"merge patch": func(n string) error {
			_, err := m.Patch(ctx, n, types.MergePatchType, []byte(`{"metadata":{"labels":{"a":"b"}}}`), metav1.PatchOptions{})
			return err
		},
		"JSON patch": func(n string) error {
			_, err := m.Patch(ctx, n, types.JSONPatchType, []byte(`[{"op":"test","path":"/metadata/name","value":"x"}]`), metav1.PatchOptions{})
			return err
		},
		"get, whole objects":  func(n string) error { _, err := d.Get(ctx, n, metav1.GetOptions{}); return err },
		"list":                func(string) error { _, err := m.List(ctx, metav1.ListOptions{}); return err },
		"list, whole objects": func(string) error { _, err := d.List(ctx, metav1.ListOptions{}); return err },
		"watch": func(string) error {
			w, err := m.Watch(ctx, metav1.ListOptions{})
			if err == nil {
				w.Stop()
			}
			return err
		},
	}
	check := func(what string) {
		t.Helper()
		for name, call := range calls {
			err := call("m")
			if err == nil {
				t.Errorf("%s: %s succeeded", what, name)
				continue
			}
			st := statusOf(t, err)
			if st.Code != http.StatusNotFound || st.Details == nil || !slices.ContainsFunc(st.Details.Causes, func(c metav1.StatusCause) bool {
				return c.Type == metav1.CauseTypeUnexpectedServerResponse
			}) {
				t.Errorf("%s: %s: %+v, want a 404 with the UnexpectedServerResponse cause", what, name, st)
			}
			if c, j := cu.Classify(err), cu.ClassifyJSONPatch(err); c != cu.ClassOther || j != cu.ClassOther {
				t.Errorf("%s: %s: Classify %v, ClassifyJSONPatch %v, want Other", what, name, c, j)
			}
		}
	}
	for _, name := range []string{"get", "merge patch", "JSON patch", "get, whole objects"} {
		err := calls[name]("missing")
		if c := cu.Classify(err); c != cu.ClassNotFound {
			t.Errorf("%s of a missing object: Classify(%v) = %v, want NotFound", name, err, c)
		}
	}
	// Inside a group-version the API server serves: a Status that names
	// nothing, without the cause.
	for _, gvr := range []schema.GroupVersionResource{{Version: "v1", Resource: "gonekinds"}, {Group: "apps", Version: "v1", Resource: "gonekinds"}} {
		_, gerr := env.Metadata.Resource(gvr).Namespace(ns).Get(ctx, "x", metav1.GetOptions{})
		_, lerr := env.Metadata.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		_, perr := env.Metadata.Resource(gvr).Namespace(ns).Patch(ctx, "x", types.MergePatchType, []byte(`{}`), metav1.PatchOptions{})
		for what, err := range map[string]error{"get": gerr, "list": lerr, "patch": perr} {
			st := statusOf(t, err)
			if st.Code != http.StatusNotFound || st.Details == nil || st.Details.Name != "" || len(st.Details.Causes) != 0 {
				t.Errorf("%s %s: %+v, want a 404 that names nothing, without a cause", what, gvr, st)
			}
			if c, j := cu.Classify(err), cu.ClassifyJSONPatch(err); c != cu.ClassOther || j != cu.ClassOther {
				t.Errorf("%s %s: Classify %v, ClassifyJSONPatch %v, want Other", what, gvr, c, j)
			}
		}
	}
	serve(false)
	check("v2 no longer served")
	if err := env.Dynamic.Resource(crdGVR).Delete(ctx, "multis."+cutest.Group, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if _, err := env.Metadata.Resource(multiV1).List(ctx, metav1.ListOptions{}); err == nil {
			return fmt.Errorf("Multis are still served: %w", cutest.ErrNotYet)
		}
		return nil
	})
	m, d = env.Metadata.Resource(multiV1).Namespace(ns), env.Dynamic.Resource(multiV1).Namespace(ns)
	check("the CRD gone")
}

// multiRig is a PerName rig of Widgets whose spec.secret names a Multi.
func multiRig(t *testing.T, idx string, logs *warnLog) *writeRig {
	t.Helper()
	return perNameRig(t, idx, func(o *cu.Options) {
		disownOptions(o)
		if logs != nil {
			o.Logger = logs
		}
		o.Protected = []cu.ProtectedKind{{GVK: multiGVK, Watch: cu.PerName}}
		o.Refs = func(obj metav1.Object) ([]cu.Ref, error) {
			name, _, _ := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "spec", "secret")
			if name == "" {
				return nil, nil
			}
			return []cu.Ref{{GroupKind: multiGVK.GroupKind(), Namespace: obj.GetNamespace(), Name: name}}, nil
		}
	}, nil)
}

// A protected kind that stops being served while the process runs (PerName):
// what it holds is unknown, not gone. A new holder doesn't connect; a hold
// whose holder goes keeps its line; and Disown's count is unknown, never zero,
// while an object may carry the finalizer. Served again, everything goes on.
func TestAProtectedKindThatStopsBeingServed(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "unserved")
	serve := multiCRD(t)
	for _, n := range []string{"m", "m2"} {
		createMulti(t, ns, n)
		createSecret(t, ns, n) // what the driver's Connect reads
	}
	r := multiRig(t, idx, nil)
	f, _ := cu.FinalizerName(widgetGK, ns)
	carries := func(name string) bool {
		u, err := env.Dynamic.Resource(multiV1).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		return err == nil && slices.Contains(u.GetFinalizers(), f)
	}
	line := cu.ID{Group: cutest.Group, Kind: "Multi", Namespace: ns, Name: "m"}.String()
	createWidget(t, ns, "w", "m")
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !carries("m") || !slices.Contains(w.GetFinalizers(), ownerFinalizer) {
			return fmt.Errorf("w isn't established, or m isn't held: %w", cutest.ErrNotYet)
		}
		return nil
	})
	lined(t, idx, line, true)

	serve(false)
	createWidget(t, ns, "w2", "m2")
	r.c.enqueue(widgetNN(ns, "w2"))
	cutest.Eventually(t, 30*time.Second, func() error {
		if r.c.passes.Load() == 0 || len(r.c.lastResult(widgetNN(ns, "w2")).Acquired.Unheld) == 0 && r.c.lastResult(widgetNN(ns, "w2")).Acquired.Err == nil {
			return fmt.Errorf("w2 has had no pass: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if res := r.c.lastResult(widgetNN(ns, "w2")); !res.Acquired.MustNotConnect || res.Connected {
		t.Errorf("a new holder of an unserved kind: MustNotConnect %v, connected %v, unheld %v; want it not to connect", res.Acquired.MustNotConnect, res.Connected, res.Acquired.Unheld)
	}
	deleteWidget(t, ns, "w")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, 30*time.Second, "w", widgetGet(ns, "w"))
	counts := watchHeldCount(t)
	set("all")
	time.Sleep(6 * time.Second) // the batch's retries, and six passes of Disown
	if lines, _ := indexLines(t, idx); !slices.Contains(lines, line) {
		t.Errorf("the index is %q: m's line went, though m carries the finalizer", lines)
	}
	if !carries("m") {
		t.Fatal("m lost the finalizer while it couldn't be patched")
	}
	if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" || a[cu.AnnotationDisownSettled] != "" {
		t.Errorf("held-count %q, disown-settled %q; want the count unknown (it was %v)", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled], counts())
	}
	eventSeen(t, r, cu.EventDisownUnreadable, "isn't served")

	serve(true)
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if carries("m") {
		t.Error("Disown settled at zero while m carries the finalizer")
	}
	lined(t, idx, line, false)
}

// The way out when the kind is gone for good (§6.2): its objects are gone with
// it, so nothing can be read. The log and the event say so, and an operator's
// vouch settles Disown without a read.
func TestAVouchGetsDisownOutOfAnUnservedKind(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "unserved-vouch")
	serve := multiCRD(t)
	createMulti(t, ns, "m")
	createSecret(t, ns, "m")
	logs := &warnLog{}
	r := multiRig(t, idx, logs)
	line := cu.ID{Group: cutest.Group, Kind: "Multi", Namespace: ns, Name: "m"}.String()
	createWidget(t, ns, "w", "m")
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(w.GetFinalizers(), ownerFinalizer) {
			return fmt.Errorf("w isn't established: %w", cutest.ErrNotYet)
		}
		return nil
	})
	serve(false)
	multis := env.Dynamic.Resource(multiV1).Namespace(ns)
	if _, err := multis.Patch(ctx, "m", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := multis.Delete(ctx, "m", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteWidget(t, ns, "w")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, 30*time.Second, "w", widgetGet(ns, "w"))
	set("all")
	eventSeen(t, r, cu.EventDisownUnreadable, "isn't served")
	cutest.Eventually(t, 30*time.Second, func() error {
		a := widgetCRDAnnotations(t)
		if a[cu.AnnotationHeldCount] != "unknown" || len(logs.matching("a release batch failed")) == 0 || len(logs.matching("Disown can't read objects")) == 0 {
			return fmt.Errorf("held-count %q, warnings %v: %w", a[cu.AnnotationHeldCount], logs.matching(""), cutest.ErrNotYet)
		}
		return nil
	})
	if lines, _ := indexLines(t, idx); !slices.Contains(lines, line) {
		t.Fatalf("the index is %q, without the line the vouch is for", lines)
	}
	editIndex(t, idx, func(cm *corev1.ConfigMap) {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[cu.AnnotationVouched] = ns + "/Multi." + cutest.Group + "/m"
	})
	cutest.Eventually(t, time.Minute, settledAtZero(t))
}

// A Secret, or a custom resource, named without a namespace can't exist,
// whatever asks for it: the ref is Missing, and its holder, cluster-scoped
// here, establishes; the line its A2 added goes with the holder, in a release
// batch that doesn't fail; and Disown settles.
func TestARefWithoutItsNamespaceIsMissing(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx := indexNamespace(t)
	// A kind of its own, so that the test can run again in one process: its CRD
	// doesn't go while the last holder has its owner finalizer.
	kind := fmt.Sprintf("ClusterWidgetN%d", crdSeq.Add(1))
	crd := cutest.CRD(kind, strings.ToLower(kind)+"s")
	crd.Spec.Scope = apiextensionsv1.ClusterScoped
	gvr := schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: crd.Spec.Names.Plural}
	if err := env.Client.Create(ctx, crd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), crd) })
	holders := env.Dynamic.Resource(gvr)
	cutest.Eventually(t, 30*time.Second, func() error { _, err := holders.List(ctx, metav1.ListOptions{}); return err })
	gvk := gvr.GroupVersion().WithKind(kind)
	c := newController(t)
	clients := clientsFor(t, env.Config)
	s := cu.NewShared(clients, cu.SharedOptions{IndexNamespace: idx})
	s.SetLandingMargin(0) // disownOptions' passes are short
	o := widgetOptions(c.enqueue)
	o.Holder, o.ProbeInterval = gvk, time.Second
	disownOptions(&o)
	perName(&o)
	o.Protected = append(o.Protected, cu.ProtectedKind{GVK: cutest.TargetGVK, Watch: cu.PerName})
	refs := []cu.Ref{cu.SecretRef("", "x"), {GroupKind: cutest.TargetGVK.GroupKind(), Name: "x"}}
	o.Refs = func(metav1.Object) ([]cu.Ref, error) { return refs, nil }
	logs := &warnLog{}
	o.Logger = logs
	tr, err := s.Track(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	running(t, s)
	c.start(&driver.Driver{Tracker: tr, Dynamic: env.Dynamic, Holder: gvr, OwnerFinalizer: ownerFinalizer})
	// lines is how many of the refs have a line.
	lines := func() int {
		cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(ctx, cu.IndexName(gvk.GroupKind()), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got := strings.Split(cm.Data["entries"], "\n")
		n := 0
		for _, r := range refs {
			if slices.Contains(got, r.ID().String()) {
				n++
			}
		}
		return n
	}
	established := func(name string) {
		t.Helper()
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetGroupVersionKind(gvk)
		u.SetName(name)
		if _, err := holders.Create(ctx, u, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		nn := types.NamespacedName{Name: name}
		c.enqueue(nn)
		cutest.Eventually(t, 30*time.Second, func() error {
			u, err := holders.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if !slices.Contains(u.GetFinalizers(), ownerFinalizer) {
				return fmt.Errorf("%s isn't established, its last pass %+v: %w", name, c.lastResult(nn).Acquired, cutest.ErrNotYet)
			}
			return nil
		})
		a := c.lastResult(nn).Acquired
		if a.MustNotConnect || a.Degraded || len(a.Unheld) != 2 || a.Unheld[0].Reason != cu.ReasonMissing || a.Unheld[1].Reason != cu.ReasonMissing {
			t.Errorf("%s: %+v; want both refs Missing, and nothing else", name, a)
		}
	}
	established("a")
	if n := lines(); n != 2 {
		t.Errorf("the index has %d of the Missing refs' lines, want 2", n)
	}
	if err := holders.Delete(ctx, "a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	c.enqueue(types.NamespacedName{Name: "a"})
	cutest.Eventually(t, 30*time.Second, func() error {
		if n := lines(); n != 0 {
			return fmt.Errorf("%d lines are still there: %w", n, cutest.ErrNotYet)
		}
		return nil
	})
	logs.mu.Lock()
	for _, w := range logs.warns {
		if strings.Contains(w, "release batch failed") {
			t.Errorf("logged %q", w)
		}
	}
	logs.mu.Unlock()
	established("b")
	annotate := func(v string) {
		t.Helper()
		if _, err := env.Dynamic.Resource(crdGVR).Patch(ctx, crd.Name, types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+cu.AnnotationDisown+`":"`+v+`"}}}`), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	annotate("all")
	cutest.Eventually(t, 30*time.Second, func() error {
		u, err := env.Dynamic.Resource(crdGVR).Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if a := u.GetAnnotations(); !cu.DisownSettled(a) {
			return fmt.Errorf("held-count %q, settled %q: %w", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled], cutest.ErrNotYet)
		}
		return nil
	})
}

// A namespaced holder's ref without a namespace is in another, the one named
// "", so the gate asks for its consent, which no Namespace can give: it is
// unreadable, and the holder proceeds, as before the rule on 404s.
func TestANamespacedHoldersRefWithoutItsNamespace(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "nameless")
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) {
		o.Refs = func(metav1.Object) ([]cu.Ref, error) { return []cu.Ref{cu.SecretRef("", "x")}, nil }
	}, nil)
	createWidget(t, ns, "w", "x")
	nn := widgetNN(ns, "w")
	r.c.enqueue(nn)
	cutest.Eventually(t, 30*time.Second, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(w.GetFinalizers(), ownerFinalizer) {
			return fmt.Errorf("w isn't established, its last pass %+v: %w", r.c.lastResult(nn).Acquired, cutest.ErrNotYet)
		}
		return nil
	})
	a := r.c.lastResult(nn).Acquired
	if a.MustNotConnect || a.Degraded || len(a.Unheld) != 1 || a.Unheld[0].Reason != cu.ReasonConsentUnreadable {
		t.Errorf("%+v; want its consent unreadable, and the holder to proceed", a)
	}
}
