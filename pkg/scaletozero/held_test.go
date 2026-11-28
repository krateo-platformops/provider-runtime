package scaletozero

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// WithHeld and WithHeldIndex (docs/design-credential-use-finalizer.md §11.6,
// §10.6.5; the plan's PR 10). Each test has a holder kind of its own, so that
// its holds, its marker label and its Disown switch are its alone.

var (
	secretGVK    = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	shieldGVK    = schema.GroupVersionKind{Group: testGroup, Version: "v1", Kind: "Shield"}
	namespaceGVK = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	// otherHolder is another holder kind, whose finalizers some objects
	// carry beside the test holder's marker label: a hash collision's
	// leftover, as far as the test holder can tell.
	otherHolder = schema.GroupKind{Group: testGroup, Kind: "Other"}
)

// newHolderKind creates a holder kind of the test's own and waits until the
// admin client's mapper serves it.
func newHolderKind(t *testing.T) schema.GroupVersionKind {
	t.Helper()
	return newKind(t, "Keeper")
}

// setCRDAnnotations makes the holder kind's CRD carry exactly ann.
func setCRDAnnotations(t *testing.T, holder schema.GroupVersionKind, ann map[string]string) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := k8s.Get(context.Background(), client.ObjectKey{Name: crdName(holder)}, crd); err != nil {
			return err
		}
		crd.Annotations = maps.Clone(ann)
		return k8s.Update(context.Background(), crd)
	})
	if err != nil {
		t.Fatalf("annotating the CRD of %s: %v", holder.Kind, err)
	}
}

func marker(holder schema.GroupVersionKind) map[string]string {
	return map[string]string{credentialuse.MarkerLabel(holder.GroupKind()): "true"}
}

func finalizer(t *testing.T, holder schema.GroupKind, ns string) string {
	t.Helper()
	f, err := credentialuse.FinalizerName(holder, ns)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// newSecret creates a Secret with the given labels and finalizers.
func newSecret(t *testing.T, ns, name string, lbls map[string]string, finalizers ...string) *corev1.Secret {
	t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls, Finalizers: finalizers}}
	if err := k8s.Create(context.Background(), s); err != nil {
		t.Fatalf("creating secret %s/%s: %v", ns, name, err)
	}
	return s
}

// held creates a Secret that holder holds: its marker label and a finalizer
// of its kind.
func held(t *testing.T, holder schema.GroupVersionKind, ns, name string) *corev1.Secret {
	t.Helper()
	return newSecret(t, ns, name, marker(holder), finalizer(t, holder.GroupKind(), ns))
}

// collision creates a Secret that carries holder's marker label without its
// prefix, as a hash collision with another kind's marker leaves.
func collision(t *testing.T, holder schema.GroupVersionKind, ns, name string) *corev1.Secret {
	t.Helper()
	return newSecret(t, ns, name, marker(holder), finalizer(t, otherHolder, ns))
}

// release takes holder's marker label and finalizers off s, as a release
// does.
func release(t *testing.T, s *corev1.Secret) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &corev1.Secret{}
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(s), cur); err != nil {
			return err
		}
		cur.Labels, cur.Finalizers = nil, nil
		return k8s.Update(context.Background(), cur)
	})
	if err != nil {
		t.Fatalf("releasing secret %s/%s: %v", s.Namespace, s.Name, err)
	}
}

// newIndex creates an index ConfigMap with lines, without the rebuilt
// marker: it is read like any other (decided 2026-11-25).
func newIndex(t *testing.T, lines ...string) types.NamespacedName {
	t.Helper()
	ns := newNamespace(t)
	data := ""
	for _, l := range lines {
		data += l + "\n"
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "cu-index"}, Data: map[string]string{"entries": data}}
	if err := k8s.Create(context.Background(), cm); err != nil {
		t.Fatalf("creating the index: %v", err)
	}
	return types.NamespacedName{Namespace: ns, Name: "cu-index"}
}

func line(gvk schema.GroupVersionKind, ns, name string) string {
	return gvk.Group + "/" + gvk.Kind + "/" + ns + "/" + name
}

func withMapper() Option { return WithRESTMapper(k8s.RESTMapper()) }

// Exists with WithHeld lists the protected kind by the holder kind's marker
// label, one item per page, metadata-only and across namespaces, pages past
// the objects that carry the label without the prefix, and stops at the
// first that carries it. Without the option, holds don't count.
func TestExistsWithHeldStopsAtTheFirstHeldObject(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	// The collisions come first in the API server's order (namespace, then
	// name), then two holds.
	for i := range 3 {
		collision(t, holder, ns, fmt.Sprintf("a%d", i))
	}
	held(t, holder, ns, "b-held")
	held(t, holder, ns, "c-held")

	c, requests := recordingClient(t, "secrets")
	if ok, err := Exists(ctx, c, holder, labels.Everything()); err != nil || ok {
		t.Fatalf("Exists = (%v, %v), want (false, nil): holds count only with WithHeld", ok, err)
	}
	ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeld(secretGVK), WithRESTMapper(c.RESTMapper()))
	if err != nil || !ok {
		t.Fatalf("ExistsWithOptions(WithHeld) = (%v, %v), want (true, nil)", ok, err)
	}
	reqs := requests()
	// One page for each collision, and one for the first hold: none after.
	if len(reqs) != 4 {
		t.Errorf("%d LIST requests of secrets, want 4: three past the collisions, then the first hold", len(reqs))
	}
	key := credentialuse.MarkerLabel(holder.GroupKind())
	for i, r := range reqs {
		if strings.Contains(r.path, "/namespaces/") {
			t.Errorf("request %d listed one namespace (%s), want all namespaces", i, r.path)
		}
		if r.limit != "1" {
			t.Errorf("request %d had limit %q, want 1", i, r.limit)
		}
		if r.labelSelector != key {
			t.Errorf("request %d had labelSelector %q, want %q", i, r.labelSelector, key)
		}
		if !strings.Contains(r.accept, "as=PartialObjectMetadataList") {
			t.Errorf("request %d had Accept %q, want a metadata-only list", i, r.accept)
		}
		if (r.cont == "") != (i == 0) {
			t.Errorf("request %d had continue %q: want none on the first only", i, r.cont)
		}
	}
}

// Objects that carry the marker label without the prefix, a collision's, are
// paged past to the end: no hold.
func TestExistsWithHeldPagesPastACollision(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	for i := range 3 {
		collision(t, holder, ns, fmt.Sprintf("a%d", i))
	}
	// A finalizer of the kind without the marker label isn't selected: every
	// hold carries both (§10.1).
	newSecret(t, ns, "unmarked", nil, finalizer(t, holder.GroupKind(), ns))
	c, requests := recordingClient(t, "secrets")
	ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeld(secretGVK), withMapper())
	if err != nil || ok {
		t.Fatalf("ExistsWithOptions(WithHeld) = (%v, %v), want (false, nil)", ok, err)
	}
	if n := len(requests()); n < 3 {
		t.Errorf("%d LIST requests of secrets, want at least 3: one past each collision", n)
	}

	// The hold behind them is found.
	held(t, holder, ns, "b-held")
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), withMapper()); err != nil || !ok {
		t.Errorf("with a hold behind the collisions = (%v, %v), want (true, nil)", ok, err)
	}
}

// A hold on an object being deleted still counts: the object waits for the
// release.
func TestExistsWithHeldCountsATerminatingHold(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	s := held(t, holder, newNamespace(t), "dying")
	if err := k8s.Delete(ctx, s); err != nil {
		t.Fatal(err)
	}
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), withMapper()); err != nil || !ok {
		t.Errorf("ExistsWithOptions = (%v, %v), want (true, nil)", ok, err)
	}
}

// Every protected kind is listed: a hold of the second counts.
func TestExistsWithHeldListsEachProtectedKind(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	collision(t, holder, ns, "s")
	newInstance(t, shieldGVK, ns, "shield", marker(holder), finalizer(t, holder.GroupKind(), ns))
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), withMapper()); err != nil || ok {
		t.Errorf("WithHeld(Secret) = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK, shieldGVK), withMapper()); err != nil || !ok {
		t.Errorf("WithHeld(Secret, Shield) = (%v, %v), want (true, nil)", ok, err)
	}
	// Given in two options, the kinds add up, in either order: the hold is on
	// the first option's kind in one, and on the second's in the other.
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), WithHeld(shieldGVK), withMapper()); err != nil || !ok {
		t.Errorf("WithHeld(Secret), WithHeld(Shield) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(shieldGVK), WithHeld(secretGVK), withMapper()); err != nil || !ok {
		t.Errorf("WithHeld(Shield), WithHeld(Secret) = (%v, %v), want (true, nil)", ok, err)
	}
}

// switchCases are the Disown switch's states on the holder kind's CRD, and
// whether each counts as an instance: a set switch that hasn't settled, by
// credentialuse.DisownSettled's three checks (decided 2026-11-23), does.
var switchCases = []struct {
	name    string
	ann     map[string]string
	running bool
}{
	{"no switch", nil, false},
	{"the switch set", map[string]string{credentialuse.AnnotationDisown: "all"}, true},
	{"the switch set and counted, not settled", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationHeldCount: "0"}, true},
	{"held-count 1", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationDisownSettled: "all", credentialuse.AnnotationHeldCount: "1"}, true},
	{"held-count unknown", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationDisownSettled: "all", credentialuse.AnnotationHeldCount: "unknown"}, true},
	{"a flag left by another value", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationDisownSettled: "Secret", credentialuse.AnnotationHeldCount: "0"}, true},
	{"a flag of true", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationDisownSettled: "true", credentialuse.AnnotationHeldCount: "0"}, true},
	// Set, but naming nothing: it never settles (decided 2026-11-25).
	{"a switch that names nothing", map[string]string{credentialuse.AnnotationDisown: ","}, true},
	{"settled", map[string]string{credentialuse.AnnotationDisown: "all", credentialuse.AnnotationDisownSettled: "all", credentialuse.AnnotationHeldCount: "0"}, false},
	{"settled, for a list of kinds", map[string]string{credentialuse.AnnotationDisown: "Secret,Shield." + testGroup, credentialuse.AnnotationDisownSettled: "Secret,Shield." + testGroup, credentialuse.AnnotationHeldCount: "0"}, false},
	{"an empty switch", map[string]string{credentialuse.AnnotationDisown: ""}, false},
	{"flags under no switch", map[string]string{credentialuse.AnnotationDisownSettled: "all", credentialuse.AnnotationHeldCount: "3"}, false},
}

// A set Disown switch that hasn't settled counts as an instance, with either
// option alone, and without them doesn't.
func TestExistsWithHeldReadsTheDisownSwitch(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	index := newIndex(t)
	for _, tc := range switchCases {
		t.Run(tc.name, func(t *testing.T) {
			setCRDAnnotations(t, holder, tc.ann)
			for name, opt := range map[string]Option{"WithHeld": WithHeld(secretGVK), "WithHeldIndex": WithHeldIndex(index)} {
				ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), opt, withMapper())
				if err != nil || ok != tc.running {
					t.Errorf("%s = (%v, %v), want (%v, nil)", name, ok, err, tc.running)
				}
			}
			if ok, err := Exists(ctx, k8s, holder, labels.Everything()); err != nil || ok {
				t.Errorf("Exists = (%v, %v), want (false, nil): the switch counts only with an option", ok, err)
			}
		})
	}
}

// A Kind at zero replicas wakes for the switch, also over a flag left by
// another value, or of "true"; a settled one leaves it at zero. Reconcile,
// without options, leaves it at zero whatever the switch.
func TestReconcileWithHeldWakesForTheSwitch(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	lastSeen := time.Now().Add(-2 * hour).Truncate(time.Second)
	for _, tc := range switchCases {
		t.Run(tc.name, func(t *testing.T) {
			setCRDAnnotations(t, holder, tc.ann)
			ns := newNamespace(t)
			newDeployment(t, ns, "controller", 0)
			key := types.NamespacedName{Namespace: ns, Name: "controller"}

			res, err := Reconcile(ctx, newCountingClient(t, nil, nil), key, holder, labels.Everything(), lastSeen, hour, 2)
			if err != nil || res.Replicas != 0 || res.Patched || res.Exists {
				t.Fatalf("Reconcile = (%+v, %v), want it left at 0", res, err)
			}

			c := newCountingClient(t, nil, nil)
			res, err = ReconcileWithOptions(ctx, c, key, holder, labels.Everything(), lastSeen, hour, 2, WithHeld(secretGVK))
			if err != nil {
				t.Fatalf("ReconcileWithOptions: %v", err)
			}
			want, reason := int32(0), ReasonIdle
			if tc.running {
				want, reason = 2, ReasonActive
			}
			if res.Replicas != want || res.Reason != reason || res.Exists != tc.running || res.Patched != tc.running {
				t.Errorf("ReconcileWithOptions = %+v, want %d replicas, %s, exists and patched %v", res, want, reason, tc.running)
			}
			if got := scaleReplicas(t, ns, "controller"); got != want {
				t.Errorf("scale replicas = %d, want %d", got, want)
			}
		})
	}
}

// A hold keeps a running controller up, and wakes one at zero; once it is
// released, the Kind scales down after the hold-down.
func TestReconcileWithHeldKeepsAHoldUp(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	s := held(t, holder, newNamespace(t), "held")
	lastSeen := time.Now().Add(-2 * hour).Truncate(time.Second)
	for _, start := range []int32{0, 1, 3} {
		ns := newNamespace(t)
		newDeployment(t, ns, "controller", start)
		res, err := ReconcileWithOptions(ctx, newCountingClient(t, nil, nil), types.NamespacedName{Namespace: ns, Name: "controller"},
			holder, labels.Everything(), lastSeen, hour, 2, WithHeld(secretGVK))
		want := start
		if start == 0 {
			want = 2
		}
		if err != nil || res.Replicas != want || res.Reason != ReasonActive || !res.Exists {
			t.Errorf("at %d: ReconcileWithOptions = (%+v, %v), want %d and Active", start, res, err, want)
		}
	}

	release(t, s)
	ns := newNamespace(t)
	newDeployment(t, ns, "controller", 1)
	res, err := ReconcileWithOptions(ctx, newCountingClient(t, nil, nil), types.NamespacedName{Namespace: ns, Name: "controller"},
		holder, labels.Everything(), lastSeen, hour, 2, WithHeld(secretGVK))
	if err != nil || res.Replicas != 0 || res.Reason != ReasonIdle || !res.Patched {
		t.Errorf("after the release: ReconcileWithOptions = (%+v, %v), want scaled to 0", res, err)
	}
}

// getRecorder is a client whose GETs of kind are recorded, and fail with
// failure when fail says so.
func getRecorder(t *testing.T, kind string, fail func(client.ObjectKey) error) (client.Client, func() []client.ObjectKey) {
	t.Helper()
	var mu sync.Mutex
	var keys []client.ObjectKey
	c := interceptor.NewClient(k8sWithWatch(t), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if gvk, err := c.GroupVersionKindFor(obj); err == nil && gvk.Kind == kind {
				mu.Lock()
				keys = append(keys, key)
				mu.Unlock()
				if fail != nil {
					if err := fail(key); err != nil {
						return err
					}
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	return c, func() []client.ObjectKey {
		mu.Lock()
		defer mu.Unlock()
		return append([]client.ObjectKey(nil), keys...)
	}
}

// WithHeldIndex GETs the objects the index names, in the order of its sorted
// lines, past those that are gone, carry no finalizer of the kind or don't
// parse, and stops at the first that still carries the prefix.
func TestExistsWithHeldIndexStopsAtTheFirstHeldLine(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	collision(t, holder, ns, "b-other")
	newSecret(t, ns, "c-bare", nil)
	held(t, holder, ns, "d-held")
	held(t, holder, ns, "e-held")
	index := newIndex(t,
		line(secretGVK, ns, "e-held"),
		line(secretGVK, ns, "a-gone"),
		"garbage",
		line(secretGVK, ns, "c-bare"),
		line(secretGVK, ns, "d-held"),
		line(secretGVK, ns, "b-other"),
	)
	c, gets := getRecorder(t, "Secret", nil)
	ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeldIndex(index), withMapper())
	if err != nil || !ok {
		t.Fatalf("ExistsWithOptions(WithHeldIndex) = (%v, %v), want (true, nil)", ok, err)
	}
	var got []string
	for _, k := range gets() {
		got = append(got, k.Name)
	}
	if want := []string{"a-gone", "b-other", "c-bare", "d-held"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("GETs of secrets = %q, want %q", got, want)
	}
	if ok, err := Exists(ctx, k8s, holder, labels.Everything()); err != nil || ok {
		t.Errorf("Exists = (%v, %v), want (false, nil): the index counts only with WithHeldIndex", ok, err)
	}

	// No line held: false.
	none := newIndex(t, line(secretGVK, ns, "a-gone"), line(secretGVK, ns, "b-other"), line(secretGVK, ns, "c-bare"))
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeldIndex(none), withMapper()); err != nil || ok {
		t.Errorf("an index of no hold = (%v, %v), want (false, nil)", ok, err)
	}
	empty := newIndex(t)
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeldIndex(empty), withMapper()); err != nil || ok {
		t.Errorf("an empty index = (%v, %v), want (false, nil)", ok, err)
	}
}

// A cluster-scoped kind's line has no namespace. One with a namespace names an
// object that can't exist, and holds nothing, even when an object of that
// name carries the prefix.
func TestExistsWithHeldIndexReadsClusterScopedLines(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	name := fmt.Sprintf("stz-held-%d", nsCounter.Add(1))
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: []string{finalizer(t, holder.GroupKind(), "")}}}
	if err := k8s.Create(ctx, nsObj); err != nil {
		t.Fatal(err)
	}
	bad := newIndex(t, line(namespaceGVK, "some-namespace", name), line(secretGVK, "", name))
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeldIndex(bad), withMapper()); err != nil || ok {
		t.Errorf("lines of objects that can't exist = (%v, %v), want (false, nil)", ok, err)
	}
	good := newIndex(t, line(namespaceGVK, "", name))
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeldIndex(good), withMapper()); err != nil || !ok {
		t.Errorf("the cluster-scoped line = (%v, %v), want (true, nil)", ok, err)
	}
}

// A missing or unreadable index, a failed GET of an object it names, and a
// line of a kind the mapper can't map are errors, never false (§10.6.5, L4;
// decided 2026-11-25). Through ReconcileWithOptions, the error keeps a running
// controller up.
func TestExistsWithHeldIndexErrorsAreErrors(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	newSecret(t, ns, "bare", nil)
	injected := errors.New("injected GET failure")
	index := newIndex(t, line(secretGVK, ns, "bare"))
	missing := types.NamespacedName{Namespace: index.Namespace, Name: "absent"}
	unmapped := newIndex(t, "nowhere.test.krateo.io/Thing/"+ns+"/x")

	failIndex, _ := getRecorder(t, "ConfigMap", func(client.ObjectKey) error { return injected })
	failLine, _ := getRecorder(t, "Secret", func(client.ObjectKey) error { return injected })
	for name, tc := range map[string]struct {
		r      client.Reader
		index  types.NamespacedName
		wantIs error
		check  func(error) bool
	}{
		"a missing index":                   {r: k8s, index: missing},
		"an unreadable index":               {r: failIndex, index: index, wantIs: injected},
		"a failed GET of a line":            {r: failLine, index: index, wantIs: injected},
		"a line of a kind that isn't known": {r: k8s, index: unmapped, check: meta.IsNoMatchError},
	} {
		t.Run(name, func(t *testing.T) {
			ok, err := ExistsWithOptions(ctx, tc.r, holder, labels.Everything(), WithHeldIndex(tc.index), withMapper())
			if err == nil || ok {
				t.Fatalf("ExistsWithOptions = (%v, %v), want an error", ok, err)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("error %v, want it to wrap %v", err, tc.wantIs)
			}
			if tc.check != nil && !tc.check(err) {
				t.Errorf("error %v isn't of the expected kind", err)
			}
		})
	}

	// The missing index keeps a running controller up past its hold-down,
	// and changes nothing at 0.
	for _, start := range []int32{0, 1} {
		dns := newNamespace(t)
		newDeployment(t, dns, "controller", start)
		c := newCountingClient(t, nil, nil)
		res, err := ReconcileWithOptions(ctx, c, types.NamespacedName{Namespace: dns, Name: "controller"}, holder, labels.Everything(),
			time.Now().Add(-2*hour), hour, 2, WithHeldIndex(missing))
		if err == nil || res.Replicas == 0 || res.Reason != ReasonUnknown || *c.patches != 0 {
			t.Errorf("at %d: ReconcileWithOptions = (%+v, %v) with %d patches, want an error, no patch, never 0", start, res, err, *c.patches)
		}
		if got := scaleReplicas(t, dns, "controller"); got != start {
			t.Errorf("at %d: scale replicas = %d, want them unchanged", start, got)
		}
	}
}

// WithHeld and WithHeldIndex add up: a hold found by either counts (decided
// 2026-11-25), as for a holder kind with LabelScoped and PerName kinds.
func TestExistsWithHeldAndWithHeldIndexAddUp(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	// A hold of a PerName kind, which only the index names: not selected by
	// the marker label, so that the list can't find it.
	newSecret(t, ns, "pername", nil, finalizer(t, holder.GroupKind(), ns))
	index := newIndex(t, line(secretGVK, ns, "pername"))
	empty := newIndex(t)
	both := func(ix types.NamespacedName) (bool, error) {
		return ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(shieldGVK), WithHeldIndex(ix), withMapper())
	}
	if ok, err := both(index); err != nil || !ok {
		t.Errorf("a hold the index names = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := both(empty); err != nil || ok {
		t.Errorf("no hold = (%v, %v), want (false, nil)", ok, err)
	}
	newInstance(t, shieldGVK, ns, "labelscoped", marker(holder), finalizer(t, holder.GroupKind(), ns))
	if ok, err := both(empty); err != nil || !ok {
		t.Errorf("a hold the list finds = (%v, %v), want (true, nil)", ok, err)
	}
}

// Failed reads of the held path are errors, never false: the list of a
// protected kind, the CRD, and a mapper that can't map the holder kind.
func TestExistsWithHeldReadErrorsAreErrors(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	injected := errors.New("injected failure")
	failList := interceptor.NewClient(k8sWithWatch(t), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if gvk, err := c.GroupVersionKindFor(list); err == nil && gvk.Kind == "SecretList" {
				return injected
			}
			return c.List(ctx, list, opts...)
		},
	})
	failCRD, _ := getRecorder(t, "CustomResourceDefinition", func(client.ObjectKey) error { return injected })
	for name, tc := range map[string]struct {
		r      client.Reader
		holder schema.GroupVersionKind
		opts   []Option
		wantIs error
		// check pins the reason: the error is of that kind, and no CRD was
		// read before it.
		check func(error) bool
	}{
		"a failed LIST of a protected kind": {r: failList, holder: holder, opts: []Option{WithHeld(secretGVK), withMapper()}, wantIs: injected},
		"a failed GET of the CRD":           {r: failCRD, holder: holder, opts: []Option{WithHeld(secretGVK), withMapper()}, wantIs: injected},
		"a mapper without the holder kind": {holder: holder,
			opts: []Option{WithHeld(secretGVK), WithRESTMapper(meta.NewDefaultRESTMapper(nil))}, check: meta.IsNoMatchError},
		"a holder kind without a group": {holder: schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
			opts:  []Option{WithHeld(secretGVK), withMapper()},
			check: func(err error) bool { return strings.Contains(err.Error(), "a holder kind needs a kind and a group") }},
	} {
		t.Run(name, func(t *testing.T) {
			sel := labels.Everything()
			if tc.holder.Kind == "ConfigMap" {
				sel = labels.SelectorFromSet(map[string]string{"scaletozero-test": "none"})
			}
			r, crdGets := tc.r, func() []client.ObjectKey { return nil }
			if r == nil {
				r, crdGets = getRecorder(t, "CustomResourceDefinition", nil)
			}
			ok, err := ExistsWithOptions(ctx, r, tc.holder, sel, tc.opts...)
			if err == nil || ok {
				t.Fatalf("ExistsWithOptions = (%v, %v), want an error", ok, err)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("error %v, want it to wrap %v", err, tc.wantIs)
			}
			if tc.check != nil && !tc.check(err) {
				t.Errorf("error %v isn't for the reason this case sets up", err)
			}
			if gets := crdGets(); len(gets) != 0 {
				t.Errorf("the CRD was read (%v) before the error, which must come first", gets)
			}
		})
	}
}

// The options are checked before anything is listed: either held option
// without a RESTMapper, WithHeld with no kind (decided 2026-11-25), a kind
// without a version, an index without a name. A nil Option is ignored.
func TestExistsWithOptionsRejectsBadOptions(t *testing.T) {
	r := listFunc(func(context.Context, client.ObjectList, ...client.ListOption) error {
		t.Error("ExistsWithOptions listed despite options it should have rejected")
		return errors.New("unexpected List")
	})
	m := WithRESTMapper(meta.NewDefaultRESTMapper(nil))
	index := types.NamespacedName{Namespace: "a", Name: "b"}
	for name, opts := range map[string][]Option{
		"WithHeld without a mapper":          {WithHeld(secretGVK)},
		"WithHeldIndex without a mapper":     {WithHeldIndex(index)},
		"WithHeld with no kind":              {WithHeld(), m},
		"WithHeld of a kind with no version": {WithHeld(schema.GroupVersionKind{Kind: "Secret"}), m},
		"WithHeldIndex without a name":       {WithHeldIndex(types.NamespacedName{Namespace: "a"}), m},
		"WithHeldIndex without a namespace":  {WithHeldIndex(types.NamespacedName{Name: "b"}), m},
		"WithHeldFilter alone":               {WithHeldFilter(func(credentialuse.ID) bool { return true }), m},
	} {
		if ok, err := ExistsWithOptions(context.Background(), r, widgetGVK, labels.Everything(), opts...); err == nil || ok {
			t.Errorf("%s: ExistsWithOptions = (%v, %v), want an error", name, ok, err)
		}
	}
	calls := 0
	empty := listFunc(func(context.Context, client.ObjectList, ...client.ListOption) error {
		calls++
		return nil
	})
	if ok, err := ExistsWithOptions(context.Background(), empty, widgetGVK, labels.Everything(), nil); err != nil || ok || calls != 1 {
		t.Errorf("with a nil Option = (%v, %v) after %d lists, want (false, nil) after 1", ok, err, calls)
	}
}

// The package documentation's grants are enough. WithHeld: list on the
// instances and the protected kind, get on the CRD by name. WithHeldIndex:
// list on the instances, get on the CRD, the index and the held object, each
// by name, with no list on Secrets.
func TestExistsWithHeldNeedsOnlyTheDocumentedGrants(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	held(t, holder, ns, "held")
	index := newIndex(t, line(secretGVK, ns, "held"))
	plural := strings.ToLower(holder.Kind) + "s"

	userClient := func(name string, cluster []rbacv1.PolicyRule, namespaced map[string][]rbacv1.PolicyRule) client.Client {
		t.Helper()
		user, err := testEnv.AddUser(envtest.User{Name: name}, nil)
		if err != nil {
			t.Fatal(err)
		}
		subject := []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: name}}
		objs := []client.Object{
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: cluster},
			&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, Subjects: subject,
				RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}},
		}
		for rns, rules := range namespaced {
			objs = append(objs,
				&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: rns, Name: name}, Rules: rules},
				&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: rns, Name: name}, Subjects: subject,
					RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}})
		}
		for _, o := range objs {
			if err := k8s.Create(ctx, o); err != nil {
				t.Fatal(err)
			}
		}
		c, err := client.New(user.Config(), client.Options{Scheme: scheme})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	instances := rbacv1.PolicyRule{APIGroups: []string{testGroup}, Resources: []string{plural}, Verbs: []string{"list"}}
	crd := rbacv1.PolicyRule{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"},
		ResourceNames: []string{crdName(holder)}, Verbs: []string{"get"}}

	lister := userClient("held-lister-"+ns, []rbacv1.PolicyRule{instances, crd,
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"list"}}}, nil)
	var ok bool
	var err error
	eventually(t, "the lister's grant to take effect", func() bool {
		ok, err = ExistsWithOptions(ctx, lister, holder, labels.Everything(), WithHeld(secretGVK), WithRESTMapper(lister.RESTMapper()))
		return err == nil
	})
	if !ok {
		t.Errorf("WithHeld as the lister = false, want true")
	}

	getter := userClient("held-getter-"+ns, []rbacv1.PolicyRule{instances, crd}, map[string][]rbacv1.PolicyRule{
		index.Namespace: {{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{index.Name}, Verbs: []string{"get"}}},
		ns:              {{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"held"}, Verbs: []string{"get"}}},
	})
	eventually(t, "the getter's grant to take effect", func() bool {
		ok, err = ExistsWithOptions(ctx, getter, holder, labels.Everything(), WithHeldIndex(index), WithRESTMapper(getter.RESTMapper()))
		return err == nil
	})
	if !ok {
		t.Errorf("WithHeldIndex as the getter = false, want true")
	}
	// The grant is narrow enough to mean something: it can't list Secrets.
	if _, err := ExistsWithOptions(ctx, getter, holder, labels.Everything(), WithHeld(secretGVK), WithRESTMapper(getter.RESTMapper())); err == nil {
		t.Error("WithHeld as the getter, who can't list Secrets: want an error")
	}
}

// filterOf returns a filter that accepts the names in accept, and records
// every ID it is asked about.
func filterOf(accept ...string) (func(credentialuse.ID) bool, func() []credentialuse.ID) {
	var mu sync.Mutex
	var asked []credentialuse.ID
	return func(id credentialuse.ID) bool {
			mu.Lock()
			defer mu.Unlock()
			asked = append(asked, id)
			return slices.Contains(accept, id.Name)
		}, func() []credentialuse.ID {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(asked)
		}
}

// WithHeldFilter counts, of WithHeld's lists, only the holds it accepts, and
// the check still stops at the first one accepted (decided 2026-11-26). It is
// asked only about holds, with the protected kind's group and kind, and
// passes over a refused one as over a collision.
func TestExistsWithHeldFilterOnTheLists(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	collision(t, holder, ns, "a-collision")
	held(t, holder, ns, "b-refused")
	held(t, holder, ns, "c-accepted")
	held(t, holder, ns, "d-accepted")

	accept, asked := filterOf("c-accepted", "d-accepted")
	c, requests := recordingClient(t, "secrets")
	ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeld(secretGVK), WithHeldFilter(accept), WithRESTMapper(c.RESTMapper()))
	if err != nil || !ok {
		t.Fatalf("ExistsWithOptions = (%v, %v), want (true, nil)", ok, err)
	}
	want := []credentialuse.ID{{Kind: "Secret", Namespace: ns, Name: "b-refused"}, {Kind: "Secret", Namespace: ns, Name: "c-accepted"}}
	if got := asked(); !slices.Equal(got, want) {
		t.Errorf("the filter was asked about %v, want %v: the holds only, up to the first accepted", got, want)
	}
	if n := len(requests()); n != 3 {
		t.Errorf("%d LIST requests of secrets, want 3: past the collision and the refused hold, then the first accepted", n)
	}

	none, _ := filterOf()
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), WithHeldFilter(none), withMapper()); err != nil || ok {
		t.Errorf("a filter that accepts none = (%v, %v), want (false, nil)", ok, err)
	}
	// Several filters must all accept: b by one, c and d by the other.
	onlyB, _ := filterOf("b-refused")
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), WithHeldFilter(accept), WithHeldFilter(onlyB), withMapper()); err != nil || ok {
		t.Errorf("two filters that accept no hold together = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), WithHeldFilter(nil), withMapper()); err != nil || !ok {
		t.Errorf("a nil filter = (%v, %v), want (true, nil): it is ignored", ok, err)
	}
}

// WithHeldFilter counts, of WithHeldIndex's lines, only the holds it accepts,
// and a line it refuses isn't read.
func TestExistsWithHeldFilterOnTheIndex(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	held(t, holder, ns, "a-refused")
	held(t, holder, ns, "b-accepted")
	held(t, holder, ns, "c-accepted")
	index := newIndex(t, line(secretGVK, ns, "a-refused"), line(secretGVK, ns, "b-accepted"), line(secretGVK, ns, "c-accepted"))

	accept, asked := filterOf("b-accepted", "c-accepted")
	c, gets := getRecorder(t, "Secret", nil)
	ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeldIndex(index), WithHeldFilter(accept), withMapper())
	if err != nil || !ok {
		t.Fatalf("ExistsWithOptions = (%v, %v), want (true, nil)", ok, err)
	}
	if got := gets(); len(got) != 1 || got[0].Name != "b-accepted" {
		t.Errorf("GETs of secrets = %v, want b-accepted only: the refused line isn't read", got)
	}
	want := []credentialuse.ID{{Kind: "Secret", Namespace: ns, Name: "a-refused"}, {Kind: "Secret", Namespace: ns, Name: "b-accepted"}}
	if got := asked(); !slices.Equal(got, want) {
		t.Errorf("the filter was asked about %v, want %v", got, want)
	}

	none, _ := filterOf()
	c, gets = getRecorder(t, "Secret", nil)
	if ok, err := ExistsWithOptions(ctx, c, holder, labels.Everything(), WithHeldIndex(index), WithHeldFilter(none), withMapper()); err != nil || ok {
		t.Errorf("a filter that accepts none = (%v, %v), want (false, nil)", ok, err)
	}
	if got := gets(); len(got) != 0 {
		t.Errorf("GETs of secrets = %v, want none", got)
	}
}

// The filter doesn't apply to the Disown switch, which isn't the hold of a
// name: a switch that hasn't settled counts whatever the filter.
func TestExistsWithHeldFilterLeavesTheSwitch(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	index := newIndex(t)
	none, asked := filterOf()
	setCRDAnnotations(t, holder, map[string]string{credentialuse.AnnotationDisown: "all"})
	for name, opt := range map[string]Option{"WithHeld": WithHeld(secretGVK), "WithHeldIndex": WithHeldIndex(index)} {
		if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), opt, WithHeldFilter(none), withMapper()); err != nil || !ok {
			t.Errorf("%s with an unsettled switch = (%v, %v), want (true, nil)", name, ok, err)
		}
	}
	if got := asked(); len(got) != 0 {
		t.Errorf("the filter was asked about %v, want nothing", got)
	}
}

// mapperClient is a client whose RESTMapper is mapper, and counts the calls
// to it.
type mapperClient struct {
	client.Client
	mapper meta.RESTMapper
	calls  *int
}

func (c mapperClient) RESTMapper() meta.RESTMapper {
	*c.calls++
	return c.mapper
}

// ReconcileWithOptions takes c.RESTMapper() only when a held option is set
// and no WithRESTMapper is given, which takes its place. Without options it
// never calls it, as Reconcile never did.
func TestReconcileTakesTheClientsMapperOnlyWhenNeeded(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	held(t, holder, newNamespace(t), "held")
	lastSeen := time.Now().Add(-2 * hour).Truncate(time.Second)
	run := func(t *testing.T, mapper meta.RESTMapper, wake bool, opts ...Option) (Result, int, error) {
		t.Helper()
		ns := newNamespace(t)
		newDeployment(t, ns, "controller", 0)
		gvk := holder
		if wake {
			// An instance, for the runs without held options.
			gvk = widgetGVK
			newInstance(t, widgetGVK, ns, "w", runLabel(ns))
		}
		calls := 0
		c := mapperClient{Client: newCountingClient(t, nil, nil).Client, mapper: mapper, calls: &calls}
		sel := labels.Everything()
		if wake {
			sel = runSelector(ns)
		}
		res, err := ReconcileWithOptions(ctx, c, types.NamespacedName{Namespace: ns, Name: "controller"}, gvk, sel, lastSeen, hour, 1, opts...)
		return res, calls, err
	}

	t.Run("Reconcile never calls it", func(t *testing.T) {
		ns := newNamespace(t)
		newDeployment(t, ns, "controller", 0)
		newInstance(t, widgetGVK, ns, "w", runLabel(ns))
		calls := 0
		c := mapperClient{Client: newCountingClient(t, nil, nil).Client, calls: &calls}
		res, err := Reconcile(ctx, c, types.NamespacedName{Namespace: ns, Name: "controller"}, widgetGVK, runSelector(ns), lastSeen, hour, 1)
		if err != nil || res.Replicas != 1 || calls != 0 {
			t.Errorf("Reconcile = (%+v, %v) after %d RESTMapper calls, want woken to 1 after none", res, err, calls)
		}
	})
	t.Run("no options", func(t *testing.T) {
		res, calls, err := run(t, nil, true)
		if err != nil || res.Replicas != 1 || calls != 0 {
			t.Errorf("ReconcileWithOptions = (%+v, %v) after %d RESTMapper calls, want woken to 1 after none", res, err, calls)
		}
	})
	t.Run("a WithRESTMapper takes its place", func(t *testing.T) {
		// The client's mapper knows nothing: were it taken, the CRD's name
		// couldn't be found.
		res, calls, err := run(t, meta.NewDefaultRESTMapper(nil), false, WithHeld(secretGVK), withMapper())
		if err != nil || res.Replicas != 1 || !res.Exists || calls != 0 {
			t.Errorf("ReconcileWithOptions = (%+v, %v) after %d RESTMapper calls, want woken by the hold after none", res, err, calls)
		}
	})
	t.Run("a held option takes the client's", func(t *testing.T) {
		res, calls, err := run(t, k8s.RESTMapper(), false, WithHeld(secretGVK))
		if err != nil || res.Replicas != 1 || !res.Exists || calls != 1 {
			t.Errorf("ReconcileWithOptions = (%+v, %v) after %d RESTMapper calls, want woken by the hold after 1", res, err, calls)
		}
	})
	t.Run("a client without a mapper is an error", func(t *testing.T) {
		recovered(t, "ReconcileWithOptions with no mapper", func() {
			res, _, err := run(t, nil, false, WithHeld(secretGVK))
			if err == nil || res.Replicas != 1 || res.Reason != ReasonUnknown || res.Patched {
				t.Errorf("ReconcileWithOptions = (%+v, %v), want an error that changes nothing", res, err)
			}
		})
	})
}

// recovered runs f, and turns a panic in it into a failure of this
// assertion: the test binary stays alive, so the other tests still run, and
// the panic is attributed to the call that should have returned an error.
func recovered(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("%s panicked, want an error: %v", what, p)
		}
	}()
	f()
}

// An index line whose name, or namespace, isn't a single path segment, as a
// hand edit may leave, names an object that can't exist, and holds nothing:
// it isn't read. Were it read, nothing on the client side would stop the GET.
// The API server refuses a name that isn't a path segment (a 400, so an
// error), but nothing refuses "..", which the client's path join cleans into
// a GET of the Namespace (so false), nor a namespace that isn't one (a 404,
// so false). So the test asserts that no Secret is read at all.
func TestExistsWithHeldIndexSkipsWhatIsNoPathSegment(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	for name, l := range map[string]string{
		"a name of ..":       line(secretGVK, ns, ".."),
		"a name with %":      line(secretGVK, ns, "a%2Fb"),
		"a namespace of ..":  line(secretGVK, "..", "x"),
		"a namespace with %": line(secretGVK, "a%2Fb", "x"),
	} {
		index := newIndex(t, l)
		r, gets := getRecorder(t, "Secret", nil)
		if ok, err := ExistsWithOptions(ctx, r, holder, labels.Everything(), WithHeldIndex(index), withMapper()); err != nil || ok {
			t.Errorf("%s (%q) = (%v, %v), want (false, nil)", name, l, ok, err)
		}
		if got := gets(); len(got) != 0 {
			t.Errorf("%s (%q): GETs of secrets %v, want none", name, l, got)
		}
	}
}

// HeldWithOptions checks only the holds and the Disown switch: it never lists
// the instances (decided 2026-11-26), and takes the same options and errors.
func TestHeldWithOptionsIgnoresInstances(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	holder := newHolderKind(t)
	ns := newNamespace(t)
	newInstance(t, holder, ns, "instance", nil)
	plural := strings.ToLower(holder.Kind) + "s"

	c, requests := recordingClient(t, plural)
	ok, err := HeldWithOptions(ctx, c, holder, WithHeld(secretGVK), WithRESTMapper(c.RESTMapper()))
	if err != nil || ok {
		t.Errorf("HeldWithOptions with an instance and no hold = (%v, %v), want (false, nil)", ok, err)
	}
	if n := len(requests()); n != 0 {
		t.Errorf("HeldWithOptions listed the instances %d times, want never", n)
	}
	if ok, err := ExistsWithOptions(ctx, k8s, holder, labels.Everything(), WithHeld(secretGVK), withMapper()); err != nil || !ok {
		t.Errorf("ExistsWithOptions with the instance = (%v, %v), want (true, nil): it is unchanged", ok, err)
	}

	// The switch counts.
	setCRDAnnotations(t, holder, map[string]string{credentialuse.AnnotationDisown: "all"})
	if ok, err := HeldWithOptions(ctx, k8s, holder, WithHeld(secretGVK), withMapper()); err != nil || !ok {
		t.Errorf("HeldWithOptions with an unsettled switch = (%v, %v), want (true, nil)", ok, err)
	}
	setCRDAnnotations(t, holder, nil)

	// Holds count, of the lists and of the index, and the filter applies.
	held(t, holder, ns, "listed")
	newSecret(t, ns, "indexed", nil, finalizer(t, holder.GroupKind(), ns))
	index := newIndex(t, line(secretGVK, ns, "indexed"))
	for name, opts := range map[string][]Option{
		"WithHeld":      {WithHeld(secretGVK)},
		"WithHeldIndex": {WithHeldIndex(index)},
	} {
		if ok, err := HeldWithOptions(ctx, k8s, holder, append(opts, withMapper())...); err != nil || !ok {
			t.Errorf("HeldWithOptions(%s) with a hold = (%v, %v), want (true, nil)", name, ok, err)
		}
		none, _ := filterOf()
		if ok, err := HeldWithOptions(ctx, k8s, holder, append(opts, WithHeldFilter(none), withMapper())...); err != nil || ok {
			t.Errorf("HeldWithOptions(%s) with a filter that accepts none = (%v, %v), want (false, nil)", name, ok, err)
		}
	}

	// Errors are errors: a missing index, and options that are wrong.
	missing := types.NamespacedName{Namespace: index.Namespace, Name: "absent"}
	if ok, err := HeldWithOptions(ctx, k8s, holder, WithHeldIndex(missing), withMapper()); err == nil || ok {
		t.Errorf("HeldWithOptions with a missing index = (%v, %v), want an error", ok, err)
	}
	for name, opts := range map[string][]Option{
		"no option":                   nil,
		"WithHeld without a mapper":   {WithHeld(secretGVK)},
		"WithHeld with no kind":       {WithHeld(), withMapper()},
		"a mapper and no held option": {withMapper()},
	} {
		recovered(t, "HeldWithOptions with "+name, func() {
			if ok, err := HeldWithOptions(ctx, k8s, holder, opts...); err == nil || ok {
				t.Errorf("HeldWithOptions with %s = (%v, %v), want an error", name, ok, err)
			}
		})
	}
	recovered(t, "HeldWithOptions with a nil reader", func() {
		if _, err := HeldWithOptions(ctx, nil, holder, WithHeld(secretGVK), withMapper()); err == nil {
			t.Error("HeldWithOptions with a nil reader: want an error")
		}
	})
	if _, err := HeldWithOptions(ctx, k8s, schema.GroupVersionKind{Group: testGroup, Kind: holder.Kind}, WithHeld(secretGVK), withMapper()); err == nil {
		t.Error("HeldWithOptions without a version: want an error")
	}
}
