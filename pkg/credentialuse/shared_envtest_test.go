package credentialuse_test

// The core's client-go runtime against envtest (plan, PR 3a): the informers,
// the relist rule, setup, and the published settings, driven by the §7.2
// driver.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

const ownerFinalizer = "credentialuse.test.krateo.io/owner"

var widgetGK = cutest.WidgetGVK.GroupKind()

// clientsFor builds the core's clients from cfg.
func clientsFor(t *testing.T, cfg *rest.Config) cu.Clients {
	t.Helper()
	md, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cu.Clients{Metadata: md, Dynamic: dyn, Mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))}
}

// secretRefs is the Widget's RefsFunc: spec.secret names a Secret in the
// Widget's namespace.
func secretRefs(obj metav1.Object) ([]cu.Ref, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("a %T, not an unstructured Widget", obj)
	}
	name, _, err := unstructured.NestedString(u.Object, "spec", "secret")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, nil
	}
	return []cu.Ref{cu.SecretRef(obj.GetNamespace(), name)}, nil
}

// enqueued records the holders the core enqueues.
type enqueued struct {
	mu  sync.Mutex
	got map[types.NamespacedName]int
}

func (e *enqueued) add(nn types.NamespacedName) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.got == nil {
		e.got = map[types.NamespacedName]int{}
	}
	e.got[nn]++
}

func (e *enqueued) count(nn types.NamespacedName) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.got[nn]
}

func widgetOptions(enq func(types.NamespacedName)) cu.Options {
	return cu.Options{
		Holder:          cutest.WidgetGVK,
		OwnerFinalizers: []string{ownerFinalizer},
		Refs:            secretRefs,
		Enqueue:         enq,
		TimeoutBound:    90 * time.Second,
	}
}

// running starts s, fails t on a Run error, and waits until it has synced.
func running(t *testing.T, s *cu.Shared) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errc; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	})
	cutest.Eventually(t, time.Minute, func() error {
		select {
		case err := <-errc:
			errc <- err
			t.Fatalf("Run returned before it synced: %v", err)
		default:
		}
		if !s.HasSynced() {
			return cutest.ErrNotYet
		}
		return nil
	})
	return cancel
}

func createWidget(t *testing.T, ns, name, secret string, finalizers ...string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": secret}}}
	u.SetGroupVersionKind(cutest.WidgetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetFinalizers(finalizers)
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating widget %s/%s: %v", ns, name, err)
	}
}

// epochOf is the Secret's epoch for the Widget kind in holder namespace ns,
// or "" without one.
func epochOf(t *testing.T, ns, name string) string {
	t.Helper()
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading secret %s/%s: %v", ns, name, err)
	}
	f, _ := cu.FinalizerName(widgetGK, ns)
	return s.Annotations[f]
}

func newDriver(t *cu.Tracker) *driver.Driver {
	return &driver.Driver{Tracker: t, Dynamic: env.Dynamic, Holder: cutest.WidgetGVR, OwnerFinalizer: ownerFinalizer}
}

// pass runs one driver pass, failing t on an error.
func pass(t *testing.T, d *driver.Driver, nn types.NamespacedName) driver.Result {
	t.Helper()
	r, err := d.Reconcile(context.Background(), nn)
	if err != nil {
		t.Fatalf("reconciling %s: %v", nn, err)
	}
	return r
}

// settled runs passes until one skips A2 (the Secret's epoch stays), which
// happens once the holder informer has delivered the version a pass works
// from; it fails t if none does within a minute.
func settled(t *testing.T, d *driver.Driver, nn types.NamespacedName, secret string) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		before := epochOf(t, nn.Namespace, secret)
		pass(t, d, nn)
		if epochOf(t, nn.Namespace, secret) != before {
			return fmt.Errorf("the pass wrote A2: %w", cutest.ErrNotYet)
		}
		return nil
	})
}

// A forced relist of the holder informer resets recordSince: lastList is a
// floor under every observation time (§6.5 L2, plan A20), so the first pass
// after the relist writes A2 again, and later ones skip. lastList is
// recorded before the handler sees any relisted version: the obligation the
// exact-version skip rule rests on (the re-review of #59). It runs on the
// watch-list path, and again with watch-list refused so that the reflector
// falls back to a List (#52's review).
func TestRelistResetsRecordSince(t *testing.T) {
	cutest.Require(t, env)
	for _, watchList := range []bool{true, false} {
		t.Run(fmt.Sprintf("watch-list %v", watchList), func(t *testing.T) {
			ns := env.Namespace(t, "relist")
			createSecret(t, ns, "creds")
			createWidget(t, ns, "w", "creds")
			nn := types.NamespacedName{Namespace: ns, Name: "w"}

			s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
			tr, err := s.Track(widgetOptions(func(types.NamespacedName) {}))
			if err != nil {
				t.Fatal(err)
			}
			tr.RefuseWatchList(!watchList)
			var mu sync.Mutex
			var floor time.Time
			var seen []time.Time // lastList as each holder event after the relist saw it
			s.OnHolderEvent(func(tk *cu.Tracker, m *metav1.PartialObjectMetadata) {
				mu.Lock()
				defer mu.Unlock()
				if !floor.IsZero() && m.Name == "w" {
					seen = append(seen, tk.LastList())
				}
			})
			running(t, s)
			lists, streams := tr.HolderLists()
			if watchList && streams == 0 {
				t.Fatalf("the holder informer's first list didn't come from a watch-list stream (lists %d): the API server or client doesn't stream lists", lists)
			}
			if !watchList && (lists == 0 || streams != 0) {
				t.Fatalf("with watch-list refused: %d lists, %d streams; want a List only", lists, streams)
			}

			d := newDriver(tr)
			pass(t, d, nn)
			settled(t, d, nn, "creds")

			before := tr.LastList()
			mu.Lock()
			floor = before
			mu.Unlock()
			tr.ExpireHolderWatch()
			cutest.Eventually(t, time.Minute, func() error {
				mu.Lock()
				defer mu.Unlock()
				if !tr.LastList().After(before) || len(seen) == 0 {
					return cutest.ErrNotYet
				}
				return nil
			})
			mu.Lock()
			for i, l := range seen {
				if !l.After(before) {
					t.Errorf("relisted version %d reached the handler with lastList %v, not after the relist's start (%v)", i, l, before)
				}
			}
			mu.Unlock()
			l2, s2 := tr.HolderLists()
			if watchList && s2 <= streams {
				t.Errorf("the relist wasn't a watch-list stream (%d lists, %d streams)", l2, s2)
			}
			if !watchList && l2 <= lists {
				t.Errorf("the relist wasn't a List (%d lists, %d streams)", l2, s2)
			}

			e := epochOf(t, ns, "creds")
			pass(t, d, nn)
			if epochOf(t, ns, "creds") == e {
				t.Error("the first pass after the relist skipped A2: recordSince wasn't reset")
			}
			settled(t, d, nn, "creds")
		})
	}
}

// Track after Run starts the kind's informers and waits for their sync; a
// second Track with equal Options returns the same tracker, and with
// different ones an error (§7.3, L8, C3).
func TestTrackAfterStartAndTwice(t *testing.T) {
	cutest.Require(t, env)
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	enq := func(types.NamespacedName) {}
	o := widgetOptions(enq)
	w, err := s.Track(o)
	if err != nil {
		t.Fatal(err)
	}
	running(t, s)

	again, err := s.Track(o)
	if err != nil || again != w {
		t.Fatalf("Track with equal Options = %p, %v; want the same tracker %p", again, err, w)
	}
	other := o
	other.ProbeInterval = time.Second
	if _, err := s.Track(other); err == nil || !strings.Contains(err.Error(), "other options") {
		t.Errorf("Track with different Options = %v, want an error", err)
	}

	// A kind discovered at runtime: Target as a holder.
	ns := env.Namespace(t, "late")
	createSecret(t, ns, "creds")
	target := createTarget(t, ns, "t")
	_ = unstructured.SetNestedField(target.Object, "creds", "spec", "secret")
	if _, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Update(context.Background(), target, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	to := widgetOptions(enq)
	to.Holder = cutest.TargetGVK
	tt, err := s.Track(to)
	if err != nil {
		t.Fatalf("Track after Run: %v", err)
	}
	if lists, streams := tt.HolderLists(); lists+streams == 0 {
		t.Fatal("Track after Run returned before the new kind's informer listed")
	}
	td := &driver.Driver{Tracker: tt, Dynamic: env.Dynamic, Holder: cutest.TargetGVR, OwnerFinalizer: ownerFinalizer}
	nn := types.NamespacedName{Namespace: ns, Name: "t"}
	if r := pass(t, td, nn); len(r.Acquired.Held) != 1 {
		t.Fatalf("a pass of the late kind held %v, want the Secret", r.Acquired.Held)
	}
	f, _ := cu.FinalizerName(cutest.TargetGVK.GroupKind(), ns)
	got, err := secrets(env.Metadata, ns).Get(context.Background(), "creds", metav1.GetOptions{})
	if err != nil || !hasString(got.Finalizers, f) {
		t.Fatalf("the Secret's finalizers are %v (%v), want %s", got.Finalizers, err, f)
	}
}

func hasString(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// The settings every process of a kind must share are published on its CRD
// once; a second process that disagrees refuses to start, and one that
// agrees, in another notation, starts (§7.3, L3). Both of a kind's owner
// finalizers make a holder established (§6.1).
func TestPublishedSettings(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	crds := env.Dynamic.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"})
	name := cutest.WidgetGVR.Resource + "." + cutest.WidgetGVR.Group
	read := func() string {
		crd, err := crds.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return crd.GetAnnotations()[cu.AnnotationConfig]
	}

	first := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	if _, err := first.Track(widgetOptions(func(types.NamespacedName) {})); err != nil {
		t.Fatal(err)
	}
	if got, want := read(), `{"deletionGrace":"60s","disownPasses":5,"disownInterval":"60s","timeoutBound":"90s"}`; got != want {
		t.Fatalf("published %s, want %s", got, want)
	}

	o := widgetOptions(func(types.NamespacedName) {})
	o.DeletionGrace = 30 * time.Second
	second := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	if _, err := second.Track(o); err == nil || !strings.Contains(err.Error(), "deletionGrace") {
		t.Errorf("a process with another deletionGrace: Track = %v, want it refused", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := second.Run(rctx); err == nil || !strings.Contains(err.Error(), "deletionGrace") {
		t.Errorf("its Run = %v, want the same refusal: the process refuses to start", err)
	}

	o = widgetOptions(func(types.NamespacedName) {})
	o.TimeoutBound = 90 * time.Second
	o.DisownInterval = time.Minute
	third := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	if _, err := third.Track(o); err != nil {
		t.Errorf("a process that agrees: %v", err)
	}

	// Two owner finalizers, as during a rename: a holder with either is
	// established, so a failed acquire leaves it Degraded, not
	// MustNotConnect.
	o = widgetOptions(func(types.NamespacedName) {})
	o.OwnerFinalizers = []string{ownerFinalizer, "credentialuse.test.krateo.io/old-owner"}
	o.Refs = func(metav1.Object) ([]cu.Ref, error) { return nil, errors.New("no refs today") }
	fourth := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	tr, err := fourth.Track(o)
	if err != nil {
		t.Fatal(err)
	}
	ns := env.Namespace(t, "owners")
	for _, f := range o.OwnerFinalizers {
		w := strings.TrimPrefix(f, "credentialuse.test.krateo.io/")
		createWidget(t, ns, w, "", f)
		obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, w, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		a, err := tr.Acquire(ctx, obj)
		if err != nil || !a.Degraded || a.MustNotConnect {
			t.Errorf("a holder with %s: Acquire = %+v, %v; want Degraded", f, a, err)
		}
		removeFinalizers(t, ns, w)
	}
}

func removeFinalizers(t *testing.T, ns, name string) {
	t.Helper()
	_, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(context.Background(), name, types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

// Run fails at once, with §7.5's message, when the Namespace informer can't
// list or can't watch namespaces (L3): an impersonated ServiceAccount
// without those verbs.
func TestRunNeedsNamespaceListAndWatch(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	for _, verbs := range [][]string{{"get", "watch"}, {"get", "list"}} {
		t.Run(strings.Join(verbs, "+"), func(t *testing.T) {
			sa := fmt.Sprintf("cu-ns-%s", strings.Join(verbs, "-"))
			role := &rbacv1.ClusterRole{
				ObjectMeta: metav1.ObjectMeta{Name: sa},
				Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: verbs}},
			}
			if _, err := env.Kube.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: sa},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: sa},
				Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "default", Name: sa}},
			}
			if _, err := env.Kube.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			s := cu.NewShared(clientsFor(t, env.AsServiceAccount("default", sa)), cu.SharedOptions{})
			s.SetSyncTimeout(30 * time.Second)
			rctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			start := time.Now()
			err := s.Run(rctx)
			if err == nil || !strings.Contains(err.Error(), "grant get, list and watch on namespaces") {
				t.Fatalf("Run = %v, want the Namespace informer's error", err)
			}
			if time.Since(start) > 20*time.Second {
				t.Errorf("Run took %s to fail: a forbidden list or watch should fail it at once", time.Since(start))
			}
		})
	}
}

// The Namespace handler acts only on a change to a credential-use.*
// annotation or to the deletionTimestamp (L5); on a new deletionTimestamp
// it enqueues the release batch of every held object in the namespace (§5).
func TestNamespaceHandlerActsOnlyOnItsFields(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	ns := env.Namespace(t, "nsh")
	createSecret(t, ns, "creds")
	createWidget(t, ns, "w", "creds")

	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	enq := &enqueued{}
	tr, err := s.Track(widgetOptions(enq.add))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	acts := 0
	s.OnNamespaceAct(func(n string) {
		if n == ns {
			mu.Lock()
			acts++
			mu.Unlock()
		}
	})
	actsNow := func() int { mu.Lock(); defer mu.Unlock(); return acts }
	running(t, s)
	d := newDriver(tr)
	nn := types.NamespacedName{Namespace: ns, Name: "w"}
	pass(t, d, nn)
	settled(t, d, nn, "creds")

	patchNS := func(patch string) {
		t.Helper()
		if _, err := env.Kube.CoreV1().Namespaces().Patch(ctx, ns, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	patchNS(`{"metadata":{"labels":{"team":"a"},"annotations":{"note":"x"}}}`)
	patchNS(`{"metadata":{"annotations":{"` + cu.AnnotationAllowHoldersFrom + `":"*"}}}`)
	cutest.Eventually(t, 30*time.Second, func() error {
		if actsNow() != 1 {
			return fmt.Errorf("%d acts: %w", actsNow(), cutest.ErrNotYet)
		}
		return nil
	})
	if enq.count(nn) == 0 {
		t.Error("a credential-use.* annotation change didn't enqueue the namespace's holders")
	}

	// Take every batch queued so far, so that what follows is the
	// deletion's own.
	s.DrainQueue()
	time.Sleep(time.Second) // let any event in flight arrive, and drain it too
	s.DrainQueue()
	if queued, _ := s.QueuedBatch(widgetGK, ns); queued {
		t.Fatal("the batch is still queued after the drain")
	}
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	secret := cu.SecretRef(ns, "creds").ID()
	cutest.Eventually(t, 30*time.Second, func() error {
		queued, ids := s.QueuedBatch(widgetGK, ns)
		if actsNow() != 2 || !queued || !hasID(ids, secret) {
			return fmt.Errorf("%d acts, batch queued %v covering %v: %w", actsNow(), queued, ids, cutest.ErrNotYet)
		}
		return nil
	})
	time.Sleep(time.Second)
	if actsNow() != 2 {
		t.Errorf("%d acts: the handler acted on a change that isn't its", actsNow())
	}
}

func hasID(ids []cu.ID, id cu.ID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// A kubectl replace that strips our metadata takes the Secret out of the
// label-scoped cache, which the informer delivers as a deletion: the
// Secret's holders are enqueued, and their next pass writes A2 again (§6.5,
// §10.2).
func TestStrippingEnqueuesTheHolders(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	ns := env.Namespace(t, "strip")
	createSecret(t, ns, "creds")
	createWidget(t, ns, "w", "creds")
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	enq := &enqueued{}
	tr, err := s.Track(widgetOptions(enq.add))
	if err != nil {
		t.Fatal(err)
	}
	running(t, s)
	d := newDriver(tr)
	nn := types.NamespacedName{Namespace: ns, Name: "w"}
	pass(t, d, nn)
	settled(t, d, nn, "creds")
	before := enq.count(nn)

	sec, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "creds", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sec.Finalizers, sec.Labels, sec.Annotations = nil, nil, nil
	if _, err := env.Kube.CoreV1().Secrets(ns).Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if enq.count(nn) <= before {
			return cutest.ErrNotYet
		}
		return nil
	})
	pass(t, d, nn)
	f, _ := cu.FinalizerName(widgetGK, ns)
	got, err := secrets(env.Metadata, ns).Get(ctx, "creds", metav1.GetOptions{})
	if err != nil || !hasString(got.Finalizers, f) || got.Labels[cu.LabelHeld] != "true" {
		t.Fatalf("after the holder's pass the Secret has finalizers %v, labels %v (%v); want the hold back", got.Finalizers, got.Labels, err)
	}
}

// A Secret already being deleted, without the kind's finalizer, gives a new
// holder MustNotConnect, with §12's message; it never gets its owner
// finalizer (§6.5, §11.4; deleting freely is PR 7's).
func TestDeletingSecretRefusesANewHolder(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	ns := env.Namespace(t, "deleting")
	createSecret(t, ns, "creds", "example.com/keep")
	if err := env.Kube.CoreV1().Secrets(ns).Delete(ctx, "creds", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	createWidget(t, ns, "w", "creds")
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	tr, err := s.Track(widgetOptions(func(types.NamespacedName) {}))
	if err != nil {
		t.Fatal(err)
	}
	running(t, s)
	d := newDriver(tr)
	nn := types.NamespacedName{Namespace: ns, Name: "w"}
	r := pass(t, d, nn)
	if !r.Acquired.MustNotConnect || r.Connected || r.Acquired.Err == nil ||
		!strings.Contains(r.Acquired.Err.Error(), "is being deleted and can't be held; not connecting until it is replaced or the reference changes") {
		t.Fatalf("pass = %+v; want MustNotConnect with §12's message, and no Connect", r)
	}
	w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
	if err != nil || hasString(w.GetFinalizers(), ownerFinalizer) {
		t.Fatalf("the holder's finalizers are %v (%v); it must not be established", w.GetFinalizers(), err)
	}
	// Clean up the Secret's own finalizer.
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, "creds", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}
