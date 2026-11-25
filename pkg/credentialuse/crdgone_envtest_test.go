package credentialuse_test

// A holder kind whose CRD goes while its tracker runs (§6.4, "The holder kind
// itself gone"), against envtest: what the API server answers, and what
// becomes of the kind's holds.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// Gizmos are the holder kind that goes.
var (
	gizmoGVK = schema.GroupVersionKind{Group: cutest.Group, Version: "v1", Kind: "Gizmo"}
	gizmoGVR = schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "gizmos"}
	gizmoCRD = "gizmos." + cutest.Group
)

func createGizmoCRD(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	cutest.Eventually(t, time.Minute, func() error {
		return env.Client.Create(ctx, cutest.CRD("Gizmo", "gizmos"))
	})
	cutest.Eventually(t, time.Minute, func() error {
		_, err := env.Dynamic.Resource(gizmoGVR).List(ctx, metav1.ListOptions{})
		return err
	})
}

// deleteGizmoCRD deletes the CRD, and waits until it is gone and its
// resource is no longer served.
func deleteGizmoCRD(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := env.Dynamic.Resource(crdGVR).Delete(ctx, gizmoCRD, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	cutest.Eventually(t, 2*time.Minute, func() error {
		if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, gizmoCRD, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the Gizmo CRD is still there (%v): %w", err, cutest.ErrNotYet)
		}
		if _, err := env.Dynamic.Resource(gizmoGVR).List(ctx, metav1.ListOptions{}); err == nil {
			return fmt.Errorf("Gizmos are still served: %w", cutest.ErrNotYet)
		}
		return nil
	})
}

// freeGizmos takes every finalizer off every Gizmo, as an uninstall's cleanup
// may, so that they go with their CRD.
func freeGizmos(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	l, err := env.Dynamic.Resource(gizmoGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for i := range l.Items {
		_, _ = env.Dynamic.Resource(gizmoGVR).Namespace(l.Items[i].GetNamespace()).Patch(ctx, l.Items[i].GetName(),
			types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	}
}

// What the evidence rests on. A CRD stays while an instance has a finalizer,
// and its kind is listed meanwhile. Once it is gone, the list is a 404 that
// may mean the kind isn't served, and the GET of the CRD one that names it.
func TestPremiseAHolderKindThatIsGone(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "gone-premise")
	createGizmoCRD(t)
	t.Cleanup(func() { freeGizmos(t); deleteGizmoCRD(t) })
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(gizmoGVK)
	u.SetNamespace(ns)
	u.SetName("g")
	u.SetFinalizers([]string{ownerFinalizer})
	if _, err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	crds, gizmos := env.Metadata.Resource(crdGVR), env.Metadata.Resource(gizmoGVR).Namespace(ns)
	if err := crds.Delete(ctx, gizmoCRD, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		crd, err := crds.Get(ctx, gizmoCRD, metav1.GetOptions{})
		if err != nil || crd.DeletionTimestamp == nil {
			t.Fatalf("the CRD with an instance left: %v, deleting %v", err, crd.GetDeletionTimestamp())
		}
		if l, err := gizmos.List(ctx, metav1.ListOptions{}); err != nil || len(l.Items) != 1 {
			t.Fatalf("listing the kind meanwhile: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	freeGizmos(t)
	deleteGizmoCRD(t)
	_, err := gizmos.List(ctx, metav1.ListOptions{})
	if !cu.KindUnserved(err) || statusOf(t, err).Code != http.StatusNotFound {
		t.Errorf("listing the kind that is gone: %v", err)
	}
	_, err = crds.Get(ctx, gizmoCRD, metav1.GetOptions{})
	if !cu.CRDNotFound(err, gizmoCRD) {
		t.Errorf("reading the CRD that is gone: %+v", statusOf(t, err))
	}
	// Of CRDs whose API isn't served, and of a kind that isn't, nothing.
	other := crdGVR
	other.Version = "v9"
	_, err = env.Metadata.Resource(other).Get(ctx, gizmoCRD, metav1.GetOptions{})
	if err == nil || cu.CRDNotFound(err, gizmoCRD) {
		t.Errorf("reading the CRD at a version that isn't served: %v", err)
	}
	_, err = gizmos.Get(ctx, gizmoCRD, metav1.GetOptions{})
	if err == nil || cu.CRDNotFound(err, gizmoCRD) {
		t.Errorf("reading a Gizmo of the CRD's name: %v", err)
	}
}

// A lossy connection can be cut, and kept from coming back.
type lossy struct {
	mu    sync.Mutex
	down  bool
	conns map[net.Conn]bool
}

// lossyConfig is cfg through a connection the test can cut.
func lossyConfig(cfg *rest.Config) (*rest.Config, *lossy) {
	l := &lossy{conns: map[net.Conn]bool{}}
	c := rest.CopyConfig(cfg)
	d := &net.Dialer{Timeout: 5 * time.Second}
	c.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		l.mu.Lock()
		down := l.down
		l.mu.Unlock()
		if down {
			return nil, fmt.Errorf("the test took the connection down")
		}
		conn, err := d.DialContext(ctx, network, addr)
		if err == nil {
			l.mu.Lock()
			l.conns[conn] = true
			l.mu.Unlock()
		}
		return conn, err
	}
	return c, l
}

func (l *lossy) set(down bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = down
	if down {
		for c := range l.conns {
			_ = c.Close()
			delete(l.conns, c)
		}
	}
}

// gizmoRig is a process that tracks Gizmos, and their controller.
type gizmoRig struct {
	s    *cu.Shared
	tr   *cu.Tracker
	c    *controller
	d    *driver.Driver
	logs *warnLog
}

type gizmoOptions struct {
	so     cu.SharedOptions
	change func(*cu.Options)
	mapper meta.RESTMapper
	before func(*cu.Shared)
}

// newGizmoRig creates the Gizmo CRD, and a process whose informers run: a
// standby, until startWrites.
func newGizmoRig(t *testing.T, cfg *rest.Config, g gizmoOptions) *gizmoRig {
	t.Helper()
	createGizmoCRD(t)
	t.Cleanup(func() { freeGizmos(t); deleteGizmoCRD(t) })
	r := &gizmoRig{c: newController(t), logs: &warnLog{}}
	clients := clientsFor(t, cfg)
	if g.mapper != nil {
		clients.Mapper = g.mapper
	}
	r.s = cu.NewShared(clients, g.so)
	o := widgetOptions(r.c.enqueue)
	o.Holder, o.DeletionGrace, o.ProbeInterval, o.Logger = gizmoGVK, testGrace, time.Second, r.logs
	if g.change != nil {
		g.change(&o)
	}
	if g.before != nil {
		g.before(r.s)
	}
	gizmosMapped(t, clients.Mapper, gizmoGVR)
	var err error
	if r.tr, err = r.s.Track(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	runningInformers(t, r.s)
	r.d = &driver.Driver{Tracker: r.tr, Dynamic: env.Dynamic, Holder: gizmoGVR, OwnerFinalizer: ownerFinalizer}
	r.c.start(r.d)
	return r
}

// gizmosMapped waits until mapper maps Gizmos to gvr: discovery may lag a list
// that works, and a Track that fails before Run makes Run fail (§7.3).
func gizmosMapped(t *testing.T, mapper meta.RESTMapper, gvr schema.GroupVersionResource) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		if rm, ok := mapper.(meta.ResettableRESTMapper); ok {
			rm.Reset()
		}
		m, err := mapper.RESTMapping(gizmoGVK.GroupKind(), gizmoGVK.Version)
		if err != nil {
			return err
		}
		if m.Resource != gvr {
			return fmt.Errorf("Gizmos are mapped to %s, not %s: %w", m.Resource, gvr, cutest.ErrNotYet)
		}
		return nil
	})
}

// startWrites runs the write loops until t ends, as a process that is
// elected does.
func (r *gizmoRig) startWrites(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.s.RunWriteLoops(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Logf("RunWriteLoops: %v", err)
		}
	})
}

// create creates Secret secret and Gizmo name, which names it, and waits
// until it is established and the Secret held.
func (r *gizmoRig) create(t *testing.T, ns, name, secret string) {
	t.Helper()
	createSecret(t, ns, secret)
	r.createGizmo(t, ns, name, secret)
	cutest.Eventually(t, time.Minute, func() error {
		g, err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(g.GetFinalizers(), ownerFinalizer) || !gizmoHolds(ns, secret) {
			return fmt.Errorf("%s/%s isn't established, or %s held: %w", ns, name, secret, cutest.ErrNotYet)
		}
		return nil
	})
}

func (r *gizmoRig) createGizmo(t *testing.T, ns, name, secret string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": secret}}}
	u.SetGroupVersionKind(gizmoGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	if _, err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.c.enqueue(types.NamespacedName{Namespace: ns, Name: name})
}

// gizmoHolds reports whether Secret ns/name carries the Gizmos' finalizer.
func gizmoHolds(ns, name string) bool {
	f, _ := cu.FinalizerName(gizmoGVK.GroupKind(), ns)
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil && hasString(s.Finalizers, f)
}

func gizmoReleased(t *testing.T, within time.Duration, ns, name string) {
	t.Helper()
	cutest.Eventually(t, within, func() error {
		if gizmoHolds(ns, name) {
			return fmt.Errorf("%s/%s is still held: %w", ns, name, cutest.ErrNotYet)
		}
		return nil
	})
}

// removeKind deletes Gizmo name, whose controller runs, then the CRD.
func (r *gizmoRig) removeKind(t *testing.T, ns, name string) {
	t.Helper()
	ctx := context.Background()
	if err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, func() error {
		r.c.enqueue(types.NamespacedName{Namespace: ns, Name: name})
		if _, err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("%s/%s is still there (%v): %w", ns, name, err, cutest.ErrNotYet)
		}
		return nil
	})
	deleteGizmoCRD(t)
}

// A standby's holder, and its kind, go before it is elected: its write loops
// release the hold, and a Secret that a user deleted goes. With the mapper
// that remembers the kind, one that no longer knows it, and
// controller-runtime's; and in PerName mode, whose index loses the line.
func TestAStandbyReleasesTheHoldsOfAKindThatIsGone(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	for _, name := range []string{"LabelScoped", "the mapper forgot", "controller-runtime's mapper", "PerName", "outside the caches"} {
		t.Run(name, func(t *testing.T) {
			var g gizmoOptions
			index := ""
			ns := env.Namespace(t, "gone-standby")
			switch name {
			case "controller-runtime's mapper":
				hc, err := rest.HTTPClientFor(env.Config)
				if err != nil {
					t.Fatal(err)
				}
				if g.mapper, err = apiutil.NewDynamicRESTMapper(env.Config, hc); err != nil {
					t.Fatal(err)
				}
			case "PerName":
				index = env.Namespace(t, "gone-idx")
				g.so.IndexNamespace = index
				g.change = perName
			case "outside the caches":
				g.so.Namespaces = []string{"default"}
			case "the mapper forgot":
				g.mapper = clientsFor(t, env.Config).Mapper
			}
			r := newGizmoRig(t, env.Config, g)
			r.create(t, ns, "g", "s")
			r.removeKind(t, ns, "g")
			if !gizmoHolds(ns, "s") {
				t.Fatal("released before the write loops ran")
			}
			if err := env.Kube.CoreV1().Secrets(ns).Delete(ctx, "s", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if name == "the mapper forgot" {
				g.mapper.(meta.ResettableRESTMapper).Reset()
			}
			r.startWrites(t)
			gizmoReleased(t, time.Minute, ns, "s")
			cutest.Eventually(t, 30*time.Second, func() error {
				if _, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "s", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					return fmt.Errorf("the Secret is still there (%v): %w", err, cutest.ErrNotYet)
				}
				return nil
			})
			if index != "" {
				noGizmoLines(t, index)
			}
		})
	}
}

// noGizmoLines waits until the Gizmos' index has no lines.
func noGizmoLines(t *testing.T, index string) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		cm, err := env.Kube.CoreV1().ConfigMaps(index).Get(context.Background(), cu.IndexName(gizmoGVK.GroupKind()), metav1.GetOptions{})
		if err != nil {
			return err
		}
		if strings.TrimSpace(cm.Data["entries"]) != "" {
			return fmt.Errorf("the index still has %q: %w", cm.Data["entries"], cutest.ErrNotYet)
		}
		return nil
	})
}

// The CRD's GET is refused, as when an uninstall takes the RBAC away first:
// the hold stays, and the log says what to grant. It goes once granted.
func TestAKindThatIsGoneNeedsItsCRDsGet(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	const sa = "cu-crd-gone"
	role := func(name string, rules ...rbacv1.PolicyRule) (bind func()) {
		t.Helper()
		cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
		if _, err := env.Kube.RbacV1().ClusterRoles().Create(ctx, cr, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = env.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{})
			_ = env.Kube.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{})
		})
		return func() {
			t.Helper()
			b := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name},
				Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "default", Name: sa}},
				RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name}}
			if _, err := env.Kube.RbacV1().ClusterRoleBindings().Create(ctx, b, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	crd := rbacv1.PolicyRule{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"}, ResourceNames: []string{gizmoCRD}}
	others, get := crd, crd
	others.Verbs, get.Verbs = []string{"list", "watch", "patch"}, []string{"get"}
	role("cu-crd-gone", others,
		rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "patch"}},
		rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get", "list", "watch"}},
		rbacv1.PolicyRule{APIGroups: []string{cutest.Group}, Resources: []string{"gizmos"}, Verbs: []string{"get", "list", "watch", "patch", "update"}})()
	grantGet := role("cu-crd-gone-get", get)
	grantGet()
	cfg := env.AsServiceAccount("default", sa)
	canGet := func(want bool) {
		t.Helper()
		md := clientsFor(t, cfg).Metadata
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := md.Resource(crdGVR).Get(ctx, gizmoCRD, metav1.GetOptions{}); apierrors.IsForbidden(err) == want {
				return fmt.Errorf("reading the CRD: %v: %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	}
	canGet(true)
	ns := env.Namespace(t, "gone-rbac")
	r := newGizmoRig(t, cfg, gizmoOptions{})
	r.create(t, ns, "g", "s")
	r.removeKind(t, ns, "g")
	if err := env.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, "cu-crd-gone-get", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	canGet(false)
	r.startWrites(t)
	cutest.Eventually(t, time.Minute, func() error {
		if len(r.logs.matching("grant get on the CRD "+gizmoCRD)) < 2 {
			return cutest.ErrNotYet
		}
		return nil
	})
	if !gizmoHolds(ns, "s") {
		t.Fatal("released, though whether the CRD is gone couldn't be read")
	}
	grantGet()
	gizmoReleased(t, time.Minute, ns, "s")
}

// The kind returns between the GET that finds its CRD gone and R3, with a
// holder that holds the Secret: R3's test fails, the next round's list finds
// the holder, and it keeps its hold, until it goes.
func TestAKindThatReturnsBeforeR3KeepsItsHold(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "gone-back")
	var r *gizmoRig
	var once sync.Once
	var patches int
	r = newGizmoRig(t, env.Config, gizmoOptions{before: func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if c.Op != "patch" || c.Name != "s" || c.PatchType != types.JSONPatchType {
				return nil
			}
			patches++ // R3s of one key run one at a time
			once.Do(func() {
				createGizmoCRD(t)
				r.createGizmo(t, ns, "g2", "s")
				f, _ := cu.FinalizerName(gizmoGVK.GroupKind(), ns)
				before := secretEpoch(ns, "s", f)
				cutest.Eventually(t, time.Minute, func() error {
					if e := secretEpoch(ns, "s", f); e == "" || e == before {
						return fmt.Errorf("g2 hasn't held the Secret: %w", cutest.ErrNotYet)
					}
					return nil
				})
			})
			return nil
		}, nil)
	}})
	r.create(t, ns, "g", "s")
	r.removeKind(t, ns, "g")
	r.startWrites(t)
	cutest.Eventually(t, time.Minute, func() error {
		g, err := env.Dynamic.Resource(gizmoGVR).Namespace(ns).Get(context.Background(), "g2", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(g.GetFinalizers(), ownerFinalizer) {
			return cutest.ErrNotYet
		}
		return nil
	})
	for range 30 {
		if !gizmoHolds(ns, "s") {
			t.Fatal("the new holder lost its hold")
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.removeKind(t, ns, "g2")
	gizmoReleased(t, 2*time.Minute, ns, "s")
	if patches < 2 {
		t.Errorf("%d R3s: the one the new holder's A2 broke never ran", patches)
	}
}

// Two processes without leader election, and a kind whose CRD is replaced by
// one of another plural: the old process, whose mapper remembers the first,
// strips none of the holds of the new one's holders, though its list and its
// CRD are gone, and says why. With the new process and the second CRD gone too,
// it releases what is left.
func TestAnotherPluralsHoldsStay(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	ns := env.Namespace(t, "gone-plural")
	oidGVR := schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "gizmoids"}
	old := newGizmoRig(t, env.Config, gizmoOptions{})
	old.create(t, ns, "g", "s")
	old.removeKind(t, ns, "g") // a standby: the hold stays

	crd := cutest.CRD("Gizmo", "gizmoids")
	cutest.Eventually(t, time.Minute, func() error { return env.Client.Create(ctx, crd.DeepCopy()) })
	removeOids := func() {
		t.Helper()
		if l, err := env.Dynamic.Resource(oidGVR).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range l.Items {
				_, _ = env.Dynamic.Resource(oidGVR).Namespace(l.Items[i].GetNamespace()).Patch(ctx, l.Items[i].GetName(),
					types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
			}
		}
		_ = env.Client.Delete(ctx, crd.DeepCopy())
		cutest.Eventually(t, 2*time.Minute, func() error {
			if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, crd.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("the Gizmoid CRD is still there (%v): %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	}
	t.Cleanup(removeOids)
	cutest.Eventually(t, time.Minute, func() error {
		_, err := env.Dynamic.Resource(oidGVR).List(ctx, metav1.ListOptions{})
		return err
	})
	c := newController(t)
	clients := clientsFor(t, env.Config)
	gizmosMapped(t, clients.Mapper, oidGVR)
	s := cu.NewShared(clients, cu.SharedOptions{})
	o := widgetOptions(c.enqueue)
	o.Holder, o.DeletionGrace, o.ProbeInterval = gizmoGVK, testGrace, time.Second
	tr, err := s.Track(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	newCtx, cancelNew := context.WithCancel(ctx)
	ended := make(chan error, 1)
	go func() { ended <- s.Run(newCtx) }()
	var once sync.Once
	stopNew := func() { // and waits: none of its batches runs after it
		once.Do(func() {
			cancelNew()
			if err := <-ended; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("the new process's Run: %v", err)
			}
		})
	}
	t.Cleanup(stopNew)
	cutest.Eventually(t, time.Minute, func() error {
		if !s.HasSynced() {
			return cutest.ErrNotYet
		}
		return nil
	})
	c.start(&driver.Driver{Tracker: tr, Dynamic: env.Dynamic, Holder: oidGVR, OwnerFinalizer: ownerFinalizer})
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": "s"}}}
	u.SetGroupVersionKind(gizmoGVK)
	u.SetNamespace(ns)
	u.SetName("n")
	if _, err := env.Dynamic.Resource(oidGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.enqueue(types.NamespacedName{Namespace: ns, Name: "n"})
	cutest.Eventually(t, time.Minute, func() error {
		g, err := env.Dynamic.Resource(oidGVR).Namespace(ns).Get(ctx, "n", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(g.GetFinalizers(), ownerFinalizer) {
			return cutest.ErrNotYet
		}
		return nil
	})

	old.startWrites(t)
	cutest.Eventually(t, time.Minute, func() error {
		if len(old.logs.matching("gizmoids in "+cutest.Group+"/v1 serves the kind now")) == 0 {
			return fmt.Errorf("the old process doesn't say why: %w", cutest.ErrNotYet)
		}
		return nil
	})
	f, _ := cu.FinalizerName(gizmoGVK.GroupKind(), ns)
	epochs := map[string]bool{}
	for range 100 {
		if !gizmoHolds(ns, "s") {
			t.Fatal("the old process stripped the new holder's hold")
		}
		epochs[secretEpoch(ns, "s", f)] = true
		time.Sleep(100 * time.Millisecond)
	}
	if len(epochs) > 3 {
		t.Errorf("%d epochs in ten seconds: the two processes fight over the hold", len(epochs))
	}

	// The new process is gone, its last batch too, before its holder and CRD:
	// only the old one is left to release, at its next retry.
	c.stop()
	stopNew()
	if !gizmoHolds(ns, "s") {
		t.Fatal("released before the new holder went")
	}
	removeOids()
	gizmoReleased(t, 3*time.Minute, ns, "s")
	cutest.Eventually(t, time.Minute, func() error { // after R3, whose answer it waits for
		if len(old.logs.informed("the holder kind's CRD is gone")) == 0 {
			return fmt.Errorf("the old process didn't release it: %w", cutest.ErrNotYet)
		}
		return nil
	})
}

// What the evidence is, on the wire: a release of a kind that is gone reads its
// CRD after its list's 404, with no resourceVersion, which makes it a
// consistent read and not one of the watch cache (§6.4), and then the group's
// discovery; R3 comes after them.
func TestTheEvidenceOnTheWire(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "gone-wire")
	var mu sync.Mutex
	var reqs []string
	cfg := rest.CopyConfig(env.Config)
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTrip(func(req *http.Request) (*http.Response, error) {
			resp, err := next.RoundTrip(req)
			if err == nil {
				mu.Lock()
				reqs = append(reqs, fmt.Sprintf("%s %s %d", req.Method, req.URL.RequestURI(), resp.StatusCode))
				mu.Unlock()
			}
			return resp, err
		})
	})
	r := newGizmoRig(t, cfg, gizmoOptions{})
	r.create(t, ns, "g", "s")
	r.removeKind(t, ns, "g")
	mu.Lock()
	reqs = nil
	mu.Unlock()
	r.startWrites(t)
	gizmoReleased(t, time.Minute, ns, "s")
	mu.Lock()
	defer mu.Unlock()
	// The round that released: the last list before R3, and what came after it,
	// in order. The list and the CRD's GET as they are, with no other parameter;
	// the discovery's client adds its timeout.
	r3 := slices.IndexFunc(reqs, func(q string) bool { return strings.HasPrefix(q, "PATCH /api/v1/namespaces/"+ns+"/secrets/s ") })
	if r3 < 0 {
		t.Fatalf("no R3; the requests:\n%s", strings.Join(reqs, "\n"))
	}
	list := "GET /apis/" + cutest.Group + "/v1/namespaces/" + ns + "/gizmos?limit=500 404"
	steps := []struct {
		what string
		is   func(q string) bool
	}{
		{"the CRD's GET", func(q string) bool {
			return q == "GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions/"+gizmoCRD+" 404"
		}},
		{"the group", func(q string) bool { return isGET(q, "/apis/"+cutest.Group, 200) }},
		{"its version", func(q string) bool { return isGET(q, "/apis/"+cutest.Group+"/v1", 200) }},
	}
	from := -1
	for i, q := range reqs[:r3] {
		if q == list {
			from = i
		}
	}
	if from < 0 {
		t.Fatalf("no list of the kind before R3 was a 404; the requests:\n%s", strings.Join(reqs, "\n"))
	}
	round := reqs[from+1 : r3]
	for _, s := range steps {
		i := slices.IndexFunc(round, s.is)
		if i < 0 {
			t.Errorf("%s isn't after the list and what comes before it, and before R3; the requests:\n%s", s.what, strings.Join(reqs, "\n"))
			break
		}
		round = round[i+1:]
	}
	for _, q := range reqs {
		if strings.Contains(q, "/customresourcedefinitions/"+gizmoCRD) && strings.Contains(q, "resourceVersion") {
			t.Errorf("the CRD is read from the watch cache: %s", q)
		}
	}
}

// isGET reports whether q, as the test above records it, is a GET of path,
// whatever its parameters, that was answered with code.
func isGET(q, path string, code int) bool {
	uri, ok := strings.CutPrefix(q, "GET ")
	uri, answered := strings.CutSuffix(uri, fmt.Sprintf(" %d", code))
	p, _, _ := strings.Cut(uri, "?")
	return ok && answered && p == path
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// The connection is down while a holder goes, its finalizer stripped as an
// uninstall's cleanup may, and then its CRD. The holder informer never sees
// it: its every list is a 404, so its cache keeps the ghost. The CRD informer
// sees the CRD gone, and what the ghost held is released; its index loses the
// line. The holder informer says once that it fails.
func TestAGhostsHoldIsReleasedWhenItsCRDGoes(t *testing.T) {
	cutest.Require(t, env)
	for _, name := range []string{"LabelScoped", "PerName", "outside the caches"} {
		t.Run(name, func(t *testing.T) {
			var g gizmoOptions
			index := ""
			ns := env.Namespace(t, "gone-ghost")
			switch name {
			case "PerName":
				index = env.Namespace(t, "gone-ghost-idx")
				g.so.IndexNamespace = index
				g.change = perName
			case "outside the caches":
				g.so.Namespaces = []string{"default"}
			}
			cfg, link := lossyConfig(env.Config)
			t.Cleanup(func() { link.set(false) })
			r := newGizmoRig(t, cfg, g)
			r.startWrites(t)
			r.create(t, ns, "g", "s")
			cutest.Eventually(t, time.Minute, func() error { // the ghost-to-be is cached
				if r.tr.CachedHolders() != 1 {
					return cutest.ErrNotYet
				}
				return nil
			})
			link.set(true)
			freeGizmos(t)
			deleteGizmoCRD(t)
			if !gizmoHolds(ns, "s") {
				t.Fatal("released while the connection was down")
			}
			link.set(false)
			gizmoReleased(t, 3*time.Minute, ns, "s")
			if n := r.tr.CachedHolders(); n != 1 {
				t.Errorf("%d holders cached: the ghost left, and the test shows nothing", n)
			}
			if index != "" {
				noGizmoLines(t, index)
			}
			informer := "the holder informer of " + gizmoGVK.GroupKind().String() + " can't list or watch"
			notFound := func() int {
				n := 0
				for _, w := range r.logs.matching(informer) {
					if strings.Contains(w, "its kind isn't served") {
						n++
					}
				}
				return n
			}
			cutest.Eventually(t, time.Minute, func() error {
				if notFound() == 0 {
					return fmt.Errorf("the holder informer's failure isn't logged: %w", cutest.ErrNotYet)
				}
				return nil
			})
			time.Sleep(10 * time.Second) // more retries
			if n := notFound(); n != 1 {
				t.Errorf("the holder informer's failure was logged %d times, want once", n)
			}
		})
	}
}

// While the process is up and connected: the kind goes with its holder, whose
// controller runs, and returns; its tracker serves it as before.
func TestAKindThatGoesAndReturns(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "gone-return")
	r := newGizmoRig(t, env.Config, gizmoOptions{})
	r.startWrites(t)
	r.create(t, ns, "g", "s")
	r.removeKind(t, ns, "g")
	gizmoReleased(t, time.Minute, ns, "s")
	createGizmoCRD(t)
	r.create(t, ns, "g2", "s2")
	for range 20 {
		if !gizmoHolds(ns, "s2") {
			t.Fatal("the returned kind's hold was released")
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.removeKind(t, ns, "g2")
	gizmoReleased(t, 2*time.Minute, ns, "s2")
}

// A holder kind whose CRD goes and returns namespaced, while a process that
// mapped it cluster-scoped runs throughout, its write loops too (§6.2, §6.4):
// the end-to-end check of the list that it makes whole, and filters. A
// holder's deletion queues what it named; R2's list finds the other holder of
// one of them, which stays, and none of the other, which goes. With the filter
// reverted the list in a namespace is empty, and both go. Not checked: an API
// server that still serves the deleted CRD.
func TestAHolderKindThatReturnsNamespacedUnderARunningProcess(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	a, cached := env.Namespace(t, "scope-run"), env.Namespace(t, "scope-run")
	crd := newScopeCRD(t, a)
	crd.create(apiextensionsv1.ClusterScoped)
	// The Secrets of a are outside its caches: R0 settles none of them, and
	// every batch reads the holders live.
	s, old := crd.track(cu.SharedOptions{Namespaces: []string{cached}})
	running(t, s)
	used, unused := cu.SecretRef(a, "a-used"), cu.SecretRef(a, "b-unused")
	crd.remove()
	crd.gone(old, used, a)
	crd.create(apiextensionsv1.NamespaceScoped)
	crd.holder(a, "h", used)
	// h2 names both, as a holder pass's A1 records them before its A2s.
	rec, err := cu.EncodeRecord([]cu.ID{used.ID(), unused.ID()})
	if err != nil {
		t.Fatal(err)
	}
	h2 := &unstructured.Unstructured{Object: map[string]any{}}
	h2.SetGroupVersionKind(crd.kind())
	h2.SetNamespace(a)
	h2.SetName("h2")
	h2.SetFinalizers([]string{ownerFinalizer})
	h2.SetAnnotations(map[string]string{cu.AnnotationRefs: rec})
	if _, err := crd.at(a).Create(ctx, h2, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	holdSecret(t, crd.kind().GroupKind(), a, "a-used")
	holdSecret(t, crd.kind().GroupKind(), a, "b-unused")
	// The old process's holder informer, which lists the kind in every
	// namespace, has both again after its 404s.
	cutest.Eventually(t, 2*time.Minute, func() error {
		if n := old.CachedHolders(); n != 2 {
			return fmt.Errorf("the holder cache has %d holders: %w", n, cutest.ErrNotYet)
		}
		return nil
	})
	if _, err := crd.at(a).Patch(ctx, "h2", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := crd.at(a).Delete(ctx, "h2", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 2*time.Minute, func() error {
		if f := finalizersOf(t, a, "b-unused"); len(f) != 0 {
			return fmt.Errorf("the unused Secret's finalizers are %v: %w", f, cutest.ErrNotYet)
		}
		return nil
	})
	for range 30 {
		if f := finalizersOf(t, a, "a-used"); len(f) != 1 {
			t.Fatalf("the used Secret's finalizers are %v, want its hold", f)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := crd.at(a).Patch(ctx, "h", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := crd.at(a).Delete(ctx, "h", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, func() error {
		if f := finalizersOf(t, a, "a-used"); len(f) != 0 {
			return fmt.Errorf("the used Secret's finalizers are %v: %w", f, cutest.ErrNotYet)
		}
		return nil
	})
}

func secretEpoch(ns, name, f string) string {
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return s.Annotations[f]
}
