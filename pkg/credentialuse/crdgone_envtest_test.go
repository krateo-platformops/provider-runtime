package credentialuse_test

// A holder kind whose CRD goes while its tracker runs (§6.4, "The holder kind
// itself gone"), against envtest: what the API server answers, and what
// becomes of the kind's holds.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
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
	cutest.Eventually(t, time.Minute, func() error { // the mapper must know the new kind
		if rm, ok := clients.Mapper.(meta.ResettableRESTMapper); ok {
			rm.Reset()
		}
		var err error
		r.tr, err = r.s.Track(context.Background(), o)
		return err
	})
	runningInformers(t, r.s)
	r.d = &driver.Driver{Tracker: r.tr, Dynamic: env.Dynamic, Holder: gizmoGVR, OwnerFinalizer: ownerFinalizer}
	r.c.start(r.d)
	return r
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

func secretEpoch(ns, name, f string) string {
	s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return s.Annotations[f]
}
