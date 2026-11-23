package credentialuse_test

// The write loops against envtest (plan, PR 3b), driven by the §7.2 driver
// as a provider's controller would drive it: the krateo-057 teardown and its
// control, U5 and the grace, the last-step reads, a deleting user, a changed
// ref, two RefsFunc versions, and the namespace-restricted caches.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// testGrace is the grace in these tests: short, so that they wait little,
// and long enough that a pass started at its beginning ends well before it.
const testGrace = 3 * time.Second

// resetPublished removes the holder kind's published settings, so that the
// next Track publishes its own: as editing the annotation and restarting
// every process of the kind would (§7.3). These tests use a shorter grace
// than the default the earlier tests published.
func resetPublished(t *testing.T, gvr schema.GroupVersionResource) {
	t.Helper()
	crds := env.Dynamic.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"})
	_, err := crds.Patch(context.Background(), gvr.Resource+"."+gvr.Group, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+cu.AnnotationConfig+`":null}}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

// controller drives a driver as a provider's controller would: it runs a
// pass for each holder enqueued, through the tracker's Enqueue or a test's
// kick, and requeues a pass that failed or asked to, after a short delay.
// Every Connect failure is recorded, with whether the holder was being
// deleted then.
type controller struct {
	t *testing.T
	d *driver.Driver
	q chan types.NamespacedName

	mu       sync.Mutex
	failures []string // Connect failures of holders being deleted: §14's I1
	lastErr  map[types.NamespacedName]string
	last     map[types.NamespacedName]driver.Result
	failed   map[types.NamespacedName]int
	passes   atomic.Int64
	// beforeConnect, if set, runs before each Connect's read.
	beforeConnect atomic.Pointer[func(ctx context.Context, holder types.NamespacedName, ref cu.Ref)]
	stop          func() // ends the controller, and waits for its pass
	cancel        func() // ends it, and doesn't wait: from inside a pass
}

func newController(t *testing.T) *controller {
	return &controller{t: t, q: make(chan types.NamespacedName, 10000), lastErr: map[types.NamespacedName]string{},
		last: map[types.NamespacedName]driver.Result{}, failed: map[types.NamespacedName]int{}}
}

func (c *controller) enqueue(nn types.NamespacedName) {
	select {
	case c.q <- nn:
	default:
	}
}

// connect is the driver's Connect: it reads the Secret the ref names, and
// records a failure of a holder being deleted.
func (c *controller) connect(ctx context.Context, holder types.NamespacedName, ref cu.Ref) error {
	if f := c.beforeConnect.Load(); f != nil {
		(*f)(ctx, holder, ref)
	}
	var err error
	switch ref.GroupKind {
	case cutest.TargetGVK.GroupKind():
		// A chain's intermediate object (§9): read as a provider's Connect
		// reads a KubernetesTarget.
		_, err = env.Dynamic.Resource(cutest.TargetGVR).Namespace(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case schema.GroupKind{Kind: "ConfigMap"}:
		_, err = env.Kube.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	default:
		_, err = env.Kube.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	}
	if err != nil {
		h, herr := env.Dynamic.Resource(c.d.Holder).Namespace(holder.Namespace).Get(ctx, holder.Name, metav1.GetOptions{})
		if herr == nil && h.GetDeletionTimestamp() != nil {
			c.mu.Lock()
			c.failures = append(c.failures, fmt.Sprintf("%s read %s/%s while being deleted: %v", holder, ref.Namespace, ref.Name, err))
			c.mu.Unlock()
		}
	}
	return err
}

// start runs the controller until the test ends.
func (c *controller) start(d *driver.Driver) {
	c.d = d
	if d.Connect == nil {
		d.Connect = c.connect
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	c.cancel = cancel
	c.stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	c.t.Cleanup(c.stop)
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case nn := <-c.q:
				c.passes.Add(1)
				res, err := d.Reconcile(ctx, nn)
				c.mu.Lock()
				retry := 100 * time.Millisecond
				if err != nil {
					c.lastErr[nn] = err.Error()
					// A pass that keeps failing backs off, to 2s.
					c.failed[nn]++
					retry = min(retry<<min(c.failed[nn], 5), 2*time.Second)
				} else {
					delete(c.lastErr, nn)
					delete(c.failed, nn)
				}
				c.last[nn] = res
				c.mu.Unlock()
				if err != nil || res.Requeue {
					time.AfterFunc(retry, func() { c.enqueue(nn) })
				}
			}
		}
	}()
}

func (c *controller) violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.failures)
}

func (c *controller) lastResult(nn types.NamespacedName) driver.Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last[nn]
}

func (c *controller) errorOf(nn types.NamespacedName) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr[nn]
}

// writeRig is a Shared with a tracker for Widgets, running informers and
// write loops, and a controller driving the Widgets.
type writeRig struct {
	s      *cu.Shared
	tr     *cu.Tracker
	c      *controller
	events *events.FakeRecorder
	stopS  context.CancelFunc

	mu   sync.Mutex
	seen []string // every event recorded
}

// stop ends the rig's process: its controller, informers and write loops.
func (r *writeRig) stop() {
	r.c.stop()
	r.stopS()
}

// kill ends the rig's process from inside one of its calls, as a crash
// would: everything it runs sees its context end, and nothing waits.
func (r *writeRig) kill() {
	r.c.cancel()
	r.stopS()
}

func newWriteRig(t *testing.T, cfg *rest.Config, so cu.SharedOptions, change func(o *cu.Options), before func(*cu.Shared)) *writeRig {
	t.Helper()
	resetPublished(t, cutest.WidgetGVR)
	// Registered first, so it runs last, after the rig has stopped: a test
	// leaves no published settings behind for the next (§7.3).
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	r := &writeRig{c: newController(t), events: events.NewFakeRecorder(100000)}
	clients := clientsFor(t, cfg)
	clients.Events = r.events
	r.s = cu.NewShared(clients, so)
	r.s.SetLandingMargin(0) // disownOptions' passes are short
	o := widgetOptions(r.c.enqueue)
	o.DeletionGrace = testGrace
	o.ProbeInterval = time.Second
	if change != nil {
		change(&o)
	}
	if before != nil {
		before(r.s)
	}
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	r.tr = tr
	r.stopS = running(t, r.s)
	r.c.start(newDriver(tr))
	// Keep every event, so that the recorder never blocks the core.
	go func() {
		for e := range r.events.Events {
			r.mu.Lock()
			r.seen = append(r.seen, e)
			r.mu.Unlock()
		}
	}()
	return r
}

func widgetNN(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}

// established waits until the Widget has the owner finalizer and its record,
// and the Secret is held for its namespace.
func established(t *testing.T, ns string, secret string, widgets ...string) {
	t.Helper()
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		for _, w := range widgets {
			obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), w, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if !hasString(obj.GetFinalizers(), ownerFinalizer) || obj.GetAnnotations()[cu.AnnotationRefs] == "" {
				return fmt.Errorf("widget %s not established: %w", w, cutest.ErrNotYet)
			}
		}
		s, err := secrets(env.Metadata, ns).Get(context.Background(), secret, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !hasString(s.Finalizers, f) || s.Annotations[f] == "" || s.Labels[cu.LabelHeld] != "true" {
			return fmt.Errorf("secret %s not held: %w", secret, cutest.ErrNotYet)
		}
		return nil
	})
}

func deleteWidget(t *testing.T, ns, name string) {
	t.Helper()
	if err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func deleteSecret(t *testing.T, ns, name string) {
	t.Helper()
	if err := env.Kube.CoreV1().Secrets(ns).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

// gone waits until get reports NotFound.
func gone(t *testing.T, timeout time.Duration, what string, get func() error) {
	t.Helper()
	cutest.Eventually(t, timeout, func() error {
		err := get()
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("%s still exists: %w", what, cutest.ErrNotYet)
	})
}

func secretGet(ns, name string) func() error {
	return func() error {
		_, err := env.Kube.CoreV1().Secrets(ns).Get(context.Background(), name, metav1.GetOptions{})
		return err
	}
}

func widgetGet(ns, name string) func() error {
	return func() error {
		_, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		return err
	}
}

// The krateo-057 teardown (§14), at the core level: a namespace is deleted,
// and its Secret is deleted before the Widgets that read it. The Secret
// stays, readable, until both Widgets have finished deleting, and no
// Widget's deletion pass ever fails to read it; then the Secret goes, and
// the namespace can be finalized. The control, the same sequence without
// credential-use, wedges: that proves the test reproduces the defect.
func TestNamespaceTeardown(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	teardown := func(t *testing.T, c *controller, ns string) {
		// 2. Delete the namespace: it terminates, and refuses new objects.
		if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		n, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if err != nil || n.DeletionTimestamp == nil {
			t.Fatalf("namespace %s after its deletion: %v, %v; want it terminating", ns, n, err)
		}
		late := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "late"}}
		if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, late, metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
			t.Fatalf("creating a Secret in the terminating namespace: %v; want Forbidden (is NamespaceLifecycle on?)", err)
		}
		// 3. The Secret first, then both Widgets, as the namespace
		// controller does on krateo-057.
		deleteSecret(t, ns, "creds")
		for _, w := range []string{"w1", "w2"} {
			deleteWidget(t, ns, w)
			c.enqueue(widgetNN(ns, w))
		}
	}

	t.Run("with the tracker", func(t *testing.T) {
		r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
		ns := env.Namespace(t, "teardown")
		// 1. A namespace, a Secret, and two Widgets naming it.
		createSecret(t, ns, "creds")
		for _, w := range []string{"w1", "w2"} {
			createWidget(t, ns, w, "creds")
			r.c.enqueue(widgetNN(ns, w))
		}
		established(t, ns, "creds", "w1", "w2")

		teardown(t, r.c, ns)
		s, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "creds", metav1.GetOptions{})
		if err != nil || s.DeletionTimestamp == nil {
			t.Fatalf("the Secret after its deletion: %v; want it kept, being deleted", err)
		}
		gone(t, time.Minute, "a Widget", widgetGet(ns, "w1"))
		gone(t, time.Minute, "a Widget", widgetGet(ns, "w2"))
		gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
		events := r.c.d.Events()
		for _, w := range []string{"w1", "w2"} {
			if !slices.Contains(events, "delete "+ns+"/"+w) {
				t.Errorf("no external Delete for %s: %v", w, events)
			}
		}
		if v := r.c.violations(); len(v) != 0 {
			t.Errorf("a Widget failed to read its credential while being deleted: %v", v)
		}
		// 4. The namespace controller's last step.
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
		gone(t, time.Minute, "the namespace", func() error {
			_, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
			return err
		})
	})

	t.Run("control: without the tracker it wedges", func(t *testing.T) {
		c := newController(t)
		c.start(&driver.Driver{Dynamic: env.Dynamic, Holder: cutest.WidgetGVR, OwnerFinalizer: ownerFinalizer, Refs: secretRefs})
		ns := env.Namespace(t, "control")
		createSecret(t, ns, "creds")
		for _, w := range []string{"w1", "w2"} {
			createWidget(t, ns, w, "creds")
			c.enqueue(widgetNN(ns, w))
		}
		cutest.Eventually(t, time.Minute, func() error {
			for _, w := range []string{"w1", "w2"} {
				obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, w, metav1.GetOptions{})
				if err != nil || !hasString(obj.GetFinalizers(), ownerFinalizer) {
					return fmt.Errorf("widget %s not established (%v): %w", w, err, cutest.ErrNotYet)
				}
			}
			return nil
		})
		teardown(t, c, ns)
		gone(t, 10*time.Second, "the Secret", secretGet(ns, "creds"))
		// The Widgets can't finish: each deletion pass fails to read the
		// Secret, as on krateo-057.
		cutest.Eventually(t, 30*time.Second, func() error {
			for _, w := range []string{"w1", "w2"} {
				if !strings.Contains(c.errorOf(widgetNN(ns, w)), "not found") {
					return fmt.Errorf("widget %s's last pass: %q: %w", w, c.errorOf(widgetNN(ns, w)), cutest.ErrNotYet)
				}
			}
			return nil
		})
		time.Sleep(2 * time.Second) // passes keep failing; nothing will change
		for _, w := range []string{"w1", "w2"} {
			if err := widgetGet(ns, w)(); err != nil {
				t.Errorf("widget %s: %v; want it wedged", w, err)
			}
			removeFinalizers(t, ns, w)
		}
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
	})
}

// U5 and the grace (§5): a Secret deleted in a live namespace, with live
// users and none deleting, is kept for the grace, so that recreating it
// fails with AlreadyExists, and then released; recreated, it is acquired
// by the holders through the prober, under its new UID. A held Secret with
// no users is released at once when it is deleted.
func TestU5AndTheGrace(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	var lonePending atomic.Bool
	loneDeleted := make(chan struct{})
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, func(s *cu.Shared) {
		s.SetReleaseWait(nil) // the test times a release against its grace
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			// Hold every batch's read of "lone" until the test has deleted
			// it, so that the batch decides on the deleted Secret.
			if c.Op == "get" && c.GK.Kind == "Secret" && c.Name == "lone" && lonePending.Load() {
				select {
				case <-loneDeleted:
				case <-ctx.Done():
				case <-time.After(30 * time.Second):
				}
			}
			return nil
		}, nil)
	})
	ns := env.Namespace(t, "u5")
	createSecret(t, ns, "creds")
	for _, w := range []string{"w1", "w2"} {
		createWidget(t, ns, w, "creds")
		r.c.enqueue(widgetNN(ns, w))
	}
	established(t, ns, "creds", "w1", "w2")
	old, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "creds", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	deleted := time.Now()
	deleteSecret(t, ns, "creds")
	again := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds"}}
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, again, metav1.CreateOptions{}); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("recreating the Secret during its grace: %v; want AlreadyExists", err)
	}
	gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
	if waited := time.Since(deleted); waited < testGrace {
		t.Errorf("the Secret went after %s, inside its grace of %s", waited, testGrace)
	}

	// The Secret left the cache, so its holders were enqueued, and their
	// passes found it Missing. Only the prober enqueues them again.
	cutest.Eventually(t, time.Minute, func() error {
		for _, w := range []string{"w1", "w2"} {
			res := r.c.lastResult(widgetNN(ns, w))
			if !slices.ContainsFunc(res.Acquired.Unheld, func(u cu.Unheld) bool { return u.Reason == cu.ReasonMissing }) {
				return fmt.Errorf("%s's last pass didn't find the Secret Missing: %w", w, cutest.ErrNotYet)
			}
		}
		return nil
	})
	// Rotation: the Secret comes back, unlabelled; the prober finds it.
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, again, metav1.CreateOptions{}); err != nil {
		t.Fatalf("recreating the Secret after its grace: %v", err)
	}
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		s, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "creds", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if s.UID == old.UID || !hasString(s.Finalizers, f) {
			return fmt.Errorf("the new Secret isn't held yet: %w", cutest.ErrNotYet)
		}
		return nil
	})

	// A hold with no users, as a crash between a holder's Forget and R3
	// leaves: deleted, it is released at once, not after a grace. Another
	// finalizer keeps the Secret, so that the release is seen.
	lonePending.Store(true)
	createSecret(t, ns, "lone", f, "example.com/keep")
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, "lone", types.MergePatchType, []byte(`{"metadata":{"labels":{"`+
		cu.LabelHeld+`":"true","`+cu.MarkerLabel(widgetGK)+`":"true"},"annotations":{"`+f+`":"e0"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	deleteSecret(t, ns, "lone")
	close(loneDeleted)
	cutest.Eventually(t, time.Minute, func() error {
		s, err := secrets(env.Metadata, ns).Get(ctx, "lone", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if hasString(s.Finalizers, f) {
			return fmt.Errorf("lone still held: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if d := time.Since(start); d >= testGrace {
		t.Errorf("a Secret with no users took %s to be released, not at once", d)
	}
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(ctx, "lone", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

// keptFor waits until the recorder has a WaitingForCredentialUsers event
// whose note says why.
func (r *writeRig) keptFor(t *testing.T, ns string, why cu.KeptReason) {
	t.Helper()
	cutest.Eventually(t, time.Minute, func() error {
		if r.sawEvent(cu.EventWaitingForUsers, "in namespace "+ns, string(why)) {
			return nil
		}
		return fmt.Errorf("no %s event for %q: %w", cu.EventWaitingForUsers, why, cutest.ErrNotYet)
	})
}

// sawEvent reports whether an event with reason and every one of parts in
// its text was recorded.
func (r *writeRig) sawEvent(reason string, parts ...string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return sawIn(r.seen, reason, parts...)
}

func sawIn(events []string, reason string, parts ...string) bool {
	for _, e := range events {
		if !strings.Contains(e, reason) {
			continue
		}
		all := true
		for _, p := range parts {
			all = all && strings.Contains(e, p)
		}
		if all {
			return true
		}
	}
	return false
}

// §6.4's last-step reads. A namespace deletion that starts just after the
// grace, before R2's live read of the Namespace, keeps the Secret for its
// users: the read is the last step, not an earlier one.
func TestANamespaceDeletionJustAfterTheGrace(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	var ns string
	var deletedNS atomic.Bool
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			// R2's read of the Namespace, once the grace is over: the
			// namespace starts deleting just before it.
			if c.Op == "get" && c.GK.Kind == "Namespace" && c.Name == ns && deletedNS.CompareAndSwap(false, true) {
				return env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
			}
			return nil
		}, nil)
	})
	ns = env.Namespace(t, "lastread")
	createSecret(t, ns, "creds")
	createWidget(t, ns, "w", "creds")
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "creds", "w")

	deleteSecret(t, ns, "creds")
	r.keptFor(t, ns, cu.KeptNamespaceDeleting)
	if !deletedNS.Load() {
		t.Fatal("the namespace was never read")
	}
	if err := secretGet(ns, "creds")(); err != nil {
		t.Fatalf("the Secret after the grace, its namespace deleting: %v; want it kept for its user", err)
	}
	deleteWidget(t, ns, "w")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
	gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
	if v := r.c.violations(); len(v) != 0 {
		t.Errorf("violations: %v", v)
	}
	if err := env.FinalizeNamespace(ctx, ns); err != nil {
		t.Fatal(err)
	}
}

// §6.4, the holder deletion between R2 and R3. After the grace, the batch
// decides to release the Secret, which has a live user; just before R3, the
// user starts deleting, and its deletion pass runs A2, which can't be
// skipped, up to its Connect. R3's test of the epoch then fails, the batch
// goes back to R1, and keeps the Secret for its deleting user, whose Connect
// reads it. Then the user goes, and the Secret with it.
func TestAHolderDeletionBetweenR2AndR3(t *testing.T) {
	cutest.Require(t, env)
	// Before the rig: its call hook reads ns on the write loops' goroutines.
	ns := env.Namespace(t, "r2r3")
	var r *writeRig
	var armed, fired atomic.Bool
	a2Done, r3Done, r2Again := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var r3Err atomic.Value
	var r3Once, r2Once sync.Once
	isR3 := func(c cu.Call) bool {
		return c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == ns && c.PatchType == types.JSONPatchType
	}
	wait := func(ch chan struct{}) {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
		}
	}
	r = newWriteRig(t, env.Config, cu.SharedOptions{}, nil, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			if !armed.Load() || !isR3(c) || !fired.CompareAndSwap(false, true) {
				return nil
			}
			// Just before R3: the user starts deleting, and its pass runs
			// its A2, in the controller.
			if err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Delete(ctx, "w", metav1.DeleteOptions{}); err != nil {
				return err
			}
			r.c.enqueue(widgetNN(ns, "w"))
			wait(a2Done)
			return nil
		}, func(c cu.Call, err error) {
			switch {
			case fired.Load() && isR3(c):
				r3Once.Do(func() {
					r3Err.Store(fmt.Sprint(err))
					close(r3Done)
				})
			case c.Op == "list" && c.GK == widgetGK && c.Namespace == ns && isClosed(r3Done):
				r2Once.Do(func() { close(r2Again) })
			}
		})
	})
	createSecret(t, ns, "creds")
	createWidget(t, ns, "w", "creds")
	hold := func(context.Context, types.NamespacedName, cu.Ref) {
		if !fired.Load() || isClosed(a2Done) {
			return
		}
		// The user's deletion pass has run A2: R3 runs now, before its
		// read, and the batch lists the holders again.
		close(a2Done)
		wait(r3Done)
		wait(r2Again)
	}
	r.c.beforeConnect.Store(&hold)
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "creds", "w")

	armed.Store(true)
	deleteSecret(t, ns, "creds")
	gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
	gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
	if !fired.Load() {
		t.Fatal("R3 never ran")
	}
	if e, _ := r3Err.Load().(string); e == "" || e == "<nil>" {
		t.Errorf("R3 after the user's A2 = %q; want its test to fail", e)
	}
	if v := r.c.violations(); len(v) != 0 {
		t.Errorf("the deleting user failed to read its credential: %v", v)
	}
	if !r.sawEvent(cu.EventWaitingForUsers, string(cu.KeptUserDeleting)) {
		t.Error("the batch after the failed R3 didn't keep the Secret for its deleting user")
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// A Secret deleted in a live namespace while a user is being deleted is
// kept until that user goes, past the grace (§5).
func TestADeletingUserKeepsTheSecretPastTheGrace(t *testing.T) {
	cutest.Require(t, env)
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	ns := env.Namespace(t, "deluser")
	createSecret(t, ns, "creds")
	createWidget(t, ns, "w", "creds")
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "creds", "w")

	// The user starts deleting, and isn't reconciled: its deletion stays
	// pending, as a holder's whose external delete is slow.
	deleteWidget(t, ns, "w")
	deleteSecret(t, ns, "creds")
	r.keptFor(t, ns, cu.KeptUserDeleting)
	time.Sleep(testGrace + time.Second) // past the grace
	if err := secretGet(ns, "creds")(); err != nil {
		t.Fatalf("the Secret past its grace, with a deleting user: %v; want it kept", err)
	}
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
	gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
}

// A ref changed on update releases the old Secret (§11.1).
func TestAChangedRefReleasesTheOldSecret(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	ns := env.Namespace(t, "changed")
	createSecret(t, ns, "old")
	createSecret(t, ns, "new")
	createWidget(t, ns, "w", "old")
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "old", "w")

	w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(w.Object, "new", "spec", "secret")
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Update(ctx, w, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.c.enqueue(widgetNN(ns, "w"))
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		o, err := secrets(env.Metadata, ns).Get(ctx, "old", metav1.GetOptions{})
		if err != nil {
			return err
		}
		n, err := secrets(env.Metadata, ns).Get(ctx, "new", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if hasString(o.Finalizers, f) || !hasString(n.Finalizers, f) {
			return fmt.Errorf("old held %v, new held %v: %w", hasString(o.Finalizers, f), hasString(n.Finalizers, f), cutest.ErrNotYet)
		}
		return nil
	})
}

// Two RefsFunc versions of one kind, as during a rollout: two processes
// reconcile the same Widgets, one naming spec.secret only, the other
// spec.secret and spec.extra. Each pass rewrites the record to its own
// version, so the holds churn; that is measured and reported. Safety
// holds throughout: no pass's Connect fails on a ref it held. When the
// rollout ends, the hold follows the version that remains.
func TestTwoRefsFuncVersions(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	refsV2 := func(obj metav1.Object) ([]cu.Ref, error) {
		refs, err := secretRefs(obj)
		if err != nil {
			return nil, err
		}
		u := obj.(*unstructured.Unstructured)
		if extra, _, _ := unstructured.NestedString(u.Object, "spec", "extra"); extra != "" {
			refs = append(refs, cu.SecretRef(obj.GetNamespace(), extra))
		}
		return refs, nil
	}
	type counts struct{ records, holds, releases atomic.Int64 }
	var churn counts
	count := func(s *cu.Shared) {
		s.HookCalls(nil, func(c cu.Call, err error) {
			if err != nil || c.Op != "patch" {
				return
			}
			switch {
			case c.GK == widgetGK:
				churn.records.Add(1)
			case c.GK.Kind == "Secret" && c.PatchType == types.StrategicMergePatchType:
				churn.holds.Add(1)
			case c.GK.Kind == "Secret" && c.PatchType == types.JSONPatchType:
				churn.releases.Add(1)
			}
		})
	}
	v1 := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, count)
	v2 := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) { o.Refs = refsV2 }, count)
	ns := env.Namespace(t, "versions")
	createSecret(t, ns, "a")
	createSecret(t, ns, "b")
	var names []string
	for i := range 3 {
		name := fmt.Sprintf("w%d", i)
		names = append(names, name)
		u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": "a", "extra": "b"}}}
		u.SetGroupVersionKind(cutest.WidgetGVK)
		u.SetNamespace(ns)
		u.SetName(name)
		if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// The rollout: each Widget's passes alternate between the versions.
	const rounds = 10
	var conflicts int
	for range rounds {
		for _, rig := range []*writeRig{v1, v2} {
			for _, n := range names {
				// Each process's controller runs passes too; a pass that
				// meets one of them conflicts, and the next round retries.
				if _, err := rig.c.d.Reconcile(ctx, widgetNN(ns, n)); err != nil {
					if !apierrors.IsConflict(err) {
						t.Fatalf("pass of %s: %v", n, err)
					}
					conflicts++
				}
			}
		}
	}
	t.Logf("churn over %d rounds of %d Widgets, alternating versions: %d record writes, %d holds written, %d releases (%d passes conflicted)",
		rounds, len(names), churn.records.Load(), churn.holds.Load(), churn.releases.Load(), conflicts)
	if v := append(v1.c.violations(), v2.c.violations()...); len(v) != 0 {
		t.Errorf("violations: %v", v)
	}

	// The rollout ends on v1: the v2 process stops, v1's passes rewrite
	// every record, and b, named by none, is released.
	v2.stop()
	for _, n := range names {
		v1.c.enqueue(widgetNN(ns, n))
	}
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		a, err := secrets(env.Metadata, ns).Get(ctx, "a", metav1.GetOptions{})
		if err != nil {
			return err
		}
		b, err := secrets(env.Metadata, ns).Get(ctx, "b", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !hasString(a.Finalizers, f) || hasString(b.Finalizers, f) {
			return fmt.Errorf("a held %v, b held %v: %w", hasString(a.Finalizers, f), hasString(b.Finalizers, f), cutest.ErrNotYet)
		}
		return nil
	})
}

// SharedOptions.Namespaces (§10.5): a ref outside the cached namespaces is
// read live on every pass; where even that is denied (RBAC), the acquire
// fails closed for a new holder and open for an established one.
func TestNamespaceRestrictedLiveRead(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	in, out, denied := env.Namespace(t, "nsin"), env.Namespace(t, "nsout"), env.Namespace(t, "nsdenied")
	sa := fmt.Sprintf("cu-restricted-%d", time.Now().UnixNano()%100000)
	cluster := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: sa},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{cutest.WidgetGVR.Group}, Resources: []string{cutest.WidgetGVR.Resource}, Verbs: []string{"get", "list", "watch", "patch", "update"}},
			{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"}, Verbs: []string{"get", "patch"}},
			// The Disown switch's informer: list and watch on the one CRD,
			// by name (§7.4).
			{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"},
				ResourceNames: []string{cutest.WidgetGVR.Resource + "." + cutest.WidgetGVR.Group}, Verbs: []string{"list", "watch"}},
			{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"selfsubjectaccessreviews"}, Verbs: []string{"create"}},
			// A restricted process rechecks the held Secrets outside its
			// caches with a list in every namespace (§10.5), so it needs
			// list cluster-wide; it still can't read or write them in
			// denied.
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"list"}},
		},
	}
	if _, err := env.Kube.RbacV1().ClusterRoles().Create(ctx, cluster, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	subject := []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "default", Name: sa}}
	if _, err := env.Kube.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: sa},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: sa},
		Subjects:   subject,
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Secrets: watch where they are cached, and the protocol's verbs in in
	// and out, none in denied.
	for _, ns := range []string{in, out} {
		verbs := []string{"get", "patch"}
		if ns == in {
			verbs = append(verbs, "watch")
		}
		role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: ns},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: verbs}}}
		if _, err := env.Kube.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := env.Kube.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: ns},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: sa},
			Subjects:   subject,
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var outPatches atomic.Int64
	r := newWriteRig(t, env.AsServiceAccount("default", sa), cu.SharedOptions{Namespaces: []string{in}}, nil, func(s *cu.Shared) {
		s.HookCalls(nil, func(c cu.Call, err error) {
			if c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == out && err == nil {
				outPatches.Add(1)
			}
		})
	})
	for _, ns := range []string{in, out, denied} {
		createSecret(t, ns, "creds")
		createWidget(t, ns, "w", "creds")
	}
	// Outside the caches: held, read live on every pass, which never skips.
	d := newDriver(r.tr)
	for range 3 {
		if res := pass(t, d, widgetNN(out, "w")); len(res.Acquired.Held) != 1 {
			t.Fatalf("a pass outside the caches held %v, want the Secret", res.Acquired.Held)
		}
	}
	if n := outPatches.Load(); n < 3 {
		t.Errorf("%d writes to the Secret outside the caches over 3 passes, want one per pass: it isn't cached, so no pass can skip", n)
	}
	// Inside: held, and later passes skip.
	pass(t, d, widgetNN(in, "w"))
	settled(t, d, widgetNN(in, "w"), "creds")

	// Denied: a new holder must not connect; an established one proceeds,
	// degraded.
	res := pass(t, d, widgetNN(denied, "w"))
	if !res.Acquired.MustNotConnect || res.Connected {
		t.Errorf("a new holder whose ref RBAC denies: %+v; want MustNotConnect", res)
	}
	createWidget(t, denied, "established", "creds", ownerFinalizer)
	obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(denied).Get(ctx, "established", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.tr.Acquire(ctx, obj)
	if err != nil || !a.Degraded || a.MustNotConnect {
		t.Errorf("an established holder whose ref RBAC denies: %+v, %v; want Degraded", a, err)
	}
	removeFinalizers(t, denied, "established")
}
