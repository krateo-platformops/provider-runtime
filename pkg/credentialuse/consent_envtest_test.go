package credentialuse_test

// The cross-namespace consent gate against envtest (plan, PR 5; §11.3).

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// crossRefs is a Widget's RefsFunc whose spec.secret may be
// "<namespace>/<name>", a Secret in another namespace.
func crossRefs(obj metav1.Object) ([]cu.Ref, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("a %T, not an unstructured Widget", obj)
	}
	v, _, err := unstructured.NestedString(u.Object, "spec", "secret")
	if err != nil || v == "" {
		return nil, err
	}
	ns, name := obj.GetNamespace(), v
	if i := strings.IndexByte(v, '/'); i >= 0 {
		ns, name = v[:i], v[i+1:]
	}
	return []cu.Ref{cu.SecretRef(ns, name)}, nil
}

func setSecretConsent(t *testing.T, ns, name, value string) {
	t.Helper()
	var v any = value
	if value == "" {
		v = nil
	}
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{cu.AnnotationAllowHoldersFrom: v}}})
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(context.Background(), name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

func heldFor(ns, name, holderNS string) func() error {
	return func() error {
		f, _ := cu.FinalizerName(widgetGK, holderNS)
		s, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !hasString(s.Finalizers, f) {
			return fmt.Errorf("%s/%s isn't held for %s: %w", ns, name, holderNS, cutest.ErrNotYet)
		}
		return nil
	}
}

func notHeldFor(ns, name, holderNS string) func() error {
	return func() error {
		if heldFor(ns, name, holderNS)() == nil {
			return fmt.Errorf("%s/%s is still held for %s: %w", ns, name, holderNS, cutest.ErrNotYet)
		}
		return nil
	}
}

// Withdrawal releases the hold through the normal protocol (§11.3): the
// change to the Secret's consent reaches the holder, whose next pass drops
// the ref, and the release batch takes the finalizer off.
func TestConsentWithdrawalReleasesTheHold(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) { o.Refs = crossRefs }, nil)
	home, target := env.Namespace(t, "consent-home"), env.Namespace(t, "consent-target")
	createSecret(t, target, "s")
	setSecretConsent(t, target, "s", home)
	createWidget(t, home, "w", target+"/s")
	r.c.enqueue(widgetNN(home, "w"))
	cutest.Eventually(t, time.Minute, heldFor(target, "s", home))

	setSecretConsent(t, target, "s", "")
	cutest.Eventually(t, time.Minute, notHeldFor(target, "s", home))
	cutest.Eventually(t, time.Minute, func() error {
		obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(home).Get(ctx, "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		u := cu.ReadUnheld(obj.GetAnnotations())
		if cu.ReadRecord(obj).Names(cu.SecretRef(target, "s").ID()) || len(u) != 1 || u[0].Reason != cu.UnheldConsentWithdrawn {
			return fmt.Errorf("record %v, unheld %+v: %w", cu.ReadRecord(obj).IDs, u, cutest.ErrNotYet)
		}
		return nil
	})
	if !r.sawEvent(cu.EventConsentWithdrawn, "no longer allowed") {
		t.Error("no CredentialConsentWithdrawn event")
	}
	// The holder still connects, as today.
	if !slices.Contains(r.c.d.Events(), "connect "+home+"/w") {
		t.Errorf("the holder never connected: %v", r.c.d.Events())
	}
}

// A held cross-namespace user keeps a Secret whose namespace is terminating
// until that user goes (§11.3, the last paragraph).
func TestACrossNamespaceUserKeepsASecretInATerminatingNamespace(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) { o.Refs = crossRefs }, nil)
	home, target := env.Namespace(t, "xns-home"), env.Namespace(t, "xns-target")
	createSecret(t, target, "s")
	setSecretConsent(t, target, "s", home)
	createWidget(t, home, "w", target+"/s")
	r.c.enqueue(widgetNN(home, "w"))
	cutest.Eventually(t, time.Minute, heldFor(target, "s", home))

	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, target, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteSecret(t, target, "s")
	time.Sleep(2 * testGrace)
	s, err := env.Kube.CoreV1().Secrets(target).Get(ctx, "s", metav1.GetOptions{})
	if err != nil || s.DeletionTimestamp == nil {
		t.Fatalf("the Secret in the terminating namespace: %v, %v; want it kept, being deleted, for its live user", s, err)
	}
	deleteWidget(t, home, "w")
	r.c.enqueue(widgetNN(home, "w"))
	gone(t, time.Minute, "the Widget", widgetGet(home, "w"))
	gone(t, time.Minute, "the Secret", secretGet(target, "s"))
	if v := r.c.violations(); len(v) != 0 {
		t.Errorf("the deleting user failed to read its Secret: %v", v)
	}
	if err := env.FinalizeNamespace(ctx, target); err != nil {
		t.Fatal(err)
	}
}

// startConsentProcess starts one process tracking Widgets with crossRefs: a
// Shared, its tracker and a controller. hook, if set, sees the Shared before
// its Track.
func startConsentProcess(t *testing.T, hook func(*cu.Shared)) *controller {
	t.Helper()
	s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
	if hook != nil {
		hook(s)
	}
	c := newController(t)
	o := widgetOptions(c.enqueue)
	o.Refs = crossRefs
	o.DeletionGrace = testGrace
	o.ProbeInterval = time.Second
	tr, err := s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	running(t, s)
	c.start(&driver.Driver{Tracker: tr, Dynamic: env.Dynamic, Holder: cutest.WidgetGVR, OwnerFinalizer: ownerFinalizer})
	return c
}

// §11.3 rule 3: process Y's pass reads consent live just before it is
// withdrawn, and its A2 lands after process X's pass has dropped the ref and
// the release has taken the finalizer off. The finalizer Y's A2 puts back has
// no record naming it, and must still be released: by the batch Y's acquirer
// enqueued after its A2, or by the batch its write's informer event enqueues.
func TestALateA2AfterAWithdrawalIsReleased(t *testing.T) {
	cutest.Require(t, env)
	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	home, target := env.Namespace(t, "late-home"), env.Namespace(t, "late-target")
	createSecret(t, target, "s")
	setSecretConsent(t, target, "s", home)
	createWidget(t, home, "w", target+"/s")

	x := startConsentProcess(t, nil)
	x.enqueue(widgetNN(home, "w"))
	cutest.Eventually(t, time.Minute, heldFor(target, "s", home))

	var armed, fired atomic.Bool
	reached, released, landed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	y := startConsentProcess(t, func(s *cu.Shared) {
		s.HookCalls(func(ctx context.Context, c cu.Call) error {
			if !armed.Load() || c.Op != "patch" || c.GK.Kind != "Secret" || c.Namespace != target ||
				c.PatchType != types.StrategicMergePatchType || !fired.CompareAndSwap(false, true) {
				return nil
			}
			// Y has read consent live; now the withdrawal, X's drop and the
			// release all happen before this A2 lands.
			close(reached)
			select {
			case <-released:
			case <-time.After(time.Minute):
			}
			return nil
		}, func(c cu.Call, err error) {
			if fired.Load() && c.Op == "patch" && c.GK.Kind == "Secret" && c.Namespace == target && c.PatchType == types.StrategicMergePatchType {
				select {
				case landed <- err:
				default:
				}
			}
		})
	})
	armed.Store(true)
	y.enqueue(widgetNN(home, "w"))
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("Y's pass never reached its A2")
	}
	setSecretConsent(t, target, "s", "")
	x.enqueue(widgetNN(home, "w"))
	cutest.Eventually(t, time.Minute, notHeldFor(target, "s", home))
	close(released)
	select {
	case err := <-landed:
		if err != nil {
			t.Fatalf("Y's late A2 failed (%v): the schedule didn't happen", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Y's late A2 never landed")
	}

	// Y's A2 has landed: the finalizer is back, with no record naming it,
	// and must go again.
	cutest.Eventually(t, time.Minute, func() error {
		obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(home).Get(context.Background(), "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cu.ReadRecord(obj).Names(cu.SecretRef(target, "s").ID()) {
			return fmt.Errorf("the record still names the Secret: %w", cutest.ErrNotYet)
		}
		return notHeldFor(target, "s", home)()
	})
}
