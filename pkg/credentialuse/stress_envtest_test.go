package credentialuse_test

// Stress and drain against envtest (plan, PR 3b). Several processes, each a
// Shared with trackers for two holder kinds and controllers that drive them,
// with no leader election (§6.8), while goroutines create, re-point and
// delete holders, and delete and recreate Secrets and namespaces. Every read
// a deleting holder's pass makes is checked (§14's I1), and at the end no
// object keeps a credential-use finalizer without a user. The drain tests
// kill a process at the three points the plan names, start another, and
// check that it converges.
//
// The default run is bounded, about 30s for both variants; set
// CREDENTIALUSE_STRESS to a duration (per variant) for a longer one, which
// nobody runs without asking.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// stressKinds are the two holder kinds.
var stressKinds = []struct {
	gvk schema.GroupVersionKind
	gvr schema.GroupVersionResource
}{
	{cutest.WidgetGVK, cutest.WidgetGVR},
	{cutest.TargetGVK, cutest.TargetGVR},
}

// stressProcess is one process: a Shared, its trackers, and a controller
// per holder kind.
type stressProcess struct {
	s     *cu.Shared
	ctrls []*controller
}

func startStressProcess(t *testing.T, so cu.SharedOptions) *stressProcess {
	t.Helper()
	p := &stressProcess{s: cu.NewShared(clientsFor(t, env.Config), so)}
	for _, k := range stressKinds {
		c := newController(t)
		o := widgetOptions(c.enqueue)
		o.Holder = k.gvk
		o.DeletionGrace = testGrace
		o.ProbeInterval = time.Second
		tr, err := p.s.Track(context.Background(), o)
		if err != nil {
			t.Fatal(err)
		}
		c.d = &driver.Driver{Tracker: tr, Dynamic: env.Dynamic, Holder: k.gvr, OwnerFinalizer: ownerFinalizer}
		p.ctrls = append(p.ctrls, c)
	}
	running(t, p.s)
	for _, c := range p.ctrls {
		c.start(c.d)
	}
	return p
}

func stressDuration(t *testing.T) time.Duration {
	if v := os.Getenv("CREDENTIALUSE_STRESS"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("CREDENTIALUSE_STRESS=%q: %v", v, err)
		}
		return d
	}
	return 8 * time.Second
}

// The stress test, with two processes and with three, and with two whose
// caches SharedOptions.Namespaces restricts to a namespace the stress
// doesn't use, so that every hold is outside the caches (§10.5): read live,
// enqueued by the passes and the namespace teardowns, and rechecked live.
func TestStress(t *testing.T) {
	cutest.Require(t, env)
	d := stressDuration(t)
	for _, n := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d processes, no leader election", n), func(t *testing.T) { stress(t, n, d, cu.SharedOptions{}) })
	}
	t.Run("2 processes, every hold outside the caches", func(t *testing.T) {
		stress(t, 2, d, cu.SharedOptions{Namespaces: []string{env.Namespace(t, "stresscached")}})
	})
}

// stressWorld is what the stress goroutines share: the namespaces in use,
// and the holders in each.
type stressWorld struct {
	t     *testing.T
	procs []*stressProcess

	mu      sync.Mutex
	nss     []string
	holders map[string][]stressHolder // by namespace
	seq     atomic.Int64
	ops     atomic.Int64
}

type stressHolder struct {
	kind int
	name string
}

const stressSecrets = 4

func (w *stressWorld) newNamespace() string {
	ns := env.Namespace(w.t, "stress")
	for i := range stressSecrets {
		w.recreateSecret(ns, i)
	}
	return ns
}

func (w *stressWorld) recreateSecret(ns string, i int) {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("s%d", i)}}
	_, err := env.Kube.CoreV1().Secrets(ns).Create(context.Background(), s, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) && !apierrors.IsForbidden(err) {
		w.t.Errorf("recreating a Secret in %s: %v", ns, err)
	}
}

// enqueue enqueues a holder in every process, as every process's controller
// sees its holder events.
func (w *stressWorld) enqueue(kind int, ns, name string) {
	for _, p := range w.procs {
		p.ctrls[kind].enqueue(types.NamespacedName{Namespace: ns, Name: name})
	}
}

func (w *stressWorld) pick(rng *rand.Rand) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.nss) == 0 {
		return "", false
	}
	return w.nss[rng.IntN(len(w.nss))], true
}

// step runs one random operation.
func (w *stressWorld) step(ctx context.Context, rng *rand.Rand) {
	ns, ok := w.pick(rng)
	if !ok {
		return
	}
	w.ops.Add(1)
	secret := fmt.Sprintf("s%d", rng.IntN(stressSecrets))
	switch op := rng.IntN(10); {
	case op < 3: // create a holder
		kind := rng.IntN(len(stressKinds))
		name := fmt.Sprintf("h%d", w.seq.Add(1))
		u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": secret}}}
		u.SetGroupVersionKind(stressKinds[kind].gvk)
		u.SetNamespace(ns)
		u.SetName(name)
		if _, err := env.Dynamic.Resource(stressKinds[kind].gvr).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
			return // a namespace being torn down refuses it
		}
		w.mu.Lock()
		w.holders[ns] = append(w.holders[ns], stressHolder{kind, name})
		w.mu.Unlock()
		w.enqueue(kind, ns, name)
	case op < 5: // re-point a holder
		h, ok := w.someHolder(ns, rng)
		if !ok {
			return
		}
		r := env.Dynamic.Resource(stressKinds[h.kind].gvr).Namespace(ns)
		u, err := r.Get(ctx, h.name, metav1.GetOptions{})
		if err != nil || u.GetDeletionTimestamp() != nil {
			return
		}
		_ = unstructured.SetNestedField(u.Object, secret, "spec", "secret")
		if _, err := r.Update(ctx, u, metav1.UpdateOptions{}); err == nil {
			w.enqueue(h.kind, ns, h.name)
		}
	case op < 7: // delete a holder
		h, ok := w.someHolder(ns, rng)
		if !ok {
			return
		}
		if err := env.Dynamic.Resource(stressKinds[h.kind].gvr).Namespace(ns).Delete(ctx, h.name, metav1.DeleteOptions{}); err == nil {
			w.enqueue(h.kind, ns, h.name)
		}
	case op < 9: // delete a Secret
		_ = env.Kube.CoreV1().Secrets(ns).Delete(ctx, secret, metav1.DeleteOptions{})
	default: // recreate one
		w.recreateSecret(ns, rng.IntN(stressSecrets))
	}
}

func (w *stressWorld) someHolder(ns string, rng *rand.Rand) (stressHolder, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	hs := w.holders[ns]
	if len(hs) == 0 {
		return stressHolder{}, false
	}
	return hs[rng.IntN(len(hs))], true
}

// teardown plays the namespace controller for ns: it deletes the
// namespace, then its Secrets, then its holders, and finalizes it once
// they are gone.
func (w *stressWorld) teardown(ctx context.Context, ns string) error {
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return err
	}
	for i := range stressSecrets {
		_ = env.Kube.CoreV1().Secrets(ns).Delete(ctx, fmt.Sprintf("s%d", i), metav1.DeleteOptions{})
	}
	w.mu.Lock()
	hs := slices.Clone(w.holders[ns])
	w.mu.Unlock()
	for _, h := range hs {
		if err := env.Dynamic.Resource(stressKinds[h.kind].gvr).Namespace(ns).Delete(ctx, h.name, metav1.DeleteOptions{}); err == nil || apierrors.IsNotFound(err) {
			w.enqueue(h.kind, ns, h.name)
		}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		empty, err := w.empty(ctx, ns)
		if err != nil {
			return err
		}
		if empty {
			return env.FinalizeNamespace(ctx, ns)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("namespace %s didn't empty: %s", ns, w.describe(ctx, ns))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// empty reports whether ns has no holders and no Secrets left.
func (w *stressWorld) empty(ctx context.Context, ns string) (bool, error) {
	for _, k := range stressKinds {
		l, err := env.Dynamic.Resource(k.gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		if len(l.Items) > 0 {
			return false, nil
		}
	}
	l, err := env.Kube.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	return len(l.Items) == 0, nil
}

// describe lists what ns holds, for a failure message.
func (w *stressWorld) describe(ctx context.Context, ns string) string {
	var parts []string
	for _, k := range stressKinds {
		l, _ := env.Dynamic.Resource(k.gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		for _, i := range l.Items {
			parts = append(parts, fmt.Sprintf("%s %s finalizers %v deleting %v", k.gvk.Kind, i.GetName(), i.GetFinalizers(), i.GetDeletionTimestamp() != nil))
		}
	}
	l, _ := env.Kube.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
	for _, s := range l.Items {
		parts = append(parts, fmt.Sprintf("Secret %s finalizers %v deleting %v", s.Name, s.Finalizers, s.DeletionTimestamp != nil))
	}
	return strings.Join(parts, "; ")
}

func stress(t *testing.T, n int, d time.Duration, so cu.SharedOptions) {
	resetPublished(t, cutest.WidgetGVR)
	resetPublished(t, cutest.TargetGVR)
	w := &stressWorld{t: t, holders: map[string][]stressHolder{}}
	for range n {
		w.procs = append(w.procs, startStressProcess(t, so))
	}
	for range 3 {
		w.nss = append(w.nss, w.newNamespace())
	}
	ctx := context.Background()
	stop := time.Now().Add(d)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i+1), uint64(n)))
			for time.Now().Before(stop) {
				w.step(ctx, rng)
				time.Sleep(time.Duration(rng.IntN(20)) * time.Millisecond)
			}
		}()
	}
	// One namespace at a time is torn down and replaced, while the
	// goroutines keep working in the others.
	wg.Add(1)
	var teardowns atomic.Int64
	go func() {
		defer wg.Done()
		for time.Now().Before(stop) {
			time.Sleep(d / 3)
			w.mu.Lock()
			if len(w.nss) == 0 {
				w.mu.Unlock()
				return
			}
			ns := w.nss[0]
			w.nss = w.nss[1:]
			w.mu.Unlock()
			if err := w.teardown(ctx, ns); err != nil {
				t.Errorf("tearing down %s: %v", ns, err)
				return
			}
			teardowns.Add(1)
			next := w.newNamespace()
			w.mu.Lock()
			w.nss = append(w.nss, next)
			w.mu.Unlock()
		}
	}()
	wg.Wait()

	// The end: every holder goes; then no Secret keeps a credential-use
	// finalizer, since none has a user.
	w.mu.Lock()
	nss := slices.Clone(w.nss)
	w.mu.Unlock()
	for _, ns := range nss {
		w.mu.Lock()
		hs := slices.Clone(w.holders[ns])
		w.mu.Unlock()
		for _, h := range hs {
			if err := env.Dynamic.Resource(stressKinds[h.kind].gvr).Namespace(ns).Delete(ctx, h.name, metav1.DeleteOptions{}); err == nil || apierrors.IsNotFound(err) {
				w.enqueue(h.kind, ns, h.name)
			}
		}
	}
	cutest.Eventually(t, 2*time.Minute, func() error {
		for _, ns := range nss {
			for _, k := range stressKinds {
				l, err := env.Dynamic.Resource(k.gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
				if err != nil {
					return err
				}
				if len(l.Items) > 0 {
					return fmt.Errorf("holders left in %s: %s: %w", ns, w.describe(ctx, ns), cutest.ErrNotYet)
				}
			}
			l, err := env.Kube.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return err
			}
			for _, s := range l.Items {
				for _, f := range s.Finalizers {
					if cu.IsCredentialUseFinalizer(f) {
						return fmt.Errorf("Secret %s/%s keeps %s with no users: %w", ns, s.Name, f, cutest.ErrNotYet)
					}
				}
			}
		}
		return nil
	})
	var violations []string
	var passes int64
	for _, p := range w.procs {
		for _, c := range p.ctrls {
			violations = append(violations, c.violations()...)
			passes += c.passes.Load()
		}
	}
	t.Logf("%d processes, %s: %d operations, %d namespace teardowns, %d passes", n, d, w.ops.Load(), teardowns.Load(), passes)
	// A stress that did next to nothing proves nothing (the review of #114).
	if w.ops.Load() < 100 || passes < 100 || teardowns.Load() < 1 {
		t.Errorf("the stress did too little: %d operations, %d passes and %d namespace teardowns; want at least 100, 100 and 1",
			w.ops.Load(), passes, teardowns.Load())
	}
	if len(violations) != 0 {
		t.Errorf("%d reads failed for holders being deleted: %v", len(violations), violations)
	}
}

// Drain (plan, PR 3b): a process is killed between a holder's A1 and its
// A2, between a batch's R1 and its R3, and in the middle of a batch. A new
// process then converges, with no violation: the holder is established and
// held, and holds with no users are released.
func TestDrain(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	f := func(ns string) string { s, _ := cu.FinalizerName(widgetGK, ns); return s }
	held := func(ns, name string) bool {
		s, err := secrets(env.Metadata, ns).Get(ctx, name, metav1.GetOptions{})
		return err == nil && hasString(s.Finalizers, f(ns))
	}

	t.Run("between A1 and A2", func(t *testing.T) {
		ns := env.Namespace(t, "drain-a")
		var r *writeRig
		var killed atomic.Bool
		r = newWriteRig(t, env.Config, cu.SharedOptions{}, nil, func(s *cu.Shared) {
			s.HookCalls(nil, func(c cu.Call, err error) {
				if err == nil && c.Op == "patch" && c.GK == widgetGK && c.Namespace == ns && killed.CompareAndSwap(false, true) {
					r.kill() // just after A1
				}
			})
		})
		createSecret(t, ns, "creds")
		createWidget(t, ns, "w", "creds")
		r.c.enqueue(widgetNN(ns, "w"))
		cutest.Eventually(t, 30*time.Second, func() error {
			if !killed.Load() {
				return cutest.ErrNotYet
			}
			return nil
		})
		if held(ns, "creds") {
			t.Fatal("A2 ran after the kill")
		}
		next := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
		next.c.enqueue(widgetNN(ns, "w"))
		established(t, ns, "creds", "w")
		deleteWidget(t, ns, "w")
		next.c.enqueue(widgetNN(ns, "w"))
		gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
		cutest.Eventually(t, time.Minute, func() error {
			if held(ns, "creds") {
				return fmt.Errorf("still held: %w", cutest.ErrNotYet)
			}
			return nil
		})
	})

	t.Run("between R1 and R3", func(t *testing.T) {
		ns := env.Namespace(t, "drain-b")
		var r *writeRig
		var armed, killed atomic.Bool
		r = newWriteRig(t, env.Config, cu.SharedOptions{}, nil, func(s *cu.Shared) {
			s.HookCalls(nil, func(c cu.Call, err error) {
				// R1's read of the Secret, in a batch after the holder went.
				if armed.Load() && c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == ns && killed.CompareAndSwap(false, true) {
					r.kill()
				}
			})
		})
		createSecret(t, ns, "creds")
		createWidget(t, ns, "w", "creds")
		r.c.enqueue(widgetNN(ns, "w"))
		established(t, ns, "creds", "w")
		armed.Store(true)
		deleteWidget(t, ns, "w")
		r.c.enqueue(widgetNN(ns, "w"))
		gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
		cutest.Eventually(t, 30*time.Second, func() error {
			if !killed.Load() {
				return cutest.ErrNotYet
			}
			return nil
		})
		if !held(ns, "creds") {
			t.Fatal("the release finished despite the kill")
		}
		newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
		cutest.Eventually(t, time.Minute, func() error {
			if held(ns, "creds") {
				return fmt.Errorf("the new process's rescan didn't release it: %w", cutest.ErrNotYet)
			}
			return nil
		})
	})

	t.Run("in the middle of a batch", func(t *testing.T) {
		ns := env.Namespace(t, "drain-c")
		var r *writeRig
		var armed, killed atomic.Bool
		r = newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) {
			o.Refs = func(obj metav1.Object) ([]cu.Ref, error) {
				return []cu.Ref{cu.SecretRef(obj.GetNamespace(), "one"), cu.SecretRef(obj.GetNamespace(), "two")}, nil
			}
		}, func(s *cu.Shared) {
			s.HookCalls(nil, func(c cu.Call, err error) {
				// After the batch's first R3.
				if armed.Load() && err == nil && c.Op == "patch" && c.PatchType == types.JSONPatchType && c.Namespace == ns && killed.CompareAndSwap(false, true) {
					r.kill()
				}
			})
		})
		createSecret(t, ns, "one")
		createSecret(t, ns, "two")
		createWidget(t, ns, "w", "")
		r.c.enqueue(widgetNN(ns, "w"))
		established(t, ns, "one", "w")
		established(t, ns, "two", "w")
		armed.Store(true)
		deleteWidget(t, ns, "w")
		r.c.enqueue(widgetNN(ns, "w"))
		gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
		cutest.Eventually(t, 30*time.Second, func() error {
			if !killed.Load() {
				return cutest.ErrNotYet
			}
			return nil
		})
		if held(ns, "one") == held(ns, "two") {
			t.Fatalf("one held %v, two held %v: want the kill between their releases", held(ns, "one"), held(ns, "two"))
		}
		newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
		cutest.Eventually(t, time.Minute, func() error {
			if held(ns, "one") || held(ns, "two") {
				return fmt.Errorf("a hold stayed: %w", cutest.ErrNotYet)
			}
			return nil
		})
	})
}
