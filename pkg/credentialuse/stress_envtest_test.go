package credentialuse_test

// Stress and drain against envtest (plan, PR 3b). Several processes, each a
// Shared with trackers for two holder kinds and controllers that drive them,
// with no leader election (§6.8), while goroutines create, re-point and
// delete holders, and delete and recreate Secrets and namespaces. The run is
// recorded (stress_record_test.go), and at its end the audits of
// stress_history_test.go go over it: every release is judged at the moment it
// lands, and every read a deletion pass fails to make (§14's I1). At the end
// no object keeps a credential-use finalizer without a user. The drain tests
// kill a process at the three points the plan names, start another, and
// check that it converges.
//
// Each sub-test runs for 8s, and as long again to tear everything down; set
// CREDENTIALUSE_STRESS to a duration for a longer one, which nobody runs
// without asking.
//
// CREDENTIALUSE_STRESS_SEED is the goroutines' seed: unset, 0, the same in
// every run; a number; or vary, a random one each time, which the run logs so
// that it can be given again. A seed fixes the operations each goroutine
// tries, not their timing.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
// per holder kind. stop ends it, and waits until all of it has ended.
type stressProcess struct {
	s     *cu.Shared
	ctrls []*controller
	stop  func()
}

// startStressProcess starts process proc, with rec recording its calls and
// its passes.
func startStressProcess(t *testing.T, rec *stressRecorder, proc int, so cu.SharedOptions, mode stressMode) *stressProcess {
	t.Helper()
	clients := clientsFor(t, env.Config)
	clients.Metadata = rec.calledBy(proc, clients.Metadata)
	p := &stressProcess{s: cu.NewShared(clients, so)}
	kinds := stressKinds
	if mode != stressPlain {
		// Widgets only, which reach their Secrets through Targets (§9).
		kinds = stressKinds[:1]
	}
	for _, k := range kinds {
		c := newController(t)
		o := widgetOptions(c.enqueue)
		o.Holder = k.gvk
		o.DeletionGrace = testGrace
		o.ProbeInterval = time.Second
		switch mode {
		case stressChained:
			p.s.FollowChains()
			chainOptions(&o)
		case stressCross:
			o.Refs = crossRefs
		}
		tr, err := p.s.Track(context.Background(), o)
		if err != nil {
			t.Fatal(err)
		}
		c.d = &driver.Driver{Tracker: tr, Dynamic: env.Dynamic, Holder: k.gvr, OwnerFinalizer: ownerFinalizer}
		rec.follow(proc, k.gvk.Kind, c)
		p.ctrls = append(p.ctrls, c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.s.Run(ctx) }()
	p.stop = sync.OnceFunc(func() {
		for _, c := range p.ctrls {
			if c.stop != nil {
				c.stop()
			}
		}
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	})
	t.Cleanup(p.stop)
	cutest.Eventually(t, time.Minute, func() error {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("Run returned before it synced: %v", err)
		default:
		}
		if !p.s.HasSynced() {
			return cutest.ErrNotYet
		}
		return nil
	})
	for _, c := range p.ctrls {
		c.start(c.d)
	}
	return p
}

// stressSeed is the seed the run's goroutines start from.
func stressSeed(t *testing.T) uint64 {
	switch v := os.Getenv("CREDENTIALUSE_STRESS_SEED"); v {
	case "":
		return 0
	case "vary":
		return rand.Uint64N(1 << 32)
	default:
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("CREDENTIALUSE_STRESS_SEED=%q: %v", v, err)
		}
		return n
	}
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
		t.Run(fmt.Sprintf("%d processes, no leader election", n), func(t *testing.T) { stress(t, n, d, cu.SharedOptions{}, stressPlain) })
	}
	t.Run("2 processes, every hold outside the caches", func(t *testing.T) {
		stress(t, 2, d, cu.SharedOptions{Namespaces: []string{env.Namespace(t, "stresscached")}}, stressPlain)
	})
	t.Run("2 processes, Widgets through Targets", func(t *testing.T) { stress(t, 2, d, cu.SharedOptions{}, stressChained) })
	t.Run("2 processes, cross-namespace refs whose consent toggles", func(t *testing.T) { stress(t, 2, d, cu.SharedOptions{}, stressCross) })
}

// stressWorld is what the stress goroutines share: the namespaces in use,
// and the holders in each.
type stressWorld struct {
	t     *testing.T
	rec   *stressRecorder
	procs []*stressProcess
	// chained: the holders are Widgets naming Targets, which name the
	// Secrets (§9).
	chained bool
	// target, in the cross mode: a namespace whose Secrets the holders
	// also name, whose consent a goroutine toggles (§11.3).
	target string
	// outside: every hold is outside the caches, so no pass skips (§10.5).
	outside bool

	mu       sync.Mutex
	nss      []string
	canaries []string                  // the canaries' namespaces
	holders  map[string][]stressHolder // by namespace
	findings []stFinding               // the canaries'
	seq      atomic.Int64
	ops      atomic.Int64
}

type stressHolder struct {
	kind int
	name string
}

const (
	stressSecrets = 4
	stressTargets = 3
)

// stressMode is what a stress run's holders name.
type stressMode int

const (
	stressPlain   stressMode = iota // Secrets in their own namespace
	stressChained                   // Targets, which name Secrets (§9)
	stressCross                     // also Secrets in another namespace (§11.3)
)

func (w *stressWorld) newNamespace() string {
	ns := env.Namespace(w.t, "stress")
	for i := range stressSecrets {
		w.recreateSecret(ns, i)
	}
	if w.chained {
		for i := range stressTargets {
			w.recreateTarget(ns, i, fmt.Sprintf("s%d", i))
		}
	}
	return ns
}

func (w *stressWorld) recreateTarget(ns string, i int, secret string) {
	err := w.rec.createTarget(context.Background(), ns, fmt.Sprintf("t%d", i), secret)
	if err != nil && !apierrors.IsAlreadyExists(err) && !apierrors.IsForbidden(err) {
		w.t.Errorf("recreating a Target in %s: %v", ns, err)
	}
}

func (w *stressWorld) recreateSecret(ns string, i int) {
	err := w.rec.createSecret(context.Background(), ns, fmt.Sprintf("s%d", i))
	if err != nil && !apierrors.IsAlreadyExists(err) && !apierrors.IsForbidden(err) {
		w.t.Errorf("recreating a Secret in %s: %v", ns, err)
	}
}

func (w *stressWorld) deleteSecret(ctx context.Context, ns, name string) {
	_ = w.rec.deleteObject(ctx, stObj{"Secret", ns, name})
}

func (w *stressWorld) deleteTarget(ctx context.Context, ns string, i int) {
	_ = w.rec.deleteObject(ctx, stObj{"Target", ns, fmt.Sprintf("t%d", i)})
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
	field, ref := "secret", secret
	if w.target != "" && rng.IntN(3) == 0 {
		ref = fmt.Sprintf("%s/c%d", w.target, rng.IntN(stressTargets))
	}
	if w.chained {
		// The holder names a Target; and a third of the steps act on the
		// Targets, as someone else would.
		field, ref = "target", fmt.Sprintf("t%d", rng.IntN(stressTargets))
		if rng.IntN(3) == 0 {
			w.targetStep(ctx, ns, rng)
			return
		}
	}
	switch op := rng.IntN(10); {
	case op < 3: // create a holder
		kind := 0
		if !w.chained && w.target == "" {
			// Only a plain run tracks both holder kinds.
			kind = rng.IntN(len(stressKinds))
		}
		name := fmt.Sprintf("h%d", w.seq.Add(1))
		u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{field: ref}}}
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
		_ = unstructured.SetNestedField(u.Object, ref, "spec", field)
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
		w.deleteSecret(ctx, ns, secret)
	default: // recreate one
		w.recreateSecret(ns, rng.IntN(stressSecrets))
	}
}

// targetStep acts on a Target, as someone other than the tracker: re-points
// it (which must enqueue its holders, §9.2), deletes it, or recreates it.
func (w *stressWorld) targetStep(ctx context.Context, ns string, rng *rand.Rand) {
	i := rng.IntN(stressTargets)
	r := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns)
	switch rng.IntN(3) {
	case 0:
		u, err := r.Get(ctx, fmt.Sprintf("t%d", i), metav1.GetOptions{})
		if err != nil || u.GetDeletionTimestamp() != nil {
			return
		}
		_ = unstructured.SetNestedField(u.Object, fmt.Sprintf("s%d", rng.IntN(stressSecrets)), "spec", "secret")
		_, _ = r.Update(ctx, u, metav1.UpdateOptions{})
	case 1:
		w.deleteTarget(ctx, ns, i)
	default:
		w.recreateTarget(ns, i, fmt.Sprintf("s%d", rng.IntN(stressSecrets)))
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
	if w.chained {
		// The intermediate objects first, as in TestChainTeardown.
		for i := range stressTargets {
			w.deleteTarget(ctx, ns, i)
		}
	}
	for i := range stressSecrets {
		w.deleteSecret(ctx, ns, fmt.Sprintf("s%d", i))
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

// A canary makes something that must happen in every run happen, out of the
// other goroutines' way, beside them: a bug that means "never" is else seen
// only by what the run doesn't have.
//
// canaryGrace is U5 (§5): an object is deleted in a live namespace with one
// live user, and is released over it after the grace.
func (w *stressWorld) canaryGrace(ctx context.Context) {
	ns := w.canaryNamespace()
	o, field := stObj{"Secret", ns, "s0"}, "secret"
	if w.chained {
		o, field = stObj{"Target", ns, "t0"}, "target"
	}
	uid, ok := w.canaryHeld(ctx, ns, field, o.name, o)
	if !ok {
		return
	}
	_ = w.rec.deleteObject(ctx, o)
	w.canaryUntil(ctx, ns, o, findNotReleased, "it was deleted, with one live user: it must go after its grace", func() (bool, string) {
		u, err := w.get(ctx, o)
		return apierrors.IsNotFound(err) || err == nil && string(u.GetUID()) != uid, fmt.Sprintf("%v", err)
	})
}

// canaryWithdrawal is §11.3: a hold of another namespace's object goes when
// its consent does, though its holder still names it.
func (w *stressWorld) canaryWithdrawal(ctx context.Context) {
	ns := w.canaryNamespace()
	o := stObj{"Secret", w.target, "canary"}
	consent := func(v any) bool {
		data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{cu.AnnotationAllowHoldersFrom: v}}})
		_, err := env.Kube.CoreV1().Secrets(o.ns).Patch(ctx, o.name, types.MergePatchType, data, metav1.PatchOptions{})
		if err != nil {
			w.t.Errorf("setting the consent of %s: %v", o, err)
		}
		return err == nil
	}
	if err := w.rec.createSecret(ctx, o.ns, o.name); err != nil {
		w.t.Errorf("creating %s: %v", o, err)
		return
	}
	if !consent(ns) {
		return
	}
	if _, ok := w.canaryHeld(ctx, ns, "secret", o.ns+"/"+o.name, o); !ok || !consent(nil) {
		return
	}
	w.canaryUntil(ctx, ns, o, findNotReleased, "its consent was withdrawn: its hold must go", func() (bool, string) {
		u, err := w.get(ctx, o)
		fin, _ := cu.FinalizerName(widgetGK, ns)
		return err == nil && !slices.Contains(u.GetFinalizers(), fin), fmt.Sprintf("%v", err)
	})
}

const canaryHolder = "hcanary"

// canaryNamespace is a namespace only a canary uses, which the end tears down
// as the others.
func (w *stressWorld) canaryNamespace() string {
	ns := w.newNamespace()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.canaries = append(w.canaries, ns)
	w.holders[ns] = []stressHolder{{0, canaryHolder}}
	return ns
}

// canaryHeld creates a Widget that names o, waits until it is established and
// o is held for it, and returns o's uid.
func (w *stressWorld) canaryHeld(ctx context.Context, ns, field, ref string, o stObj) (string, bool) {
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{field: ref}}}
	u.SetGroupVersionKind(cutest.WidgetGVK)
	u.SetNamespace(ns)
	u.SetName(canaryHolder)
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
		w.t.Errorf("creating the canary in %s: %v", ns, err)
		return "", false
	}
	fin, _ := cu.FinalizerName(widgetGK, ns)
	uid := ""
	ok := w.canaryUntil(ctx, ns, o, findNotHeld, "its holder must be established, and it held", func() (bool, string) {
		held, err := w.get(ctx, o)
		if err != nil {
			return false, err.Error()
		}
		holder, err := w.get(ctx, stObj{"Widget", ns, canaryHolder})
		if err != nil {
			return false, err.Error()
		}
		uid = string(held.GetUID())
		return slices.Contains(held.GetFinalizers(), fin) && slices.Contains(holder.GetFinalizers(), ownerFinalizer),
			fmt.Sprintf("finalizers %v; the holder's %v", held.GetFinalizers(), holder.GetFinalizers())
	})
	return uid, ok
}

// canaryTimeout is how long a canary waits: many times the grace, the
// prober's interval and a batch's first backoffs.
const canaryTimeout = 30 * time.Second

// canaryUntil waits until done, enqueuing the canary's holder each second as
// a poll interval would. If it isn't in time, it is a finding.
func (w *stressWorld) canaryUntil(ctx context.Context, ns string, o stObj, class, what string, done func() (bool, string)) bool {
	began := time.Now()
	for i := 0; ; i++ {
		ok, state := done()
		if ok {
			return true
		}
		if time.Since(began) > canaryTimeout {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.findings = append(w.findings, stFinding{class: class, text: fmt.Sprintf("%s: %s, and it isn't so after %s: %s", o, what, canaryTimeout, state),
				obj: o, holders: []stObj{{"Widget", ns, canaryHolder}}, from: began.Add(-5 * time.Second), to: time.Now(), zero: began})
			return false
		}
		if i%10 == 0 {
			w.enqueue(0, ns, canaryHolder)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (w *stressWorld) get(ctx context.Context, o stObj) (*unstructured.Unstructured, error) {
	return env.Dynamic.Resource(recordedKinds[o.kind]).Namespace(o.ns).Get(ctx, o.name, metav1.GetOptions{})
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

func stress(t *testing.T, n int, d time.Duration, so cu.SharedOptions, mode stressMode) {
	chained := mode == stressChained
	resetPublished(t, cutest.WidgetGVR)
	resetPublished(t, cutest.TargetGVR)
	t.Cleanup(func() {
		resetPublished(t, cutest.WidgetGVR)
		resetPublished(t, cutest.TargetGVR)
	})
	seed := stressSeed(t)
	rec := startRecorder(t, mode)
	w := &stressWorld{t: t, rec: rec, holders: map[string][]stressHolder{}, chained: chained, outside: len(so.Namespaces) > 0}
	for i := range n {
		w.procs = append(w.procs, startStressProcess(t, rec, i+1, so, mode))
	}
	var teardowns atomic.Int64
	// The audits run whatever ends the test.
	defer func() { w.audit(fmt.Sprintf("%d processes, %s, seed %d", n, d, seed), teardowns.Load()) }()
	if mode == stressCross {
		w.target = env.Namespace(t, "stresstarget")
		for i := range stressTargets {
			if err := rec.createSecret(context.Background(), w.target, fmt.Sprintf("c%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for range 3 {
		w.nss = append(w.nss, w.newNamespace())
	}
	ctx := context.Background()
	stop := time.Now().Add(d)
	var wg sync.WaitGroup
	wg.Go(func() { w.canaryGrace(ctx) })
	if w.target != "" {
		wg.Go(func() { w.canaryWithdrawal(ctx) })
		// One of the target's Secrets is deleted and recreated, so that §5's
		// rows for an object being deleted see holders of other namespaces.
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(seed+77, uint64(n)))
			for time.Now().Before(stop) {
				time.Sleep(time.Duration(500+rng.IntN(1000)) * time.Millisecond)
				w.deleteSecret(ctx, w.target, "c0")
				time.Sleep(time.Duration(rng.IntN(500)) * time.Millisecond)
				_ = rec.createSecret(ctx, w.target, "c0")
				w.ops.Add(2)
			}
		})
	}
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed+uint64(i+1), uint64(n)))
			for time.Now().Before(stop) {
				w.step(ctx, rng)
				time.Sleep(time.Duration(rng.IntN(20)) * time.Millisecond)
			}
		}()
	}
	if w.target != "" {
		// Consent toggles, on a Secret or on its Namespace: granted to every
		// namespace in use, or to none (§11.3).
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed+99, uint64(n)))
			for time.Now().Before(stop) {
				w.mu.Lock()
				grant := strings.Join(w.nss, ",")
				w.mu.Unlock()
				if rng.IntN(2) == 0 {
					grant = ""
				}
				var v any = grant
				if grant == "" {
					v = nil
				}
				data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{cu.AnnotationAllowHoldersFrom: v}}})
				if rng.IntN(2) == 0 {
					_, _ = env.Kube.CoreV1().Secrets(w.target).Patch(ctx, fmt.Sprintf("c%d", rng.IntN(stressTargets)), types.MergePatchType, data, metav1.PatchOptions{})
				} else {
					_, _ = env.Kube.CoreV1().Namespaces().Patch(ctx, w.target, types.MergePatchType, data, metav1.PatchOptions{})
				}
				w.ops.Add(1)
				time.Sleep(time.Duration(50+rng.IntN(150)) * time.Millisecond)
			}
		}()
	}
	// One namespace at a time is torn down and replaced, while the
	// goroutines keep working in the others.
	wg.Add(1)
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
	nss := slices.Concat(w.nss, w.canaries)
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
		if chained {
			// With no users left, each Target is released, so it goes.
			for i := range stressTargets {
				w.deleteTarget(ctx, ns, i)
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
		if w.target != "" {
			l, err := env.Kube.CoreV1().Secrets(w.target).List(ctx, metav1.ListOptions{})
			if err != nil {
				return err
			}
			for _, s := range l.Items {
				for _, f := range s.Finalizers {
					if cu.IsCredentialUseFinalizer(f) {
						return fmt.Errorf("the target's Secret %s keeps %s with no users: %w", s.Name, f, cutest.ErrNotYet)
					}
				}
			}
		}
		return nil
	})
}

// maxFindings bounds how many findings of one class are printed whole.
const maxFindings = 3

// audit stops the processes, ends the recording, and fails the test on
// everything the audits find, each with its timeline.
func (w *stressWorld) audit(what string, teardowns int64) {
	t := w.t
	var passes int64
	for _, p := range w.procs {
		p.stop()
		for _, c := range p.ctrls {
			passes += c.passes.Load()
		}
	}
	h := w.rec.finish(t)
	r := h.audit()
	t.Logf("%s: %d operations, %d namespace teardowns, %d passes; %s", what, w.ops.Load(), teardowns, passes, r)
	printed := map[string]int{}
	w.mu.Lock()
	findings := slices.Concat(w.findings, r.findings)
	w.mu.Unlock()
	for _, f := range findings {
		if printed[f.class]++; printed[f.class] <= maxFindings {
			t.Errorf("AUDIT %s: %s\n%s", f.class, f.text, h.timeline(f))
		}
	}
	for class, n := range printed {
		if n > maxFindings {
			t.Errorf("AUDIT %s: and %d more", class, n-maxFindings)
		}
	}
	// A stress that did next to nothing proves nothing (the review of #114),
	// and nor do audits that had next to nothing to look at.
	if w.ops.Load() < 100 || passes < 100 || teardowns < 1 {
		t.Errorf("the stress did too little: %d operations, %d passes and %d namespace teardowns; want at least 100, 100 and 1",
			w.ops.Load(), passes, teardowns)
	}
	for _, f := range h.floors(w.outside) {
		if n := r.counts[f.count]; n < f.min {
			t.Errorf("AUDIT TOO-LITTLE: %s: %d, want at least %d; %s", f.count, n, f.min, r)
		}
	}
	if n := r.counts[countSkipsUnchecked]; 10*n > r.counts[countSkips] {
		t.Errorf("AUDIT TOO-LITTLE: %s: %d, of %d checked", countSkipsUnchecked, n, r.counts[countSkips])
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
