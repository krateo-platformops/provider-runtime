package credentialuse_test

// H1 (§6.4): the API server removes an object that is being deleted in two steps,
// so a release of it, or a write to it, can be lost between them. What the wait
// before such a release is for.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// raced is one Secret of a race: held with epoch e0, with the finalizers extra
// besides, and being deleted; release is its R3.
func raced(t *testing.T, ns, name string, extra ...string) (release cu.Patch) {
	t.Helper()
	ctx := context.Background()
	f, err := cu.FinalizerName(widgetGK, ns)
	if err != nil {
		t.Fatal(err)
	}
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns, Finalizers: append([]string{f}, extra...), Annotations: map[string]string{f: "e0"},
		Labels: map[string]string{cu.LabelHeld: "true", cu.MarkerLabel(widgetGK): "true"},
	}}
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, s, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r := secrets(env.Metadata, ns)
	if err := r.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	read, err := r.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if release, err = cu.ReleasePatch(read, widgetGK, ns); err != nil {
		t.Fatal(err)
	}
	return release
}

// raceA2 runs one R3 of the object against four A2s, and reports which
// succeeded.
func raceA2(t *testing.T, ns, name string, release cu.Patch) (released bool, acquired int64) {
	t.Helper()
	ctx := context.Background()
	r := secrets(env.Metadata, ns)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var oks atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		time.Sleep(rand.N(2 * time.Millisecond))
		_, err := send(ctx, r, name, release)
		released = err == nil
	}()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			time.Sleep(rand.N(4 * time.Millisecond))
			epoch, err := cu.NewEpoch()
			if err != nil {
				t.Error(err)
				return
			}
			pt, err := cu.AcquireStrategicPatch(widgetGK, ns, epoch)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := send(ctx, r, name, pt); err == nil {
				oks.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	return released, oks.Load()
}

// The API server tests an R3 that empties the finalizers of an object being
// deleted, and then removes the object, in two steps: an A2 between them
// succeeds, and is lost with it. On a server that did both in one, an R3 and an
// A2 of one epoch couldn't both succeed. **If this fails, Kubernetes has changed,
// for the better: §6.4's H1, the wait and P23 need another look.** The control
// gives the object another finalizer, so that the R3 removes nothing.
func TestPremiseARemovalIsTwoSteps(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "twosteps")
	const most = 2000
	deadline := time.Now().Add(3 * time.Minute)
	races, both, r3s, a2s := 0, 0, 0, 0
	for ; races < most && both == 0 && time.Now().Before(deadline); races++ {
		name := fmt.Sprintf("o%d", races)
		released, acquired := raceA2(t, ns, name, raced(t, ns, name))
		if released {
			r3s++
		}
		if acquired > 0 {
			a2s++
		}
		if released && acquired > 0 {
			both++
			if _, err := secrets(env.Metadata, ns).Get(context.Background(), name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("race %d: an R3 and %d A2s succeeded, and the object is there: %v", races, acquired, err)
			}
		}
	}
	t.Logf("TWOSTEPS %d races: R3 succeeded in %d, an A2 in %d, both in %d", races, r3s, a2s, both)
	if both == 0 {
		t.Errorf("in %d races no A2 was lost: the API server may remove in one step now (or the races didn't overlap: R3 succeeded in %d, an A2 in %d)", races, r3s, a2s)
	}
	for i := range 200 {
		name := fmt.Sprintf("control%d", i)
		if released, acquired := raceA2(t, ns, name, raced(t, ns, name, "example.com/keep")); released && acquired > 0 {
			// Both can succeed in one order only: the R3, and then an A2 is
			// refused (no new finalizer). So an R3 landed over an A2.
			t.Fatalf("control, race %d: an R3 and %d A2s succeeded on an object that wasn't removed", i, acquired)
		}
	}
}
