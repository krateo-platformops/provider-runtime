package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// A stress run with the real reconciler, smaller than the core's (plan,
// PR 7): several namespaces, each with two Secrets and four Widgets naming
// one of them, torn down concurrently in random orders, half of them with
// the namespace deleted too. No Widget's deletion ever fails for want of its
// credential (I1), and everything goes. Bounded to about 45s under -race.
func TestStress(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	const namespaces, widgets = 6, 4
	rng := rand.New(rand.NewPCG(1, 7))
	type space struct {
		ns       string
		i1       *i1Watch
		names    []string
		secrets  []string
		withNS   bool
		shuffled []int
	}
	var spaces []*space
	for i := range namespaces {
		sp := &space{ns: env.Namespace(t, "stress"), secrets: []string{"a", "b"}, withNS: i%2 == 0}
		sp.i1 = watchI1(t, sp.ns)
		for _, s := range sp.secrets {
			createSecret(t, sp.ns, s)
		}
		for j := range widgets {
			name := fmt.Sprintf("w%d", j)
			sp.names = append(sp.names, name)
			createWidget(t, w, sp.ns, name, WidgetSpec{Secret: sp.secrets[rng.IntN(2)]})
		}
		sp.shuffled = rng.Perm(len(sp.secrets) + len(sp.names))
		spaces = append(spaces, sp)
	}
	for _, sp := range spaces {
		cutest.Eventually(t, time.Minute, func() error {
			for _, name := range sp.names {
				if a := getWidget(t, w, sp.ns, name).Annotations; len(a) == 0 {
					return fmt.Errorf("%s/%s not established: %w", sp.ns, name, cutest.ErrNotYet)
				}
			}
			return nil
		})
	}
	var wg sync.WaitGroup
	for i, sp := range spaces {
		local := rand.New(rand.NewPCG(uint64(i), 11)) // one per goroutine
		wg.Go(func() {
			if sp.withNS {
				if err := env.Kube.CoreV1().Namespaces().Delete(ctx, sp.ns, metav1.DeleteOptions{}); err != nil {
					t.Error(err)
				}
			}
			for _, k := range sp.shuffled {
				if k < len(sp.secrets) {
					deleteSecret(t, sp.ns, sp.secrets[k])
				} else {
					deleteWidget(t, sp.ns, sp.names[k-len(sp.secrets)])
				}
				time.Sleep(time.Duration(local.IntN(300)) * time.Millisecond)
			}
		})
	}
	wg.Wait()
	for _, sp := range spaces {
		for _, name := range sp.names {
			gone(t, 45*time.Second, sp.ns+"/"+name, widgetGet(sp.ns, name))
		}
		for _, s := range sp.secrets {
			gone(t, 45*time.Second, "Secret "+sp.ns+"/"+s, secretGet(sp.ns, s))
		}
		if v := sp.i1.seen(); len(v) != 0 {
			t.Errorf("I1 in %s: %v", sp.ns, v)
		}
		if sp.withNS {
			if err := env.FinalizeNamespace(ctx, sp.ns); err != nil {
				t.Error(err)
			}
		}
	}
}

// A drain that stops the manager in the middle of a pass: the pass ends with
// its context, the manager returns, and a new manager, a fresh process,
// takes the Widget over and establishes it.
func TestDrain(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	ns := env.Namespace(t, "drain")
	createSecret(t, ns, "creds")
	entered := make(chan struct{})
	var once sync.Once
	block := func(ctx context.Context, _ *Widget) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
	}
	w.connect.Store(&block)
	m := w.start(t, managerOptions{refs: true})
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
	select {
	case <-entered:
	case <-time.After(time.Minute):
		t.Fatal("no pass reached Connect")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- m.stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("the manager's Start returned %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("the manager didn't drain")
	}
	w.connect.Store(nil)
	w.start(t, managerOptions{refs: true})
	established(t, w, ns, nn(ns, "creds"), "w")
}
