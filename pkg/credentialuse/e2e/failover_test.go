package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// noReleaseConfig is env's config with every JSON patch of a Secret refused:
// a process on it can hold Secrets (A2 is a strategic merge patch) and never
// release one (R3 is a JSON patch), as a leader that dies before R3.
func noReleaseConfig() *rest.Config {
	cfg := rest.CopyConfig(env.Config)
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPatch && strings.Contains(req.URL.Path, "/secrets/") &&
				strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json") {
				return nil, errors.New("test: this process can't release")
			}
			return rt.RoundTrip(req)
		})
	}
	return cfg
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

var leaseSeq = 0

// heldBy reports whether Secret ns/name carries the Widget kind's finalizer
// for ns.
func heldBy(ns, name string) (bool, error) {
	f, _ := cu.FinalizerName(widgetGK, ns)
	s, err := env.Kube.CoreV1().Secrets(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return slices.Contains(s.Finalizers, f), nil
}

// Failover (M1 of #52's review), with two managers under leader election.
// The leader deletes a holder, removing its owner finalizer and record, and
// stops before R3. The standby's informers, which run without election, saw
// the holder go while it was standby; once elected, it releases the Secret.
// Again with the Secret being deleted, so that a grace timer is pending at the
// failover: the standby releases it, and it goes. The standby's controller
// warms up before election (EnableWarmup), so its source starts the informers.
func TestFailover(t *testing.T) {
	cutest.Require(t, env)
	for _, pending := range []bool{false, true} {
		name := "between Forget and R3"
		if pending {
			name = "with a grace timer pending"
		}
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			leaseSeq++
			lease := fmt.Sprintf("cu-failover-%d", leaseSeq)
			leader := w.start(t, managerOptions{refs: true, lease: lease, cfg: noReleaseConfig()})
			ns := env.Namespace(t, "failover")
			createSecret(t, ns, "creds")
			createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
			established(t, w, ns, nn(ns, "creds"), "w")
			w.start(t, managerOptions{refs: true, lease: lease, warmup: true})
			time.Sleep(2 * time.Second) // the standby's informers sync

			if pending {
				deleteSecret(t, ns, "creds")
			}
			deleteWidget(t, ns, "w")
			gone(t, 30*time.Second, "the Widget", widgetGet(ns, "w"))
			time.Sleep(time.Second)
			if held, err := heldBy(ns, "creds"); err != nil || !held {
				t.Fatalf("the Secret after the holder went: held %v, %v; want it still held, the leader unable to release", held, err)
			}
			if err := leader.stop(); err != nil {
				t.Fatalf("the leader's Start returned %v", err)
			}
			if pending {
				gone(t, 30*time.Second, "the Secret", secretGet(ns, "creds"))
				return
			}
			cutest.Eventually(t, 30*time.Second, func() error {
				held, err := heldBy(ns, "creds")
				if err != nil {
					return err
				}
				if held {
					return fmt.Errorf("the Secret is still held: %w", cutest.ErrNotYet)
				}
				return nil
			})
		})
	}
}
