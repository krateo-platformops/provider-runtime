package credentialuse_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// A ref to a name no object can have (§6.2), as a holder whose data says
// "<namespace>/<name>" where a name is meant: the ref is Missing, and the
// holder goes on; and Disown settles, though no pass drops the ref.
func TestARefToANameNoObjectCanHave(t *testing.T) {
	cutest.Require(t, env)
	for _, mode := range []string{"PerName", "LabelScoped"} {
		t.Run(mode, func(t *testing.T) {
			set := disowning(t)
			idx, ns := indexNamespace(t), env.Namespace(t, "segment")
			createWidget(t, ns, "w", "a/b")
			var r *writeRig
			if mode == "PerName" {
				r = perNameRig(t, idx, disownOptions, nil)
			} else {
				r = newWriteRig(t, env.Config, cu.SharedOptions{}, disownOptions, nil)
			}
			nn := widgetNN(ns, "w")
			r.c.enqueue(nn)
			cutest.Eventually(t, 30*time.Second, func() error {
				u, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), "w", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if !slices.Contains(u.GetFinalizers(), ownerFinalizer) {
					return fmt.Errorf("w isn't established, its last pass %+v: %w", r.c.lastResult(nn).Acquired, cutest.ErrNotYet)
				}
				return nil
			})
			a := r.c.lastResult(nn).Acquired
			if a.MustNotConnect || a.Degraded || len(a.Unheld) != 1 || a.Unheld[0].Reason != cu.ReasonMissing {
				t.Errorf("%+v; want the ref Missing, and nothing else", a)
			}
			r.c.stop()
			set("all")
			cutest.Eventually(t, time.Minute, settledAtZero(t))
		})
	}
}
