package reconcilertest

import (
	"context"
	"strings"
	"testing"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// failingStatus is a client over one stored Thing whose status writes fail
// with a conflict when fail is true, and that Thing as read through it. Every
// other call, the helper's own Update included, goes through.
func failingStatus(t *testing.T, fail bool) (client.Client, *Thing, *int) {
	t.Helper()
	s := runtime.NewScheme()
	s.AddKnownTypes(thingGV, &Thing{}, &ThingList{})
	metav1.AddToGroupVersion(s, thingGV)
	seed := &Thing{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "demo"}}
	seed.SetGroupVersionKind(thingGV.WithKind("Thing"))
	attempts := 0
	kube := clientfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&Thing{}).WithObjects(seed).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				attempts++
				if fail {
					return kerrors.NewConflict(schema.GroupResource{Group: thingGV.Group, Resource: "things"}, obj.GetName(),
						errors.New("the object has been modified"))
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	mg := &Thing{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(seed), mg); err != nil {
		t.Fatal(err)
	}
	return kube, mg, &attempts
}

// writeNote re-reads the Thing and writes its status note, as a handler does.
// copyVersion says when it copies the re-read version onto mg: "after
// success", "before the write", or "despite the error". With retry, it
// retries on conflict as retry.RetryOnConflict does.
func writeNote(kube client.Client, copyVersion string, withRetry bool) Method {
	return func(ctx context.Context, mg resource.Managed) error {
		attempt := func() error {
			cp, err := freshCopy(ctx, kube, mg)
			if err != nil {
				return err
			}
			cp.Status.Note = "written"
			if copyVersion == "before the write" {
				mg.SetResourceVersion(cp.GetResourceVersion())
			}
			werr := kube.Status().Update(ctx, cp)
			if werr == nil || copyVersion == "despite the error" {
				mg.SetResourceVersion(cp.GetResourceVersion())
			}
			if werr == nil {
				mg.(*Thing).Status.Note = cp.Status.Note
			}
			return werr
		}
		if withRetry {
			return retry.RetryOnConflict(retry.DefaultRetry, attempt)
		}
		return attempt()
	}
}

func TestAssertFailedWriteLeavesManaged(t *testing.T) {
	cases := map[string]struct {
		method func(kube client.Client) Method
		// writesSucceed lets the method's status writes through.
		writesSucceed bool
		staleMg       bool
		wantFail      string
		// wantAttempts is the number of status writes the method makes, or 0 to skip.
		wantAttempts int
	}{
		"CopiesTheVersionOnlyAfterSuccess": {
			method:       func(kube client.Client) Method { return writeNote(kube, "after success", false) },
			wantAttempts: 1,
		},
		"RetriesAndCopiesTheVersionOnlyAfterSuccess": {
			method:       func(kube client.Client) Method { return writeNote(kube, "after success", true) },
			wantAttempts: retry.DefaultRetry.Steps,
		},
		"CopiesTheVersionBeforeTheWrite": {
			method:   func(kube client.Client) Method { return writeNote(kube, "before the write", false) },
			wantFail: "mg's resourceVersion changed",
		},
		"CopiesTheVersionDespiteTheError": {
			method:   func(kube client.Client) Method { return writeNote(kube, "despite the error", false) },
			wantFail: "mg's resourceVersion changed",
		},
		"RetriesAndCopiesTheVersionBeforeEachWrite": {
			method:       func(kube client.Client) Method { return writeNote(kube, "before the write", true) },
			wantFail:     "mg's resourceVersion changed",
			wantAttempts: retry.DefaultRetry.Steps,
		},
		// The idiom of re-reading into mg itself, inside a retry loop, fails too,
		// and the message says why and what to do instead.
		"RetriesAndReReadsIntoMg": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					return retry.RetryOnConflict(retry.DefaultRetry, func() error {
						if err := kube.Get(ctx, client.ObjectKeyFromObject(mg), mg); err != nil {
							return err
						}
						mg.(*Thing).Status.Note = "written"
						return kube.Status().Update(ctx, mg)
					})
				}
			},
			wantFail:     "Re-reading into mg itself, for example inside RetryOnConflict, also does this: re-read into a copy",
			wantAttempts: retry.DefaultRetry.Steps,
		},
		"ReturnsNil": {
			method:   func(client.Client) Method { return func(context.Context, resource.Managed) error { return nil } },
			wantFail: "the method returned nil",
		},
		"WriteLandedButTheMethodFailed": {
			method: func(kube client.Client) Method {
				write := writeNote(kube, "after success", false)
				return func(ctx context.Context, mg resource.Managed) error {
					if err := write(ctx, mg); err != nil {
						return err
					}
					return errors.New("a later step failed")
				}
			},
			writesSucceed: true,
			wantFail:      "the method's write landed",
		},
		"MgIsStale": {
			method: func(client.Client) Method {
				return func(context.Context, resource.Managed) error {
					t.Error("the method must not run on a stale mg")
					return errors.New("boom")
				}
			},
			staleMg:  true,
			wantFail: "read mg through kube before",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			kube, mg, attempts := failingStatus(t, !tc.writesSucceed)
			if tc.staleMg {
				mg.SetResourceVersion("0")
			}
			tb := &recordingTB{TB: t}

			err := AssertFailedWriteLeavesManaged(tb, context.Background(), kube, mg, tc.method(kube))

			if failed := len(tb.failures) > 0; failed != (tc.wantFail != "") {
				t.Fatalf("want failure %t, got failures %q", tc.wantFail != "", tb.failures)
			}
			if tc.wantFail != "" && !strings.Contains(strings.Join(tb.failures, "\n"), tc.wantFail) {
				t.Errorf("want a failure naming %q, got %q", tc.wantFail, tb.failures)
			}
			if tc.wantAttempts != 0 && *attempts != tc.wantAttempts {
				t.Errorf("status writes = %d, want %d", *attempts, tc.wantAttempts)
			}
			if tc.wantFail == "" && !kerrors.IsConflict(err) {
				t.Errorf("want the method's conflict returned, got %v", err)
			}
		})
	}
}
