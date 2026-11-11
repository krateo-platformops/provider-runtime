package reconcilertest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// Thing is a managed resource with a status subresource, for
// AssertLeavesManagedCurrent's tests.
type Thing struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ThingSpec   `json:"spec,omitempty"`
	Status            ThingStatus `json:"status,omitempty"`
}

type ThingSpec struct {
	Wanted string `json:"wanted,omitempty"`
}

type ThingStatus struct {
	prv1.ConditionedStatus `json:",inline"`
	Note                   string `json:"note,omitempty"`
	ID                     string `json:"id,omitempty"`
}

func (t *Thing) DeepCopyObject() runtime.Object {
	out := &Thing{}
	b, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
	return out
}

func (t *Thing) GetCondition(ct prv1.ConditionType) prv1.Condition { return t.Status.GetCondition(ct) }
func (t *Thing) SetConditions(c ...prv1.Condition)                 { t.Status.SetConditions(c...) }

type ThingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Thing `json:"items"`
}

func (l *ThingList) DeepCopyObject() runtime.Object {
	out := &ThingList{}
	b, _ := json.Marshal(l)
	_ = json.Unmarshal(b, out)
	return out
}

var thingGV = schema.GroupVersion{Group: "current.test.krateo.io", Version: "v1"}

// stored is a client over one stored Thing, and that Thing as read through it.
func storedThing(t *testing.T) (client.Client, *Thing) {
	t.Helper()
	s := runtime.NewScheme()
	s.AddKnownTypes(thingGV, &Thing{}, &ThingList{})
	metav1.AddToGroupVersion(s, thingGV)
	seed := &Thing{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "demo"}}
	seed.SetGroupVersionKind(thingGV.WithKind("Thing"))
	kube := clientfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&Thing{}).WithObjects(seed).Build()
	mg := &Thing{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(seed), mg); err != nil {
		t.Fatal(err)
	}
	return kube, mg
}

// freshCopy reads the stored Thing, as a handler that re-reads does.
func freshCopy(ctx context.Context, kube client.Client, mg resource.Managed) (*Thing, error) {
	cp := &Thing{}
	return cp, kube.Get(ctx, client.ObjectKeyFromObject(mg), cp)
}

func TestAssertLeavesManagedCurrent(t *testing.T) {
	errBoom := errors.New("boom")

	cases := map[string]struct {
		method      func(kube client.Client) Method
		staleMg     bool
		wantFail    bool
		wantMessage string
		wantErr     error
		// wantStored is the stored status note after the helper's write.
		wantStored string
	}{
		"WritesMgItself": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					mg.(*Thing).Status.Note = "written"
					return kube.Status().Update(ctx, mg)
				}
			},
			wantStored: "written",
		},
		"MirrorsTheVersionAndTheFields": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					cp, err := freshCopy(ctx, kube, mg)
					if err != nil {
						return err
					}
					cp.Status.Note = "written"
					cp.SetConditions(prv1.Available())
					if err := kube.Status().Update(ctx, cp); err != nil {
						return err
					}
					th := mg.(*Thing)
					th.SetResourceVersion(cp.GetResourceVersion())
					th.Status.Note = cp.Status.Note
					th.SetConditions(cp.GetCondition(prv1.TypeReady))
					return nil
				}
			},
			wantStored: "written",
		},
		"WritesNothing": {
			method: func(client.Client) Method {
				return func(context.Context, resource.Managed) error { return nil }
			},
		},
		"ReturnsTheMethodsError": {
			method: func(client.Client) Method {
				return func(context.Context, resource.Managed) error { return errBoom }
			},
			wantErr: errBoom,
		},
		"FreshCopyWithoutMirroring": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					cp, err := freshCopy(ctx, kube, mg)
					if err != nil {
						return err
					}
					cp.Status.Note = "written"
					return kube.Status().Update(ctx, cp)
				}
			},
			wantFail:    true,
			wantMessage: "conflicted",
			wantStored:  "written",
		},
		"MirrorsOnlyTheVersion": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					cp, err := freshCopy(ctx, kube, mg)
					if err != nil {
						return err
					}
					cp.Status.Note = "written"
					if err := kube.Status().Update(ctx, cp); err != nil {
						return err
					}
					mg.SetResourceVersion(cp.GetResourceVersion())
					return nil
				}
			},
			wantFail:    true,
			wantMessage: "reverted what the method wrote: status.note",
			// The revert happened: that's what the helper reports.
			wantStored: "",
		},
		"MirrorsOnlyTheVersionOfAWrittenCondition": {
			method: func(kube client.Client) Method {
				return func(ctx context.Context, mg resource.Managed) error {
					cp, err := freshCopy(ctx, kube, mg)
					if err != nil {
						return err
					}
					cp.SetConditions(prv1.Available())
					if err := kube.Status().Update(ctx, cp); err != nil {
						return err
					}
					mg.SetResourceVersion(cp.GetResourceVersion())
					return nil
				}
			},
			wantFail:    true,
			wantMessage: "status.conditions[Ready]",
		},
		"MgIsStale": {
			method: func(client.Client) Method {
				return func(context.Context, resource.Managed) error {
					t.Error("the method must not run on a stale mg")
					return nil
				}
			},
			staleMg:     true,
			wantFail:    true,
			wantMessage: "read mg through kube before",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			kube, mg := storedThing(t)
			if tc.staleMg {
				mg.SetResourceVersion("0")
			}
			tb := &recordingTB{TB: t}

			err := AssertLeavesManagedCurrent(tb, ctx, kube, mg, tc.method(kube))

			if failed := len(tb.failures) > 0; failed != tc.wantFail {
				t.Fatalf("want failure %t, got failures %q", tc.wantFail, tb.failures)
			}
			if tc.wantFail && !strings.Contains(strings.Join(tb.failures, "\n"), tc.wantMessage) {
				t.Errorf("want a failure naming %q, got %q", tc.wantMessage, tb.failures)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("want the method's error %v, got %v", tc.wantErr, err)
			}
			got := &Thing{}
			if gerr := kube.Get(ctx, client.ObjectKeyFromObject(mg), got); gerr != nil {
				t.Fatal(gerr)
			}
			if got.Status.Note != tc.wantStored {
				t.Errorf("stored note = %q, want %q", got.Status.Note, tc.wantStored)
			}
			if tc.wantFail || tc.staleMg {
				return
			}
			// What the helper wrote is what the runtime would: Synced from the method's result.
			want := prv1.ReconcileSuccess()
			if tc.wantErr != nil {
				want = prv1.ReconcileError(tc.wantErr)
			}
			if c := got.GetCondition(prv1.TypeSynced); c.Reason != want.Reason {
				t.Errorf("stored Synced = %s, want %s", c.Reason, want.Reason)
			}
		})
	}
}
