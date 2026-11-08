package scaletozero

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const hour = time.Hour

// countingClient wraps the admin client, counts scale patches, and can fail
// LIST and scale PATCH requests.
type countingClient struct {
	client.Client
	patches *int
}

func newCountingClient(t *testing.T, listErr, patchErr error) countingClient {
	t.Helper()
	patches := new(int)
	c := interceptor.NewClient(k8sWithWatch(t), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if listErr != nil {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			*patches++
			if patchErr != nil {
				return patchErr
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	return countingClient{Client: c, patches: patches}
}

func scaleReplicas(t *testing.T, ns, name string) int32 {
	t.Helper()
	s := &autoscalingv1.Scale{}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := k8s.SubResource("scale").Get(context.Background(), d, s); err != nil {
		t.Fatalf("reading the scale of %s/%s: %v", ns, name, err)
	}
	return s.Spec.Replicas
}

// The outcomes on a working API server: woken from 0 to active when an
// instance exists, a running count left alone, down to 0 after the hold-down,
// held within it, and a patch only on change.
func TestReconcile(t *testing.T) {
	requireAPIServer(t)
	cases := []struct {
		name        string
		start       int32
		active      int32
		instances   int
		lastSeen    time.Duration // before now; 0 means zero time; negative means in the future
		holdDown    time.Duration
		want        int32
		wantReason  Reason
		wantPatch   bool
		wantRequeue [2]time.Duration // inclusive bounds
		// wantLastSeen: "now", "kept"
		wantLastSeen string
	}{
		{"an instance wakes it from zero to active", 0, 2, 1, 2 * hour, hour, 2, ReasonActive, true, [2]time.Duration{0, 0}, "now"},
		{"an instance wakes it from zero to an active of 1", 0, 1, 1, 2 * hour, hour, 1, ReasonActive, true, [2]time.Duration{0, 0}, "now"},
		{"an instance keeps it up without a patch", 1, 1, 2, 2 * hour, hour, 1, ReasonActive, false, [2]time.Duration{0, 0}, "now"},
		{"a running count above active is left alone", 3, 1, 1, 2 * hour, hour, 3, ReasonActive, false, [2]time.Duration{0, 0}, "now"},
		{"a running count below active is left alone", 1, 3, 1, 2 * hour, hour, 1, ReasonActive, false, [2]time.Duration{0, 0}, "now"},
		{"empty past the hold-down scales down", 1, 1, 0, 2 * hour, hour, 0, ReasonIdle, true, [2]time.Duration{0, 0}, "kept"},
		{"empty past the hold-down scales 3 down to 0", 3, 1, 0, 2 * hour, hour, 0, ReasonIdle, true, [2]time.Duration{0, 0}, "kept"},
		{"empty and already at zero stays without a patch", 0, 2, 0, 2 * hour, hour, 0, ReasonIdle, false, [2]time.Duration{0, 0}, "kept"},
		{"empty within the hold-down stays up without a patch", 1, 1, 0, 10 * time.Minute, hour, 1, ReasonIdleHoldDown, false, [2]time.Duration{49 * time.Minute, 50 * time.Minute}, "kept"},
		{"empty within the hold-down leaves 3 alone", 3, 1, 0, 10 * time.Minute, hour, 3, ReasonIdleHoldDown, false, [2]time.Duration{49 * time.Minute, 50 * time.Minute}, "kept"},
		{"empty on first sight starts the hold-down", 1, 1, 0, 0, hour, 1, ReasonIdleHoldDown, false, [2]time.Duration{hour, hour}, "now"},
		{"empty on first sight at zero wakes it for the hold-down", 0, 2, 0, 0, hour, 2, ReasonIdleHoldDown, true, [2]time.Duration{hour, hour}, "now"},
		{"a future lastSeen stays up and is kept", 1, 1, 0, -hour, time.Minute, 1, ReasonIdleHoldDown, false, [2]time.Duration{time.Minute, time.Minute}, "kept"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := newNamespace(t)
			newDeployment(t, ns, "controller", tc.start)
			// The instances live in another namespace than the Deployment:
			// counting is across all namespaces.
			other := newNamespace(t)
			for i := range tc.instances {
				newInstance(t, widgetGVK, other, fmt.Sprintf("w%d", i), runLabel(ns))
			}
			var lastSeen time.Time
			if tc.lastSeen != 0 {
				lastSeen = time.Now().Add(-tc.lastSeen).Truncate(time.Second)
			}
			c := newCountingClient(t, nil, nil)

			before := time.Now()
			res, err := Reconcile(context.Background(), c, types.NamespacedName{Namespace: ns, Name: "controller"}, widgetGVK, runSelector(ns), lastSeen, tc.holdDown, tc.active)
			after := time.Now()
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.Replicas != tc.want || res.Reason != tc.wantReason || res.Exists != (tc.instances > 0) {
				t.Errorf("Reconcile = %d/%s/exists %v, want %d/%s/exists %v", res.Replicas, res.Reason, res.Exists, tc.want, tc.wantReason, tc.instances > 0)
			}
			if res.RequeueAfter < tc.wantRequeue[0] || res.RequeueAfter > tc.wantRequeue[1] {
				t.Errorf("RequeueAfter = %v, want within %v", res.RequeueAfter, tc.wantRequeue)
			}
			switch tc.wantLastSeen {
			case "now":
				if res.LastSeen.Before(before) || res.LastSeen.After(after) {
					t.Errorf("LastSeen = %v, want now (%v..%v)", res.LastSeen, before, after)
				}
			case "kept":
				if !res.LastSeen.Equal(lastSeen) {
					t.Errorf("LastSeen = %v, want it kept at %v", res.LastSeen, lastSeen)
				}
			}
			if res.Patched != tc.wantPatch {
				t.Errorf("Patched = %v, want %v", res.Patched, tc.wantPatch)
			}
			wantPatches := 0
			if tc.wantPatch {
				wantPatches = 1
			}
			if *c.patches != wantPatches {
				t.Errorf("scale patches = %d, want %d", *c.patches, wantPatches)
			}
			if got := scaleReplicas(t, ns, "controller"); got != tc.want {
				t.Errorf("scale replicas = %d, want %d", got, tc.want)
			}
			if got := deploymentReplicas(t, ns, "controller"); got != tc.want {
				t.Errorf("deployment spec.replicas = %d, want %d", got, tc.want)
			}
		})
	}
}

// Any error changes nothing: no patch, the scale as it was, and lastSeen
// passed back unchanged. A failed LIST never scales up from 0 nor down. The
// Result never reads as 0: its Replicas are the current ones when non-zero,
// else active (else 1, when active itself is the bad argument), with reason
// Unknown, so an owner that renders them without checking the error never
// scales a controller down.
func TestReconcileErrorChangesNothing(t *testing.T) {
	requireAPIServer(t)
	injected := errors.New("injected failure")
	const active = 2
	cases := []struct {
		name     string
		missing  bool
		unserved bool
		nilSel   bool
		holdDown time.Duration
		active   int32
		listErr  error
		patchErr error
		wantIs   error
	}{
		{name: "a LIST error", holdDown: hour, active: active, listErr: injected, wantIs: injected},
		{name: "a missing CRD", missing: true, holdDown: hour, active: active},
		{name: "an unserved version", unserved: true, holdDown: hour, active: active},
		{name: "a nil selector", nilSel: true, holdDown: hour, active: active},
		{name: "a zero holdDown", holdDown: 0, active: active},
		{name: "a negative holdDown", holdDown: -hour, active: active},
		{name: "a zero active", holdDown: hour, active: 0},
		{name: "a negative active", holdDown: hour, active: -1},
		{name: "a failed scale patch", holdDown: hour, active: active, patchErr: injected, wantIs: injected},
	}
	for _, tc := range cases {
		for _, start := range []int32{0, 1, 3} {
			t.Run(fmt.Sprintf("%s at %d", tc.name, start), func(t *testing.T) {
				ns := newNamespace(t)
				newDeployment(t, ns, "controller", start)
				// An instance exists for the start at 0, so a failure taken
				// for "active" would scale it up. None exists for the other
				// starts, and lastSeen is old, so a failure taken for "empty"
				// would scale them down. A failed patch is only reached when
				// the decision differs from the start: waking from 0, or
				// going idle.
				if start == 0 {
					newInstance(t, widgetGVK, ns, "w", runLabel(ns))
				}
				lastSeen := time.Now().Add(-2 * hour).Truncate(time.Second)
				gvk := widgetGVK
				switch {
				case tc.missing:
					gvk = missingGVK
				case tc.unserved:
					gvk = unservedWidgetGVK
				}
				sel := runSelector(ns)
				if tc.nilSel {
					sel = nil
				}
				c := newCountingClient(t, tc.listErr, tc.patchErr)

				res, err := Reconcile(context.Background(), c, types.NamespacedName{Namespace: ns, Name: "controller"}, gvk, sel, lastSeen, tc.holdDown, tc.active)
				if err == nil {
					t.Fatalf("Reconcile = %+v, want an error", res)
				}
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Errorf("error %v, want it to wrap %v", err, tc.wantIs)
				}
				wantReplicas := start
				if wantReplicas == 0 {
					wantReplicas = max(tc.active, 1)
				}
				want := Result{Replicas: wantReplicas, Reason: ReasonUnknown, LastSeen: lastSeen}
				if res.Replicas != want.Replicas || res.Reason != want.Reason || !res.LastSeen.Equal(want.LastSeen) ||
					res.Exists || res.RequeueAfter != 0 || res.Patched {
					t.Errorf("Result = %+v, want %+v", res, want)
				}
				if tc.patchErr == nil && *c.patches != 0 {
					t.Errorf("scale patches = %d, want 0", *c.patches)
				}
				if got := scaleReplicas(t, ns, "controller"); got != start {
					t.Errorf("scale replicas = %d, want them unchanged at %d", got, start)
				}
			})
		}
	}
}

// Errors found before the scale is read report active (or 1), never 0.
func TestReconcileRejectsBadArguments(t *testing.T) {
	sel := labels.Everything()
	lastSeen := time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)
	check := func(name string, res Result, err error, want int32) {
		t.Helper()
		if err == nil {
			t.Errorf("%s: want an error", name)
		}
		if res.Replicas != want || res.Reason != ReasonUnknown || !res.LastSeen.Equal(lastSeen) {
			t.Errorf("%s: Result = %+v, want %d replicas, reason Unknown and lastSeen kept", name, res, want)
		}
	}
	res, err := Reconcile(context.Background(), nil, types.NamespacedName{Namespace: "a", Name: "b"}, widgetGVK, sel, lastSeen, hour, 2)
	check("a nil client", res, err, 2)
	res, err = Reconcile(context.Background(), nil, types.NamespacedName{Namespace: "a", Name: "b"}, widgetGVK, sel, lastSeen, hour, 0)
	check("a nil client and a zero active", res, err, 1)
	for _, d := range []types.NamespacedName{{Name: "b"}, {Namespace: "a"}} {
		res, err := Reconcile(context.Background(), countingClient{}, d, widgetGVK, sel, lastSeen, hour, 3)
		check(fmt.Sprintf("deployment %q", d), res, err, 3)
	}
}

// A Deployment that doesn't exist is an error that changes nothing, and
// reports active.
func TestReconcileMissingDeploymentIsAnError(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	lastSeen := time.Now().Add(-2 * hour).Truncate(time.Second)
	c := newCountingClient(t, nil, nil)
	res, err := Reconcile(context.Background(), c, types.NamespacedName{Namespace: ns, Name: "absent"}, widgetGVK, runSelector(ns), lastSeen, hour, 2)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Reconcile = (%+v, %v), want NotFound", res, err)
	}
	if res.Replicas != 2 || res.Reason != ReasonUnknown || !res.LastSeen.Equal(lastSeen) || *c.patches != 0 {
		t.Errorf("Result %+v and %d patches, want 2 replicas, reason Unknown, lastSeen unchanged and no patch", res, *c.patches)
	}
}

// Reconcile needs list on the watched resource and get and patch on
// deployments/scale, and nothing on the Deployment itself: it reads the
// current replicas from the scale subresource and patches only that.
func TestReconcileNeedsOnlyTheScaleSubresource(t *testing.T) {
	requireAPIServer(t)
	ctx := context.Background()
	ns := newNamespace(t)
	newDeployment(t, ns, "controller", 0)
	newInstance(t, widgetGVK, ns, "w", runLabel(ns))

	user, err := testEnv.AddUser(envtest.User{Name: "scaler-" + ns}, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant := func(role client.Object, rules []rbacv1.PolicyRule, binding client.Object) {
		t.Helper()
		switch r := role.(type) {
		case *rbacv1.ClusterRole:
			r.Rules = rules
		case *rbacv1.Role:
			r.Rules = rules
		}
		if err := k8s.Create(ctx, role); err != nil {
			t.Fatal(err)
		}
		if err := k8s.Create(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	subject := []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: "scaler-" + ns}}
	grant(&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "list-widgets-" + ns}},
		[]rbacv1.PolicyRule{{APIGroups: []string{testGroup}, Resources: []string{"widgets"}, Verbs: []string{"list"}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "list-widgets-" + ns}, Subjects: subject,
			RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "list-widgets-" + ns}})
	grant(&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "scale"}},
		[]rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"deployments/scale"}, Verbs: []string{"get", "patch"}}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "scale"}, Subjects: subject,
			RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "scale"}})

	c, err := client.New(user.Config(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	// The grant is narrow enough to mean something: the Deployment itself is
	// out of reach.
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "controller"}, &appsv1.Deployment{}); !apierrors.IsForbidden(err) {
		t.Fatalf("reading the Deployment as the scaler: %v, want Forbidden", err)
	}

	key := types.NamespacedName{Namespace: ns, Name: "controller"}
	// RBAC bindings take effect at once in kube-apiserver, but retry the
	// first call briefly in case the authorizer's informer lags.
	var res Result
	eventually(t, "the scaler's grant to take effect", func() bool {
		res, err = Reconcile(ctx, c, key, widgetGVK, runSelector(ns), time.Time{}, hour, 1)
		return !apierrors.IsForbidden(err)
	})
	if err != nil || res.Replicas != 1 || !res.Patched {
		t.Fatalf("Reconcile as the scaler = (%+v, %v), want scaled to 1", res, err)
	}
	if got := deploymentReplicas(t, ns, "controller"); got != 1 {
		t.Errorf("deployment spec.replicas = %d, want 1", got)
	}

	// And back down, with no instance left and the hold-down passed.
	if err := k8s.DeleteAllOf(ctx, newInstanceObj(), client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	res, err = Reconcile(ctx, c, key, widgetGVK, runSelector(ns), time.Now().Add(-2*hour), hour, 1)
	if err != nil || res.Replicas != 0 || !res.Patched {
		t.Fatalf("Reconcile as the scaler = (%+v, %v), want scaled to 0", res, err)
	}
	if got := deploymentReplicas(t, ns, "controller"); got != 0 {
		t.Errorf("deployment spec.replicas = %d, want 0", got)
	}
}

func newInstanceObj() client.Object {
	o := &metav1.PartialObjectMetadata{}
	o.SetGroupVersionKind(widgetGVK)
	return o
}
