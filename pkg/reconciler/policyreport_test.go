package reconciler

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
)

func policyManaged(mp, dp string, synced prv1.Condition) *fake.Managed {
	m := &fake.Managed{}
	a := map[string]string{}
	if mp != "" {
		a[meta.AnnotationKeyManagementPolicy] = mp
	}
	if dp != "" {
		a[meta.AnnotationKeyDeletionPolicy] = dp
	}
	m.SetAnnotations(a)
	m.SetConditions(prv1.Available(), synced)
	return m
}

// TestPolicySynced pins the Synced report's precedence (the user's rules of
// 2026-11-21, unstructured-runtime#110): an invalid management policy wins over
// the pass's outcome, success or error; else an invalid deletion policy does,
// on every pass; paused and deferred passes keep their reasons; Ready is never
// touched; valid annotations change nothing.
func TestPolicySynced(t *testing.T) {
	passErr := errors.New("boom")
	cases := []struct {
		name       string
		mp, dp     string
		synced     prv1.Condition
		wantReason prv1.ConditionReason
		wantMsg    []string
	}{
		{"valid annotations keep success", "observe,create", "orphan", prv1.ReconcileSuccess(), prv1.ReasonReconcileSuccess, nil},
		{"valid annotations keep an error", "", "", prv1.ReconcileError(passErr), prv1.ReasonReconcileError, nil},
		{"invalid policy replaces success", "observe-create", "", prv1.ReconcileSuccess(), prv1.ReasonManagementPolicyInvalid, []string{"observe-create", "acting on nothing"}},
		{"invalid policy replaces an error", "observe-create", "", prv1.ReconcileError(passErr), prv1.ReasonManagementPolicyInvalid, []string{"observe-create"}},
		{"both invalid: the policy wins and names the deletion policy", "observe-create", "orphaned", prv1.ReconcileSuccess(), prv1.ReasonManagementPolicyInvalid, []string{"observe-create", "also", "orphaned"}},
		{"invalid deletion policy replaces success", "", "orphaned", prv1.ReconcileSuccess(), prv1.ReasonDeletionPolicyInvalid, []string{"orphaned", "kept"}},
		{"invalid deletion policy replaces an error", "default", "Orphan", prv1.ReconcileError(passErr), prv1.ReasonDeletionPolicyInvalid, []string{"Orphan"}},
		{"paused keeps its reason", "observe-create", "orphaned", prv1.ReconcilePaused(), prv1.ReasonReconcilePaused, nil},
		{"deferred keeps its reason", "observe-create", "orphaned", prv1.ReconcileDeferred("rate-limited"), prv1.ReasonReconcileDeferred, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := policyManaged(tc.mp, tc.dp, tc.synced)
			policySynced(m, passConditions{})
			got := m.GetCondition(prv1.TypeSynced)
			if got.Reason != tc.wantReason {
				t.Fatalf("Synced reason = %s, want %s (message %q)", got.Reason, tc.wantReason, got.Message)
			}
			if tc.wantReason == prv1.ReasonManagementPolicyInvalid || tc.wantReason == prv1.ReasonDeletionPolicyInvalid {
				if got.Status != metav1.ConditionFalse {
					t.Errorf("Synced status = %s, want False", got.Status)
				}
			}
			for _, w := range tc.wantMsg {
				if !strings.Contains(got.Message, w) {
					t.Errorf("Synced message %q lacks %q", got.Message, w)
				}
			}
			if r := m.GetCondition(prv1.TypeReady); r.Reason != prv1.ReasonAvailable {
				t.Errorf("Ready reason = %s, want it untouched (Available)", r.Reason)
			}
		})
	}
}

// TestUpdateWithheldReady: a healthy Ready becomes UpdateWithheldByPolicy
// (True); an unhealthy one keeps its status and reason, with the note added
// once, however many passes add it.
func TestUpdateWithheldReady(t *testing.T) {
	p := policyManaged("observe-delete", "", prv1.ReconcileSuccess())

	got := updateWithheldReady(prv1.Available(), p)
	if got.Status != metav1.ConditionTrue || got.Reason != prv1.ReasonUpdateWithheldByPolicy || !strings.Contains(got.Message, `"observe-delete"`) {
		t.Errorf("healthy: got %s/%s %q, want True/UpdateWithheldByPolicy naming the policy", got.Status, got.Reason, got.Message)
	}

	bad := prv1.Unavailable()
	bad.Message = "the API reports it degraded"
	got = updateWithheldReady(updateWithheldReady(bad, p), p)
	if got.Status != metav1.ConditionFalse || got.Reason != prv1.ReasonUnavailable {
		t.Errorf("unhealthy: got %s/%s, want the provider's False/Unavailable kept", got.Status, got.Reason)
	}
	if want := "the API reports it degraded; " + messageUpdateWithheld; got.Message != want {
		t.Errorf("unhealthy message = %q, want %q", got.Message, want)
	}

	got = updateWithheldReady(prv1.Unavailable(), p)
	if got.Message != messageUpdateWithheld {
		t.Errorf("unhealthy, no message: got %q, want %q", got.Message, messageUpdateWithheld)
	}
}

// TestNoStatusWriteBypassesWriteStatus: writeStatus applies policySynced, so
// every status write of the reconciler must go through it. The package's only
// Status().Update or Status().Patch call must be writeStatus's.
func TestNoStatusWriteBypassesWriteStatus(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var sites []string
	scanned := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			// A status write is <client>.Status().Update(...) or .Patch(...).
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Update" && sel.Sel.Name != "Patch") {
				return true
			}
			inner, ok := sel.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			if isel, ok := inner.Fun.(*ast.SelectorExpr); ok && isel.Sel.Name == "Status" && len(inner.Args) == 0 {
				sites = append(sites, fset.Position(call.Pos()).String())
			}
			return true
		})
	}
	if scanned < 3 {
		t.Fatalf("premise: scanned %d files", scanned)
	}
	if len(sites) != 1 || !strings.HasPrefix(sites[0], "reconciler.go:") {
		t.Fatalf("status writes %v: want exactly one, writeStatus's in reconciler.go, so policySynced can't be bypassed", sites)
	}
	src, err := os.ReadFile("reconciler.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(string(src), "writeStatus := func(passErr error) error {")
	j := strings.Index(string(src), "r.client.Status().Update(ctx, managed)")
	k := strings.Index(string(src), "policySynced(managed, start)")
	if i < 0 || j < 0 || k < 0 || i >= k || k >= j {
		t.Fatalf("writeStatus must call policySynced before its Status().Update (offsets %d, %d, %d)", i, k, j)
	}
}

// TestPolicyReportsKeepTheirTransitionTime: a steady withheld or invalid state
// writes the same conditions on every pass, so their LastTransitionTime must
// stay the one of the real transition. Pass 1 reaches the state; the stored
// times are then set to a fixed old time; pass 2 must leave them there.
// Without passConditions.keep, Observe's Ready and each path's Synced replace
// the stored report before it is set again, with a new time, so every poll
// wrote a changed status, and a watching controller reconciled again at once.
func TestPolicyReportsKeepTheirTransitionTime(t *testing.T) {
	old := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	absent := mgmtTableStates[0]
	upToDate := mgmtTableStates[1]
	drifted := mgmtTableStates[2]
	cases := []struct {
		name           string
		mp, dp         string
		st             mgmtTableState
		ready, synced  prv1.ConditionReason
		changePolicyTo string // non-empty: pass 2 runs under this policy, so Ready must move
		wantReadyMoved bool
		unhealthy      bool // Observe reports the existing resource Unavailable, "degraded"
	}{
		{"control: default, up to date", "", "", upToDate, prv1.ReasonAvailable, prv1.ReasonReconcileSuccess, "", false, false},
		{"observe-only, absent", "observe", "", absent, prv1.ReasonCreateWithheldByPolicy, prv1.ReasonReconcileError, "", false, false},
		{"update without create, absent", "observe,update", "", absent, prv1.ReasonCreateWithheldByPolicy, prv1.ReasonReconcileSuccess, "", false, false},
		{"update withheld, drifted", "observe-delete", "", drifted, prv1.ReasonUpdateWithheldByPolicy, prv1.ReasonReconcileSuccess, "", false, false},
		{"invalid policy, up to date", "observe-create", "", upToDate, prv1.ReasonAvailable, prv1.ReasonManagementPolicyInvalid, "", false, false},
		{"invalid policy, absent", "observe-create", "", absent, prv1.ReasonCreateWithheldByPolicy, prv1.ReasonManagementPolicyInvalid, "", false, false},
		{"invalid deletion policy, up to date", "", "orphaned", upToDate, prv1.ReasonAvailable, prv1.ReasonDeletionPolicyInvalid, "", false, false},
		{"control: the policy changes, so Ready moves", "observe", "", absent, prv1.ReasonCreating, prv1.ReasonReconcileSuccess, meta.ManagementPolicyDefault, true, false},
		// The unhealthy Ready keeps the provider's reason, with the note added;
		// that condition too must keep its stored time.
		{"update withheld, drifted and unhealthy", "observe-delete", "", drifted, prv1.ReasonUnavailable, prv1.ReasonReconcileSuccess, "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ann := map[string]string{}
			if tc.mp != "" {
				ann[meta.AnnotationKeyManagementPolicy] = tc.mp
			}
			if tc.dp != "" {
				ann[meta.AnnotationKeyDeletionPolicy] = tc.dp
			}
			w := newWidget("w", ann)
			w.SetFinalizers([]string{FinalizerName})
			s := newTraceServer(t, w)
			var calls mgmtTableCalls
			r := NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK),
				WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
					c := mgmtTableClient(tc.st, &calls).(*ExternalClientFns)
					if tc.unhealthy {
						observe := c.ObserveFn
						c.ObserveFn = func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
							o, err := observe(ctx, mg)
							mg.SetConditions(prv1.Unavailable().WithMessage("degraded"))
							return o, err
						}
					}
					return c, nil
				})),
				WithRecorder(&eventCapture{}),
			)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			key := reqFor("w").NamespacedName

			_, _ = r.Reconcile(ctx, reqFor("w"))
			stored := &Widget{}
			if err := s.raw.Get(ctx, key, stored); err != nil {
				t.Fatal(err)
			}
			for _, ct := range []prv1.ConditionType{prv1.TypeReady, prv1.TypeSynced} {
				c := stored.GetCondition(ct)
				c.LastTransitionTime = old
				stored.SetConditions(prv1.Condition{Type: ct}) // differs, so the next call replaces it
				stored.SetConditions(c)
			}
			if err := s.raw.Status().Update(ctx, stored); err != nil {
				t.Fatal(err)
			}
			if tc.changePolicyTo != "" {
				if err := s.raw.Get(ctx, key, stored); err != nil {
					t.Fatal(err)
				}
				meta.AddAnnotations(stored, map[string]string{meta.AnnotationKeyManagementPolicy: tc.changePolicyTo})
				if err := s.raw.Update(ctx, stored); err != nil {
					t.Fatal(err)
				}
			}

			_, _ = r.Reconcile(ctx, reqFor("w"))
			if err := s.raw.Get(ctx, key, stored); err != nil {
				t.Fatal(err)
			}
			ready, synced := stored.GetCondition(prv1.TypeReady), stored.GetCondition(prv1.TypeSynced)
			if ready.Reason != tc.ready || synced.Reason != tc.synced {
				t.Fatalf("after pass 2: Ready %s, Synced %s; want %s, %s", ready.Reason, synced.Reason, tc.ready, tc.synced)
			}
			if want := "degraded; " + messageUpdateWithheld; tc.unhealthy && ready.Message != want {
				t.Fatalf("after pass 2: Ready message %q, want %q", ready.Message, want)
			}
			if moved := !ready.LastTransitionTime.Equal(&old); moved != tc.wantReadyMoved {
				t.Errorf("Ready's LastTransitionTime %v (old %v): moved=%v, want %v", ready.LastTransitionTime, old, moved, tc.wantReadyMoved)
			}
			if !tc.wantReadyMoved && !synced.LastTransitionTime.Equal(&old) {
				t.Errorf("Synced's LastTransitionTime moved to %v in a steady state", synced.LastTransitionTime)
			}
		})
	}
}

// TestDeletingKeepsItsTransitionTime: while the external resource is still
// being deleted, every pass sets Ready=Deleting again after Observe set its
// own Ready. Deleting must keep the time of its real transition, or each pass
// writes a changed status and a watching controller reconciles again at once,
// calling Delete again.
func TestDeletingKeepsItsTransitionTime(t *testing.T) {
	old := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := newWidget("w", map[string]string{})
	w.SetFinalizers([]string{FinalizerName})
	ts := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w.SetDeletionTimestamp(&ts)
	s := newTraceServer(t, w)
	var calls mgmtTableCalls
	st := mgmtTableState{name: "deleting-present", exists: true, upToDate: true, deleting: true}
	r := NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return mgmtTableClient(st, &calls), nil
		})),
		WithRecorder(&eventCapture{}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := reqFor("w").NamespacedName

	_, _ = r.Reconcile(ctx, reqFor("w"))
	stored := &Widget{}
	if err := s.raw.Get(ctx, key, stored); err != nil {
		t.Fatal(err)
	}
	c := stored.GetCondition(prv1.TypeReady)
	if c.Reason != prv1.ReasonDeleting {
		t.Fatalf("after pass 1: Ready %s, want Deleting", c.Reason)
	}
	c.LastTransitionTime = old
	stored.SetConditions(prv1.Condition{Type: prv1.TypeReady}) // differs, so the next call replaces it
	stored.SetConditions(c)
	if err := s.raw.Status().Update(ctx, stored); err != nil {
		t.Fatal(err)
	}

	_, _ = r.Reconcile(ctx, reqFor("w"))
	if err := s.raw.Get(ctx, key, stored); err != nil {
		t.Fatal(err)
	}
	if calls.delete != 2 {
		t.Fatalf("Delete called %d times, want 2: pass 2 didn't run the delete path", calls.delete)
	}
	if got := stored.GetCondition(prv1.TypeReady); got.Reason != prv1.ReasonDeleting || !got.LastTransitionTime.Equal(&old) {
		t.Errorf("after pass 2: Ready %s at %v, want Deleting at %v", got.Reason, got.LastTransitionTime, old)
	}
}

// TestCreatingKeepsItsTransitionTime: inside the create-recovery window (a
// recent external-create-pending marker, the resource not observed yet) every
// pass refuses to create and sets Ready=Creating again after Observe set its
// own Ready. Creating must keep the time of its real transition.
func TestCreatingKeepsItsTransitionTime(t *testing.T) {
	old := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := newWidget("w", map[string]string{})
	w.SetFinalizers([]string{FinalizerName})
	meta.SetExternalCreatePending(w, time.Now())
	s := newTraceServer(t, w)
	var calls mgmtTableCalls
	r := NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return mgmtTableClient(mgmtTableStates[0], &calls), nil
		})),
		WithRecorder(&eventCapture{}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := reqFor("w").NamespacedName

	_, _ = r.Reconcile(ctx, reqFor("w"))
	stored := &Widget{}
	if err := s.raw.Get(ctx, key, stored); err != nil {
		t.Fatal(err)
	}
	c := stored.GetCondition(prv1.TypeReady)
	if c.Reason != prv1.ReasonCreating {
		t.Fatalf("after pass 1: Ready %s, want Creating (the recovery window)", c.Reason)
	}
	c.LastTransitionTime = old
	stored.SetConditions(prv1.Condition{Type: prv1.TypeReady}) // differs, so the next call replaces it
	stored.SetConditions(c)
	if err := s.raw.Status().Update(ctx, stored); err != nil {
		t.Fatal(err)
	}

	_, _ = r.Reconcile(ctx, reqFor("w"))
	if err := s.raw.Get(ctx, key, stored); err != nil {
		t.Fatal(err)
	}
	if calls.observe != 2 || calls.create != 0 {
		t.Fatalf("observe %d, create %d; want 2 and 0: the passes weren't in the recovery window", calls.observe, calls.create)
	}
	if got := stored.GetCondition(prv1.TypeReady); got.Reason != prv1.ReasonCreating || !got.LastTransitionTime.Equal(&old) {
		t.Errorf("after pass 2: Ready %s at %v, want Creating at %v", got.Reason, got.LastTransitionTime, old)
	}
}
