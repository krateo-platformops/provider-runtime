package reconciler

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
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
			policySynced(m)
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
	p := managementpolicy.Parse("observe-delete")

	got := updateWithheldReady(prv1.Available(), p)
	if got.Status != metav1.ConditionTrue || got.Reason != prv1.ReasonUpdateWithheldByPolicy || !strings.Contains(got.Message, `"observe,delete"`) {
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
	k := strings.Index(string(src), "policySynced(managed)")
	if i < 0 || j < 0 || k < 0 || !(i < k && k < j) {
		t.Fatalf("writeStatus must call policySynced before its Status().Update (offsets %d, %d, %d)", i, k, j)
	}
}
