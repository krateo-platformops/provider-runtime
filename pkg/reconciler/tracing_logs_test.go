package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"strings"
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

func jsonRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not a JSON record: %v\n%s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// design-tracing §10, test 10: the reconciler's records carry the root span's
// ids; a provider Observe that binds its own Logger to its ctx carries the
// observe span's id; with tracing off nothing is stamped, even with an
// annotation.
func TestTracingLogCorrelation(t *testing.T) {
	t.Run("TracingOn", func(t *testing.T) {
		var buf bytes.Buffer
		log := logging.NewSlogLogger(*slog.New(logging.NewOTelJSONHandler(slog.LevelDebug, &buf)))
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops}
		p.observeHook = func(ctx context.Context, _ *Widget) {
			logging.WithContext(log, ctx).Info("provider observing")
		}
		mustReconcile(t, newTraceReconciler(s, p, WithTracerProvider(tp), WithLogger(log)), "w")

		root := lastRoot(t, rec)
		var observe string
		for _, sp := range rec.Ended() {
			if sp.Name() == tracing.SpanObserve {
				observe = sp.SpanContext().SpanID().String()
			}
		}
		recs := jsonRecords(t, &buf)
		found := map[string]bool{}
		for _, r := range recs {
			msg, _ := r["msg"].(string)
			found[msg] = true
			if r["trace_id"] != root.SpanContext().TraceID().String() {
				t.Errorf("%q: trace_id %v, want the root's %s", msg, r["trace_id"], root.SpanContext().TraceID())
			}
			switch msg {
			case "provider observing":
				if r["span_id"] != observe {
					t.Errorf("the provider's record has span_id %v, want observe's %s", r["span_id"], observe)
				}
			default:
				if r["span_id"] != root.SpanContext().SpanID().String() {
					t.Errorf("%q: span_id %v, want the root's %s", msg, r["span_id"], root.SpanContext().SpanID())
				}
			}
		}
		if !found["Reconciling"] || !found["provider observing"] || !found["External resource is up to date"] {
			t.Errorf("records = %v, want Reconciling, the provider's and a later reconciler record", found)
		}
	})

	t.Run("TracingOff", func(t *testing.T) {
		tracing.SetTracerProvider(nil)
		var buf bytes.Buffer
		log := logging.NewSlogLogger(*slog.New(logging.NewOTelJSONHandler(slog.LevelDebug, &buf)))
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithLogger(log)), "w")
		for _, r := range jsonRecords(t, &buf) {
			if _, ok := r["trace_id"]; ok {
				t.Errorf("%v: stamped with a trace_id with tracing off", r["msg"])
			}
		}
	})
}

// design-tracing §10, test 9, the source tests. They read reconciler.go:
//   - every SetConditions(...ReconcileError(...)) that a nil-error return
//     follows marks the root (pass.markError) before that return;
//   - every kerrors.IsConflict records its step (pass.conflict).
func TestTracingSourceRules(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "reconciler.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}

	isCall := func(n ast.Node, sel string) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return false
		}
		s, ok := c.Fun.(*ast.SelectorExpr)
		return ok && s.Sel.Name == sel
	}
	contains := func(n ast.Node, sel string) bool {
		found := false
		ast.Inspect(n, func(x ast.Node) bool {
			if isCall(x, sel) {
				found = true
			}
			return !found
		})
		return found
	}
	nilErrorReturn := func(n ast.Stmt) bool {
		r, ok := n.(*ast.ReturnStmt)
		if !ok || len(r.Results) != 2 {
			return false
		}
		id, ok := r.Results[1].(*ast.Ident)
		return ok && id.Name == "nil"
	}

	conflicts, reconcileErrors := 0, 0
	ast.Inspect(f, func(n ast.Node) bool {
		// Statement lists live in blocks and in switch and select clauses.
		var list []ast.Stmt
		switch b := n.(type) {
		case *ast.BlockStmt:
			list = b.List
		case *ast.CaseClause:
			list = b.Body
		case *ast.CommClause:
			list = b.Body
		default:
			return true
		}
		block := struct{ List []ast.Stmt }{list}
		for i, stmt := range block.List {
			// A ReconcileError set in this block, then a nil-error return in the same block.
			if expr, ok := stmt.(*ast.ExprStmt); ok && isCall(expr.X, "SetConditions") && contains(expr.X, "ReconcileError") {
				reconcileErrors++
				marked := false
				for _, later := range block.List[i+1:] {
					if contains(later, "markError") {
						marked = true
					}
					if nilErrorReturn(later) && !marked {
						t.Errorf("%s: ReconcileError is set, then the pass returns a nil error without pass.markError",
							fset.Position(stmt.Pos()))
					}
				}
			}
			// if kerrors.IsConflict(err) { ... pass.conflict(step) ... }
			if ifs, ok := stmt.(*ast.IfStmt); ok && isCall(ifs.Cond, "IsConflict") {
				conflicts++
				if !contains(ifs.Body, "conflict") {
					t.Errorf("%s: kerrors.IsConflict without pass.conflict(step)", fset.Position(ifs.Pos()))
				}
			}
		}
		return true
	})
	if conflicts != 9 {
		t.Errorf("found %d IsConflict sites, want the 9 design §4.5 lists; update tracing.Steps and this test together", conflicts)
	}
	if reconcileErrors == 0 {
		t.Error("found no ReconcileError site: the source test is reading the wrong thing")
	}
}
