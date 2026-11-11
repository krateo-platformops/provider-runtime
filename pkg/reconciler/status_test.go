package reconciler

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// Status writes that conflict with a handler's own write
// (docs/design-status-write-conflict.md, option 1, §7.1 and §12).

const handlerState = "set-by-handler"

// handlerWrite is an ExternalClient method's own write of the widget's status,
// through a copy it reads fresh, as oasgen-provider's Update did before 0.59.0
// (oasgen-provider#579). mirror says what it copies back onto the pointer the
// runtime holds.
func handlerWrite(t *testing.T, s *traceServer, mirror string) func(ctx context.Context, w *Widget) {
	return func(ctx context.Context, w *Widget) {
		cp := &Widget{}
		if err := s.raw.Get(ctx, client.ObjectKeyFromObject(w), cp); err != nil {
			t.Errorf("handler read: %v", err)
			return
		}
		cp.Status.State = handlerState
		if err := s.raw.Status().Update(ctx, cp); err != nil {
			t.Errorf("handler write: %v", err)
			return
		}
		switch mirror {
		case "version and fields":
			w.SetResourceVersion(cp.GetResourceVersion())
			w.Status.State = cp.Status.State
		case "version only":
			w.SetResourceVersion(cp.GetResourceVersion())
		}
	}
}

// liveReader is an APIReader over s that stamps managedFields on what it reads,
// as the API server would.
type liveReader struct {
	client.Reader
	fields []metav1.ManagedFieldsEntry
	err    error
	gets   int
}

func (l *liveReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	l.gets++
	if l.err != nil {
		return l.err
	}
	if err := l.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	obj.SetManagedFields(l.fields)
	return nil
}

var (
	fieldsOlder = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	fieldsNewer = time.Date(2026, 11, 11, 10, 0, 5, 0, time.UTC)
)

func stampedFields() []metav1.ManagedFieldsEntry {
	return []metav1.ManagedFieldsEntry{
		{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: fieldsOlder}},
		{Manager: "my-provider", Operation: metav1.ManagedFieldsOperationUpdate, Subresource: "status", Time: &metav1.Time{Time: fieldsNewer}},
		{Manager: "no-time", Operation: metav1.ManagedFieldsOperationApply},
	}
}

// plainMessage is the status conflict's message without a live read.
func plainMessage(rv string, passErr string) string {
	m := errUpdateManagedStatus + ": the object changed during this pass (read at resourceVersion " + rv + "): " + errStatusConflictCause
	if passErr != "" {
		m += "; pass error: " + passErr
	}
	return m
}

func TestStatusConflictNamesItsCause(t *testing.T) {
	errBoom := errors.New("boom")

	cases := map[string]struct {
		// provider builds the pass, on a server over one widget that has work to do.
		provider func(t *testing.T, s *traceServer) *widgetProvider
		wantPass error
		wantMsg  string
	}{
		// oasgen-provider#579's case, row 20 of §2.2: Update wrote and succeeded.
		"UpdateWroteAndSucceeded": {
			provider: func(t *testing.T, s *traceServer) *widgetProvider {
				return &widgetProvider{ops: s.ops, updateHook: handlerWrite(t, s, "")}
			},
		},
		// Row 19: Update wrote, then failed. The pass's own error must survive the conflict.
		"UpdateWroteAndFailed": {
			provider: func(t *testing.T, s *traceServer) *widgetProvider {
				return &widgetProvider{ops: s.ops, updateHook: handlerWrite(t, s, ""), updateErr: errBoom}
			},
			wantPass: errBoom,
			wantMsg:  errReconcileUpdate + ": boom",
		},
		// Row 5: Observe failed and the status write conflicted (§2.3's example).
		"ObserveFailed": {
			provider: func(t *testing.T, s *traceServer) *widgetProvider {
				s.faults.status = func(client.Object) error { return conflictErr() }
				return &widgetProvider{ops: s.ops, observeErr: errBoom}
			},
			wantPass: errBoom,
			wantMsg:  errReconcileObserve + ": boom",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, newWidget("w", nil))
			rv := s.widget("w").GetResourceVersion()
			tp, rec := tracer(t)

			res, err := newTraceReconciler(s, tc.provider(t, s), WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w"))

			if err == nil {
				t.Fatalf("Reconcile = %v, nil; a status conflict must stay an error", res)
			}
			if res != (reconcile.Result{}) {
				t.Errorf("result = %+v, want none: the error requeues", res)
			}
			if want := plainMessage(rv, tc.wantMsg); err.Error() != want {
				t.Errorf("error:\n got %q\nwant %q", err.Error(), want)
			}
			if !strings.HasPrefix(err.Error(), errUpdateManagedStatus+": ") {
				t.Errorf("error %q lost the %q prefix that searches match", err, errUpdateManagedStatus)
			}
			if !kerrors.IsConflict(err) {
				t.Errorf("kerrors.IsConflict(%q) = false; the conflict must stay in the chain", err)
			}
			if tc.wantPass != nil && !errors.Is(err, tc.wantPass) {
				t.Errorf("errors.Is(err, pass error) = false; the pass's own error must stay in the chain")
			}
			root := lastRoot(t, rec)
			if got, ok := eventAttr(root, tracing.EventConflict, string(tracing.AttrStep)); !ok || got != string(tracing.StepStatus) {
				t.Errorf("k8s.conflict step = %q (%v), want %s; events %v", got, ok, tracing.StepStatus, events(root))
			}
			if root.Status().Code != codes.Error {
				t.Errorf("root status = %v, want Error", root.Status())
			}
			// No retry and no merge: one status write, refused.
			if n := s.ops.count("status") + s.ops.count("status-failed") + s.ops.count("status-rejected"); n != 1 {
				t.Errorf("status writes = %d (ops %v), want exactly one", n, s.ops.list())
			}
		})
	}
}

// The unresolved create inside its recovery window (row 6) returns nil, but
// errCreateIncomplete is the error it reports, so a conflict there keeps it.
func TestStatusConflictKeepsTheUnresolvedCreatesError(t *testing.T) {
	w := newWidget("w", nil)
	meta.SetExternalCreatePending(w, time.Now())
	s := newTraceServer(t, w)
	rv := s.widget("w").GetResourceVersion()
	s.faults.status = func(client.Object) error { return conflictErr() }
	p := &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}

	_, err := newTraceReconciler(s, p).Reconcile(context.Background(), reqFor("w"))

	if want := plainMessage(rv, errCreateIncomplete); err == nil || err.Error() != want {
		t.Errorf("error:\n got %v\nwant %q", err, want)
	}
}

// updateWritesAndFails reconciles a widget whose Update writes its status
// without mirroring, then fails, with the options o builds over the server.
func updateWritesAndFails(t *testing.T, errBoom error, o func(*traceServer) []ReconcilerOption) (*traceServer, reconcile.Result, error) {
	t.Helper()
	s := newTraceServer(t, newWidget("w", nil))
	p := &widgetProvider{ops: s.ops, updateHook: handlerWrite(t, s, ""), updateErr: errBoom}
	res, err := newTraceReconciler(s, p, o(s)...).Reconcile(context.Background(), reqFor("w"))
	return s, res, err
}

func TestStatusConflictReadsTheLiveObject(t *testing.T) {
	errBoom := errors.New("boom")

	var reader *liveReader
	s, res, err := updateWritesAndFails(t, errBoom, func(s *traceServer) []ReconcilerOption {
		reader = &liveReader{Reader: s.raw, fields: stampedFields()}
		return []ReconcilerOption{WithAPIReader(reader)}
	})
	if err == nil {
		t.Fatal("Reconcile returned nil; a status conflict must stay an error")
	}
	if reader.gets != 1 {
		t.Errorf("live reads = %d, want exactly one per status conflict", reader.gets)
	}

	live := "live resourceVersion " + s.widget("w").GetResourceVersion() +
		", status keys that differ from the pass's read: [state]" +
		`, newest managedFields entry (a hint, not proof of who wrote): manager "my-provider", Update of status, at 2026-11-11T10:00:05Z`
	msg := err.Error()
	if want := "; " + live + "; pass error: " + errReconcileUpdate + ": boom"; !strings.HasSuffix(msg, want) {
		t.Errorf("error:\n got %q\nwant it to end with %q", msg, want)
	}
	if !kerrors.IsConflict(err) || !errors.Is(err, errBoom) {
		t.Errorf("the conflict and the pass error must stay in the chain: %v", err)
	}

	// Nothing is decided from the read: without it, the same pass returns the same
	// result and makes the same writes, and the message only lacks the live part.
	s2, res2, err2 := updateWritesAndFails(t, errBoom, func(*traceServer) []ReconcilerOption { return nil })
	if res != res2 {
		t.Errorf("result with a live reader %+v, without %+v; want the same", res, res2)
	}
	if got, want := strings.Join(s.ops.list(), ","), strings.Join(s2.ops.list(), ","); got != want {
		t.Errorf("ops with a live reader %s, without %s; want the same", got, want)
	}
	if got := strings.Replace(msg, "; "+live, "", 1); err2 == nil || got != err2.Error() {
		t.Errorf("message without the live part:\n got %q\nwant %v", got, err2)
	}
}

// With a live reader and no other writer's status change, the message says
// so, rather than leaving the key list out.
func TestStatusConflictNoStatusKeyDiffers(t *testing.T) {
	s := newTraceServer(t, newWidget("w", nil))
	s.faults.status = func(client.Object) error { return conflictErr() }
	p := &widgetProvider{ops: s.ops}

	_, err := newTraceReconciler(s, p, WithAPIReader(&liveReader{Reader: s.raw})).Reconcile(context.Background(), reqFor("w"))

	want := "; live resourceVersion " + s.widget("w").GetResourceVersion() + ", no status key differs from the pass's read"
	if err == nil || !strings.HasSuffix(err.Error(), want) {
		t.Errorf("error:\n got %v\nwant it to end with %q", err, want)
	}
}

// Without a usable live reader the message is the plain one, and nothing
// panics. newTraceReconciler builds on fake.Manager, which embeds a nil
// manager.Manager, as consumers' tests do (§8.3): a GetAPIReader call on it
// would panic, so these cases pass only if NewReconciler never makes one.
func TestStatusConflictWithoutALiveReader(t *testing.T) {
	cases := map[string]func(*traceServer) []ReconcilerOption{
		"FakeManagerAndNoOption": func(*traceServer) []ReconcilerOption { return nil },
		"NilReader":              func(*traceServer) []ReconcilerOption { return []ReconcilerOption{WithAPIReader(nil)} },
		"ReadFails": func(s *traceServer) []ReconcilerOption {
			return []ReconcilerOption{WithAPIReader(&liveReader{Reader: s.raw, err: errors.New("connection refused")})}
		},
		"ObjectGone": func(s *traceServer) []ReconcilerOption {
			return []ReconcilerOption{WithAPIReader(&liveReader{Reader: s.raw, err: kerrors.NewNotFound(widgetGR, "w")})}
		},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, newWidget("w", nil))
			rv := s.widget("w").GetResourceVersion()
			p := &widgetProvider{ops: s.ops, updateHook: handlerWrite(t, s, "")}

			_, err := newTraceReconciler(s, p, opts(s)...).Reconcile(context.Background(), reqFor("w"))

			if want := plainMessage(rv, ""); err == nil || err.Error() != want {
				t.Errorf("error:\n got %v\nwant %q", err, want)
			}
		})
	}
}

// The contract's two compliant forms land: the handler's field and the
// runtime's conditions are both stored (§7.4, tests 1 and 2). Copying only
// the version lands too, and reverts the handler's field (§7.4, test 3): the
// doc's warning has a test behind it.
func TestStatusWriteAfterAHandlerWrite(t *testing.T) {
	cases := map[string]struct {
		hook      func(t *testing.T, s *traceServer) func(context.Context, *Widget)
		wantState string
	}{
		"WritesMgItself": {
			hook: func(t *testing.T, s *traceServer) func(context.Context, *Widget) {
				return func(ctx context.Context, w *Widget) {
					w.Status.State = handlerState
					if err := s.raw.Status().Update(ctx, w); err != nil {
						t.Errorf("handler write: %v", err)
					}
				}
			},
			wantState: handlerState,
		},
		"MirrorsTheVersionAndTheFields": {
			hook: func(t *testing.T, s *traceServer) func(context.Context, *Widget) {
				return handlerWrite(t, s, "version and fields")
			},
			wantState: handlerState,
		},
		"MirrorsOnlyTheVersion": {
			hook: func(t *testing.T, s *traceServer) func(context.Context, *Widget) {
				return handlerWrite(t, s, "version only")
			},
			wantState: "", // reverted, silently
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, newWidget("w", nil))
			p := &widgetProvider{ops: s.ops, updateHook: tc.hook(t, s)}

			if _, err := newTraceReconciler(s, p).Reconcile(context.Background(), reqFor("w")); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got := s.widget("w")
			if got.Status.State != tc.wantState {
				t.Errorf("stored state = %q, want %q", got.Status.State, tc.wantState)
			}
			if c := got.GetCondition(prv1.TypeSynced); c.Reason != prv1.ReasonReconcileSuccess {
				t.Errorf("stored Synced = %s, want %s", c.Reason, prv1.ReasonReconcileSuccess)
			}
		})
	}
}

// Every status write goes through writeStatus, so every one names the cause on
// a conflict: reconciler.go makes exactly one Status() call, inside it, and
// deferReconcile (ratelimit.go) makes none and writes through the function
// it's given.
func TestStatusWritesGoThroughWriteStatus(t *testing.T) {
	statusCalls := func(file string) (calls []token.Position, inWriteStatus int) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == 1 {
				if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name == "writeStatus" {
					ast.Inspect(as, func(x ast.Node) bool {
						if c, ok := x.(*ast.CallExpr); ok {
							if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Status" {
								inWriteStatus++
							}
						}
						return true
					})
				}
			}
			if c, ok := n.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Status" {
					calls = append(calls, fset.Position(c.Pos()))
				}
			}
			return true
		})
		return calls, inWriteStatus
	}

	calls, in := statusCalls("reconciler.go")
	if len(calls) != 1 || in != 1 {
		t.Errorf("reconciler.go: Status() calls at %v, %d of them in writeStatus; want exactly one, in writeStatus", calls, in)
	}
	if calls, _ := statusCalls("ratelimit.go"); len(calls) != 0 {
		t.Errorf("ratelimit.go: Status() calls at %v; want none, deferReconcile writes through writeStatus", calls)
	}
}

func TestStatusKeysThatDiffer(t *testing.T) {
	read := newWidget("w", nil)
	read.Status.State = "Open"  // changed in live
	read.Status.Script = "same" // the same in both
	read.Status.Observed = "a"  // gone from live
	live := newWidget("w", nil)
	live.Status.State = "Closed"
	live.Status.Script = "same"
	live.Status.Initialized = true // new in live

	keys, ok := statusKeysThatDiffer(read, live)
	if !ok {
		t.Fatal("statusKeysThatDiffer: not ok")
	}
	if got, want := strings.Join(keys, ","), "initialized,observed,state"; got != want {
		t.Errorf("keys = %s, want %s", got, want)
	}
	if keys, _ := statusKeysThatDiffer(read, read); len(keys) != 0 {
		t.Errorf("keys of an object against itself = %v, want none", keys)
	}
}

func TestNewestManagedFields(t *testing.T) {
	fields := stampedFields()
	// Order mustn't matter: the newest is by time, not position.
	for _, entries := range [][]metav1.ManagedFieldsEntry{fields, {fields[2], fields[1], fields[0]}} {
		e, ok := newestManagedFields(entries)
		if !ok || e.Manager != "my-provider" {
			t.Errorf("newest of %v = %q (%v), want my-provider", entries, e.Manager, ok)
		}
	}
	if _, ok := newestManagedFields([]metav1.ManagedFieldsEntry{{Manager: "no-time"}}); ok {
		t.Error("an entry without a time must not be chosen")
	}
}
