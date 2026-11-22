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

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// statusRow is one status write of design-status-write-conflict §2.2: the
// widget and provider that reach it, and the error its path returns.
type statusRow struct {
	widget   func() *Widget
	provider func(s *traceServer) *widgetProvider
	options  func(t *testing.T) []ReconcilerOption
	// pass is the text of the error the path returns, or "" for none.
	pass string
	// boom says the pass's error wraps errRowBoom.
	boom bool
	// world, when set, builds the row's server with a credential-use Shared
	// over it, and the options that track the Widget in it.
	world func(t *testing.T, w *Widget) (*traceServer, []ReconcilerOption)
}

var errRowBoom = errors.New("boom")

func deletedWidget(kv map[string]string) *Widget {
	w := initialized(newWidget("w", kv))
	w.Finalizers = []string{FinalizerName}
	now := metav1.Now()
	w.DeletionTimestamp = &now
	return w
}

func finalizerFails(add, remove error) func(*testing.T) []ReconcilerOption {
	return func(*testing.T) []ReconcilerOption {
		return []ReconcilerOption{WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return add },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { return remove },
		})}
	}
}

func statusRows() map[string]statusRow {
	plain := func(s *traceServer) *widgetProvider { return &widgetProvider{ops: s.ops} }
	absent := func(s *traceServer) *widgetProvider {
		return &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}
	}
	fresh := func() *Widget { return newWidget("w", nil) }
	return map[string]statusRow{
		"01Paused": {
			widget:   func() *Widget { return newWidget("w", ann(meta.AnnotationKeyReconciliationPaused, "true")) },
			provider: plain,
		},
		"02OrphanFinalizerRemovalFailed": {
			widget:   func() *Widget { return deletedWidget(ann(meta.AnnotationKeyDeletionPolicy, meta.DeletionPolicyOrphan)) },
			provider: plain,
			options:  finalizerFails(nil, errRowBoom),
			pass:     errUpdateManagedStatus + ": boom", boom: true,
		},
		"03Deferred": {
			widget: fresh,
			provider: func(s *traceServer) *widgetProvider {
				p := &widgetProvider{ops: s.ops, key: "api.example.com"}
				p.observeHook = func(ctx context.Context, _ *Widget) {
					ratelimit.Report(ctx, ratelimit.Signal{Key: "api.example.com", Throttled: true, RetryAfter: time.Minute})
				}
				return p
			},
			options: func(t *testing.T) []ReconcilerOption {
				return []ReconcilerOption{WithRateLimitGovernor(newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1}))}
			},
		},
		"04ConnectFailed": {
			widget:   fresh,
			provider: func(s *traceServer) *widgetProvider { return &widgetProvider{ops: s.ops, connectErr: errRowBoom} },
			pass:     errReconcileConnect + ": boom", boom: true,
		},
		"05ObserveFailed": {
			widget:   fresh,
			provider: func(s *traceServer) *widgetProvider { return &widgetProvider{ops: s.ops, observeErr: errRowBoom} },
			pass:     errReconcileObserve + ": boom", boom: true,
		},
		"06UnresolvedCreateInsideItsWindow": {
			widget: func() *Widget {
				w := newWidget("w", nil)
				meta.SetExternalCreatePending(w, time.Now())
				return w
			},
			provider: absent,
			pass:     errCreateIncomplete,
		},
		"07ObserveOnlyAndAbsent": {
			widget: func() *Widget {
				return newWidget("w", ann(meta.AnnotationKeyManagementPolicy, meta.ManagementPolicyObserve))
			},
			provider: absent,
			pass:     errReconcileObserve + ": " + errExternalResourceNotExist,
		},
		"08DeleteFailed": {
			widget:   func() *Widget { return deletedWidget(nil) },
			provider: func(s *traceServer) *widgetProvider { return &widgetProvider{ops: s.ops, deleteErr: errRowBoom} },
			pass:     errReconcileDelete + ": boom", boom: true,
		},
		"09DeleteRequested": {
			widget:   func() *Widget { return deletedWidget(nil) },
			provider: plain,
		},
		"10FinalizerRemovalFailed": {
			widget:   func() *Widget { return deletedWidget(nil) },
			provider: absent,
			options:  finalizerFails(nil, errRowBoom),
			pass:     errUpdateManagedStatus + ": boom", boom: true,
		},
		"11FinalizerAddFailed": {
			widget:   fresh,
			provider: plain,
			options:  finalizerFails(errRowBoom, nil),
			pass:     errCreateIncomplete + ": boom", boom: true,
		},
		"12CreatePendingFailed": {
			widget: fresh,
			provider: func(s *traceServer) *widgetProvider {
				s.faults.update = func(client.Object) error { return forbiddenErr() }
				return absent(s)
			},
			pass: errUpdateManaged + ": " + forbiddenErr().Error(),
		},
		"13CreateFailed": {
			widget: fresh,
			provider: func(s *traceServer) *widgetProvider {
				return &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }, createErr: errRowBoom}
			},
			pass: errReconcileCreate + ": boom", boom: true,
		},
		"14CriticalAnnotationsFailed": {
			widget:   fresh,
			provider: absent,
			options: func(*testing.T) []ReconcilerOption {
				return []ReconcilerOption{WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(context.Context, client.Object) error { return errRowBoom }))}
			},
			pass: errUpdateManagedAnnotations + ": boom", boom: true,
		},
		"15CreateRequested": {
			widget:   fresh,
			provider: absent,
		},
		"15bCreatesSkippedByPolicy": {
			widget: func() *Widget {
				return newWidget("w", ann(meta.AnnotationKeyManagementPolicy, meta.ManagementPolicyObserveDelete))
			},
			provider: absent,
		},
		"16LateInitFailed": {
			widget: func() *Widget { return initialized(newWidget("w", nil)) },
			provider: func(s *traceServer) *widgetProvider {
				s.faults.update = func(client.Object) error { return forbiddenErr() }
				return &widgetProvider{ops: s.ops, lateInit: true}
			},
			pass: errUpdateManaged + ": " + forbiddenErr().Error(),
		},
		"17UpToDate": {
			widget:   func() *Widget { return initialized(newWidget("w", nil)) },
			provider: plain,
		},
		"18UpdatesSkippedByPolicy": {
			widget: func() *Widget {
				return newWidget("w", ann(meta.AnnotationKeyManagementPolicy, meta.ManagementPolicyObserve))
			},
			provider: plain,
		},
		"19UpdateFailed": {
			widget:   fresh,
			provider: func(s *traceServer) *widgetProvider { return &widgetProvider{ops: s.ops, updateErr: errRowBoom} },
			pass:     errReconcileUpdate + ": boom", boom: true,
		},
		"20UpdateRequested": {
			widget:   fresh,
			provider: plain,
		},
		// Credential-use (design-credential-use-finalizer §8): a new object whose
		// acquire failed must not connect (step 5), and a ref that appeared
		// before the finalizer can't be read (step 8).
		"21CredentialUseMustNotConnect": {
			widget:   fresh,
			provider: plain,
			world:    cuRowWorld(func(resource.Managed) ([]credentialuse.Ref, error) { return nil, errRowBoom }, nil),
			pass:     "credential-use: naming the refs of demo/w: boom", boom: true,
		},
		"22CredentialUseBeforeEstablishFailed": {
			widget:   fresh,
			provider: absent,
			world: cuRowWorld(secretRef("creds"), func(w *cuWorld) {
				// The Secret is Missing for Acquire's write; the live read
				// before the finalizer fails.
				w.mdGet = func(gvr schema.GroupVersionResource, _, _ string) error {
					if gvr.Resource == "secrets" {
						return errRowBoom
					}
					return nil
				}
			}),
			pass: "credential-use: checking /Secret/demo/creds before establishing: boom", boom: true,
		},
	}
}

// cuRowWorld builds a status row's server with a Shared over it, tracking
// the Widget with refs; change adjusts the world.
func cuRowWorld(refs func(resource.Managed) ([]credentialuse.Ref, error), change func(w *cuWorld)) func(*testing.T, *Widget) (*traceServer, []ReconcilerOption) {
	return func(t *testing.T, wd *Widget) (*traceServer, []ReconcilerOption) {
		w := newCUWorld(t, wd)
		if change != nil {
			change(w)
		}
		return w.srv, []ReconcilerOption{
			WithFinalizer(resource.NewAPIFinalizer(w.srv.c, FinalizerName)),
			WithCredentialUseShared(w.s),
			withRowRefs(refs),
		}
	}
}

// withRowRefs makes the connecter a status row's provider name refs.
func withRowRefs(refs func(resource.Managed) ([]credentialuse.Ref, error)) ReconcilerOption {
	return func(r *Reconciler) {
		r.cu.refs = refsConnecter{refs: refs}
	}
}

// Every status write, when it conflicts, returns the message with the pass's
// own error kept, an empty Result, and k8s.conflict step status
// (design-status-write-conflict §2.2, §7.1). The status write is made to
// conflict at the API server, so each row is reached by its own path.
func TestStatusConflictOnEveryPath(t *testing.T) {
	rows := statusRows()
	if got, want := len(rows), writeStatusCalls(t); got != want {
		t.Fatalf("%d rows, but reconciler.go calls writeStatus %d times: add the new path here", got, want)
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			var s *traceServer
			var cuOpts []ReconcilerOption
			if row.world != nil {
				s, cuOpts = row.world(t, row.widget())
			} else {
				s = newTraceServer(t, row.widget())
			}
			s.faults.status = func(client.Object) error { return conflictErr() }
			tp, rec := tracer(t)
			o := []ReconcilerOption{WithTracerProvider(tp)}
			if row.options != nil {
				o = append(o, row.options(t)...)
			}
			o = append(o, cuOpts...)

			r := newTraceReconciler(s, row.provider(s), o...)
			if r.cu.enabled {
				startSource(t, r)
			}
			res, err := r.Reconcile(context.Background(), reqFor("w"))

			if s.ops.count("status-rejected") != 1 {
				t.Fatalf("ops = %v, want exactly one status write, refused: this row's path wasn't reached", s.ops.list())
			}
			if err == nil {
				t.Fatalf("Reconcile = %+v, nil; a status conflict must stay an error", res)
			}
			if res != (reconcile.Result{}) {
				t.Errorf("result = %+v, want none: the error requeues", res)
			}
			msg := err.Error()
			if prefix := errUpdateManagedStatus + ": " + conflictText(t, err) + ": the object changed since the pass's read (read at resourceVersion "; !strings.HasPrefix(msg, prefix) {
				t.Errorf("error:\n got %q\nwant it to start with %q", msg, prefix)
			}
			suffix := "): " + errStatusConflictCause
			if row.pass != "" {
				suffix += "; pass error: " + row.pass
			}
			if !strings.HasSuffix(msg, suffix) {
				t.Errorf("error:\n got %q\nwant it to end with %q", msg, suffix)
			}
			if !kerrors.IsConflict(err) {
				t.Error("kerrors.IsConflict = false; the conflict must stay in the chain")
			}
			if row.boom && !errors.Is(err, errRowBoom) {
				t.Error("errors.Is(err, the pass's error) = false; it must stay in the chain")
			}
			root := lastRoot(t, rec)
			if got, ok := eventAttr(root, tracing.EventConflict, string(tracing.AttrStep)); !ok || got != string(tracing.StepStatus) {
				t.Errorf("k8s.conflict step = %q (%v), want %s; events %v", got, ok, tracing.StepStatus, events(root))
			}
		})
	}
}

// writeStatusCalls counts the calls to writeStatus in reconciler.go, and in
// credentialuse.go, whose acquire gets it from the pass.
func writeStatusCalls(t *testing.T) int {
	t.Helper()
	n := 0
	for _, name := range []string{"reconciler.go", "credentialuse.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(x ast.Node) bool {
			if c, ok := x.(*ast.CallExpr); ok {
				if id, ok := c.Fun.(*ast.Ident); ok && id.Name == "writeStatus" {
					n++
				}
			}
			return true
		})
	}
	return n
}
