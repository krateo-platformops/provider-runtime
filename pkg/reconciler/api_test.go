package reconciler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

// Annotations another writer sets on the object while the reconciler holds an
// older copy of it.
const (
	keyTraceparent = "krateo.io/traceparent"
	keyTracestate  = "krateo.io/tracestate"
	keyOther       = "example.com/written-by-someone-else"

	traceparentA = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	traceparentB = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

// storedObject is a one-object API server for a fake.Managed: Get returns a
// copy of what is stored, and Update stores a copy of what it is given.
// beforeGet, when set, runs before each Get with its 1-based count, so a test
// can act as another writer between the reconciler's reads.
type storedObject struct {
	mu        sync.Mutex
	obj       *fake.Managed
	gets      int
	updates   []*fake.Managed
	beforeGet func(n int, stored *fake.Managed)
	// updateErrs, when set, are returned by the Updates in order, one each,
	// without storing anything; an Update past the list stores as usual.
	updateErrs []error
}

func (s *storedObject) client() *test.MockClient {
	c := test.NewMockClient()
	c.MockGet = func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.gets++
		if s.beforeGet != nil {
			s.beforeGet(s.gets, s.obj)
		}
		*obj.(*fake.Managed) = *s.obj.DeepCopyObject().(*fake.Managed)
		return nil
	}
	c.MockUpdate = func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.updateErrs) > 0 {
			err := s.updateErrs[0]
			s.updateErrs = s.updateErrs[1:]
			return err
		}
		s.obj = obj.DeepCopyObject().(*fake.Managed)
		s.updates = append(s.updates, s.obj.DeepCopyObject().(*fake.Managed))
		return nil
	}
	c.MockStatusUpdate = test.NewMockSubResourceUpdateFn(nil)
	return c
}

func (s *storedObject) annotations() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.obj.GetAnnotations()
}

// The updater writes back only the runtime's own keys. A traceparent and a
// tracestate another writer set after the object was read survive, and so does
// any other annotation written meanwhile. A key the in-memory copy carries but
// the runtime doesn't own is not persisted.
func TestUpdateCriticalAnnotationsWritesOnlyRuntimeKeys(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)

	stored := &fake.Managed{}
	stored.SetName("cool")
	stored.SetAnnotations(map[string]string{
		keyTraceparent: traceparentB,
		keyTracestate:  "vendor=b",
		keyOther:       "set-after-the-read",
	})
	srv := &storedObject{obj: stored}

	inMemory := &fake.Managed{}
	inMemory.SetName("cool")
	inMemory.SetAnnotations(map[string]string{
		keyTraceparent:                            traceparentA, // stale: the copy read before B was written
		keyTracestate:                             "vendor=a",
		meta.AnnotationKeyExternalName:            "ext-1",
		meta.AnnotationKeyExternalCreatePending:   now,
		meta.AnnotationKeyExternalCreateSucceeded: now,
		meta.AnnotationKeyExternalCreateFailed:    now,
		meta.AnnotationKeyTraceparentContinued:    traceparentA,
		"example.com/only-in-memory":              "never-persisted",
	})

	u := NewRetryingCriticalAnnotationUpdater(srv.client())
	if err := u.UpdateCriticalAnnotations(context.Background(), inMemory); err != nil {
		t.Fatalf("UpdateCriticalAnnotations: %v", err)
	}

	want := map[string]string{
		keyTraceparent:                            traceparentB,
		keyTracestate:                             "vendor=b",
		keyOther:                                  "set-after-the-read",
		meta.AnnotationKeyExternalName:            "ext-1",
		meta.AnnotationKeyExternalCreatePending:   now,
		meta.AnnotationKeyExternalCreateSucceeded: now,
		meta.AnnotationKeyExternalCreateFailed:    now,
		meta.AnnotationKeyTraceparentContinued:    traceparentA,
	}
	if diff := cmp.Diff(want, srv.annotations()); diff != "" {
		t.Errorf("stored annotations after the critical write: -want, +got:\n%s", diff)
	}
	if diff := cmp.Diff(want, inMemory.GetAnnotations()); diff != "" {
		t.Errorf("the caller's object after the critical write: -want, +got:\n%s", diff)
	}
}

// An object with none of the runtime's keys is written back as the API server
// has it, and its annotations are neither added to nor dropped.
func TestUpdateCriticalAnnotationsWithoutRuntimeKeys(t *testing.T) {
	stored := &fake.Managed{}
	stored.SetAnnotations(map[string]string{keyOther: "kept"})
	srv := &storedObject{obj: stored}

	inMemory := &fake.Managed{}
	inMemory.SetAnnotations(map[string]string{keyTraceparent: traceparentA})

	if err := NewRetryingCriticalAnnotationUpdater(srv.client()).UpdateCriticalAnnotations(context.Background(), inMemory); err != nil {
		t.Fatalf("UpdateCriticalAnnotations: %v", err)
	}
	if diff := cmp.Diff(map[string]string{keyOther: "kept"}, srv.annotations()); diff != "" {
		t.Errorf("stored annotations: -want, +got:\n%s", diff)
	}
}

// Errors are wrapped as before.
func TestUpdateCriticalAnnotationsError(t *testing.T) {
	errBoom := errors.New("boom")
	c := test.NewMockClient()
	c.MockGet = test.NewMockGetFn(nil)
	c.MockUpdate = test.NewMockUpdateFn(errBoom)

	err := NewRetryingCriticalAnnotationUpdater(c).UpdateCriticalAnnotations(context.Background(), &fake.Managed{})
	if diff := cmp.Diff(errors.Wrap(errBoom, errUpdateCriticalAnnotations), err, test.EquateErrors()); diff != "" {
		t.Errorf("-want, +got:\n%s", diff)
	}
}

// reconcileWithStored runs one Reconcile against srv with the default critical
// annotation updater, so the runtime's own write path is what's tested.
func reconcileWithStored(t *testing.T, srv *storedObject, ec ExternalClient) {
	t.Helper()
	if _, err := reconcileStored(srv, ec); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// reconcileStored is reconcileWithStored returning Reconcile's error.
func reconcileStored(srv *storedObject, ec ExternalClient) (reconcile.Result, error) {
	r := NewReconciler(&fake.Manager{Client: srv.client(), Scheme: fake.SchemeWith(&fake.Managed{})},
		resource.ManagedKind(fake.GVK(&fake.Managed{})),
		WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
			return ec, nil
		})),
		WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
	)
	return r.Reconcile(context.Background(), reconcile.Request{})
}

// design-tracing §10, test 14. A fresh traceparent B, written by another
// controller while Create runs, survives the critical write that follows Create
// (reconciler.go, after external.Create), and so does a third annotation
// written meanwhile. Before, the write reverted both to the copy read at the
// start of the pass.
func TestCriticalWriteAfterCreateKeepsAFreshTraceparent(t *testing.T) {
	stored := &fake.Managed{}
	stored.SetName("cool")
	stored.SetAnnotations(map[string]string{keyTraceparent: traceparentA})
	srv := &storedObject{obj: stored}
	// Get 1 is the pass's own read. Get 2 is the critical write's re-read, after
	// Create: another controller has stamped B and a third annotation by then.
	srv.beforeGet = func(n int, s *fake.Managed) {
		if n == 2 {
			meta.AddAnnotations(s, map[string]string{keyTraceparent: traceparentB, keyOther: "set-during-create"})
		}
	}

	reconcileWithStored(t, srv, &ExternalClientFns{
		ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: false}, nil
		},
		CreateFn: func(_ context.Context, mg resource.Managed) error {
			meta.SetExternalName(mg, "ext-1")
			return nil
		},
	})

	got := srv.annotations()
	if got[keyTraceparent] != traceparentB {
		t.Errorf("krateo.io/traceparent = %q after the critical write, want the fresh %q", got[keyTraceparent], traceparentB)
	}
	if got[keyOther] != "set-during-create" {
		t.Errorf("%s = %q after the critical write, want it kept", keyOther, got[keyOther])
	}
	if got[meta.AnnotationKeyExternalName] != "ext-1" {
		t.Errorf("krateo.io/external-name = %q, want the name Create set", got[meta.AnnotationKeyExternalName])
	}
	if got[meta.AnnotationKeyExternalCreateSucceeded] == "" {
		t.Error("krateo.io/external-create-succeeded was not persisted")
	}
}

// design-tracing §10, test 14, part 3. The same through the unresolved create
// handshake: Observe finds the resource, and the critical write that records
// create-succeeded keeps a traceparent written after the pass read the object.
func TestCriticalWriteOnHandshakeAdoptionKeepsAFreshTraceparent(t *testing.T) {
	stored := &fake.Managed{}
	stored.SetName("cool")
	meta.SetExternalCreatePending(stored, time.Now().Add(-time.Minute))
	meta.AddAnnotations(stored, map[string]string{keyTraceparent: traceparentA})
	srv := &storedObject{obj: stored}
	srv.beforeGet = func(n int, s *fake.Managed) {
		if n == 2 {
			meta.AddAnnotations(s, map[string]string{keyTraceparent: traceparentB, keyOther: "set-during-observe"})
		}
	}

	reconcileWithStored(t, srv, &ExternalClientFns{
		ObserveFn: func(_ context.Context, mg resource.Managed) (ExternalObservation, error) {
			mg.SetConditions(prv1.Available())
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	if len(srv.updates) == 0 {
		t.Fatal("the handshake adoption made no critical write")
	}
	got := srv.annotations()
	if got[keyTraceparent] != traceparentB {
		t.Errorf("krateo.io/traceparent = %q after the adoption's critical write, want the fresh %q", got[keyTraceparent], traceparentB)
	}
	if got[keyOther] != "set-during-observe" {
		t.Errorf("%s = %q after the adoption's critical write, want it kept", keyOther, got[keyOther])
	}
	if got[meta.AnnotationKeyExternalCreateSucceeded] == "" {
		t.Error("krateo.io/external-create-succeeded was not persisted")
	}
}

// The failed-Create path (reconciler.go, the critical write after a Create
// error): create-failed is persisted, and a traceparent B written by another
// controller while Create ran survives, as on the success path.
func TestCriticalWriteAfterFailedCreateKeepsAFreshTraceparent(t *testing.T) {
	stored := &fake.Managed{}
	stored.SetName("cool")
	stored.SetAnnotations(map[string]string{keyTraceparent: traceparentA})
	srv := &storedObject{obj: stored}
	srv.beforeGet = func(n int, s *fake.Managed) {
		if n == 2 {
			meta.AddAnnotations(s, map[string]string{keyTraceparent: traceparentB, keyOther: "set-during-create"})
		}
	}

	errBoom := errors.New("boom")
	_, err := reconcileStored(srv, &ExternalClientFns{
		ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: false}, nil
		},
		CreateFn: func(_ context.Context, _ resource.Managed) error { return errBoom },
	})
	if diff := cmp.Diff(errors.Wrap(errBoom, errReconcileCreate), err, test.EquateErrors()); diff != "" {
		t.Errorf("Reconcile error: -want, +got:\n%s", diff)
	}

	got := srv.annotations()
	if got[meta.AnnotationKeyExternalCreateFailed] == "" {
		t.Error("krateo.io/external-create-failed was not persisted after the failed Create")
	}
	if got[keyTraceparent] != traceparentB {
		t.Errorf("krateo.io/traceparent = %q after the critical write, want the fresh %q", got[keyTraceparent], traceparentB)
	}
	if got[keyOther] != "set-during-create" {
		t.Errorf("%s = %q after the critical write, want it kept", keyOther, got[keyOther])
	}
}

// The retry path. The first Update returns a Conflict; the second attempt
// re-reads the object and writes the runtime's keys onto that fresh read. A
// value another writer set between the attempts survives, and the keys the
// caller set in memory are still written.
func TestUpdateCriticalAnnotationsRetriesOnConflict(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	stored := &fake.Managed{}
	stored.SetName("cool")
	stored.SetAnnotations(map[string]string{keyTraceparent: traceparentA})
	srv := &storedObject{obj: stored}
	srv.updateErrs = []error{kerrors.NewConflict(schema.GroupResource{Resource: "manageds"}, "cool", errors.New("modified"))}
	// Get 1 is the first attempt's read. Get 2 is the retry's: another writer
	// has stamped B and a third annotation since.
	srv.beforeGet = func(n int, s *fake.Managed) {
		if n == 2 {
			meta.AddAnnotations(s, map[string]string{keyTraceparent: traceparentB, keyOther: "set-between-attempts"})
		}
	}

	inMemory := &fake.Managed{}
	inMemory.SetName("cool")
	inMemory.SetAnnotations(map[string]string{
		keyTraceparent:                         traceparentA,
		meta.AnnotationKeyExternalName:         "ext-1",
		meta.AnnotationKeyExternalCreateFailed: now,
	})

	if err := NewRetryingCriticalAnnotationUpdater(srv.client()).UpdateCriticalAnnotations(context.Background(), inMemory); err != nil {
		t.Fatalf("UpdateCriticalAnnotations: %v", err)
	}
	if srv.gets != 2 || len(srv.updates) != 1 {
		t.Fatalf("gets = %d, stored updates = %d; want 2 reads and 1 stored update after one conflict", srv.gets, len(srv.updates))
	}

	want := map[string]string{
		keyTraceparent:                         traceparentB,
		keyOther:                               "set-between-attempts",
		meta.AnnotationKeyExternalName:         "ext-1",
		meta.AnnotationKeyExternalCreateFailed: now,
	}
	if diff := cmp.Diff(want, srv.annotations()); diff != "" {
		t.Errorf("stored annotations after the retry: -want, +got:\n%s", diff)
	}
}

// The capture stays before the loop so a runtime key the caller didn't carry
// is never written back from an earlier attempt's read. After a failed
// attempt the object holds that attempt's read plus the merge, so a capture
// taken inside the loop would pick up external-name x1 from the first read and
// write it over the x2 another writer set before the second.
func TestUpdateCriticalAnnotationsNeverWritesBackAnEarlierRead(t *testing.T) {
	stored := &fake.Managed{}
	stored.SetName("cool")
	stored.SetAnnotations(map[string]string{meta.AnnotationKeyExternalName: "x1"})
	srv := &storedObject{obj: stored}
	srv.updateErrs = []error{kerrors.NewConflict(schema.GroupResource{Resource: "manageds"}, "cool", errors.New("modified"))}
	srv.beforeGet = func(n int, s *fake.Managed) {
		if n == 2 {
			meta.AddAnnotations(s, map[string]string{meta.AnnotationKeyExternalName: "x2"})
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	inMemory := &fake.Managed{}
	inMemory.SetName("cool")
	inMemory.SetAnnotations(map[string]string{meta.AnnotationKeyExternalCreateFailed: now})

	if err := NewRetryingCriticalAnnotationUpdater(srv.client()).UpdateCriticalAnnotations(context.Background(), inMemory); err != nil {
		t.Fatalf("UpdateCriticalAnnotations: %v", err)
	}
	got := srv.annotations()
	if got[meta.AnnotationKeyExternalName] != "x2" {
		t.Errorf("krateo.io/external-name = %q, want the x2 another writer set; the first attempt's read was written back", got[meta.AnnotationKeyExternalName])
	}
	if got[meta.AnnotationKeyExternalCreateFailed] != now {
		t.Errorf("krateo.io/external-create-failed = %q, want the caller's %q", got[meta.AnnotationKeyExternalCreateFailed], now)
	}
}
