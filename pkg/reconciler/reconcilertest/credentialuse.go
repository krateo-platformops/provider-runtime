package reconcilertest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// secretGroupKind is the core Secret's GroupKind, always protected.
var secretGroupKind = schema.GroupKind{Kind: "Secret"}

// A Read is one object a RecordingReader served: one Get, or one item of a
// List, with the List itself recorded too, with no name.
type Read struct {
	GroupKind schema.GroupKind
	Namespace string
	Name      string
	// List: the read was a List; Name is empty.
	List bool
}

func (r Read) String() string {
	if r.List {
		return fmt.Sprintf("list of %s in %q", r.GroupKind, r.Namespace)
	}
	return fmt.Sprintf("%s %s/%s", r.GroupKind, r.Namespace, r.Name)
}

// A RecordingReader is a client.Reader that records every read it serves,
// for AssertCredentialRefsCover. Build the connecter under test with it as
// the reader Connect reads Secrets and protected objects with.
type RecordingReader struct {
	client.Reader
	scheme *runtime.Scheme

	mu    sync.Mutex
	reads []Read
}

// NewRecordingReader wraps rd, typically a fake client holding the fixture's
// Secrets and intermediate objects. s maps Go types to kinds.
func NewRecordingReader(rd client.Reader, s *runtime.Scheme) *RecordingReader {
	return &RecordingReader{Reader: rd, scheme: s}
}

// Get records the read, then serves it.
func (r *RecordingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if gvk, err := apiutil.GVKForObject(obj, r.scheme); err == nil {
		r.record(Read{GroupKind: gvk.GroupKind(), Namespace: key.Namespace, Name: key.Name})
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// List records the read, then serves it.
func (r *RecordingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	lo := (&client.ListOptions{}).ApplyOptions(opts)
	if gvk, err := apiutil.GVKForObject(list, r.scheme); err == nil {
		gk := gvk.GroupKind()
		gk.Kind = strings.TrimSuffix(gk.Kind, "List")
		r.record(Read{GroupKind: gk, Namespace: lo.Namespace, List: true})
	}
	return r.Reader.List(ctx, list, opts...)
}

func (r *RecordingReader) record(rd Read) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, rd)
}

// Reads returns what the reader has served since it was built or last reset.
func (r *RecordingReader) Reads() []Read {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reads)
}

// Reset forgets every read recorded.
func (r *RecordingReader) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = nil
}

// AssertCredentialRefsCover checks that c's CredentialRefs names everything
// its Connect reads for mg (docs/design-credential-use-finalizer.md §11.9):
// a Secret, or an object of one of protected, that Connect reads and the
// refs don't name is unprotected, a provider defect that only shows when a
// namespace teardown wedges.
//
// It calls c.CredentialRefs(mg), and follows each ref's Next, as the runtime
// does after holding the object: it reads each object a ref names through
// rec's own reader, without recording it. A ref whose object isn't there
// fails t, since a mistyped or unmatched path resolves to nothing and so
// silently protects nothing (§16.2): the fixture must hold every object the
// refs name. It then calls c.Connect(ctx, mg), with the objects it followed
// on ctx for credentialuse.Resolved, and fails t for each read Connect made
// through rec, a Get or a List, of a Secret or of protected, that the refs,
// after Next, don't name. Guaranteed refs count as named.
//
// rec must be the reader the connecter reads Secrets and protected objects
// with; build the connecter with it. The connecter must implement
// reconciler.CredentialRefsProvider. It returns the client Connect returned,
// and its error, for further assertions.
func AssertCredentialRefsCover(t testing.TB, ctx context.Context, c reconciler.ExternalConnecter, mg resource.Managed, rec *RecordingReader,
	protected ...schema.GroupKind) (reconciler.ExternalClient, error) {
	t.Helper()
	p, ok := c.(reconciler.CredentialRefsProvider)
	if !ok {
		t.Fatalf("the connecter, a %T, doesn't implement reconciler.CredentialRefsProvider", c)
		return nil, nil
	}
	refs, err := p.CredentialRefs(mg)
	if err != nil {
		t.Fatalf("CredentialRefs: %v", err)
		return nil, nil
	}
	kinds := map[schema.GroupKind]bool{secretGroupKind: true}
	for _, gk := range protected {
		kinds[gk] = true
	}
	named := map[credentialuse.ID]bool{}
	resolved := map[credentialuse.ID]*unstructured.Unstructured{}
	seen := map[credentialuse.ID]bool{}
	level := refs
	for depth := 0; len(level) > 0; depth++ {
		if depth > 16 {
			t.Errorf("the refs' Next go deeper than 16 levels")
			break
		}
		var next []credentialuse.Ref
		for _, r := range level {
			id := r.ID()
			if seen[id] {
				continue
			}
			seen[id] = true
			named[id] = true
			obj, err := readRef(ctx, rec, r)
			if kerrors.IsNotFound(err) {
				t.Errorf("ref %s resolves to nothing: no such object in the fixture, so a holder would silently protect nothing", id)
				continue
			}
			if err != nil {
				t.Errorf("reading ref %s: %v", id, err)
				continue
			}
			if r.Next == nil {
				continue
			}
			resolved[id] = obj
			more, err := r.Next(obj.DeepCopy())
			if err != nil {
				t.Errorf("following ref %s: %v", id, err)
				continue
			}
			next = append(next, more...)
		}
		level = next
	}

	rec.Reset()
	ec, cerr := c.Connect(credentialuse.WithResolvedObjects(ctx, resolved), mg)
	for _, rd := range rec.Reads() {
		if !kinds[rd.GroupKind] {
			continue
		}
		if rd.List {
			t.Errorf("Connect listed %s: CredentialRefs must name each protected object Connect reads", rd)
			continue
		}
		id := credentialuse.ID{Group: rd.GroupKind.Group, Kind: rd.GroupKind.Kind, Namespace: rd.Namespace, Name: rd.Name}
		if !named[id] {
			t.Errorf("Connect read %s, which CredentialRefs doesn't name (after Next): nothing would hold it", rd)
		}
	}
	return ec, cerr
}

// readRef reads the object r names, whole, through rec's own reader.
func readRef(ctx context.Context, rec *RecordingReader, r credentialuse.Ref) (*unstructured.Unstructured, error) {
	gvk, err := versionOf(rec.scheme, r.GroupKind)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	if err := rec.Reader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Name}, u); err != nil {
		return nil, err
	}
	return u, nil
}

// versionOf picks the version of gk that s prefers.
func versionOf(s *runtime.Scheme, gk schema.GroupKind) (schema.GroupVersionKind, error) {
	for _, gv := range s.PrioritizedVersionsForGroup(gk.Group) {
		gvk := gv.WithKind(gk.Kind)
		if s.Recognizes(gvk) {
			return gvk, nil
		}
	}
	for gvk := range s.AllKnownTypes() {
		if gvk.GroupKind() == gk {
			return gvk, nil
		}
	}
	return schema.GroupVersionKind{}, &meta.NoKindMatchError{GroupKind: gk}
}
