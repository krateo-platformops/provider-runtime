// Package apimodel is an in-memory model of the Kubernetes API server, as far
// as the credential-use protocol touches it, and a scheduler that runs actors
// against it one API call at a time. pkg/credentialuse's tests use it to
// enumerate the schedules of the design's §14 against the protocol's own
// code.
//
// The store is linearizable: every call is atomic. It keeps metadata only,
// because the protocol reads and writes nothing else. Its semantics are the
// API server's, through the same libraries the API server uses
// (k8s.io/apiserver@v0.37.2 pkg/endpoints/handlers/patch.go and
// pkg/registry/rest/update.go):
//
//   - JSON and merge patches through gopkg.in/evanphx/json-patch.v4, and
//     strategic merge patches through k8s.io/apimachinery's strategicpatch,
//     which only core kinds accept (a custom resource answers 415);
//   - a failed JSON patch is a 422 with the generic message and no causes;
//   - a resourceVersion in a patch is a precondition (409 when it differs);
//   - an object being deleted takes no new finalizer (ValidateNoNewFinalizers,
//     a 422 with a FieldValueForbidden cause), and its deletionTimestamp
//     can't change;
//   - a write that changes nothing doesn't bump the resourceVersion;
//   - an object with a deletionTimestamp and no finalizers is removed;
//   - a Namespace's deletion leaves it Terminating, and NamespaceLifecycle
//     refuses creates in it with a 403.
//
// The package doesn't import pkg/credentialuse, which imports it in tests.
// pkg/credentialuse's differential test runs the same operations against
// envtest and checks the two agree.
package apimodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/watch"
)

// NamespaceKind is a core Namespace's GroupKind.
var NamespaceKind = schema.GroupKind{Kind: "Namespace"}

// A Key names an object.
type Key struct {
	GK        schema.GroupKind
	Namespace string
	Name      string
}

func (k Key) String() string { return k.GK.String() + " " + k.Namespace + "/" + k.Name }

// An Event is one change the model made, in order, as a watch delivers it.
type Event struct {
	Seq  int
	Type watch.EventType
	Key  Key
	// Obj is the object after the change, or as it was when deleted.
	Obj metav1.PartialObjectMetadata
}

// Model is the store.
type Model struct {
	mu   sync.Mutex
	objs map[Key]*metav1.ObjectMeta
	rv   int64
	uid  int64
	log  []Event
	now  func() time.Time
}

// New returns an empty model whose timestamps come from now.
func New(now func() time.Time) *Model {
	return &Model{objs: map[Key]*metav1.ObjectMeta{}, now: now}
}

func resourceOf(gk schema.GroupKind) schema.GroupResource {
	return schema.GroupResource{Group: gk.Group, Resource: strings.ToLower(gk.Kind) + "s"}
}

func (m *Model) partial(k Key, meta *metav1.ObjectMeta) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: schema.GroupVersion{Group: k.GK.Group, Version: "v1"}.String(), Kind: k.GK.Kind},
		ObjectMeta: *meta.DeepCopy(),
	}
}

func (m *Model) bump(meta *metav1.ObjectMeta) {
	m.rv++
	meta.ResourceVersion = strconv.FormatInt(m.rv, 10)
}

func (m *Model) emit(t watch.EventType, k Key, meta *metav1.ObjectMeta) {
	m.log = append(m.log, Event{Seq: len(m.log) + 1, Type: t, Key: k, Obj: m.partial(k, meta)})
}

// Create creates an object with the given metadata (name and namespace come
// from k).
func (m *Model) Create(k Key, meta metav1.ObjectMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[k]; ok {
		return apierrors.NewAlreadyExists(resourceOf(k.GK), k.Name)
	}
	if k.Namespace != "" {
		ns, ok := m.objs[Key{GK: NamespaceKind, Name: k.Namespace}]
		if !ok {
			return apierrors.NewNotFound(resourceOf(NamespaceKind), k.Namespace)
		}
		if ns.DeletionTimestamp != nil {
			return apierrors.NewForbidden(resourceOf(k.GK), k.Name, fmt.Errorf("unable to create new content in namespace %s because it is being terminated", k.Namespace))
		}
	}
	meta = *meta.DeepCopy()
	meta.Name, meta.Namespace = k.Name, k.Namespace
	m.uid++
	meta.UID = types.UID(fmt.Sprintf("uid-%d", m.uid))
	meta.CreationTimestamp = metav1.NewTime(m.now())
	meta.DeletionTimestamp = nil
	m.bump(&meta)
	m.objs[k] = &meta
	m.emit(watch.Added, k, &meta)
	return nil
}

// Get reads one object.
func (m *Model) Get(k Key) (*metav1.PartialObjectMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.objs[k]
	if !ok {
		return nil, apierrors.NewNotFound(resourceOf(k.GK), k.Name)
	}
	p := m.partial(k, meta)
	return &p, nil
}

// List reads every object of gk in namespace, sorted by name.
func (m *Model) List(gk schema.GroupKind, namespace string) []metav1.PartialObjectMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []metav1.PartialObjectMetadata
	for _, k := range m.sortedKeys() {
		if k.GK == gk && k.Namespace == namespace {
			out = append(out, m.partial(k, m.objs[k]))
		}
	}
	return out
}

func (m *Model) sortedKeys() []Key {
	keys := slices.Collect(maps.Keys(m.objs))
	slices.SortFunc(keys, func(a, b Key) int { return strings.Compare(a.String(), b.String()) })
	return keys
}

// Snapshot returns every object, sorted, and the sequence number of the last
// event: what a list returns, and where its watch starts.
func (m *Model) Snapshot() ([]Event, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for _, k := range m.sortedKeys() {
		out = append(out, Event{Type: watch.Added, Key: k, Obj: m.partial(k, m.objs[k])})
	}
	return out, len(m.log)
}

// EventsAfter returns the events after sequence number seq.
func (m *Model) EventsAfter(seq int) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.log[min(seq, len(m.log)):])
}

// LastSeq returns the sequence number of the last event.
func (m *Model) LastSeq() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.log)
}

// dataStruct is the Go type a strategic merge patch is applied against, for
// the core kinds the model knows.
func dataStruct(gk schema.GroupKind) (any, bool) {
	if gk.Group != "" {
		return nil, false
	}
	switch gk.Kind {
	case "Secret":
		return &corev1.Secret{}, true
	case "ConfigMap":
		return &corev1.ConfigMap{}, true
	case "Namespace":
		return &corev1.Namespace{}, true
	}
	return nil, false
}

// Patch applies one patch, as the API server would.
func (m *Model) Patch(k Key, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gr := resourceOf(k.GK)
	cur, ok := m.objs[k]
	if !ok {
		return nil, apierrors.NewNotFound(gr, k.Name)
	}
	doc, err := json.Marshal(map[string]any{
		"apiVersion": schema.GroupVersion{Group: k.GK.Group, Version: "v1"}.String(),
		"kind":       k.GK.Kind,
		"metadata":   cur,
	})
	if err != nil {
		return nil, err
	}
	var patched []byte
	switch pt {
	case types.StrategicMergePatchType:
		ds, ok := dataStruct(k.GK)
		if !ok {
			return nil, apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "patch", gr, k.Name, "the body of the request was in an unknown format", 0, false)
		}
		patched, err = strategicpatch.StrategicMergePatch(doc, data, ds)
		if err != nil {
			return nil, apierrors.NewBadRequest(err.Error())
		}
	case types.MergePatchType:
		patched, err = jsonpatch.MergePatch(doc, data)
		if err != nil {
			return nil, apierrors.NewBadRequest(err.Error())
		}
	case types.JSONPatchType:
		ops, err := jsonpatch.DecodePatch(data)
		if err != nil {
			return nil, apierrors.NewBadRequest(err.Error())
		}
		patched, err = ops.Apply(doc)
		if err != nil {
			// patch.go:423-426: the json-patch text is dropped.
			return nil, apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
		}
	default:
		return nil, apierrors.NewBadRequest("unknown patch type " + string(pt))
	}
	var out struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal(patched, &out); err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	next := out.Metadata
	if next.ResourceVersion != cur.ResourceVersion {
		return nil, apierrors.NewConflict(gr, k.Name, errors.New("the object has been modified; please apply your changes to the latest version and try again"))
	}
	// What an update can't change (rest.BeforeUpdate).
	next.UID, next.Name, next.Namespace = cur.UID, cur.Name, cur.Namespace
	next.CreationTimestamp = cur.CreationTimestamp
	next.DeletionTimestamp = cur.DeletionTimestamp
	if cur.DeletionTimestamp != nil {
		if errs := validation.ValidateNoNewFinalizers(next.Finalizers, cur.Finalizers, field.NewPath("metadata", "finalizers")); len(errs) > 0 {
			return nil, apierrors.NewInvalid(k.GK, k.Name, errs)
		}
	}
	if equalMeta(&next, cur) {
		p := m.partial(k, cur)
		return &p, nil
	}
	m.bump(&next)
	m.store(k, &next)
	p := m.partial(k, &next)
	return &p, nil
}

// store writes meta, or removes the object when it is being deleted and no
// finalizer is left, and emits the event.
func (m *Model) store(k Key, meta *metav1.ObjectMeta) {
	if meta.DeletionTimestamp != nil && len(meta.Finalizers) == 0 && k.GK != NamespaceKind {
		delete(m.objs, k)
		m.emit(watch.Deleted, k, meta)
		return
	}
	m.objs[k] = meta
	m.emit(watch.Modified, k, meta)
}

// equalMeta compares two versions as the API server's no-op check does, for
// the fields the model keeps.
func equalMeta(a, b *metav1.ObjectMeta) bool {
	norm := func(m *metav1.ObjectMeta) metav1.ObjectMeta {
		c := *m.DeepCopy()
		if len(c.Labels) == 0 {
			c.Labels = nil
		}
		if len(c.Annotations) == 0 {
			c.Annotations = nil
		}
		if len(c.Finalizers) == 0 {
			c.Finalizers = nil
		}
		return c
	}
	return reflect.DeepEqual(norm(a), norm(b))
}

// Delete deletes an object: at once when it has no finalizers, otherwise by
// setting its deletionTimestamp. A Namespace always goes Terminating, and is
// removed only by FinalizeNamespace.
func (m *Model) Delete(k Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.objs[k]
	if !ok {
		return apierrors.NewNotFound(resourceOf(k.GK), k.Name)
	}
	if cur.DeletionTimestamp != nil {
		return nil
	}
	next := cur.DeepCopy()
	now := metav1.NewTime(m.now())
	next.DeletionTimestamp = &now
	m.bump(next)
	m.store(k, next)
	return nil
}

// FinalizeNamespace removes a Terminating namespace, as the namespace
// controller's last step does.
func (m *Model) FinalizeNamespace(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := Key{GK: NamespaceKind, Name: name}
	cur, ok := m.objs[k]
	if !ok {
		return apierrors.NewNotFound(resourceOf(NamespaceKind), name)
	}
	if cur.DeletionTimestamp == nil {
		return fmt.Errorf("apimodel: namespace %s isn't being deleted", name)
	}
	delete(m.objs, k)
	m.emit(watch.Deleted, k, cur)
	return nil
}
