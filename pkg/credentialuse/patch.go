package credentialuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// A Patch is a patch body and the type to send it as.
type Patch struct {
	Type types.PatchType
	Data []byte
}

var (
	// ErrNotHeld: the object doesn't carry the finalizer a release would
	// remove, so it is already released (§6.2 R1).
	ErrNotHeld = errors.New("credential-use: the object doesn't carry the finalizer")
	// ErrNoEpoch: the object carries the finalizer but not its epoch. Only a
	// third party causes that, and R1 then sets a fresh epoch and requeues.
	ErrNoEpoch = errors.New("credential-use: the object carries the finalizer but no epoch")
	// ErrNothingToClean: the janitor found no label to remove.
	ErrNothingToClean = errors.New("credential-use: no label to remove")
)

// keys are the keys an acquiring write sets for holder kind K in holder
// namespace N.
type keys struct {
	finalizer string // also the epoch annotation's key
	marker    string
	prefix    string
}

func keysFor(holder schema.GroupKind, namespace string) (keys, error) {
	f, err := FinalizerName(holder, namespace)
	if err != nil {
		return keys{}, err
	}
	prefix, err := FinalizerPrefix(holder)
	if err != nil {
		return keys{}, err
	}
	return keys{finalizer: f, marker: MarkerLabel(holder), prefix: prefix}, nil
}

// AcquireStrategicPatch builds A2 for a built-in kind such as a Secret or a
// ConfigMap (§6.2): one strategic merge patch, sent with no precondition. It
// merges the finalizer into metadata.finalizers as a set, sets the epoch, and
// sets the held label and K's marker label. Finalizers are tagged
// patchStrategy:"merge", so the patch can't drop or duplicate anyone else's.
//
// Custom resources don't accept strategic merge patches: use
// AcquireGuardedPatch for them.
func AcquireStrategicPatch(holder schema.GroupKind, namespace, epoch string) (Patch, error) {
	k, err := keysFor(holder, namespace)
	if err != nil {
		return Patch{}, err
	}
	if epoch == "" {
		return Patch{}, errors.New("credential-use: an acquiring write needs an epoch")
	}
	body := map[string]any{"metadata": map[string]any{
		"finalizers":  []string{k.finalizer},
		"annotations": map[string]string{k.finalizer: epoch},
		"labels":      map[string]string{LabelHeld: "true", k.marker: "true"},
	}}
	data, err := json.Marshal(body)
	return Patch{Type: types.StrategicMergePatchType, Data: data}, err
}

// AcquireGuardedPatch builds A2 for a custom resource (§6.2) from the object
// a live GET just returned: a merge patch guarded by its resourceVersion that
// writes the whole finalizer list with the finalizer added, the epoch and both
// labels. A merge patch replaces the list, so the guard is what keeps another
// writer's change from being lost: a conflict GETs and retries.
func AcquireGuardedPatch(current metav1.Object, holder schema.GroupKind, namespace, epoch string) (Patch, error) {
	k, err := keysFor(holder, namespace)
	if err != nil {
		return Patch{}, err
	}
	if epoch == "" {
		return Patch{}, errors.New("credential-use: an acquiring write needs an epoch")
	}
	rv := current.GetResourceVersion()
	if rv == "" {
		return Patch{}, errors.New("credential-use: a guarded write needs the object's resourceVersion")
	}
	finalizers := slices.Clone(current.GetFinalizers())
	if !slices.Contains(finalizers, k.finalizer) {
		finalizers = append(finalizers, k.finalizer)
	}
	body := map[string]any{"metadata": map[string]any{
		"resourceVersion": rv,
		"finalizers":      finalizers,
		"annotations":     map[string]string{k.finalizer: epoch},
		"labels":          map[string]string{LabelHeld: "true", k.marker: "true"},
	}}
	data, err := json.Marshal(body)
	return Patch{Type: types.MergePatchType, Data: data}, err
}

// jsonPatchOp is one RFC 6902 operation. omitempty drops only a nil Value,
// which is a remove's: an interface holding "" or an empty list is not empty
// to encoding/json, so a test of an empty value keeps its value.
type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// escapePointer escapes a key for a JSON pointer (RFC 6901): ~ is written as
// ~0, then / as ~1.
func escapePointer(key string) string {
	return strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
}

// ReleasePatch builds R3 for holder kind K in holder namespace N, from the
// object as R1 read it (§6.2):
//
//	test   the epoch annotation is the one R1 read
//	test   metadata.finalizers[i] is the finalizer
//	remove metadata.finalizers[i]
//	remove the epoch annotation
//
// When the finalizer is K's last on the object, the patch also removes K's
// marker label, and when it is the last credential-use finalizer of any kind,
// the held label. Either way it then also tests the whole finalizer array as R1
// read it, so that a concurrent acquiring write of any kind, which adds its
// finalizer, breaks the patch before a label it needs is removed. A label that
// is already absent is left alone, so that its remove can't fail, and a label
// it removes is tested first with its value as read. So the patch tests every
// value it removes, and applies to exactly the state it was built from.
//
// The epoch test is what makes a concurrent A2 break the release: every A2
// writes a fresh epoch (§6.4). A failed test is ClassStateChanged
// (ClassifyJSONPatch), and the releaser goes back to R1.
//
// It returns ErrNotHeld when the object lacks the finalizer, and ErrNoEpoch
// when it has the finalizer but no epoch, or an empty one.
func ReleasePatch(read metav1.Object, holder schema.GroupKind, namespace string) (Patch, error) {
	k, err := keysFor(holder, namespace)
	if err != nil {
		return Patch{}, err
	}
	finalizers := read.GetFinalizers()
	i := slices.Index(finalizers, k.finalizer)
	if i < 0 {
		return Patch{}, ErrNotHeld
	}
	epoch := read.GetAnnotations()[k.finalizer]
	if epoch == "" {
		// Absent, or emptied by a third party: a fresh epoch is set first.
		return Patch{}, ErrNoEpoch
	}
	otherOfK, otherOfAny := false, false
	for j, f := range finalizers {
		if j == i {
			continue
		}
		if strings.HasPrefix(f, k.prefix) {
			otherOfK = true
		}
		if IsCredentialUseFinalizer(f) {
			otherOfAny = true
		}
	}
	labels := read.GetLabels()
	markerValue, hasMarker := labels[k.marker]
	heldValue, hasHeld := labels[LabelHeld]
	removeMarker := !otherOfK && hasMarker
	removeHeld := !otherOfAny && hasHeld

	annotation := "/metadata/annotations/" + escapePointer(k.finalizer)
	index := "/metadata/finalizers/" + strconv.Itoa(i)
	markerPath := "/metadata/labels/" + escapePointer(k.marker)
	heldPath := "/metadata/labels/" + escapePointer(LabelHeld)
	// Every value the patch removes is tested first, as R1 read it (L1 of
	// the review of revision 16): a failure then always means a change, and
	// a failure against an unchanged object is a defect, not contention.
	ops := []jsonPatchOp{
		{Op: "test", Path: annotation, Value: epoch},
		{Op: "test", Path: index, Value: k.finalizer},
	}
	if removeMarker || removeHeld {
		ops = append(ops, jsonPatchOp{Op: "test", Path: "/metadata/finalizers", Value: finalizers})
	}
	if removeMarker {
		ops = append(ops, jsonPatchOp{Op: "test", Path: markerPath, Value: markerValue})
	}
	if removeHeld {
		ops = append(ops, jsonPatchOp{Op: "test", Path: heldPath, Value: heldValue})
	}
	ops = append(ops,
		jsonPatchOp{Op: "remove", Path: index},
		jsonPatchOp{Op: "remove", Path: annotation},
	)
	if removeMarker {
		ops = append(ops, jsonPatchOp{Op: "remove", Path: markerPath})
	}
	if removeHeld {
		ops = append(ops, jsonPatchOp{Op: "remove", Path: heldPath})
	}
	data, err := json.Marshal(ops)
	return Patch{Type: types.JSONPatchType, Data: data}, err
}

// RestoreEpochPatch builds R1's repair for an object that carries holder kind
// K's finalizer for namespace N but not its epoch (§6.2 R1): it sets a fresh
// epoch. It tests the object's resourceVersion as read, so it lands only on
// the state that was read: any change in between is ClassStateChanged, and R1
// reads again.
func RestoreEpochPatch(read metav1.Object, holder schema.GroupKind, namespace, epoch string) (Patch, error) {
	k, err := keysFor(holder, namespace)
	if err != nil {
		return Patch{}, err
	}
	if epoch == "" {
		return Patch{}, errors.New("credential-use: restoring an epoch needs an epoch")
	}
	if !slices.Contains(read.GetFinalizers(), k.finalizer) {
		return Patch{}, ErrNotHeld
	}
	ops, err := testResourceVersion(read)
	if err != nil {
		return Patch{}, err
	}
	if read.GetAnnotations() == nil {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/metadata/annotations", Value: map[string]string{k.finalizer: epoch}})
	} else {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/metadata/annotations/" + escapePointer(k.finalizer), Value: epoch})
	}
	data, err := json.Marshal(ops)
	return Patch{Type: types.JSONPatchType, Data: data}, err
}

// JanitorPatch builds the label janitor's patch for holder kind K (§10.1): on
// an object a third party stripped of its finalizers but not its labels, it
// removes K's marker label when no finalizer of K is left, and the held label
// when no credential-use finalizer of any kind is left. It tests the
// object's resourceVersion as read, which implies the design's test that the
// finalizer array is unchanged, so it can't race a new acquiring write: any
// write in between is ClassStateChanged.
//
// It returns ErrNothingToClean when no label is to be removed.
func JanitorPatch(read metav1.Object, holder schema.GroupKind) (Patch, error) {
	prefix, err := FinalizerPrefix(holder)
	if err != nil {
		return Patch{}, err
	}
	marker := MarkerLabel(holder)
	anyOfK, anyOfAny := false, false
	for _, f := range read.GetFinalizers() {
		if strings.HasPrefix(f, prefix) {
			anyOfK = true
		}
		if IsCredentialUseFinalizer(f) {
			anyOfAny = true
		}
	}
	labels := read.GetLabels()
	_, hasMarker := labels[marker]
	_, hasHeld := labels[LabelHeld]
	removeMarker := hasMarker && !anyOfK
	removeHeld := hasHeld && !anyOfAny
	if !removeMarker && !removeHeld {
		return Patch{}, ErrNothingToClean
	}
	ops, err := testResourceVersion(read)
	if err != nil {
		return Patch{}, err
	}
	if removeMarker {
		ops = append(ops, jsonPatchOp{Op: "remove", Path: "/metadata/labels/" + escapePointer(marker)})
	}
	if removeHeld {
		ops = append(ops, jsonPatchOp{Op: "remove", Path: "/metadata/labels/" + escapePointer(LabelHeld)})
	}
	data, err := json.Marshal(ops)
	return Patch{Type: types.JSONPatchType, Data: data}, err
}

func testResourceVersion(read metav1.Object) ([]jsonPatchOp, error) {
	rv := read.GetResourceVersion()
	if rv == "" {
		return nil, fmt.Errorf("credential-use: a patch that tests the state read needs the object's resourceVersion")
	}
	return []jsonPatchOp{{Op: "test", Path: "/metadata/resourceVersion", Value: rv}}, nil
}
