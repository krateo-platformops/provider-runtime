package credentialuse

import (
	"cmp"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// secretGroupKind is the GroupKind of a core Secret.
var secretGroupKind = schema.GroupKind{Group: "", Kind: "Secret"}

// A Ref names an object a holder reads to reach its external system: a
// Secret, or an intermediate object that leads to one (§9).
type Ref struct {
	GroupKind schema.GroupKind // {"" "Secret"} for a Secret
	Namespace string           // empty for a cluster-scoped object
	Name      string

	// Next, when set, names the refs found inside this object. The tracker
	// calls it after the object is held, with the object it read (§9),
	// without managedFields and status. A held ref with a Next must be of a
	// protected kind with Content (§10.4): the content cache is how a
	// re-point of it is seen.
	//
	// When the object lacks what Next reads, which may not be populated yet,
	// or be lost in a restore, Next must return an error, never nil refs: an
	// error keeps the record, and so the holds, while nil refs say nothing
	// lies behind the object, and drop what the holder still uses. The error
	// must not contain the object's values: it becomes an event and a log.
	// The errors of unstructured.NestedString and its like do contain them,
	// so don't wrap those.
	//
	// Of the refs of one object that a pass meets, the first wins: a later one
	// ends there, whatever it asks for (§9.1, AsksMoreThan).
	Next func(obj *unstructured.Unstructured) ([]Ref, error)

	// Guaranteed declares that the object's own controller already keeps it
	// alive while the holder exists (§9.3). It is neither recorded nor held;
	// its Next is still followed. No change to it enqueues a holder that
	// names it only as Guaranteed, which sees the change on its next pass.
	Guaranteed bool
}

// AsksMoreThan reports what r asks for that first, another ref of the same
// object, doesn't: to hold the object, where first is Guaranteed; and to follow
// a Next, where first has none. Two different Nexts can't be told apart. An
// Acquire takes the first ref it meets of an object, and reports a later one
// that asks for more, for a hold only when it could have been made (§9.1).
func (r Ref) AsksMoreThan(first Ref) (hold, next bool) {
	return first.Guaranteed && !r.Guaranteed, first.Next == nil && r.Next != nil
}

// SecretRef returns a Ref to the Secret namespace/name.
func SecretRef(namespace, name string) Ref {
	return Ref{GroupKind: secretGroupKind, Namespace: namespace, Name: name}
}

// ID returns the ref's identity, which is what the record stores (§4).
func (r Ref) ID() ID {
	return ID{Group: r.GroupKind.Group, Kind: r.GroupKind.Kind, Namespace: r.Namespace, Name: r.Name}
}

// An ID is the identity of a protected object: its group, kind, namespace and
// name. Two keys of one Secret are one ID. The JSON form is the record's
// (§4), so every field is always present, the empty group included.
type ID struct {
	Group     string `json:"group"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// GroupKind returns the ID's GroupKind.
func (id ID) GroupKind() schema.GroupKind {
	return schema.GroupKind{Group: id.Group, Kind: id.Kind}
}

// String returns <group>/<kind>/<namespace>/<name>, the index line of §10.6.
// The group is empty for core kinds, and so is the namespace for a
// cluster-scoped object.
func (id ID) String() string {
	return id.Group + "/" + id.Kind + "/" + id.Namespace + "/" + id.Name
}

// compareIDs orders IDs by group, kind, namespace, then name.
func compareIDs(a, b ID) int {
	return cmp.Or(
		cmp.Compare(a.Group, b.Group),
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
	)
}

// SortedIDs returns ids sorted and deduplicated, as the record keeps them
// (§4). It doesn't modify ids.
func SortedIDs(ids []ID) []ID {
	out := slices.Clone(ids)
	slices.SortFunc(out, compareIDs)
	return slices.Compact(out)
}
