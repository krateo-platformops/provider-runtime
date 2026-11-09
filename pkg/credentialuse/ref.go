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
	// calls it after the object is held, with the object it read (§9).
	Next func(obj *unstructured.Unstructured) ([]Ref, error)

	// Guaranteed declares that the object's own controller already keeps it
	// alive while the holder exists (§9.3). It is neither recorded nor held;
	// its Next is still followed.
	Guaranteed bool
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
