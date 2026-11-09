package credentialuse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A Record is what a holder's AnnotationRefs says: the protected objects it
// reads (§4). Release reads only records, never RefsFunc.
type Record struct {
	// Present reports whether the holder carries the annotation at all.
	Present bool
	// Unparseable reports a present annotation that can't be read. Such a
	// holder counts as a user of every protected object its namespace's
	// check considers: it fails safe (§4).
	Unparseable bool
	// IDs are the recorded refs, sorted and deduplicated. Empty when the
	// record is absent or unparseable.
	IDs []ID
}

// ReadRecord reads obj's record.
//
// A record is unparseable when its value isn't a JSON array of IDs with no
// unknown fields, or when an entry has no kind or no name. A record written by
// a later format goes under a new key (refs.v2), so an unknown field here is
// not a newer format: it is damage, and it fails safe.
func ReadRecord(obj metav1.Object) Record {
	v, ok := obj.GetAnnotations()[AnnotationRefs]
	if !ok {
		return Record{}
	}
	ids, err := decodeRecord(v)
	if err != nil {
		return Record{Present: true, Unparseable: true}
	}
	return Record{Present: true, IDs: SortedIDs(ids)}
}

func decodeRecord(v string) ([]ID, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(v)))
	dec.DisallowUnknownFields()
	var ids []ID
	if err := dec.Decode(&ids); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after the record")
	}
	if ids == nil {
		// "null" is not an array.
		return nil, fmt.Errorf("the record is not an array")
	}
	for _, id := range ids {
		if id.Kind == "" || id.Name == "" {
			return nil, fmt.Errorf("record entry %s has no kind or no name", id)
		}
	}
	return ids, nil
}

// Names reports whether the record names id. An unparseable record names
// every id (§4, §6.1).
func (r Record) Names(id ID) bool {
	if r.Unparseable {
		return true
	}
	_, found := slices.BinarySearchFunc(r.IDs, id, compareIDs)
	return found
}

// EncodeRecord returns the AnnotationRefs value for ids: a JSON array, sorted
// and deduplicated by identity (§4). No refs encode as "[]".
func EncodeRecord(ids []ID) (string, error) {
	ids = SortedIDs(ids)
	if ids == nil {
		ids = []ID{}
	}
	for _, id := range ids {
		if id.Kind == "" || id.Name == "" {
			return "", fmt.Errorf("credential-use: ref %s has no kind or no name", id)
		}
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
