package credentialuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

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
// A record is read strictly, and anything it can't read exactly fails safe:
// the holder then counts as a user of everything (§4). A record is a JSON
// array of objects, each with exactly the four keys group, kind, namespace
// and name, spelled exactly, each once, each a JSON string, kind and name
// non-empty, and nothing after the array. So a missing or null field, which
// a lenient decoder would read as an empty one and so as another object (a
// missing namespace reads as cluster-scoped), is unparseable. A record of a
// later format goes under a new key (refs.v2), so an unknown key here is
// damage, not a newer format.
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

// recordKeys are an entry's keys; each must appear exactly once.
var recordKeys = []string{"group", "kind", "namespace", "name"}

func decodeRecord(v string) ([]ID, error) {
	dec := json.NewDecoder(strings.NewReader(v))
	if err := expectDelim(dec, '['); err != nil {
		return nil, err
	}
	ids := []ID{}
	for dec.More() {
		id, err := decodeEntry(dec)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := expectDelim(dec, ']'); err != nil {
		return nil, err
	}
	// Nothing may follow: not another value, not a stray ] or }, which
	// More() doesn't see.
	if t, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("data after the record: %v %v", t, err)
	}
	return ids, nil
}

func decodeEntry(dec *json.Decoder) (ID, error) {
	if err := expectDelim(dec, '{'); err != nil {
		return ID{}, err
	}
	values := map[string]string{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return ID{}, err
		}
		key, ok := t.(string)
		if !ok || !slices.Contains(recordKeys, key) {
			return ID{}, fmt.Errorf("record entry has the key %v", t)
		}
		if _, dup := values[key]; dup {
			return ID{}, fmt.Errorf("record entry has the key %q twice", key)
		}
		t, err = dec.Token()
		if err != nil {
			return ID{}, err
		}
		value, ok := t.(string) // a JSON null is nil, not a string
		if !ok {
			return ID{}, fmt.Errorf("record entry's %q is %v, not a string", key, t)
		}
		values[key] = value
	}
	if err := expectDelim(dec, '}'); err != nil {
		return ID{}, err
	}
	if len(values) != len(recordKeys) {
		return ID{}, fmt.Errorf("record entry has %d of the keys %v", len(values), recordKeys)
	}
	id := ID{Group: values["group"], Kind: values["kind"], Namespace: values["namespace"], Name: values["name"]}
	if id.Kind == "" || id.Name == "" {
		return ID{}, fmt.Errorf("record entry %s has no kind or no name", id)
	}
	return id, nil
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != want {
		return fmt.Errorf("record: expected %v, found %v", want, t)
	}
	return nil
}

// Names reports whether the record names id. An unparseable record names
// every id (§4, §6.1). It doesn't rely on IDs being sorted, so a Record built
// by hand answers correctly too.
func (r Record) Names(id ID) bool {
	if r.Unparseable {
		return true
	}
	return slices.Contains(r.IDs, id)
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
