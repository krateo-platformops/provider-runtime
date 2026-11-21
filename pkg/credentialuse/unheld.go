package credentialuse

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

// The reasons a ref is listed in a holder's unheld annotation (§11.3).
const (
	// UnheldNotConsented: never held, because its target didn't consent.
	UnheldNotConsented = "NotConsented"
	// UnheldConsentWithdrawn: held, then released when consent was
	// withdrawn.
	UnheldConsentWithdrawn = "ConsentWithdrawn"
	// UnheldReadDenied: held, then released after a persistent 403 on the
	// consent read (PerNamePoll mode, plan PR 9).
	UnheldReadDenied = "ReadDenied"
	// unheldMore is the last entry of a trimmed list.
	unheldMore = "More"
)

// maxUnheld bounds the annotation (§11.3).
const maxUnheld = 16

// An UnheldEntry is one entry of credential-use.krateo.io/unheld.
type UnheldEntry struct {
	Ref    string    `json:"ref"`
	Reason string    `json:"reason"`
	Since  time.Time `json:"since,omitzero"`
	// Count is the number of entries left out, on the More entry only.
	Count int `json:"count,omitempty"`
}

// unheldRef is how the annotation names a ref: <namespace>/<Kind>/<name>,
// with the Kind qualified by its group when it has one, as
// team-b/Secret/db-creds (§11.3).
func unheldRef(id ID) string {
	kind := id.Kind
	if id.Group != "" {
		kind += "." + id.Group
	}
	return id.Namespace + "/" + kind + "/" + id.Name
}

// ReadUnheld parses a holder's unheld annotation. A value that doesn't parse
// reads as empty: the annotation is the runtime's, and the next A1 rewrites
// it.
func ReadUnheld(annotations map[string]string) []UnheldEntry {
	v, ok := annotations[AnnotationUnheld]
	if !ok {
		return nil
	}
	var out []UnheldEntry
	if err := json.Unmarshal([]byte(v), &out); err != nil {
		return nil
	}
	return out
}

// encodeUnheld returns the annotation's value for entries, trimmed to
// maxUnheld (§11.3): ConsentWithdrawn and ReadDenied entries are kept ahead
// of NotConsented ones, the most recent first within each, and past the
// bound the last entry is More, with the count left out. It returns nil
// (remove the annotation) for no entries.
func encodeUnheld(entries []UnheldEntry) (*string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	es := slices.Clone(entries)
	rank := func(e UnheldEntry) int {
		if e.Reason == UnheldNotConsented {
			return 1
		}
		return 0
	}
	slices.SortStableFunc(es, func(a, b UnheldEntry) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), b.Since.Compare(a.Since), cmp.Compare(a.Ref, b.Ref))
	})
	if len(es) > maxUnheld {
		more := len(es) - (maxUnheld - 1)
		es = append(es[:maxUnheld-1], UnheldEntry{Ref: "…", Reason: unheldMore, Count: more})
	}
	b, err := json.Marshal(es)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}
