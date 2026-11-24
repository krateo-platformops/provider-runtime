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

	// listed: the entry was in the annotation the pass found. Not written.
	listed bool
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
// bound the last entry is More, with the count left out. A More entry among
// entries is one that a list already trimmed: its count is added. It returns
// nil (remove the annotation) for no entries.
func encodeUnheld(entries []UnheldEntry) (*string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	already := 0
	es := slices.DeleteFunc(slices.Clone(entries), func(e UnheldEntry) bool {
		if e.Reason != unheldMore {
			return false
		}
		already += e.Count
		return true
	})
	rank := func(e UnheldEntry) int {
		if e.Reason == UnheldNotConsented {
			return 1
		}
		return 0
	}
	// §11.3's order: withdrawals and ReadDenied ahead of NotConsented, the
	// most recent first within each.
	order := func(a, b UnheldEntry) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), b.Since.Compare(a.Since), cmp.Compare(a.Ref, b.Ref))
	}
	if len(es) > maxUnheld || already > 0 {
		// Which entries a trim keeps: among NotConsented ones, those already
		// listed before those that aren't. A ref trimmed into More isn't in
		// the annotation, so it comes back each pass as if new, and must not
		// displace a listed one, or the list would rotate, and be rewritten,
		// on every pass. Withdrawals keep their real since.
		listedFirst := func(a, b UnheldEntry) int {
			if rank(a) == 0 || a.listed == b.listed {
				return 0
			}
			if a.listed {
				return -1
			}
			return 1
		}
		slices.SortStableFunc(es, func(a, b UnheldEntry) int {
			return cmp.Or(cmp.Compare(rank(a), rank(b)), listedFirst(a, b), order(a, b))
		})
		shown := min(len(es), maxUnheld-1)
		more := already + len(es) - shown
		es = es[:shown]
		slices.SortStableFunc(es, order)
		es = append(es, UnheldEntry{Ref: "…", Reason: unheldMore, Count: more})
	} else {
		slices.SortStableFunc(es, order)
	}
	b, err := json.Marshal(es)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}
