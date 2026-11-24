package credentialuse

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A holder kind whose CRD is gone while its tracker runs (§6.4, "The holder
// kind itself gone"). Its holders are gone with it: a CRD's deletion removes
// its custom resources first. But R2 can't list a kind that isn't served, and
// a hold of it would stay until someone runs DisownPrefix (§7.4).

// noHolders answers for R2's list of holders that failed with listErr: nil
// when the holder kind is gone, so that no holder exists, and otherwise the
// error the batch fails with, to be retried.
//
// The list's failure is never the evidence. An API server answers 404 for a
// kind that exists while its own CRD informer lags, or the version isn't
// served, or the names aren't accepted yet, and a mapper may know less than
// the API server. The evidence is one live GET of the tracker's CRD, by name,
// with no resourceVersion: a consistent read, whichever API server answers.
// It is made after the list, so after every R1 (§6.4).
//
// Only the API server's own answer that the CRD doesn't exist counts. A CRD
// that is there, being deleted included, and any error, keep every hold. So
// does a mapper that finds the kind under another resource: the CRD that is
// gone isn't the kind's any more.
func (t *Tracker) noHolders(ctx context.Context, listErr error) error {
	if !kindUnserved(listErr) {
		return listErr
	}
	m, err := t.s.c.Mapper.RESTMapping(t.gk)
	switch {
	case err == nil && m.Resource.GroupResource() != t.gvr.GroupResource():
		return fmt.Errorf("%w (it is tracked as %s: restart the process)", listErr, t.gvr.GroupResource())
	case err != nil && !meta.IsNoMatchError(err):
		return listErr
	}
	_, err = t.s.c.Metadata.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{})
	switch {
	case err == nil:
		return listErr
	case !crdNotFound(err, t.crdName):
		return fmt.Errorf("%w (whether its CRD is gone, and its holders with it, can't be read, so its holds stay: grant get on the CRD %s: %w)", listErr, t.crdName, err)
	}
	t.sinkInfo("credential-use: the holder kind's CRD is gone, and its holders with it: what they held is released",
		"kind", t.gk.String(), "crd", t.crdName)
	return nil
}

// kindUnserved reports whether err, of a list, may mean that the kind isn't
// served: a 404, or a mapper that doesn't know the kind.
func kindUnserved(err error) bool {
	if meta.IsNoMatchError(err) {
		return true
	}
	st, ok := statusOf(err)
	return ok && st.Code == http.StatusNotFound
}

// crdNotFound reports whether err, of a GET of CRD name, is the API server's
// answer that it doesn't exist: a 404 that names an object, as Classify takes
// for NotFound (namesObject), and that object is this CRD. Any other 404, as a
// proxy's page, or an API server's that doesn't serve CRDs, says nothing of
// it.
func crdNotFound(err error, name string) bool {
	st, ok := statusOf(err)
	return ok && Classify(err) == ClassNotFound && st.Details.Name == name && st.Details.Group == crdGVR.Group
}
