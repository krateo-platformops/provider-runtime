package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// that is there, being deleted included, and any error, keep every hold.
//
// And the CRD that is gone must still be the kind's: one of another plural, or
// an aggregated API server, may serve the kind since, with holders that use
// the same finalizers. So a mapper that finds it under another resource, the
// API server's discovery, read live after the GET (servedElsewhere), and a
// discovery that can't be read, keep every hold too. Discovery is a veto,
// never the evidence: it isn't a consistent read.
func (t *Tracker) noHolders(ctx context.Context, listErr error) error {
	if !kindUnserved(listErr) {
		return listErr
	}
	m, err := t.s.c.Mapper.RESTMapping(t.gk)
	switch {
	case err == nil && m.Resource.GroupResource() != t.gvr.GroupResource():
		return fmt.Errorf("%w (it is tracked as %s, and is %s now: restart the process)", listErr, t.gvr.GroupResource(), m.Resource.GroupResource())
	case err != nil && !meta.IsNoMatchError(err):
		return listErr
	}
	_, err = t.s.c.Metadata.Resource(crdGVR).Get(ctx, t.crdName, metav1.GetOptions{})
	switch {
	case err == nil:
		return listErr
	case !crdNotFound(err, t.crdName):
		return unread(listErr, "whether its CRD is gone, and its holders with it", "get on the CRD "+t.crdName, err)
	}
	resource, err := t.servedElsewhere(ctx)
	switch {
	case err != nil:
		return unread(listErr, "whether another resource serves it, now that the CRD "+t.crdName+" is gone", "get on the URLs /apis/*, as system:discovery does", err)
	case resource != "":
		return fmt.Errorf("%w (the CRD %s is gone, but %s serves the kind now, so its holds stay: restart the process)", listErr, t.crdName, resource)
	}
	return nil
}

// unread is listErr, with what couldn't be read beside it, and why; and, when
// the API server refused, what to grant.
func unread(listErr error, what, grant string, err error) error {
	if apierrors.IsForbidden(err) {
		return fmt.Errorf("%w (%s, can't be read, so its holds stay: grant %s: %w)", listErr, what, grant, err)
	}
	return fmt.Errorf("%w (%s, can't be read, so its holds stay: %w)", listErr, what, err)
}

// discoveryTimeout bounds the reads of servedElsewhere together: an aggregated
// API server that hangs answers its group's discovery through the API server's
// own timeout, and a release worker waits for it.
const discoveryTimeout = 10 * time.Second

// servedElsewhere reads K's group in the API server's discovery, live: the
// group, then each version of it. It returns the first resource of kind K, or
// "" when none is, as when the group isn't served at all. A version that can't
// be read, of an aggregated API server that is down for one, is an error: the
// kind may be there. No other group is read.
//
// The group's own document, and not the list of groups: just after a CRD is
// created, the API server serves its objects and this document before the
// list, which waits for the group's APIService.
func (t *Tracker) servedElsewhere(ctx context.Context) (string, error) {
	if t.s.c.Discovery == nil {
		return "", errors.New("Clients.Discovery isn't set")
	}
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	var group metav1.APIGroup
	err := t.discover(ctx, &group, t.gk.Group)
	if st, ok := statusOf(err); ok && st.Code == http.StatusNotFound {
		return "", nil // the group isn't served
	}
	if err != nil {
		return "", err
	}
	if group.Kind != "APIGroup" || group.Name != t.gk.Group || len(group.Versions) == 0 {
		return "", fmt.Errorf("/apis/%s isn't the group's discovery: a %q named %q with %d versions", t.gk.Group, group.Kind, group.Name, len(group.Versions))
	}
	for _, v := range group.Versions {
		var list metav1.APIResourceList
		if err := t.discover(ctx, &list, t.gk.Group, v.Version); err != nil {
			return "", err
		}
		if list.Kind != "APIResourceList" || list.GroupVersion != v.GroupVersion {
			return "", fmt.Errorf("/apis/%s isn't the version's discovery: a %q of %q", v.GroupVersion, list.Kind, list.GroupVersion)
		}
		for _, r := range list.APIResources {
			// The finalizer has the kind in lower case.
			if strings.EqualFold(r.Kind, t.gk.Kind) {
				return r.Name + " in " + v.GroupVersion, nil
			}
		}
	}
	return "", nil
}

// discover reads /apis/<path> into out, as JSON: the form that has one group
// or one version a document.
func (t *Tracker) discover(ctx context.Context, out any, path ...string) error {
	data, err := t.s.c.Discovery.Get().AbsPath(append([]string{"/apis"}, path...)...).
		SetHeader("Accept", "application/json").Do(ctx).Raw()
	if err != nil {
		return fmt.Errorf("reading /apis/%s: %w", strings.Join(path, "/"), err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("reading /apis/%s: %w", strings.Join(path, "/"), err)
	}
	return nil
}

// releasedGone logs the first object released for want of the kind, once for
// each time its CRD goes: after R3, which an API server may refuse.
func (t *Tracker) releasedGone(id ID) {
	if t.goneLogged.CompareAndSwap(false, true) {
		t.sinkInfo("credential-use: the holder kind's CRD is gone, and its holders with it: what they held is being released",
			"kind", t.gk.String(), "crd", t.crdName, "first", id.String())
	}
}

// crdAbsent reports whether t's CRD is absent from its informer, which has
// synced: the kind's lists and watches fail then, so the holder cache shows
// what it last saw until the CRD returns, holders that are gone among it. It
// is a hint, as every cache is; whether the CRD is gone, noHolders reads live.
func (t *Tracker) crdAbsent() bool {
	if t.crd == nil || !t.crd.HasSynced() {
		return false
	}
	_, ok, err := t.crd.GetStore().GetByKey(t.crdName)
	return err == nil && !ok
}

// requeueHeld enqueues the batch of every (K, N) finalizer of t on the
// protected objects in the caches, as the write loops' start does, when t's
// CRD goes: the holder informer delivers no more deletions, so nothing else
// is certain to look at a hold whose holder it missed (§10.2). What the
// caches can't show, the objects outside them (§10.5) and those an index
// names, whose watch may be refused, the recheck looks at, if the write loops
// run; their start does otherwise. It also prunes the lines that only ghosts'
// records kept (§10.6.2).
func (s *Shared) requeueHeld(t *Tracker) {
	for _, p := range append(s.protectedInformers(), t.names.all()...) {
		for _, obj := range p.inf.GetStore().List() {
			m, ok := metadataOf(obj)
			if !ok {
				continue
			}
			id := ID{Group: p.gk.Group, Kind: p.gk.Kind, Namespace: m.Namespace, Name: m.Name}
			for n := range holds(t, m) {
				s.queue.Add(batchKey{t.gk, n}, id)
			}
		}
	}
	if len(s.uncachedKinds(t)) > 0 || t.idx != nil {
		s.recheckSoon(t)
	}
}

// recheckSoon runs t's recheck of what the caches can't show beside the write
// loops, if they run. One runs at a time: a recheck asked for meanwhile, as
// while one retries a list that fails, runs once more after it.
func (s *Shared) recheckSoon(t *Tracker) {
	// As endTracking does: an Add only under mu, while the write loops run,
	// so that their end waits for it.
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := s.writeCtx
	if ctx == nil || ctx.Err() != nil {
		return
	}
	if t.rechecking {
		t.recheckAgain = true
		return
	}
	t.rechecking = true
	s.writeWG.Add(1)
	go func() {
		defer s.writeWG.Done()
		for {
			s.recheckUncached(ctx, []*Tracker{t})
			s.mu.Lock()
			again := t.recheckAgain && ctx.Err() == nil
			t.rechecking, t.recheckAgain = again, false
			s.mu.Unlock()
			if !again {
				return
			}
		}
	}()
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
