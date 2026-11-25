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
		// As just after the CRD is created; for good, the kind is served no
		// more as Track mapped it, which a new process maps anew.
		return fmt.Errorf("%w (the CRD %s is there, so its holds stay; if this lasts, it no longer serves %s as this process maps it, as after it was created again with another scope, or without that version: restart the process)",
			listErr, t.crdName, t.gvr)
	case !crdNotFound(err, t.crdName):
		return unread(listErr, "whether its CRD is gone, and its holders with it", "get on the CRD "+t.crdName, err)
	}
	resource, gv, err := t.servedElsewhere(ctx)
	plural, _, _ := strings.Cut(resource, "/")
	switch {
	case err != nil:
		return unread(listErr, "whether another resource serves it, now that the CRD "+t.crdName+" is gone", "get on the URLs /apis/*, as system:discovery does", err)
	case plural == t.gvr.Resource:
		// As just after the CRD went: no restart is needed, or would help,
		// since Track needs the CRD.
		return fmt.Errorf("%w (the CRD %s is gone, but discovery still shows %s in %s, so its holds stay until it doesn't)", listErr, t.crdName, resource, gv)
	case resource != "":
		return fmt.Errorf("%w (the CRD %s is gone, but %s in %s serves the kind now, so its holds stay: restart the process)", listErr, t.crdName, resource, gv)
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
// group, then each version of it. It returns the first resource of kind K, with
// its group-version, or "" when none is, as when the group isn't served at all.
// A version that can't be read, of an aggregated API server that is down for
// one, is an error: the kind may be there. No other group is read.
//
// The group's own document, and not the list of groups: just after a group's
// first CRD is created, the API server serves its objects and this document
// before the list, which waits for the group's APIService. A version that comes
// later to a group waits for its APIService in both.
func (t *Tracker) servedElsewhere(ctx context.Context) (resource, groupVersion string, err error) {
	if t.s.c.Discovery == nil {
		return "", "", errors.New("Clients.Discovery isn't set")
	}
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	var group metav1.APIGroup
	err = t.discover(ctx, &group, t.gk.Group)
	if st, ok := statusOf(err); ok && st.Code == http.StatusNotFound {
		return "", "", nil // the group isn't served
	}
	if err != nil {
		return "", "", err
	}
	if group.Kind != "APIGroup" || group.Name != t.gk.Group || len(group.Versions) == 0 {
		return "", "", fmt.Errorf("/apis/%s isn't the group's discovery: a %q named %q with %d versions", t.gk.Group, group.Kind, group.Name, len(group.Versions))
	}
	for _, v := range group.Versions {
		var list metav1.APIResourceList
		if err := t.discover(ctx, &list, t.gk.Group, v.Version); err != nil {
			return "", "", err
		}
		if list.Kind != "APIResourceList" || list.GroupVersion != v.GroupVersion {
			return "", "", fmt.Errorf("/apis/%s isn't the version's discovery: a %q of %q", v.GroupVersion, list.Kind, list.GroupVersion)
		}
		for _, r := range list.APIResources {
			if r.Group != "" && r.Group != t.gk.Group {
				continue // another group's kind, as a CRD's scale subresource is
			}
			// The finalizer has the kind in lower case.
			if strings.EqualFold(r.Kind, t.gk.Kind) {
				return r.Name, v.GroupVersion, nil
			}
		}
	}
	return "", "", nil
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
	// As endTracking: only for the kind's tracker, and not one that a Track
	// rolled back as the CRD went.
	s.mu.Lock()
	live := s.trackers[t.gk] == t
	s.mu.Unlock()
	if !live {
		return
	}
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
// loops, if they run, and t is its kind's tracker. One runs at a time. A
// recheck asked for meanwhile cuts the run's wait short: if the run waits to
// retry a part that fails, as for good, it starts again at once with every
// part, and nothing of the last try counted as done; if it doesn't, it runs
// once more when it ends. Many asks are one. A part in flight runs to its end:
// a choice, since none needs it, an index prune least, which finishes its write
// and its check under its own bound whatever ctx does (removeChunk).
func (s *Shared) recheckSoon(t *Tracker) {
	// As endTracking does: an Add only under mu, while the write loops run,
	// so that their end waits for it.
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := s.writeCtx
	if ctx == nil || ctx.Err() != nil || s.trackers[t.gk] != t {
		return
	}
	if t.recheckAsk == nil {
		t.recheckAsk = make(chan struct{}, 1)
	}
	if t.rechecking {
		select {
		case t.recheckAsk <- struct{}{}:
		default: // one is asked for already
		}
		return
	}
	t.rechecking = true
	ask := t.recheckAsk
	s.writeWG.Add(1)
	go func() {
		defer s.writeWG.Done()
		for {
			s.recheckUncachedAsked(ctx, []*Tracker{t}, ask)
			// Under mu, as the send is: an ask is either taken here, or
			// made after rechecking is false, and starts a run itself.
			s.mu.Lock()
			again := false
			select {
			case <-ask:
				again = ctx.Err() == nil
			default:
			}
			t.rechecking = again
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
