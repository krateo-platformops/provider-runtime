package credentialuse

import (
	"context"
	"io"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
)

// An informer's lists and watches that fail. The core sets its own watch
// error handler on every informer, to fail a start on a 403, and that takes
// client-go's, which logs, away. So the core logs them itself, bounded: once
// per informer and cause until the informer watches again, however often the
// reflector retries, and on the tracker's own sink, where an operator of the
// kind looks, for the informers that belong to one. The shared ones, of the
// Namespaces and the protected kinds, belong to no tracker: theirs go where
// client-go's would.

// A watchFailure is one informer, or one set of per-name informers, failing
// for one cause: the error's class (shortError).
type watchFailure struct{ what, cause string }

// watchFailures are the failures that last, with which informers of the set
// fail with each: one, the zero ID, for an informer that isn't per-name.
type watchFailures struct {
	mu sync.Mutex
	by map[watchFailure]map[ID]bool
}

// failed records that informer who of what failed for cause, and reports
// whether it is the first of them that does.
func (f *watchFailures) failed(what, cause string, who ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.by == nil {
		f.by = map[watchFailure]map[ID]bool{}
	}
	k := watchFailure{what, cause}
	first := f.by[k] == nil
	if first {
		f.by[k] = map[ID]bool{}
	}
	f.by[k][who] = true
	return first
}

// recovered records that informer who of what watches again, or is gone.
func (f *watchFailures) recovered(what string, who ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, ids := range f.by {
		if k.what != what {
			continue
		}
		if delete(ids, who); len(ids) == 0 {
			delete(f.by, k)
		}
	}
}

// routineWatchEnd reports whether err is how a watch ends in the normal run
// of things, which client-go's handler doesn't log as an error: exactly its
// cases. An EOF that something wraps is a connection that broke.
func routineWatchEnd(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err) || err == io.EOF || err == io.ErrUnexpectedEOF //nolint:errorlint // as client-go
}

// watchFailed logs that informer what failed with err, once per cause until
// it watches again: on t's sink, or, when t is nil, as client-go does.
// Before the first sync a 403 is the start's error, which is returned.
func (s *Shared) watchFailed(ctx context.Context, r *cache.Reflector, t *Tracker, what string, synced bool, err error) {
	switch {
	case ctx.Err() != nil:
		return // it stops
	case routineWatchEnd(err):
		cache.DefaultWatchErrorHandler(ctx, r, err)
		return
	case !synced && apierrors.IsForbidden(err):
		return
	case !s.failures.failed(what, shortError(err), ID{}):
		return
	}
	if t == nil {
		cache.DefaultWatchErrorHandler(ctx, r, err)
		return
	}
	t.warn("credential-use: "+what+" can't list or watch: it is retried with backoff, and this is logged once until it watches again",
		"kind", t.gk.String(), "why", shortError(err), "error", err.Error())
}

// watchRecovered returns what informer what calls when it watches again.
func (s *Shared) watchRecovered(what string) func() {
	return func() { s.failures.recovered(what, ID{}) }
}

// watchFailed is the watch error handler of w, the per-name informer of id:
// a 403 is logged once per name, since each name needs its own grant; any
// other failure once per kind and cause, until every informer of the kind
// watches again, since what one meets the others do.
func (n *perNames) watchFailed(ctx context.Context, r *cache.Reflector, id ID, w *nameWatch, err error) {
	if ctx.Err() != nil {
		return // it stops
	}
	if routineWatchEnd(err) {
		cache.DefaultWatchErrorHandler(ctx, r, err)
		return
	}
	if apierrors.IsForbidden(err) {
		n.forbiddenOnce(id, w, err)
		return
	}
	n.mu.Lock()
	first := n.by[id] == w && n.failures.failed(id.GroupKind().String(), shortError(err), id)
	n.mu.Unlock()
	if first {
		n.t.warn("credential-use: a per-name informer can't list or watch: it is retried with backoff, and this is logged once, whatever the number of objects, until they all watch again",
			"kind", n.t.gk.String(), "object", id.String(), "why", shortError(err), "error", err.Error())
	}
}

// watching records that w, the per-name informer of id, watches again: a
// failure after it is logged again. One that has stopped records nothing.
func (n *perNames) watching(id ID, w *nameWatch) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.by[id] == w {
		delete(n.forbidden, id)
		n.failures.recovered(id.GroupKind().String(), id)
	}
}
