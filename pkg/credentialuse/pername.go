package credentialuse

import (
	"context"
	"slices"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// The per-name watches (§13 shape 1, §10.6). Under resourceNames RBAC, list
// and watch are allowed only with a metadata.name field selector, so a
// PerName kind has no label-scoped informer: its tracker watches each object
// its index names, one field-selected informer per line, in place of the
// label-scoped cache. Each delivers every change to its one object, and the
// tracker's protected handler acts on it as on the label-scoped cache's:
// release batches on a change to the kind's finalizers, grace timers on a
// deletion, and the holders on a loss (§10.2). The janitor's handler sees it
// too. The watches follow the index's lines, as the tracker's index informer
// delivers them; they serve the fast paths only, so one that hasn't synced
// yet makes a pass write when it could have skipped.

// A nameWatch is one per-name informer and the tracker's handler on it.
type nameWatch struct {
	p    *protectedInformer
	h    *protectedHandler
	stop context.CancelFunc
	// dead, under the tracker's heldMu: the watch has stopped, so its
	// handler's counts no longer reach the gauges.
	dead bool
}

// perNames are one tracker's per-name watches.
type perNames struct {
	t *Tracker
	// infos are the PerName kinds' resources, mapped when Track built the
	// tracker: a handler mustn't wait on the mapper.
	infos map[schema.GroupKind]perNameKind

	mu sync.Mutex
	// parent is the context the watches run under: RunInformers', or a
	// Track after it's; nil until then.
	parent context.Context
	// want are the IDs the index names, as last delivered.
	want []ID
	by   map[ID]*nameWatch
	// forbidden are the names whose watch the server forbade, logged once.
	forbidden map[ID]bool
}

// perNameKind is a PerName kind's resource.
type perNameKind struct {
	gvr        schema.GroupVersionResource
	namespaced bool
	content    bool
}

func newPerNames(t *Tracker) *perNames {
	return &perNames{t: t, infos: map[schema.GroupKind]perNameKind{}, by: map[ID]*nameWatch{}, forbidden: map[ID]bool{}}
}

// mapKinds maps t's PerName kinds' resources.
func (n *perNames) mapKinds(mapper meta.RESTMapper, protected []ProtectedKind) error {
	for _, pk := range protected {
		if pk.Watch != PerName {
			continue
		}
		m, err := mapper.RESTMapping(pk.GVK.GroupKind(), pk.GVK.Version)
		if err != nil {
			return err
		}
		n.infos[pk.GVK.GroupKind()] = perNameKind{gvr: m.Resource, namespaced: m.Scope.Name() == meta.RESTScopeNameNamespace, content: pk.Content}
	}
	return nil
}

// start sets the context the watches run under, and starts those the
// index already names.
func (n *perNames) start(parent context.Context) {
	n.mu.Lock()
	n.parent = parent
	want := slices.Clone(n.want)
	n.mu.Unlock()
	n.follow(want)
}

// follow starts a watch for every ID of want without one, and stops every
// watch whose ID want no longer has. Before start it only remembers want.
func (n *perNames) follow(want []ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.want = slices.Clone(want)
	if n.parent == nil || n.parent.Err() != nil {
		return
	}
	keep := map[ID]bool{}
	for _, id := range want {
		keep[id] = true
		if n.by[id] == nil {
			if w := n.newWatch(id); w != nil {
				n.by[id] = w
			}
		}
	}
	for id, w := range n.by {
		if !keep[id] {
			n.stopLocked(id, w)
		}
	}
}

// stopLocked stops w, the watch of id, and takes its counts off the
// gauges. n.mu is held.
func (n *perNames) stopLocked(id ID, w *nameWatch) {
	w.stop()
	delete(n.by, id)
	n.t.dropCounts(w)
}

// stopAll stops every watch, as a failed Track does.
func (n *perNames) stopAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, w := range n.by {
		n.stopLocked(id, w)
	}
	n.parent = nil
}

// newWatch builds and starts the per-name informer of id, with the
// tracker's handler and the janitor's on it, or returns nil for what can't be
// watched. n.mu is held.
func (n *perNames) newWatch(id ID) *nameWatch {
	t, s := n.t, n.t.s
	info, ok := n.infos[id.GroupKind()]
	if !ok {
		return nil
	}
	if !mayExist(info.namespaced, id.Namespace, id.Name) {
		return nil // it names no object (clientServer.object)
	}
	ns := id.Namespace
	p := &protectedInformer{gk: id.GroupKind(), ns: ns, name: id.Name, gvr: info.gvr, content: info.content, failed: make(chan struct{})}
	var example runtime.Object = &metav1.PartialObjectMetadata{}
	tf := transform(isCredentialUseKey, isCredentialUseKey)
	if info.content {
		p.lw = newContentListWatch(s.c.Dynamic, info.gvr, ns, "", s.clock)
		example, tf = &unstructured.Unstructured{}, contentTransform
	} else {
		p.lw = newListWatch(s.c.Metadata, info.gvr, ns, "", s.clock, nil)
	}
	p.lw.field = fields.OneTermEqualSelector("metadata.name", id.Name).String()
	p.inf = cache.NewSharedIndexInformerWithOptions(p.lw, example, cache.SharedIndexInformerOptions{
		Indexers:          cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		ObjectDescription: "credential-use " + p.what(),
	})
	if err := p.inf.SetTransform(tf); err != nil {
		return nil
	}
	w := &nameWatch{p: p}
	w.h = &protectedHandler{s: s, p: p, t: t, w: w}
	if _, err := p.inf.AddEventHandler(janitorHandler{s, p}); err != nil {
		return nil
	}
	if _, err := p.inf.AddEventHandler(w.h); err != nil {
		return nil
	}
	p.add(t)
	_ = p.inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		if apierrors.IsForbidden(err) {
			n.forbiddenOnce(id, err)
		}
	})
	ctx, stop := context.WithCancel(n.parent)
	w.stop = stop
	go p.inf.RunWithContext(ctx)
	return w
}

// forbiddenOnce logs, once per name, a per-name watch the server forbade:
// the provider's resourceNames RBAC doesn't list it (§13 shape 1). Holds on
// it still work through live reads, if those are allowed; the reflector
// keeps retrying.
func (n *perNames) forbiddenOnce(id ID, err error) {
	n.mu.Lock()
	first := !n.forbidden[id]
	n.forbidden[id] = true
	n.mu.Unlock()
	if first {
		n.t.warn("credential-use: the watch of a protected object the index names is forbidden: grant list and watch on it by name (resourceNames, §13)",
			"kind", n.t.gk.String(), "object", id.String(), "error", err.Error())
	}
}

// get returns the per-name informer of id, or nil.
func (n *perNames) get(id ID) *protectedInformer {
	n.mu.Lock()
	defer n.mu.Unlock()
	if w := n.by[id]; w != nil {
		return w.p
	}
	return nil
}

// all returns every per-name informer.
func (n *perNames) all() []*protectedInformer {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]*protectedInformer, 0, len(n.by))
	for _, w := range n.by {
		out = append(out, w.p)
	}
	return out
}

// released reports whether id's per-name cache has synced and shows it
// without a finalizer of the kind, or gone: the R3 that went with a dropped
// line is visible there (§10.6.6, L5).
func (n *perNames) released(id ID) bool {
	p := n.get(id)
	if p == nil || !p.inf.HasSynced() {
		return false
	}
	m, ok := p.get(id.Namespace, id.Name)
	return !ok || countPrefix(m.Finalizers, n.t.prefix) == 0
}

// cachedID reports whether the core's caches cover id for t: a PerName
// kind's per-name watches cover every namespace, so §10.5's live paths don't
// apply to it; any other kind's, when SharedOptions.Namespaces covers id's
// namespace.
func (t *Tracker) cachedID(id ID) bool {
	return t.isPerName(id.GroupKind()) || t.s.namespaceCached(id.Namespace)
}

// allProtected returns every protected informer: the shared label-scoped
// ones and every tracker's per-name ones.
func (s *Shared) allProtected() []*protectedInformer {
	out := s.protectedInformers()
	for _, t := range s.trackerList() {
		if t.idx != nil {
			out = append(out, t.names.all()...)
		}
	}
	return out
}

// A modeClaim is the watch mode the trackers of one protected kind share,
// and how many claim it.
type modeClaim struct {
	mode   WatchMode
	claims int
}

// claimModesLocked claims o's protected kinds' watch modes: one informer
// set serves a kind for every tracker, so they must agree on its mode, as
// on its Content (§10.4). s.mu is held.
func (s *Shared) claimModesLocked(o Options) error {
	for _, pk := range o.Protected {
		gk := pk.GVK.GroupKind()
		if c := s.modes[gk]; c != nil && c.mode != pk.Watch {
			return &modeError{holder: o.Holder.GroupKind(), kind: gk, want: pk.Watch, have: c.mode}
		}
	}
	for _, pk := range o.Protected {
		gk := pk.GVK.GroupKind()
		c := s.modes[gk]
		if c == nil {
			c = &modeClaim{mode: pk.Watch}
			s.modes[gk] = c
		}
		c.claims++
	}
	return nil
}

// releaseModesLocked gives up o's claims. s.mu is held.
func (s *Shared) releaseModesLocked(o Options) {
	for _, pk := range o.Protected {
		gk := pk.GVK.GroupKind()
		if c := s.modes[gk]; c != nil {
			if c.claims--; c.claims <= 0 {
				delete(s.modes, gk)
			}
		}
	}
}

// modeError is a Track whose watch mode for a protected kind disagrees
// with another holder kind's.
type modeError struct {
	holder, kind schema.GroupKind
	want, have   WatchMode
}

func (e *modeError) Error() string {
	return "credential-use: " + e.holder.String() + ": protected kind " + e.kind.String() + " has Watch " + e.want.String() +
		", but another holder kind tracks it with Watch " + e.have.String() + ": every holder kind of a Shared must agree on a protected kind's watch mode"
}

// String names a watch mode.
func (m WatchMode) String() string {
	switch m {
	case LabelScoped:
		return "LabelScoped"
	case PerName:
		return "PerName"
	case PerNamePoll:
		return "PerNamePoll"
	}
	return "WatchMode(?)"
}
