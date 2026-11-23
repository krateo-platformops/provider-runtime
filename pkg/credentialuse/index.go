package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// The index ConfigMap (§10.6, M1). Under resourceNames RBAC a tracker can't
// list a protected kind, so a holder kind with PerName kinds keeps one
// ConfigMap in SharedOptions.IndexNamespace that names every protected object
// of those kinds it may hold, one line each, <group>/<kind>/<namespace>/<name>
// (ID.String). The index is a superset of the holds (§10.6.3): every object
// that carries a finalizer with the kind's prefix has its line. Two rules
// keep it so, Dekker on both sides (§10.6.2):
//
//   - the acquirer adds the line before A2, and live-reads the index after a
//     successful A2, adding the line again if a concurrent prune removed it;
//   - a pruner removes lines only after R3, in one write per batch, and then
//     live-reads each object it pruned, adding the line back if a concurrent
//     A2 put a finalizer of the kind on it again.
//
// Both reads are consistent, so of an A2 and a prune that race, at least one
// side sees the other. A line is pruned only when its object, read live,
// carries no finalizer of the kind, or is gone, and no holder's record names
// it (§10.6.2, revision 16: a Missing ref keeps its line while a record names
// it). Every write is a read, then an Update guarded by the ConfigMap's
// resourceVersion; a conflict reads again and re-applies this write's own
// adds or removes to what it read, so no other writer's change is lost.

const (
	// DefaultIndexMaxLines is SharedOptions.IndexMaxLines' default (§10.6.4).
	DefaultIndexMaxLines = 2000
	// IndexMaxLinesCeiling is the most SharedOptions.IndexMaxLines may be:
	// about what fits of typical size (60 bytes a line) in a ConfigMap's
	// 1 MiB (§10.6.4, A19). Setup refuses a cap above it.
	IndexMaxLinesCeiling = 17000
	// indexMaxBytes is a ConfigMap's size limit, its keys and values
	// together (core validation's MaxSecretSize, which ConfigMaps share).
	// Every write checks it, since lines longer than typical meet it before
	// the cap.
	indexMaxBytes = 1 << 20
	// indexDataKey is the data key the lines live under.
	indexDataKey = "entries"
	// indexNamePrefix starts every index's name; the namespace's admission
	// policy allows only ConfigMaps so named (§10.6.1).
	indexNamePrefix = "cu-index-"
	// indexFillWarning is the fill ratio at which the tracker records a
	// Warning event on the index (§10.6.4).
	indexFillWarning = 0.8
	// maxIndexWriteAttempts bounds one index write's read-and-update
	// attempts. An index has at most two writers at once, old and new pods
	// in a rollout (§10.6.1), so conflicts are few.
	maxIndexWriteAttempts = 5
)

// The index's events (§12, §10.6.4, §10.6.6), on the index ConfigMap.
const (
	// EventIndexFilling (Warning): the index holds at least 80% of its cap.
	EventIndexFilling = "CredentialUseIndexFilling"
	// EventIndexTampered (Warning): a line dropped by someone other than
	// this process named an object that still carries the kind's prefix; the
	// tracker added it back.
	EventIndexTampered = "CredentialUseIndexTampered"
)

// errIndexFull is an add that would take the index past its cap or its
// size: A2 doesn't run, so a new holder fails closed and an established one
// is Degraded (§10.6.4).
var errIndexFull = errors.New("credential-use: the index is full")

// configMapGVR is the ConfigMap resource.
var configMapGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

// configMapGroupKind is the ConfigMap kind.
var configMapGroupKind = schema.GroupKind{Kind: "ConfigMap"}

// IndexName returns holder kind K's index name, cu-index-<lowercase
// kind>.<group> (§10.6.1).
func IndexName(holder schema.GroupKind) string {
	return indexNamePrefix + strings.ToLower(holder.Kind) + "." + holder.Group
}

// validIndexName checks that holder kind K's index name is a valid
// ConfigMap name.
func validIndexName(holder schema.GroupKind) error {
	if errs := validation.IsDNS1123Subdomain(IndexName(holder)); len(errs) > 0 {
		return fmt.Errorf("credential-use: holder kind %s gives the invalid index name %q: %s", holder, IndexName(holder), strings.Join(errs, "; "))
	}
	return nil
}

// parseIndexLine reads one line, <group>/<kind>/<namespace>/<name>. A line
// that doesn't parse, as a hand edit may leave, names nothing: readers skip
// it and writers keep it as it is.
func parseIndexLine(line string) (ID, bool) {
	parts := strings.SplitN(line, "/", 4)
	if len(parts) != 4 || parts[1] == "" || parts[3] == "" {
		return ID{}, false
	}
	return ID{Group: parts[0], Kind: parts[1], Namespace: parts[2], Name: parts[3]}, true
}

// decodeIndex returns the lines of an index's data, sorted and without
// blanks or repeats.
func decodeIndex(data string) []string {
	var out []string
	for _, l := range strings.Split(data, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// encodeIndex is the data that holds lines, which are sorted: one line each,
// each ending in a newline, or nothing for none.
func encodeIndex(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

// readsBack reports whether id's line, written to the index, is read as that
// one line, and as id.
func readsBack(id ID) bool {
	line := id.String()
	got, ok := parseIndexLine(line)
	return ok && got == id && slices.Equal(decodeIndex(encodeIndex([]string{line})), []string{line})
}

// maxLineName is the longest name, or namespace, a line may have: that of a DNS
// subdomain, which the names of most kinds are. Without it a few refs would
// fill the ConfigMap.
const maxLineName = 253

// lineErr returns why id can't be a line of an index, or nil (§10.6).
func lineErr(id ID) error {
	if len(id.Name) > maxLineName || len(id.Namespace) > maxLineName {
		return fmt.Errorf("its name, or its namespace, is longer than %d bytes", maxLineName)
	}
	if !readsBack(id) {
		return errors.New("it would be read as another, or as more than one: correct the reference, as a line break or a blank at its end")
	}
	return nil
}

// quotedLine is id's line as an error of the index quotes it: cut, after the
// quoting, so that an event's note, of 1,024 bytes, holds it twice.
func quotedLine(id ID) string { return fmt.Sprintf("%.300s", fmt.Sprintf("%+.300q", id.String())) }

// linesOf returns ids' lines, sorted and without repeats.
func linesOf(ids []ID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// union returns the sorted lines of a and b, both sorted.
func union(a, b []string) []string {
	out := append(slices.Clone(a), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

// An indexVersion is one version of the index as a read or a write returned
// it.
type indexVersion struct {
	lines []string
	rv    string
	// rebuilt: the index carries AnnotationRebuilt (§10.6.6).
	rebuilt bool
	// vouched is AnnotationVouched's value, as the operator wrote it;
	// vouchedIndex, AnnotationVouchedIndex's. uid is the ConfigMap's.
	vouched, vouchedIndex, uid string
	// obj is the ConfigMap itself, for the next guarded Update; nil from the
	// model's store.
	obj *unstructured.Unstructured
}

// complete reports whether v names every hold: a rebuild of it completed, or
// an operator vouched for this very ConfigMap after a loss (§7.4).
func (v indexVersion) complete() bool {
	return v.rebuilt || v.uid != "" && v.vouchedIndex == v.uid
}

// indexStore is where the index lives: the ConfigMap, through the dynamic
// client, or the model's store in the tests. Every read is live.
type indexStore interface {
	// get reads the index; an error classified ClassNotFound means it
	// doesn't exist.
	get(ctx context.Context) (indexVersion, error)
	// update writes lines over cur, guarded by cur's resourceVersion, and
	// with rebuilt sets AnnotationRebuilt in the same write; without, it
	// leaves the annotation as cur had it.
	update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error)
	// create creates the index with lines, carrying AnnotationRebuilt only
	// with rebuilt.
	create(ctx context.Context, lines []string, rebuilt bool) (indexVersion, error)
}

// configMapStore is the index ConfigMap, through the dynamic client: get,
// update and create are what §10.6.7's RBAC grants.
type configMapStore struct {
	c         dynamic.ResourceInterface
	namespace string
	name      string
}

func newConfigMapStore(dyn dynamic.Interface, namespace, name string) configMapStore {
	return configMapStore{c: dyn.Resource(configMapGVR).Namespace(namespace), namespace: namespace, name: name}
}

// versionOf reads an index ConfigMap.
func versionOf(u *unstructured.Unstructured) indexVersion {
	data, _, _ := unstructured.NestedString(u.Object, "data", indexDataKey)
	ann := u.GetAnnotations()
	return indexVersion{lines: decodeIndex(data), rv: u.GetResourceVersion(), obj: u, uid: string(u.GetUID()),
		rebuilt: ann[AnnotationRebuilt] == "true", vouched: ann[AnnotationVouched], vouchedIndex: ann[AnnotationVouchedIndex]}
}

func (s configMapStore) get(ctx context.Context) (indexVersion, error) {
	u, err := s.c.Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return indexVersion{}, err
	}
	return versionOf(u), nil
}

func (s configMapStore) update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	if cur.obj == nil {
		return indexVersion{}, errors.New("credential-use: an index update needs the version it read")
	}
	u := cur.obj.DeepCopy()
	if err := unstructured.SetNestedField(u.Object, encodeIndex(lines), "data", indexDataKey); err != nil {
		return indexVersion{}, err
	}
	if rebuilt {
		ann := u.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann[AnnotationRebuilt] = "true"
		u.SetAnnotations(ann)
	}
	out, err := s.c.Update(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return indexVersion{}, err
	}
	return versionOf(out), nil
}

func (s configMapStore) create(ctx context.Context, lines []string, rebuilt bool) (indexVersion, error) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": s.name, "namespace": s.namespace},
		"data":       map[string]any{indexDataKey: encodeIndex(lines)},
	}}
	if rebuilt {
		u.SetAnnotations(map[string]string{AnnotationRebuilt: "true"})
	}
	out, err := s.c.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return indexVersion{}, err
	}
	return versionOf(out), nil
}

// indexSnapshot is the index as this process last saw it: the informer's
// latest version, or this process's latest write.
type indexSnapshot struct {
	lines   map[string]bool
	vouched map[ID]bool
	rebuilt bool
}

// index is one holder kind's index (§10.6), for its PerName kinds. Its
// writes and live reads are the protocol's; its view, from a name-selected
// informer, serves only fast paths, which a stale view makes write when
// they could have skipped (§6.4).
type index struct {
	holder    schema.GroupKind
	prefix    string // the kind's finalizer prefix
	namespace string
	name      string
	maxLines  int
	// kinds are the holder kind's PerName protected kinds: only their refs
	// have lines.
	kinds map[schema.GroupKind]bool
	store indexStore
	// srv reads protected objects and holders live.
	srv server

	// cached, if set, replaces the snapshot as the acquirer's view of
	// whether a line is present: the model reads live, as a step of its
	// schedule.
	cached func(ctx context.Context, line string) bool
	// recorded reports whether some holder's record names id, from the
	// holder cache: a line for an object without the kind's finalizer is
	// kept while one does (§10.6.2). Nil is never.
	recorded func(id ID) bool
	// disowning reads live whether the Disown switch covers a PerName kind:
	// the records may have been stripped, so a rebuild from them can't tell
	// that it found every hold (§7.4). Nil is never.
	disowning func(ctx context.Context) (bool, error)
	// keptLive reads live, and keptCached tells from the CRD's cache, the
	// kinds whose lines stay: those the Disown switch covers while it hasn't
	// settled (§7.4). Nil is none.
	keptLive   func(ctx context.Context) (disownSet, error)
	keptCached func(gk schema.GroupKind) bool
	// onLines sees every version of the index this process reads or
	// writes, with its lines: the fill gauge and event (§10.6.4).
	onLines func(lines []string)
	// onTampered reports a dropped line restored because its object still
	// carried the kind's prefix (§10.6.6).
	onTampered func(id ID)
	// onPolicyConfig reports a write the namespace's admission policy
	// couldn't be configured for (§10.6.1, class (c)).
	onPolicyConfig func(err error)
	// onUnreadable reports the recorded objects a rebuild couldn't read, and
	// kept the lines of.
	onUnreadable func(ids []ID)
	// onDenied reports, once per object, a read the API server refused.
	onDenied func(id ID, err error)
	// onRestoredUnread reports a dropped line restored for an object this
	// process can't read: not tampering, since whether it carries the
	// kind's prefix is unknown.
	onRestoredUnread func(id ID)
	// onVouchMalformed reports the malformed entries of the vouched
	// annotation; onVouchWrong, a vouched object that carries the prefix.
	onVouchMalformed func(entries []string)
	onVouchWrong     func(id ID)

	snap atomic.Pointer[indexSnapshot]

	// The informer on the ConfigMap, selected by name (§10.6.6); nil until
	// the tracker builds it.
	inf cache.SharedIndexInformer

	mu sync.Mutex
	// own are this process's latest removals whose drop its informer hasn't
	// delivered, at most maxOwnRemovals: a drop is this process's own only
	// when the version that shows it is one a successful write of it
	// returned (§10.6.6).
	own []ownRemoval
	// pending are the objects to check live, and re-add the line of if
	// they carry the kind's prefix: lines someone else dropped, and re-adds
	// that failed. tampered marks those that came from a drop.
	pending map[ID]*recheck
	// lost: the index was seen deleted, or recreated by an add; the write
	// loops rebuild it from the records (§10.6.6). lostDue is when the next
	// try is due, lostFails how many failed in a row.
	lost      bool
	lostDue   time.Time
	lostFails int
	// fresh: the kind was never tracked before this process's Track, and
	// had no index, so that a missing one, until this process has rebuilt
	// it, isn't a lost one (§7.4).
	fresh bool
	// deniedSeen are the objects whose refused read was reported.
	deniedSeen map[ID]bool
	// vouchWarned are the vouched annotation's values whose malformed
	// entries were reported; vouchWrongSeen, the vouched objects found
	// carrying the prefix.
	vouchWarned    map[string]bool
	vouchWrongSeen map[ID]bool
}

// A recheck is one pending live check of an object whose line may be
// missing.
type recheck struct {
	tampered bool
	due      time.Time
	fails    int
	// queued counts how often it was queued: a check deletes it only if it
	// wasn't queued again meanwhile, since what it read may be older.
	queued int
	// drop, if set, is the version whose drop alone queued it: should this
	// process's own write turn out to have made that version, it goes.
	drop string
}

// An ownRemoval is one successful removal by this process: the version it
// returned, and the lines it removed.
type ownRemoval struct {
	rv    string
	lines []string
}

const (
	// maxOwnRemovals bounds index.own. A removal it forgets costs a live
	// check of each of its lines.
	maxOwnRemovals = 64
	// pruneBound bounds a prune, its check after it and its re-add included,
	// once it has begun, whatever its context does: below the 30s a manager
	// gives its runnables to stop by default, so that a graceful stop
	// doesn't come between a removal and its re-add.
	pruneBound = 15 * time.Second
)

func newIndex(holder schema.GroupKind, prefix, namespace string, maxLines int, kinds map[schema.GroupKind]bool, store indexStore, srv server) *index {
	return &index{
		holder: holder, prefix: prefix, namespace: namespace, name: IndexName(holder), maxLines: maxLines,
		kinds: kinds, store: store, srv: srv,
		pending: map[ID]*recheck{},
	}
}

// ref names the index in messages: <namespace>/<name>.
func (ix *index) ref() string { return ix.namespace + "/" + ix.name }

// covers reports whether refs of kind gk have lines.
func (ix *index) covers(gk schema.GroupKind) bool { return ix != nil && ix.kinds[gk] }

// saw keeps v as this process's view, and reports its lines.
func (ix *index) saw(v indexVersion) {
	s := &indexSnapshot{lines: make(map[string]bool, len(v.lines)), vouched: ix.parseVouched(v.vouched), rebuilt: v.rebuilt}
	for _, l := range v.lines {
		s.lines[l] = true
	}
	ix.snap.Store(s)
	if ix.onLines != nil {
		ix.onLines(v.lines)
	}
}

// has reports whether the view shows line. Without a view it reports false,
// so the caller reads live.
func (ix *index) has(ctx context.Context, line string) bool {
	if ix.cached != nil {
		return ix.cached(ctx, line)
	}
	s := ix.snap.Load()
	return s != nil && s.lines[line]
}

// fits checks that next, which adds lines to an index of before lines,
// stays within the cap and the size (§10.6.4). An index already past a cap
// since lowered fails only for an add.
func (ix *index) fits(next []string, before int) error {
	if len(next) > before && len(next) > ix.maxLines {
		return fmt.Errorf("%w: %s holds %d lines, and its cap is %d (SharedOptions.IndexMaxLines): move the kind to LabelScoped, or raise the cap within %d (§10.6.4)",
			errIndexFull, ix.ref(), before, ix.maxLines, IndexMaxLinesCeiling)
	}
	if size := len(indexDataKey) + len(encodeIndex(next)); size > indexMaxBytes {
		return fmt.Errorf("%w: %s would hold %d bytes, past a ConfigMap's %d: move the kind to LabelScoped (§10.6.4)",
			errIndexFull, ix.ref(), size, indexMaxBytes)
	}
	return nil
}

// writeErr wraps an index write's error. A write the namespace's admission
// policy couldn't be configured for, a class (c) 422 that reads like a
// Deleting outcome or missing RBAC, is matched by its text and named
// (§10.6.1).
func (ix *index) writeErr(what string, err error) error {
	if policyParamsMissing(err) {
		if ix.onPolicyConfig != nil {
			ix.onPolicyConfig(err)
		}
		return fmt.Errorf("credential-use: %s the index %s: the namespace's admission policy has no parameters: check that its parameter ConfigMap (cu-policy-params) exists: %w", what, ix.ref(), err)
	}
	return fmt.Errorf("credential-use: %s the index %s: %w", what, ix.ref(), err)
}

// policyName is the index namespace's ValidatingAdmissionPolicy (§10.6.1).
const policyName = "krateo-credential-use-namespace"

// policyParamsMissing reports whether err is the index namespace's policy
// failing for want of its parameter ConfigMap: matched by its text, never
// by its code, since it is a 422 Invalid like a Deleting outcome (§10.6.1).
func policyParamsMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ValidatingAdmissionPolicy '"+policyName+"'") &&
		strings.Contains(msg, "failed to configure binding: no params found")
}

// add adds ids' lines to the index, merging with what it reads: it
// re-reads and re-applies them on a conflict. An index that is gone is
// created with them, without AnnotationRebuilt, and marked for the rebuild
// from the records (§10.6.6). It fails with errIndexFull past the cap or the
// size. It adds no line that can't be one (lineErr): nothing of such a name is
// held (ensure).
func (ix *index) add(ctx context.Context, ids []ID) error { return ix.merge(ctx, ids, false) }

// merge is add; with rebuilt, it is a rebuild's last merge, and sets
// AnnotationRebuilt in the same guarded write (§10.6.6), writing even when
// no line is new.
func (ix *index) merge(ctx context.Context, ids []ID, rebuilt bool) error {
	lines := linesOf(slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return lineErr(id) != nil }))
	var last error
	for range maxIndexWriteAttempts {
		cur, err := ix.store.get(ctx)
		switch Classify(err) {
		case ClassNone:
		case ClassNotFound:
			if err := ix.fits(lines, 0); err != nil {
				return err
			}
			v, err := ix.store.create(ctx, lines, rebuilt)
			if Classify(err) == ClassAlreadyExists {
				last = err
				continue
			}
			if err != nil {
				return ix.writeErr("creating", err)
			}
			ix.saw(v)
			if !rebuilt {
				ix.markLost()
			}
			return nil
		default:
			return fmt.Errorf("credential-use: reading the index %s: %w", ix.ref(), err)
		}
		next := union(cur.lines, lines)
		if len(next) == len(cur.lines) && (!rebuilt || cur.rebuilt) {
			ix.saw(cur)
			return nil
		}
		if err := ix.fits(next, len(cur.lines)); err != nil {
			ix.saw(cur)
			return err
		}
		v, err := ix.store.update(ctx, cur, next, rebuilt)
		if Classify(err) == ClassConflict {
			last = err
			continue
		}
		if err != nil {
			return ix.writeErr("adding to", err)
		}
		ix.saw(v)
		return nil
	}
	return fmt.Errorf("credential-use: %d conflicting writes of the index %s in a row: %w", maxIndexWriteAttempts, ix.ref(), last)
}

// remove removes lines from the index, merging with what it reads, and
// returns every line one of its attempts tried to remove, whatever came of
// it: an attempt that conflicted, because someone else removed the line
// first, and one whose outcome is unknown, leave a line out as well as one
// that succeeded. A missing index has nothing to remove.
func (ix *index) remove(ctx context.Context, lines []string) (tried []string, err error) {
	var last error
	for range maxIndexWriteAttempts {
		cur, err := ix.store.get(ctx)
		switch Classify(err) {
		case ClassNone:
		case ClassNotFound:
			return tried, nil
		default:
			return tried, fmt.Errorf("credential-use: reading the index %s: %w", ix.ref(), err)
		}
		var present []string
		next := slices.DeleteFunc(slices.Clone(cur.lines), func(l string) bool {
			if slices.Contains(lines, l) {
				present = append(present, l)
				return true
			}
			return false
		})
		if len(present) == 0 {
			ix.saw(cur)
			return tried, nil
		}
		tried = union(tried, present)
		v, err := ix.store.update(ctx, cur, next, false)
		if Classify(err) == ClassConflict {
			last = err
			continue
		}
		if err != nil {
			return tried, ix.writeErr("removing from", err)
		}
		ix.saw(v)
		ix.removed(v.rv, present)
		return tried, nil
	}
	return tried, fmt.Errorf("credential-use: %d conflicting writes of the index %s in a row: %w", maxIndexWriteAttempts, ix.ref(), last)
}

// removed records that this process's removal of lines made version rv. The
// informer may have delivered it before the write returned, and queued its
// drops: those go, unless something else queued them too.
func (ix *index) removed(rv string, lines []string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for _, l := range lines {
		if id, ok := parseIndexLine(l); ok {
			if r := ix.pending[id]; r != nil && rv != "" && r.drop == rv {
				delete(ix.pending, id)
			}
		}
	}
	ix.own = append(ix.own, ownRemoval{rv: rv, lines: lines})
	if n := len(ix.own) - maxOwnRemovals; n > 0 {
		ix.own = slices.Delete(ix.own, 0, n)
	}
}

// prune removes ids' lines in one write, then reads every object whose line
// an attempt tried to remove live, and adds back the line of any that
// carries the kind's prefix again, or can't be read (§10.6.2): an A2 that
// raced the prune, or a line someone else removed while this write
// conflicted. A re-add that fails is queued for the write loops, which retry
// it until it lands, and the error is returned.
//
// Once begun it runs to its end, within pruneBound, whatever ctx does: the
// queue dies with the process, so a stop between the removal and the re-add
// would lose the line.
//
// While Disown covers a kind and hasn't settled, its lines stay: a hold that
// lands late has only its line for Disown's count to find it by (§7.4). The
// switch is read again, live, after the removal, which the cache may lag; one
// read as off then was set after the removal, so that Disown's first pass
// comes after it and adds the lines of every record (§7.4, E2).
func (ix *index) prune(ctx context.Context, ids []ID) error {
	if ix.keptCached != nil {
		ids = slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return ix.keptCached(id.GroupKind()) })
	}
	return ix.removeAndCheck(ctx, ids, true)
}

// empty is prune for Disown's own emptying, once it has settled: the switch
// is on, and doesn't keep the lines.
func (ix *index) empty(ctx context.Context, ids []ID) error {
	return ix.removeAndCheck(ctx, ids, false)
}

func (ix *index) removeAndCheck(ctx context.Context, ids []ID, disown bool) error {
	if len(ids) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pruneBound)
	defer cancel()
	tried, err := ix.remove(ctx, linesOf(ids))
	if len(tried) == 0 {
		return err
	}
	kept := func(schema.GroupKind) bool { return false }
	if disown && ix.keptLive != nil {
		set, kerr := ix.keptLive(ctx)
		if kept = set.covers; kerr != nil {
			// Whether Disown keeps them is unknown: they go back.
			kept = func(schema.GroupKind) bool { return true }
			err = errors.Join(err, fmt.Errorf("credential-use: reading the Disown switch after pruning the index %s: %w", ix.ref(), kerr))
		}
	}
	return errors.Join(err, ix.checkRemoved(ctx, tried, kept))
}

// checkRemoved is the check after a removal of lines tried: it adds back the
// lines that must stay, those of the kinds kept among them.
func (ix *index) checkRemoved(ctx context.Context, tried []string, kept func(schema.GroupKind) bool) error {
	var back, carry, unread []ID
	for _, l := range tried {
		id, ok := parseIndexLine(l)
		if !ok {
			continue
		}
		if kept(id.GroupKind()) {
			back = append(back, id)
			continue
		}
		switch ix.carries(ctx, id) {
		case carriesNot:
		case carriesYes:
			carry = append(carry, id)
		default:
			unread = append(unread, id)
		}
	}
	if len(back)+len(carry)+len(unread) == 0 {
		return nil
	}
	back = append(back, carry...)
	// What can't be read may carry the prefix, unless an operator vouched
	// for it (§10.6.6): read live, since the vouch may have been withdrawn.
	// An index that can't be read vouches for nothing.
	var vouched map[ID]bool
	if cur, err := ix.store.get(ctx); err == nil {
		vouched = ix.parseVouched(cur.vouched)
	}
	for _, id := range carry {
		if vouched[id] {
			ix.vouchWrong(id)
		}
	}
	for _, id := range unread {
		if !vouched[id] {
			back = append(back, id)
		}
	}
	if len(back) == 0 {
		return nil
	}
	if err := ix.add(ctx, back); err != nil {
		ix.queue(back, false)
		return err
	}
	return nil
}

// What a live read found of an object's holds.
type carriage int

const (
	carriesNot carriage = iota // gone, or no finalizer of the kind
	carriesYes
	carriesUnknown // the read failed, and may succeed if tried again
	// carriesRBACDenied: the authorizer refused the read, class (d) of §6.2,
	// as for an object outside a resourceNames grant; trying again can't
	// help until RBAC changes. Any other 403 is carriesUnknown.
	carriesRBACDenied
)

// carries reads id live, and reports whether it carries a finalizer with the
// kind's prefix. An RBAC 403, recognised by the authorizer's message as §6.2
// does (class (d)), is reported once per object (denied); an admission or
// policy 403 is an ordinary failure, retried.
func (ix *index) carries(ctx context.Context, id ID) carriage {
	m, err := ix.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
	switch Classify(err) {
	case ClassNone:
		ix.readable(id)
		if countPrefix(m.Finalizers, ix.prefix) > 0 {
			return carriesYes
		}
		return carriesNot
	case ClassNotFound:
		ix.readable(id)
		return carriesNot
	}
	if Classify(err) == ClassRBAC {
		ix.denied(id, err)
		return carriesRBACDenied
	}
	return carriesUnknown
}

// parseVouched reads AnnotationVouched: comma-separated refs written
// <namespace>/<Kind[.group]>/<name>, blanks trimmed; a malformed entry is
// ignored, and reported once per value of the annotation.
func (ix *index) parseVouched(v string) map[ID]bool {
	out := map[ID]bool{}
	var bad []string
	for _, e := range strings.Split(v, ",") {
		if e = strings.TrimSpace(e); e == "" {
			continue
		}
		id, ok := parseUnheldRef(e)
		if !ok {
			bad = append(bad, e)
			continue
		}
		out[id] = true
	}
	if len(bad) > 0 {
		ix.mu.Lock()
		first := !ix.vouchWarned[v]
		if first {
			if ix.vouchWarned == nil {
				ix.vouchWarned = map[string]bool{}
			}
			ix.vouchWarned[v] = true
		}
		ix.mu.Unlock()
		if first && ix.onVouchMalformed != nil {
			ix.onVouchMalformed(bad)
		}
	}
	return out
}

// parseUnheldRef reads a ref as the unheld annotation writes it,
// <namespace>/<Kind[.group]>/<name> (unheldRef).
func parseUnheldRef(s string) (ID, bool) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" || strings.Contains(parts[2], "/") {
		return ID{}, false
	}
	kind, group, _ := strings.Cut(parts[1], ".")
	if kind == "" {
		return ID{}, false
	}
	return ID{Group: group, Kind: kind, Namespace: parts[0], Name: parts[2]}, true
}

// vouchedNow is the vouched objects of the index as this process last saw
// it.
func (ix *index) vouchedNow() map[ID]bool {
	if s := ix.snap.Load(); s != nil {
		return s.vouched
	}
	return nil
}

// vouchWrong reports, once per object, a vouched object found carrying the
// kind's prefix: the vouch is ignored for it.
func (ix *index) vouchWrong(id ID) {
	ix.mu.Lock()
	first := !ix.vouchWrongSeen[id]
	if first {
		if ix.vouchWrongSeen == nil {
			ix.vouchWrongSeen = map[ID]bool{}
		}
		ix.vouchWrongSeen[id] = true
	}
	ix.mu.Unlock()
	if first && ix.onVouchWrong != nil {
		ix.onVouchWrong(id)
	}
}

// readable forgets that a read of id was refused, once one has been
// answered: a grant revoked again is then reported again.
func (ix *index) readable(id ID) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	delete(ix.deniedSeen, id)
}

// denied reports, once per object, a read of id the API server refused: its
// line is kept, since the object may carry the kind's finalizer (§10.6.3),
// and nothing retries the read, which can't succeed until RBAC changes.
func (ix *index) denied(id ID, err error) {
	ix.mu.Lock()
	first := !ix.deniedSeen[id]
	if first {
		if ix.deniedSeen == nil {
			ix.deniedSeen = map[ID]bool{}
		}
		ix.deniedSeen[id] = true
	}
	ix.mu.Unlock()
	if first && ix.onDenied != nil {
		ix.onDenied(id, err)
	}
}

// ensure is the acquirer's step before A2 (§10.6.2): it adds id's line
// unless the view shows it. A stale view that shows a line a prune has
// removed is caught by postCheck.
func (ix *index) ensure(ctx context.Context, id ID) error {
	if err := lineErr(id); err != nil {
		return fmt.Errorf("credential-use: %s can't be a line of the index %s: %w (§10.6)", quotedLine(id), ix.ref(), err)
	}
	if ix.has(ctx, id.String()) {
		return nil
	}
	return ix.add(ctx, []ID{id})
}

// postCheck is the acquirer's step after a successful A2 (§10.6.2): it reads
// the index live, and adds id's line again if a concurrent prune removed it.
// When it can't tell, or the add fails, as on a full index, P may carry the
// finalizer without its line: id is queued for the write loops, which add
// the line once a live read shows P carrying the kind's prefix and the add
// lands, retrying with backoff (§10.6.2).
func (ix *index) postCheck(ctx context.Context, id ID) error {
	cur, err := ix.store.get(ctx)
	switch Classify(err) {
	case ClassNone:
		if slices.Contains(cur.lines, id.String()) {
			ix.saw(cur)
			return nil
		}
	case ClassNotFound:
	default:
		ix.queue([]ID{id}, false)
		return fmt.Errorf("credential-use: reading the index %s after holding %s: %w", ix.ref(), id, err)
	}
	if err := ix.add(ctx, []ID{id}); err != nil {
		ix.queue([]ID{id}, false)
		return err
	}
	return nil
}

// prunable returns those of ids whose lines may go: of a PerName kind, and
// named by no holder's record (§10.6.2). The caller has read each, live,
// without a finalizer of the kind, or gone.
func (ix *index) prunable(ids []ID) []ID {
	var out []ID
	for _, id := range ids {
		if !ix.kinds[id.GroupKind()] {
			continue
		}
		if ix.recorded != nil && ix.recorded(id) {
			continue
		}
		out = append(out, id)
	}
	return out
}

// queue adds ids to the live checks the write loops make: tampered marks
// lines someone else dropped.
func (ix *index) queue(ids []ID, tampered bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.queueLocked(ids, tampered, "")
}

// queueLocked is queue, for a drop that version drop showed, if set. ix.mu
// is held.
func (ix *index) queueLocked(ids []ID, tampered bool, drop string) {
	for _, id := range ids {
		if r := ix.pending[id]; r != nil {
			r.tampered = r.tampered || tampered
			r.queued++
			r.drop = ""
			continue
		}
		ix.pending[id] = &recheck{tampered: tampered, drop: drop}
	}
}

// markLost asks the write loops to rebuild the index from the records.
func (ix *index) markLost() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.lost {
		ix.lost, ix.lostDue, ix.lostFails = true, time.Time{}, 0
	}
}

// observed is the informer's handler's work on a new version of the index
// (§10.6.6). old is the version before it, or nil for the first. Every line
// old had that cur hasn't was dropped. A drop is this process's own only if
// cur is the version a successful removal of it returned; every other is
// queued for a live check, which costs a GET at most. One delivered before
// this process's write returned is queued too, until the write returns with
// that version (removed).
func (ix *index) observed(old *indexVersion, cur indexVersion) {
	ix.saw(cur)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var mine []string
	if i := slices.IndexFunc(ix.own, func(o ownRemoval) bool { return o.rv != "" && o.rv == cur.rv }); i >= 0 {
		mine = ix.own[i].lines
		ix.own = slices.Delete(ix.own, i, i+1)
	}
	if old == nil {
		return
	}
	var check []ID
	for _, l := range old.lines {
		if slices.Contains(cur.lines, l) || slices.Contains(mine, l) {
			continue
		}
		if id, ok := parseIndexLine(l); ok && ix.kinds[id.GroupKind()] {
			check = append(check, id)
		}
	}
	ix.queueLocked(check, true, cur.rv)
}

// ids returns the IDs of v's lines of the PerName kinds.
func (ix *index) ids(lines []string) []ID {
	var out []ID
	for _, l := range lines {
		if id, ok := parseIndexLine(l); ok && ix.kinds[id.GroupKind()] {
			out = append(out, id)
		}
	}
	return out
}

// rebuild recreates the index if it is gone, and merges into it the line of
// every object a holder's record names that carries the kind's prefix: it
// lists the holders live, takes the union of their records, and reads each
// object (§10.6.6). The merge never loses another writer's add. An object
// whose last record was gone before its R3 landed is in no record, so the
// rebuild can't find it: its marker label still does (§10.6.6, the
// residual).
//
// An object it can't read, as one outside a resourceNames grant, or whose
// grant was revoked after its hold, may carry the prefix: it keeps its line,
// since the index is a superset (§10.6.3), and the rebuild still completes
// and sets the marker. Each is logged once.
func (ix *index) rebuild(ctx context.Context) error {
	holders, err := ix.srv.List(ctx, ix.holder, "")
	if err != nil {
		return fmt.Errorf("credential-use: listing the holders of %s to rebuild the index %s: %w", ix.holder, ix.ref(), err)
	}
	named := map[ID]bool{}
	for i := range holders {
		r := ReadRecord(&holders[i])
		if r.Unparseable {
			continue
		}
		for _, id := range r.IDs {
			if ix.kinds[id.GroupKind()] {
				named[id] = true
			}
		}
	}
	var vouched map[ID]bool
	if cur, err := ix.store.get(ctx); err == nil {
		vouched = ix.parseVouched(cur.vouched)
	}
	var keep, unread []ID
	for _, id := range SortedIDs(keysOf(named)) {
		switch ix.carries(ctx, id) {
		case carriesYes:
			if vouched[id] {
				ix.vouchWrong(id)
			}
			keep = append(keep, id)
		case carriesUnknown, carriesRBACDenied:
			if vouched[id] {
				continue // vouched by an operator while it can't be read
			}
			keep = append(keep, id)
			unread = append(unread, id)
		}
	}
	if len(unread) > 0 && ix.onUnreadable != nil {
		ix.onUnreadable(unread)
	}
	// The last merge, which sets the marker in the same write, unless
	// Disown may have stripped the records it was rebuilt from: read after
	// the list, a switch that is off had stripped none of them.
	mark := true
	if ix.disowning != nil && !ix.isFresh() {
		on, err := ix.disowning(ctx)
		if err != nil {
			return fmt.Errorf("credential-use: reading the Disown switch to rebuild the index %s: %w", ix.ref(), err)
		}
		mark = !on
	}
	if err := ix.merge(ctx, keep, mark); err != nil {
		return err
	}
	if mark {
		ix.setFresh(false)
	}
	return nil
}

func (ix *index) isFresh() bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.fresh
}

func (ix *index) setFresh(fresh bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.fresh = fresh
}

// unsure reports whether a line of one of kinds may be missing: one whose
// live check, or re-add, is pending, or any, until a rebuild that is due.
func (ix *index) unsure(kinds map[schema.GroupKind]bool) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.lost {
		return true
	}
	for id := range ix.pending {
		if kinds[id.GroupKind()] {
			return true
		}
	}
	return false
}

// switchChanged asks for the rebuild of an index without the marker, which a
// rebuild under the switch leaves it without.
func (ix *index) switchChanged() {
	if s := ix.snap.Load(); s != nil && !s.rebuilt {
		ix.markLost()
	}
}

// needsRebuild reports whether v, a version of the index read live, lacks
// the marker of a completed rebuild (§10.6.6): Track and the startup recheck
// rebuild it then.
func (ix *index) needsRebuild(v indexVersion) bool { return !v.rebuilt }

// lostTick runs the rebuild if one is due, and reports whether one ran
// and failed.
func (ix *index) lostTick(ctx context.Context, now time.Time) error {
	ix.mu.Lock()
	due := ix.lost && !now.Before(ix.lostDue)
	if due {
		ix.lost = false
	}
	ix.mu.Unlock()
	if !due {
		return nil
	}
	err := ix.rebuild(ctx)
	if err == nil {
		ix.mu.Lock()
		ix.lostFails = 0
		ix.mu.Unlock()
		return nil
	}
	ix.mu.Lock()
	ix.lost = true
	ix.lostDue = now.Add(backoffAfter(ix.lostFails))
	ix.lostFails++
	ix.mu.Unlock()
	return err
}

// backoffAfter is the write loops' backoff after n failures in a row: from
// releaseBackoffBase, doubling, to releaseBackoffMax.
func backoffAfter(n int) time.Duration {
	return min(releaseBackoffBase<<min(n, 9), releaseBackoffMax)
}

// recheckTick makes the live checks that are due: each object that carries
// the kind's prefix gets its line back, in one write, and a dropped line so
// restored is reported as tampering (§10.6.6). A check that fails is retried
// with backoff. An object queued again while its check runs is checked
// again: what this check read may be older.
func (ix *index) recheckTick(ctx context.Context, now time.Time) error {
	ix.mu.Lock()
	var due []ID
	queued := map[ID]int{}
	for id, r := range ix.pending {
		if !now.Before(r.due) {
			due = append(due, id)
			queued[id] = r.queued
		}
	}
	ix.mu.Unlock()
	if len(due) == 0 {
		return nil
	}
	due = SortedIDs(due)
	vouched := ix.vouchedNow()
	// The kinds whose lines Disown keeps, read live once, if a bare object's
	// line was dropped.
	var kept *disownSet
	keeps := func(gk schema.GroupKind) (bool, error) {
		if ix.keptLive == nil {
			return false, nil
		}
		if kept == nil {
			set, err := ix.keptLive(ctx)
			if err != nil {
				return false, err
			}
			kept = &set
		}
		return kept.covers(gk), nil
	}
	var back, failed, done, unread, bare []ID
	for _, id := range due {
		c := ix.carries(ctx, id)
		if vouched[id] && c != carriesYes && c != carriesNot {
			// An operator vouched that it carries no finalizer of the
			// kind: its dropped line stays dropped while it can't be read
			// (§10.6.6).
			done = append(done, id)
			continue
		}
		switch c {
		case carriesYes:
			if vouched[id] {
				ix.vouchWrong(id)
			}
			back = append(back, id)
		case carriesRBACDenied:
			// It may carry the prefix, and no retry can tell before RBAC
			// changes: its line goes back, as for an object that does
			// (§10.6.3), and the refusal is reported once. Whether it
			// carries the prefix is unknown, so it isn't tampering.
			back = append(back, id)
			unread = append(unread, id)
		case carriesNot:
			switch keep, err := keeps(id.GroupKind()); {
			case err != nil:
				failed = append(failed, id)
			case keep:
				// Disown hasn't settled: a hold may still land on it, and
				// only its line would show it (§7.4).
				back = append(back, id)
				bare = append(bare, id)
			default:
				done = append(done, id)
			}
		default:
			failed = append(failed, id)
		}
	}
	var err error
	if len(back) > 0 {
		if err = ix.add(ctx, back); err != nil {
			failed = append(failed, back...)
			back = nil
		}
	}
	ix.mu.Lock()
	var tampered, restoredUnread []ID
	for _, id := range append(done, back...) {
		if r := ix.pending[id]; r != nil {
			switch {
			case slices.Contains(bare, id): // nothing is held
			case r.tampered && slices.Contains(unread, id) && slices.Contains(back, id):
				restoredUnread = append(restoredUnread, id)
			case r.tampered && slices.Contains(back, id):
				tampered = append(tampered, id)
			}
			if r.queued != queued[id] {
				r.due, r.fails = time.Time{}, 0
				continue
			}
			delete(ix.pending, id)
		}
	}
	for _, id := range failed {
		if r := ix.pending[id]; r != nil {
			r.due = now.Add(backoffAfter(r.fails))
			r.fails++
		}
	}
	ix.mu.Unlock()
	if ix.onTampered != nil {
		for _, id := range tampered {
			ix.onTampered(id)
		}
	}
	if ix.onRestoredUnread != nil {
		for _, id := range restoredUnread {
			ix.onRestoredUnread(id)
		}
	}
	if err == nil && len(failed) > 0 {
		err = fmt.Errorf("credential-use: %d live checks of objects dropped from the index %s failed; they are retried", len(failed), ix.ref())
	}
	return err
}

// --- the index's informer (§10.6.6) ---

// indexHandler hands each version of the index the informer delivers to
// its tracker.
type indexHandler struct{ t *Tracker }

func (h indexHandler) OnAdd(obj any, _ bool) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		h.t.indexChanged(nil, versionOf(u))
	}
}

func (h indexHandler) OnUpdate(oldObj, newObj any) {
	o, ok1 := oldObj.(*unstructured.Unstructured)
	n, ok2 := newObj.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		return
	}
	old := versionOf(o)
	if o.GetUID() != n.GetUID() {
		// Deleted and recreated between two deliveries: the deletion lost
		// whatever the new one lacks.
		h.t.idx.markLost()
	}
	h.t.indexChanged(&old, versionOf(n))
}

func (h indexHandler) OnDelete(any) {
	// A deleted index is recreated and rebuilt from the records, by the
	// write loops (§10.6.6). Its per-name watches stay until the rebuilt
	// index says otherwise.
	h.t.idx.markLost()
}

// newIndexInformer builds the informer on the index ConfigMap, selected by
// name (§10.6.6, §10.6.7: watch by resourceNames needs the field selector).
func newIndexInformer(t *Tracker) (cache.SharedIndexInformer, error) {
	s := t.s
	lw := newContentListWatch(s.c.Dynamic, configMapGVR, t.idx.namespace, "", s.clock)
	lw.field = "metadata.name=" + t.idx.name
	inf := cache.NewSharedIndexInformerWithOptions(lw, &unstructured.Unstructured{}, cache.SharedIndexInformerOptions{
		ObjectDescription: "credential-use index " + t.idx.ref(),
	})
	if _, err := inf.AddEventHandler(indexHandler{t}); err != nil {
		return nil, err
	}
	return inf, nil
}

// indexInformer is t's informer on its index, for the RBAC check before it
// starts: list and watch on that one ConfigMap.
func (t *Tracker) indexInformer() informer {
	return informer{inf: t.idx.inf, gvr: configMapGVR, ns: t.idx.namespace, name: t.idx.name, what: "the index informer of " + t.gk.String()}
}

// indexChanged is a new version of t's index: the index's checks, then the
// per-name watches follow its lines (§13 shape 1).
func (t *Tracker) indexChanged(old *indexVersion, cur indexVersion) {
	t.idx.observed(old, cur)
	t.names.follow(t.idx.ids(cur.lines))
}

// --- setup (§7.3, §10.6.1) ---

// setupIndex runs Track's index checks and writes (§7.3, §10.6.1): the index
// namespace exists and holds no holders of the kind; this process may read,
// write and create the index; the CRD's pointer is written once, or agrees;
// and the index is created if absent, then rebuilt from the records.
func (t *Tracker) setupIndex(ctx context.Context) error {
	s, ix := t.s, t.idx
	_, err := s.c.Metadata.Resource(namespaceGVR).Get(ctx, ix.namespace, metav1.GetOptions{})
	switch Classify(err) {
	case ClassNone:
	case ClassNotFound:
		return fmt.Errorf("credential-use: index namespace %s not found: install it (installer release) before enabling PerName", ix.namespace)
	default:
		return fmt.Errorf("credential-use: %s: reading the index namespace %s: %w", t.gk, ix.namespace, err)
	}
	if err := t.noHoldersIn(ctx, ix.namespace); err != nil {
		return err
	}
	if err := s.canAccessNamed(ctx, configMapGVR, ix.namespace, ix.name, "get", "update", "list", "watch"); err != nil {
		return fmt.Errorf("credential-use: %s: grant get, update, list and watch on the configmap %s, and create on configmaps in namespace %s (§10.6.7): %w", t.gk, ix.ref(), ix.namespace, err)
	}
	if err := s.canAccess(ctx, configMapGVR, ix.namespace, "create"); err != nil {
		return fmt.Errorf("credential-use: %s: grant create on configmaps in namespace %s, for the index %s (§10.6.7): %w", t.gk, ix.namespace, ix.ref(), err)
	}
	if err := t.pointIndex(ctx); err != nil {
		return err
	}
	cur, err := ix.store.get(ctx)
	switch Classify(err) {
	case ClassNone:
		ix.setFresh(false)
	case ClassNotFound:
		// Created without the marker: only the rebuild below sets it.
		switch _, err := ix.store.create(ctx, nil, false); {
		case Classify(err) == ClassAlreadyExists:
			ix.setFresh(false)
		case err != nil:
			return ix.writeErr("creating", err)
		}
		cur = indexVersion{}
	default:
		return fmt.Errorf("credential-use: %s: reading the index %s: %w", t.gk, ix.ref(), err)
	}
	if !ix.needsRebuild(cur) {
		return nil
	}
	// An index without the marker may lack the holds made before it: a kind
	// moved from LabelScoped, an index deleted while no process ran and
	// recreated by an acquirer, or a process that died between creating it
	// and rebuilding it (§10.6.6). A rebuild that fails here is left to the
	// write loops, which retry it with backoff, and the next start finds the
	// marker still absent.
	if err := ix.rebuild(ctx); err != nil {
		ix.markLost()
		t.warn("credential-use: rebuilding the new index failed; the write loops retry it",
			"kind", t.gk.String(), "index", ix.ref(), "error", err.Error())
	}
	return nil
}

// noHoldersIn refuses a namespace that holders of t's kind live in (§10.6.1):
// the index must outlive every holder, so it can't share a namespace with
// one. Admission makes it unreachable; it stays for a cluster without the
// policy. A cluster-scoped kind's holders live in no namespace.
func (t *Tracker) noHoldersIn(ctx context.Context, namespace string) error {
	m, err := t.s.c.Mapper.RESTMapping(t.gk, t.o.Holder.Version)
	if err != nil {
		return err
	}
	if m.Scope.Name() != meta.RESTScopeNameNamespace {
		return nil
	}
	l, err := t.s.c.Metadata.Resource(t.gvr).Namespace(namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("credential-use: %s: listing its holders in the index namespace %s: %w", t.gk, namespace, err)
	}
	if len(l.Items) > 0 {
		return fmt.Errorf("credential-use: %s: the index namespace %s holds holders of the kind (%s): it must outlive every holder, so use a namespace no holder lives in, such as krateo-credential-use (§10.6.1)",
			t.gk, namespace, l.Items[0].Name)
	}
	return nil
}

// pointIndex writes the index's location on the holder CRD once, after a
// GET that finds it absent, with a merge patch guarded by the CRD's
// resourceVersion; a conflict reads and decides again, at most
// maxCRDWriteAttempts times (§7.3, "Writing the CRD"). A pointer that names
// another index refuses the start: two processes of one kind can never keep
// two indexes (§10.6.1).
func (t *Tracker) pointIndex(ctx context.Context) error {
	want := t.idx.ref()
	crds := t.s.c.Dynamic.Resource(crdGVR)
	check := func() (*unstructured.Unstructured, bool, error) {
		crd, err := crds.Get(ctx, t.crdName, metav1.GetOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("credential-use: reading the CRD of %s for its index pointer (%s): %w", t.gk, AnnotationIndex, err)
		}
		cur, ok := crd.GetAnnotations()[AnnotationIndex]
		if !ok {
			return crd, false, nil
		}
		if cur != want {
			return nil, false, fmt.Errorf("credential-use: CRD %s points at the index %s (%s), but this process's IndexNamespace gives %s: every process of the kind must share one index; "+
				"to move it, Disown the kind, then re-point the annotation by hand (§10.6.1)", t.crdName, cur, AnnotationIndex, want)
		}
		return crd, true, nil
	}
	var last error
	for range maxCRDWriteAttempts {
		crd, done, err := check()
		if err != nil || done {
			return err
		}
		patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": crd.GetResourceVersion(),
			"annotations":     map[string]string{AnnotationIndex: want},
		}})
		if err != nil {
			return err
		}
		_, err = crds.Patch(ctx, t.crdName, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			return nil
		}
		if Classify(err) != ClassConflict {
			return fmt.Errorf("credential-use: writing the index pointer of %s on CRD %s: %w", t.gk, t.crdName, err)
		}
		last = err
	}
	if _, done, err := check(); err != nil || done {
		return err
	}
	sinkMetrics{t}.CRDWriteStarved(kindLabel(t.gk))
	t.warn("credential-use: writing the index pointer conflicted every time; it will be written at the next start",
		"crd", t.crdName, "attempts", maxCRDWriteAttempts, "lastConflict", last.Error())
	return nil
}

// --- the tracker's side ---

// isPerName reports whether t finds the holds of protected kind gk through
// its index (§10.6).
func (t *Tracker) isPerName(gk schema.GroupKind) bool { return t.idx.covers(gk) }

// recorded reports whether some cached holder's record names id, or can't
// be parsed, and so may: its line stays (§10.6.2).
func (t *Tracker) recorded(id ID) bool {
	idx := t.holders.GetIndexer()
	for _, key := range []string{id.String(), unparseableKey} {
		if objs, err := idx.ByIndex(refsIndex, key); err == nil && len(objs) > 0 {
			return true
		}
	}
	return false
}

// indexRef is the index ConfigMap, for events.
func (t *Tracker) indexRef() *metav1.PartialObjectMetadata {
	o := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: t.idx.namespace, Name: t.idx.name}}
	o.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
	return o
}

// newTrackerIndex builds t's index, for its PerName kinds, or none.
func (t *Tracker) newTrackerIndex() error {
	s := t.s
	kinds := map[schema.GroupKind]bool{}
	for _, pk := range t.o.Protected {
		if pk.Watch == PerName {
			kinds[pk.GVK.GroupKind()] = true
		}
	}
	if len(kinds) == 0 {
		return nil
	}
	var store indexStore = newConfigMapStore(s.c.Dynamic, s.o.IndexNamespace, IndexName(t.gk))
	if f := s.hooks.wrapIndex; f != nil {
		store = f(store)
	}
	ix := newIndex(t.gk, t.prefix, s.o.IndexNamespace, s.indexMaxLines(), kinds, store, t.proto.srv)
	ix.recorded = t.recorded
	ix.disowning = func(ctx context.Context) (bool, error) {
		set, err := t.disownLive(ctx)
		return len(t.disownPerName(set)) > 0, err
	}
	ix.keptLive = t.disownKeeps
	ix.keptCached = func(gk schema.GroupKind) bool { return keptKinds(t.crdCached()).covers(gk) }
	ix.onLines = func(lines []string) {
		ratio := float64(len(lines)) / float64(ix.maxLines)
		sinkMetrics{t}.IndexFillRatio(kindLabel(t.gk), ratio)
		if ratio >= indexFillWarning {
			s.events.record(t.indexRef(), "Warning", EventIndexFilling, fmt.Sprintf(
				"the index of %s holds at least %d%% of its cap of %d lines: move the kind to LabelScoped, or raise SharedOptions.IndexMaxLines within %d (§10.6.4)",
				kindLabel(t.gk), int(indexFillWarning*100), ix.maxLines, IndexMaxLinesCeiling))
		}
	}
	ix.onTampered = func(id ID) {
		sinkMetrics{t}.IndexTampered(kindLabel(t.gk))
		s.events.record(t.indexRef(), "Warning", EventIndexTampered,
			"the line of "+unheldRef(id)+" was dropped while the object still carries a finalizer of "+kindLabel(t.gk)+": it was added back")
		t.warn("credential-use: a line dropped from the index named an object that still carries the kind's finalizer; it was added back",
			"kind", t.gk.String(), "index", ix.ref(), "object", id.String())
	}
	ix.onDenied = func(id ID, err error) {
		sinkMetrics{t}.IndexReadDenied(kindLabel(t.gk))
		t.warn("credential-use: an object the index names can't be read (403); its line is kept, and the read isn't retried until RBAC changes: grant get on it (§10.6.5)",
			"kind", t.gk.String(), "index", ix.ref(), "object", id.String(), "error", err.Error())
	}
	ix.onRestoredUnread = func(id ID) {
		t.warn("credential-use: restored the line of an object this process can't read: someone else dropped it, and it may carry the kind's finalizer (§10.6.3)",
			"kind", t.gk.String(), "index", ix.ref(), "object", id.String())
	}
	ix.onVouchMalformed = func(entries []string) {
		t.warn("credential-use: entries of the index's vouched annotation can't be read, and are ignored: write each as <namespace>/<Kind[.group]>/<name> (§10.6.6)",
			"kind", t.gk.String(), "index", ix.ref(), "entries", strings.Join(entries, ", "))
	}
	ix.onVouchWrong = func(id ID) {
		t.warn("credential-use: an object vouched for in the index carries a finalizer of the kind: the vouch is wrong, and is ignored for it (§10.6.6)",
			"kind", t.gk.String(), "index", ix.ref(), "object", id.String())
	}
	unreadLogged := newReportedSet()
	ix.onUnreadable = func(ids []ID) {
		for _, id := range ids {
			if unreadLogged.first(consentKey{ref: id}) {
				t.warn("credential-use: the rebuild couldn't read an object a record names; its line is kept, since it may carry the kind's finalizer (§10.6.3)",
					"kind", t.gk.String(), "index", ix.ref(), "object", id.String(), "unreadable", len(ids))
			}
		}
	}
	ix.onPolicyConfig = func(err error) {
		t.warn("credential-use: the index namespace's admission policy has no parameters; check that cu-policy-params exists (§10.6.1)",
			"kind", t.gk.String(), "index", ix.ref(), "error", err.Error())
	}
	t.idx = ix
	inf, err := newIndexInformer(t)
	if err != nil {
		return err
	}
	ix.inf = inf
	t.proto.lines = ix
	return nil
}

// indexMaxLines is SharedOptions.IndexMaxLines, or its default.
func (s *Shared) indexMaxLines() int {
	if s.o.IndexMaxLines == 0 {
		return DefaultIndexMaxLines
	}
	return s.o.IndexMaxLines
}

// --- the write loops' side ---

// indexTick is how often the write loops look for an index to rebuild, and
// live checks that are due.
const indexTick = time.Second

// runIndexes runs every index's rebuilds and live checks until ctx ends.
func (s *Shared) runIndexes(ctx context.Context) {
	tick := s.clock.NewTicker(indexTick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			now := s.clock.Now()
			for _, t := range s.trackerList() {
				if t.idx == nil {
					continue
				}
				if err := t.idx.lostTick(ctx, now); err != nil {
					t.warn("credential-use: rebuilding the index failed; it is retried", "kind", t.gk.String(), "index", t.idx.ref(), "error", err.Error())
				}
				if err := t.idx.recheckTick(ctx, now); err != nil {
					t.warn("credential-use: a live check for the index failed; it is retried", "kind", t.gk.String(), "index", t.idx.ref(), "error", err.Error())
				}
			}
		}
	}
}

// recheckIndex is the write loops' startup recheck from t's index (§10.2,
// §10.6.5, M1 of the review of revision 16), run at their start and after a
// Track while they run.
func (s *Shared) recheckIndex(ctx context.Context, t *Tracker, done map[ID]bool) error {
	return t.idx.recheckRest(ctx, done, func(id ID, n string) { s.queue.Add(batchKey{t.gk, n}, id) })
}

// recheckRest reads the index live, reads each object it names, and hands
// enqueue every (object, holder namespace) of a finalizer of the kind it
// finds, for its release batch; the lines of objects that carry none, or are
// gone, and that no record names, are pruned under §10.6.2's rule. A missing
// index is rebuilt first. A read that fails fails the recheck, which the
// caller retries: a skipped recheck can strand a hold. done are the objects a
// try before this one dealt with, which this one skips, and adds to.
func (ix *index) recheckRest(ctx context.Context, done map[ID]bool, enqueue func(id ID, n string)) error {
	var errs []error
	cur, err := ix.store.get(ctx)
	if Classify(err) == ClassNotFound || err == nil && ix.needsRebuild(cur) {
		// Gone, or never rebuilt since its creation (§10.6.6). A rebuild
		// that fails doesn't stop the recheck of the lines there are: its
		// error is returned with the others, and the caller retries.
		if err := ix.rebuild(ctx); err != nil {
			errs = append(errs, err)
		}
		cur, err = ix.store.get(ctx)
	}
	if err != nil {
		errs = append(errs, fmt.Errorf("credential-use: reading the index %s for the startup recheck: %w", ix.ref(), err))
		return errors.Join(errs...)
	}
	ix.saw(cur)
	vouched := ix.parseVouched(cur.vouched)
	var bare []ID
	for _, id := range ix.ids(cur.lines) {
		if done[id] {
			continue
		}
		m, err := ix.srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
		switch Classify(err) {
		case ClassNone:
			ix.readable(id)
		case ClassNotFound:
			ix.readable(id)
			bare = append(bare, id)
			continue
		default:
			if vouched[id] {
				// An operator checked by hand that it carries no finalizer
				// of the kind: while it can't be read, it is skipped,
				// neither reported nor retried (§10.6.6).
				done[id] = true
				continue
			}
			if Classify(err) == ClassRBAC {
				// The authorizer refused it (class (d)): its line stays,
				// and no retry can read it before RBAC changes, so it is
				// reported once, never retried as an error. Any other 403,
				// an admission or a policy denial, is retried.
				ix.denied(id, err)
				done[id] = true
				continue
			}
			errs = append(errs, fmt.Errorf("reading %s: %w", id, err))
			continue
		}
		held := false
		for _, f := range m.Finalizers {
			if n, ok := namespaceOfFinalizer(f, ix.prefix); ok {
				held = true
				enqueue(id, n)
			}
		}
		if held {
			done[id] = true
		}
		if !held {
			bare = append(bare, id)
		} else if vouched[id] {
			// Readable, and held: treated as any other, its batch enqueued.
			ix.vouchWrong(id)
		}
	}
	if err := ix.prune(ctx, ix.prunable(bare)); err != nil {
		errs = append(errs, err)
	} else {
		for _, id := range bare {
			done[id] = true
		}
	}
	return errors.Join(errs...)
}
