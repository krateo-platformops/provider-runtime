package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The index's pieces without an API server (plan, PR 8; §10.6).

var configMapsGR = schema.GroupResource{Resource: "configmaps"}

// memStore is an index store in memory, with resourceVersions; beforeUpdate,
// if set, runs before each update, as a concurrent writer would.
type memStore struct {
	exists       bool
	lines        []string
	rv           int
	beforeUpdate func(s *memStore)
	beforeCreate func(s *memStore)
	failGet      error
	updates      int
	rebuilt      bool
	vouched      string
}

func (s *memStore) get(context.Context) (indexVersion, error) {
	if s.failGet != nil {
		return indexVersion{}, s.failGet
	}
	if !s.exists {
		return indexVersion{}, apierrors.NewNotFound(configMapsGR, "cu-index")
	}
	return indexVersion{lines: slices.Clone(s.lines), rv: strconv.Itoa(s.rv), rebuilt: s.rebuilt, vouched: s.vouched}, nil
}

func (s *memStore) update(_ context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	if f := s.beforeUpdate; f != nil {
		f(s)
	}
	if !s.exists {
		return indexVersion{}, apierrors.NewNotFound(configMapsGR, "cu-index")
	}
	if cur.rv != strconv.Itoa(s.rv) {
		return indexVersion{}, apierrors.NewConflict(configMapsGR, "cu-index", errors.New("modified"))
	}
	s.lines, s.rv = slices.Clone(lines), s.rv+1
	s.rebuilt = s.rebuilt || rebuilt
	s.updates++
	return indexVersion{lines: slices.Clone(lines), rv: strconv.Itoa(s.rv), rebuilt: s.rebuilt, vouched: s.vouched}, nil
}

func (s *memStore) create(_ context.Context, lines []string, rebuilt bool) (indexVersion, error) {
	if f := s.beforeCreate; f != nil {
		f(s)
	}
	if s.exists {
		return indexVersion{}, apierrors.NewAlreadyExists(configMapsGR, "cu-index")
	}
	s.exists, s.lines, s.rv, s.rebuilt = true, slices.Clone(lines), s.rv+1, rebuilt
	return indexVersion{lines: slices.Clone(lines), rv: strconv.Itoa(s.rv), rebuilt: rebuilt}, nil
}

// objServer serves Get from a map of objects, and fails the names in fail.
type objServer struct {
	server
	objs   map[ID]*metav1.PartialObjectMetadata
	fail   map[ID]bool
	forbid map[ID]bool // refused by the authorizer (class (d)), as outside a resourceNames grant
	policy map[ID]bool // refused with a policy's 403 (class (b))
	gets   []ID
}

func (s *objServer) Get(_ context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	id := ID{Group: gk.Group, Kind: gk.Kind, Namespace: ns, Name: name}
	s.gets = append(s.gets, id)
	if s.fail[id] {
		return nil, apierrors.NewServiceUnavailable("down")
	}
	if s.forbid[id] {
		return nil, rbacForbidden(ns, name)
	}
	if s.policy[id] {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name,
			errors.New("ValidatingAdmissionPolicy 'p' with binding 'p' denied request: no"))
	}
	if o := s.objs[id]; o != nil {
		return o, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
}

// List lists nothing: the tests that use objServer read by name.
func (s *objServer) List(context.Context, schema.GroupKind, string) ([]metav1.PartialObjectMetadata, error) {
	return nil, nil
}

func testIndex(st indexStore, srv server) *index {
	prefix, _ := FinalizerPrefix(widgetGK)
	return newIndex(widgetGK, prefix, "idx", 5, map[schema.GroupKind]bool{secretGroupKind: true}, st, srv)
}

func heldObj(name string) *metav1.PartialObjectMetadata {
	f, _ := FinalizerName(widgetGK, "ns")
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Finalizers: []string{f}}}
}

// Lines are <group>/<kind>/<namespace>/<name>, sorted, one each, ending in
// a newline; a line that doesn't parse is kept but names nothing.
func TestIndexLineCodec(t *testing.T) {
	id := ID{Group: "core.krateo.io", Kind: "KubernetesTarget", Namespace: "team-b", Name: "prod"}
	if got := id.String(); got != "core.krateo.io/KubernetesTarget/team-b/prod" {
		t.Errorf("line = %q", got)
	}
	if got, ok := parseIndexLine(id.String()); !ok || got != id {
		t.Errorf("parse(%q) = %v, %v", id.String(), got, ok)
	}
	if got, ok := parseIndexLine("/Secret/team-a/git-token"); !ok || got != SecretRef("team-a", "git-token").ID() {
		t.Errorf("a core kind's line parses to %v, %v", got, ok)
	}
	if got, ok := parseIndexLine("x/Cluster//c"); !ok || got.Namespace != "" || got.Name != "c" {
		t.Errorf("a cluster-scoped line parses to %v, %v", got, ok)
	}
	for _, bad := range []string{"", "nonsense", "/Secret/ns/", "//ns/n"} {
		if _, ok := parseIndexLine(bad); ok {
			t.Errorf("%q parses", bad)
		}
	}
	lines := decodeIndex("  b/K/n/x\n\na/K/n/y\nb/K/n/x\ngarbage\n")
	if want := []string{"a/K/n/y", "b/K/n/x", "garbage"}; !slices.Equal(lines, want) {
		t.Errorf("decode = %q, want %q", lines, want)
	}
	if got := encodeIndex(lines); got != "a/K/n/y\nb/K/n/x\ngarbage\n" {
		t.Errorf("encode = %q", got)
	}
	if encodeIndex(nil) != "" {
		t.Error("an empty index isn't empty")
	}
	ix := testIndex(&memStore{}, nil)
	if ids := ix.ids(lines); len(ids) != 0 {
		t.Errorf("ids of other kinds' lines = %v", ids)
	}
	if got := IndexName(schema.GroupKind{Group: "git.krateo.io", Kind: "Repo"}); got != "cu-index-repo.git.krateo.io" {
		t.Errorf("IndexName = %q", got)
	}
}

// A line that wouldn't read back as its ID, or is of a name too long (§10.6),
// is refused before A2, so that its holder isn't connected, or is Degraded;
// and no add writes it.
func TestAnIndexLineReadsBack(t *testing.T) {
	ctx := context.Background()
	good := SecretRef("ns", "good").ID()
	for _, id := range []ID{
		SecretRef("ns", "a\nb").ID(),
		SecretRef("ns", "a\n").ID(),
		SecretRef("ns", "a ").ID(),
		SecretRef("ns\n", "a").ID(),
		SecretRef("a/b", "c").ID(),
		// A name or a namespace longer than 253 bytes.
		SecretRef("ns", strings.Repeat("n", 254)).ID(),
		SecretRef(strings.Repeat("n", 254), "a").ID(),
		SecretRef("ns", strings.Repeat("n", 250_000)).ID(),
		SecretRef("ns", strings.Repeat("\x01", 254)).ID(),
	} {
		st := &memStore{exists: true, rebuilt: true}
		ix := testIndex(st, nil)
		if err := ix.ensure(ctx, id); err == nil || errors.Is(err, errIndexFull) || st.updates != 0 || len(err.Error()) > 1000 {
			t.Errorf("ensure of %.40q: %.200v, %d updates; want it refused, and the error short", id, err, st.updates)
		}
		if err := ix.add(ctx, []ID{id, good}); err != nil || !slices.Equal(st.lines, linesOf([]ID{good})) {
			t.Errorf("an add with %.40q: %v, %d lines; want the other's only", id, err, len(st.lines))
		}
		st = &memStore{}
		if err := testIndex(st, nil).add(ctx, []ID{id}); err != nil || len(st.lines) != 0 {
			t.Errorf("an index created for %.40q: %v, %d lines", id, err, len(st.lines))
		}
	}
	for _, id := range []ID{good, SecretRef("ns", "a/b").ID(), {Group: "x.io", Kind: "Thing", Name: "c"}, SecretRef(strings.Repeat("n", 253), strings.Repeat("n", 253)).ID()} {
		st := &memStore{exists: true, rebuilt: true}
		if err := testIndex(st, nil).ensure(ctx, id); err != nil || !slices.Equal(st.lines, linesOf([]ID{id})) {
			t.Errorf("ensure of %.40q: %v, %d lines; want its line", id, err, len(st.lines))
		}
	}
	// What the holder gets, which fits in an event's note.
	for _, name := range []string{"a\n", strings.Repeat("\x01", 254), strings.Repeat("n", 250_000)} {
		for _, established := range []bool{false, true} {
			r := newRig(t)
			st := &memStore{exists: true, rebuilt: true}
			r.p.lines = testIndex(st, r.srv)
			a, err := r.p.acquire(ctx, r.holder(t, "h", established), []Ref{SecretRef("ns", name)})
			if err != nil || a.Err == nil || a.MustNotConnect == established || a.Degraded != established || len(st.lines) != 0 || len(a.Err.Error()) > 1024 {
				t.Errorf("%.10q, established %v: must not connect %v, degraded %v, %.200v, %d lines; want it refused, and the error short", name, established, a.MustNotConnect, a.Degraded, a.Err, len(st.lines))
			}
		}
	}
}

// An add past the cap, or past a ConfigMap's 1 MiB, fails with
// errIndexFull, before any write (§10.6.4); an index already past a lowered
// cap still takes an add of a line it has.
func TestIndexFull(t *testing.T) {
	st := &memStore{exists: true}
	ix := testIndex(st, nil)
	var ids []ID
	for i := range 5 {
		ids = append(ids, SecretRef("ns", "s"+strconv.Itoa(i)).ID())
	}
	if err := ix.add(context.Background(), ids); err != nil {
		t.Fatal(err)
	}
	err := ix.add(context.Background(), []ID{SecretRef("ns", "s5").ID()})
	if !errors.Is(err, errIndexFull) || !strings.Contains(err.Error(), "LabelScoped") {
		t.Errorf("a sixth line = %v, want errIndexFull naming the remedy", err)
	}
	if st.updates != 1 {
		t.Errorf("%d updates, want the first only", st.updates)
	}
	if err := ix.add(context.Background(), ids[:1]); err != nil {
		t.Errorf("a line the full index has: %v", err)
	}
	ix.maxLines = IndexMaxLinesCeiling
	long := ID{Kind: "Secret", Namespace: "ns", Name: strings.Repeat("n", 249)} // 253 bytes with its number
	var many []ID
	for i := range 4300 {
		l := long
		l.Name = strconv.Itoa(i) + l.Name
		many = append(many, l)
	}
	if err := ix.add(context.Background(), many); !errors.Is(err, errIndexFull) || !strings.Contains(err.Error(), "bytes") {
		t.Errorf("past 1 MiB = %v, want errIndexFull for its size", err)
	}
}

// Every write merges: on a conflict it reads again and re-applies its own
// adds or removes, so another writer's change is never lost (§10.6.2, L5).
func TestIndexWritesMerge(t *testing.T) {
	a, b, c := SecretRef("ns", "a").ID(), SecretRef("ns", "b").ID(), SecretRef("ns", "c").ID()
	st := &memStore{exists: true, lines: []string{a.String()}}
	once := true
	st.beforeUpdate = func(s *memStore) {
		if once {
			once = false
			s.lines, s.rv = union(s.lines, []string{c.String()}), s.rv+1
		}
	}
	ix := testIndex(st, nil)
	if err := ix.add(context.Background(), []ID{b}); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{a, b, c}); !slices.Equal(st.lines, want) {
		t.Errorf("after a conflicting add: %q, want %q", st.lines, want)
	}
	once = true
	st.beforeUpdate = func(s *memStore) {
		if once {
			once = false
			s.lines, s.rv = union(s.lines, []string{SecretRef("ns", "d").ID().String()}), s.rv+1
		}
	}
	removed, err := ix.remove(context.Background(), []string{a.String(), "never/there/x/y"})
	if err != nil || !slices.Equal(removed, []string{a.String()}) {
		t.Fatalf("remove = %v, %v", removed, err)
	}
	if want := linesOf([]ID{b, c, SecretRef("ns", "d").ID()}); !slices.Equal(st.lines, want) {
		t.Errorf("after a conflicting remove: %q, want %q", st.lines, want)
	}

	// A create that finds the index created meanwhile merges into it, and
	// asks for the rebuild.
	st2 := &memStore{}
	st2.beforeCreate = func(s *memStore) {
		if !s.exists {
			s.exists, s.lines, s.rv = true, []string{c.String()}, 1
		}
	}
	ix2 := testIndex(st2, nil)
	if err := ix2.add(context.Background(), []ID{a}); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{a, c}); !slices.Equal(st2.lines, want) {
		t.Errorf("after a create race: %q, want %q", st2.lines, want)
	}
	if ix2.lost {
		t.Error("an add that lost the create race asks for a rebuild: the creator does")
	}
	// An add that creates a missing index asks for the rebuild from the
	// records (§10.6.6).
	ix3 := testIndex(&memStore{}, nil)
	if err := ix3.add(context.Background(), []ID{a}); err != nil || !ix3.lost {
		t.Errorf("an add that created the index: %v, rebuild asked %v", err, ix3.lost)
	}
}

// The prune's check after its write (§10.6.2): an object that carries the
// kind's prefix again, or can't be read, gets its line back; a bare or gone
// one doesn't.
func TestIndexPruneChecksAfter(t *testing.T) {
	held, bare, gone, unread := SecretRef("ns", "held").ID(), SecretRef("ns", "bare").ID(), SecretRef("ns", "gone").ID(), SecretRef("ns", "unread").ID()
	srv := &objServer{
		objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held"), bare: {ObjectMeta: metav1.ObjectMeta{Name: "bare"}}},
		fail: map[ID]bool{unread: true},
	}
	st := &memStore{exists: true, lines: linesOf([]ID{held, bare, gone, unread})}
	ix := testIndex(st, srv)
	if err := ix.prune(context.Background(), []ID{held, bare, gone, unread}); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{held, unread}); !slices.Equal(st.lines, want) {
		t.Errorf("after the prune: %q, want %q", st.lines, want)
	}
	// A re-add that fails is queued for the write loops.
	st.lines = linesOf([]ID{held})
	st.failGet = nil
	st.beforeUpdate = func(s *memStore) {
		// The prune's write lands; the re-add's read fails.
		s.failGet = errors.New("index unreadable")
	}
	if err := ix.prune(context.Background(), []ID{held}); err == nil {
		t.Fatal("a failed re-add is no error")
	}
	if ix.pending[held] == nil {
		t.Errorf("the failed re-add isn't queued: %v", ix.pending)
	}
}

// A drop is this process's own only in the version a successful removal of
// it returned, and only for the lines that removal removed (§10.6.6);
// whichever comes first, the event or the write's return.
func TestIndexObservesDrops(t *testing.T) {
	mine, theirs := SecretRef("ns", "mine").ID(), SecretRef("ns", "theirs").ID()
	kept := SecretRef("ns", "kept").ID().String()
	old := indexVersion{lines: linesOf([]ID{mine, theirs, SecretRef("ns", "kept").ID()}), rv: "7"}
	only := func(t *testing.T, ix *index, want ...ID) {
		t.Helper()
		if got := SortedIDs(keysOf(ix.pending)); !slices.Equal(got, SortedIDs(want)) {
			t.Errorf("pending = %v, want %v", got, want)
		}
	}
	for _, eventFirst := range []bool{true, false} {
		ix := testIndex(&memStore{}, nil)
		// One delivery shows this process's removal of mine, and, coalesced
		// with it, someone else's of theirs.
		event := func() { ix.observed(&old, indexVersion{lines: []string{kept}, rv: "9"}) }
		if eventFirst {
			event()
		}
		ix.removed("9", []string{mine.String()})
		if !eventFirst {
			event()
		}
		only(t, ix, theirs)
		if r := ix.pending[theirs]; r == nil || !r.tampered {
			t.Fatalf("a drop by someone else isn't queued as tampering: %v", ix.pending)
		}
	}
	// Coalesced past this process's version into someone else's: checked.
	ix := testIndex(&memStore{}, nil)
	ix.removed("9", []string{mine.String()})
	ix.observed(&old, indexVersion{lines: []string{kept}, rv: "10"})
	only(t, ix, mine, theirs)
	// A line this process removed, back since, is someone else's to drop.
	ix = testIndex(&memStore{}, nil)
	ix.removed("9", []string{mine.String()})
	ix.observed(&indexVersion{lines: linesOf([]ID{mine}), rv: "8"}, indexVersion{rv: "9"})
	ix.observed(&indexVersion{rv: "9"}, indexVersion{lines: linesOf([]ID{mine}), rv: "10"})
	ix.observed(&indexVersion{lines: linesOf([]ID{mine}), rv: "10"}, indexVersion{rv: "11"})
	only(t, ix, mine)
	// A drop queued for another reason too isn't taken back.
	ix = testIndex(&memStore{}, nil)
	ix.observed(&indexVersion{lines: linesOf([]ID{mine}), rv: "8"}, indexVersion{rv: "9"})
	ix.queue([]ID{mine}, false)
	ix.removed("9", []string{mine.String()})
	only(t, ix, mine)
	// What this process remembers is bounded; a removal it forgot is checked.
	ix = testIndex(&memStore{}, nil)
	ix.removed("9", []string{mine.String()})
	for i := range maxOwnRemovals {
		ix.removed(strconv.Itoa(100+i), nil)
	}
	if len(ix.own) != maxOwnRemovals {
		t.Errorf("%d removals remembered, want %d", len(ix.own), maxOwnRemovals)
	}
	ix.observed(&indexVersion{lines: linesOf([]ID{mine}), rv: "8"}, indexVersion{rv: "9"})
	only(t, ix, mine)
}

// lossyStore is a memStore whose next update lands, and then fails as with
// its response lost; afterLoss runs in between, and afterGet after each get.
type lossyStore struct {
	*memStore
	lose      bool
	afterLoss func()
	afterGet  func()
}

func (s *lossyStore) get(ctx context.Context) (indexVersion, error) {
	if err := ctx.Err(); err != nil {
		return indexVersion{}, err
	}
	v, err := s.memStore.get(ctx)
	if s.afterGet != nil {
		s.afterGet()
	}
	return v, err
}

func (s *lossyStore) update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	if err := ctx.Err(); err != nil {
		return indexVersion{}, err
	}
	v, err := s.memStore.update(ctx, cur, lines, rebuilt)
	if err == nil && s.lose {
		s.lose = false
		if s.afterLoss != nil {
			s.afterLoss()
		}
		return indexVersion{}, apierrors.NewTimeoutError("the response was lost", 1)
	}
	return v, err
}

// Every line an attempt of a prune tried to remove is checked after it,
// whatever came of the attempt, and whether its drop is delivered before or
// after the attempt returns (§10.6.2, §10.6.6): a conflict with someone
// else's removal of the line, and a removal whose response was lost. The
// object is held, or can't be read; either way its line is back, without a
// later check's help.
func TestPruneChecksWhatItTried(t *testing.T) {
	id := SecretRef("ns", "x").ID()
	for _, unreadable := range []bool{false, true} {
		for _, fault := range []string{"conflict", "lost response"} {
			for _, event := range []string{"before", "after", "never"} {
				t.Run(fmt.Sprintf("unreadable=%v/%s/event %s the return", unreadable, fault, event), func(t *testing.T) {
					srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: heldObj("x")}, forbid: map[ID]bool{id: unreadable}}
					st := &lossyStore{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{id}), rv: 1}}
					ix := testIndex(st, srv)
					deliver := func() { ix.observed(&indexVersion{lines: linesOf([]ID{id}), rv: "1"}, indexVersion{rv: "2"}) }
					before := func() {
						if event == "before" {
							deliver()
						}
					}
					if fault == "conflict" {
						st.beforeUpdate = func(s *memStore) {
							s.beforeUpdate, s.lines, s.rv = nil, nil, s.rv+1
							before()
						}
					} else {
						st.lose, st.afterLoss = true, before
					}
					err := ix.prune(context.Background(), []ID{id})
					if (err != nil) != (fault == "lost response") {
						t.Errorf("prune = %v", err)
					}
					if !slices.Equal(st.lines, linesOf([]ID{id})) {
						t.Fatalf("after the prune: %q, want its line back", st.lines)
					}
					if event == "after" {
						deliver()
					}
					if err := ix.recheckTick(context.Background(), time.Now()); err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(st.lines, linesOf([]ID{id})) || len(ix.pending) != 0 {
						t.Errorf("after the live checks: %q, pending %v", st.lines, ix.pending)
					}
				})
			}
		}
	}
}

// A drop delivered while its object's check runs survives the check, which
// read the line before it (§10.6.6).
func TestADropDuringItsCheckSurvives(t *testing.T) {
	id := SecretRef("ns", "x").ID()
	for _, unreadable := range []bool{false, true} {
		srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: heldObj("x")}, forbid: map[ID]bool{id: unreadable}}
		st := &lossyStore{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{id}), rv: 1}}
		ix := testIndex(st, srv)
		ix.queue([]ID{id}, false)
		st.afterGet = func() { // the check's add has read the line
			st.afterGet = nil
			st.lines, st.rv = nil, st.rv+1
			ix.observed(&indexVersion{lines: linesOf([]ID{id}), rv: "1"}, indexVersion{rv: "2"})
		}
		now := time.Now()
		if err := ix.recheckTick(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		if ix.pending[id] == nil {
			t.Fatal("the check took the drop delivered during it")
		}
		if err := ix.recheckTick(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(st.lines, linesOf([]ID{id})) || len(ix.pending) != 0 {
			t.Errorf("unreadable=%v: after the next check: %q, pending %v", unreadable, st.lines, ix.pending)
		}
	}
}

// A vouch withdrawn while a prune of its line is under way doesn't count: the
// check after it reads the vouch live, whatever this process last saw.
func TestAVouchWithdrawnDuringAPrune(t *testing.T) {
	id := SecretRef("ns", "x").ID()
	st := &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{id}), rv: 1, vouched: "ns/Secret/x"}
	srv := &getHook{objServer: &objServer{forbid: map[ID]bool{id: true}}}
	ix := testIndex(st, srv)
	srv.on = func() { st.vouched = "" } // after the prune's write has returned
	if err := ix.prune(context.Background(), []ID{id}); err != nil || !slices.Equal(st.lines, linesOf([]ID{id})) {
		t.Errorf("the vouch withdrawn: %v, lines %q; want its line back", err, st.lines)
	}
	srv.on = func() { st.vouched = "ns/Secret/x" }
	if err := ix.prune(context.Background(), []ID{id}); err != nil || len(st.lines) != 0 {
		t.Errorf("vouched for meanwhile: %v, lines %q; want it pruned", err, st.lines)
	}
}

// getHook is an objServer that runs on before each Get.
type getHook struct {
	*objServer
	on func()
}

func (s *getHook) Get(ctx context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	s.on()
	return s.objServer.Get(ctx, gk, ns, name)
}

// stopServer is an objServer that fails once its context is done, as the API
// client does.
type stopServer struct{ *objServer }

func (s stopServer) Get(ctx context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.objServer.Get(ctx, gk, ns, name)
}

// A graceful stop ends ctx in the middle of a prune: the write isn't
// aborted, and the check after it and the re-add run to their end, within
// pruneBound (§10.6.2). A prune that hasn't begun doesn't.
func TestAPruneOutlivesAStop(t *testing.T) {
	held, denied := SecretRef("ns", "held").ID(), SecretRef("ns", "denied").ID()
	srv := stopServer{&objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held")}, forbid: map[ID]bool{denied: true}}}
	st := &lossyStore{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{denied, held}), rv: 1}}
	ix := testIndex(st, srv)
	ctx, stop := context.WithCancel(context.Background())
	st.beforeUpdate = func(*memStore) { stop() }
	if err := ix.prune(ctx, []ID{denied, held}); err != nil {
		t.Errorf("prune = %v", err)
	}
	if want := linesOf([]ID{denied, held}); !slices.Equal(st.lines, want) {
		t.Errorf("after a stop in the middle of the prune: %q, want %q", st.lines, want)
	}
	updates := st.updates
	if err := ix.prune(ctx, []ID{held}); !errors.Is(err, context.Canceled) || st.updates != updates {
		t.Errorf("a prune begun after the stop = %v, with %d updates", err, st.updates-updates)
	}
}

// While Disown covers the kind and hasn't settled, a prune's lines stay,
// whatever the CRD's cache shows: the live read after the removal decides,
// and one that fails puts them back. Disown's own emptying removes them
// (§7.4).
func TestAPruneWhileDisownHasntSettled(t *testing.T) {
	ctx := context.Background()
	bare := SecretRef("ns", "bare").ID()
	all := parseDisown("all")
	for name, tc := range map[string]struct {
		cached, live disownSet
		fail         bool
		empty        bool
		kept         bool
		updates      int
	}{
		"off":                          {updates: 1},
		"on, and cached":               {cached: all, live: all, kept: true},
		"on, and not cached":           {live: all, kept: true, updates: 2},
		"on for another kind":          {live: parseDisown("ConfigMap"), updates: 1},
		"unreadable":                   {fail: true, kept: true, updates: 2},
		"on, and the emptying":         {cached: all, live: all, empty: true, updates: 1},
		"off, with a cache that isn't": {cached: all, kept: true},
	} {
		st := &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{bare}), rv: 1}
		ix := testIndex(st, &objServer{objs: map[ID]*metav1.PartialObjectMetadata{bare: {ObjectMeta: metav1.ObjectMeta{Name: "bare"}}}})
		ix.keptCached = tc.cached.covers
		ix.keptLive = func(context.Context) (disownSet, error) {
			if tc.fail {
				return disownSet{}, errors.New("the CRD can't be read")
			}
			return tc.live, nil
		}
		remove := ix.prune
		if tc.empty {
			remove = ix.empty
		}
		if err := remove(ctx, []ID{bare}); (err != nil) != tc.fail {
			t.Errorf("%s: %v", name, err)
		}
		if got := len(st.lines) == 1; got != tc.kept || st.updates != tc.updates {
			t.Errorf("%s: the line kept: %v, in %d updates; want %v, in %d", name, got, st.updates, tc.kept, tc.updates)
		}
	}
}

// A bare object's dropped line goes back while Disown covers the kind and
// hasn't settled, unreported; a switch that can't be read retries the check
// (§7.4).
func TestABareDropWhileDisownHasntSettled(t *testing.T) {
	bare := SecretRef("ns", "bare").ID()
	st := &memStore{exists: true, rebuilt: true}
	ix := testIndex(st, &objServer{objs: map[ID]*metav1.PartialObjectMetadata{bare: {ObjectMeta: metav1.ObjectMeta{Name: "bare"}}}})
	ix.onTampered = func(ID) { t.Error("a bare object's line was reported as tampering") }
	var set disownSet
	fail := errors.New("the CRD can't be read")
	ix.keptLive = func(context.Context) (disownSet, error) { return set, fail }
	now := time.Now()
	ix.queue([]ID{bare}, true)
	if err := ix.recheckTick(context.Background(), now); err == nil || ix.pending[bare] == nil || len(st.lines) != 0 {
		t.Errorf("with the switch unreadable: %v, pending %v, lines %q", err, ix.pending, st.lines)
	}
	set, fail = parseDisown("all"), nil
	if err := ix.recheckTick(context.Background(), now.Add(time.Hour)); err != nil || len(ix.pending) != 0 || !slices.Equal(st.lines, linesOf([]ID{bare})) {
		t.Errorf("under the switch: %v, pending %v, lines %q; want the line back", err, ix.pending, st.lines)
	}
	set, st.lines = disownSet{}, nil
	ix.queue([]ID{bare}, true)
	if err := ix.recheckTick(context.Background(), now); err != nil || len(ix.pending) != 0 || len(st.lines) != 0 {
		t.Errorf("without it: %v, pending %v, lines %q; want it dropped", err, ix.pending, st.lines)
	}
}

// Once settled, Disown removes only the lines a live read proves free: not
// one whose strip failed, which can't be read and isn't vouched for, or that
// carries the prefix (§7.4).
func TestFreeLines(t *testing.T) {
	id := func(n string) ID { return SecretRef("ns", n).ID() }
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{
		id("bare"): {ObjectMeta: metav1.ObjectMeta{Name: "bare"}}, id("unstripped"): {ObjectMeta: metav1.ObjectMeta{Name: "unstripped"}}, id("held"): heldObj("held"), id("vouched-held"): heldObj("vouched-held"),
	}, forbid: map[ID]bool{id("denied"): true, id("vouched"): true}, fail: map[ID]bool{id("failing"): true}}
	prefix, _ := FinalizerPrefix(widgetGK)
	tr := &Tracker{gk: widgetGK, prefix: prefix}
	var lined []ID
	for _, n := range []string{"bare", "denied", "failing", "gone", "held", "unstripped", "vouched", "vouched-held"} {
		lined = append(lined, id(n))
	}
	got := tr.freeLines(context.Background(), srv, lined, map[ID]bool{id("unstripped"): true}, map[ID]bool{id("vouched"): true, id("vouched-held"): true})
	if want := []ID{id("bare"), id("gone"), id("vouched")}; !slices.Equal(got, want) {
		t.Errorf("free = %v, want %v", got, want)
	}
}

// The live checks restore the lines of objects that carry the kind's
// prefix, report a dropped one as tampering, and back off a failed one.
func TestIndexRecheck(t *testing.T) {
	held, bare, unread := SecretRef("ns", "held").ID(), SecretRef("ns", "bare").ID(), SecretRef("ns", "unread").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held"), bare: {ObjectMeta: metav1.ObjectMeta{Name: "bare"}}}, fail: map[ID]bool{unread: true}}
	st := &memStore{exists: true}
	ix := testIndex(st, srv)
	var tampered []ID
	ix.onTampered = func(id ID) { tampered = append(tampered, id) }
	ix.queue([]ID{held, bare, unread}, true)
	now := time.Now()
	if err := ix.recheckTick(context.Background(), now); err == nil {
		t.Error("a failed check is no error")
	}
	if !slices.Equal(st.lines, linesOf([]ID{held})) || !slices.Equal(tampered, []ID{held}) {
		t.Errorf("lines %q, tampered %v; want %v restored and reported", st.lines, tampered, held)
	}
	if r := ix.pending[unread]; r == nil || !r.due.After(now) || len(ix.pending) != 1 {
		t.Errorf("pending = %v, want only %v, backed off", ix.pending, unread)
	}
}

// The rebuild reads the records of every holder, and keeps the objects that
// carry the kind's prefix (§10.6.6); it creates the index if it is gone.
func TestIndexRebuild(t *testing.T) {
	m := apimodel.New(time.Now)
	seed(m, "ns", "s", []string{"h"}, true)
	must(m.Create(secretKey("ns", "bare"), metav1.ObjectMeta{}))
	rec, _ := EncodeRecord([]ID{SecretRef("ns", "s").ID(), SecretRef("ns", "bare").ID(), SecretRef("ns", "missing").ID()})
	must(m.Create(widgetKey("ns", "h2"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationRefs: rec}}))
	st := &memStore{}
	ix := testIndex(st, nsServer{modelServer{m: m}, "ns"})
	if err := ix.rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{SecretRef("ns", "s").ID().String()}; !slices.Equal(st.lines, want) {
		t.Errorf("rebuilt %q, want %q", st.lines, want)
	}
}

// nsServer lists namespace ns for a list of every namespace, which the
// model keeps for cluster-scoped kinds.
type nsServer struct {
	modelServer
	ns string
}

func (s nsServer) List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	if namespace == "" {
		namespace = s.ns
	}
	return s.modelServer.List(ctx, gk, namespace)
}

// Setup refuses a PerName kind without IndexNamespace, with one that isn't
// a namespace name, and with a cap outside 0 to the ceiling (§7.3, A19).
func TestTrackValidatesIndex(t *testing.T) {
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(widgetGK.WithVersion("v1"), meta.RESTScopeNamespace)
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	o := Options{
		Holder:          widgetGK.WithVersion("v1"),
		OwnerFinalizers: []string{"owner"},
		Refs:            func(metav1.Object) ([]Ref, error) { return nil, nil },
		Enqueue:         func(types.NamespacedName) {},
		TimeoutBound:    90 * time.Second,
		Protected:       []ProtectedKind{{GVK: secretGVK, Watch: PerName}},
	}
	cases := map[string]struct {
		so   SharedOptions
		want string
	}{
		"no IndexNamespace":      {SharedOptions{}, "SharedOptions.IndexNamespace"},
		"a bad namespace":        {SharedOptions{IndexNamespace: "Not_A_Namespace"}, "not a namespace name"},
		"a cap over the ceiling": {SharedOptions{IndexNamespace: "krateo-credential-use", IndexMaxLines: IndexMaxLinesCeiling + 1}, "the ceiling"},
		"a negative cap":         {SharedOptions{IndexNamespace: "krateo-credential-use", IndexMaxLines: -1}, "the ceiling"},
	}
	md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	for name, c := range cases {
		s := newShared(Clients{Metadata: md, Dynamic: dyn, Mapper: mapper}, c.so, testingclock.NewFakeClock(time.Now()))
		if _, err := s.Track(context.Background(), o); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Track = %v, want an error containing %q", name, err, c.want)
		}
	}
	s := newShared(Clients{Metadata: md, Dynamic: dyn, Mapper: mapper}, SharedOptions{IndexNamespace: "ok", IndexMaxLines: IndexMaxLinesCeiling}, testingclock.NewFakeClock(time.Now()))
	if _, err := s.validate(o); err != nil {
		t.Errorf("a cap at the ceiling: %v", err)
	}
}

// Every holder kind of a Shared must agree on a protected kind's watch mode;
// a refused or failed Track gives its claim back.
func TestWatchModesAgree(t *testing.T) {
	s := newShared(Clients{}, SharedOptions{}, testingclock.NewFakeClock(time.Now()))
	label := Options{Holder: widgetGK.WithVersion("v1"), Protected: []ProtectedKind{{GVK: secretGVK}}}
	perName := Options{Holder: schema.GroupVersionKind{Group: "x.io", Version: "v1", Kind: "Gadget"}, Protected: []ProtectedKind{{GVK: secretGVK, Watch: PerName}}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.claimModesLocked(label); err != nil {
		t.Fatal(err)
	}
	err := s.claimModesLocked(perName)
	var me *modeError
	if !errors.As(err, &me) || !strings.Contains(err.Error(), "LabelScoped") || !strings.Contains(err.Error(), "PerName") {
		t.Errorf("a disagreeing mode = %v", err)
	}
	s.releaseModesLocked(label)
	if err := s.claimModesLocked(perName); err != nil {
		t.Errorf("once the other claim is gone: %v", err)
	}
}

// The missing-parameters denial is matched by its text, never its code
// (§10.6.1): it is a 422 Invalid, as a Deleting outcome is.
func TestPolicyParamsMissing(t *testing.T) {
	msg := `configmaps "cu-index-x" is forbidden: ValidatingAdmissionPolicy 'krateo-credential-use-namespace' with binding 'krateo-credential-use-namespace' denied request: failed to configure binding: no params found for policy binding with ` + "`Deny`" + ` parameterNotFoundAction`
	if !policyParamsMissing(errors.New(msg)) {
		t.Error("the missing-parameters message isn't recognised")
	}
	for _, other := range []string{
		`configmaps "cu-index-x" is forbidden: ValidatingAdmissionPolicy 'other' with binding 'other' denied request: failed to configure binding: no params found`,
		`configmaps "cu-index-x" is forbidden: ValidatingAdmissionPolicy 'krateo-credential-use-namespace' with binding 'b' denied request: only cu-index-*`,
	} {
		if policyParamsMissing(errors.New(other)) {
			t.Errorf("%q is taken for the missing parameters", other)
		}
	}
	ix := testIndex(&memStore{}, nil)
	var seen error
	ix.onPolicyConfig = func(err error) { seen = err }
	if err := ix.writeErr("adding to", errors.New(msg)); !strings.Contains(err.Error(), "cu-policy-params") || seen == nil {
		t.Errorf("writeErr = %v, reported %v", err, seen)
	}
}

// A PerName kind is probed with GETs only, however many names are due
// (§10.3, §13 shape 1).
func TestProbePerNameGetsOnly(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	s := newShared(Clients{}, SharedOptions{}, clk)
	cs := &countingServer{server: &objServer{objs: map[ID]*metav1.PartialObjectMetadata{}}}
	tr := &Tracker{s: s, gk: widgetGK, o: Options{ProbeInterval: time.Second}, missing: newMissingRefs(),
		proto: &protocol{srv: cs}, idx: testIndex(&memStore{}, nil), sink: sinks{enqueue: func(types.NamespacedName) {}}}
	var ids []ID
	for i := range probeGetLimit + 2 {
		ids = append(ids, SecretRef("ns", "s"+strconv.Itoa(i)).ID())
	}
	tr.missing.set(types.NamespacedName{Namespace: "ns", Name: "h"}, ids, clk.Now(), time.Second)
	clk.Step(2 * time.Second)
	tr.probe(context.Background())
	for _, c := range cs.calls {
		if strings.HasPrefix(c, "list") {
			t.Errorf("a PerName kind was listed: %v", cs.calls)
		}
	}
	if len(cs.calls) != len(ids) {
		t.Errorf("calls = %v, want a GET per name", cs.calls)
	}
}

// fakeLines is a lineKeeper that records what the acquirer asks of it, and
// fails postCheck while failPost is set.
type fakeLines struct {
	ensured, checked, unknown []ID
	failPost                  bool
}

func (f *fakeLines) unknownA2(_ context.Context, id ID, _ string) { f.unknown = append(f.unknown, id) }

func (f *fakeLines) covers(gk schema.GroupKind) bool { return gk == secretGroupKind }
func (f *fakeLines) ensure(_ context.Context, id ID) error {
	f.ensured = append(f.ensured, id)
	return nil
}
func (f *fakeLines) postCheck(_ context.Context, id ID) error {
	f.checked = append(f.checked, id)
	if f.failPost {
		return errors.New("the index can't be read")
	}
	return nil
}

// steadyRig is a rig whose holder h has held Secret s, and whose next pass
// skips A2 (as in TestAcquireSkipsInTheSteadyState), with lines kept by l.
func steadyRig(t *testing.T, l *fakeLines) (*rig, []Ref) {
	t.Helper()
	r := newRig(t)
	r.p.lines = l
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", true)
	refs := []Ref{SecretRef("ns", "s")}
	if _, err := r.p.acquire(context.Background(), h, refs); err != nil {
		t.Fatal(err)
	}
	r.views = cacheOf{m: r.m}
	current := r.get(t, widgetKey("ns", "h"))
	r.p.ver.holderEvent(current.UID, current, r.now)
	r.tick(time.Millisecond)
	if _, err := r.p.acquire(context.Background(), current, refs); err != nil {
		t.Fatal(err)
	}
	return r, refs
}

// The acquirer adds the line before every A2, checks it after, and checks
// it on a pass that skips A2 too (§10.6.2).
func TestASkipStillChecksTheLine(t *testing.T) {
	l := &fakeLines{}
	r, refs := steadyRig(t, l)
	s := SecretRef("ns", "s").ID()
	if !slices.Equal(l.ensured, []ID{s, s}) || !slices.Equal(l.checked, []ID{s, s}) {
		t.Fatalf("two passes with A2: ensured %v, checked %v", l.ensured, l.checked)
	}
	l.ensured, l.checked = nil, nil
	r.srv.calls = nil
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), refs)
	if err != nil || len(a.Held) != 1 || len(r.srv.calls) != 0 {
		t.Fatalf("the steady pass: %+v, %v, calls %v; want a skip", a, err, r.srv.calls)
	}
	if !slices.Equal(l.ensured, []ID{s}) || len(l.checked) != 0 {
		t.Errorf("a skipping pass: ensured %v, checked %v; want the line ensured, and no check", l.ensured, l.checked)
	}
}

// A check after A2 that fails fails the hold, and drops the A2's written
// entry, so that the next pass writes A2 and checks again rather than
// skipping (§10.6.3).
func TestAFailedLineCheckDropsTheWrittenEntry(t *testing.T) {
	l := &fakeLines{}
	r, refs := steadyRig(t, l)
	sUID := r.get(t, secretKey("ns", "s")).UID
	if !r.p.ver.hasWritten(sUID, "ns") {
		t.Fatal("no written entry after the steady passes")
	}
	// A pass that writes A2, from a version the informer hasn't delivered,
	// whose check fails.
	if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, []byte(`{"metadata":{"labels":{"x":"1"}}}`)); err != nil {
		t.Fatal(err)
	}
	l.failPost = true
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), refs)
	if err != nil || !a.Degraded || a.Err == nil || !strings.Contains(a.Err.Error(), "index line") {
		t.Fatalf("a failed check: %+v, %v; want Degraded", a, err)
	}
	if r.p.ver.hasWritten(sUID, "ns") {
		t.Error("the written entry survived a failed check")
	}
}

// A release batch reports as Bare what it read, live, without a finalizer
// of the kind, or gone: in R1, or in R3's response; never an object that
// still carries one for another namespace (§10.6.2).
func TestReleaseReportsBare(t *testing.T) {
	r := newRig(t)
	must(r.m.Create(nsKey("other"), metav1.ObjectMeta{}))
	fOther, _ := FinalizerName(widgetGK, "other")
	held := func(fs ...string) metav1.ObjectMeta {
		ann := map[string]string{}
		for _, f := range fs {
			ann[f] = "e"
		}
		return metav1.ObjectMeta{Finalizers: fs, Annotations: ann, Labels: map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"}}
	}
	must(r.m.Create(secretKey("ns", "released"), held(fNS)))
	must(r.m.Create(secretKey("ns", "shared"), held(fNS, fOther)))
	must(r.m.Create(secretKey("ns", "bare"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("ns", "other-only"), held(fOther)))
	ids := []ID{SecretRef("ns", "released").ID(), SecretRef("ns", "shared").ID(), SecretRef("ns", "bare").ID(), SecretRef("ns", "gone").ID(), SecretRef("ns", "other-only").ID()}
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil {
		t.Fatal(err)
	}
	want := []ID{SecretRef("ns", "bare").ID(), SecretRef("ns", "gone").ID(), SecretRef("ns", "released").ID()}
	if got := SortedIDs(res.Bare); !slices.Equal(got, want) {
		t.Errorf("Bare = %v, want %v", got, want)
	}
}

// A line is prunable only for a PerName kind, and while no record names
// its object (§10.6.2, A14).
func TestIndexPrunable(t *testing.T) {
	named, free := SecretRef("ns", "named").ID(), SecretRef("ns", "free").ID()
	other := ID{Group: "x.io", Kind: "Target", Namespace: "ns", Name: "t"}
	ix := testIndex(&memStore{}, nil)
	ix.recorded = func(id ID) bool { return id == named }
	if got := ix.prunable([]ID{named, free, other}); !slices.Equal(got, []ID{free}) {
		t.Errorf("prunable = %v, want %v", got, []ID{free})
	}
}

// listOnly is a ListWatch the reflector lists with a List, not a watch-list
// stream.
type listOnly struct{ *cache.ListWatch }

func (listOnly) IsWatchListSemanticsUnSupported() bool { return true }

// syncedInformer is a started informer over an empty list, which syncs at
// once; with block, its list waits until block is closed.
func syncedInformer(t *testing.T, block chan struct{}) cache.SharedIndexInformer {
	t.Helper()
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			if block != nil {
				select {
				case <-block:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &metav1.PartialObjectMetadataList{}, nil
		},
		WatchFuncWithContext: func(context.Context, metav1.ListOptions) (watch.Interface, error) {
			return watch.NewFake(), nil
		},
	}
	inf := cache.NewSharedIndexInformer(listOnly{lw}, &metav1.PartialObjectMetadata{}, 0, cache.Indexers{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go inf.RunWithContext(ctx)
	return inf
}

// HasSynced waits on the index informer too, in PerName mode: the
// controller's workers start only once the index has been seen (§7.5).
func TestHasSyncedWaitsOnTheIndex(t *testing.T) {
	block := make(chan struct{})
	tr := &Tracker{holders: syncedInformer(t, nil), crd: syncedInformer(t, nil),
		idx: &index{inf: syncedInformer(t, block)}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !cache.WaitForCacheSync(ctx.Done(), tr.holders.HasSynced, tr.crd.HasSynced) {
		t.Fatal("the holder and CRD informers didn't sync")
	}
	time.Sleep(100 * time.Millisecond)
	if tr.HasSynced() {
		t.Fatal("HasSynced before the index informer synced")
	}
	close(block)
	deadline := time.Now().Add(10 * time.Second)
	for !tr.HasSynced() {
		if time.Now().After(deadline) {
			t.Fatal("HasSynced stays false once the index informer synced")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A per-name watch that stops takes its counts off the held gauge, and a
// count its handler makes after the stop doesn't reach it (§12).
func TestAStoppedPerNameWatchLeavesTheGauge(t *testing.T) {
	m := &countingMetrics{}
	tr := &Tracker{gk: widgetGK, heldBy: map[countKey]int{}, waitingBy: map[countKey]int{}, sink: sinks{metrics: m}}
	tr.names = newPerNames(tr)
	label := &protectedInformer{gk: secretGroupKind}
	tr.setCounts(label.countKey(), 1, 0, nil)
	w := &nameWatch{p: &protectedInformer{gk: secretGroupKind, ns: "ns", name: "s"}, stop: func() {}}
	tr.setCounts(w.p.countKey(), 2, 1, w)
	if n, _ := m.heldGauge(kindLabel(widgetGK)); n != 3 {
		t.Fatalf("held gauge %d, want 3", n)
	}
	tr.names.parent = context.Background()
	tr.names.by[ID{Kind: "Secret", Namespace: "ns", Name: "s"}] = w
	tr.names.follow(nil)
	if n, _ := m.heldGauge(kindLabel(widgetGK)); n != 1 {
		t.Errorf("held gauge %d after the watch stopped, want 1", n)
	}
	if got := m.waitingGauge(kindLabel(widgetGK)); got != 0 {
		t.Errorf("waiting gauge %d after the watch stopped, want 0", got)
	}
	tr.setCounts(w.p.countKey(), 5, 0, w)
	if n, _ := m.heldGauge(kindLabel(widgetGK)); n != 1 {
		t.Errorf("held gauge %d after a late count from the stopped watch, want 1", n)
	}
}

// A post-check that finds the line gone and can't add it, the index being
// full, queues the object: once there is room, the write loops' live check
// restores the line, since P carries the kind's prefix (§10.6.2).
func TestAFullPostCheckIsRetriedByTheWriteLoops(t *testing.T) {
	p := SecretRef("ns", "held").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{p: heldObj("held")}}
	var others []ID
	for i := range 5 {
		others = append(others, SecretRef("ns", "o"+strconv.Itoa(i)).ID())
	}
	st := &memStore{exists: true, lines: linesOf(others)}
	ix := testIndex(st, srv)
	if err := ix.postCheck(context.Background(), p); !errors.Is(err, errIndexFull) {
		t.Fatalf("the post-check on a full index = %v, want errIndexFull", err)
	}
	if ix.pending[p] == nil {
		t.Fatalf("the failed post-check queued nothing: %v", ix.pending)
	}
	now := time.Now()
	if err := ix.recheckTick(context.Background(), now); err == nil {
		t.Error("a re-add into a full index is no error")
	}
	if slices.Contains(st.lines, p.String()) {
		t.Fatal("the line went into a full index")
	}
	st.lines = st.lines[1:]
	if err := ix.recheckTick(context.Background(), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(st.lines, p.String()) || len(ix.pending) != 0 {
		t.Errorf("after room was freed: lines %q, pending %v", st.lines, ix.pending)
	}
}

// Only a completed rebuild sets the marker, in its last merge's write, even
// when that merge adds no line; an add, a create by an add, and a rebuild
// that fails leave it absent (§10.6.6).
func TestTheRebuiltMarker(t *testing.T) {
	m := apimodel.New(time.Now)
	seed(m, "ns", "s", []string{"h"}, true)
	srv := nsServer{modelServer{m: m}, "ns"}
	st := &memStore{}
	ix := testIndex(st, srv)
	if err := ix.add(context.Background(), []ID{SecretRef("ns", "other").ID()}); err != nil || st.rebuilt {
		t.Fatalf("a create by an add: %v, marker %v", err, st.rebuilt)
	}
	if err := ix.add(context.Background(), []ID{SecretRef("ns", "third").ID()}); err != nil || st.rebuilt {
		t.Fatalf("a merge by an add: %v, marker %v", err, st.rebuilt)
	}
	failing := testIndex(st, failingList{srv})
	if err := failing.rebuild(context.Background()); err == nil || st.rebuilt {
		t.Fatalf("a failed rebuild: %v, marker %v", err, st.rebuilt)
	}
	if err := ix.rebuild(context.Background()); err != nil || !st.rebuilt || !slices.Contains(st.lines, SecretRef("ns", "s").ID().String()) {
		t.Fatalf("a rebuild: %v, marker %v, lines %q", err, st.rebuilt, st.lines)
	}
	// A rebuild with nothing new still writes the marker on an index
	// without it.
	st2 := &memStore{exists: true, lines: []string{SecretRef("ns", "s").ID().String()}}
	if err := testIndex(st2, srv).rebuild(context.Background()); err != nil || !st2.rebuilt {
		t.Errorf("a rebuild that adds no line: %v, marker %v", err, st2.rebuilt)
	}
	if !testIndex(st, srv).needsRebuild(indexVersion{}) || testIndex(st, srv).needsRebuild(indexVersion{rebuilt: true}) {
		t.Error("needsRebuild doesn't follow the marker")
	}
}

// While Disown is on, a rebuild can't tell that the records still name every
// hold, so it merges the lines and leaves the marker off, unless the kind
// was never tracked before; the rebuild after the switch sets it. A switch
// that can't be read fails the rebuild (§7.4, §10.6.6).
func TestARebuildUnderDisownLeavesNoMarker(t *testing.T) {
	ctx := context.Background()
	m := apimodel.New(time.Now)
	seed(m, "ns", "s", []string{"h"}, true)
	srv := nsServer{modelServer{m: m}, "ns"}
	line := SecretRef("ns", "s").ID().String()
	st := &memStore{}
	ix := testIndex(st, srv)
	on, fail := true, error(nil)
	ix.disowning = func(context.Context) (bool, error) { return on, fail }
	fail = errors.New("the CRD can't be read")
	if err := ix.rebuild(ctx); !errors.Is(err, fail) || st.exists {
		t.Errorf("a rebuild that can't read the switch = %v, index created %v", err, st.exists)
	}
	fail = nil
	if err := ix.rebuild(ctx); err != nil || st.rebuilt || !slices.Contains(st.lines, line) {
		t.Errorf("a rebuild under the switch: %v, marker %v, lines %q; want the line and no marker", err, st.rebuilt, st.lines)
	}
	ix.lost = false
	ix.saw(indexVersion{})
	if ix.switchChanged(); !ix.lost {
		t.Error("a switch that changes over an index without the marker asks for no rebuild")
	}
	on = false
	if err := ix.rebuild(ctx); err != nil || !st.rebuilt {
		t.Errorf("a rebuild after the switch: %v, marker %v", err, st.rebuilt)
	}
	ix.lost = false
	ix.saw(indexVersion{rebuilt: true})
	if ix.switchChanged(); ix.lost {
		t.Error("a switch that changes over a rebuilt index asks for a rebuild")
	}

	// A kind never tracked before had no hold; once rebuilt, a loss is one.
	on = true
	st = &memStore{}
	ix = testIndex(st, srv)
	ix.disowning = func(context.Context) (bool, error) { return on, nil }
	ix.setFresh(true)
	if err := ix.rebuild(ctx); err != nil || !st.rebuilt || ix.isFresh() {
		t.Errorf("the first rebuild of a kind never tracked: %v, marker %v, still fresh %v", err, st.rebuilt, ix.isFresh())
	}
	st.exists, st.rebuilt, st.lines = false, false, nil
	if err := ix.rebuild(ctx); err != nil || st.rebuilt {
		t.Errorf("a rebuild after it was lost: %v, marker %v; want none", err, st.rebuilt)
	}
}

// An index names every hold once a rebuild completed, or an operator
// vouched for that very ConfigMap (§7.4).
func TestIndexVersionComplete(t *testing.T) {
	for _, c := range []struct {
		v    indexVersion
		want bool
	}{
		{indexVersion{uid: "a"}, false},
		{indexVersion{}, false}, // no uid vouches for no uid
		{indexVersion{uid: "a", rebuilt: true}, true},
		{indexVersion{uid: "a", vouchedIndex: "a"}, true},
		{indexVersion{uid: "b", vouchedIndex: "a"}, false}, // restored, or lost again
		{indexVersion{uid: "a", vouched: "a"}, false},
	} {
		if got := c.v.complete(); got != c.want {
			t.Errorf("%+v is complete: %v, want %v", c.v, got, c.want)
		}
	}
}

// A pending live check of a kind, or a rebuild that is due, means a line of
// it may be missing (§7.4).
func TestIndexUnsure(t *testing.T) {
	secrets, others := map[schema.GroupKind]bool{secretGroupKind: true}, map[schema.GroupKind]bool{configMapGroupKind: true}
	ix := testIndex(&memStore{}, nil)
	if ix.unsure(secrets) {
		t.Error("unsure with nothing pending")
	}
	ix.queue([]ID{SecretRef("ns", "s").ID()}, false)
	if !ix.unsure(secrets) || ix.unsure(others) {
		t.Errorf("with a Secret's check pending: unsure of Secrets %v, of ConfigMaps %v", ix.unsure(secrets), ix.unsure(others))
	}
	ix = testIndex(&memStore{}, nil)
	ix.markLost()
	if !ix.unsure(secrets) {
		t.Error("sure with a rebuild due")
	}
}

// failingList fails every List, as a rebuild meets an API server that can't
// list the holders.
type failingList struct{ server }

func (failingList) List(context.Context, schema.GroupKind, string) ([]metav1.PartialObjectMetadata, error) {
	return nil, apierrors.NewServiceUnavailable("down")
}

// holdersServer is objServer with holders to list.
type holdersServer struct {
	*objServer
	holders []metav1.PartialObjectMetadata
}

func (s holdersServer) List(context.Context, schema.GroupKind, string) ([]metav1.PartialObjectMetadata, error) {
	return s.holders, nil
}

// An object a record names that can't be read, as a Secret outside a
// resourceNames grant, keeps its line in the rebuild, which still completes
// and sets the marker; the startup recheck goes on through every line past
// it, enqueuing what it can read and pruning what is bare, and returns the
// error so that it is retried (§10.6.3, §10.6.5; the reviewer's probe of
// the marker).
func TestRecheckWithAnUnreadableRecordedObject(t *testing.T) {
	unread, held, bare := SecretRef("ns", "a-unread").ID(), SecretRef("ns", "b-held").ID(), SecretRef("ns", "c-bare").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("b-held"), bare: {ObjectMeta: metav1.ObjectMeta{Name: "c-bare"}}}, fail: map[ID]bool{unread: true}}
	rec, _ := EncodeRecord([]ID{unread})
	h := metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "h", Annotations: map[string]string{AnnotationRefs: rec}}}
	st := &memStore{exists: true, lines: linesOf([]ID{held, bare}), rv: 1}
	ix := testIndex(st, holdersServer{srv, []metav1.PartialObjectMetadata{h}})
	var logged []ID
	ix.onUnreadable = func(ids []ID) { logged = append(logged, ids...) }
	var enqueued []ID
	err := ix.recheck(context.Background(), func(id ID, n string) { enqueued = append(enqueued, id) })
	if err == nil {
		t.Error("a line that can't be read is no error: the recheck isn't retried")
	}
	if !slices.Equal(enqueued, []ID{held}) {
		t.Errorf("enqueued %v, want %v", enqueued, []ID{held})
	}
	if !st.rebuilt {
		t.Error("the rebuild didn't complete and set the marker")
	}
	if want := linesOf([]ID{unread, held}); !slices.Equal(st.lines, want) {
		t.Errorf("lines %q, want %q: the unreadable one kept, the bare one pruned", st.lines, want)
	}
	if !slices.Equal(logged, []ID{unread}) {
		t.Errorf("reported unreadable %v, want %v", logged, []ID{unread})
	}
}

// rbacForbidden is the authorizer's refusal of a GET of Secret ns/name, as
// the API server writes it: class (d) of §6.2.
func rbacForbidden(ns, name string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name, fmt.Errorf(
		`User "system:serviceaccount:default:provider" cannot get resource "secrets" in API group "" in the namespace %q`, ns))
}

// A line whose object the authorizer refuses to read (an RBAC 403, class
// (d)) is kept, reported once, and isn't an error: the recheck succeeds,
// since retrying can't help until RBAC changes. A policy's 403, and a
// transient error, still make it retry. A live check that meets an RBAC 403
// puts the line back and leaves nothing pending (§10.6.3, §10.6.5).
func TestADeniedLineIsReportedOnceAndNotRetried(t *testing.T) {
	denied, held := SecretRef("ns", "a-denied").ID(), SecretRef("ns", "b-held").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("b-held")}, forbid: map[ID]bool{denied: true}}
	st := &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{denied, held}), rv: 1}
	ix := testIndex(st, srv)
	var reported []ID
	ix.onDenied = func(id ID, _ error) { reported = append(reported, id) }
	var enqueued []ID
	for range 2 {
		if err := ix.recheck(context.Background(), func(id ID, _ string) { enqueued = append(enqueued, id) }); err != nil {
			t.Fatalf("the recheck with a denied line = %v, want no error", err)
		}
	}
	if !slices.Equal(enqueued, []ID{held, held}) || !slices.Equal(reported, []ID{denied}) {
		t.Errorf("enqueued %v, reported %v; want the held one each time, and the denied one reported once", enqueued, reported)
	}
	if !slices.Contains(st.lines, denied.String()) {
		t.Errorf("the denied line went: %q", st.lines)
	}
	if c := Classify(rbacForbidden("ns", "a-denied")); c != ClassRBAC {
		t.Fatalf("the test's RBAC 403 classifies as %v", c)
	}
	srv.fail = map[ID]bool{held: true}
	if err := ix.recheck(context.Background(), func(ID, string) {}); err == nil {
		t.Error("a transient error is no error: the recheck isn't retried")
	}
	srv.fail, srv.policy = nil, map[ID]bool{held: true}
	if err := ix.recheck(context.Background(), func(ID, string) {}); err == nil {
		t.Error("a policy's 403 is no error: the recheck isn't retried")
	}
	if !slices.Equal(reported, []ID{denied}) {
		t.Errorf("reported %v; a policy's 403 was reported as RBAC's", reported)
	}
	srv.policy = nil

	// A live check: the line goes back, and nothing stays pending.
	st.lines = nil
	ix.queue([]ID{denied}, true)
	if err := ix.recheckTick(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(st.lines, denied.String()) || len(ix.pending) != 0 {
		t.Errorf("after a denied live check: lines %q, pending %v", st.lines, ix.pending)
	}
}

// A dropped line whose object this process can't read (an RBAC 403) is
// restored, and reported as such: not as tampering, since whether it carries
// the kind's prefix is unknown (§10.6.3, §10.6.6).
func TestARestoredUnreadableLineIsNotTampering(t *testing.T) {
	denied, held := SecretRef("ns", "denied").ID(), SecretRef("ns", "held").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held")}, forbid: map[ID]bool{denied: true}}
	st := &memStore{exists: true}
	ix := testIndex(st, srv)
	var tampered, restored []ID
	ix.onTampered = func(id ID) { tampered = append(tampered, id) }
	ix.onRestoredUnread = func(id ID) { restored = append(restored, id) }
	ix.queue([]ID{denied, held}, true)
	if err := ix.recheckTick(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{denied, held}); !slices.Equal(st.lines, want) {
		t.Errorf("lines %q, want both restored", st.lines)
	}
	if !slices.Equal(tampered, []ID{held}) || !slices.Equal(restored, []ID{denied}) {
		t.Errorf("tampered %v, restored unreadable %v; want %v and %v", tampered, restored, []ID{held}, []ID{denied})
	}
}

// A refusal is reported again after the object was readable in between: a
// grant restored and then revoked again is counted twice.
func TestARefusalAfterAReadIsReportedAgain(t *testing.T) {
	id := SecretRef("ns", "s").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: heldObj("s")}, forbid: map[ID]bool{id: true}}
	ix := testIndex(&memStore{exists: true, rebuilt: true, lines: linesOf([]ID{id})}, srv)
	reports := 0
	ix.onDenied = func(ID, error) { reports++ }
	for _, forbid := range []bool{true, true, false, true} {
		srv.forbid[id] = forbid
		if err := ix.recheck(context.Background(), func(ID, string) {}); err != nil {
			t.Fatal(err)
		}
	}
	if reports != 2 {
		t.Errorf("%d reports for refused, refused, readable, refused; want 2", reports)
	}
	reports = 0
	ix.deniedSeen = nil
	for _, forbid := range []bool{true, false, true} {
		srv.forbid[id] = forbid
		_ = ix.carries(context.Background(), id)
	}
	if reports != 2 {
		t.Errorf("%d reports from live checks for refused, readable, refused; want 2", reports)
	}
}

// The vouched annotation is read tolerantly: blanks trimmed, a malformed
// entry ignored and reported once per value (§10.6.6).
func TestParseVouched(t *testing.T) {
	ix := testIndex(&memStore{}, nil)
	var reports [][]string
	ix.onVouchMalformed = func(e []string) { reports = append(reports, e) }
	v := " team-a/Secret/nogrant, ,/Cluster.x.io/c,nonsense, team-b/KubernetesTarget.core.krateo.io/prod ,a/b/c/d"
	for range 2 {
		got := ix.parseVouched(v)
		want := map[ID]bool{
			SecretRef("team-a", "nogrant").ID():                                                    true,
			{Group: "x.io", Kind: "Cluster", Name: "c"}:                                            true,
			{Group: "core.krateo.io", Kind: "KubernetesTarget", Namespace: "team-b", Name: "prod"}: true,
		}
		if len(got) != len(want) {
			t.Fatalf("parsed %v, want %v", got, want)
		}
		for id := range want {
			if !got[id] {
				t.Errorf("%v missing from %v", id, got)
			}
		}
	}
	if len(reports) != 1 || !slices.Equal(reports[0], []string{"nonsense", "a/b/c/d"}) {
		t.Errorf("reports %q, want the two malformed entries once", reports)
	}
	if got, ok := parseUnheldRef(unheldRef(SecretRef("ns", "s").ID())); !ok || got != SecretRef("ns", "s").ID() {
		t.Errorf("parseUnheldRef(unheldRef) = %v, %v", got, ok)
	}
}

// A vouched object, while it can't be read, is skipped: the startup recheck
// doesn't read it, its dropped line isn't restored, a prune may remove it,
// and a rebuild doesn't add it. Every other unreadable line keeps the
// superset's rule. A vouched object found carrying the prefix is treated as
// any other, and the wrong vouch reported (§7.4, §10.6.6).
func TestAVouchedLine(t *testing.T) {
	vouched, unvouched, carrying := SecretRef("ns", "vouched").ID(), SecretRef("ns", "unvouched").ID(), SecretRef("ns", "carrying").ID()
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{carrying: heldObj("carrying")}, forbid: map[ID]bool{vouched: true, unvouched: true}}
	annotation := unheldRef(vouched) + "," + unheldRef(carrying)
	st := &memStore{exists: true, rebuilt: true, vouched: annotation, lines: linesOf([]ID{vouched})}
	ix := testIndex(st, srv)
	var wrong []ID
	ix.onVouchWrong = func(id ID) { wrong = append(wrong, id) }

	// The startup recheck reads it, and, unreadable, skips it: no report,
	// no error, its line kept. A vouched object that can be read and
	// carries the prefix gets its batch, and the wrong vouch is reported.
	var denials []ID
	ix.onDenied = func(id ID, _ error) { denials = append(denials, id) }
	st.lines = linesOf([]ID{vouched, carrying})
	var enqueued []ID
	if err := ix.recheck(context.Background(), func(id ID, _ string) { enqueued = append(enqueued, id) }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(enqueued, []ID{carrying}) || len(denials) != 0 || !slices.Equal(wrong, []ID{carrying}) {
		t.Errorf("the recheck: enqueued %v, denials %v, wrong vouches %v; want %v enqueued and reported wrong, nothing denied", enqueued, denials, wrong, []ID{carrying})
	}
	if !slices.Contains(st.lines, vouched.String()) {
		t.Errorf("the recheck dropped the unreadable vouched line: %q", st.lines)
	}
	wrong = nil
	ix.vouchWrongSeen = nil

	// A dropped line: the vouched one stays dropped, the unvouched
	// unreadable one and the vouched one that carries come back.
	st.lines = nil
	ix.queue([]ID{vouched, unvouched, carrying}, true)
	if err := ix.recheckTick(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{unvouched, carrying}); !slices.Equal(st.lines, want) {
		t.Errorf("after the live checks: %q, want %q", st.lines, want)
	}
	if !slices.Equal(wrong, []ID{carrying}) || len(ix.pending) != 0 {
		t.Errorf("wrong vouches %v, pending %v", wrong, ix.pending)
	}

	// A prune removes the vouched line for good; the unvouched one comes
	// back.
	st.lines = linesOf([]ID{vouched, unvouched})
	if err := ix.prune(context.Background(), []ID{vouched, unvouched}); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{unvouched}); !slices.Equal(st.lines, want) {
		t.Errorf("after the prune: %q, want %q", st.lines, want)
	}

	// A rebuild doesn't add the vouched object a record names.
	rec, _ := EncodeRecord([]ID{vouched, unvouched})
	h := metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "h", Annotations: map[string]string{AnnotationRefs: rec}}}
	st2 := &memStore{exists: true, vouched: annotation}
	if err := testIndex(st2, holdersServer{srv, []metav1.PartialObjectMetadata{h}}).rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := linesOf([]ID{unvouched}); !slices.Equal(st2.lines, want) {
		t.Errorf("after the rebuild: %q, want %q", st2.lines, want)
	}
}

// The index's own writes keep the annotations an operator set, the vouched
// list among them (§10.6.6).
func TestIndexWritesPreserveTheVouch(t *testing.T) {
	cm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "cu-index-x", "namespace": "idx", "annotations": map[string]any{AnnotationVouched: "ns/Secret/s"}},
		"data":     map[string]any{indexDataKey: ""},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cm)
	st := newConfigMapStore(dyn, "idx", "cu-index-x")
	ix := testIndex(st, nil)
	if err := ix.add(context.Background(), []ID{SecretRef("ns", "a").ID()}); err != nil {
		t.Fatal(err)
	}
	if err := ix.merge(context.Background(), nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := ix.remove(context.Background(), []string{SecretRef("ns", "a").ID().String()}); err != nil {
		t.Fatal(err)
	}
	v, err := st.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v.vouched != "ns/Secret/s" || !v.rebuilt {
		t.Errorf("after the writes: vouched %q, rebuilt %v", v.vouched, v.rebuilt)
	}
}
