package credentialuse

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}

func (s *memStore) get(context.Context) (indexVersion, error) {
	if s.failGet != nil {
		return indexVersion{}, s.failGet
	}
	if !s.exists {
		return indexVersion{}, apierrors.NewNotFound(configMapsGR, "cu-index")
	}
	return indexVersion{lines: slices.Clone(s.lines), rv: strconv.Itoa(s.rv)}, nil
}

func (s *memStore) update(_ context.Context, cur indexVersion, lines []string) (indexVersion, error) {
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
	s.updates++
	return indexVersion{lines: slices.Clone(lines), rv: strconv.Itoa(s.rv)}, nil
}

func (s *memStore) create(_ context.Context, lines []string) (indexVersion, error) {
	if f := s.beforeCreate; f != nil {
		f(s)
	}
	if s.exists {
		return indexVersion{}, apierrors.NewAlreadyExists(configMapsGR, "cu-index")
	}
	s.exists, s.lines, s.rv = true, slices.Clone(lines), s.rv+1
	return indexVersion{lines: slices.Clone(lines), rv: strconv.Itoa(s.rv)}, nil
}

// objServer serves Get from a map of objects, and fails the names in fail.
type objServer struct {
	server
	objs map[ID]*metav1.PartialObjectMetadata
	fail map[ID]bool
	gets []ID
}

func (s *objServer) Get(_ context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	id := ID{Group: gk.Group, Kind: gk.Kind, Namespace: ns, Name: name}
	s.gets = append(s.gets, id)
	if s.fail[id] {
		return nil, apierrors.NewServiceUnavailable("down")
	}
	if o := s.objs[id]; o != nil {
		return o, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
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
	long := ID{Kind: "Secret", Namespace: "ns", Name: strings.Repeat("n", 250)}
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

// A drop is someone else's unless this process removed the line; a drop
// whose R3 the per-name cache shows needs no check (§10.6.6, L5).
func TestIndexObservesDrops(t *testing.T) {
	mine, theirs, released := SecretRef("ns", "mine").ID(), SecretRef("ns", "theirs").ID(), SecretRef("ns", "released").ID()
	ix := testIndex(&memStore{}, nil)
	ix.released = func(id ID) bool { return id == released }
	old := indexVersion{lines: linesOf([]ID{mine, theirs, released, SecretRef("ns", "kept").ID()})}
	ix.notePruned([]string{mine.String()}, 1)
	ix.observed(&old, indexVersion{lines: linesOf([]ID{SecretRef("ns", "kept").ID()})})
	if got := keysOf(ix.pending); len(got) != 1 || got[0] != theirs {
		t.Errorf("pending = %v, want only %v", got, theirs)
	}
	if !ix.pending[theirs].tampered {
		t.Error("a drop by someone else isn't marked as tampering")
	}
	if len(ix.pruned) != 0 {
		t.Errorf("this process's drop is still expected: %v", ix.pruned)
	}
	// A line this process removed, back since, is someone else's to drop.
	ix.notePruned([]string{mine.String()}, 1)
	ix.observed(nil, indexVersion{lines: linesOf([]ID{mine})})
	ix.observed(&indexVersion{lines: linesOf([]ID{mine})}, indexVersion{})
	if ix.pending[mine] == nil {
		t.Error("a line removed, put back, then dropped by someone else isn't checked")
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
	ensured, checked []ID
	failPost         bool
}

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
