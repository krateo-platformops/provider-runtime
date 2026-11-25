package credentialuse

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	testingclock "k8s.io/utils/clock/testing"
)

// An A2 whose outcome its sender doesn't learn, and one its caller's stop
// would cut short (§6.2, §7.2, §10.6.2).

// a2Server is a server whose A2s go through on.
type a2Server struct {
	server
	on func(ctx context.Context, name string, next func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error)
}

func (s a2Server) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	next := func() (*metav1.PartialObjectMetadata, error) { return s.server.Patch(ctx, gk, ns, name, pt, data) }
	if gk != secretGroupKind || pt != types.StrategicMergePatchType {
		return next()
	}
	return s.on(ctx, name, next)
}

// ctxLines is a fakeLines that keeps the state of the contexts it was given,
// and, with hang, waits for them to end.
type ctxLines struct {
	fakeLines
	hang bool
	errs []error
}

func (l *ctxLines) seen(ctx context.Context) {
	if l.hang {
		<-ctx.Done()
	}
	l.errs = append(l.errs, ctx.Err())
}

func (l *ctxLines) postCheck(ctx context.Context, id ID) error {
	l.seen(ctx)
	return l.fakeLines.postCheck(ctx, id)
}

func (l *ctxLines) unknownA2(ctx context.Context, id ID, n string) {
	l.seen(ctx)
	l.fakeLines.unknownA2(ctx, id, n)
}

// lateRig is a rig with Secrets a and b, whose A2s go through on, and the
// batches enqueued.
func lateRig(t *testing.T, on func(ctx context.Context, name string, next func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error)) (*rig, *ctxLines, *[]ID) {
	t.Helper()
	r := newRig(t)
	must(r.m.Create(secretKey("ns", "a"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("ns", "b"), metav1.ObjectMeta{}))
	l := &ctxLines{}
	r.p.lines = l
	r.p.srv = a2Server{r.srv, on}
	var batches []ID
	r.p.enqueueBatch = func(id ID, _ string) { batches = append(batches, id) }
	return r, l, &batches
}

func heldIn(t *testing.T, r *rig, name string) bool {
	t.Helper()
	prefix, _ := FinalizerPrefix(widgetGK)
	return countPrefix(r.get(t, secretKey("ns", name)).Finalizers, prefix) > 0
}

// A caller's stop in the middle of an A2 doesn't cut it short: it lands, and
// its line is checked; no other A2 of the call begins.
func TestAnA2OutlivesItsCaller(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	r, l, batches := lateRig(t, func(wctx context.Context, _ string, next func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
		stop()
		time.Sleep(20 * time.Millisecond) // whatever follows the cancellation has run
		if err := wctx.Err(); err != nil {
			return nil, err
		}
		return next()
	})
	a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{SecretRef("ns", "a"), SecretRef("ns", "b")})
	if err != nil || a.Err == nil {
		t.Fatalf("acquire = %+v, %v; want the stop's error for the second ref", a, err)
	}
	if !heldIn(t, r, "a") || heldIn(t, r, "b") {
		t.Errorf("held: a %v, b %v; want the A2 in flight to land, and no other to begin", heldIn(t, r, "a"), heldIn(t, r, "b"))
	}
	if !slices.Equal(l.checked, []ID{SecretRef("ns", "a").ID()}) || !slices.Equal(l.errs, []error{nil}) {
		t.Errorf("checked %v, with its context's state %v; want a, under a live context", l.checked, l.errs)
	}
	if len(l.unknown) != 0 || len(*batches) != 0 {
		t.Errorf("an A2 that landed is of unknown outcome: %v, %v", l.unknown, *batches)
	}
}

// An A2 keeps its caller's deadline; without one, the protocol's bound.
func TestAnA2KeepsItsCallersDeadline(t *testing.T) {
	for _, withDeadline := range []bool{true, false} {
		var got time.Time
		var ok bool
		r, _, _ := lateRig(t, func(wctx context.Context, _ string, next func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
			got, ok = wctx.Deadline()
			return next()
		})
		r.p.a2Bound = time.Hour
		ctx, want := context.Background(), time.Now().Add(time.Hour)
		if withDeadline {
			want = time.Now().Add(time.Minute)
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, want)
			defer cancel()
		}
		if _, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{SecretRef("ns", "a")}); err != nil {
			t.Fatal(err)
		}
		if d := got.Sub(want); !ok || withDeadline && d != 0 || d < 0 || d > time.Minute {
			t.Errorf("with a deadline %v: the A2's is %s from %s (set: %v)", withDeadline, got, want, ok)
		}
	}
}

// With an API server that hangs, a call returns callGrace after its caller's
// context ended at the latest, its A2 and its check included, however many
// refs it has: Acquire and BeforeEstablish alike.
func TestACallReturnsThoughTheServerHangs(t *testing.T) {
	const a2, call = 100 * time.Millisecond, 200 * time.Millisecond
	calls := map[string]func(r *rig, ctx context.Context, h *metav1.PartialObjectMetadata) error{
		"Acquire": func(r *rig, ctx context.Context, h *metav1.PartialObjectMetadata) error {
			got, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "a"), SecretRef("ns", "b")})
			return errors.Join(err, got.Err)
		},
		"BeforeEstablish": func(r *rig, ctx context.Context, h *metav1.PartialObjectMetadata) error {
			return r.p.beforeEstablish(ctx, h, Acquired{Unheld: []Unheld{
				{Ref: SecretRef("ns", "a"), Reason: ReasonMissing}, {Ref: SecretRef("ns", "b"), Reason: ReasonMissing}}})
		},
	}
	for name, do := range calls {
		ctx, stop := context.WithCancel(context.Background())
		var ended time.Time
		r, l, batches := lateRig(t, func(wctx context.Context, _ string, _ func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
			ended = time.Now()
			stop()
			<-wctx.Done()
			return nil, wctx.Err()
		})
		r.p.a2Grace, r.p.callGrace, l.hang = a2, call, true
		if err := do(r, ctx, r.holder(t, "h", true)); err == nil {
			t.Errorf("%s: no error", name)
		}
		if d := time.Since(ended); d < a2 || d > call+time.Second {
			t.Errorf("%s returned %s after its context ended, want from %s to %s", name, d, a2, call)
		}
		if id := SecretRef("ns", "a").ID(); !slices.Equal(l.unknown, []ID{id}) || !slices.Equal(*batches, []ID{id}) {
			t.Errorf("%s: of unknown outcome %v, batches %v; want a's", name, l.unknown, *batches)
		}
	}
}

// An A2 that may have landed is checked, and its batch enqueued; one the API
// server refused isn't (§10.6.2).
func TestAnA2OfUnknownOutcomeIsChecked(t *testing.T) {
	gr := schema.GroupResource{Resource: "secrets"}
	for name, tc := range map[string]struct {
		err     error
		unknown bool
	}{
		"the response was lost": {errors.New("EOF"), true},
		"its context ended":     {context.DeadlineExceeded, true},
		"a gateway timeout":     {apierrors.NewTimeoutError("timed out", 1), true},
		"an internal error":     {apierrors.NewInternalError(errors.New("etcd")), true},
		"unavailable":           {apierrors.NewServiceUnavailable("down"), true},
		"a request timeout":     {&apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 408}}, true},
		"not found":             {apierrors.NewNotFound(gr, "a"), false},
		"forbidden":             {rbacForbidden("ns", "a"), false},
		"a conflict":            {apierrors.NewConflict(gr, "a", errors.New("changed")), false},
		"too many requests":     {apierrors.NewTooManyRequests("slow down", 1), false},
		"invalid":               {apierrors.NewBadRequest("no"), false},
	} {
		r, l, batches := lateRig(t, func(context.Context, string, func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
			return nil, tc.err
		})
		if _, err := r.p.acquire(context.Background(), r.holder(t, "h", true), []Ref{SecretRef("ns", "a")}); err != nil {
			t.Fatal(err)
		}
		if got := len(l.unknown) == 1 && len(*batches) == 1; got != tc.unknown || !tc.unknown && len(l.unknown)+len(*batches) != 0 {
			t.Errorf("%s: of unknown outcome %v, batches %v; want unknown %v", name, l.unknown, *batches, tc.unknown)
		}
	}
}

// The index is told of an A2 before its line is ensured, and of its end once
// its line was checked, or its unknown outcome told; whatever came of it, in a
// pass and before establishing (§10.6.2).
func TestTheIndexIsToldOfAnA2(t *testing.T) {
	ctx := context.Background()
	refused := rbacForbidden("ns", "a")
	for name, tc := range map[string]struct {
		err                  error
		failEnsure, failPost bool
		want                 []string
	}{
		"held":                 {want: []string{"holding", "ensure", "check", "done"}},
		"its check fails":      {failPost: true, want: []string{"holding", "ensure", "check", "done"}},
		"its line isn't there": {failEnsure: true, want: []string{"holding", "ensure", "done"}},
		"refused":              {err: refused, want: []string{"holding", "ensure", "done"}},
		"of unknown outcome":   {err: errors.New("EOF"), want: []string{"holding", "ensure", "unknown", "done"}},
	} {
		for _, establishing := range []bool{false, true} {
			r, l, _ := lateRig(t, func(_ context.Context, _ string, next func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
				if tc.err != nil {
					return nil, tc.err
				}
				return next()
			})
			l.failEnsure, l.failPost = tc.failEnsure, tc.failPost
			h := r.holder(t, "h", false)
			if establishing {
				_ = r.p.beforeEstablish(ctx, h, Acquired{Unheld: []Unheld{{Ref: SecretRef("ns", "a"), Reason: ReasonMissing}}})
			} else if _, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "a")}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(l.steps, tc.want) || l.flying[SecretRef("ns", "a").ID()] != 0 {
				t.Errorf("%s, establishing %v: %q, with %v in flight; want %q, and none", name, establishing, l.steps, l.flying, tc.want)
			}
		}
	}
}

// A prune leaves the lines of the A2s this process has in flight, or of
// unknown outcome, those begun during its removal included, and takes them
// later, unless the object is held, named, or can't be read (§10.6.2).
func TestAPruneLeavesWhatMayLand(t *testing.T) {
	ctx := context.Background()
	bare, flying, late, begun, lost := SecretRef("ns", "a-bare").ID(), SecretRef("ns", "b-flying").ID(), SecretRef("ns", "c-late").ID(), SecretRef("ns", "d-begun").ID(), SecretRef("ns", "e-lost").ID()
	all := []ID{bare, flying, late, begun, lost}
	for name, empty := range map[string]bool{"a prune": false, "Disown's emptying": true} {
		srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{}, fail: map[ID]bool{}, forbid: map[ID]bool{}}
		st := &memStore{exists: true, rebuilt: true, lines: linesOf(all), rv: 1}
		ix := testIndex(st, srv)
		now := time.Now()
		ix.now = func() time.Time { return now }
		done := ix.holding(flying)
		doneTwice := ix.holding(flying)
		ix.unknownA2(ctx, late, "ns")
		var removed []string // the lines between the removal and the re-add
		st.beforeUpdate = func(s *memStore) {
			s.beforeUpdate = func(s *memStore) { s.beforeUpdate, removed = nil, slices.Clone(s.lines) }
			ix.holding(begun)
			ix.late[lateHold{lost, "ns"}] = now.Add(ix.margin)
		}
		remove := ix.prune
		if empty {
			remove = ix.empty
		}
		if err := remove(ctx, all); err != nil || !slices.Equal(st.lines, linesOf(all[1:])) {
			t.Fatalf("%s: %v, lines %q; want the bare one's only gone", name, err, st.lines)
		}
		if !slices.Equal(removed, linesOf([]ID{flying, late})) {
			t.Errorf("%s: the removal left %q, want those of the A2s from before it", name, removed)
		}
		// Its outcome known, one of two in flight isn't enough.
		done()
		at := func(d time.Duration, want ...ID) {
			t.Helper()
			if err := ix.deferredTick(ctx, now.Add(d)); err != nil || !slices.Equal(st.lines, linesOf(want)) {
				t.Errorf("%s: after %s: %v, lines %q; want %q", name, d, err, st.lines, linesOf(want))
			}
		}
		at(a2Grace-time.Millisecond, flying, late, begun, lost)
		at(a2Grace, flying, late, begun, lost) // still in flight, so put off again
		doneTwice()
		at(2*a2Grace, late, begun, lost)
		now = now.Add(2 * a2Grace)
		at(0, late, begun, lost)
		// Past the margin: one is held by now, one is named, one can't be read.
		now = now.Add(ix.margin)
		srv.objs[late] = heldObj(late.Name)
		ix.lateTick(now, func(ID, string) {})
		srv.fail[lost] = true
		at(0, late, begun, lost)
		srv.fail[lost] = false
		at(deferRetry-time.Millisecond, late, begun, lost)
		at(deferRetry, late, begun)
	}
}

// The write loops' start keeps every prune back, but Disown's own emptying,
// for as long as an A2 of the process before may land; the lines go then,
// unless they are needed (§10.6.2).
func TestThePrunesStartQuiet(t *testing.T) {
	ctx := context.Background()
	bare, held, named, denied := SecretRef("ns", "bare").ID(), SecretRef("ns", "held").ID(), SecretRef("ns", "named").ID(), SecretRef("ns", "denied").ID()
	all := []ID{bare, held, named, denied}
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{}, forbid: map[ID]bool{}}
	st := &memStore{exists: true, rebuilt: true, lines: linesOf(all), rv: 1}
	ix := testIndex(st, srv)
	now := time.Now()
	ix.now = func() time.Time { return now }
	ix.startQuiet(time.Minute)
	if err := ix.recheck(ctx, func(ID, string) {}); err != nil || st.updates != 0 {
		t.Errorf("the startup recheck: %v, %d updates", err, st.updates)
	}
	now = now.Add(time.Minute - time.Millisecond)
	if err := ix.prune(ctx, all); err != nil || st.updates != 0 {
		t.Errorf("a prune just before the end: %v, %d updates", err, st.updates)
	}
	if err := ix.deferredTick(ctx, now); err != nil || st.updates != 0 {
		t.Errorf("the tick just before the end: %v, %d updates", err, st.updates)
	}
	// Meanwhile one was held, one named, and one can't be read any more.
	now = now.Add(time.Millisecond)
	srv.objs[held], srv.forbid[denied] = heldObj("held"), true
	ix.recorded = func(id ID) bool { return id == named }
	if err := ix.deferredTick(ctx, now); err != nil || !slices.Equal(st.lines, linesOf([]ID{held, named, denied})) || st.updates != 1 || len(ix.deferred) != 0 {
		t.Errorf("at the end: %v, lines %q, %d updates, deferred %v", err, st.lines, st.updates, ix.deferred)
	}
	ix.startQuiet(time.Minute)
	if err := ix.empty(ctx, []ID{named}); err != nil || slices.Contains(st.lines, named.String()) {
		t.Errorf("Disown's emptying: %v, lines %q", err, st.lines)
	}
	// A prune that fails is tried again.
	now = now.Add(time.Minute)
	st.lines = linesOf([]ID{bare})
	ix.deferred[bare] = now
	st.beforeUpdate = func(s *memStore) { s.beforeUpdate, s.exists = nil, false }
	if err := ix.deferredTick(ctx, now); err == nil {
		t.Error("the prune didn't fail")
	}
	st.exists = true
	if err := ix.deferredTick(ctx, now.Add(deferRetry)); err != nil || len(st.lines) != 0 {
		t.Errorf("the retry: %v, lines %q", err, st.lines)
	}
}

// The prunes are kept back for the timeout bound and the landing margin, when
// the write loops start, and for a kind tracked while they run (§10.6.2).
func TestTheQuietTime(t *testing.T) {
	now := time.Now()
	track := func(s *Shared) *Tracker {
		ix := testIndex(&memStore{exists: true, rebuilt: true}, &objServer{})
		ix.now = func() time.Time { return now }
		return &Tracker{s: s, gk: widgetGK, o: Options{TimeoutBound: 90 * time.Second}, idx: ix}
	}
	clk := testingclock.NewFakeClock(now)
	s := newShared(Clients{}, SharedOptions{}, clk)
	tr := track(s)
	tr.startQuiet()
	if d := tr.idx.quietUntil.Sub(now); d != 150*time.Second {
		t.Errorf("the prunes are kept back for %s, want 2m30s", d)
	}
	for name, running := range map[string]bool{"running": true, "not running": false} {
		s := newShared(Clients{}, SharedOptions{}, clk)
		tr := track(s)
		s.trackers[widgetGK] = tr
		if running {
			s.writeCtx = context.Background()
		}
		s.endTracking(widgetGK, &inflight{done: make(chan struct{}), t: tr})
		s.writeWG.Wait()
		if got := !tr.idx.quietUntil.IsZero(); got != running {
			t.Errorf("the write loops %s: the prunes of a kind tracked now are kept back %v", name, got)
		}
	}
	(&Tracker{s: s}).startQuiet() // without an index, nothing
}

// The late entry of an A2 of unknown outcome (§10.6.2): the line is checked
// at once; one entry per object and holder namespace, however often it is
// retried; and once the margin is over the object's live check is queued and
// its batch enqueued. Until it is settled the count isn't sure. A hold that
// landed after a prune took its line gets it back.
func TestTheLateCheck(t *testing.T) {
	id := SecretRef("ns", "x").ID()
	secrets := map[schema.GroupKind]bool{secretGroupKind: true}
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: {ObjectMeta: metav1.ObjectMeta{Name: "x"}}}}
	st := &memStore{exists: true, rebuilt: true}
	ix := testIndex(st, srv)
	now := time.Now()
	ix.now, ix.margin = func() time.Time { return now }, time.Minute
	for range 3 {
		ix.unknownA2(context.Background(), id, "ns")
	}
	if !slices.Equal(st.lines, linesOf([]ID{id})) || len(ix.late) != 1 || !ix.unsure(secrets) {
		t.Fatalf("after three A2s of unknown outcome: lines %q, %d entries, unsure %v", st.lines, len(ix.late), ix.unsure(secrets))
	}
	var batches []string
	tick := func(at time.Time) {
		ix.lateTick(at, func(id ID, n string) { batches = append(batches, n+" "+id.String()) })
	}
	tick(now.Add(time.Minute - time.Millisecond))
	if len(ix.late) != 1 || len(batches) != 0 || len(ix.pending) != 0 {
		t.Errorf("before the margin is over: %d entries, batches %v, pending %v", len(ix.late), batches, ix.pending)
	}
	// Another process's prune takes the line, the object being bare, and then
	// it lands.
	if err := testIndex(st, srv).prune(context.Background(), []ID{id}); err != nil || len(st.lines) != 0 {
		t.Fatalf("the prune: %v, lines %q", err, st.lines)
	}
	srv.objs[id] = heldObj("x")
	tick(now.Add(time.Minute))
	if len(ix.late) != 0 || !slices.Equal(batches, []string{"ns " + id.String()}) || !ix.unsure(secrets) {
		t.Errorf("once the margin is over: %d entries, batches %v, unsure %v", len(ix.late), batches, ix.unsure(secrets))
	}
	if err := ix.recheckTick(context.Background(), now); err != nil || !slices.Equal(st.lines, linesOf([]ID{id})) || ix.unsure(secrets) {
		t.Errorf("after the late check: %v, lines %q, unsure %v; want its line back", err, st.lines, ix.unsure(secrets))
	}
}
