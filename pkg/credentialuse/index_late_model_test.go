package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The index's drop check, and an A2 whose outcome its sender doesn't learn,
// in the model (§10.6.2, §10.6.3, §10.6.6): every schedule within the bounds,
// through the index's own code, the acquirer's and the release batch's.

// errResponseLost is a write that may have landed, as the client sees it.
var errResponseLost = errors.New("the response was lost")

// lossyModelStore is a modelIndexStore whose first update lands, and then
// fails as with its response lost, if lose is set.
type lossyModelStore struct {
	modelIndexStore
	lose *bool
}

func (s lossyModelStore) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (indexVersion, error) {
	v, err := s.modelIndexStore.update(ctx, cur, lines, mark)
	if err == nil && *s.lose {
		*s.lose = false
		return indexVersion{}, errResponseLost
	}
	return v, err
}

// TestModelIndexDrops: a prune of a held object's line, as of one a late A2
// put back, races a hand removal of the same line, with the informer
// delivering each version in order, whenever; also with the prune's write
// landing and its response lost. At the end the line is there: whichever
// drop the informer saw while the prune was writing, or checking, was
// someone else's, or was checked after it.
func TestModelIndexDrops(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	p := SecretRef("ns", "s").ID()
	for _, lose := range []bool{false, true} {
		t.Run(fmt.Sprintf("response lost=%v", lose), func(t *testing.T) {
			handWon := 0
			res := apimodel.Explore(apimodel.Bounds{Preemptions: 3, MaxRuns: 2_000_000}, func(s *apimodel.Sched) error {
				var log []string
				m := apimodel.New(s.Clock.Now)
				seed(m, "ns", "s", []string{"h"}, true)
				must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
				must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{modelIndexAnnotation: encodeIndex([]string{p.String()}), AnnotationRebuilt: "true"}}))
				seq := m.LastSeq()
				var ix *index
				handDone := false
				// versions are the index's made since, and delivered the
				// informer's of them.
				versions, delivered := 0, 0
				m.OnEvent = func(ev apimodel.Event) {
					if ev.Key == modelIndexKey {
						versions++
					}
				}
				s.Go("pruner", 0, func(a *apimodel.Actor) {
					ix = modelIndex(m, a, "prune", &log)
					losing := lose
					st := modelIndexStore{m: m, a: a, who: "prune", log: &log}
					ix.store = lossyModelStore{st, &losing}
					_ = ix.prune(context.Background(), []ID{p})
					// The write loops' ticks, each far past the last one's
					// backoff: none of them depends on the real clock.
					for now := s.Clock.Now(); ; now = now.Add(24 * time.Hour) {
						a.Wait("tick", time.Time{}, func() bool { return len(ix.pending) > 0 || handDone && delivered == versions })
						if len(ix.pending) == 0 {
							return
						}
						_ = ix.recheckTick(context.Background(), now)
					}
				})
				s.Go("hand", 0, func(a *apimodel.Actor) {
					st := modelIndexStore{m: m, a: a, who: "hand", log: &log}
					cur, err := st.get(context.Background())
					if err == nil && slices.Contains(cur.lines, p.String()) {
						if _, err := st.update(context.Background(), cur, nil, nil); err == nil {
							handWon++
						}
					}
					handDone = true
				})
				s.Go("informer", apimodel.Daemon, func(a *apimodel.Actor) {
					old := indexVersion{lines: []string{p.String()}}
					for {
						a.Wait("deliver", time.Time{}, func() bool { return ix != nil && delivered < versions })
						for _, ev := range m.EventsAfter(seq) {
							seq = ev.Seq
							if ev.Key != modelIndexKey {
								continue
							}
							delivered++
							cur := modelIndexStore{}.version(&ev.Obj)
							ix.observed(&old, cur)
							old = cur
							break
						}
					}
				})
				if err := s.Run(); err != nil {
					return err
				}
				if err := indexSuperset(m, p); err != nil {
					return fmt.Errorf("%w; log %q", err, log)
				}
				return nil
			})
			for _, f := range res.Failures {
				t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
			}
			if !res.Finished && len(res.Failures) == 0 {
				t.Errorf("the search didn't finish: %d runs", res.Runs)
			}
			if handWon == 0 {
				t.Error("no schedule had the hand removal land")
			}
			t.Logf("%d schedules; the hand removal landed in %d", res.Runs, handWon)
		})
	}
}

// lateCase is which of the three conditions of §10.6.3's residual hold, and
// how.
type lateCase struct {
	// The sender makes no check after its A2: the A2's response is lost, or
	// the sender is killed between the response and its check.
	lost, killedBeforeCheck bool
	// The late check doesn't see the landing: the sender is killed within
	// the landing margin, or the A2 lands after it.
	killedWithinMargin, landsPastMargin bool
	// A prune may take the line before the landing: nothing names the
	// object any more.
	unnamed bool
	// Whose prune it is: the sender's own (own), which leaves the line of an
	// A2 that may land yet; its successor's, whose write loops start once the
	// sender has ended, as with leader election, and prune nothing at first
	// (quiet), or do; or, with neither, another process's, which runs all
	// along.
	own, successor, quiet bool
}

// lateChecks reports whether the sender makes its late check.
func (c lateCase) lateChecks() bool { return c.lost && !c.killedBeforeCheck && !c.killedWithinMargin }

// preemptions is the bound c is searched within: 3, and 2 with the late check
// and its batch racing a holder that goes, whose schedules with 3 are past
// 2,000,000; one less of each in CI.
func (c lateCase) preemptions() int {
	if c.lateChecks() && c.unnamed {
		return modelBound(1, 2)
	}
	return modelBound(2, 3)
}

// senderServer is the sender's server: a lost A2 is sent, and lands when the
// network actor delivers it.
type senderServer struct {
	loggingServer
	lost bool
	sent *[]byte
}

func (s senderServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if !s.lost || gk != secretGroupKind || pt != types.StrategicMergePatchType {
		return s.loggingServer.Patch(ctx, gk, namespace, name, pt, data)
	}
	_ = s.a.Call("send A2", func() error {
		*s.sent = data
		*s.log = append(*s.log, "acq A2 sent")
		return nil
	})
	return nil, errResponseLost
}

// senderLines is the sender's index, dead before its check if killed is set.
type senderLines struct {
	*index
	killed bool
}

func (l senderLines) postCheck(ctx context.Context, id ID) error {
	if l.killed {
		return errors.New("killed")
	}
	return l.index.postCheck(ctx, id)
}

func (l senderLines) unknownA2(ctx context.Context, id ID, n string) {
	if !l.killed {
		l.index.unknownA2(ctx, id, n)
	}
}

// lateResult is what a search of a case saw: how many schedules ended with P
// held and without its line, and one such log; how many had the prune take the
// line before the landing; how many had it left by the rules; and how many
// ended with P held.
type lateResult struct {
	violations int
	sample     string
	pruned     int
	left, held int
}

// exploreLate searches every schedule of case c.
func exploreLate(t *testing.T, c lateCase) (r lateResult) {
	t.Helper()
	p := SecretRef("ns", "s").ID()
	res := apimodel.Explore(apimodel.Bounds{Preemptions: c.preemptions(), MaxRuns: 2_000_000}, func(s *apimodel.Sched) error {
		var log []string
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, false)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{AnnotationRebuilt: "true"}}))
		must(m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{}))
		var sent []byte
		landed, acquired, left := false, false, false
		// process is the index of one process, the sender's: whichever of its
		// actors is running.
		var process *index
		if c.own {
			prefix, _ := FinalizerPrefix(widgetGK)
			st := runningStore{s, m, &log}
			process = newIndex(widgetGK, prefix, "idx", DefaultIndexMaxLines, kindSet{secretGroupKind: true}, st, runningServer{s, m, &log})
			process.cached = func(ctx context.Context, line string) bool {
				v, err := st.get(ctx)
				return err == nil && slices.Contains(v.lines, line)
			}
		}
		indexOf := func(a *apimodel.Actor, who string) *index {
			if process != nil {
				return process
			}
			return modelIndex(m, a, who, &log)
		}
		// batch is a release batch of P, and its prune. The pruner's view of
		// the records is the most adversarial one, unless something names P.
		batch := func(a *apimodel.Actor, who string) {
			srv := loggingServer{modelServer{m, a}, who, &log}
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			r, err := pr.release(context.Background(), "ns", []ID{p})
			if err != nil {
				return
			}
			ix := indexOf(a, who)
			if !c.unnamed {
				ix.recorded = func(ID) bool { return true }
			}
			now := time.Unix(0, 0)
			if c.quiet {
				ix.now = func() time.Time { return now }
				ix.startQuiet(time.Minute)
			}
			_ = ix.prune(context.Background(), ix.prunable(r.Bare))
			left = left || len(ix.deferred) > 0
			if !c.quiet || len(ix.deferred) == 0 {
				return
			}
			// The quiet time is over, by when the sender's A2 has landed, or
			// past the margin not.
			if sent != nil && !landed && !c.landsPastMargin {
				a.Wait("quiet", time.Time{}, func() bool { return landed })
			}
			now = now.Add(time.Minute)
			_ = ix.deferredTick(context.Background(), now)
		}
		s.Go("acquirer", 0, func(a *apimodel.Actor) {
			defer func() { acquired = true }()
			log1 := loggingServer{modelServer{m, a}, "acq", &log}
			h, err := log1.Get(context.Background(), widgetGK, "ns", "h")
			if err != nil {
				return
			}
			pr := newProtocol(senderServer{log1, c.lost, &sent}, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.lines = senderLines{indexOf(a, "acq"), c.killedBeforeCheck}
			_, _ = pr.acquire(context.Background(), h, []Ref{SecretRef("ns", "s")})
		})
		if c.lost {
			s.Go("network", 0, func(a *apimodel.Actor) {
				a.Wait("in flight", time.Time{}, func() bool { return sent != nil || acquired })
				if sent == nil {
					return
				}
				_ = a.Call("A2 lands", func() error {
					_, err := m.Patch(secretKey("ns", "s"), types.StrategicMergePatchType, sent)
					landed = true
					log = append(log, "net A2 lands")
					return err
				})
			})
		}
		if c.unnamed {
			// The holder never got its owner finalizer, and goes.
			s.Go("deleter", 0, func(a *apimodel.Actor) {
				a.Wait("announced", time.Time{}, func() bool {
					h, err := m.Get(widgetKey("ns", "h"))
					return err == nil && h.Annotations[AnnotationRefs] != "" || acquired
				})
				_ = a.Call("delete h", func() error {
					log = append(log, "holder gone")
					return m.Delete(widgetKey("ns", "h"))
				})
			})
		}
		s.Go("releaser", 0, func(a *apimodel.Actor) {
			a.Wait("batch", time.Time{}, func() bool {
				_, err := m.Get(widgetKey("ns", "h"))
				return err != nil && !c.successor || acquired
			})
			batch(a, "rel")
		})
		if c.lateChecks() {
			// The late check: the landing margin is over, by when the A2 has
			// landed, or not.
			s.Go("late", 0, func(a *apimodel.Actor) {
				a.Wait("margin", time.Time{}, func() bool { return acquired && (sent == nil || landed || c.landsPastMargin) })
				if sent == nil {
					return
				}
				ix := indexOf(a, "late")
				ix.late[lateHold{p, "ns"}] = time.Time{}
				owed := false
				ix.lateTick(time.Now(), func(ID, string) { owed = true })
				_ = ix.recheckTick(context.Background(), time.Now())
				if owed {
					batch(a, "late")
				}
				// The prune the releaser's batch left, if it did, once due.
				_ = ix.deferredTick(context.Background(), time.Now().Add(24*time.Hour))
			})
		}
		if err := s.Run(); err != nil {
			return err
		}
		if i := slices.IndexFunc(log, func(e string) bool { return e == "rel update []" }); i >= 0 {
			for j, e := range log {
				if j > i && (e == "net A2 lands" || e == fmt.Sprintf("acq patch %s s", types.StrategicMergePatchType)) {
					r.pruned++
					break
				}
			}
		}
		if left {
			r.left++
		}
		if o, err := m.Get(secretKey(p.Namespace, p.Name)); err == nil && len(o.Finalizers) > 0 {
			r.held++
		}
		if err := indexSuperset(m, p); err != nil {
			r.violations++
			if r.sample == "" {
				r.sample = fmt.Sprintf("%v; log %q", err, log)
			}
		}
		return nil
	})
	for _, f := range res.Failures {
		t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
	}
	if !res.Finished {
		t.Errorf("the search didn't finish: %d runs", res.Runs)
	}
	t.Logf("%+v: %d preemptions, %d schedules, %d with the line taken before the landing, %d with it left, %d held, %d without a line", c, c.preemptions(), res.Runs, r.pruned, r.left, r.held, r.violations)
	return r
}

// TestModelLateHold: the residual of §10.6.3 (the user's decision,
// 2026-11-23) needs its three conditions together: the sender makes no check
// after its A2, in either of two ways; its late check doesn't see the
// landing, in either of two ways; and a prune takes the line before the
// landing. With all three, a hold without its line is reached; with any one
// off, none is, on any schedule within each case's bound (preemptions).
//
// With the pruner's two rules for a hold that may land yet (§10.6.2; the
// user's decision, 2026-11-24), none is with all three either, if the A2 lands
// within the margin, and the prune is the sender's successor's, or its own.
// The successor's that prunes at once reaches it, as another process's does,
// which has neither rule, and is the one without its own for the sender's: it
// shares nothing with it. Past the margin, a successor's reaches it too.
func TestModelLateHold(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	reached := map[string]lateCase{
		"lost, killed within the margin": {lost: true, killedWithinMargin: true, unnamed: true},
		"lost, lands past the margin":    {lost: true, landsPastMargin: true, unnamed: true},
		"killed before its check":        {killedBeforeCheck: true, killedWithinMargin: true, unnamed: true},
		"lost, and killed before it":     {lost: true, killedBeforeCheck: true, killedWithinMargin: true, unnamed: true},

		"a successor at once, lost, ended within the margin":  {lost: true, killedWithinMargin: true, unnamed: true, successor: true},
		"a successor at once, lost, and killed before it":     {lost: true, killedBeforeCheck: true, killedWithinMargin: true, unnamed: true, successor: true},
		"a successor quiet at first, lost, lands past margin": {lost: true, killedWithinMargin: true, landsPastMargin: true, unnamed: true, successor: true, quiet: true},
	}
	for name, c := range reached {
		t.Run("all three/"+name, func(t *testing.T) {
			r := exploreLate(t, c)
			if r.violations == 0 || r.pruned == 0 {
				t.Errorf("a hold without its line in %d schedules, the line taken before the landing in %d; want both reached", r.violations, r.pruned)
			}
		})
	}
	ruled := map[string]lateCase{
		"a successor, lost, ended within the margin": {lost: true, killedWithinMargin: true, unnamed: true, successor: true, quiet: true},
		"a successor, lost, and killed before it":    {lost: true, killedBeforeCheck: true, killedWithinMargin: true, unnamed: true, successor: true, quiet: true},
		"its own, lost, ended within the margin":     {lost: true, killedWithinMargin: true, unnamed: true, own: true},
		"its own, lost, alive":                       {lost: true, unnamed: true, own: true},
	}
	for name, c := range ruled {
		t.Run("the pruner's rules/"+name, func(t *testing.T) {
			r := exploreLate(t, c)
			if r.violations != 0 {
				t.Errorf("a hold without its line in %d schedules: %s", r.violations, r.sample)
			}
			if r.left == 0 || r.held == 0 && !c.lateChecks() {
				t.Errorf("%d schedules have the line left, and %d end with a hold; want both", r.left, r.held)
			}
		})
	}
	safe := map[string]lateCase{
		"1 off: it checks, then is killed":        {killedWithinMargin: true, unnamed: true},
		"1 off: it checks":                        {unnamed: true},
		"2 off: lost, alive, lands in margin":     {lost: true, unnamed: true},
		"3 off: lost, killed, still named":        {lost: true, killedWithinMargin: true},
		"3 off: lost, past margin, still named":   {lost: true, landsPastMargin: true},
		"3 off: killed before its check, named":   {killedBeforeCheck: true, killedWithinMargin: true},
		"3 off: lost and killed before it, named": {lost: true, killedBeforeCheck: true, killedWithinMargin: true},
	}
	for name, c := range safe {
		t.Run("one off/"+name, func(t *testing.T) {
			r := exploreLate(t, c)
			if r.violations != 0 {
				t.Errorf("a hold without its line in %d schedules: %s", r.violations, r.sample)
			}
			if c.unnamed && r.pruned == 0 {
				t.Error("no schedule has the line taken before the landing: the other rules weren't needed")
			}
		})
	}
}
