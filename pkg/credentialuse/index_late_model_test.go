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

func (s lossyModelStore) update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	v, err := s.modelIndexStore.update(ctx, cur, lines, rebuilt)
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
				// undelivered reports whether the informer has a version of
				// the index to deliver.
				undelivered := func() bool {
					n := 0
					m.EachEventAfter(seq, func(ev *apimodel.Event) {
						if ev.Key == modelIndexKey {
							n++
						}
					})
					return n > 0
				}
				pending := func() int {
					if ix == nil {
						return 0
					}
					ix.mu.Lock()
					defer ix.mu.Unlock()
					return len(ix.pending)
				}
				s.Go("pruner", 0, func(a *apimodel.Actor) {
					ix = modelIndex(m, a, "prune", &log)
					losing := lose
					st := modelIndexStore{m: m, a: a, who: "prune", log: &log}
					ix.store = lossyModelStore{st, &losing}
					_ = ix.prune(context.Background(), []ID{p})
					for {
						// The write loops' tick.
						a.Wait("tick", time.Time{}, func() bool { return pending() > 0 || handDone && !undelivered() })
						if pending() == 0 {
							return
						}
						_ = ix.recheckTick(context.Background(), time.Now().Add(time.Hour))
					}
				})
				s.Go("hand", 0, func(a *apimodel.Actor) {
					st := modelIndexStore{m: m, a: a, who: "hand", log: &log}
					cur, err := st.get(context.Background())
					if err == nil && slices.Contains(cur.lines, p.String()) {
						if _, err := st.update(context.Background(), cur, nil, false); err == nil {
							handWon++
						}
					}
					handDone = true
				})
				s.Go("informer", apimodel.Daemon, func(a *apimodel.Actor) {
					old := indexVersion{lines: []string{p.String()}}
					for {
						a.Wait("deliver", time.Time{}, func() bool { return ix != nil && undelivered() })
						for _, ev := range m.EventsAfter(seq) {
							seq = ev.Seq
							if ev.Key != modelIndexKey {
								continue
							}
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

// exploreLate searches every schedule of case c, and returns how many ended
// with P held and without its line, one such log, and how many had the prune
// take the line before the landing.
func exploreLate(t *testing.T, c lateCase) (violations int, sample string, pruned int) {
	t.Helper()
	p := SecretRef("ns", "s").ID()
	res := apimodel.Explore(apimodel.Bounds{Preemptions: 3, MaxRuns: 2_000_000}, func(s *apimodel.Sched) error {
		var log []string
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, false)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{AnnotationRebuilt: "true"}}))
		must(m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{}))
		var sent []byte
		landed, acquired := false, false
		// batch is a release batch of P, and its prune. The pruner's view of
		// the records is the most adversarial one, unless something names P.
		batch := func(a *apimodel.Actor, who string) {
			srv := loggingServer{modelServer{m, a}, who, &log}
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			r, err := pr.release(context.Background(), "ns", []ID{p})
			if err != nil {
				return
			}
			ix := modelIndex(m, a, who, &log)
			if !c.unnamed {
				ix.recorded = func(ID) bool { return true }
			}
			_ = ix.prune(context.Background(), ix.prunable(r.Bare))
		}
		recorded := func() bool {
			h, err := m.Get(widgetKey("ns", "h"))
			return err == nil && ReadRecord(h).Present
		}
		s.Go("acquirer", 0, func(a *apimodel.Actor) {
			defer func() { acquired = true }()
			log1 := loggingServer{modelServer{m, a}, "acq", &log}
			h, err := log1.Get(context.Background(), widgetGK, "ns", "h")
			if err != nil {
				return
			}
			pr := newProtocol(senderServer{log1, c.lost, &sent}, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.lines = senderLines{modelIndex(m, a, "acq", &log), c.killedBeforeCheck}
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
				a.Wait("announced", time.Time{}, func() bool { return recorded() || acquired })
				_ = a.Call("delete h", func() error {
					log = append(log, "holder gone")
					return m.Delete(widgetKey("ns", "h"))
				})
			})
		}
		s.Go("releaser", 0, func(a *apimodel.Actor) {
			a.Wait("batch", time.Time{}, func() bool {
				_, err := m.Get(widgetKey("ns", "h"))
				return err != nil || acquired
			})
			batch(a, "rel")
		})
		if c.lost && !c.killedBeforeCheck && !c.killedWithinMargin {
			// The late check: the landing margin is over, by when the A2 has
			// landed, or not.
			s.Go("late", 0, func(a *apimodel.Actor) {
				a.Wait("margin", time.Time{}, func() bool { return acquired && (sent == nil || landed || c.landsPastMargin) })
				if sent == nil {
					return
				}
				ix := modelIndex(m, a, "late", &log)
				ix.late[lateHold{p, "ns"}] = time.Time{}
				owed := false
				ix.lateTick(time.Now(), func(ID, string) { owed = true })
				_ = ix.recheckTick(context.Background(), time.Now())
				if owed {
					batch(a, "late")
				}
			})
		}
		if err := s.Run(); err != nil {
			return err
		}
		if i := slices.IndexFunc(log, func(e string) bool { return e == "rel update []" }); i >= 0 {
			for j, e := range log {
				if j > i && (e == "net A2 lands" || e == fmt.Sprintf("acq patch %s s", types.StrategicMergePatchType)) {
					pruned++
					break
				}
			}
		}
		if err := indexSuperset(m, p); err != nil {
			violations++
			if sample == "" {
				sample = fmt.Sprintf("%v; log %q", err, log)
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
	t.Logf("%+v: %d schedules, %d with the line taken before the landing, %d held without a line", c, res.Runs, pruned, violations)
	return violations, sample, pruned
}

// TestModelLateHold: the residual of §10.6.3 (the user's decision,
// 2026-11-23) needs its three conditions together: the sender makes no check
// after its A2, in either of two ways; its late check doesn't see the
// landing, in either of two ways; and a prune takes the line before the
// landing. With all three, a hold without its line is reached; with any one
// off, none is, on any schedule.
func TestModelLateHold(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	reached := map[string]lateCase{
		"lost, killed within the margin": {lost: true, killedWithinMargin: true, unnamed: true},
		"lost, lands past the margin":    {lost: true, landsPastMargin: true, unnamed: true},
		"killed before its check":        {killedBeforeCheck: true, killedWithinMargin: true, unnamed: true},
		"lost, and killed before it":     {lost: true, killedBeforeCheck: true, killedWithinMargin: true, unnamed: true},
	}
	for name, c := range reached {
		t.Run("all three/"+name, func(t *testing.T) {
			n, _, pruned := exploreLate(t, c)
			if n == 0 || pruned == 0 {
				t.Errorf("a hold without its line in %d schedules, the line taken before the landing in %d; want both reached", n, pruned)
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
			if n, sample, _ := exploreLate(t, c); n != 0 {
				t.Errorf("a hold without its line in %d schedules: %s", n, sample)
			}
		})
	}
}
