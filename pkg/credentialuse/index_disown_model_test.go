package credentialuse

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// Disown in PerName mode, in the model (§7.4): its count sees a hold only
// through a record or the index's line, so a line that goes while a hold may
// still land hides it. The steps are disownPass's, through the index's own
// code, the acquirer's, the release batch's, and Disown's decisions; the
// switch is set, turned off and on again, widened, or narrowed and widened.
//
// The process holds, makes its late checks and runs Disown over one index;
// another process releases, and prunes with a holder cache that shows no
// record, and a CRD cache that never shows the switch. As in the flag's
// model, no Disown pass learns of the switch but by its live reads.
//
// Two things are taken as given. The landing margin: a pass that may settle
// starts once every hold whose pass read the switch before the run's first
// pass has landed, or failed. And a prune's bound (pruneBound), which is
// shorter: it starts once every prune begun before the run's first pass has
// ended, its check and its re-add included.

// disownIndexCase is one script of the operator's, and one way of an A2.
type disownIndexCase struct {
	// initial is the switch's value at the start, the process one pass from
	// settling under it if it covers P's kind, and settled if it doesn't;
	// switches are the values after it, "" for off.
	initial  string
	switches []string
	// lost: the A2's response is lost, and it lands later. killed: so is its
	// sender, as a process of its own, before any check of its line.
	lost, killed bool
	// unnamed: the holder goes once it has announced.
	unnamed bool
	// The rules off, for the controls: Disown empties in every pass; it
	// doesn't add the lines of what the records name; a prune doesn't read
	// the switch before its removal, or after it; a count doesn't wait for
	// what the process owes; a run goes on over a changed CRD.
	emptyEarly, noLines, noBefore, noAfter, noUnsure, noVersion bool
}

// preemptions is the bound c is searched within: 2, and 1 with two or more of
// the network, the late check and the holder's deletion, whose schedules with
// 2 are too many.
func (c disownIndexCase) preemptions() int {
	if c.killed || !c.lost && !c.unnamed {
		return 2
	}
	return 1
}

// runningStore is a modelIndexStore for whichever actor is running.
type runningStore struct {
	s   *apimodel.Sched
	m   *apimodel.Model
	log *[]string
}

func (r runningStore) now() modelIndexStore {
	return modelIndexStore{m: r.m, a: r.s.Running(), who: r.s.Running().Name, log: r.log}
}
func (r runningStore) get(ctx context.Context) (indexVersion, error) { return r.now().get(ctx) }
func (r runningStore) update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	return r.now().update(ctx, cur, lines, rebuilt)
}
func (r runningStore) create(ctx context.Context, lines []string, rebuilt bool) (indexVersion, error) {
	return r.now().create(ctx, lines, rebuilt)
}

// runningServer is a loggingServer for whichever actor is running.
type runningServer struct {
	s   *apimodel.Sched
	m   *apimodel.Model
	log *[]string
}

func (r runningServer) now() loggingServer {
	return loggingServer{modelServer{r.m, r.s.Running()}, r.s.Running().Name, r.log}
}
func (r runningServer) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	return r.now().Get(ctx, gk, namespace, name)
}
func (r runningServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	return r.now().Patch(ctx, gk, namespace, name, pt, data)
}
func (r runningServer) List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	return r.now().List(ctx, gk, namespace)
}

// exploreDisownIndex searches every schedule of case c, and returns how many
// had P held under a settled flag, one such log, how many settled after the
// hold had landed, and how many had it land.
func exploreDisownIndex(t *testing.T, c disownIndexCase) (violations int, sample string, settles, lands int) {
	t.Helper()
	const passes, rounds = 2, 5
	records := 0
	ctx := context.Background()
	p := SecretRef("ns", "s").ID()
	kinds := map[schema.GroupKind]bool{secretGroupKind: true}
	prefix, _ := FinalizerPrefix(widgetGK)
	res := apimodel.Explore(apimodel.Bounds{Preemptions: c.preemptions(), MaxRuns: 2_000_000}, func(s *apimodel.Sched) error {
		var log []string
		m := apimodel.New(s.Clock.Now)
		var crdAnn map[string]string
		var fins []string
		var bad error
		settled, landed, sawRecord := false, false, false
		m.OnEvent = func(ev apimodel.Event) {
			switch ev.Key {
			case modelCRDKey:
				crdAnn = ev.Obj.Annotations
				log = append(log, fmt.Sprintf("CRD %v", crdAnn))
			case secretKey("ns", "s"):
				fins = ev.Obj.Finalizers
				landed = landed || countPrefix(fins, prefix) > 0
			}
			if !DisownSettled(crdAnn) || !parseDisown(crdAnn[AnnotationDisown]).covers(secretGroupKind) {
				return
			}
			settled = settled || landed
			if bad == nil && countPrefix(fins, prefix) > 0 {
				bad = fmt.Errorf("held under %v: log %q", crdAnn, log)
			}
		}
		seed(m, "ns", "s", nil, false)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{}))
		initial := parseDisown(c.initial)
		d := &disownState{}
		ann, lines := map[string]string{}, map[string]string{AnnotationRebuilt: "true"}
		switch {
		case initial.covers(secretGroupKind):
			ann = map[string]string{AnnotationDisown: c.initial, AnnotationHeldCount: "0"}
			lines[modelIndexAnnotation] = encodeIndex([]string{p.String()})
			d.zeros = passes - 1
		case initial.on():
			ann = map[string]string{AnnotationDisown: c.initial, AnnotationHeldCount: "0", AnnotationDisownSettled: c.initial}
			d.zeros = passes
		}
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: lines}))
		must(m.Create(modelCRDKey, metav1.ObjectMeta{Annotations: ann}))
		crd0, err := m.Get(modelCRDKey)
		must(err)
		d.rv = crd0.ResourceVersion

		patchCRD := func(rv string, ann map[string]any) error {
			meta := map[string]any{"annotations": ann}
			if rv != "" {
				meta["resourceVersion"] = rv
			}
			data, err := json.Marshal(map[string]any{"metadata": meta})
			must(err)
			_, err = m.Patch(modelCRDKey, types.MergePatchType, data)
			return err
		}
		readCRD := func(a *apimodel.Actor, label string) *metav1.PartialObjectMetadata {
			var crd *metav1.PartialObjectMetadata
			must(a.Call(label, func() error {
				var err error
				crd, err = m.Get(modelCRDKey)
				return err
			}))
			return crd
		}
		keptLive := func(context.Context) (disownSet, error) {
			return keptKinds(readCRD(s.Running(), "switch, for a prune").Annotations), nil
		}
		// ix is the process's index.
		srv := runningServer{s, m, &log}
		ix := newIndex(widgetGK, prefix, "idx", DefaultIndexMaxLines, kinds, runningStore{s, m, &log}, srv)
		ix.cached = func(ctx context.Context, line string) bool {
			v, err := ix.store.get(ctx)
			return err == nil && slices.Contains(v.lines, line)
		}
		// later is past every backoff and margin before it.
		hour := 0
		later := func() time.Time {
			hour += 24
			return time.Now().Add(time.Duration(hour) * time.Hour)
		}
		// step counts what reads the switch, for the margin: inFlight are the
		// steps at which the holds not yet landed read it.
		step := 0
		// pruning are those at which the prunes not yet done began.
		inFlight, pruning := map[int]bool{}, map[int]bool{}
		var sent []byte
		sentLanded, acquired := false, false
		recorded := func() bool {
			h, err := m.Get(widgetKey("ns", "h"))
			return err == nil && ReadRecord(h).Present
		}
		// batch is a release batch of P, and its prune over bx.
		batch := func(a *apimodel.Actor, bx *index) {
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			r, err := pr.release(ctx, "ns", []ID{p})
			if err != nil {
				return
			}
			step++
			at := step
			pruning[at] = true
			_ = bx.prune(ctx, bx.prunable(r.Bare))
			delete(pruning, at)
		}

		s.Go("operator", 0, func(a *apimodel.Actor) {
			for i, v := range c.switches {
				_ = a.Call(fmt.Sprintf("switch %d %q", i, v), func() error {
					step++
					var value any
					if v != "" {
						value = v
					}
					return patchCRD("", map[string]any{AnnotationDisown: value})
				})
			}
		})
		s.Go("disown", 0, func(a *apimodel.Actor) {
			runStart := 0
			for range rounds {
				if d.zeros == passes-1 {
					a.Wait("margin", time.Time{}, func() bool {
						for at := range inFlight {
							if at < runStart {
								return false
							}
						}
						for at := range pruning {
							if at < runStart {
								return false
							}
						}
						return true
					})
				}
				var start *metav1.PartialObjectMetadata
				startStep := 0
				must(a.Call("crd at the start", func() error {
					step++
					startStep = step
					var err error
					start, err = m.Get(modelCRDKey)
					return err
				}))
				set := parseDisown(start.Annotations[AnnotationDisown])
				if !set.on() {
					continue
				}
				known, count, lined := true, 0, false
				if set.covers(secretGroupKind) {
					// The model's List is of one namespace, and "" of none of
					// them: the holder's is every one there is.
					holders, _ := srv.List(ctx, widgetGK, "ns")
					sawRecord = sawRecord || slices.ContainsFunc(holders, func(h metav1.PartialObjectMetadata) bool { return recordsDisowned(&h, set) })
					named := false
					for i := range holders {
						if recordsDisowned(&holders[i], set) {
							count++
							named = true
						}
					}
					if err := ix.recheckTick(ctx, later()); err != nil {
						known = false
					}
					cur, sure, err := ix.readSure(ctx, kinds)
					if err != nil || !sure && !c.noUnsure {
						known = false
					}
					lined = slices.Contains(cur.lines, p.String())
					var sec *metav1.PartialObjectMetadata
					listed := 0
					if lined || named {
						if sec, err = srv.Get(ctx, secretGroupKind, "ns", "s"); err != nil {
							known = false
						} else {
							listed = countPrefix(sec.Finalizers, prefix)
							count += listed
						}
					}
					unlined := false
					if named && !lined && !c.noLines {
						if err := ix.add(ctx, []ID{p}); err != nil {
							known, unlined = false, true
						}
					}
					for i := range holders {
						if patch, ok := disownHolderPatch(&holders[i], set); ok && !unlined {
							_, _ = srv.Patch(ctx, widgetGK, "ns", holders[i].Name, types.MergePatchType, patch)
						}
					}
					if sec != nil {
						_, removed, err := stripDisowned(ctx, srv, secretGroupKind, widgetGK, sec)
						count += max(0, removed-listed)
						if err != nil {
							lined = false // not free
						}
					}
				}
				cur := "unknown"
				if known {
					cur = strconv.Itoa(count)
				}
				if cur == "0" {
					d.zeros++
				} else {
					d.zeros = 0
				}
				crd := readCRD(a, "crd")
				step++
				if parseDisown(crd.Annotations[AnnotationDisown]).key() != set.key() {
					d.zeros = 0
					continue
				}
				d.startRV = start.ResourceVersion
				if c.noVersion {
					d.startRV, d.rv = "", crd.ResourceVersion
				}
				done, write := d.decide(crd.Annotations, crd.ResourceVersion, cur, passes)
				if d.zeros == 1 {
					runStart = startStep // the run's first pass is this one
				}
				d.rv = crd.ResourceVersion
				if write {
					var value any
					if done {
						value = crd.Annotations[AnnotationDisown]
					}
					if err := a.Call("write the count", func() error {
						step++
						if err := patchCRD(crd.ResourceVersion, map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: value}); err != nil {
							return err
						}
						now, err := m.Get(modelCRDKey)
						d.rv = now.ResourceVersion
						return err
					}); err != nil {
						continue
					}
				}
				if !lined || !done && !c.emptyEarly {
					continue
				}
				if sec, err := srv.Get(ctx, secretGroupKind, "ns", "s"); err == nil && countPrefix(sec.Finalizers, prefix) == 0 {
					_ = ix.empty(ctx, []ID{p})
				}
			}
		})
		s.Go("holder", 0, func(a *apimodel.Actor) {
			defer func() { acquired = true }()
			h, err := srv.Get(ctx, widgetGK, "ns", "h")
			if err != nil {
				return
			}
			at := 0
			pr := newProtocol(senderServer{loggingServer{modelServer{m, a}, "acq", &log}, c.lost, &sent}, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.disownLive = func(context.Context) (disownSet, error) {
				var crd *metav1.PartialObjectMetadata
				must(a.Call("read the switch", func() error {
					step++
					at = step
					var err error
					crd, err = m.Get(modelCRDKey)
					return err
				}))
				set := parseDisown(crd.Annotations[AnnotationDisown])
				if !set.covers(secretGroupKind) {
					inFlight[at] = true
				}
				if staleSettled(crd.Annotations) {
					if err := a.Call("clear the flag", func() error {
						step++
						return patchCRD(crd.ResourceVersion, map[string]any{AnnotationDisownSettled: nil})
					}); err != nil {
						return disownSet{}, err
					}
				}
				return set, nil
			}
			pr.lines = senderLines{ix, false}
			if c.killed {
				pr.lines = senderLines{modelIndex(m, a, "acq", &log), true}
			}
			_, _ = pr.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
			if sent == nil || sentLanded {
				delete(inFlight, at)
			}
		})
		if c.lost {
			s.Go("network", 0, func(a *apimodel.Actor) {
				a.Wait("in flight", time.Time{}, func() bool { return sent != nil || acquired })
				if sent == nil {
					return
				}
				_ = a.Call("A2 lands", func() error {
					_, err := m.Patch(secretKey("ns", "s"), types.StrategicMergePatchType, sent)
					sentLanded = true
					clear(inFlight)
					log = append(log, "net A2 lands")
					return err
				})
			})
		}
		if !c.killed {
			// The late check, once the margin is over, by when the A2 has
			// landed; as late as the schedule has it.
			s.Go("ticks", 0, func(a *apimodel.Actor) {
				a.Wait("margin over", time.Time{}, func() bool { return acquired && (sent == nil || sentLanded) })
				owed := false
				ix.lateTick(later(), func(ID, string) { owed = true })
				for range 3 {
					if err := ix.recheckTick(ctx, later()); err == nil {
						break
					}
				}
				if owed {
					batch(a, ix)
				}
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
			bx := modelIndex(m, a, "rel", &log)
			reads := 0 // of the switch by its one prune, before and after
			bx.keptLive = func(ctx context.Context) (disownSet, error) {
				if reads++; reads == 1 && c.noBefore || reads == 2 && c.noAfter {
					return disownSet{}, nil
				}
				return keptLive(ctx)
			}
			batch(a, bx)
		})
		ix.keptLive = keptLive

		if err := s.Run(); err != nil {
			return err
		}
		if settled {
			settles++
		}
		if sawRecord {
			records++
		}
		if landed {
			lands++
		}
		if bad != nil {
			violations++
			if sample == "" {
				sample = bad.Error()
			}
		}
		if c.killed {
			return nil
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
	if records == 0 {
		t.Error("no schedule had Disown see a record")
	}
	t.Logf("%+v: %d preemptions, %d schedules, %d had Disown see a record, %d settled after the hold landed, %d had it land, %d had it under a settled flag", c, c.preemptions(), res.Runs, records, settles, lands, violations)
	return violations, sample, settles, lands
}

// TestModelIndexDisown: Disown settles in PerName mode only with nothing
// held, whether the switch is set, turned off and on again, widened, or
// narrowed and widened again; whether the A2 of a pass that read it off is
// answered or not; and whether its holder is gone, which lets a prune take its
// line before it lands. A sender that is killed leaves its holder's record,
// and Disown lines what that names. Without any one rule it doesn't.
func TestModelIndexDisown(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	scripts := map[string]disownIndexCase{
		"set":                  {switches: []string{"all"}},
		"off and on":           {initial: "all", switches: []string{"", "all"}},
		"widened":              {initial: "ConfigMap", switches: []string{"all"}},
		"narrowed and widened": {initial: "all", switches: []string{"ConfigMap", "all"}},
	}
	ways := map[string]disownIndexCase{
		"answered":              {},
		"answered, holder gone": {unnamed: true},
		"lost":                  {lost: true},
		"lost, holder gone":     {lost: true, unnamed: true},
		"lost, sender killed":   {lost: true, killed: true},
	}
	for script, c := range scripts {
		for way, w := range ways {
			// A switch that was on before may have stripped the record of a
			// hold whose sender was killed: nothing names its object then, and
			// a prune with the switch off may take its line. That is the
			// residual of §10.6.3, and TestModelLateHold's.
			if w.killed && parseDisown(c.initial).covers(secretGroupKind) {
				continue
			}
			cc := c
			cc.lost, cc.killed, cc.unnamed = w.lost, w.killed, w.unnamed
			t.Run(script+"/"+way, func(t *testing.T) {
				v, sample, settles, lands := exploreDisownIndex(t, cc)
				if v != 0 {
					t.Errorf("%d schedules have the hold under a settled flag: %s", v, sample)
				}
				if settles == 0 || lands == 0 {
					t.Errorf("%d schedules settled after the hold landed, and it landed in %d; want both reached", settles, lands)
				}
			})
		}
	}
	for name, c := range map[string]disownIndexCase{
		"emptied in every pass":                {switches: []string{"all"}, lost: true, killed: true, emptyEarly: true},
		"records stripped without their lines": {switches: []string{"all"}, lost: true, killed: true, noLines: true},
		"a prune with no read after it":        {switches: []string{"all"}, lost: true, killed: true, noAfter: true},
		"a count that waits for nothing owed":  {switches: []string{"all"}, lost: true, unnamed: true, noUnsure: true},
		"a run over a changed CRD":             {initial: "all", switches: []string{"", "all"}, noVersion: true},
		// With no read before the removal either.
		"a pruner whose CRD cache is behind": {switches: []string{"all"}, lost: true, killed: true, noBefore: true},
	} {
		t.Run("control/"+name, func(t *testing.T) {
			if v, _, _, _ := exploreDisownIndex(t, c); v == 0 {
				t.Error("no schedule has the hold under a settled flag: the model doesn't see what the rule is for")
			}
		})
	}
}
