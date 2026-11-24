package credentialuse

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// What a reader of disown-settled may conclude (§7.4), in the model: whenever
// the CRD says that Disown has settled under the switch it has, nothing of a
// kind it covers is held, and nothing lands. It runs the decisions the code
// runs (decide, staleSettled, parseDisown) over the model's CRD, every read
// and write a step of the schedule.
//
// The informer is the most adversarial one: no Disown process ever sees the
// switch off, so nothing starts its run over but what it reads live. The
// record is gone: the count is of P's finalizers alone. The landing margin
// is taken as given: the pass that may settle starts once every hold issued
// before the run's first pass has landed.

var modelCRDKey = apimodel.Key{GK: crdGVR.GroupVersion().WithKind("CustomResourceDefinition").GroupKind(), Name: "widgets.credentialuse.test.krateo.io"}

// flagCase is one script of the operator's, and which rules are off.
type flagCase struct {
	// switches are the switch's values, one after another; "" is off.
	switches []string
	// processes is how many processes run Disown.
	processes int
	// noClear: a pass holds without removing a stale flag. noVersion: a run
	// goes on over a CRD that changed.
	noClear, noVersion bool
}

// preemptions is the bound c is searched within: 3, and in CI 2 with two
// processes.
func (c flagCase) preemptions() int {
	if c.processes > 1 {
		return modelBound(2, 3)
	}
	return 3
}

// exploreFlag searches every schedule of case c, and returns how many had a
// hold under a settled flag, one such log, and how many saw one settle and
// one hold land.
func exploreFlag(t *testing.T, c flagCase) (violations int, sample string, settles, lands int) {
	t.Helper()
	const passes, rounds = 2, 4
	secret := secretKey("ns", "s")
	f, _ := FinalizerName(widgetGK, "ns")
	prefix, _ := FinalizerPrefix(widgetGK)
	res := apimodel.Explore(apimodel.Bounds{Preemptions: c.preemptions(), MaxRuns: 2_000_000}, func(s *apimodel.Sched) error {
		var log []string
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, false)
		must(m.Create(modelCRDKey, metav1.ObjectMeta{}))
		var bad error
		settled, landed := false, false
		m.OnEvent = func(ev apimodel.Event) {
			log = append(log, fmt.Sprintf("%s %v %v", ev.Key.Name, ev.Obj.Annotations, ev.Obj.Finalizers))
		}
		check := func() {
			crd, err := m.Get(modelCRDKey)
			must(err)
			sec, err := m.Get(secret)
			must(err)
			if DisownSettled(crd.Annotations) {
				settled = true
				if bad == nil && countPrefix(sec.Finalizers, prefix) > 0 && parseDisown(crd.Annotations[AnnotationDisown]).covers(secretGroupKind) {
					bad = fmt.Errorf("held under %v: log %q", crd.Annotations, log)
				}
			}
		}
		patchCRD := func(rv string, ann map[string]any) error {
			meta := map[string]any{"annotations": ann}
			if rv != "" {
				meta["resourceVersion"] = rv
			}
			data, err := json.Marshal(map[string]any{"metadata": meta})
			must(err)
			_, err = m.Patch(modelCRDKey, types.MergePatchType, data)
			check()
			return err
		}
		// step counts the calls made, for the margin: inFlight are the steps
		// at which the holds not yet landed read the switch.
		step := 0
		inFlight := map[int]bool{}

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
		for n := range c.processes {
			s.Go("disown"+strconv.Itoa(n), 0, func(a *apimodel.Actor) {
				d := &disownState{}
				runStart := 0
				for range rounds {
					if d.zeros == passes-1 {
						a.Wait("margin", time.Time{}, func() bool {
							for at := range inFlight {
								if at < runStart {
									return false
								}
							}
							return true
						})
					}
					var start *metav1.PartialObjectMetadata
					startStep := 0
					_ = a.Call("crd at the start", func() error {
						step++
						startStep = step
						var err error
						start, err = m.Get(modelCRDKey)
						return err
					})
					set := parseDisown(start.Annotations[AnnotationDisown])
					if !set.on() {
						continue
					}
					count := 0
					if set.covers(secretGroupKind) {
						_ = a.Call("count", func() error {
							step++
							sec, err := m.Get(secret)
							if err == nil {
								count = countPrefix(sec.Finalizers, prefix)
							}
							return err
						})
						if count > 0 {
							_ = a.Call("strip", func() error {
								step++
								_, err := m.Patch(secret, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`))
								check()
								return err
							})
						}
					}
					if count == 0 {
						d.zeros++
					} else {
						d.zeros = 0
					}
					var crd *metav1.PartialObjectMetadata
					_ = a.Call("crd", func() error {
						step++
						var err error
						crd, err = m.Get(modelCRDKey)
						return err
					})
					if parseDisown(crd.Annotations[AnnotationDisown]).key() != set.key() {
						d.zeros = 0
						continue
					}
					d.startRV = start.ResourceVersion
					if c.noVersion {
						d.startRV, d.rv = "", crd.ResourceVersion
					}
					cur := strconv.Itoa(count)
					done, write := d.decide(crd.Annotations, crd.ResourceVersion, cur, passes)
					if d.zeros == 1 {
						runStart = startStep // the run's first pass is this one
					}
					d.rv = crd.ResourceVersion
					if !write {
						continue
					}
					var value any
					if done {
						value = crd.Annotations[AnnotationDisown]
					}
					err := a.Call("write the count", func() error {
						step++
						return patchCRD(crd.ResourceVersion, map[string]any{AnnotationHeldCount: cur, AnnotationDisownSettled: value})
					})
					if err == nil {
						now, err := m.Get(modelCRDKey)
						must(err)
						d.rv = now.ResourceVersion
					}
				}
			})
		}
		s.Go("holder", 0, func(a *apimodel.Actor) {
			for range 2 {
				var crd *metav1.PartialObjectMetadata
				at := 0
				_ = a.Call("read the switch", func() error {
					step++
					at = step
					var err error
					crd, err = m.Get(modelCRDKey)
					return err
				})
				if parseDisown(crd.Annotations[AnnotationDisown]).covers(secretGroupKind) {
					continue
				}
				inFlight[at] = true
				if staleSettled(crd.Annotations) && !c.noClear {
					if err := a.Call("clear the flag", func() error {
						step++
						return patchCRD(crd.ResourceVersion, map[string]any{AnnotationDisownSettled: nil})
					}); err != nil {
						delete(inFlight, at)
						continue // no A2
					}
				}
				_ = a.Call("A2 lands", func() error {
					step++
					data, err := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": []string{f}, "annotations": map[string]string{f: "e"}}})
					must(err)
					_, err = m.Patch(secret, types.MergePatchType, data)
					delete(inFlight, at)
					landed = true
					check()
					return err
				})
			}
		})
		if err := s.Run(); err != nil {
			return err
		}
		if settled {
			settles++
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
		return nil
	})
	for _, fl := range res.Failures {
		t.Errorf("%v\n  schedule: %s", fl.Err, fl.Trace)
	}
	if !res.Finished {
		t.Errorf("the search didn't finish: %d runs", res.Runs)
	}
	t.Logf("%+v: %d preemptions, %d schedules, %d settled, %d had a hold land, %d had one under a settled flag", c, c.preemptions(), res.Runs, settles, lands, violations)
	return violations, sample, settles, lands
}

// TestModelDisownFlag: a settled flag of the switch's value means that
// nothing is held, for a switch turned off and on again with the same value,
// widened, narrowed and widened again, or set late, with one Disown process
// or two, none of which sees the change; a hold may land at any time after
// it read the switch off. Without either rule it doesn't.
func TestModelDisownFlag(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	scripts := map[string][]string{
		"set":                       {"all"},
		"off and on, the same":      {"all", "", "all"},
		"widened":                   {"ConfigMap", "all"},
		"narrowed and widened":      {"all", "ConfigMap", "all"},
		"reordered, the same kinds": {"Secret,ConfigMap", "", "ConfigMap,Secret"},
	}
	for name, switches := range scripts {
		for _, n := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d processes", name, n), func(t *testing.T) {
				v, sample, settles, lands := exploreFlag(t, flagCase{switches: switches, processes: n})
				if v != 0 {
					t.Errorf("%d schedules have a hold under a settled flag: %s", v, sample)
				}
				if settles == 0 || lands == 0 {
					t.Errorf("%d schedules settled, and a hold landed in %d; want both reached", settles, lands)
				}
			})
		}
	}
	for name, c := range map[string]flagCase{
		"no clear before a hold":   {switches: []string{"all", "", "all"}, processes: 1, noClear: true},
		"a run over a changed CRD": {switches: []string{"all", "", "all"}, processes: 1, noVersion: true},
		"narrowed, with no clear":  {switches: []string{"all", "ConfigMap", "all"}, processes: 1, noClear: true},
		"narrowed, over a changed": {switches: []string{"all", "ConfigMap", "all"}, processes: 1, noVersion: true},
	} {
		t.Run("control/"+name, func(t *testing.T) {
			if v, _, _, _ := exploreFlag(t, c); v == 0 {
				t.Error("no schedule has a hold under a settled flag: the model doesn't see what the rule is for")
			}
		})
	}
}
