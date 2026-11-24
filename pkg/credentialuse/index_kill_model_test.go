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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// A process killed inside a prune, between its removal of P's line and its
// re-add, with the switch set and a Disown pass begun meanwhile, and P held
// anew by a pass from before the switch (§7.4, §10.6.2), in the model: whether
// Disown can settle with P held, what of it is needed, and whether stripping
// no record until a prune's bound is over closes it.
//
// Two givens, as in TestModelIndexDisown: the landing margin, and a prune's
// bound. What is cut, so that the searches finish: the switch is set by the
// actor of the first Disown pass, and the process started after a death by the
// one that died, so that other actors' steps in between cost a preemption; one
// holder's pass, one batch, and one pass of the dying process's Disown.

// errProcDead is every call of a process that was killed.
var errProcDead = errors.New("the process was killed")

// killCase is where the process dies, who runs Disown, and what is off.
type killCase struct {
	// at is where its prune's process dies: instead of its "removal", of its
	// "read after" of the switch, or of its "re-add"; at its "end"; or never.
	at string
	// same: the process that dies runs Disown, one pass of it, and the
	// process started after it does; other: so does another process, which
	// lives.
	same, other bool
	// switchOutside: the switch isn't set between the prune's live read of it
	// and its removal. readOutside: no Disown pass reads the index there.
	switchOutside, readOutside bool
	// freshHolders: the prune's holder cache shows every record there is.
	freshHolders bool
	// senderLives: the holder's pass is another process's, which lives, and
	// makes its check. landsFirst: no Disown pass of any process starts before
	// the A2 has landed, if one is ever sent; a schedule where one does is an
	// earlyPass, which fails the search.
	senderLives, landsFirst bool
	// early is the process whose passes landsFirst doesn't hold: a control of
	// earlyPass.
	early string
	// recordsFirst: a pass strips the records before the objects, as it once
	// did.
	recordsFirst bool
	// b: no pass strips a record while a prune that began before its
	// process's first pass under the switch may go on.
	b bool
}

// earlyPass is the error of a schedule in which process who started a Disown
// pass before the A2 had landed, in a case that says none does.
type earlyPass struct{ who string }

func (e earlyPass) Error() string {
	return "a Disown pass of " + e.who + " started before the A2 had landed"
}

// procServer is a server of a process that may die.
type procServer struct {
	loggingServer
	dead *bool
}

func (s procServer) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	if *s.dead {
		return nil, errProcDead
	}
	return s.loggingServer.Get(ctx, gk, namespace, name)
}

func (s procServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if *s.dead {
		return nil, errProcDead
	}
	return s.loggingServer.Patch(ctx, gk, namespace, name, pt, data)
}

func (s procServer) List(ctx context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	if *s.dead {
		return nil, errProcDead
	}
	return s.loggingServer.List(ctx, gk, namespace)
}

// inFlightServer is the sender's: its A2 waits for its response, until it has
// landed or the process is dead.
type inFlightServer struct {
	procServer
	sent   *[]byte
	landed *bool
}

func (s inFlightServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if gk != secretGroupKind || pt != types.StrategicMergePatchType || *s.dead {
		return s.procServer.Patch(ctx, gk, namespace, name, pt, data)
	}
	_ = s.a.Call("send A2", func() error {
		*s.sent = data
		*s.log = append(*s.log, "acq A2 sent")
		return nil
	})
	s.a.Wait("response", time.Time{}, func() bool { return *s.landed || *s.dead })
	if *s.dead {
		return nil, errProcDead
	}
	return s.m.Get(apimodel.Key{GK: gk, Namespace: namespace, Name: name})
}

// procStore is an index store of a process that may die; before, if set, runs
// before each update, with its number, and after it when it landed.
type procStore struct {
	modelIndexStore
	dead          *bool
	before, after func(update int)
	updates       *int
}

func (s procStore) get(ctx context.Context) (indexVersion, error) {
	if *s.dead {
		return indexVersion{}, errProcDead
	}
	return s.modelIndexStore.get(ctx)
}

func (s procStore) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (indexVersion, error) {
	*s.updates++
	n := *s.updates
	if s.before != nil {
		s.before(n)
	}
	if *s.dead {
		return indexVersion{}, errProcDead
	}
	v, err := s.modelIndexStore.update(ctx, cur, lines, mark)
	if err == nil && s.after != nil {
		s.after(n)
	}
	return v, err
}

// killStats is what a search of a case saw: how many schedules had the process
// die, had a record stripped, settled, and had P held under a settled flag,
// and a log of each of the first shapes of those.
type killStats struct {
	Died, Strips, Settles, Violations int
	Shapes                            killShapes
}

// killShapes counts the violating schedules by shape, the order of what the
// processes did, and keeps a log of each of the first maxShapes.
type killShapes struct {
	count map[string]int
	logs  map[string]string
	order []string
}

const maxShapes = 5

// add counts a schedule with log, of which steps are the shape.
func (k *killShapes) add(steps []string, log string) {
	shape := strings.Join(steps, ", ")
	if k.count == nil {
		k.count, k.logs = map[string]int{}, map[string]string{}
	}
	if k.count[shape]++; k.count[shape] == 1 && len(k.order) < maxShapes {
		k.order = append(k.order, shape)
		k.logs[shape] = log
	}
}

func (k killShapes) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d shapes", len(k.count))
	for _, shape := range k.order {
		fmt.Fprintf(&b, "\n  %d schedules: %s\n    one: %s", k.count[shape], shape, k.logs[shape])
	}
	return b.String()
}

// killScenario is case c's scenario, recording what it sees in st.
func killScenario(c killCase, st *killStats) func(s *apimodel.Sched) error {
	const passes = 2
	// A pass that strips, and a run of zeros; with B, one that strips nothing
	// before it.
	rounds := passes + 1
	if c.b {
		rounds++
	}
	ctx := context.Background()
	p := SecretRef("ns", "s").ID()
	kinds := map[schema.GroupKind]bool{secretGroupKind: true}
	prefix, _ := FinalizerPrefix(widgetGK)
	return func(s *apimodel.Sched) error {
		var log []string
		m := apimodel.New(s.Clock.Now)
		var crdAnn map[string]string
		var fins []string
		var bad, early error
		// steps is what the processes did, the shape of the schedule.
		var steps []string
		settled, stripped := false, false
		m.OnEvent = func(ev apimodel.Event) {
			switch ev.Key {
			case modelCRDKey:
				crdAnn = ev.Obj.Annotations
				log = append(log, fmt.Sprintf("CRD %v", crdAnn))
			case secretKey("ns", "s"):
				fins = ev.Obj.Finalizers
			case widgetKey("ns", "h"):
				log = append(log, fmt.Sprintf("holder %v", ev.Obj.Annotations))
			}
			if !DisownSettled(crdAnn) {
				return
			}
			settled = true
			if bad == nil && countPrefix(fins, prefix) > 0 {
				bad = fmt.Errorf("held under %v: log %q", crdAnn, log)
			}
		}
		seed(m, "ns", "s", nil, false)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{}))
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{AnnotationRebuilt: "true", modelIndexAnnotation: encodeIndex([]string{p.String()})}}))
		must(m.Create(modelCRDKey, metav1.ObjectMeta{}))

		dead, alive := false, false // of the process that may die, and of the others
		on, acquired, pruneDone, landed := false, false, false, false
		// window: the prune has read the switch as off, and not removed yet.
		window := false
		step, pruneAt := 0, 0
		pruning := func(before int) bool { return pruneAt > 0 && !pruneDone && !dead && pruneAt < before }
		inFlight := map[int]bool{}
		holding := func(before int) bool {
			for at := range inFlight {
				if at < before {
					return true
				}
			}
			return false
		}
		var sent []byte
		readCRD := func(a *apimodel.Actor, label string) *metav1.PartialObjectMetadata {
			var crd *metav1.PartialObjectMetadata
			must(a.Call(label, func() error {
				step++
				var err error
				crd, err = m.Get(modelCRDKey)
				return err
			}))
			return crd
		}
		server := func(a *apimodel.Actor, who string, dead *bool) procServer {
			return procServer{loggingServer{modelServer{m, a}, who, &log}, dead}
		}
		procIndex := func(a *apimodel.Actor, who string, dead *bool, before, after func(int)) *index {
			st := procStore{modelIndexStore{m: m, a: a, who: who, log: &log}, dead, before, after, new(int)}
			ix := newIndex(widgetGK, prefix, "idx", DefaultIndexMaxLines, kinds, st, server(a, who, dead))
			ix.cached = func(ctx context.Context, line string) bool {
				v, err := st.get(ctx)
				return err == nil && slices.Contains(v.lines, line)
			}
			return ix
		}
		// disown is a process's Disown loop.
		disown := func(a *apimodel.Actor, who string, gone *bool, rounds int) {
			if c.landsFirst && who != c.early && !landed && (!acquired || sent != nil) {
				a.Wait("landed", time.Time{}, func() bool { return landed || acquired && sent == nil })
			}
			srv, ix := server(a, who, gone), procIndex(a, who, gone, nil, nil)
			ix.keptLive = func(context.Context) (disownSet, error) {
				if *gone {
					return disownSet{}, errProcDead
				}
				return keptKinds(readCRD(a, "switch, for a prune").Annotations), nil
			}
			d := &disownState{}
			runStart, first := 0, 0
			for range rounds {
				// A wait that is over is no step of the schedule.
				if d.zeros == passes-1 && (pruning(runStart) || holding(runStart)) {
					a.Wait("margin", time.Time{}, func() bool {
						for at := range inFlight {
							if at < runStart {
								return false
							}
						}
						return pruneAt == 0 || pruneDone || dead || pruneAt >= runStart
					})
				}
				if *gone {
					return
				}
				if c.landsFirst && early == nil && !landed && (!acquired || sent != nil) {
					early = earlyPass{who}
				}
				steps = append(steps, who+" passes")
				start := readCRD(a, "crd at the start")
				startStep := step
				if first == 0 {
					first = startStep
				}
				set := parseDisown(start.Annotations[AnnotationDisown])
				// Decided before the pass reads the index: the read that the
				// strip rests on must come after the wait.
				hold := c.b && pruning(first)
				known, count := true, 0
				holders, err := srv.List(ctx, widgetGK, "ns")
				if err != nil {
					return
				}
				named := false
				for i := range holders {
					if recordsDisowned(&holders[i], set) {
						count++
						named = true
					}
				}
				var cur indexVersion
				for {
					if c.readOutside && window {
						a.Wait("outside the window", time.Time{}, func() bool { return !window })
					}
					var sure bool
					if cur, sure, err = ix.readSure(ctx, kinds); err != nil || !sure {
						known = false
					}
					if !c.readOutside || !window {
						break
					}
				}
				if *gone {
					return
				}
				lined := slices.Contains(cur.lines, p.String())
				var sec *metav1.PartialObjectMetadata
				listed := 0
				if lined || named {
					if sec, err = srv.Get(ctx, secretGroupKind, "ns", "s"); err != nil {
						return
					}
					listed = countPrefix(sec.Finalizers, prefix)
					count += listed
				}
				if named && !lined {
					if err := ix.add(ctx, []ID{p}); err != nil {
						known, hold = false, true
					}
				}
				records := func() {
					for i := range holders {
						if patch, ok := disownHolderPatch(&holders[i], set); ok && !hold {
							if _, err := srv.Patch(ctx, widgetGK, "ns", holders[i].Name, types.MergePatchType, patch); err == nil {
								stripped = true
								steps = append(steps, who+" strips the record")
							}
						}
					}
				}
				if c.recordsFirst {
					records()
				}
				if sec != nil {
					_, removed, err := stripDisowned(ctx, srv, secretGroupKind, widgetGK, sec)
					if removed > 0 {
						steps = append(steps, who+" strips P")
					}
					count += max(0, removed-listed)
					if err != nil {
						lined, hold = false, true // not free, and named still
					}
				}
				if !c.recordsFirst {
					records()
				}
				if *gone {
					return
				}
				n := "unknown"
				if known {
					n = strconv.Itoa(count)
				}
				if n == "0" {
					d.zeros++
				} else {
					d.zeros = 0
				}
				step++
				done, same := countStep(a, m, d, set, start.ResourceVersion, n, passes, false)
				if d.zeros == 1 {
					runStart = startStep
				}
				if !same || !lined || !done {
					continue
				}
				if sec, err := srv.Get(ctx, secretGroupKind, "ns", "s"); err == nil && countPrefix(sec.Finalizers, prefix) == 0 {
					_ = ix.empty(ctx, []ID{p})
				}
			}
		}

		// operate sets the switch.
		operate := func(a *apimodel.Actor) {
			for !on {
				if c.switchOutside && window {
					a.Wait("outside the window", time.Time{}, func() bool { return !window })
				}
				_ = a.Call("switch", func() error {
					if c.switchOutside && window {
						return nil
					}
					step++
					on = true
					steps = append(steps, "the switch is set")
					return patchModelCRD(m, "", map[string]any{AnnotationDisown: "all"})
				})
			}
		}
		sender := &dead
		if c.senderLives {
			sender = &alive
		}
		s.Go("holder", 0, func(a *apimodel.Actor) {
			defer func() { acquired = true }()
			srv := server(a, "acq", sender)
			h, err := srv.Get(ctx, widgetGK, "ns", "h")
			if err != nil {
				return
			}
			at := 0
			pr := newProtocol(inFlightServer{srv, &sent, &landed}, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.disownLive = func(context.Context) (disownSet, error) {
				if *sender {
					return disownSet{}, errProcDead
				}
				set := parseDisown(readCRD(a, "read the switch").Annotations[AnnotationDisown])
				if !set.covers(secretGroupKind) {
					at = step
					inFlight[at] = true
				}
				return set, nil
			}
			pr.lines = procIndex(a, "acq", sender, nil, nil)
			_, _ = pr.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
			if sent == nil || landed {
				delete(inFlight, at)
			}
		})
		s.Go("network", 0, func(a *apimodel.Actor) {
			a.Wait("in flight", time.Time{}, func() bool { return sent != nil || acquired })
			if sent == nil {
				return
			}
			_ = a.Call("A2 lands", func() error {
				_, err := m.Patch(secretKey("ns", "s"), types.StrategicMergePatchType, sent)
				landed = true
				clear(inFlight)
				log = append(log, "net A2 lands")
				steps = append(steps, "the A2 lands")
				return err
			})
		})
		// The batch of P's earlier holder, and its prune, which the process
		// may die in.
		prune := func(a *apimodel.Actor) {
			defer func() { pruneDone, window = true, false }()
			die := func() {
				dead, window = true, false
				log = append(log, "killed")
				steps = append(steps, "the pruner's process is killed")
			}
			srv := server(a, "pruner", &dead)
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			r, err := pr.release(ctx, "ns", []ID{p})
			if err != nil {
				return
			}
			ix := procIndex(a, "pruner", &dead,
				func(n int) {
					if n == 1 && c.at == "removal" || n == 2 && c.at == "re-add" {
						die()
					}
				},
				func(n int) {
					if n == 1 {
						window = false
					}
					steps = append(steps, "the pruner writes the index")
				})
			if c.freshHolders {
				ix.recorded = func(ID) bool {
					h, err := m.Get(widgetKey("ns", "h"))
					return err == nil && ReadRecord(h).Present
				}
			}
			reads := 0
			ix.keptLive = func(context.Context) (disownSet, error) {
				if reads++; reads == 2 && c.at == "read after" {
					die()
				}
				if dead {
					return disownSet{}, errProcDead
				}
				set := keptKinds(readCRD(a, "switch, for a prune").Annotations)
				window = reads == 1 && !set.on()
				return set, nil
			}
			step++
			pruneAt = step
			_ = ix.prune(ctx, ix.prunable(r.Bare))
			if c.at == "end" {
				die()
			}
		}
		s.Go("pruner", 0, func(a *apimodel.Actor) {
			prune(a)
			if !dead || !c.same {
				return
			}
			if !on {
				a.Wait("restart", time.Time{}, func() bool { return on })
			}
			disown(a, "restarted", &alive, rounds)
		})
		if c.same {
			s.Go("disown", 0, func(a *apimodel.Actor) {
				operate(a)
				n := rounds
				if c.at != "" {
					n = 1
				}
				disown(a, "disown", &dead, n)
			})
		}
		if c.other {
			s.Go("other", 0, func(a *apimodel.Actor) {
				if c.same {
					a.Wait("switch", time.Time{}, func() bool { return on })
				} else {
					operate(a)
				}
				disown(a, "other", &alive, rounds)
			})
		}
		if err := s.Run(); err != nil {
			return err
		}
		if early != nil {
			return early
		}
		if dead {
			st.Died++
		}
		if settled {
			st.Settles++
		}
		if stripped {
			st.Strips++
		}
		if bad != nil {
			st.Violations++
			st.Shapes.add(steps, bad.Error())
		}
		return nil
	}
}

// TestModelKillInAPrune: a process killed between its prune's removal and
// its re-add, the switch set and the index read by a Disown pass between the
// prune's read of the switch and its removal, and an object held past that
// pass, leave a hold that Disown settles over, whichever process that pass was
// of, and whether or not the sender lives: one schedule shows it. The object
// is held past the pass by an A2 that lands after the pass has read it.
// Without the kill there, or the switch or the read there, or with the A2
// landing before the pass, it doesn't; nor, anywhere, if no pass strips a
// record until every prune begun before its process's first pass has ended.
// With the records stripped before the objects, an A2 that lands before the
// pass is enough, if the same kill falls between the pass's two strips.
//
// A case in which the A2 lands before the pass checks that itself: in each, a
// pass of any process that starts before fails the search, and "early/..."
// shows that check fire for each process that passes.
//
// With one Disown process the clean cases are searched to two preemptions,
// the "one off" ones to one in CI; with two, "both", to one, the "one off"
// ones, and B with the A2 landing before the pass, under
// CREDENTIALUSE_MODEL_DEEP only. What needs more isn't covered.
func TestModelKillInAPrune(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	who := map[string]killCase{"same": {same: true}, "other": {other: true}, "both": {same: true, other: true}}
	reached := func(t *testing.T, name string, c killCase) {
		t.Run(name, func(t *testing.T) {
			if !reachable(t, 2, func(s *apimodel.Sched) error {
				st := &killStats{}
				if err := killScenario(c, st)(s); err != nil {
					return err
				}
				if st.Violations > 0 {
					return errReached
				}
				return nil
			}) {
				t.Error("no schedule has the hold under a settled flag")
			}
		})
	}
	clean := func(t *testing.T, name string, c killCase) {
		t.Run(name, func(t *testing.T) {
			bound := modelBound(1, 2)
			if c.b {
				bound = 2
			}
			if c.same && c.other {
				if bound = 1; (!c.b || c.landsFirst) && modelBound(0, 1) == 0 {
					t.Skip("searched under CREDENTIALUSE_MODEL_DEEP only")
				}
			}
			st := exhaust(t, bound, func(st *killStats) func(*apimodel.Sched) error { return killScenario(c, st) }, func(st *killStats) {
				if st.Violations != 0 {
					t.Errorf("%d schedules have the hold under a settled flag: %s", st.Violations, st.Shapes)
				}
			})
			if st.Settles == 0 || st.Strips == 0 || st.Died == 0 && c.at != "" {
				t.Errorf("%d schedules settled, %d stripped a record, and the process died in %d; want all reached", st.Settles, st.Strips, st.Died)
			}
		})
	}
	for name, w := range who {
		for _, at := range []string{"read after", "re-add"} {
			c := w
			c.at = at
			fresh := c
			fresh.freshHolders = true
			reached(t, "reached/"+name+"/"+at, c)
			reached(t, "reached/"+name+"/"+at+", holder cache fresh", fresh)
			lives := c
			lives.senderLives = true
			reached(t, "reached/"+name+"/"+at+", sender lives", lives)
			c.b = true
			clean(t, "B/"+name+"/"+at, c)
		}
		for title, off := range map[string]killCase{
			"no kill":                   {},
			"killed before the removal": {at: "removal"},
			"killed after the prune":    {at: "end"},
			"switch outside":            {at: "read after", switchOutside: true},
			"index read outside":        {at: "read after", readOutside: true},
		} {
			off.same, off.other = w.same, w.other
			clean(t, "one off/"+name+"/"+title, off)
		}
		first := w
		first.at, first.landsFirst = "read after", true
		clean(t, "one off/"+name+"/lands before the pass", first)
		if w.same && w.other {
			b := first
			b.b = true
			clean(t, "B/"+name+"/lands before the pass", b)
		}
		var passes []string // the processes that pass in the case
		if w.same {
			passes = append(passes, "disown", "restarted")
		}
		if w.other {
			passes = append(passes, "other")
		}
		for _, who := range passes {
			c := first
			c.early = who
			t.Run("early/"+name+"/"+who, func(t *testing.T) {
				if !reachable(t, 1, func(s *apimodel.Sched) error {
					err := killScenario(c, &killStats{})(s)
					if errors.Is(err, earlyPass{who}) {
						return errReached
					}
					return err
				}) {
					t.Errorf("no schedule has a pass of %s start before the A2 landed: the check can't fire", who)
				}
			})
		}
		if w.same {
			first.recordsFirst = true
			reached(t, "reached/"+name+"/lands before the pass, records first", first)
		}
	}
}
