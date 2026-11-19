package credentialuse

// The (K, N) queue's orderings, enumerated (plan, PR 3b: the user's
// decision). The queue serializes its operations under one lock, so its
// behaviour is a function of the order of its operations: these tests run
// every distinct order of small sets of them on the fake clock, and check,
// after each step and after a final drain, properties that don't restate
// the implementation:
//
//   - a key is never held by two workers at once;
//   - no object is lost: every object added to a key, directly or by a
//     timer that fired, is in a later batch of that key, unless ShutDown
//     dropped it;
//   - no batch has an object that wasn't added to its key since the key's
//     previous batch was taken;
//   - a key is never queued twice: Len never exceeds the keys waiting;
//   - a timer adds nothing before its time, and nothing after ShutDown;
//   - after ShutDown, Get takes nothing and Add queues nothing.
//
// The goroutine races are the D1 probes' (shared_unit_test.go), and the
// stress test's.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// An enumOp is one operation of a scenario.
type enumOp struct {
	kind   string // add, after, step, get, done, rescan, shutdown
	key    string // A or B
	id     string
	worker int
}

func (o enumOp) String() string {
	switch o.kind {
	case "add", "after":
		return o.kind + "(" + o.key + o.id + ")"
	case "get", "done":
		return fmt.Sprintf("%s%d", o.kind, o.worker)
	}
	return o.kind
}

// enumState runs one order against a queue, and checks it.
type enumState struct {
	t     *testing.T
	order string
	clk   *testingclock.FakeClock
	q     *batchQueue
	start time.Time

	held    map[int]batchKey // what each worker holds
	stopped bool
	// owed are the additions not yet in a batch: per key, the objects added
	// since its last take, each with whether ShutDown excuses it.
	owed map[batchKey]map[ID]bool
	// since are the objects added to each key since its last take began,
	// which is all a batch may hold.
	since map[batchKey]map[ID]bool
	// timers are the AddAfters not yet fired: when, and what.
	timers []enumTimer
}

type enumTimer struct {
	at  time.Time
	key batchKey
	id  ID
}

func enumKey(k string) batchKey { return batchKey{widgetGK, k} }
func enumID(id string) ID       { return SecretRef("ns", id).ID() }

const enumDelay = time.Second

func newEnumState(t *testing.T, order string) *enumState {
	clk := testingclock.NewFakeClock(time.Unix(0, 0))
	return &enumState{t: t, order: order, clk: clk, q: newBatchQueue(clk), start: clk.Now(),
		held: map[int]batchKey{}, owed: map[batchKey]map[ID]bool{}, since: map[batchKey]map[ID]bool{}}
}

func (s *enumState) fail(format string, args ...any) {
	s.t.Helper()
	s.t.Fatalf("order %s: %s", s.order, fmt.Sprintf(format, args...))
}

// added notes that id was added to k now, as the queue saw it.
func (s *enumState) added(k batchKey, id ID) {
	if s.stopped {
		return
	}
	if s.owed[k] == nil {
		s.owed[k], s.since[k] = map[ID]bool{}, map[ID]bool{}
	}
	s.owed[k][id] = true
	s.since[k][id] = true
}

func (s *enumState) apply(o enumOp) {
	s.t.Helper()
	switch o.kind {
	case "add":
		s.q.Add(enumKey(o.key), enumID(o.id))
		s.added(enumKey(o.key), enumID(o.id))
	case "rescan":
		// The write loops' Start: every held object's batch, at once.
		for _, k := range []string{"A", "B"} {
			s.q.Add(enumKey(k), enumID("1"))
			s.added(enumKey(k), enumID("1"))
		}
	case "after":
		s.q.AddAfter(enumKey(o.key), enumDelay, enumID(o.id))
		if !s.stopped {
			s.timers = append(s.timers, enumTimer{s.clk.Now().Add(enumDelay), enumKey(o.key), enumID(o.id)})
		}
	case "step":
		s.step(enumDelay / 2)
	case "get":
		if _, busy := s.held[o.worker]; busy || s.q.Len() == 0 {
			return
		}
		k, ids, ok := s.q.Get(context.Background())
		if s.stopped {
			if ok {
				s.fail("Get took %v after ShutDown", k)
			}
			return
		}
		if !ok {
			s.fail("Get took nothing with %d waiting", s.q.Len())
		}
		for w, hk := range s.held {
			if hk == k {
				s.fail("worker %d took %v, which worker %d holds", o.worker, k, w)
			}
		}
		for _, id := range ids {
			if !s.since[k][id] {
				s.fail("the batch of %v has %s, not added since its last take", k, id)
			}
			delete(s.owed[k], id)
		}
		if len(s.owed[k]) != 0 {
			s.fail("the batch of %v is %v, without %v added before it was taken", k, ids, s.owed[k])
		}
		s.since[k] = map[ID]bool{}
		s.held[o.worker] = k
	case "done":
		k, busy := s.held[o.worker]
		if !busy {
			return
		}
		s.q.Done(k)
		delete(s.held, o.worker)
	case "shutdown":
		s.q.ShutDown()
		s.stopped = true
		s.timers = nil
	}
	s.check()
}

// step moves the clock by d, and expects the timers due by then to fire.
func (s *enumState) step(d time.Duration) {
	s.clk.Step(d)
	now := s.clk.Now()
	var left []enumTimer
	for _, tm := range s.timers {
		if !now.Before(tm.at) {
			s.added(tm.key, tm.id)
		} else {
			left = append(left, tm)
		}
	}
	s.timers = left
}

// check holds after every step.
func (s *enumState) check() {
	s.t.Helper()
	waiting := 0
	for k, owed := range s.owed {
		heldNow := false
		for _, hk := range s.held {
			heldNow = heldNow || hk == k
		}
		if len(owed) > 0 && !heldNow {
			waiting++
		}
	}
	if s.stopped {
		if s.q.Len() != 0 {
			s.fail("%d batches wait after ShutDown", s.q.Len())
		}
		return
	}
	if s.q.Len() > waiting {
		s.fail("Len is %d with %d keys waiting: a key is queued twice", s.q.Len(), waiting)
	}
	// A timer not yet due must not have added: its object is owed only if
	// another add owes it too, which the coverage check settles at the end.
}

// drain finishes the run: every worker finishes, every timer fires, and
// batches are taken until none waits. Then nothing may be owed, unless
// ShutDown excused it.
func (s *enumState) drain() {
	s.t.Helper()
	for w := range s.held {
		s.apply(enumOp{kind: "done", worker: w})
	}
	if !s.stopped {
		s.step(10 * enumDelay)
	}
	for range 10 {
		if s.q.Len() == 0 {
			break
		}
		s.apply(enumOp{kind: "get", worker: 0})
		s.apply(enumOp{kind: "done", worker: 0})
	}
	if s.stopped {
		if _, _, ok := s.q.Get(context.Background()); ok {
			s.fail("Get took a batch after ShutDown")
		}
		s.q.Add(enumKey("A"), enumID("9"))
		if s.q.Len() != 0 {
			s.fail("Add queued a batch after ShutDown")
		}
		if s.clk.HasWaiters() {
			s.fail("a timer outlived ShutDown")
		}
		return
	}
	for k, owed := range s.owed {
		if len(owed) > 0 {
			s.fail("lost: %v of %v were never in a batch", owed, k)
		}
	}
}

// permutations calls f with every distinct order of ops.
func permutations(ops []enumOp, f func([]enumOp)) int {
	counts := map[string]int{}
	byName := map[string]enumOp{}
	var names []string
	for _, o := range ops {
		n := o.String()
		if counts[n] == 0 {
			names = append(names, n)
			byName[n] = o
		}
		counts[n]++
	}
	seq := make([]enumOp, 0, len(ops))
	total := 0
	var rec func()
	rec = func() {
		if len(seq) == len(ops) {
			total++
			f(seq)
			return
		}
		for _, n := range names {
			if counts[n] == 0 {
				continue
			}
			counts[n]--
			seq = append(seq, byName[n])
			rec()
			seq = seq[:len(seq)-1]
			counts[n]++
		}
	}
	rec()
	return total
}

func ops(spec string) []enumOp {
	var out []enumOp
	for _, f := range strings.Fields(spec) {
		switch {
		case strings.HasPrefix(f, "add") || strings.HasPrefix(f, "after"):
			kind, rest, _ := strings.Cut(f, ":")
			out = append(out, enumOp{kind: kind, key: rest[:1], id: rest[1:]})
		case strings.HasPrefix(f, "get") || strings.HasPrefix(f, "done"):
			kind := strings.TrimRight(f, "0123456789")
			var w int
			fmt.Sscanf(f[len(kind):], "%d", &w)
			out = append(out, enumOp{kind: kind, worker: w})
		default:
			out = append(out, enumOp{kind: f})
		}
	}
	return out
}

// Every order of each scenario. The scenarios cover what the plan names:
// enqueue, AddAfter firing, the buffering before the write loops run (adds
// before any Get), the rescan, the drain (workers finishing after the last
// add), and stop (ShutDown at every point, with batches held and timers
// pending).
func TestBatchQueueEveryOrdering(t *testing.T) {
	scenarios := map[string]string{
		"buffering and accumulation":  "add:A1 add:A2 add:B1 get1 done1 get2 done2",
		"two workers, one key":        "add:A1 add:A2 get1 get2 done1 done2 add:A1",
		"work, then more for the key": "add:A1 get1 add:A2 add:B1 done1 get2 done2",
		"timers":                      "after:A1 add:A2 after:B2 step step get1 done1",
		"the rescan":                  "rescan add:A2 after:B1 step get1 get2 done1 done2",
		"stop":                        "add:A1 after:B2 get1 step shutdown done1 add:A2",
		"stop with both held":         "add:A1 add:B2 get1 get2 after:A2 shutdown done1 done2",
	}
	total := 0
	for name, spec := range scenarios {
		t.Run(name, func(t *testing.T) {
			n := permutations(ops(spec), func(seq []enumOp) {
				parts := make([]string, len(seq))
				for i, o := range seq {
					parts[i] = o.String()
				}
				s := newEnumState(t, strings.Join(parts, " "))
				for _, o := range seq {
					s.apply(o)
				}
				s.drain()
			})
			t.Logf("%d orders", n)
			total += n
		})
	}
	t.Logf("%d orders in all", total)
}
