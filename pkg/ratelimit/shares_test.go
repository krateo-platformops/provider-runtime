package ratelimit

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utilclock "k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
)

// The guaranteed share per controller (docs/design-capacity-model.md §6 and
// §9): a freed slot goes first to a queue that holds less than its share, and
// a queue lends what it doesn't use.

func receiveHeld(t *testing.T, ch <-chan heldItem, what string) heldItem {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: nothing was popped within 5s", what)
		return heldItem{}
	}
}

func noneHeld(t *testing.T, ch <-chan heldItem, what string) {
	t.Helper()
	select {
	case h := <-ch:
		t.Fatalf("%s: %s was popped, want nothing", what, h.key)
	case <-time.After(50 * time.Millisecond):
	}
}

// finish lets h's worker call Done, and waits for it.
func finish(t *testing.T, h heldItem) {
	t.Helper()
	close(h.release)
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Done for %s did not return within 5s", h.key)
	}
}

func backlog(q *gatedQueue[string], prefix string, n int) {
	for i := range n {
		q.Add(prefix + strconv.Itoa(i))
	}
}

func controllerByName(t *testing.T, g *Governor, name string) ControllerSnapshot {
	t.Helper()
	for _, c := range g.Snapshot().Controllers {
		if c.Controller == name {
			return c
		}
	}
	t.Fatalf("no controller %q in the snapshot", name)
	return ControllerSnapshot{}
}

// With the other queue idle, a queue takes every slot: more than its share.
func TestAQueueTakesEverySlotWhileTheOtherIsIdle(t *testing.T) {
	g, _ := saturationGovernor(t, 8, "healthy", false)
	g.setLocalCapacity(4)
	deep, idle := gated(t, g, "lend-deep"), gated(t, g, "lend-idle")
	held, _ := boundWorkers(deep, 6)
	boundWorkers(idle, 2)
	backlog(deep, "d", 10)
	var running []heldItem
	for range 4 {
		running = append(running, receiveHeld(t, held, "the deep queue"))
	}
	noneHeld(t, held, "with every slot taken")
	if c := controllerByName(t, g, "lend-deep"); c.InFlight != 4 || c.Guaranteed != 2 || c.Waiting != 6 {
		t.Errorf("the deep queue: %+v, want 4 in flight over a guaranteed 2, and 6 waiting", c)
	}
	if c := controllerByName(t, g, "lend-idle"); c.InFlight != 0 || c.Guaranteed != 2 || c.Waiting != 0 {
		t.Errorf("the idle queue: %+v, want nothing in flight or waiting, and a guaranteed 2", c)
	}
	for _, h := range running {
		finish(t, h)
	}
}

// A deep-backlog queue and a light one: the light one's item gets a slot
// within one completion, every time, and nothing is preempted for it. Two deep
// queues are here so that plain round-robin would hand the freed slot to the
// other deep queue instead.
func TestALightQueueGetsASlotWithinOneCompletion(t *testing.T) {
	g, _ := saturationGovernor(t, 8, "healthy", false)
	g.setLocalCapacity(0)
	deepA, light, deepB := gated(t, g, "share-deep-a"), gated(t, g, "share-light"), gated(t, g, "share-deep-b")
	heldA, _ := boundWorkers(deepA, 4)
	heldL, _ := boundWorkers(light, 2)
	heldB, _ := boundWorkers(deepB, 4)
	eventually(t, "every worker waiting", func() bool {
		return idleWorkers(deepA) == 4 && idleWorkers(light) == 2 && idleWorkers(deepB) == 4
	})
	backlog(deepA, "a", 30)
	backlog(deepB, "b", 30)

	var deep []heldItem // the deep queues' reconciles in flight, oldest first
	receiveDeep := func(what string) {
		t.Helper()
		select {
		case h := <-heldA:
			deep = append(deep, h)
		case h := <-heldB:
			deep = append(deep, h)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no deep item was popped within 5s", what)
		}
	}
	// finishDeep completes one reconcile of a deep queue that has two or more
	// in flight, so that neither deep queue falls under its own share of 1.
	finishDeep := func() {
		t.Helper()
		inFlight := map[byte]int{}
		for _, h := range deep {
			inFlight[h.key[0]]++
		}
		for i, h := range deep {
			if inFlight[h.key[0]] >= 2 {
				finish(t, h)
				deep = append(deep[:i], deep[i+1:]...)
				return
			}
		}
		t.Fatalf("no deep queue has two reconciles in flight: %v", inFlight)
	}
	// Two slots first: each deep queue is owed one. Then two more, lent to
	// either.
	g.setLocalCapacity(2)
	receiveDeep("the first deep queue's share")
	receiveDeep("the second deep queue's share")
	if deep[0].key[0] == deep[1].key[0] {
		t.Fatalf("both of the first two slots went to one deep queue (%s, %s): each was owed one", deep[0].key, deep[1].key)
	}
	g.setLocalCapacity(4)
	for range 2 {
		receiveDeep("filling the capacity")
	}
	if n := g.inFlight(); n != 4 {
		t.Fatalf("%d slots in flight, want 4", n)
	}
	if c := controllerByName(t, g, "share-light"); c.Guaranteed != 1 {
		t.Fatalf("guaranteed share = %d, want 1: a capacity of 4 over 3 queues", c.Guaranteed)
	}

	for round := range 8 {
		key := "x" + strconv.Itoa(round)
		light.Add(key)
		noneHeld(t, heldL, "with every slot lent to the deep queues")
		if c := controllerByName(t, g, "share-light"); c.Waiting != 1 || c.InFlight != 0 {
			t.Fatalf("round %d: the light queue: %+v, want 1 waiting and none in flight", round, c)
		}

		// One completion of a deep queue.
		finishDeep()
		got := receiveHeld(t, heldL, "the light queue, after one completion")
		if got.key != key {
			t.Fatalf("round %d: the light queue popped %s, want %s", round, got.key, key)
		}
		if len(heldA)+len(heldB) != 0 {
			t.Fatalf("round %d: a deep queue was handed the freed slot while the light one was owed", round)
		}
		if n := g.inFlight(); n != 4 {
			t.Fatalf("round %d: %d slots in flight, want 4", round, n)
		}

		// The light queue's slot, given back with nothing more to run, is
		// lent to a deep queue again.
		finish(t, got)
		receiveDeep("lending the light queue's slot")
	}
	for _, h := range deep {
		finish(t, h)
	}
}

// With local capacity under the number of queues, each queue's share is 1 and
// they can't all be met: the owed queues are served round-robin, so each still
// runs, and none holds two slots while another with work holds none.
func TestEveryQueueRunsWithLocalUnderTheNumberOfQueues(t *testing.T) {
	g, _ := saturationGovernor(t, 8, "healthy", false)
	g.setLocalCapacity(0)
	names := []string{"few-a", "few-b", "few-c"}
	merged := make(chan heldItem, 1024)
	for _, name := range names {
		q := gated(t, g, name)
		held, _ := boundWorkers(q, 2)
		eventually(t, "the workers of "+name+" waiting", func() bool { return idleWorkers(q) == 2 })
		backlog(q, name+"/", 12)
		go func() {
			for h := range held {
				merged <- h
			}
		}()
	}
	g.setLocalCapacity(2)
	queueOf := func(h heldItem) string { return h.key[:len("few-a")] }

	running := []heldItem{receiveHeld(t, merged, "the first slot"), receiveHeld(t, merged, "the second slot")}
	grants := map[string]int{queueOf(running[0]): 1, queueOf(running[1]): 1}
	var order []string
	for range 24 {
		noneHeld(t, merged, "at capacity")
		if a, b := queueOf(running[0]), queueOf(running[1]); a == b {
			t.Fatalf("%s holds both slots while the other queues have work (grants so far %v)", a, order)
		}
		finish(t, running[0])
		next := receiveHeld(t, merged, "the freed slot")
		running = []heldItem{running[1], next}
		grants[queueOf(next)]++
		order = append(order, queueOf(next))
	}
	for _, name := range names {
		if grants[name] < 6 {
			t.Errorf("%s was granted %d of 26 slots, want about a third: grants %v", name, grants[name], order)
		}
	}
	// Round-robin among the owed: no queue waits for more than the two others.
	for i := 2; i < len(order); i++ {
		if order[i] == order[i-1] || order[i] == order[i-2] || order[i-1] == order[i-2] {
			t.Fatalf("grants %v: not round-robin at %d", order, i)
		}
	}
	for _, h := range running {
		finish(t, h)
	}
}

// Shutdown with shares: a queue that shuts down keeps the slots of its running
// reconciles until their Done, stops counting towards the shares, and every
// slot comes back.
func TestShutDownWithSharesGivesEverySlotBack(t *testing.T) {
	g, _ := saturationGovernor(t, 8, "healthy", false)
	g.setLocalCapacity(0)
	names := []string{"down-a", "down-b", "down-c"}
	queues := map[string]*gatedQueue[string]{}
	held := map[string]<-chan heldItem{}
	waits := map[string]func(){}
	for _, name := range names {
		q := newGatedQueue[string](g, name, nil, utilclock.RealClock{})
		queues[name] = q
		held[name], waits[name] = boundWorkers(q, 4)
		eventually(t, "the workers of "+name+" waiting", func() bool { return idleWorkers(q) == 4 })
		backlog(q, name+"/", 10)
	}
	t.Cleanup(func() {
		for _, q := range queues {
			shutDownWithin(t, q)
		}
	})
	// Each queue is owed 2 of the 6, and has the backlog to take them. The
	// capacity comes in two steps of 3, so that no queue is in the middle of
	// a pop while another slot is handed out.
	running := map[string][]heldItem{}
	for range 2 {
		g.setLocalCapacity(g.LocalCapacity() + 3)
		for _, name := range names {
			running[name] = append(running[name], receiveHeld(t, held[name], name))
		}
	}
	for _, name := range names {
		noneHeld(t, held[name], name+" at its share with the capacity full")
	}
	if n := g.inFlight(); n != 6 {
		t.Fatalf("%d slots in flight, want 6", n)
	}

	within(t, "ShutDown", queues["down-b"].ShutDown)
	if n := g.inFlight(); n != 6 {
		t.Fatalf("%d slots in flight after one queue shut down, want 6: running reconciles keep theirs", n)
	}
	if c := controllerByName(t, g, "down-a"); c.Guaranteed != 3 {
		t.Errorf("guaranteed share = %d with two queues left, want 3", c.Guaranteed)
	}
	// The shut-down queue's slots, given back, go to the two that are left.
	for _, h := range running["down-b"] {
		finish(t, h)
	}
	running["down-a"] = append(running["down-a"], receiveHeld(t, held["down-a"], "down-a's third slot"))
	running["down-c"] = append(running["down-c"], receiveHeld(t, held["down-c"], "down-c's third slot"))
	noneHeld(t, held["down-b"], "a queue that shut down")

	for _, name := range []string{"down-a", "down-c"} {
		within(t, "ShutDown", queues[name].ShutDown)
	}
	if n := g.inFlight(); n != 6 {
		t.Fatalf("%d slots in flight with every queue shut down and 6 reconciles running, want 6", n)
	}
	for _, name := range []string{"down-a", "down-c"} {
		for _, h := range running[name] {
			finish(t, h)
		}
	}
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d slots in flight after every Done, want 0", n)
	}
	for _, name := range names {
		within(t, "the workers of "+name, waits[name])
		if h := queues[name].stats.held.Load(); h != 0 {
			t.Errorf("%s reports %d slots held after shutdown, want 0", name, h)
		}
	}
	if cs := g.Snapshot().Controllers; len(cs) != 0 {
		t.Errorf("the snapshot still has controllers after every queue shut down: %+v", cs)
	}
}

// checkCounts verifies, under q.mu, that what the queue reports for metrics is
// what its record holds.
func checkCounts(q *gatedQueue[string]) error {
	for deadline := time.Now().Add(5 * time.Second); !q.mu.TryLock(); time.Sleep(100 * time.Microsecond) {
		if time.Now().After(deadline) {
			return fmt.Errorf("the queue's lock was not free within 5s")
		}
	}
	defer q.mu.Unlock()
	if held, want := q.stats.held.Load(), int64(q.granted+len(q.processing)); held != want {
		return fmt.Errorf("reports %d slots held, the record has %d", held, want)
	}
	if waiting, want := q.stats.waiting.Load(), int64(q.free-q.granted); waiting != want {
		return fmt.Errorf("reports %d items waiting, the record has %d", waiting, want)
	}
	return nil
}

// The stress tests of the gated queue, with several queues on one Governor and
// a local capacity that moves: by the sampler, with the capacity model on, and
// by a stand-in that sets it outright. Adds of every kind, Done from the
// workers themselves, the records checked while the load runs; then every queue
// must drain, every slot come back, and every queue have run.
//
// RLGOV_CONCURRENT_RUNS sets the number of runs (default 10).
func TestGatedQueuesStayExactWithSharesAndAMovingLocal(t *testing.T) {
	runs := 10
	if v, err := strconv.Atoi(os.Getenv("RLGOV_CONCURRENT_RUNS")); err == nil && v > 0 {
		runs = v
	}
	before := brokenBooks.Load()
	for run := range runs {
		if !t.Run(strconv.Itoa(run), func(t *testing.T) { sharedRun(t, run) }) {
			break
		}
	}
	if n := brokenBooks.Load() - before; n != 0 {
		t.Errorf("the record disagreed with what happened %d time(s)", n)
	}
}

func sharedRun(t *testing.T, run int) {
	f := newModelFeed(t, modelOn())
	g := f.g
	g.setLocalCapacity(1 + run%4)
	nQueues := 2 + run%3
	limiter := fixedLimiter{delays: map[string]time.Duration{}}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		limiter.delays[keys[i]] = time.Duration(i%3) * time.Millisecond
	}
	queues := make([]*gatedQueue[string], nQueues)
	var workers sync.WaitGroup
	for i := range queues {
		q := newGatedQueue[string](g, "shared"+strconv.Itoa(run)+"-"+strconv.Itoa(i), limiter, utilclock.RealClock{})
		queues[i] = q
		for range 1 + (run+i)%5 {
			workers.Go(func() {
				for {
					k, _, shut := q.GetWithPriority()
					if shut {
						return
					}
					time.Sleep(time.Duration(rand.IntN(100)) * time.Microsecond)
					q.Done(k)
				}
			})
		}
	}

	stop := make(chan struct{})
	var load sync.WaitGroup
	for _, q := range queues {
		for range 2 {
			load.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					k := keys[rand.IntN(len(keys))]
					p := rand.IntN(5) - 2
					switch rand.IntN(4) {
					case 0, 1:
						q.AddWithOpts(priorityqueue.AddOpts{Priority: &p}, k)
					case 2:
						q.AddWithOpts(priorityqueue.AddOpts{After: time.Duration(rand.IntN(3000)) * time.Microsecond, Priority: &p}, k)
					default:
						q.AddRateLimited(k)
					}
					time.Sleep(time.Duration(rand.IntN(20)) * time.Microsecond)
				}
			})
		}
	}
	var samples atomic.Int64
	load.Go(func() { // the sampler, and a stand-in that sets local outright
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(1+rand.IntN(3)) * time.Millisecond):
			}
			if rand.IntN(3) == 0 {
				g.setLocalCapacity(1 + rand.IntN(6))
				continue
			}
			// A second of the container's life: CPU used up to the 500m
			// limit, sometimes stalled, read by the real sampler.
			f.c.add(time.Second)
			f.usage += time.Duration(rand.IntN(500)) * time.Millisecond
			if rand.IntN(4) == 0 {
				f.stall += time.Second
			}
			g.sample()
			samples.Add(1)
		}
	})
	var checked atomic.Int64
	checkErr := make(chan error, 1)
	load.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Microsecond):
			}
			for _, q := range queues {
				err := checkBooks(q)
				if err == nil {
					err = checkCounts(q)
				}
				if err != nil {
					select {
					case checkErr <- fmt.Errorf("%s: %w", q.name, err):
					default:
					}
					return
				}
			}
			g.Snapshot()
			checked.Add(1)
		}
	})
	time.Sleep(200 * time.Millisecond)
	close(stop)
	within(t, "stopping the load", load.Wait)
	select {
	case err := <-checkErr:
		t.Fatalf("while running: %v", err)
	default:
	}

	g.setLocalCapacity(2)
	deadline := time.Now().Add(drainDeadline)
	for {
		drained, state := true, ""
		for _, q := range queues {
			lockWithin(t, q, "draining")
			drained = drained && len(q.waiting) == 0 && len(q.ready) == 0 && len(q.processing) == 0 && q.free == 0 && q.granted == 0 && q.inner.Len() == 0
			state += fmt.Sprintf("%s: waiting %d ready %v processing %v free %d granted %d inner.Len %d; ", q.name, len(q.waiting), q.ready, q.processing, q.free, q.granted, q.inner.Len())
			q.mu.Unlock()
		}
		if drained && g.inFlight() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not drain within %s: %s local slots in flight %d", drainDeadline, state, g.inFlight())
		}
		time.Sleep(time.Millisecond)
	}
	var completions int64
	for _, q := range queues {
		if err := checkBooks(q); err != nil {
			t.Fatalf("%s after draining: %v", q.name, err)
		}
		if err := checkCounts(q); err != nil {
			t.Fatalf("%s after draining: %v", q.name, err)
		}
		if h, w := q.stats.held.Load(), q.stats.waiting.Load(); h != 0 || w != 0 {
			t.Errorf("%s reports %d slots held and %d items waiting after draining, want none", q.name, h, w)
		}
		n := q.stats.completions.Load()
		if n == 0 {
			t.Errorf("%s completed nothing: a queue with work must run whatever the others do", q.name)
		}
		if a := q.stats.arrivals.Load(); a != n {
			t.Errorf("%s: %d arrivals and %d completions after draining, want them equal", q.name, a, n)
		}
		completions += n
	}
	if got := g.completions.Load(); got != completions {
		t.Errorf("the Governor counted %d completions, the queues %d", got, completions)
	}
	if checked.Load() == 0 || samples.Load() == 0 {
		t.Errorf("the records were checked %d time(s) and the sampler ran %d time(s) while the load ran, want both", checked.Load(), samples.Load())
	}
	for _, q := range queues {
		within(t, "ShutDown", q.ShutDown)
	}
	within(t, "the workers", workers.Wait)
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d local slots in flight after shutdown, want 0", n)
	}
}
