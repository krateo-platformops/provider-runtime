package apimodel

import (
	"slices"
	"sync"
)

// ExploreParallel is Explore, the stateless search, split across workers by
// choice prefix (docs/credential-use-model-reduction.md §11.2). It runs
// exactly the schedules Explore runs, each once, in another order: the same
// Runs, the same Usage, and the same set of schedules handed to scenario. It
// differs only in which failures it keeps when there are more than ten, and
// in that scenario runs on several goroutines at once, so it must be safe
// for concurrent use. MaxRuns stops it within a worker's run of the cap.
//
// A worker owns a subtree: a prefix and a floor, the depth above which it
// doesn't backtrack. It searches it depth first, as Explore does. When the
// queue of subtrees runs short, a worker hands off the untried options at
// the shallowest depth it owns that has any, each as a subtree of its own,
// and raises its floor past that depth. So every schedule is in exactly one
// subtree, and no schedule runs only to find the tree's shape.
func ExploreParallel(b Bounds, workers int, scenario func(s *Sched) error) Result {
	if workers < 1 {
		workers = 1
	}
	type subtree struct {
		prefix []int
		floor  int
	}
	var (
		mu      sync.Mutex
		cond    = sync.NewCond(&mu)
		queue   = []subtree{{}}
		busy    int
		stopped bool
		res     Result
	)
	// take blocks until there is a subtree to search, or nothing is left.
	take := func() (subtree, bool) {
		mu.Lock()
		defer mu.Unlock()
		for len(queue) == 0 && busy > 0 && !stopped {
			cond.Wait()
		}
		if len(queue) == 0 || stopped {
			return subtree{}, false
		}
		t := queue[0]
		queue = queue[1:]
		busy++
		return t, true
	}
	done := func() {
		mu.Lock()
		busy--
		cond.Broadcast()
		mu.Unlock()
	}
	// record adds one schedule's result, and says whether to go on.
	record := func(s *Sched, err error) bool {
		mu.Lock()
		defer mu.Unlock()
		res.Runs++
		res.Usage.Merge(s.usage)
		if err != nil {
			res.Failures = append(res.Failures, Failure{Err: err, Trace: s.Trace()})
		}
		if len(res.Failures) >= 10 || (b.MaxRuns > 0 && res.Runs >= b.MaxRuns) {
			stopped = true
			cond.Broadcast()
		}
		return !stopped
	}
	// split hands off the untried options at the shallowest owned depth that
	// has any, when the queue is short; it returns the new floor.
	split := func(choices, options []int, floor int) int {
		mu.Lock()
		defer mu.Unlock()
		if len(queue) >= workers {
			return floor
		}
		for d := floor; d < len(choices); d++ {
			if choices[d]+1 >= options[d] {
				continue
			}
			for c := choices[d] + 1; c < options[d]; c++ {
				queue = append(queue, subtree{append(slices.Clone(choices[:d]), c), d + 1})
			}
			cond.Broadcast()
			return d + 1
		}
		return floor
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				t, ok := take()
				if !ok {
					return
				}
				prefix, floor := t.prefix, t.floor
				for {
					s := newSched(b, prefix, nil, nil)
					err := scenario(s)
					if !record(s, err) {
						break
					}
					floor = split(s.choices, s.options, floor)
					// The next schedule of the subtree: the deepest owned choice
					// with an untried option.
					i := len(s.choices) - 1
					for i >= floor && s.choices[i]+1 >= s.options[i] {
						i--
					}
					if i < floor {
						break
					}
					prefix = append(slices.Clone(s.choices[:i]), s.choices[i]+1)
				}
				done()
			}
		}()
	}
	wg.Wait()
	res.Finished = !stopped && len(queue) == 0
	return res
}
