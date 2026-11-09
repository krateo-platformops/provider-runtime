package apimodel

import (
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The scheduler runs actors so that exactly one runs at a time. Each actor
// is a coroutine (iter.Pull): it runs until its next yield point, a Call or a
// Wait, and hands control back. There the scheduler chooses which actor goes
// next, and whether its call fails. A call runs atomically right after it is
// chosen, so a schedule is the order of the calls, and replaying the same
// choices replays the same run. Coroutine switches stay on one thread, so a
// step costs far less than a channel handoff between goroutines.
//
// Exploring every schedule of a scenario is a depth-first search over those
// choices, by replay. It is bounded the usual way for stateless model
// checking: by the number of preemptions (switching away from an actor that
// could go on, as in CHESS) and by the number of injected faults. Within the
// bounds the search is exhaustive, and Explore says whether it finished.

// A Fault is what the scheduler does to one call.
type Fault int

const (
	// FaultNone: the call runs and returns its result.
	FaultNone Fault = iota
	// FaultError: the call doesn't run, and returns an error.
	FaultError
	// FaultLost: the call runs, but its response is lost: it returns an
	// error, as a timeout would.
	FaultLost
	// FaultCrash: the call doesn't run, and the actor's process dies. Every
	// call returns ErrCrashed until the actor calls Restart.
	FaultCrash
)

func (f Fault) String() string {
	return [...]string{"", "!error", "!lost", "!crash"}[f]
}

var (
	// ErrInjected is FaultError's error: the server was unavailable.
	ErrInjected = apierrors.NewServiceUnavailable("apimodel: injected error")
	// ErrLost is FaultLost's error. It carries no API status, as a timeout
	// or a reset connection doesn't.
	ErrLost = errors.New("apimodel: the response was lost")
	// ErrCrashed is returned to a crashed actor until it restarts.
	ErrCrashed = errors.New("apimodel: the process crashed")
)

// errStopped unwinds an actor the run no longer needs.
var errStopped = errors.New("apimodel: stopped")

// Clock is the scheduler's logical clock: it moves 1ms on every step, and
// jumps forward when only waiting actors are left.
type Clock struct{ t time.Time }

// Now returns the clock's time.
func (c *Clock) Now() time.Time { return c.t }

// Since returns the time since t.
func (c *Clock) Since(t time.Time) time.Duration { return c.t.Sub(t) }

// Bounds limit one exploration.
type Bounds struct {
	// Preemptions is how often a schedule may switch away from an actor that
	// could go on.
	Preemptions int
	// Faults is how many faults one schedule may inject.
	Faults int
	// MaxRuns stops the search; Explore then reports it unfinished.
	MaxRuns int
}

// Flags say how the scheduler treats an actor.
type Flags int

const (
	// Daemon: the actor, such as an informer, doesn't keep the run going:
	// the run ends when only daemons are left with nothing to do.
	Daemon Flags = 1 << iota
	// Faulty: the scheduler may inject faults into the actor's calls. Other
	// actors' calls always succeed.
	Faulty
)

// Sched runs one schedule.
type Sched struct {
	Clock  *Clock
	bounds Bounds
	actors []*Actor

	// The choices to replay, then the ones made.
	prefix  []int
	choices []int
	options []int
	trace   []string

	preemptions, faults int
	last                *Actor
	rng                 *rand.Rand // for random walks; nil means the first option
}

// An Actor is one coroutine of a scenario.
type Actor struct {
	Name   string
	s      *Sched
	daemon bool
	faulty bool

	next  func() (struct{}, bool)
	stop  func()
	yield func(struct{}) bool
	fault Fault // what the scheduler chose for the step it resumes

	// Where it is parked.
	label     string
	faultable bool
	notBefore time.Time
	cond      func() bool
	done      bool
	crashed   bool
}

func newSched(b Bounds, prefix []int, rng *rand.Rand) *Sched {
	return &Sched{
		Clock:  &Clock{t: time.Date(2026, 11, 8, 16, 26, 0, 0, time.UTC)},
		bounds: b,
		prefix: prefix,
		rng:    rng,
	}
}

// Go adds an actor. It starts parked: nothing of it runs until the scheduler
// chooses it.
func (s *Sched) Go(name string, flags Flags, f func(a *Actor)) {
	a := &Actor{Name: name, s: s, daemon: flags&Daemon != 0, faulty: flags&Faulty != 0, label: "start"}
	a.next, a.stop = iter.Pull(func(yield func(struct{}) bool) {
		defer func() {
			if r := recover(); r != nil && r != errStopped { //nolint:errorlint // the sentinel itself
				panic(r)
			}
		}()
		a.yield = yield
		f(a)
	})
	s.actors = append(s.actors, a)
}

// park hands control back to the scheduler, and returns the fault it chose
// when it resumes the actor.
func (a *Actor) park(label string, faultable bool, notBefore time.Time, cond func() bool) Fault {
	a.label, a.faultable, a.notBefore, a.cond = label, faultable, notBefore, cond
	if !a.yield(struct{}{}) {
		panic(errStopped)
	}
	return a.fault
}

// Call is an API call: the scheduler chooses when it runs, and whether it
// fails.
func (a *Actor) Call(label string, op func() error) error {
	if a.crashed {
		return ErrCrashed
	}
	switch a.park(label, true, time.Time{}, nil) {
	case FaultError:
		return ErrInjected
	case FaultLost:
		_ = op()
		return ErrLost
	case FaultCrash:
		a.crashed = true
		return ErrCrashed
	}
	return op()
}

// Wait parks the actor until cond holds (nil: always) and the clock has
// reached notBefore (zero: now). It is a yield point that can't fail.
func (a *Actor) Wait(label string, notBefore time.Time, cond func() bool) {
	if a.crashed {
		return
	}
	a.park(label, false, notBefore, cond)
}

// Crashed reports whether the actor's process has crashed.
func (a *Actor) Crashed() bool { return a.crashed }

// Restart brings a crashed actor's process back: its calls run again.
func (a *Actor) Restart() { a.crashed = false }

// Now is the scheduler's clock.
func (a *Actor) Now() time.Time { return a.s.Clock.Now() }

// Clock is the scheduler's clock, for code that takes one.
func (a *Actor) Clock() *Clock { return a.s.Clock }

type option struct {
	actor *Actor
	fault Fault
}

func (a *Actor) enabled(now time.Time) bool {
	return !a.done && !a.notBefore.After(now) && (a.cond == nil || a.cond())
}

// resume runs a until its next yield point, or its end.
func (a *Actor) resume(f Fault) {
	a.fault = f
	if _, ok := a.next(); !ok {
		a.done = true
	}
}

// Run runs the schedule to its end; a scenario calls it once its actors are
// added. Every actor still parked at the end is stopped, so nothing outlives
// its schedule.
func (s *Sched) Run() error {
	defer func() {
		for _, a := range s.actors {
			if !a.done {
				a.stop()
				a.done = true
			}
		}
	}()
	for {
		var opts []option
		now := s.Clock.Now()
		for _, a := range s.actors {
			if !a.enabled(now) {
				continue
			}
			if s.last != nil && s.last != a && s.last.enabled(now) && s.preemptions >= s.bounds.Preemptions {
				continue // no preemption left: the last actor must go on
			}
			opts = append(opts, option{a, FaultNone})
			if a.faultable && a.faulty && s.faults < s.bounds.Faults {
				opts = append(opts, option{a, FaultError}, option{a, FaultLost}, option{a, FaultCrash})
			}
		}
		if len(opts) == 0 {
			// Nothing can go now: jump to the earliest waiting actor, or end.
			var next time.Time
			live := false
			for _, a := range s.actors {
				if a.done || a.daemon {
					continue
				}
				live = true
				if !a.notBefore.IsZero() && a.notBefore.After(now) && (next.IsZero() || a.notBefore.Before(next)) {
					next = a.notBefore
				}
			}
			if !live {
				return nil
			}
			if next.IsZero() {
				return fmt.Errorf("apimodel: deadlock: %s", s.parked())
			}
			s.Clock.t = next
			continue
		}
		o := opts[s.choose(len(opts))]
		if s.last != nil && s.last != o.actor && s.last.enabled(now) {
			s.preemptions++
		}
		if o.fault != FaultNone {
			s.faults++
		}
		s.last = o.actor
		s.trace = append(s.trace, o.actor.Name+":"+o.actor.label+o.fault.String())
		s.Clock.t = s.Clock.t.Add(time.Millisecond)
		o.actor.resume(o.fault)
	}
}

func (s *Sched) parked() string {
	var b strings.Builder
	for _, a := range s.actors {
		if !a.done {
			fmt.Fprintf(&b, "%s@%s ", a.Name, a.label)
		}
	}
	return b.String()
}

func (s *Sched) choose(n int) int {
	i := 0
	switch {
	case len(s.choices) < len(s.prefix):
		i = s.prefix[len(s.choices)]
	case s.rng != nil:
		i = s.rng.Intn(n)
	}
	if i >= n {
		i = n - 1
	}
	s.choices = append(s.choices, i)
	s.options = append(s.options, n)
	return i
}

// Trace is the schedule that ran, one step per entry: actor:label, with any
// fault.
func (s *Sched) Trace() string { return strings.Join(s.trace, " ") }

// A Failure is a schedule a scenario's checks refused.
type Failure struct {
	Err   error
	Trace string
}

// Result is one exploration.
type Result struct {
	Runs     int
	Finished bool
	Failures []Failure
}

// Explore runs scenario once per schedule within b, depth first. scenario
// builds its actors on s, then calls s.Run, then checks the run and returns
// an error for a refused one. It stops at the first 10 failures.
func Explore(b Bounds, scenario func(s *Sched) error) Result {
	var res Result
	var prefix []int
	for {
		if b.MaxRuns > 0 && res.Runs >= b.MaxRuns {
			return res
		}
		s := newSched(b, prefix, nil)
		err := scenario(s)
		res.Runs++
		if err != nil {
			res.Failures = append(res.Failures, Failure{Err: err, Trace: s.Trace()})
			if len(res.Failures) >= 10 {
				return res
			}
		}
		// The next schedule: the deepest choice with an untried option.
		i := len(s.choices) - 1
		for i >= 0 && s.choices[i]+1 >= s.options[i] {
			i--
		}
		if i < 0 {
			res.Finished = true
			return res
		}
		prefix = append(append([]int(nil), s.choices[:i]...), s.choices[i]+1)
	}
}

// Walk runs scenario on n random schedules within b, seeded with seed.
func Walk(b Bounds, n int, seed int64, scenario func(s *Sched) error) Result {
	var res Result
	// A seeded generator, so that a failing schedule replays: crypto/rand
	// can't be seeded, and nothing here is security.
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // #nosec G404 -- reproducible schedules, not security
	for range n {
		s := newSched(b, nil, rng)
		if err := scenario(s); err != nil {
			res.Failures = append(res.Failures, Failure{Err: err, Trace: s.Trace()})
			if len(res.Failures) >= 10 {
				break
			}
		}
		res.Runs++
	}
	res.Finished = true
	return res
}
