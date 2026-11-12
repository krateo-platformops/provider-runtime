package apimodel

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The scheduler runs actors so that exactly one runs at a time.
//
// Time is event-driven (docs/credential-use-model-reduction.md §3.1): the
// clock ticks on each call step, by at most BaseTick, and on no other step,
// and moves further only by a jump to a pending deadline. A schedule may choose up to
// Bounds.Jumps jumps, at any step; when nothing else can go, the clock jumps
// to the earliest notBefore, uncounted. Deadlines are every actor's
// notBefore and whatever Sched.Deadlines returns (grace ends, a scenario's
// thresholds). A call's tick never reaches a pending deadline (see tick), so
// that only jumps cross them; one that can't fit panics.
//
// A crash loses only the work that has made a call (§3.2): each actor
// records its process's generation at the first call of its unit of work,
// and a call returns ErrCrashed if the process restarted since. Restart and
// Wait end a unit. Each actor
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
// could go on, as in CHESS) and by the number of injected faults. As in
// CHESS, only visible steps count: a quiet resume (§4) is never a
// preemption and never the last actor, whether rule R runs it silently or
// as a step of its own (§5, C-bounds). Within the bounds the search is
// exhaustive, and Explore says whether it finished.

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
	// FaultCrash: the call doesn't run, and the actor's process dies and
	// restarts (see Proc). Every unit of work of the process that has made a
	// call, this one included, gets ErrCrashed from its calls until it ends
	// (Restart or Wait).
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

// Clock is the scheduler's logical clock: it ticks on every call step, and
// jumps to deadlines.
type Clock struct {
	t time.Time
	s *Sched
}

// Now returns the clock's time. A reading by an actor's code is one of its
// inputs (docs/credential-use-model-reduction.md §10.1).
func (c *Clock) Now() time.Time {
	if c.s != nil && c.s.inQuiet {
		panic("apimodel: a quiet segment read the clock (§4, M-a)")
	}
	if c.s != nil && c.s.running != nil {
		c.s.running.RecordInput(TimeInput(c.t))
	}
	return c.t
}

// TimeInput is a time an actor read, as recorded among its inputs.
type TimeInput time.Time

// Since panics: in the model, time is only compared, stored, or offset by a
// constant (docs/credential-use-model-reduction.md §10.2), and a duration
// read off the clock would be a value.
func (c *Clock) Since(time.Time) time.Duration {
	panic("apimodel: Clock.Since is not allowed in the model: time is only compared, stored, or offset by a constant")
}

// Bounds limit one exploration.
type Bounds struct {
	// Preemptions is how often a schedule may switch away from an actor that
	// could go on.
	Preemptions int
	// Faults is how many faults one schedule may inject.
	Faults int
	// Jumps is how many times one schedule may move the clock to a pending
	// deadline by choice (the forced jump, when nothing else can go, isn't
	// counted).
	Jumps int
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
	// QuietStart: the actor's start is a quiet park (§4): the code from its
	// first line to its first park is a quiet segment.
	QuietStart
)

// A Proc is a process: actors that share its memory, so that a crash of any
// of them is a crash of all. restart runs at the crash, before any actor
// runs again, and resets the process's memory, in place, to a fresh
// process's.
type Proc struct {
	Name    string
	restart func(now time.Time)
	gen     int
	crashes int
}

// Crashes is how often the process has crashed in this schedule.
func (p *Proc) Crashes() int { return p.crashes }

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

	preemptions, faults, jumps int
	last                       *Actor
	running                    *Actor
	rng                        *rand.Rand // for random walks; nil means the first option

	// Deadlines, if set, returns the scenario's pending deadlines besides the
	// actors' notBefores: grace ends, and the thresholds it declares.
	Deadlines func() []time.Time

	// Key, if set, returns the scenario's abstract state, for a stateful
	// search (§10): everything but the scheduler's own part, which Run adds.
	Key func() string
	// NoQuiet turns rule R off (§4): each quiet resume is a step of its own,
	// still invisible to the preemption bound (never a preemption, never the
	// last actor). The differential test compares the two.
	NoQuiet bool
	inQuiet bool
	// Terms, if set, returns every live time term of the state (§10.2), for
	// tick's invariant.
	Terms func() []time.Time

	visited   *Visited
	pruned    bool
	prunedAt  Witness
	prunedKey string

	// For the merge audit's replays.
	recordKeys bool
	keys       []string
	maxSteps   int
	// labelPlan, when set, is the labelled trace a replay follows
	// (ReplayLabels); unmatched collects the entries no option matched.
	labelPlan, unmatched []string
	// lastOption makes a replay past its prefix take the last option every
	// time: jumps, then faults, come last.
	lastOption bool
	// left, when set, is the budget the replay keeps once past its prefix:
	// the prefix replays under the original bounds, so that its choice
	// indices mean what they meant.
	left *Budget

	// For the capacity condition (§10.2): calls since the last jump, chosen
	// or forced; and what the schedule used, for Result.
	callsSinceJump int
	usage          Usage
}

// Usage is what one schedule, or the most of any schedule in a search, used
// of the time representation's capacity (§10.2): the most calls between two
// jumps, the most jumps (chosen and forced), and the smallest tick.
type Usage struct {
	MaxCallsBetweenJumps, MaxJumps int
	MinTick                        time.Duration
}

// Merge keeps the most of u and o.
func (u *Usage) Merge(o Usage) {
	u.MaxCallsBetweenJumps = max(u.MaxCallsBetweenJumps, o.MaxCallsBetweenJumps)
	u.MaxJumps = max(u.MaxJumps, o.MaxJumps)
	if o.MinTick > 0 && (u.MinTick == 0 || o.MinTick < u.MinTick) {
		u.MinTick = o.MinTick
	}
}

// ReplayLabels runs one schedule that follows a labelled trace (each step's
// "actor:label!fault" or "jump:+d", as Trace writes them), not choice
// indices: the declared mappings of the gate (§10.4) and debugging use it.
// Trace entries that match no option (a quiet resume the run took silently)
// are skipped. It returns the steps it couldn't follow.
func ReplayLabels(b Bounds, labels []string, scenario func(s *Sched) error) ([]string, error) {
	s := newSched(b, nil, nil, nil)
	s.labelPlan = labels
	err := scenario(s)
	return s.unmatched, err
}

func (s *Sched) planned(opts []option, now time.Time) option {
	for len(s.labelPlan) > 0 {
		want := s.labelPlan[0]
		s.labelPlan = s.labelPlan[1:]
		for i, o := range opts {
			var got string
			if !o.jump.IsZero() {
				got = "jump:+" + o.jump.Sub(now).String()
			} else {
				got = o.actor.Name + ":" + o.actor.label + o.fault.String()
			}
			if got == want {
				s.choices = append(s.choices, i)
				s.options = append(s.options, len(opts))
				return o
			}
		}
		s.unmatched = append(s.unmatched, want)
	}
	s.choices = append(s.choices, 0)
	s.options = append(s.options, len(opts))
	return opts[0]
}

// ErrCapped ends an audit replay at its step cap.
var ErrCapped = errors.New("apimodel: the audit replay reached its step cap")

// ErrPruned ends a run that reached a state already searched with a budget
// at least as large (§10.3).
var ErrPruned = errors.New("apimodel: pruned at a visited state")

// Pruned reports whether the run ended at a visited state.
func (s *Sched) Pruned() bool { return s.pruned }

// A Budget is what a schedule has left of its bounds.
type Budget struct{ Preemptions, Faults, Jumps int }

func (b Budget) dominates(o Budget) bool {
	return b.Preemptions >= o.Preemptions && b.Faults >= o.Faults && b.Jumps >= o.Jumps
}

// Visited is a stateful search's memory: every abstract state reached, with
// the budgets it was reached with, none dominated by another (§10.3).
type Visited struct {
	m map[string][]Budget
	// witness[key] is the first path that reached the state, and the
	// budget it had, for the merge audit (§10.5).
	witness map[string]Witness
	// States counts distinct abstract states; Flagged counts, per flag the
	// scenario's key reports, the distinct states in which it is set.
	States int
}

// NewVisited returns an empty memory.
func NewVisited() *Visited {
	return &Visited{m: map[string][]Budget{}, witness: map[string]Witness{}}
}

// A Witness is a path to a state: its choices, and the budget it had left
// there.
type Witness struct {
	Choices []int
	Left    Budget
	Key     string
}

// see reports whether key was reached with a budget that dominates b; if
// not, it records b, dropping the budgets b dominates.
func (v *Visited) see(key string, b Budget, choices []int) bool {
	bs, ok := v.m[key]
	for _, o := range bs {
		if o.dominates(b) {
			return true
		}
	}
	if !ok {
		v.States++
		v.witness[key] = Witness{slices.Clone(choices), b, key}
	}
	keep := bs[:0]
	for _, o := range bs {
		if !b.dominates(o) {
			keep = append(keep, o)
		}
	}
	v.m[key] = append(keep, b)
	return false
}

// An Actor is one coroutine of a scenario.
type Actor struct {
	Name   string
	s      *Sched
	proc   *Proc
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

	// unitGen is the process generation at the first call of the current
	// unit of work, or 0 before it.
	unitGen int

	// quiet: the actor is parked at a quiet park (§4); nextQuiet: the park
	// it is about to make is one.
	quiet, nextQuiet bool

	// inputs are what the actor's code received in its current unit: call
	// results, values it read from shared state, times it read (§10.1).
	inputs []any
	// Locals, if set, returns the actor's declared persistent locals.
	Locals func() []any
}

// RecordInput adds v to the actor's inputs in its current unit.
func (a *Actor) RecordInput(v any) { a.inputs = append(a.inputs, v) }

// An ActorView is an actor's part of the abstract state.
type ActorView struct {
	Name, Label          string
	Done, Daemon, AtCall bool
	NotBefore            time.Time
	Unit                 string // "idle", "begun" or "stale"
	Inputs               []any
	Locals               []any
}

// Views returns every actor's part of the state, in registration order.
func (s *Sched) Views() []ActorView {
	out := make([]ActorView, 0, len(s.actors))
	for _, a := range s.actors {
		v := ActorView{Name: a.Name, Label: a.label, Done: a.done, Daemon: a.daemon, AtCall: a.faultable, NotBefore: a.notBefore, Inputs: a.inputs}
		switch {
		case a.Stale():
			v.Unit = "stale"
		case a.unitGen != 0:
			v.Unit = "begun"
		default:
			v.Unit = "idle"
		}
		if a.Locals != nil {
			v.Locals = a.Locals()
		}
		out = append(out, v)
	}
	return out
}

// Now returns the clock, for the state hash, without recording a read.
func (s *Sched) Now() time.Time { return s.Clock.t }

// Start is the clock at the start of every schedule: on a whole hour, the
// unit U every time constant is a multiple of (§10.2).
var Start = time.Date(2026, 11, 8, 16, 0, 0, 0, time.UTC)

func newSched(b Bounds, prefix []int, rng *rand.Rand, visited *Visited) *Sched {
	if err := CheckCapacity(b.Jumps); err != nil {
		panic(err.Error())
	}
	s := &Sched{
		bounds:  b,
		prefix:  prefix,
		rng:     rng,
		visited: visited,
	}
	s.Clock = &Clock{t: Start, s: s}
	return s
}

// Proc adds a process. restart, if set, runs when it crashes.
func (s *Sched) Proc(name string, restart func(now time.Time)) *Proc {
	return &Proc{Name: name, restart: restart}
}

// Go adds an actor of its own process. It starts parked: nothing of it runs
// until the scheduler chooses it.
func (s *Sched) Go(name string, flags Flags, f func(a *Actor)) {
	s.GoIn(nil, name, flags, f)
}

// GoIn adds an actor to process p (nil: a process of its own, with nothing
// to restart).
func (s *Sched) GoIn(p *Proc, name string, flags Flags, f func(a *Actor)) {
	if p == nil {
		p = &Proc{Name: name}
	}
	a := &Actor{Name: name, s: s, proc: p, daemon: flags&Daemon != 0, faulty: flags&Faulty != 0, label: "start", quiet: flags&QuietStart != 0}
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
	a.quiet, a.nextQuiet = a.nextQuiet, false
	if !a.yield(struct{}{}) {
		panic(errStopped)
	}
	return a.fault
}

// Call is an API call: the scheduler chooses when it runs, and whether it
// fails.
func (a *Actor) Call(label string, op func() error) error {
	if a.Stale() {
		return ErrCrashed
	}
	f := a.park(label, true, time.Time{}, nil)
	if a.Stale() {
		// The process restarted while this call waited its turn, and the
		// unit had made a call before: the call never runs.
		return ErrCrashed
	}
	if a.unitGen == 0 {
		a.unitGen = a.proc.gen + 1 // generations count from 1 here
	}
	a.RecordInput(callInput{label, f})
	switch f {
	case FaultError:
		return ErrInjected
	case FaultLost:
		_ = op()
		return ErrLost
	case FaultCrash:
		a.crash()
		return ErrCrashed
	}
	return op()
}

// crash kills the actor's process: it restarts at once, and every unit of
// work in it that has made a call, this one included, is lost.
func (a *Actor) crash() {
	a.proc.crashes++
	a.proc.gen++
	if a.proc.restart != nil {
		// The restart is the new process's doing, not the lost unit's.
		running := a.s.running
		a.s.running = nil
		a.proc.restart(a.s.Clock.Now())
		a.s.running = running
	}
}

// Stale reports whether the actor's current unit of work made a call before
// its process last restarted: its work is lost, and its calls fail.
func (a *Actor) Stale() bool {
	return a.unitGen != 0 && a.unitGen != a.proc.gen+1
}

// Wait parks the actor until cond holds (nil: always) and the clock has
// reached notBefore (zero: now). It is a yield point that can't fail. It
// ends the actor's unit of work: whatever runs after it is the process's
// new work, as it is now.
func (a *Actor) Wait(label string, notBefore time.Time, cond func() bool) {
	a.unitGen = 0
	a.inputs = nil
	a.park(label, false, notBefore, cond)
}

// callInput is a call that ran, as recorded among the actor's inputs: its
// label and fault. The harness records the result after it.
type callInput struct {
	Label string
	Fault Fault
}

// CallInput returns a recorded call's label and fault, for the state hash.
func CallInput(v any) (label string, fault Fault, ok bool) {
	c, ok := v.(callInput)
	return c.Label, c.Fault, ok
}

// WaitQuiet is a Wait whose resume is silent (§4): the code after it, up to
// the next park, is a quiet segment. It reads only the actor's own variables,
// immutable values and the process pointer, writes only the actor's own
// variables, and makes no call; and cond, once true, stays true. The
// scheduler runs it the moment the wait is ready, outside any choice. The
// harness's guard (M-a) panics on any read or write of shared state inside
// it.
func (a *Actor) WaitQuiet(label string, notBefore time.Time, cond func() bool) {
	a.nextQuiet = true
	a.Wait(label, notBefore, cond)
}

// Settle is rule R (§4): every actor at a ready quiet park resumes now,
// silently (no choice, no tick, no preemption, no change to last), in the
// order the actors were added, until none is left. Run applies it before
// every choice; the differential test applies it to an unreduced search's
// terminal state, to compare it with a reduced one's.
func (s *Sched) Settle() {
	for again := true; again; {
		again = false
		for _, a := range s.actors {
			if a.quiet && a.enabled(s.Clock.t) {
				a.quiet = false
				last := s.last
				s.inQuiet = true
				a.resume(FaultNone)
				s.inQuiet = false
				s.last = last
				again = true
			}
		}
	}
}

// InQuiet reports whether a quiet segment is running: the guard of §4 (M-a).
func (s *Sched) InQuiet() bool { return s.inQuiet }

// Crashed reports whether the actor's current unit of work is lost to a
// crash (Stale).
func (a *Actor) Crashed() bool { return a.Stale() }

// Restart ends the actor's unit of work, and begins a new one in its
// process as it is now. Actors call it at the start of each unit (a pass, a
// batch).
func (a *Actor) Restart() { a.unitGen = 0; a.inputs = nil }

// Running is the actor whose code is running now, or nil between steps.
func (s *Sched) Running() *Actor { return s.running }

// Now is the scheduler's clock.
func (a *Actor) Now() time.Time { return a.s.Clock.Now() }

// Clock is the scheduler's clock, for code that takes one.
func (a *Actor) Clock() *Clock { return a.s.Clock }

type option struct {
	actor *Actor
	fault Fault
	jump  time.Time // set: move the clock here, and run no actor
}

// pending returns the deadlines after now, sorted and without repeats: every
// actor's notBefore, and the scenario's.
func (s *Sched) pending(now time.Time) []time.Time {
	var ds []time.Time
	for _, a := range s.actors {
		if !a.done && a.notBefore.After(now) {
			ds = append(ds, a.notBefore)
		}
	}
	if s.Deadlines != nil {
		for _, d := range s.Deadlines() {
			if d.After(now) {
				ds = append(ds, d)
			}
		}
	}
	slices.SortFunc(ds, func(x, y time.Time) int { return x.Compare(y) })
	return slices.CompactFunc(ds, func(x, y time.Time) bool { return x.Equal(y) })
}

func (a *Actor) enabled(now time.Time) bool {
	return !a.done && !a.notBefore.After(now) && (a.cond == nil || a.cond())
}

// resume runs a until its next yield point, or its end.
func (a *Actor) resume(f Fault) {
	a.fault = f
	a.s.running = a
	_, ok := a.next()
	a.s.running = nil
	if !ok {
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
		if !s.NoQuiet {
			s.Settle()
		}
		if s.left != nil && len(s.choices) == len(s.prefix) {
			s.bounds.Preemptions = s.preemptions + s.left.Preemptions
			s.bounds.Faults = s.faults + s.left.Faults
			s.bounds.Jumps = s.jumps + s.left.Jumps
			s.left = nil
		}
		var opts []option
		now := s.Clock.t
		for _, a := range s.actors {
			if !a.enabled(now) {
				continue
			}
			// A quiet resume (rule R off) is invisible: it is never a
			// preemption, and doesn't become the last actor (§5, C-bounds).
			if !a.quiet && s.last != nil && s.last != a && s.last.enabled(now) && s.preemptions >= s.bounds.Preemptions {
				continue // no preemption left: the last actor must go on
			}
			opts = append(opts, option{actor: a, fault: FaultNone})
			if a.faultable && a.faulty && s.faults < s.bounds.Faults {
				opts = append(opts, option{actor: a, fault: FaultError}, option{actor: a, fault: FaultLost}, option{actor: a, fault: FaultCrash})
			}
		}
		if len(opts) > 0 && s.jumps < s.bounds.Jumps && s.liveActors() {
			for _, d := range s.pending(now) {
				opts = append(opts, option{jump: d})
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
			s.jumped()
			continue
		}
		// The state the next choice is made from, after any forced jump: one
		// check per choice, so a choice prefix names exactly one state. A
		// replayed prefix retraces states the search has recorded already;
		// only a state reached past it is new (§10.3).
		if s.Key != nil && s.visited != nil && len(s.choices) >= len(s.prefix) {
			last := ""
			if s.last != nil {
				last = s.last.Name
			}
			b := Budget{s.bounds.Preemptions - s.preemptions, s.bounds.Faults - s.faults, s.bounds.Jumps - s.jumps}
			k := s.Key() + "\x00last=" + last
			if s.visited.see(k, b, s.choices) {
				s.pruned = true
				s.prunedAt = Witness{slices.Clone(s.choices), b, k}
				s.prunedKey = k
				return ErrPruned
			}
		}
		if s.recordKeys && s.Key != nil && len(s.choices) >= len(s.prefix) {
			last := ""
			if s.last != nil {
				last = s.last.Name
			}
			s.keys = append(s.keys, s.Key()+"\x00last="+last)
			if s.maxSteps > 0 && len(s.keys) > s.maxSteps {
				return ErrCapped
			}
		}
		var o option
		if s.labelPlan != nil {
			o = s.planned(opts, now)
		} else {
			o = opts[s.choose(len(opts))]
		}
		if !o.jump.IsZero() {
			s.jumps++
			s.trace = append(s.trace, "jump:+"+o.jump.Sub(now).String())
			s.Clock.t = o.jump
			s.jumped()
			continue
		}
		quiet := o.actor.quiet
		if !quiet && s.last != nil && s.last != o.actor && s.last.enabled(now) {
			s.preemptions++
		}
		if o.fault != FaultNone {
			s.faults++
		}
		if !quiet {
			s.last = o.actor
		}
		s.trace = append(s.trace, o.actor.Name+":"+o.actor.label+o.fault.String())
		if o.actor.faultable {
			s.Clock.t = now.Add(s.tick(now))
		}
		o.actor.resume(o.fault)
	}
}

// TimeUnit is U: every constant the scenarios and the protocol add to a time
// is a multiple of it (docs/credential-use-model-reduction.md §10.2). A fine
// part, a time's offset from the nearest whole U, stays within ±U/2.
const TimeUnit = time.Hour

const (
	// BaseTick is the most one call moves the clock.
	BaseTick = time.Second
	// TickDivisor is D: a tick is also at most a D-th of the gap to the next
	// pending deadline, and of the gap to the next larger fine part of a live
	// term (§10.2's invariant).
	TickDivisor = 1024
	// MaxCallsBetweenJumps is K: the most calls a schedule may make between
	// two jumps, chosen or forced. CheckCapacity is derived for it, and a
	// schedule that makes more panics.
	MaxCallsBetweenJumps = 512
)

// CheckCapacity checks that time.Time's 1ns resolution leaves room for the
// ticks of a schedule with up to jumps nested jumps and K calls between any
// two (§10.2, the capacity condition):
//
//   - K ≤ D calls shrink a gap by at most a factor (1 − 1/D)^K ≥ 1/e, so
//     every tick of a level is at least its starting gap over e·D;
//   - a jump lands on a term, and the next larger fine part, made at the
//     level before, is at least one of that level's ticks away: so a level's
//     gap is at least the level before's smallest tick;
//   - level 0's gap is U/2, and its ticks are at most B: its smallest tick
//     is at least min(B, U/2/(e·D));
//   - so level T's smallest tick is at least min(B, U/2/(e·D))/(e·D)^T.
//
// It must be at least 1ns:
//
//	room = min(e·D·B, U/2) ≥ (e·D)^(T+1) ns.
func CheckCapacity(jumps int) error {
	if MaxCallsBetweenJumps > TickDivisor {
		return fmt.Errorf("apimodel: the capacity condition needs K ≤ D, so that K calls shrink a gap by at most e: K = %d, D = %d (§10.2)",
			MaxCallsBetweenJumps, TickDivisor)
	}
	eD := math.E * TickDivisor
	room := math.Min(eD*float64(BaseTick), float64(TimeUnit/2))
	need := math.Pow(eD, float64(jumps+1))
	if room < need {
		return fmt.Errorf("apimodel: the time representation has no room for %d nested jumps: room = min(e·D·B, U/2) = min(e·%d·%s, %s) = %.4g ns, "+
			"below (e·D)^(T+1) = (e·%d)^%d = %.4g ns, so a tick at the deepest level could fall below 1ns (§10.2): raise U or B, or lower D or T",
			jumps, TickDivisor, BaseTick, TimeUnit/2, room, TickDivisor, jumps+1, need)
	}
	return nil
}

// Fine returns t's fine part: its offset from the nearest whole U, counted
// from anchor (the scheduler and the state hash use Start, itself a whole
// U).
func Fine(t, anchor time.Time) time.Duration {
	d := t.Sub(anchor)
	n := (d + TimeUnit/2) / TimeUnit
	return d - n*TimeUnit
}

// jumped starts a new level: the clock jumped, by choice or forced.
func (s *Sched) jumped() {
	s.callsSinceJump = 0
	s.usage.MaxJumps++
}

// tick is how far a call moves the clock: at most BaseTick, and less, so
// that it never reaches a pending deadline. Deadlines derived from the times
// of events (two processes' first sights, a deletion and its first sight)
// lie only a few ticks apart; after a jump to one, the next must still be
// crossed only by a jump. So the tick is at most a D-th of the time left to
// the next deadline (and of the gap to the next larger fine part, below),
// and at least the clock's 1ns resolution. CheckCapacity shows that, with at
// most K calls between jumps, the 1ns floor is never reached; a tick that
// can't fit panics all the same: time would have to move through a call.
func (s *Sched) tick(now time.Time) time.Duration {
	s.callsSinceJump++
	s.usage.MaxCallsBetweenJumps = max(s.usage.MaxCallsBetweenJumps, s.callsSinceJump)
	if s.callsSinceJump > MaxCallsBetweenJumps {
		panic(fmt.Sprintf("apimodel: %d calls since the last jump, more than K = %d, the most the capacity condition is derived for (§10.2); raise K within D = %d, and recheck CheckCapacity (trace %s)",
			s.callsSinceJump, MaxCallsBetweenJumps, TickDivisor, s.Trace()))
	}
	d := BaseTick
	if ds := s.pending(now); len(ds) > 0 {
		gap := ds[0].Sub(now)
		if g := gap / TickDivisor; g < d {
			d = max(g, time.Nanosecond)
		}
		if d >= gap {
			panic(fmt.Sprintf("apimodel: a call's tick would reach a pending deadline: too many calls since the last jump (trace %s)", s.Trace()))
		}
	}
	// The representation's invariant (§10.2): a tick never moves now's fine
	// part past any live term's, so the global order of fine parts changes
	// only when a term is added, and now's place in it doesn't depend on
	// tick sizes.
	if s.Terms != nil {
		fn := Fine(now, Start)
		gap := TimeUnit/2 - fn
		for _, t := range s.Terms() {
			if f := Fine(t, Start); f > fn && f-fn < gap {
				gap = f - fn
			}
		}
		if g := gap / TickDivisor; g < d {
			d = max(g, time.Nanosecond)
		}
		if d >= gap {
			panic(fmt.Sprintf("apimodel: a call's tick would pass a live term's fine part: too many calls since the last jump (trace %s)", s.Trace()))
		}
	}
	if s.usage.MinTick == 0 || d < s.usage.MinTick {
		s.usage.MinTick = d
	}
	return d
}

// liveActors reports whether some actor that keeps the run going has more
// to do than a quiet segment that is due: a jump after that would change
// nothing anyone sees. An actor at a ready quiet park doesn't count. Rule R
// runs its segment before any choice, so with R the actor is past it; and
// if the segment ends the actor, a jump there must not be offered without R
// either, or the unreduced search would reach states the reduced one can't
// (docs/credential-use-model-reduction.md §5, C-bounds, jumps).
func (s *Sched) liveActors() bool {
	now := s.Clock.t
	for _, a := range s.actors {
		if !a.done && !a.daemon && !(a.quiet && a.enabled(now)) {
			return true
		}
	}
	return false
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
	case s.lastOption:
		i = n - 1
	case s.rng != nil:
		i = s.rng.Intn(n)
	}
	if i >= n {
		// A replay that offers fewer options than the run it replays: the
		// scenario isn't deterministic, and the search would be wrong.
		panic(fmt.Sprintf("apimodel: replayed choice %d of %d options at step %d: the scenario is not deterministic (trace %s)",
			i, n, len(s.choices), s.Trace()))
	}
	s.choices = append(s.choices, i)
	s.options = append(s.options, n)
	return i
}

// Choices returns the schedule's choices and, for each, how many options it
// had: its path through the search's choice tree, for estimating the tree's
// size (docs/credential-use-model-reduction.md §11).
func (s *Sched) Choices() (choices, options []int) {
	return slices.Clone(s.choices), slices.Clone(s.options)
}

// Steps is the schedule that ran, one entry per step, as Trace writes them.
func (s *Sched) Steps() []string { return slices.Clone(s.trace) }

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
	// States is a stateful search's count of distinct abstract states;
	// Pruned, of runs that ended at a visited one.
	States, Pruned int
	// Usage is the most any schedule used of the time representation's
	// capacity (§10.2).
	Usage Usage
}

// Explore runs scenario once per schedule within b, depth first. scenario
// builds its actors on s, then calls s.Run, then checks the run and returns
// an error for a refused one. It stops at the first 10 failures.
func Explore(b Bounds, scenario func(s *Sched) error) Result {
	return explore(b, nil, nil, scenario)
}

// ExploreStateful is Explore with a memory of visited abstract states
// (§10): a run that reaches one already searched with a budget at least as
// large ends there. scenario must set Sched.Key.
func ExploreStateful(b Bounds, scenario func(s *Sched) error) Result {
	return explore(b, NewVisited(), nil, scenario)
}

// ExploreAudited is ExploreStateful that hands every prune to audit: the
// pruned path and the witness of the state it reached (§10.5).
func ExploreAudited(b Bounds, audit func(pruned, witness Witness), scenario func(s *Sched) error) Result {
	return explore(b, NewVisited(), audit, scenario)
}

// Replay runs one schedule: prefix, then each choice drawn from a generator
// seeded with seed (seed −1: always the first option; −2: always the last,
// which favours jumps and faults), within b. It returns
// the abstract states after the prefix, one per step, and the scenario's
// error. maxSteps caps it (ErrCapped). scenario must set Sched.Key.
func Replay(b Bounds, prefix []int, left Budget, seed int64, maxSteps int, scenario func(s *Sched) error) ([]string, error) {
	var rng *rand.Rand
	last := seed == -2
	if seed >= 0 {
		//nolint:gosec // reproducible schedules, not security
		rng = rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducible schedules, not security
	}
	s := newSched(b, prefix, rng, nil)
	s.recordKeys, s.maxSteps, s.left, s.lastOption = true, maxSteps, &left, last
	err := scenario(s)
	return s.keys, err
}

func explore(b Bounds, visited *Visited, audit func(pruned, witness Witness), scenario func(s *Sched) error) (res Result) {
	var prefix []int
	defer func() {
		if visited != nil {
			res.States = visited.States
		}
	}()
	for {
		if b.MaxRuns > 0 && res.Runs >= b.MaxRuns {
			return res
		}
		s := newSched(b, prefix, nil, visited)
		err := scenario(s)
		res.Runs++
		res.Usage.Merge(s.usage)
		if s.pruned {
			res.Pruned++
			if audit != nil {
				audit(s.prunedAt, visited.witness[s.prunedKey])
			}
		}
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
	// can't be seeded, and nothing here is security. gosec reads #nosec only
	// at the start of a comment, and golangci-lint reads its directive on the
	// line before.
	//nolint:gosec // reproducible schedules, not security
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducible schedules, not security
	for range n {
		s := newSched(b, nil, rng, nil)
		err := scenario(s)
		res.Usage.Merge(s.usage)
		if err != nil {
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
