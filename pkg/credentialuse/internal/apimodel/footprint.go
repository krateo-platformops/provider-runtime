package apimodel

import (
	"fmt"
	"slices"
	"strings"
)

// Footprints (docs/credential-use-model-reduction.md §12.2): what a
// transition reads and writes of the shared state, recorded through one API
// that the harness's tracked accessors, the CE tests, the generated
// differential and the write detector all use.

// An Object names one piece of shared state. Kind is one of the kinds below;
// Name says which one (a model key, a process's name, a world field).
type Object struct {
	Kind, Name string
}

// The kinds of object, one naming scheme for every user of the recorder.
const (
	// ObjModelKey is one object of the model, by key (Key.String).
	ObjModelKey = "model"
	// ObjLog is the model's event log, with its resourceVersion and UID
	// counters: every model write writes it; informers read it.
	ObjLog = "log"
	// ObjCache, ObjVerified, ObjSeen and ObjGen are a process's informer
	// cache, verified set, first sights and generation, by process name.
	ObjCache    = "cache"
	ObjVerified = "verified"
	ObjSeen     = "seen"
	ObjGen      = "gen"
	// ObjWorld is one field of the harness's world (a monitor or a counter),
	// by field name, with a key where the field is a map.
	ObjWorld = "world"
	// ObjClock is the clock. A tick is its own mode; a change of the live
	// terms or of the pending deadlines is a write.
	ObjClock = "clock"
)

func (o Object) String() string { return o.Kind + ":" + o.Name }

// A Mode is how a transition touched an object: a set of these bits.
type Mode uint8

const (
	ModeRead Mode = 1 << iota
	ModeWrite
	// ModeTick is a call's tick of the clock. Ticks commute with ticks, and
	// conflict with reads and writes of the clock (§12.2).
	ModeTick
)

func (m Mode) String() string {
	var b strings.Builder
	for _, x := range []struct {
		m Mode
		s string
	}{{ModeRead, "r"}, {ModeWrite, "w"}, {ModeTick, "t"}} {
		if m&x.m != 0 {
			b.WriteString(x.s)
		}
	}
	return b.String()
}

// A Footprint is what one transition, or one segment, touched.
type Footprint map[Object]Mode

// Add records that the footprint touched o in mode m.
func (f Footprint) Add(o Object, m Mode) { f[o] |= m }

// Union adds every object of g to f.
func (f Footprint) Union(g Footprint) {
	for o, m := range g {
		f[o] |= m
	}
}

// Conflicts reports whether f and g touch some object in modes that don't
// commute: a write against any access, or a tick against a read or a write.
// Two reads commute, and so do two ticks.
func (f Footprint) Conflicts(g Footprint) bool {
	small, big := f, g
	if len(big) < len(small) {
		small, big = big, small
	}
	for o, a := range small {
		b, ok := big[o]
		if !ok {
			continue
		}
		if conflict(a, b) {
			return true
		}
	}
	return false
}

func conflict(a, b Mode) bool {
	if a&ModeWrite != 0 && b != 0 || b&ModeWrite != 0 && a != 0 {
		return true
	}
	return a&ModeTick != 0 && b&ModeRead != 0 || b&ModeTick != 0 && a&ModeRead != 0
}

// Within reports whether every access of f is in g, mode for mode: the
// realized ⊆ union assertion of §12.3.
func (f Footprint) Within(g Footprint) bool {
	for o, m := range f {
		if g[o]&m != m {
			return false
		}
	}
	return true
}

// String lists the footprint, sorted: for test failures and logs.
func (f Footprint) String() string {
	parts := make([]string, 0, len(f))
	for o, m := range f {
		parts = append(parts, fmt.Sprintf("%s=%s", o, m))
	}
	slices.Sort(parts)
	return "{" + strings.Join(parts, " ") + "}"
}

// The recording context (§12.2, the third review's Q2) is a stack on each
// Sched: a scope records into a footprint, or is off. Off scopes (a tick's
// instrumentation) run inside on scopes (a call's body), and guard
// evaluation, which is on, runs inside the option loop; the innermost scope
// decides. The stack is empty between steps.
type recScope struct {
	fp Footprint // nil: an off scope
}

// RecordInto opens a recording scope that adds every access to fp, until the
// matching EndScope.
func (s *Sched) RecordInto(fp Footprint) { s.rec = append(s.rec, recScope{fp: fp}) }

// RecordOff opens a scope in which nothing is recorded, until the matching
// EndScope: for the instrumentation (Key, Terms, Deadlines, the diffs, the
// write detector).
func (s *Sched) RecordOff() { s.rec = append(s.rec, recScope{}) }

// EndScope closes the innermost scope.
func (s *Sched) EndScope() {
	if len(s.rec) == 0 {
		panic("apimodel: EndScope with no recording scope open")
	}
	s.rec = s.rec[:len(s.rec)-1]
}

// RecordingScopes is how many scopes are open: 0 between steps.
func (s *Sched) RecordingScopes() int { return len(s.rec) }

func (s *Sched) record(o Object, m Mode) {
	if n := len(s.rec); n > 0 && s.rec[n-1].fp != nil {
		s.rec[n-1].fp.Add(o, m)
	}
}

// Read records a read of o in the innermost scope, if it is recording.
func (s *Sched) Read(o Object) { s.record(o, ModeRead) }

// Write records a write of o in the innermost scope, if it is recording.
func (s *Sched) Write(o Object) { s.record(o, ModeWrite) }

// Tick records a call's tick of the clock in the innermost scope, if it is
// recording.
func (s *Sched) Tick() { s.record(Object{Kind: ObjClock}, ModeTick) }
