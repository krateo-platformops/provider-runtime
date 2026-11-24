package credentialuse_test

// The stress test's audits, on made-up histories: each thing a run must not
// have done is found, and what §5 and §6.4 allow isn't.

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// The objects of the histories, in holder namespace "ns".
var (
	auditS   = stObj{"Secret", "ns", "s"}
	auditS2  = stObj{"Secret", "ns", "s2"}
	auditH   = stObj{"Widget", "ns", "h"}
	auditFin = "credential-use.widget.credentialuse.test.krateo.io/ns"
)

// auditRun builds a history, one step a millisecond and one revision a write.
type auditRun struct {
	mode   stressMode
	at     time.Time
	rv     uint64
	vs     []stVersion
	calls  []stCall
	passes []*stPass
	reads  []stRead
	writes []stWrite
	events []stEvent
	gaps   []string
	last   map[stObj]stVersion
	// synced and stopped bound what the feeds must deliver: none without them.
	synced  map[int]time.Time
	stopped time.Time
	outside bool
}

// newAuditRun begins a history with a live namespace, the Secret s, and the
// Widget h, established, naming s in its spec and in its record.
func newAuditRun(mode stressMode) *auditRun {
	b := &auditRun{mode: mode, at: time.Unix(1000, 0), rv: 100, last: map[stObj]stVersion{}}
	b.put(nsObj("ns"), "uns", nil)
	b.create(auditS, "u1")
	b.put(auditH, "uh", func(v *stVersion) { v.spec, v.fins = "s", []string{ownerFinalizer} })
	return b
}

func (b *auditRun) tick() time.Time {
	b.at = b.at.Add(time.Millisecond)
	return b.at
}

func (b *auditRun) wait(d time.Duration) { b.at = b.at.Add(d) }

// put writes a version of o: its last state, changed by f.
func (b *auditRun) put(o stObj, uid string, f func(v *stVersion)) stVersion {
	v := b.last[o]
	if v.uid != uid {
		v = stVersion{stObj: o, uid: uid}
	}
	v.fins, v.epochs = slices.Clone(v.fins), maps.Clone(v.epochs)
	if v.epochs == nil {
		v.epochs = map[string]string{}
	}
	if f != nil {
		f(&v)
	}
	b.rv++
	v.rv, v.at = b.rv, b.tick()
	b.last[o] = v
	b.vs = append(b.vs, v)
	return v
}

// remove ends o's incarnation.
func (b *auditRun) remove(o stObj) {
	v := b.last[o]
	b.rv++
	v.rv, v.at, v.gone = b.rv, b.tick(), true
	delete(b.last, o)
	b.vs = append(b.vs, v)
}

// create and DELETE are the test's own, answered.
func (b *auditRun) create(o stObj, uid string) {
	w := stWrite{op: "create", obj: o, start: b.tick(), answer: answerCreated, uid: uid}
	w.rv, w.end = b.put(o, uid, nil).rv, b.tick()
	b.writes = append(b.writes, w)
}

// began is a DELETE that has begun, and answer ends it.
func (b *auditRun) began(o stObj) stWrite { return stWrite{op: "DELETE", obj: o, start: b.tick()} }
func (b *auditRun) answer(w stWrite, answer, uid string) {
	w.answer, w.uid, w.rv, w.end = answer, uid, b.last[w.obj].rv, b.tick()
	b.writes = append(b.writes, w)
}

// mark is a DELETE of o that finds a finalizer.
func (b *auditRun) mark(o stObj) {
	w := b.began(o)
	uid := b.last[o].uid
	b.put(o, uid, func(v *stVersion) { v.deleting, v.deletedAt = true, b.at.Truncate(time.Second) })
	b.answer(w, answerMarked, uid)
}

func (b *auditRun) hold(v *stVersion, epoch string) {
	if !slices.Contains(v.fins, auditFin) {
		v.fins = append(v.fins, auditFin)
	}
	v.epochs[auditFin] = epoch
}

// call ends c, on goroutine g of process proc.
func (b *auditRun) call(c stCall, f func(c *stCall)) stCall {
	if c.proc == 0 {
		c.proc, c.g = 2, "9" // the release worker
	}
	c.start = b.tick()
	f(&c)
	c.end = b.tick()
	b.calls = append(b.calls, c)
	return c
}

func (c *stCall) from(v stVersion) {
	c.rv, c.uid, c.deleting, c.fins, c.epochs, c.consent = v.rv, v.uid, v.deleting, v.fins, v.epochs, v.consent
}

// pass begins a pass of the Widget h.
func (b *auditRun) pass() *stPass {
	v := b.last[auditH]
	p := &stPass{id: int64(len(b.passes) + 1), proc: 1, holder: auditH, began: b.tick(), read: true, rv: v.rv, uid: v.uid,
		deleting: v.deleting, refs: refsOf(b.mode, "ns", v.spec), unheld: map[stObj]string{}}
	b.passes = append(b.passes, p)
	return p
}

func (b *auditRun) end(p *stPass, held ...stObj) { p.held, p.done = held, b.tick() }

func (b *auditRun) a1(p *stPass, rec ...stObj) {
	b.call(stCall{proc: 1, g: "5", pass: p.id, op: "a1", obj: auditH}, func(c *stCall) {
		c.from(b.put(auditH, p.uid, func(v *stVersion) { v.rec = rec }))
	})
}

func (b *auditRun) a2(p *stPass, o stObj, epoch string) {
	b.call(stCall{proc: 1, g: "5", pass: p.id, op: "a2", obj: o, fin: auditFin, wrote: epoch}, func(c *stCall) {
		c.from(b.put(o, b.last[o].uid, func(v *stVersion) { b.hold(v, epoch) }))
	})
}

// deliver has process proc's Widget tracker be delivered o's latest version,
// or, if o is gone, its removal.
func (b *auditRun) deliver(proc int, o stObj) {
	i := len(b.vs) - 1
	for b.vs[i].stObj != o {
		i--
	}
	v := b.vs[i]
	e := stEvent{stFeed: stFeed{proc, "Widget", "protected"}, uid: v.uid, rv: v.rv, at: b.tick()}
	switch o.kind {
	case "Widget":
		e.what = "holder"
	case "Namespace":
		e.stFeed, e.uid = stFeed{proc, "", "namespace"}, o.name
	}
	b.events = append(b.events, e)
}

// acquired is a live pass that announces s and holds it, in process 1, whose
// informers deliver both.
func (b *auditRun) acquired() *auditRun {
	p := b.pass()
	b.a1(p, auditS)
	b.deliver(1, auditH)
	b.a2(p, auditS, "e1")
	b.deliver(1, auditS)
	b.end(p, auditS)
	return b
}

// deletion deletes the holder, and returns its first deletion pass.
func (b *auditRun) deletion() *stPass {
	b.put(auditH, "uh", func(v *stVersion) { v.deleting = true })
	return b.pass()
}

// r1, namespace, list and r3 are the calls of a release batch.
func (b *auditRun) r1() {
	b.call(stCall{op: "get", obj: auditS}, func(c *stCall) { c.from(b.last[auditS]) })
}
func (b *auditRun) namespace() {
	b.call(stCall{op: "get", obj: nsObj("ns")}, func(c *stCall) { c.from(b.last[nsObj("ns")]) })
}
func (b *auditRun) list() { b.listBy(2, "9") }

// listBy is a list of the holders on goroutine g of process proc.
func (b *auditRun) listBy(proc int, g string) {
	b.call(stCall{proc: proc, g: g, op: "list", obj: stObj{"Widget", "ns", ""}}, func(c *stCall) {
		c.rv = b.rv
		for o, v := range b.last {
			if o.kind == "Widget" && o.ns == "ns" {
				c.items = append(c.items, stItem{name: o.name, uid: v.uid, rv: v.rv, deleting: v.deleting, owner: v.owner(), rec: v.rec, recBad: v.recBad})
			}
		}
	})
}

func (b *auditRun) batch() {
	b.r1()
	if b.last[auditS].deleting {
		b.namespace()
	}
	b.list()
}

func (b *auditRun) r3() {
	b.call(stCall{op: "r3", obj: auditS, fin: auditFin}, func(c *stCall) {
		v := b.last[auditS]
		c.testedUID, c.tested = v.uid, v.epochs[auditFin]
		drop := func(v *stVersion) {
			v.fins = slices.DeleteFunc(v.fins, func(f string) bool { return f == auditFin })
			delete(v.epochs, auditFin)
		}
		if v.deleting {
			c.from(v)
			c.fins, c.epochs = nil, nil
			b.remove(auditS)
			return
		}
		c.from(b.put(auditS, v.uid, drop))
	})
}

// lostRead is p's Connect, failing to read s.
func (b *auditRun) lostRead(p *stPass) {
	b.reads = append(b.reads, stRead{pass: p.id, holder: auditH, obj: auditS, start: b.tick(), end: b.tick(), err: `secrets "s" not found`, notFound: true})
}

func (b *auditRun) history() *stHistory {
	return newStHistory(stRecording{mode: b.mode, grace: testGrace, versions: b.vs, calls: b.calls, passes: b.passes, reads: b.reads, writes: b.writes, events: b.events, gaps: b.gaps,
		synced: b.synced, stopped: b.stopped, outside: b.outside})
}

// want checks that the audits find exactly classes, each with a timeline, and
// returns the report.
func (b *auditRun) want(t *testing.T, classes ...string) *stReport {
	t.Helper()
	h := b.history()
	r := h.audit()
	var got []string
	for _, f := range r.findings {
		got = append(got, f.class)
		if f.obj != (stObj{}) && !strings.Contains(h.timeline(f), f.obj.String()) {
			t.Errorf("%s: its timeline doesn't show %s:\n%s", f.class, f.obj, h.timeline(f))
		}
	}
	slices.Sort(classes)
	if !slices.Equal(got, classes) {
		var all []string
		for _, f := range r.findings {
			all = append(all, f.class+": "+f.text)
		}
		t.Errorf("found %v, want %v:\n%s", got, classes, strings.Join(all, "\n"))
	}
	return r
}

func wantCount(t *testing.T, r *stReport, name string, n int) {
	t.Helper()
	if r.counts[name] != n {
		t.Errorf("%s: %d, want %d", name, r.counts[name], n)
	}
}

// A release is judged at the moment it lands, against the users the watch
// had, and what §5 allows.
func TestAuditReleases(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(b *auditRun)
		want  []string
	}{
		{"no user", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.r3()
		}, nil},
		{"a live object with a user", func(b *auditRun) {
			b.acquired()
			b.batch()
			b.r3()
		}, []string{findReleasedOverUser}},
		{"a live object with a user, and then the test's DELETE: nobody reads it, and the removal is the DELETE's", func(b *auditRun) {
			b.acquired()
			b.batch()
			b.r3()
			w := b.began(auditS)
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
		}, []string{findReleasedOverUser}},
		{"a user whose record can't be parsed", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.recBad, v.rec = true, nil })
			b.batch()
			b.r3()
		}, []string{findReleasedOverUser, findRecordDrop}},
		{"a holder being deleted without its owner finalizer is no user", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.deleting, v.fins = true, []string{"example.com/other"} })
			b.batch()
			b.r3()
		}, nil},
		{"a user that announced after the list, whose A2 hasn't landed: §6.4's third case", func(b *auditRun) {
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.batch()
			p := b.pass()
			b.a1(p, auditS)
			b.r3()
			b.a2(p, auditS, "e1")
			b.end(p, auditS)
		}, nil},
		{"a user that announced after the list, and whose A2 landed before R3: the epoch wasn't tested", func(b *auditRun) {
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.batch()
			p := b.pass()
			b.a1(p, auditS)
			b.a2(p, auditS, "e1")
			b.end(p, auditS)
			b.r3()
		}, []string{findReleasedOverUser, findReleasedUnread}},
		{"the same, and the A2 wrote the old epoch", func(b *auditRun) {
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.batch()
			p := b.pass()
			b.a1(p, auditS)
			b.a2(p, auditS, "e0")
			b.end(p, auditS)
			b.r3()
		}, []string{findReleasedOverUser, findReleasedUnread}},
		{"the list came before R1, and missed a user", func(b *auditRun) {
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.r1()
			b.list()
			p := b.pass()
			b.a1(p, auditS)
			b.a2(p, auditS, "e1")
			b.end(p, auditS)
			b.r1() // what the patch was built from
			b.r3()
		}, []string{findReleasedOverUser, findReleasedUnread}},
		{"another incarnation than R1 read", func(b *auditRun) {
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.batch()
			w := b.began(auditS)
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
			b.create(auditS, "u2")
			b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "e0") })
			b.r3()
		}, []string{findReleasedUnread, findUnexplainedRemoval}}, // a DELETE that began after the hold doesn't remove it
		{"no list", func(b *auditRun) {
			b.acquired()
			b.r1()
			b.r3()
		}, []string{findReleasedUnread, findReleasedOverUser}},
		{"no list, and no user", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.r1()
			b.r3()
		}, []string{findReleasedUnread}},
		{"a read after the list", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.r1()
			b.r3()
		}, []string{findReleasedUnread}},
		{"another object's read between its own and the list", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.r1()
			b.call(stCall{op: "get", obj: auditS2}, func(c *stCall) { c.rv, c.uid = 1, "v1" })
			b.list()
			b.call(stCall{op: "r3", obj: auditS2, fin: auditFin, err: "not found", notFound: true}, func(*stCall) {})
			b.call(stCall{op: "get", obj: auditS2, err: "not found", notFound: true}, func(*stCall) {})
			b.r3()
		}, nil},
		{"a list in two pages", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.calls[len(b.calls)-1].more = true
			b.call(stCall{op: "list", obj: stObj{"Widget", "ns", ""}, continued: true}, func(c *stCall) { c.rv = b.rv })
			b.r3()
		}, nil},
		{"half a list", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.calls[len(b.calls)-1].continued = true
			b.r3()
		}, []string{findReleasedUnread}},
		{"a list that lacks a holder", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.calls[len(b.calls)-1].items = nil
			b.r3()
		}, []string{findStaleList}},
		{"a list with an older version of a holder", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.calls[len(b.calls)-1].items[0].rv--
			b.r3()
		}, []string{findStaleList}},
		{"a deleting object with no users, inside the grace", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.mark(auditS)
			b.batch()
			b.r3()
		}, nil},
		{"a deleting object with a live user, inside the grace", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace - time.Second)
			b.batch()
			b.r3()
		}, []string{findReleasedEarly}},
		{"after the grace", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.batch()
			b.r3()
		}, nil},
		{"after the grace, and the Namespace wasn't read", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.r1()
			b.list()
			b.r3()
		}, []string{findReleasedOverUser}},
		{"in a terminating namespace", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.put(nsObj("ns"), "uns", func(v *stVersion) { v.deleting = true })
			b.batch()
			b.r3()
		}, []string{findReleasedOverUser}},
		{"the namespace's deletion came after R2's read: §6.4's window", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.r1()
			b.namespace()
			b.put(nsObj("ns"), "uns", func(v *stVersion) { v.deleting = true })
			b.list()
			b.r3()
		}, nil},
		{"a user being deleted, after the grace", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.put(auditH, "uh", func(v *stVersion) { v.deleting = true })
			b.batch()
			b.r3()
		}, []string{findReleasedOverUser}},
		{"the user's deletion came after the list: §6.4's window", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.batch()
			b.put(auditH, "uh", func(v *stVersion) { v.deleting = true })
			b.r3()
		}, nil},
		{"the same, and its deletion pass's A2 landed first", func(b *auditRun) {
			b.acquired()
			b.mark(auditS)
			b.wait(testGrace)
			b.batch()
			p := b.deletion()
			b.a2(p, auditS, "e2")
			b.end(p, auditS)
			b.r3()
		}, []string{findReleasedOverUser, findReleasedUnread}},
		{"a list older than the watch", func(b *auditRun) {
			b.acquired()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.batch()
			b.calls[len(b.calls)-1].rv = b.last[auditH].rv - 1
			b.r3()
		}, []string{findStaleList, findReleasedOverUser}},
		{"another goroutine's list isn't the batch's", func(b *auditRun) {
			b.acquired()
			b.batch()
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			b.listBy(2, "10")
			b.r3()
		}, []string{findReleasedOverUser}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			c.build(b)
			r := b.want(t, c.want...)
			wantCount(t, r, countReleases, 1)
		})
	}

	t.Run("a release of one kind isn't kept by a user of another", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		target := stObj{"Target", "ns", "t"}
		b.put(target, "ut", func(v *stVersion) { v.spec, v.fins, v.rec = "s", []string{ownerFinalizer}, []stObj{auditS} })
		b.acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.r3()
		b.want(t)
	})
}

// What went, and what took it.
func TestAuditRemovals(t *testing.T) {
	t.Run("a release of the last finalizer", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.mark(auditS)
		b.batch()
		b.r3()
		r := b.want(t)
		wantCount(t, r, countRemovals, 1)
		wantCount(t, r, countLostA2, 0)
	})
	t.Run("an A2 between the release's tests and the removal: H1, counted", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.rec = nil; v.spec = "" })
		b.mark(auditS)
		b.batch()
		read := b.last[auditS]
		b.put(auditH, "uh", func(v *stVersion) { v.spec = "s" })
		p := b.pass()
		b.a1(p, auditS)
		b.a2(p, auditS, "e2")
		b.r3()
		b.end(p, auditS)
		// The answer is the version the tests ran on.
		b.calls[len(b.calls)-1].rv = read.rv
		r := b.want(t)
		wantCount(t, r, countLostA2, 1)
	})
	t.Run("a held object that nothing explains", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.remove(auditS)
		b.want(t, findUnexplainedRemoval)
	})
	t.Run("an unheld object that nothing explains", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		b.remove(auditS)
		b.want(t, findUnexplainedRemoval)
	})
	t.Run("a deleting object that no release took", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		b.remove(auditS)
		b.want(t, findUnexplainedRemoval)
	})
	t.Run("a DELETE in flight that takes a first hold: H3", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		w := b.began(auditS)
		b.acquired()
		b.remove(auditS)
		b.answer(w, answerStatus, "u1")
		r := b.want(t)
		wantCount(t, r, countH3, 1)
	})
	t.Run("a DELETE of an object held once, and released since", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.r3()
		w := b.began(auditS)
		b.remove(auditS)
		b.answer(w, answerStatus, "u1")
		r := b.want(t)
		wantCount(t, r, countH3, 0)
	})
	t.Run("H3 on a second hold: the DELETE read it between a release and the hold", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.r3()
		w := b.began(auditS)
		b.put(auditH, "uh", func(v *stVersion) { v.spec = "s" })
		p := b.pass()
		b.a1(p, auditS)
		b.a2(p, auditS, "e2")
		b.end(p, auditS)
		b.remove(auditS)
		b.answer(w, answerStatus, "u1")
		r := b.want(t)
		wantCount(t, r, countH3, 1)
	})
	t.Run("a finalizer that an A2 dropped", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditS, "u1", func(v *stVersion) { v.fins, v.epochs = nil, nil })
		b.want(t, findFinalizerLost)
	})
}

// §14's I1, and H3's exemption: only on proof.
func TestAuditReads(t *testing.T) {
	// h3 is a deletion pass whose first hold of s a DELETE in flight removes.
	h3 := func(b *auditRun) *stPass {
		b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
		w := b.began(auditS)
		p := b.deletion()
		b.a2(p, auditS, "e1")
		b.remove(auditS)
		b.answer(w, answerStatus, "u1")
		b.lostRead(p)
		b.end(p, auditS)
		return p
	}
	t.Run("H3", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		h3(b)
		r := b.want(t)
		wantCount(t, r, countH3Reads, 1)
		wantCount(t, r, countH3, 1)
	})
	for _, c := range []struct {
		name  string
		build func(b *auditRun)
		want  []string
	}{
		{"the DELETE's answer names another incarnation", func(b *auditRun) {
			h3(b)
			b.writes[len(b.writes)-1].uid = "u0"
		}, []string{findIncomplete, findLostRead, findUnexplainedRemoval}},
		{"the DELETE's answer was the object, not a Status", func(b *auditRun) {
			h3(b)
			b.writes[len(b.writes)-1].answer = answerMarked
		}, []string{findIncomplete, findLostRead, findUnexplainedRemoval}},
		{"the DELETE's answer was lost", func(b *auditRun) {
			h3(b)
			w := &b.writes[len(b.writes)-1]
			w.answer, w.uid, w.err = answerUnknown, "", "connection reset"
		}, []string{findIncomplete, findLostRead, findUnexplainedRemoval}},
		{"the DELETE's answer was lost, and the client's own retry was answered NotFound", func(b *auditRun) {
			h3(b)
			w := &b.writes[len(b.writes)-1]
			w.answer, w.uid = answerNotFound, ""
		}, []string{findLostRead, findUnexplainedRemoval}},
		{"no DELETE", func(b *auditRun) {
			h3(b)
			b.writes = b.writes[:len(b.writes)-1]
		}, []string{findLostRead, findUnexplainedRemoval}},
		{"the DELETE began after the hold", func(b *auditRun) {
			b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
			p := b.deletion()
			b.a2(p, auditS, "e1")
			w := b.began(auditS)
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
			b.lostRead(p)
			b.end(p, auditS)
		}, []string{findLostRead, findUnexplainedRemoval}},
		{"it had been marked as being deleted", func(b *auditRun) {
			h3(b)
			for i := range b.vs {
				if v := &b.vs[i]; v.stObj == auditS && v.epochs[auditFin] != "" && !v.gone {
					v.deleting = true
				}
			}
		}, []string{findLostRead, findUnexplainedRemoval}},
		{"it didn't carry the finalizer", func(b *auditRun) {
			h3(b)
			for i := range b.vs {
				if v := &b.vs[i]; v.stObj == auditS {
					v.fins, v.epochs = nil, nil
				}
			}
		}, []string{findLostRead, findHeldWithoutA2}},
		{"a release of the name in its life, of another incarnation", func(b *auditRun) {
			h3(b)
			b.call(stCall{op: "r3", obj: auditS, fin: auditFin, rv: 102, uid: "u0", deleting: true}, func(c *stCall) {})
			c := &b.calls[len(b.calls)-1]
			c.start = b.calls[0].start
		}, []string{findIncomplete, findIncomplete, findLostRead}}, // it has neither version nor removal in the watch
		{"a release of the name in its life, of unknown outcome", func(b *auditRun) {
			h3(b)
			b.call(stCall{op: "r3", obj: auditS, fin: auditFin, err: "connection refused", unknown: true}, func(c *stCall) {})
			b.calls[len(b.calls)-1].start = b.calls[0].start
		}, []string{findLostRead}},
		{"a released hold", func(b *auditRun) {
			b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
			p := b.deletion()
			b.a2(p, auditS, "e1")
			b.batch()
			b.r3()
			w := b.began(auditS)
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
			b.lostRead(p)
			b.end(p, auditS)
		}, []string{findReleasedOverUser, findLostRead}},
		{"an error that isn't NotFound", func(b *auditRun) {
			p := h3(b)
			b.reads[0].notFound = false
			_ = p
		}, []string{findLostRead}},
		{"no A2", func(b *auditRun) {
			b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
			w := b.began(auditS)
			p := b.deletion()
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
			b.lostRead(p)
			b.end(p, auditS)
		}, []string{findLostRead, findHeldWithoutA2}},
		{"no removal", func(b *auditRun) {
			b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
			p := b.deletion()
			b.a2(p, auditS, "e1")
			b.lostRead(p)
			b.end(p, auditS)
		}, []string{findLostRead}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			c.build(b)
			b.want(t, c.want...)
		})
	}
	t.Run("an earlier release that ended before the incarnation's create", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		// u1 is held and released, and goes; u2 is H3's.
		b.acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.rec = nil; v.spec = "" })
		b.mark(auditS)
		b.batch()
		b.r3()
		b.create(auditS, "u2")
		b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS}; v.spec = "s" })
		w := b.began(auditS)
		p := b.deletion()
		b.a2(p, auditS, "e1")
		b.remove(auditS)
		b.answer(w, answerStatus, "u2")
		b.lostRead(p)
		b.end(p, auditS)
		r := b.want(t)
		wantCount(t, r, countH3Reads, 1)
	})
	t.Run("a release of the same incarnation that ended before its last hold", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.r3()
		b.put(auditH, "uh", func(v *stVersion) { v.spec = "s" })
		h3(b)
		r := b.want(t)
		wantCount(t, r, countH3Reads, 1)
	})
	t.Run("the same, and its outcome isn't known", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.r3()
		b.call(stCall{op: "r3", obj: auditS, fin: auditFin, err: "connection refused", unknown: true}, func(*stCall) {})
		b.put(auditH, "uh", func(v *stVersion) { v.spec = "s" })
		h3(b)
		b.want(t, findLostRead)
	})
	t.Run("a pass that began live isn't counted, and stays in the audits", func(t *testing.T) {
		b := newAuditRun(stressPlain)
		p := b.pass()
		b.a1(p, auditS)
		b.a2(p, auditS, "e1")
		b.put(auditH, "uh", func(v *stVersion) { v.deleting = true })
		b.batch()
		b.r3()
		b.lostRead(p)
		b.end(p, auditS)
		r := b.want(t, findReleasedOverUser)
		wantCount(t, r, countLiveReads, 1)
	})
	t.Run("the holder was gone when the object went", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		p := b.deletion()
		b.a2(p, auditS, "e2")
		b.remove(auditH) // another process's pass finished it
		b.batch()
		b.r3()
		b.lostRead(p)
		b.end(p, auditS)
		r := b.want(t)
		wantCount(t, r, countHolderGone, 1)
	})
}

// What a pass reports of its refs.
func TestAuditPasses(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(b *auditRun)
		want  []string
		count string
	}{
		{"a deletion pass with its A2", func(b *auditRun) {
			b.acquired()
			p := b.deletion()
			b.a2(p, auditS, "e2")
			b.end(p, auditS)
		}, nil, countHeldByA2},
		{"a deletion pass without one", func(b *auditRun) {
			b.acquired()
			b.end(b.deletion(), auditS)
		}, []string{findHeldWithoutA2}, ""},
		{"a deletion pass on another pass's A2", func(b *auditRun) {
			b.acquired()
			p := b.deletion()
			other := b.pass()
			b.a2(other, auditS, "e2")
			b.end(other, auditS)
			b.end(p, auditS)
		}, []string{findHeldWithoutA2}, ""},
		{"a live pass that skips, after the record was announced and held", func(b *auditRun) {
			b.acquired()
			b.end(b.pass(), auditS)
		}, nil, countSkips},
		{"a live pass that skips on a write from before the record named the ref again", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.a1(p)
			b.deliver(1, auditH)
			b.end(p)
			p.refs = nil
			b.a1(b.pass(), auditS) // a pass that never returns
			b.deliver(1, auditH)
			b.end(b.pass(), auditS)
		}, []string{findHeldWithoutA2, findRecordDrop}, ""},
		{"a live pass that skips, in a process that never wrote", func(b *auditRun) {
			b.acquired()
			b.deliver(2, auditH)
			p := b.pass()
			p.proc = 2
			b.end(p, auditS)
		}, []string{findHeldWithoutA2}, ""},
		{"a live pass that skips a ref the record doesn't name", func(b *auditRun) {
			b.deliver(1, auditH)
			b.end(b.pass(), auditS)
		}, []string{findHeldWithoutA2, findRecordDiffers}, ""},
		{"an A2 before the A1 that names the ref", func(b *auditRun) {
			p := b.pass()
			b.a2(p, auditS, "e1")
			b.a1(p, auditS)
			b.end(p, auditS)
		}, []string{findHeldUnannounced}, ""},
		{"a ref a pass says nothing of", func(b *auditRun) {
			b.acquired()
			b.end(b.pass())
		}, []string{findUnreported}, ""},
		{"a pass that must not connect, as its A2 was refused", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, code: 422, refused: true})
			p.mustNot, p.acqErr, p.unheld[auditS] = true, "being deleted", string(cu.ReasonDeleting)
			b.end(p)
		}, nil, ""},
		{"a Degraded deletion pass, as its A2 was refused", func(b *auditRun) {
			b.acquired()
			p := b.deletion()
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, code: 422, refused: true})
			p.degraded, p.acqErr, p.unheld[auditS] = true, "being deleted", string(cu.ReasonDeleting)
			b.end(p)
		}, nil, countDegraded},
		{"a pass whose A1 conflicted", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.refused(p, stCall{op: "a1", obj: auditH, code: 409})
			p.conflict = true
			b.end(p)
		}, nil, ""},
		{"a pass whose holder was gone", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.refused(p, stCall{op: "a1", obj: auditH, code: 404, notFound: true})
			p.mustNot, p.acqErr = true, "not found"
			b.end(p)
		}, nil, ""},
		{"a pass whose call had no answer", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, unknown: true})
			p.degraded, p.acqErr = true, "connection reset"
			b.end(p)
		}, nil, countAPIErrors},
		{"a pass whose patch was refused with a 404, of an object that is there", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, code: 404, notFound: true})
			b.call(stCall{proc: 1, g: "5", pass: p.id, op: "get", obj: auditS}, func(c *stCall) { c.from(b.last[auditS]) })
			p.degraded, p.acqErr = true, "the object is there"
			b.end(p)
		}, nil, ""},
		{"a pass that its controller's stop cut", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			p.degraded, p.acqErr, p.cut = true, "context canceled", true
			b.end(p)
		}, nil, ""},
		{"a ref that is Missing, and no read says so", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			p.unheld[auditS] = string(cu.ReasonMissing)
			b.end(p)
		}, []string{findUnreported}, ""},
		{"a ref that is Missing, as an A2 was answered", func(b *auditRun) {
			b.acquired()
			p := b.pass()
			b.call(stCall{proc: 1, g: "5", pass: p.id, op: "a2", obj: auditS, fin: auditFin, err: "not found", notFound: true}, func(*stCall) {})
			p.unheld[auditS] = string(cu.ReasonMissing)
			b.end(p)
		}, nil, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			c.build(b)
			r := b.want(t, c.want...)
			if c.count != "" {
				wantCount(t, r, c.count, 1)
			}
		})
	}
}

// The records.
func TestAuditRecords(t *testing.T) {
	t.Run("a dropped ref", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		p := b.deletion()
		b.a1(p)
		b.a2(p, auditS, "e2")
		b.end(p, auditS)
		b.want(t, findRecordDrop, findRecordDiffers)
	})
	t.Run("a ref the holder names no more", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.create(auditS2, "v1")
		b.put(auditH, "uh", func(v *stVersion) { v.spec = "s2" })
		p := b.pass()
		b.a1(p, auditS2)
		b.a2(p, auditS2, "e2")
		b.end(p, auditS2)
		r := b.want(t)
		wantCount(t, r, countRecords, 2)
	})
	t.Run("Forget, in the write that removes the owner finalizer", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.put(auditH, "uh", func(v *stVersion) { v.deleting, v.fins, v.rec = true, []string{"example.com/other"}, nil })
		b.want(t)
	})
	for _, c := range []struct {
		consent string
		want    []string
	}{{"", nil}, {"other, ns", []string{findRecordDrop, findUnreported}}} {
		t.Run(fmt.Sprintf("a ref in another namespace, whose consent is %q", c.consent), func(t *testing.T) {
			b := newAuditRun(stressCross)
			other := stObj{"Secret", "target", "c"}
			b.put(nsObj("target"), "ut", nil)
			b.create(other, "uc")
			b.put(other, "uc", func(v *stVersion) { v.consent = c.consent })
			b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "target/c", []stObj{other} })
			p := b.pass()
			for _, o := range []stObj{nsObj("target"), other} {
				b.call(stCall{proc: 1, g: "5", pass: p.id, op: "get", obj: o}, func(c *stCall) { c.from(b.last[o]) })
			}
			b.a1(p)
			p.unheld[other] = string(cu.ReasonConsentWithdrawn)
			b.end(p)
			b.want(t, c.want...)
		})
	}
	t.Run("a Secret behind a Target", func(t *testing.T) {
		b := newAuditRun(stressChained)
		target := stObj{"Target", "ns", "t"}
		b.create(target, "ut")
		b.put(target, "ut", func(v *stVersion) { v.spec = "s" })
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "t", []stObj{target, auditS} })
		p := b.pass()
		b.a1(p, target)
		b.a2(p, target, "e1")
		b.end(p, target)
		r := b.want(t)
		wantCount(t, r, countDrops, 1)
	})
}

// A recording that can't be trusted fails the run.
func TestAuditCompleteness(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(b *auditRun)
	}{
		{"a gap", func(b *auditRun) { b.gaps = []string{"the watch of Secret ended"} }},
		{"an A2 without its version", func(b *auditRun) {
			b.acquired()
			b.calls[len(b.calls)-1].rv += 1000
		}},
		{"a create without its version", func(b *auditRun) { b.writes[0].rv += 1000 }},
		{"a DELETE without its removal", func(b *auditRun) { b.answer(b.began(auditS), answerStatus, "u1") }},
		{"a DELETE of unknown outcome", func(b *auditRun) { b.answer(b.began(auditS), answerUnknown, "") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			c.build(b)
			b.want(t, findIncomplete)
		})
	}
}

// What the recorder makes of the calls it sees: the patches the package
// builds, and the API server's answers to a DELETE.
func TestRecorderReads(t *testing.T) {
	m := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "s", UID: "u1", ResourceVersion: "7"}}
	strategic, err := cu.AcquireStrategicPatch(widgetGK, "ns", "e1")
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := cu.AcquireGuardedPatch(m, widgetGK, "ns", "e1")
	if err != nil {
		t.Fatal(err)
	}
	held := m.DeepCopy()
	held.Finalizers = []string{"example.com/other", auditFin}
	held.Annotations = map[string]string{auditFin: "e1"}
	held.Labels = map[string]string{cu.LabelHeld: "true", cu.MarkerLabel(widgetGK): "true"}
	release, err := cu.ReleasePatch(held, widgetGK, "ns")
	if err != nil {
		t.Fatal(err)
	}
	disown, err := cu.JanitorPatch(m, widgetGK)
	if err == nil {
		t.Fatalf("a janitor's patch of an object without labels: %s", disown.Data)
	}
	labelled := m.DeepCopy()
	labelled.Labels = held.Labels
	janitor, err := cu.JanitorPatch(labelled, widgetGK)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		pt   types.PatchType
		data []byte
		want stCall
	}{
		{"A1", types.MergePatchType, []byte(`{"metadata":{"resourceVersion":"7","annotations":{"` + cu.AnnotationRefs + `":null}}}`), stCall{op: "a1"}},
		{"A2 of a Secret", strategic.Type, strategic.Data, stCall{op: "a2", fin: auditFin, wrote: "e1"}},
		{"A2 of a custom resource", guarded.Type, guarded.Data, stCall{op: "a2", fin: auditFin, wrote: "e1"}},
		{"R3", release.Type, release.Data, stCall{op: "r3", fin: auditFin, testedUID: "u1", tested: "e1"}},
		{"the janitor's", janitor.Type, janitor.Data, stCall{op: string(types.JSONPatchType)}},
	} {
		if got := patchOf(c.pt, c.data); got.String() != c.want.String() || got.fin != c.want.fin || got.wrote != c.want.wrote || got.tested != c.want.tested || got.testedUID != c.want.testedUID {
			t.Errorf("%s: read as %s, want %s", c.name, got, c.want)
		}
	}

	for _, c := range []struct{ name, data, answer, uid string }{
		{"a Status", `{"kind":"Status","apiVersion":"v1","status":"Success","details":{"name":"s","kind":"secrets","uid":"u1"}}`, answerStatus, "u1"},
		{"a Status that names nothing", `{"kind":"Status","apiVersion":"v1","status":"Success","details":{"name":"s"}}`, answerUnknown, ""},
		{"the object, being deleted", `{"kind":"Secret","metadata":{"uid":"u1","resourceVersion":"9","deletionTimestamp":"2026-11-24T08:00:00Z"},"data":{}}`, answerMarked, "u1"},
		{"the object, with a status", `{"kind":"Target","metadata":{"uid":"u1","resourceVersion":"9","deletionTimestamp":"2026-11-24T08:00:00Z"},"status":{"x":1}}`, answerMarked, "u1"},
		{"the object, not being deleted", `{"kind":"Secret","metadata":{"uid":"u1","resourceVersion":"9"}}`, answerUnknown, ""},
		{"nothing", ``, answerUnknown, ""},
	} {
		answer, uid, _, _ := deleteAnswer([]byte(c.data))
		if answer != c.answer || uid != c.uid {
			t.Errorf("a DELETE answered with %s: %s %s, want %s %s", c.name, answer, uid, c.answer, c.uid)
		}
	}

	for _, c := range []struct {
		name, v string
		want    []stObj
		bad     bool
	}{
		{"empty", `[]`, nil, false},
		{"null", `null`, nil, true},
		{"not a list", `{}`, nil, true},
		{"a missing namespace", `[{"group":"","kind":"Secret","name":"s"}]`, nil, true},
		{"an unknown key", `[{"group":"","kind":"Secret","namespace":"ns","name":"s","x":""}]`, nil, true},
		{"something after it", `[] []`, nil, true},
	} {
		rec, bad := parseRecord(map[string]string{cu.AnnotationRefs: c.v})
		if bad != c.bad || !slices.Equal(rec, c.want) {
			t.Errorf("the record %s: %v %v, want %v %v", c.name, rec, bad, c.want, c.bad)
		}
	}
	if rec, bad := parseRecord(nil); rec != nil || bad {
		t.Errorf("no record: %v %v", rec, bad)
	}
	enc, err := cu.EncodeRecord([]cu.ID{cu.SecretRef("ns", "s").ID(), {Group: widgetGK.Group, Kind: "Target", Namespace: "ns", Name: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec, bad := parseRecord(map[string]string{cu.AnnotationRefs: enc}); bad || !slices.Equal(rec, []stObj{auditS, {"Target", "ns", "t"}}) {
		t.Errorf("the package's record: %v %v", rec, bad)
	}

	for _, c := range []struct {
		object, ns string
		want       bool
	}{{"", "", false}, {"ns", "", true}, {"", "a, ns", true}, {"*", "", true}, {"", "*", false}, {"nsx", "xns", false}} {
		if consented(c.object, c.ns, "ns") != c.want {
			t.Errorf("consent %q on the object and %q on its namespace: want %v", c.object, c.ns, c.want)
		}
	}
	f, err := cu.FinalizerName(widgetGK, "ns")
	if err != nil || f != auditFin {
		t.Errorf("the finalizer is %s (%v), and these tests use %s", f, err, auditFin)
	}
	if !json.Valid(release.Data) {
		t.Errorf("%s", release.Data)
	}
}
