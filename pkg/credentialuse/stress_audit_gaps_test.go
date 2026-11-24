package credentialuse_test

// What a cold review of the stress test's audits found them blind to, each as
// a made-up history.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// auditC is a Secret in another namespace, whose consent a history sets.
var auditC = stObj{"Secret", "target", "c"}

// newCrossRun begins a cross-namespace history: the holder names auditC,
// whose own consent is consent.
func newCrossRun(consent string) *auditRun {
	b := newAuditRun(stressCross)
	b.put(nsObj("target"), "ut", nil)
	b.create(auditC, "uc")
	b.put(auditC, "uc", func(v *stVersion) { v.consent = consent })
	b.put(auditH, "uh", func(v *stVersion) { v.spec = "target/c" })
	return b
}

// consentReads are p's live reads of auditC's consent: its Namespace's, and
// its own.
func (b *auditRun) consentReads(p *stPass) {
	for _, o := range []stObj{nsObj(auditC.ns), auditC} {
		b.call(stCall{proc: 1, g: "5", pass: p.id, op: "get", obj: o}, func(c *stCall) { c.from(b.last[o]) })
	}
}

// refused is a call of p's that the API server refused.
func (b *auditRun) refused(p *stPass, c stCall) {
	c.proc, c.g, c.pass = 1, "5", p.id
	if c.err == "" {
		c.err = "refused"
	}
	b.call(c, func(*stCall) {})
}

// A release is judged on the reads its patch was built from: a list of an
// older batch, or of an older round, isn't R2's.
func TestAuditGapBatch(t *testing.T) {
	b := newAuditRun(stressPlain)
	b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
	b.r1()
	b.list()
	p := b.pass()
	b.a1(p, auditS)
	b.r1()
	b.r3() // the A1 is known, and no list was made since R1
	b.a2(p, auditS, "e1")
	b.end(p, auditS)
	b.want(t, "RELEASED-WHAT-IT-HADNT-READ")
}

// An A2 of another namespace's object follows a live read of its consent, and
// a ref reported without consent was refused it.
func TestAuditGapConsent(t *testing.T) {
	for _, c := range []struct {
		name, consent string
		build         func(b *auditRun, p *stPass)
		want          []string
	}{
		{"an A2 without a read", "ns", func(b *auditRun, p *stPass) {
			b.a1(p, auditC)
			b.a2(p, auditC, "e1")
			b.end(p, auditC)
		}, []string{"HELD-WITHOUT-CONSENT"}},
		{"an A2 after a read that refuses", "other", func(b *auditRun, p *stPass) {
			b.consentReads(p)
			b.a1(p, auditC)
			b.a2(p, auditC, "e1")
			b.end(p, auditC)
		}, []string{"HELD-WITHOUT-CONSENT"}},
		{"an A2 before the read", "ns", func(b *auditRun, p *stPass) {
			b.a1(p, auditC)
			b.a2(p, auditC, "e1")
			b.consentReads(p)
			b.end(p, auditC)
		}, []string{"HELD-WITHOUT-CONSENT"}},
		{"an A2 after a read that consents", "ns", func(b *auditRun, p *stPass) {
			b.consentReads(p)
			b.a1(p, auditC)
			b.a2(p, auditC, "e1")
			b.end(p, auditC)
		}, nil},
		{"NotConsented without a read", "", func(b *auditRun, p *stPass) {
			p.unheld[auditC] = string(cu.ReasonNotConsented)
			b.end(p)
		}, []string{"REF-UNACCOUNTED"}},
		{"NotConsented after a read that consents", "ns", func(b *auditRun, p *stPass) {
			b.consentReads(p)
			p.unheld[auditC] = string(cu.ReasonNotConsented)
			b.end(p)
		}, []string{"REF-UNACCOUNTED"}},
		{"NotConsented after a read that refuses", "", func(b *auditRun, p *stPass) {
			b.consentReads(p)
			p.unheld[auditC] = string(cu.ReasonNotConsented)
			b.end(p)
		}, nil},
		{"ConsentUnreadable, and no read failed", "", func(b *auditRun, p *stPass) {
			b.consentReads(p)
			p.unheld[auditC] = string(cu.ReasonConsentUnreadable)
			b.end(p)
		}, []string{"REF-UNACCOUNTED"}},
		{"ConsentUnreadable, as a read failed", "", func(b *auditRun, p *stPass) {
			b.refused(p, stCall{op: "get", obj: nsObj(auditC.ns), err: "timeout", unknown: true})
			p.unheld[auditC] = string(cu.ReasonConsentUnreadable)
			b.end(p)
		}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newCrossRun(c.consent)
			c.build(b, b.pass())
			b.want(t, c.want...)
		})
	}

	// held is a history with auditC announced and held, with consent.
	held := func() *auditRun {
		b := newCrossRun("ns")
		p := b.pass()
		b.consentReads(p)
		b.a1(p, auditC)
		b.deliver(1, auditH)
		b.a2(p, auditC, "e1")
		b.deliver(1, auditC)
		b.end(p, auditC)
		return b
	}
	t.Run("a live pass skips on a cached yes", func(t *testing.T) {
		b := held()
		b.end(b.pass(), auditC)
		b.want(t)
	})
	t.Run("a live pass skips, after its informer delivered that the consent went", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		b.deliver(1, auditC)
		b.wait(2 * time.Second)
		b.end(b.pass(), auditC)
		b.want(t, "HELD-WITHOUT-CONSENT")
	})
	t.Run("the same, and the Namespace's consent isn't delivered as gone", func(t *testing.T) {
		b := newCrossRun("")
		b.put(nsObj("target"), "ut", func(v *stVersion) { v.consent = "ns" })
		p := b.pass()
		b.consentReads(p)
		b.a1(p, auditC)
		b.deliver(1, auditH)
		b.a2(p, auditC, "e1")
		b.deliver(1, auditC)
		b.end(p, auditC)
		b.put(nsObj("target"), "ut", func(v *stVersion) { v.consent = "" })
		b.wait(2 * time.Second)
		b.end(b.pass(), auditC)
		b.want(t)
		b.deliver(1, nsObj("target"))
		b.wait(2 * time.Second)
		b.end(b.pass(), auditC)
		b.want(t, "HELD-WITHOUT-CONSENT")
	})
	t.Run("a live pass skips, and its informer hasn't delivered that the consent went", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		b.wait(6 * time.Second)
		b.end(b.pass(), auditC)
		b.want(t)
	})
	t.Run("a live pass skips before a consent that comes after it", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		b.deliver(1, auditC)
		b.wait(2 * time.Second)
		b.end(b.pass(), auditC)
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "ns" })
		b.deliver(1, auditC)
		b.want(t, "HELD-WITHOUT-CONSENT")
	})
	t.Run("a drop after two reads, the last of which consents", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		p := b.pass()
		b.consentReads(p)
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "ns" })
		b.consentReads(p)
		b.a1(p)
		p.unheld[auditC] = string(cu.ReasonConsentWithdrawn)
		b.end(p)
		b.want(t, "RECORD-DROP", "REF-UNACCOUNTED")
	})
}

// A pass whose acquire stopped says why, in what the API server answered it.
func TestAuditGapErrors(t *testing.T) {
	b := newAuditRun(stressPlain).acquired()
	p := b.pass()
	p.degraded, p.acqErr = true, "something"
	b.end(p)
	b.want(t, "ACQUIRE-FAILED")

	b = newAuditRun(stressPlain).acquired()
	p = b.pass()
	p.conflict = true
	b.end(p)
	b.want(t, "ACQUIRE-FAILED")

	b = newAuditRun(stressPlain).acquired()
	p = b.pass()
	p.unheld[auditS] = string(cu.ReasonDeleting)
	b.end(p)
	b.want(t, "REF-UNACCOUNTED")
}

// A release of unknown outcome explains the loss of a finalizer it was built
// for, once.
func TestAuditGapUnknownRelease(t *testing.T) {
	lose := func(b *auditRun, uid, epoch string) {
		b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
		b.batch()
		b.call(stCall{op: "r3", obj: auditS, fin: auditFin, testedUID: uid, tested: epoch, err: "connection reset", unknown: true}, func(*stCall) {
			b.put(auditS, "u1", func(v *stVersion) {
				v.fins = slices.DeleteFunc(v.fins, func(f string) bool { return f == auditFin })
				delete(v.epochs, auditFin)
			})
		})
	}
	for _, c := range []struct {
		name, uid, epoch string
		want             []string
	}{
		{"its own", "u1", "e1", nil},
		{"another incarnation's", "u0", "e1", []string{"FINALIZER-LOST"}},
		{"another epoch's", "u1", "e0", []string{"FINALIZER-LOST"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain).acquired()
			lose(b, c.uid, c.epoch)
			b.want(t, c.want...)
		})
	}
	t.Run("two losses", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		lose(b, "u1", "e1")
		b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e1") })
		b.put(auditS, "u1", func(v *stVersion) { v.fins, v.epochs = nil, nil })
		b.want(t, "FINALIZER-LOST")
	})
	t.Run("it is judged", func(t *testing.T) {
		b := newAuditRun(stressPlain).acquired()
		b.batch()
		b.call(stCall{op: "r3", obj: auditS, fin: auditFin, testedUID: "u1", tested: "e1", err: "connection reset", unknown: true}, func(*stCall) {
			b.put(auditS, "u1", func(v *stVersion) { v.fins, v.epochs = nil, nil })
		})
		b.want(t, "RELEASED-OVER-A-USER")
	})
}

// A skip rests on what the process's own informers had delivered.
func TestAuditGapSkip(t *testing.T) {
	// released is a history whose Secret, held by process 1, was deleted and
	// released after its grace, by process proc, and recreated.
	released := func(proc int, delivered bool) *auditRun {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		b.wait(testGrace)
		from := len(b.calls)
		b.batch()
		b.r3()
		for i := from; i < len(b.calls); i++ {
			b.calls[i].proc = proc
		}
		if delivered {
			b.deliver(1, auditS)
		}
		b.create(auditS, "u2")
		return b
	}
	for _, c := range []struct {
		name  string
		build func() *auditRun
		want  []string
	}{
		{"another process released it, and this one's informer hasn't delivered that", func() *auditRun {
			b := released(2, false)
			b.wait(6 * time.Second)
			return b
		}, nil}, // §6.5's window
		{"its informer has delivered that", func() *auditRun {
			b := released(2, true)
			b.wait(time.Second)
			return b
		}, []string{"HELD-WITHOUT-A2"}},
		{"its informer is just delivering that", func() *auditRun { return released(2, true) }, nil},
		{"its informer has delivered that, and something since", func() *auditRun {
			b := released(2, true)
			b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "x") })
			b.deliver(1, auditS)
			return b
		}, []string{"HELD-WITHOUT-A2"}},
		{"it released it itself", func() *auditRun {
			b := released(1, false)
			b.wait(time.Second)
			return b
		}, []string{"HELD-WITHOUT-A2"}},
		{"another process released it over a user, and its informer has delivered the loss", func() *auditRun {
			b := newAuditRun(stressPlain).acquired()
			b.batch()
			b.r3()
			b.deliver(1, auditS)
			b.wait(time.Second)
			return b
		}, []string{"HELD-WITHOUT-A2", "RELEASED-OVER-A-USER"}},
		{"it wrote again since", func() *auditRun {
			b := released(2, true)
			p := b.pass()
			b.a2(p, auditS, "e3")
			b.deliver(1, auditS)
			b.end(p, auditS)
			b.wait(time.Second)
			return b
		}, nil},
		{"its informer hasn't delivered the version it works from", func() *auditRun {
			b := newAuditRun(stressPlain).acquired()
			b.put(auditH, "uh", nil)
			return b
		}, []string{"HELD-WITHOUT-A2"}},
		{"its A2 came before its informer delivered the record", func() *auditRun {
			b := newAuditRun(stressPlain)
			p := b.pass()
			b.a1(p, auditS)
			b.a2(p, auditS, "e1")
			b.deliver(1, auditH)
			b.end(p, auditS)
			return b
		}, []string{"HELD-WITHOUT-A2"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := c.build()
			b.end(b.pass(), auditS)
			b.want(t, c.want...)
		})
	}
}

// What an A2 writes with the finalizer, and a release takes with the last of
// them (§3).
func TestAuditGapLabels(t *testing.T) {
	b := newAuditRun(stressPlain)
	p := b.pass()
	b.a1(p, auditS)
	b.put(auditS, "u1", func(v *stVersion) { v.badLabels = "" })
	b.a2(p, auditS, "e1")
	b.vs[len(b.vs)-1].badLabels = "the held label is there: false"
	b.end(p, auditS)
	b.want(t, "LABELS")

	held := map[string]string{cu.LabelHeld: "true", cu.MarkerLabel(widgetGK): "true"}
	for _, c := range []struct {
		name   string
		fins   []string
		labels map[string]string
		ok     bool
	}{
		{"held", []string{"example.com/other", auditFin}, held, true},
		{"neither", []string{"example.com/other"}, nil, true},
		{"no label", []string{auditFin}, nil, false},
		{"no held label", []string{auditFin}, map[string]string{cu.MarkerLabel(widgetGK): "true"}, false},
		{"no marker", []string{auditFin}, map[string]string{cu.LabelHeld: "true"}, false},
		{"labels left", nil, held, false},
	} {
		if got := badLabels(c.fins, c.labels); (got == "") != c.ok {
			t.Errorf("%s: %q", c.name, got)
		}
	}
}

// A record is exactly what the holder names, less what isn't consented.
func TestAuditGapRecord(t *testing.T) {
	b := newAuditRun(stressPlain)
	p := b.pass()
	b.a1(p, auditS, auditS2)
	b.a2(p, auditS, "e1")
	b.end(p, auditS)
	b.want(t, "RECORD-DIFFERS")
}

// A holder is established only with each ref of its record held, or found
// absent by a read of that pass made after its last A2 (§6.7).
func TestAuditGapEstablish(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(b *auditRun, p *stPass)
		want  []string
	}{
		{"held", func(b *auditRun, p *stPass) {
			b.a2(p, auditS, "e1")
			p.held = []stObj{auditS}
		}, nil},
		{"absent, by a read after the A2", func(b *auditRun, p *stPass) {
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, notFound: true})
			b.refused(p, stCall{op: "get", obj: auditS, notFound: true})
			p.unheld[auditS] = string(cu.ReasonMissing)
		}, nil},
		{"absent, by an A2 alone", func(b *auditRun, p *stPass) {
			b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, notFound: true})
			p.unheld[auditS] = string(cu.ReasonMissing)
		}, []string{"ESTABLISHED-UNHELD"}},
		{"absent, by a read before the last A2", func(b *auditRun, p *stPass) {
			b.refused(p, stCall{op: "get", obj: auditS, notFound: true})
			b.a2(p, auditS2, "e1")
			p.unheld[auditS] = string(cu.ReasonMissing)
		}, []string{"ESTABLISHED-UNHELD"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			b.create(auditS2, "v1")
			if c.name != "held" {
				w := b.began(auditS)
				b.remove(auditS)
				b.answer(w, answerStatus, "u1")
			}
			b.put(auditH, "uh2", func(v *stVersion) { v.spec = "s" })
			p := b.pass()
			b.a1(p, auditS)
			c.build(b, p)
			b.put(auditH, "uh2", func(v *stVersion) { v.fins = []string{ownerFinalizer} })
			b.end(p, p.held...)
			b.want(t, c.want...)
		})
	}
}

// recreated is a history where processes procs each release u1, which the
// first of them removes; u2 is created, f happens to it, and it goes. The
// last release is in flight the whole time if inFlight, and had ended before
// the create if not.
func recreated(procs []int, inFlight bool, f func(b *auditRun)) *auditRun {
	b := newAuditRun(stressPlain).acquired()
	b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
	b.mark(auditS)
	read := b.last[auditS]
	gs := []string{"9", "10", "11"}
	for i, proc := range procs {
		for _, o := range []stObj{auditS, nsObj("ns")} {
			b.call(stCall{proc: proc, g: gs[i], op: "get", obj: o}, func(c *stCall) { c.from(b.last[o]) })
		}
		b.listBy(proc, gs[i])
	}
	removed := func(c *stCall) {
		c.from(read)
		c.fins, c.epochs, c.testedUID, c.tested = nil, nil, "u1", "e1"
	}
	next := func() {
		b.create(auditS, "u2")
		if f != nil {
			f(b)
		}
		if b.last[auditS].uid == "u2" {
			b.remove(auditS)
		}
	}
	first := b.call(stCall{proc: procs[0], g: gs[0], op: "r3", obj: auditS, fin: auditFin}, func(c *stCall) {
		removed(c)
		b.remove(auditS)
	})
	for i := 1; i < len(procs); i++ {
		b.call(stCall{proc: procs[i], g: gs[i], op: "r3", obj: auditS, fin: auditFin}, func(c *stCall) {
			removed(c)
			if inFlight && i == 1 {
				next()
			}
		})
		b.calls[len(b.calls)-1].start = first.start
	}
	if len(procs) == 1 || !inFlight {
		next()
	}
	return b
}

// follow is an A1 of a pass of the holder, which changes nothing: a write of
// another object, that bounds what came before it.
func (b *auditRun) follow() {
	p := b.pass()
	b.a1(p)
	b.end(p)
}

// H1's recreate has a class of its own: two processes' releases of one
// incarnation answered, and the next one went while the second was in flight.
// One process's two releases are a bug of ours, and anything less than the
// whole shape explains nothing.
func TestAuditGapRecreate(t *testing.T) {
	for _, c := range []struct {
		name     string
		procs    []int
		inFlight bool
		f        func(b *auditRun)
		want     []string
	}{
		{"two processes", []int{1, 2}, true, nil, []string{"H1-RECREATE"}},
		{"and the next had been held", []int{1, 2}, true, func(b *auditRun) {
			b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "e2") })
		}, []string{"H1-RECREATE"}},
		{"and the next had been marked", []int{1, 2}, true, func(b *auditRun) {
			b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "e2") })
			b.mark(auditS)
		}, []string{"H1-RECREATE"}},
		{"one process, twice", []int{2, 2}, true, nil, []string{"RELEASED-TWICE", "UNEXPLAINED-REMOVAL"}},
		{"one release", []int{2}, true, nil, []string{"UNEXPLAINED-REMOVAL"}},
		{"both had ended before the create", []int{1, 2}, false, nil, []string{"UNEXPLAINED-REMOVAL"}},
		{"a third process released the incarnation before it too", []int{1, 2, 3}, true, nil, []string{"H1-RECREATE"}},
		{"one of the two processes released it twice", []int{1, 2, 2}, true, nil, []string{"RELEASED-TWICE", "UNEXPLAINED-REMOVAL"}},
		{"a third release, which may have landed", []int{1, 2}, true, func(b *auditRun) {
			b.call(stCall{proc: 3, g: "12", op: "r3", obj: auditS, fin: auditFin, testedUID: "u1", tested: "e1", err: "connection reset", unknown: true}, func(*stCall) {})
		}, []string{"UNEXPLAINED-REMOVAL"}},
		{"a third release, of the next one's own hold", []int{1, 2}, true, func(b *auditRun) {
			b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "e2") })
			b.call(stCall{proc: 3, g: "12", op: "get", obj: auditS}, func(c *stCall) { c.from(b.last[auditS]) })
			b.listBy(3, "12")
			b.call(stCall{proc: 3, g: "12", op: "r3", obj: auditS, fin: auditFin, testedUID: "u2", tested: "e2"}, func(c *stCall) {
				c.from(b.put(auditS, "u2", func(v *stVersion) { v.fins, v.epochs = nil, nil }))
			})
		}, []string{"UNEXPLAINED-REMOVAL"}},
		{"another write of ours", []int{1, 2}, true, func(b *auditRun) {
			b.call(stCall{proc: 3, g: "13", op: "application/json-patch+json", obj: auditS}, func(c *stCall) { c.from(b.put(auditS, "u2", nil)) })
		}, []string{"UNEXPLAINED-REMOVAL"}},
		{"an A2 that left its finalizer", []int{1, 2}, true, func(b *auditRun) {
			b.call(stCall{proc: 3, g: "13", op: "a2", obj: auditS, fin: auditFin, wrote: "e2"}, func(c *stCall) {
				c.from(b.put(auditS, "u2", func(v *stVersion) { b.hold(v, "e2") }))
			})
		}, []string{"H1-RECREATE"}},
		{"and a DELETE of the test's found nothing", []int{1, 2}, true, func(b *auditRun) {
			b.answer(b.began(auditS), answerNotFound, "")
		}, []string{"H1-RECREATE"}},
		{"and a DELETE of the test's may have landed", []int{1, 2}, true, func(b *auditRun) {
			b.answer(b.began(auditS), answerUnknown, "")
		}, []string{"INCOMPLETE", "UNEXPLAINED-REMOVAL"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := recreated(c.procs, c.inFlight, c.f)
			b.follow()
			b.want(t, c.want...)
		})
	}
	t.Run("the first pair fails, and another holds", func(t *testing.T) {
		// The release that took u1 ended after the create began, and before it
		// landed; and a write followed it, before the other took u2.
		b := recreated([]int{1, 2}, true, func(b *auditRun) { b.follow() })
		for i := range b.writes {
			if w := &b.writes[i]; w.uid == "u2" {
				w.start = b.calls[0].start
			}
		}
		b.follow()
		b.want(t, "H1-RECREATE")
	})
	t.Run("a DELETE that may have landed, whose client gave up before the create", func(t *testing.T) {
		c := recreated([]int{1, 2}, true, nil)
		c.writes = append([]stWrite{{op: "DELETE", obj: auditS, start: c.calls[0].start, end: c.calls[0].start, answer: answerUnknown}}, c.writes...)
		c.follow()
		c.want(t, "INCOMPLETE", "UNEXPLAINED-REMOVAL")
	})
	// The create's answer may come before the late release's, as above, or
	// after it.
	t.Run("the create's answer comes last", func(t *testing.T) {
		b := recreated([]int{1, 2}, true, nil)
		late := b.calls[len(b.calls)-1]
		b.follow()
		for i := range b.writes {
			if w := &b.writes[i]; w.uid == "u2" {
				if !w.end.Before(late.end) {
					t.Fatalf("the create's answer came after the late release's already")
				}
				w.end = late.end.Add(time.Millisecond)
			}
		}
		b.want(t, "H1-RECREATE")
	})
	t.Run("the late release ended as the create began", func(t *testing.T) {
		b := recreated([]int{1, 2}, true, nil)
		for i := range b.writes {
			if w := &b.writes[i]; w.uid == "u2" {
				b.calls[len(b.calls)-1].end = w.start
			}
		}
		b.want(t, "UNEXPLAINED-REMOVAL")
	})
}

// The recreate's far end is bound without clocks: what began after the late
// release had ended, and was answered as of before the removal, shows that
// the release didn't make it.
func TestAuditGapRecreateEnd(t *testing.T) {
	// late is a recreate whose removal comes after the late release ended,
	// and after f; before, if f is nil. A write follows in gap.
	late := func(f func(b *auditRun), gap time.Duration) *auditRun {
		var gone stVersion
		b := recreated([]int{1, 2}, true, func(b *auditRun) {
			if f != nil {
				gone = b.last[auditS]
				delete(b.last, auditS) // removed below
			}
		})
		if f != nil {
			b.last[auditS] = gone
			f(b)
			b.remove(auditS)
		}
		if gap > 0 {
			b.wait(gap)
			b.follow()
		}
		return b
	}
	late(nil, time.Millisecond).want(t, "H1-RECREATE")
	late(nil, time.Second).want(t, "H1-RECREATE-UNBOUNDED")
	late(nil, 0).want(t, "H1-RECREATE-UNBOUNDED")
	// What bounds it has to have been answered within the bound: till then,
	// its revision may be later.
	b := late(nil, time.Millisecond)
	b.calls[len(b.calls)-1].end = b.at.Add(time.Second)
	b.want(t, "H1-RECREATE-UNBOUNDED")
	// A page after the first is of the first's revision.
	b = late(nil, 0)
	b.listBy(3, "12")
	b.calls[len(b.calls)-1].continued = true
	b.calls[len(b.calls)-1].rv = b.vs[0].rv
	b.follow()
	b.want(t, "H1-RECREATE")
	late(func(b *auditRun) {
		p := b.pass()
		b.a1(p)
		b.end(p)
	}, 0).want(t, "UNEXPLAINED-REMOVAL")
	late(func(b *auditRun) {
		b.call(stCall{op: "get", obj: auditS}, func(c *stCall) { c.from(b.last[auditS]) })
	}, 0).want(t, "UNEXPLAINED-REMOVAL")
	late(func(b *auditRun) { b.listBy(3, "12") }, 0).want(t, "UNEXPLAINED-REMOVAL")
	late(func(b *auditRun) { b.create(auditS2, "v1") }, 0).want(t, "UNEXPLAINED-REMOVAL")
}

// Each version of a holder, and of a held object, reaches every process's
// feed within 5s of the test's watch, or a later one does.
func TestAuditGapLag(t *testing.T) {
	base := func() *auditRun {
		b := newAuditRun(stressPlain)
		b.synced = map[int]time.Time{1: b.tick()}
		b.put(auditH, "uh", func(v *stVersion) { v.rec = []stObj{auditS} })
		return b
	}
	deliver := func(b *auditRun) {
		b.deliver(1, auditH)
		b.events = append(b.events, stEvent{stFeed: stFeed{1, "Target", "holder"}, uid: "x", rv: 1, at: b.tick()})
	}
	for _, c := range []struct {
		name  string
		build func(b *auditRun)
		want  []string
	}{
		{"delivered", func(b *auditRun) {
			deliver(b)
			b.wait(10 * time.Second)
		}, nil},
		{"never", func(b *auditRun) { b.wait(10 * time.Second) }, []string{"LAG"}},
		{"late", func(b *auditRun) {
			b.wait(6 * time.Second)
			deliver(b)
		}, []string{"LAG"}},
		{"a later one, in time", func(b *auditRun) {
			b.put(auditH, "uh", nil)
			deliver(b)
			b.wait(10 * time.Second)
		}, nil},
		{"the process stopped", func(b *auditRun) { b.wait(4 * time.Second) }, nil},
		{"it was gone before it had to be", func(b *auditRun) {
			b.wait(time.Second)
			b.remove(auditH)
			b.wait(10 * time.Second)
		}, nil}, // a relist delivers no more of it
		{"a held object, and a deletion of it with an old revision", func(b *auditRun) {
			deliver(b)
			w := b.began(auditS)
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			for _, k := range []string{"Widget", "Target"} {
				b.events = append(b.events, stEvent{stFeed: stFeed{1, k, "protected"}, uid: "u1", rv: b.rv, at: b.tick()})
			}
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e1") })
			b.remove(auditS)
			b.answer(w, answerStatus, "u1")
			for _, k := range []string{"Widget", "Target"} {
				b.events = append(b.events, stEvent{stFeed: stFeed{1, k, "protected"}, uid: "u1", rv: b.vs[len(b.vs)-3].rv, at: b.tick()})
			}
			b.wait(10 * time.Second)
		}, nil}, // a relist's tombstone
		{"a held object", func(b *auditRun) {
			deliver(b)
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.wait(10 * time.Second)
		}, []string{"LAG", "LAG"}}, // to each of the two kinds' trackers
		{"an object that isn't held", func(b *auditRun) {
			deliver(b)
			b.put(auditS, "u1", nil)
			b.wait(10 * time.Second)
		}, nil},
		{"a process that syncs after it", func(b *auditRun) {
			b.synced[1] = b.tick()
			b.wait(10 * time.Second)
		}, nil},
		{"outside the caches, a held object", func(b *auditRun) {
			b.outside = true
			deliver(b)
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e0") })
			b.wait(10 * time.Second)
		}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := base()
			c.build(b)
			b.stopped = b.at
			b.want(t, c.want...)
		})
	}
}

// Contexts that end aren't the API server's answers; and a process's own
// release drops what it wrote once it has returned.
func TestAuditGapThirdReview(t *testing.T) {
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		ctx  context.Context
		err  error
		want []string
	}{
		{context.Background(), errors.New("connect: connection refused"), nil},
		{context.Background(), fmt.Errorf("patching: %w", context.Canceled), []string{"ACQUIRE-FAILED"}},
		{context.Background(), fmt.Errorf("patching: %w", context.DeadlineExceeded), []string{"ACQUIRE-FAILED"}},
		{context.Background(), errors.New("client rate limiter Wait returned an error: rate: Wait(n=1) would exceed context deadline"), []string{"ACQUIRE-FAILED"}},
		{ended, errors.New("connect: operation was canceled"), []string{"ACQUIRE-FAILED"}},
		{context.Background(), apierrors.NewTimeoutError("context deadline exceeded", 1), nil}, // the API server's 504
		{ended, apierrors.NewInternalError(errors.New("etcd")), nil},                           // it answered, whatever became of the context
	} {
		b := newAuditRun(stressPlain).acquired()
		p := b.pass()
		b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, unknown: !answered(c.err), ours: endedByUs(c.ctx, c.err), err: c.err.Error()})
		p.degraded, p.acqErr = true, c.err.Error()
		b.end(p)
		b.want(t, c.want...)
	}

	// own is a history whose process 1 released its own hold just before a
	// live pass that skips.
	own := func(since func(b *auditRun)) *auditRun {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		b.wait(testGrace)
		from := len(b.calls)
		b.batch()
		b.r3()
		for i := from; i < len(b.calls); i++ {
			b.calls[i].proc = 1
		}
		if since != nil {
			since(b)
		}
		b.end(b.pass(), auditS)
		return b
	}
	own(nil).want(t) // the worker may not have got to it
	own(func(b *auditRun) { b.wait(time.Second) }).want(t, "HELD-WITHOUT-A2")
	own(func(b *auditRun) {
		b.call(stCall{proc: 1, g: "9", op: "get", obj: auditS2, err: "not found", notFound: true}, func(*stCall) {})
	}).want(t, "HELD-WITHOUT-A2") // it has made its next call
}

// A batch met the canary's deleting user only if it listed it after the grace
// that its own process counts, in a live namespace.
func TestAuditGapKept(t *testing.T) {
	canary := stObj{"Widget", "ns", canaryHolder}
	for _, c := range []struct {
		name      string
		wait      time.Duration
		seen      bool
		torn, del bool
		want      int
	}{
		{"after the grace", testGrace, true, false, true, 1},
		{"before it", testGrace - 100*time.Millisecond, true, false, true, 0},
		{"its process hasn't seen the deletion", testGrace, false, false, true, 0},
		{"its namespace is being deleted", testGrace, true, true, true, 0},
		{"the user isn't being deleted", testGrace, true, false, false, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain)
			b.put(canary, "uc", func(v *stVersion) { v.spec, v.rec, v.fins = "s", []stObj{auditS}, []string{ownerFinalizer} })
			b.put(auditS, "u1", func(v *stVersion) { b.hold(v, "e1") })
			b.mark(auditS)
			if c.seen {
				b.deliver(2, auditS)
			}
			b.wait(c.wait)
			if c.torn {
				b.put(nsObj("ns"), "uns", func(v *stVersion) { v.deleting = true })
			}
			b.put(canary, "uc", func(v *stVersion) { v.deleting = c.del })
			b.batch()
			wantCount(t, b.want(t), countKept, c.want)
		})
	}
}

// A read that answers less than the watch had delivered before it began isn't
// a live one.
func TestAuditGapStaleRead(t *testing.T) {
	b := newAuditRun(stressPlain).acquired()
	b.mark(auditS)
	b.wait(testGrace)
	live := b.last[nsObj("ns")]
	b.put(nsObj("ns"), "uns", func(v *stVersion) { v.deleting = true })
	b.r1()
	b.call(stCall{op: "get", obj: nsObj("ns")}, func(c *stCall) { c.from(live) })
	b.list()
	b.r3()
	b.want(t, "STALE-READ")

	b = newCrossRun("ns")
	p := b.pass()
	old := b.last[auditC]
	b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
	b.call(stCall{proc: 1, g: "5", pass: p.id, op: "get", obj: nsObj("target")}, func(c *stCall) { c.from(b.last[nsObj("target")]) })
	b.call(stCall{proc: 1, g: "5", pass: p.id, op: "get", obj: auditC}, func(c *stCall) { c.from(old) })
	b.a1(p, auditC)
	b.a2(p, auditC, "e1")
	b.end(p, auditC)
	b.want(t, "STALE-READ")

	b = newAuditRun(stressPlain)
	b.call(stCall{op: "get", obj: auditS, err: "not found", notFound: true}, func(*stCall) {})
	b.put(auditS, "u1", func(v *stVersion) { v.consent = "x" })
	b.want(t, "STALE-READ")

	b = newAuditRun(stressPlain)
	b.call(stCall{op: "get", obj: auditS, err: "not found", notFound: true}, func(*stCall) {})
	w := b.began(auditS)
	b.remove(auditS)
	b.answer(w, answerStatus, "u1")
	b.want(t) // it may have gone before the read, and the watch be late

	// The same, with a hold between (H3): the watch is late by two versions.
	gone := func(f func(b *auditRun)) *auditRun {
		b := newAuditRun(stressPlain)
		w := b.began(auditS)
		b.call(stCall{op: "get", obj: auditS, err: "not found", notFound: true}, func(*stCall) {})
		if f != nil {
			f(b)
		}
		p := b.pass()
		b.a1(p, auditS)
		b.a2(p, auditS, "e1")
		b.end(p, auditS)
		b.remove(auditS)
		b.answer(w, answerStatus, "u1")
		return b
	}
	gone(nil).want(t, "STALE-READ") // the A2 began after the read, and held it
	b = gone(nil)
	for i := range b.calls[1:] {
		b.calls[i+1].start = b.calls[0].start // it may have begun before
	}
	b.want(t)
}

// What explains a pass that stopped is what it was answered last.
func TestAuditGapStop(t *testing.T) {
	b := newAuditRun(stressPlain).acquired()
	p := b.pass()
	b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, unknown: true})
	b.a2(p, auditS, "e2")
	p.degraded, p.acqErr = true, "something else"
	b.end(p, auditS)
	b.want(t, "ACQUIRE-FAILED")

	b = newAuditRun(stressPlain).acquired()
	p = b.pass()
	b.refused(p, stCall{op: "a2", obj: auditS2, fin: auditFin, unknown: true})
	p.degraded, p.acqErr = true, "connection reset"
	b.end(p)
	b.want(t, "ACQUIRE-FAILED") // of an object it doesn't name

	// A pass that stopped at its A2 had written its record.
	b = newAuditRun(stressPlain)
	p = b.pass()
	b.refused(p, stCall{op: "a2", obj: auditS, fin: auditFin, unknown: true})
	p.degraded, p.acqErr = true, "connection reset"
	b.end(p)
	b.want(t, "RECORD-DIFFERS")
}

// H1's lost A2 has a class of its own, where a deletion pass's read fails for
// it: the A2 is the incarnation's last version, and every release that
// answered had tested an older one.
func TestAuditGapLostA2(t *testing.T) {
	lost := func(tested func(read, last stVersion) uint64) *auditRun {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		b.wait(testGrace)
		b.batch() // over a live user, after the grace
		read := b.last[auditS]
		p := b.deletion()
		b.a2(p, auditS, "e2")
		b.r3()
		b.calls[len(b.calls)-1].rv = tested(read, b.vs[len(b.vs)-2])
		b.lostRead(p)
		b.end(p, auditS)
		return b
	}
	r := lost(func(read, _ stVersion) uint64 { return read.rv }).want(t, "H1-LOST-A2")
	wantCount(t, r, countLostA2, 1)
	lost(func(_, last stVersion) uint64 { return last.rv }).want(t, "LOST-READ", "RELEASED-OVER-A-USER", "RELEASED-WHAT-IT-HADNT-READ")

	// A release of unknown outcome may have taken it without testing the epoch.
	b := lost(func(read, _ stVersion) uint64 { return read.rv })
	b.call(stCall{op: "r3", obj: auditS, fin: auditFin, testedUID: "u1", err: "connection reset", unknown: true}, func(*stCall) {})
	b.want(t, "LOST-READ")
}

// A release of unknown outcome may have taken the last finalizer, and the
// object with it; and is judged.
func TestAuditGapUnknownRemoval(t *testing.T) {
	for _, c := range []struct {
		name, epoch string
		users       bool
		want        []string
	}{
		{"its own", "e1", false, nil},
		{"another epoch's", "e0", false, []string{"UNEXPLAINED-REMOVAL"}},
		{"over a user", "e1", true, []string{"RELEASED-INSIDE-THE-GRACE"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newAuditRun(stressPlain).acquired()
			if !c.users {
				b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
			}
			b.mark(auditS)
			b.batch()
			b.call(stCall{op: "r3", obj: auditS, fin: auditFin, testedUID: "u1", tested: c.epoch, err: "connection reset", unknown: true}, func(*stCall) {
				b.remove(auditS)
			})
			b.want(t, c.want...)
		})
	}
}

// One key of the queue is one worker's at a time (§10.2).
func TestAuditGapTwoWorkers(t *testing.T) {
	b := newAuditRun(stressPlain)
	b.listBy(2, "9")
	b.listBy(2, "10")
	b.want(t)
	b.calls[1].start = b.calls[0].start
	b.want(t, "RELEASED-TWICE")
	b.calls[1].proc = 1
	b.want(t)
}
