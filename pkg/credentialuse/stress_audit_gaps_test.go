package credentialuse_test

// What a cold review of the stress test's audits found them blind to, each as
// a made-up history.

import (
	"slices"
	"testing"
	"time"

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
		b.a2(p, auditC, "e1")
		b.end(p, auditC)
		return b
	}
	t.Run("a live pass skips on a cached yes", func(t *testing.T) {
		b := held()
		b.end(b.pass(), auditC)
		b.want(t)
	})
	t.Run("a live pass skips, long after the consent went", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		b.wait(6 * time.Second)
		b.end(b.pass(), auditC)
		b.want(t, "HELD-WITHOUT-CONSENT")
	})
	t.Run("a live pass skips, just after the consent went", func(t *testing.T) {
		b := held()
		b.put(auditC, "uc", func(v *stVersion) { v.consent = "" })
		b.wait(time.Second)
		b.end(b.pass(), auditC)
		b.want(t)
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

// A skip rests on the incarnation that was written, and on an object that
// carries the finalizer.
func TestAuditGapSkip(t *testing.T) {
	// replaced is a history whose Secret was held, deleted and released after
	// its grace, and recreated.
	replaced := func() *auditRun {
		b := newAuditRun(stressPlain).acquired()
		b.mark(auditS)
		b.wait(testGrace)
		b.batch()
		b.r3()
		b.create(auditS, "u2")
		return b
	}
	t.Run("long after the incarnation it wrote went", func(t *testing.T) {
		b := replaced()
		b.wait(6 * time.Second)
		b.end(b.pass(), auditS)
		b.want(t, "HELD-WITHOUT-A2")
	})
	t.Run("just after", func(t *testing.T) {
		b := replaced()
		b.wait(time.Second)
		b.end(b.pass(), auditS)
		b.want(t) // §6.5's window: the cache still has it
	})
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

// H1's recreate has a class of its own: two releases of one incarnation
// answered, and the next one went while the second was in flight.
func TestAuditGapRecreate(t *testing.T) {
	b := newAuditRun(stressPlain).acquired()
	b.put(auditH, "uh", func(v *stVersion) { v.spec, v.rec = "", nil })
	b.mark(auditS)
	read := b.last[auditS]
	for _, g := range []string{"9", "10"} {
		for _, o := range []stObj{auditS, nsObj("ns")} {
			b.call(stCall{proc: 2, g: g, op: "get", obj: o}, func(c *stCall) { c.from(b.last[o]) })
		}
		b.listBy(2, g)
	}
	removed := func(c *stCall) {
		c.from(read)
		c.fins, c.epochs, c.testedUID, c.tested = nil, nil, "u1", "e1"
	}
	b.call(stCall{proc: 2, g: "9", op: "r3", obj: auditS, fin: auditFin}, func(c *stCall) {
		removed(c)
		b.remove(auditS)
	})
	b.call(stCall{proc: 2, g: "10", op: "r3", obj: auditS, fin: auditFin}, func(c *stCall) {
		removed(c)
		b.create(auditS, "u2")
		b.remove(auditS)
	})
	b.want(t, "H1-RECREATE")
}
