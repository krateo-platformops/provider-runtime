package apimodel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func checkSource(t *testing.T, body string) []string {
	t.Helper()
	src := "package p\n\n" + body
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cond.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, src)
	}
	return CheckConditions(fset, []*ast.File{f})
}

// CheckConditions refuses every way a condition could write that the
// independent review of the C-park fix found the runtime check blind to,
// and the ones like them; and lets a guard through.
func TestCheckConditions(t *testing.T) {
	for _, c := range []struct {
		name, src, want string
	}{
		{"a guard", `func f(a *A, w *W) {
	a.Wait("deleting", t0, func() bool {
		o, err := w.m.Get(keyOf(w.p))
		return err != nil || o.DeletionTimestamp != nil
	})
}`, ""},
		{"a guard with its own variables", `func f(a *A, w *W) {
	a.WaitQuiet("done", t0, func() bool {
		n := 0
		for _, k := range []string{"a", "b"} {
			n += w.livePasses[k]
		}
		var ok bool
		ok = n >= 2
		return ok && len(w.started) > 0
	})
}`, ""},
		{"a captured variable, declared as a local or not", `func f(a *A) {
	saw := 0
	a.Locals = func() []any { return []any{&saw} }
	a.Wait("x", t0, func() bool { saw = 1; return true })
}`, "writes saw"},
		{"through a pointer", `func f(a *A, p *int) {
	a.Wait("x", t0, func() bool { *p = 1; return true })
}`, "writes *p"},
		{"a map a local names", `func f(a *A) {
	m := map[string]int{}
	a.Locals = func() []any { return []any{m} }
	a.Wait("x", t0, func() bool { m["k"] = 1; return true })
}`, "writes m[...]"},
		{"a counter, directly", `func f(a *A, w *W) {
	a.Wait("x", t0, func() bool { w.started["h1"]++; return true })
}`, "writes w.started[...]"},
		{"a world field", `func f(a *A, w *W) {
	a.Wait("x", t0, func() bool { w.nsDeleted = now; return true })
}`, "writes w.nsDeleted"},
		{"a cache, through a method", `func f(a *A, p **P) {
	a.Wait("watch", t0, func() bool { _, ok := (*p).cache.Next(); return ok })
}`, "calls (*p).cache.Next"},
		{"a closure held in a local", `func f(a *A, g func()) {
	a.Wait("x", t0, func() bool { h := g; h(); return true })
}`, "calls h"},
		{"a closure inside it", `func f(a *A) {
	n := 0
	a.Wait("x", t0, func() bool { func() { n++ }(); return true })
}`, "writes n"},
		{"a channel", `func f(a *A, c chan int) {
	a.Wait("x", t0, func() bool { c <- 1; return true })
}`, "sends on a channel"},
		{"a method value", `func f(a *A, c *C) {
	a.Wait("watch", t0, c.Pending)
}`, "method value"},
		{"a function declared at top level", `func ready() bool { return true }
func f(a *A) {
	a.Wait("x", t0, ready)
}`, "the function ready"},
		{"a condition returned by another function", `func before(n int) func() bool {
	saw := 0
	return func() bool { saw = n; return true }
}`, "writes saw"},
		{"a function with a named result is not a condition", `func f(a *A) {
	n := 0
	run := func() (gone bool) { n++; return true }
	_ = run
}`, ""},
		{"but passed to Wait it is one", `func f(a *A) {
	n := 0
	a.Wait("x", t0, func() (ok bool) { n++; return true })
}`, "writes n"},
		{"the directive", `func f(a *A) {
	saw := 0
	// condcheck:writes, on purpose.
	a.Wait("x", t0, func() bool { saw = 1; return true })
}`, ""},
	} {
		got := checkSource(t, c.src)
		if c.want == "" {
			if len(got) != 0 {
				t.Errorf("%s: want no problem, got %q", c.name, got)
			}
			continue
		}
		if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), c.want) {
			t.Errorf("%s: want a problem containing %q, got %q", c.name, c.want, got)
		}
	}
}
