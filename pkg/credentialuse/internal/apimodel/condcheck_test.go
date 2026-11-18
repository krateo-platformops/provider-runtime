package apimodel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

// prelude stands in for the API model and the harness, in package p: an
// Actor with Wait and WaitQuiet, a Model with Get, a Cache with Pending and
// Next, a Server whose Get is not the model's, and a world.
const prelude = `package p

type Time struct{}

type Actor struct{ Locals func() []any }

func (a *Actor) Wait(label string, notBefore Time, cond func() bool)      {}
func (a *Actor) WaitQuiet(label string, notBefore Time, cond func() bool) {}

type Obj struct {
	DeletionTimestamp *Time
	UID               string
}

type Model struct{}

func (m *Model) Get(k string) (*Obj, error) { return nil, nil }

type Cache struct{}

func (c *Cache) Pending() bool     { return false }
func (c *Cache) Next() (int, bool) { return 0, false }

type Server struct{ hook func() }

func (s Server) Get(k string) (*Obj, error) { s.hook(); return nil, nil }

type W struct {
	m          *Model
	p          string
	started    map[string]int
	livePasses map[string]int
	nsDeleted  Time
	cache      *Cache
}

var t0 Time

func keyOf(s string) string { return s }

func pure(w *W) bool { return w.started["a"] >= 1 }

func impure(w *W) bool { w.started["a"]++; return true }
`

func checkSource(t *testing.T, body string, pure ...string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cond.go", prelude+"\n"+body, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, body)
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{Error: func(error) {}} // a snippet may leave a variable unused
	if _, err := conf.Check("p", fset, []*ast.File{f}, info); err != nil && !strings.Contains(err.Error(), "declared and not used") {
		t.Fatalf("type-check: %v\n%s", err, body)
	}
	return CheckConditions(fset, []*ast.File{f}, info, CondConfig{APIModel: "p", Pure: append([]string{"keyOf", "pure"}, pure...)})
}

// CheckConditions refuses every way a condition could write that the two
// independent reviews of the C-park fix found, and the ones like them; and
// lets a guard through.
func TestCheckConditions(t *testing.T) {
	for _, c := range []struct {
		name, src, want string
	}{
		{"a guard", `func f(a *Actor, w *W) {
	a.Wait("deleting", t0, func() bool {
		o, err := w.m.Get(keyOf(w.p))
		return err != nil || o.DeletionTimestamp != nil
	})
}`, ""},
		{"a guard with its own variables, and a pure call", `func f(a *Actor, w *W) {
	a.WaitQuiet("done", t0, func() bool {
		n := 0
		for _, k := range []string{"a", "b"} {
			n += w.livePasses[k]
		}
		var ok bool
		ok = n >= 2
		return ok && len(w.started) > 0 && pure(w) && w.cache.Pending()
	})
}`, ""},
		{"nil", `func f(a *Actor) { a.Wait("x", t0, nil) }`, ""},
		{"a captured variable, declared as a local or not", `func f(a *Actor) {
	saw := 0
	a.Locals = func() []any { return []any{&saw} }
	a.Wait("x", t0, func() bool { saw = 1; return true })
}`, "writes saw"},
		{"a captured variable, shadowed in a block of the condition", `func f(a *Actor) {
	saw := 0
	a.Wait("x", t0, func() bool {
		{
			saw := 1
			_ = saw
		}
		saw = 2
		return true
	})
	_ = saw
}`, "writes saw"},
		{"through a pointer", `func f(a *Actor, p *int) {
	a.Wait("x", t0, func() bool { *p = 1; return true })
}`, "writes *p"},
		{"a map a local names", `func f(a *Actor) {
	m := map[string]int{}
	a.Wait("x", t0, func() bool { m["k"] = 1; return true })
}`, "writes m[...]"},
		{"a counter, directly", `func f(a *Actor, w *W) {
	a.Wait("x", t0, func() bool { w.started["h1"]++; return true })
}`, "writes w.started[...]"},
		{"a world field", `func f(a *Actor, w *W) {
	a.Wait("x", t0, func() bool { w.nsDeleted = t0; return true })
}`, "writes w.nsDeleted"},
		{"a cache, through a method", `func f(a *Actor, w *W) {
	a.Wait("watch", t0, func() bool { _, ok := w.cache.Next(); return ok })
}`, "calls w.cache.Next"},
		{"a Get that isn't the model's", `func f(a *Actor, s Server) {
	a.Wait("x", t0, func() bool { _, err := s.Get("k"); return err == nil })
}`, "calls s.Get"},
		{"a function named pure elsewhere is not it", `type other struct{}
func (other) pure() bool { return true }
func f(a *Actor) {
	a.Wait("x", t0, func() bool { return other{}.pure() })
}`, "pure, which isn't known to be read-only"},
		{"a pure function that writes", `func f(a *Actor, w *W) {
	a.Wait("x", t0, func() bool { return impure(w) })
}`, "calls impure"},
		{"a closure held in a local", `func f(a *Actor, g func()) {
	a.Wait("x", t0, func() bool { h := g; h(); return true })
}`, "calls h"},
		{"a closure inside it", `func f(a *Actor) {
	n := 0
	a.Wait("x", t0, func() bool { func() { n++ }(); return true })
	_ = n
}`, "writes n"},
		{"a receive", `func f(a *Actor, c chan int) {
	a.Wait("x", t0, func() bool { return <-c > 0 })
}`, "receives from a channel"},
		{"a range over a channel", `func f(a *Actor, c chan int) {
	a.Wait("x", t0, func() bool { for range c { return true }; return false })
}`, "receives from a channel"},
		{"a send", `func f(a *Actor, c chan int) {
	a.Wait("x", t0, func() bool { c <- 1; return true })
}`, "sends on a channel"},
		{"a literal held in a local, with a named result", `func f(a *Actor) {
	saw := 0
	c := func() (ok bool) { saw = 2; return true }
	a.Wait("x", t0, c)
	_ = saw
}`, "not a function literal written at the call site"},
		{"a method value", `func f(a *Actor, w *W) {
	a.Wait("watch", t0, w.cache.Pending)
}`, "not a function literal written at the call site"},
		{"a call's result", `func mk() func() bool { return func() bool { return true } }
func f(a *Actor) {
	a.Wait("x", t0, mk())
}`, "not a function literal written at the call site"},
		{"a wrapper's parameter", `func wrap(a *Actor, cond func() bool) {
	a.WaitQuiet("x", t0, cond)
}`, "not a function literal written at the call site"},
		{"a top-level function", `func ready() bool { return true }
func f(a *Actor) {
	a.Wait("x", t0, ready)
}`, "not a function literal written at the call site"},
		{"a method value", `func f(a *Actor) {
	saw := 0
	wt := a.Wait
	wt("x", t0, func() bool { saw = 2; return true })
	_ = saw
}`, "method value or a method expression"},
		{"a method expression", `func f(a *Actor) {
	saw := 0
	(*Actor).Wait(a, "x", t0, func() bool { saw = 2; return true })
	_ = saw
}`, "method value or a method expression"},
		{"a struct field holding WaitQuiet", `type holder struct{ wq func(string, Time, func() bool) }
func f(a *Actor) {
	saw := 0
	h := holder{wq: a.WaitQuiet}
	h.wq("x", t0, func() bool { saw = 2; return true })
	_ = saw
}`, "method value or a method expression"},
		{"an interface with Wait", `type waiter interface{ Wait(string, Time, func() bool) }
func f(a *Actor) {
	saw := 0
	var w waiter = a
	w.Wait("x", t0, func() bool { saw = 2; return true })
	_ = saw
}`, "whose Wait method could call Actor's"},
		{"a generic wrapper", `func g[T interface{ WaitQuiet(string, Time, func() bool) }](t T, saw *int) {
	t.WaitQuiet("x", t0, func() bool { *saw = 2; return true })
}
func f(a *Actor) {
	saw := 0
	g(a, &saw)
}`, "whose WaitQuiet method could call Actor's"},
		{"the directive", `func f(a *Actor) {
	saw := 0
	// condcheck:writes, on purpose.
	a.Wait("x", t0, func() bool { saw = 1; return true })
	_ = saw
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

// A function on the Pure list is checked by the same rules: listing one
// that writes reports its write.
func TestCheckConditionsChecksPureFunctions(t *testing.T) {
	got := checkSource(t, `func f(a *Actor, w *W) {
	a.Wait("x", t0, func() bool { return impure(w) })
}`, "impure")
	if !strings.Contains(strings.Join(got, "\n"), "writes w.started[...]") {
		t.Errorf("want impure's write reported, got %q", got)
	}
	if got := checkSource(t, `func f() {}`, "missing"); !strings.Contains(strings.Join(got, "\n"), "isn't declared") {
		t.Errorf("want a missing Pure function reported, got %q", got)
	}
}
