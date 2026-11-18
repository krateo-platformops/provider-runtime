package apimodel

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
)

// CondConfig says what CheckConditions trusts.
type CondConfig struct {
	// APIModel is the import path of the package that declares Actor, Model
	// and Cache: Actor's Wait and WaitQuiet are the calls checked, and
	// Model.Get and Cache.Pending the trusted reads.
	APIModel string
	// Pure names functions and methods of the checked package ("keyOf",
	// "waitFor.met") that conditions may call. Each one's body is checked by
	// the same rules as a condition's, so it adds nothing to what is
	// trusted.
	Pure []string
}

// CheckConditions is C-park's static check (docs/credential-use-model-reduction.md
// §5): a Wait's condition is a guard and writes nothing. It inspects code,
// typed, not runs, so it covers every state a condition could be evaluated
// in, whether a search reaches it or not, and writes no runtime check can
// see, such as to a variable nobody declared.
//
// The rule is structural. Actor.Wait and Actor.WaitQuiet may be used only
// in direct calls, a.Wait(label, notBefore, cond), whose condition is nil or
// a function literal written at the call site; anything else is refused:
//   - a condition that is a variable, a parameter, a method value or a
//     call's result;
//   - any other use of either method: a method value (wt := a.Wait, a
//     struct field holding a.WaitQuiet) or a method expression
//     ((*Actor).Wait(a, ...));
//   - any expression, type expressions included, whose type is an
//     interface or a type parameter with a Wait or a WaitQuiet method,
//     through which *Actor could be called unseen (a generic wrapper, a
//     conversion of *Actor to such an interface).
//
// In the literal, and in every function of cfg.Pure, it reports:
//   - an assignment or ++/-- to anything but a variable declared inside it
//     (scoped as Go scopes it, block by block): a captured variable, a
//     field, a map or slice element, or through a pointer;
//   - a channel receive or send, a select, a go or a defer statement;
//   - a call that doesn't resolve, by type, to a builtin that doesn't
//     write (len, cap, min, max), a conversion, a function literal inside
//     it, Model.Get, Cache.Pending, or a function of cfg.Pure.
//
// A literal whose call is preceded, on the line before, by a comment
// containing "condcheck:writes" is skipped: the tests that show what the
// check guards against use one on purpose. So is the body of the API's own
// Actor.WaitQuiet, which passes its condition on to Wait.
func CheckConditions(fset *token.FileSet, files []*ast.File, info *types.Info, cfg CondConfig) []string {
	c := &condChecker{fset: fset, info: info, cfg: cfg, pure: map[string]bool{}, decls: map[string]*ast.FuncDecl{}}
	for _, p := range cfg.Pure {
		c.pure[p] = true
	}
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok {
				c.decls[declName(fd)] = fd
			}
		}
	}
	for _, p := range cfg.Pure {
		fd, ok := c.decls[p]
		if !ok {
			c.problems = append(c.problems, fmt.Sprintf("CondConfig.Pure names %s, which isn't declared in the files checked", p))
			continue
		}
		c.body(fd.Body, fd.Pos(), fd.End())
	}
	// direct holds the Wait and WaitQuiet identifiers that are a direct
	// call's method: a.Wait(label, notBefore, cond). Every other use of
	// either method, a method value or a method expression, is refused
	// below, since its calls are out of the check's sight.
	direct := map[*ast.Ident]bool{}
	for _, f := range files {
		allowed := map[int]bool{}
		for _, cg := range f.Comments {
			for _, cm := range cg.List {
				if strings.Contains(cm.Text, "condcheck:writes") {
					allowed[fset.Position(cm.End()).Line+1] = true
				}
			}
		}
		var inAPIWaitQuiet func(token.Pos) bool
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && declName(fd) == "Actor.WaitQuiet" && c.info.Defs[fd.Name] != nil &&
				c.info.Defs[fd.Name].Pkg() != nil && c.info.Defs[fd.Name].Pkg().Path() == cfg.APIModel {
				from, to := fd.Pos(), fd.End()
				inAPIWaitQuiet = func(p token.Pos) bool { return p >= from && p < to }
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
			if !ok || !c.isWait(call) {
				return true
			}
			if selection, ok := info.Selections[sel]; !ok || selection.Kind() != types.MethodVal || len(call.Args) != 3 {
				return true // a method expression: refused below
			}
			direct[sel.Sel] = true
			// The API's own WaitQuiet hands its condition on to Wait: that
			// is the one call whose condition is a parameter.
			if inAPIWaitQuiet != nil && inAPIWaitQuiet(call.Pos()) {
				return true
			}
			arg := call.Args[2]
			if tv, ok := info.Types[arg]; ok && tv.IsNil() {
				return true
			}
			lit, ok := arg.(*ast.FuncLit)
			if !ok {
				c.report(arg, "is not a function literal written at the call site, so the check can't see it: write the condition there")
				return true
			}
			if !allowed[fset.Position(call.Pos()).Line] {
				c.body(lit.Body, lit.Pos(), lit.End())
			}
			return true
		})
	}
	for id, obj := range info.Uses {
		f, _ := obj.(*types.Func)
		if (c.method(f, "Actor", "Wait") || c.method(f, "Actor", "WaitQuiet")) && !direct[id] {
			c.report(id, "is passed through %s used other than in a direct call (a method value or a method expression), whose calls the check can't see", id.Name)
		}
	}
	// An interface, or a type parameter's constraint, with a Wait or a
	// WaitQuiet method would let *Actor be called through it, out of the
	// check's sight: every expression, type expression included, of such a
	// type is refused, whichever package declares the interface.
	seen := map[token.Pos]bool{}
	for e, tv := range info.Types {
		if tv.Type == nil || seen[e.Pos()] {
			continue
		}
		if m := waitMethod(tv.Type); m != "" {
			seen[e.Pos()] = true
			c.report(e, "is of %s, whose %s method could call Actor's out of the check's sight", tv.Type, m)
		}
	}
	sort.Strings(c.problems)
	return c.problems
}

// waitMethod returns "Wait" or "WaitQuiet" if t is an interface or a type
// parameter whose method set has a method by that name, or "".
func waitMethod(t types.Type) string {
	var iface *types.Interface
	switch t := t.(type) {
	case *types.TypeParam:
		iface, _ = t.Constraint().Underlying().(*types.Interface)
	default:
		iface, _ = t.Underlying().(*types.Interface)
	}
	if iface == nil {
		return ""
	}
	for i := range iface.NumMethods() {
		if n := iface.Method(i).Name(); n == "Wait" || n == "WaitQuiet" {
			return n
		}
	}
	return ""
}

func declName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + fd.Name.Name
	}
	return fd.Name.Name
}

type condChecker struct {
	fset     *token.FileSet
	info     *types.Info
	cfg      CondConfig
	pure     map[string]bool
	decls    map[string]*ast.FuncDecl
	problems []string
}

func (c *condChecker) report(n ast.Node, format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf("%s: a condition %s (C-park: a Wait's condition writes nothing)", c.fset.Position(n.Pos()), fmt.Sprintf(format, args...)))
}

// method reports whether f is method name of type typ in the API model.
func (c *condChecker) method(f *types.Func, typ, name string) bool {
	if f == nil || f.Name() != name || f.Pkg() == nil || f.Pkg().Path() != c.cfg.APIModel {
		return false
	}
	sig, ok := f.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Name() == typ
}

func (c *condChecker) callee(call *ast.CallExpr) types.Object {
	switch f := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		return c.info.Uses[f]
	case *ast.SelectorExpr:
		return c.info.Uses[f.Sel]
	}
	return nil
}

func (c *condChecker) isWait(call *ast.CallExpr) bool {
	f, _ := c.callee(call).(*types.Func)
	return c.method(f, "Actor", "Wait") || c.method(f, "Actor", "WaitQuiet")
}

// body checks a condition's or a pure function's body, whose own variables
// are those declared in [from, to).
func (c *condChecker) body(b *ast.BlockStmt, from, to token.Pos) {
	s := scope{c, from, to}
	if b != nil {
		for _, st := range b.List {
			s.stmt(st)
		}
	}
}

type scope struct {
	c        *condChecker
	from, to token.Pos
}

// local reports whether e is a variable declared inside the scope.
func (s scope) local(e ast.Expr) bool {
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok {
		return false
	}
	if id.Name == "_" {
		return true
	}
	obj := s.c.info.Defs[id]
	if obj == nil {
		obj = s.c.info.Uses[id]
	}
	v, ok := obj.(*types.Var)
	return ok && !v.IsField() && v.Pos() >= s.from && v.Pos() < s.to
}

func (s scope) target(n ast.Node, e ast.Expr) {
	if !s.local(e) {
		s.c.report(n, "writes %s", exprString(e))
	}
}

func (s scope) stmts(list []ast.Stmt) {
	for _, st := range list {
		s.stmt(st)
	}
}

func (s scope) stmt(st ast.Stmt) {
	switch st := st.(type) {
	case *ast.AssignStmt:
		for _, e := range st.Rhs {
			s.expr(e)
		}
		for _, e := range st.Lhs {
			s.target(st, e)
			s.expr(e)
		}
	case *ast.IncDecStmt:
		s.target(st, st.X)
		s.expr(st.X)
	case *ast.DeclStmt:
		if gd, ok := st.Decl.(*ast.GenDecl); ok {
			for _, sp := range gd.Specs {
				if vs, ok := sp.(*ast.ValueSpec); ok {
					for _, e := range vs.Values {
						s.expr(e)
					}
				}
			}
		}
	case *ast.ExprStmt:
		s.expr(st.X)
	case *ast.ReturnStmt:
		for _, e := range st.Results {
			s.expr(e)
		}
	case *ast.IfStmt:
		if st.Init != nil {
			s.stmt(st.Init)
		}
		s.expr(st.Cond)
		s.stmts(st.Body.List)
		if st.Else != nil {
			s.stmt(st.Else)
		}
	case *ast.BlockStmt:
		s.stmts(st.List)
	case *ast.ForStmt:
		if st.Init != nil {
			s.stmt(st.Init)
		}
		if st.Cond != nil {
			s.expr(st.Cond)
		}
		if st.Post != nil {
			s.stmt(st.Post)
		}
		s.stmts(st.Body.List)
	case *ast.RangeStmt:
		s.expr(st.X)
		if tv, ok := s.c.info.Types[st.X]; ok {
			if _, isChan := tv.Type.Underlying().(*types.Chan); isChan {
				s.c.report(st, "receives from a channel")
			}
		}
		for _, e := range []ast.Expr{st.Key, st.Value} {
			if e != nil {
				s.target(st, e)
			}
		}
		s.stmts(st.Body.List)
	case *ast.SwitchStmt:
		if st.Init != nil {
			s.stmt(st.Init)
		}
		if st.Tag != nil {
			s.expr(st.Tag)
		}
		for _, cc := range st.Body.List {
			if clause, ok := cc.(*ast.CaseClause); ok {
				for _, e := range clause.List {
					s.expr(e)
				}
				s.stmts(clause.Body)
			}
		}
	case *ast.LabeledStmt:
		s.stmt(st.Stmt)
	case *ast.BranchStmt, *ast.EmptyStmt:
	case *ast.SendStmt:
		s.c.report(st, "sends on a channel")
	case *ast.SelectStmt:
		s.c.report(st, "selects on channels")
	case *ast.GoStmt:
		s.c.report(st, "starts a goroutine")
	case *ast.DeferStmt:
		s.c.report(st, "defers a call")
	default:
		s.c.report(st, "has a statement the check doesn't know (%T)", st)
	}
}

func (s scope) expr(e ast.Expr) {
	ast.Inspect(e, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			// A closure inside a condition is part of it, in its scope.
			s.stmts(n.Body.List)
			return false
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				s.c.report(n, "receives from a channel")
			}
		case *ast.CallExpr:
			s.call(n)
		}
		return true
	})
}

var readOnlyBuiltins = map[string]bool{"len": true, "cap": true, "min": true, "max": true, "real": true, "imag": true, "complex": true}

func (s scope) call(call *ast.CallExpr) {
	if tv, ok := s.c.info.Types[call.Fun]; ok && tv.IsType() {
		return // a conversion
	}
	if _, ok := ast.Unparen(call.Fun).(*ast.FuncLit); ok {
		return // its body is checked as part of the condition
	}
	switch f := s.c.callee(call).(type) {
	case *types.Builtin:
		if readOnlyBuiltins[f.Name()] {
			return
		}
	case *types.Func:
		if s.c.method(f, "Model", "Get") || s.c.method(f, "Cache", "Pending") {
			return
		}
		if s.c.pure[pureName(f)] && f.Pkg() != nil && s.c.declIn(f) {
			return
		}
	}
	s.c.report(call, "calls %s, which isn't known to be read-only", exprString(call.Fun))
}

// declIn reports whether f is the function the checked files declare under
// its name, so that a same-named function elsewhere isn't trusted.
func (c *condChecker) declIn(f *types.Func) bool {
	fd, ok := c.decls[pureName(f)]
	return ok && c.info.Defs[fd.Name] == f
}

func pureName(f *types.Func) string {
	sig, ok := f.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return f.Name()
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n.Obj().Name() + "." + f.Name()
	}
	return f.Name()
}

func exprString(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *ast.IndexExpr:
		return exprString(e.X) + "[...]"
	case *ast.StarExpr:
		return "*" + exprString(e.X)
	case *ast.ParenExpr:
		return "(" + exprString(e.X) + ")"
	case *ast.CallExpr:
		return exprString(e.Fun) + "(...)"
	case *ast.FuncLit:
		return "a function literal"
	}
	return fmt.Sprintf("%T", e)
}
