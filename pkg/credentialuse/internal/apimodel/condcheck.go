package apimodel

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
)

// CheckConditions is C-park's static check (docs/credential-use-model-reduction.md
// §5): a Wait's condition is a guard and writes nothing. It inspects code,
// not runs, so it covers every state a condition could be evaluated in,
// reached by a search or not, and writes that no runtime check can see,
// such as to a variable the condition captures and nobody declared.
//
// Every function literal of type func() bool in files is taken as a
// condition: in the model's harness every such literal is one, and treating
// all of them so errs on the safe side. Inside one, it reports:
//   - an assignment or ++/-- to anything but a variable declared inside the
//     literal: a captured variable, a field, a map or slice element, or
//     through a pointer;
//   - a send, a go or a defer statement;
//   - a call to anything but the read-only functions in readOnlyCalls, and
//     the builtins that don't write (len, cap, min, max).
//
// And at every call of Wait or WaitQuiet, it reports a condition argument
// that is a method value or a function declared at top level, whose body the
// literal rule doesn't see.
//
// A literal preceded, on the line before it, by a comment containing
// "condcheck:writes" is skipped: the tests that show what the check guards
// against use one on purpose.
func CheckConditions(fset *token.FileSet, files []*ast.File) []string {
	var out []string
	for _, f := range files {
		funcs := map[string]bool{}
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil {
				funcs[fd.Name.Name] = true
			}
		}
		allowed := map[int]bool{}
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if strings.Contains(c.Text, "condcheck:writes") {
					allowed[fset.Position(c.End()).Line+1] = true
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncLit:
				if isCondType(n.Type) && !allowed[fset.Position(n.Pos()).Line] {
					c := &condChecker{fset: fset, locals: map[string]bool{}}
					c.block(n.Body)
					out = append(out, c.problems...)
					return false
				}
			case *ast.CallExpr:
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Wait" && sel.Sel.Name != "WaitQuiet") || len(n.Args) != 3 {
					return true
				}
				switch arg := n.Args[2].(type) {
				case *ast.FuncLit:
					if !isCondType(arg.Type) && !allowed[fset.Position(arg.Pos()).Line] {
						c := &condChecker{fset: fset, locals: map[string]bool{}}
						c.block(arg.Body)
						out = append(out, c.problems...)
					}
				case *ast.SelectorExpr:
					out = append(out, fmt.Sprintf("%s: %s's condition is a method value, which the check can't see into", fset.Position(arg.Pos()), sel.Sel.Name))
				case *ast.Ident:
					if funcs[arg.Name] {
						out = append(out, fmt.Sprintf("%s: %s's condition is the function %s, which the check can't see into: pass a literal", fset.Position(arg.Pos()), sel.Sel.Name, arg.Name))
					}
				}
			}
			return true
		})
	}
	sort.Strings(out)
	return out
}

// readOnlyCalls are the functions and methods a condition may call, by name:
// the model's and a cache's reads, and the harness's key constructors.
var readOnlyCalls = map[string]bool{
	"Get": true, "Pending": true,
	"keyOf": true, "widgetKey": true, "nsKey": true,
	"len": true, "cap": true, "min": true, "max": true,
}

func isCondType(t *ast.FuncType) bool {
	if t.Params != nil && len(t.Params.List) > 0 {
		return false
	}
	// An unnamed bool result: a function with a named one, such as the
	// holder's pass (gone bool), is not a condition. A literal passed to
	// Wait is checked whatever its form.
	if t.Results == nil || len(t.Results.List) != 1 || len(t.Results.List[0].Names) > 0 {
		return false
	}
	id, ok := t.Results.List[0].Type.(*ast.Ident)
	return ok && id.Name == "bool"
}

type condChecker struct {
	fset     *token.FileSet
	locals   map[string]bool
	problems []string
}

func (c *condChecker) report(n ast.Node, format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf("%s: a condition %s (C-park: a Wait's condition writes nothing)", c.fset.Position(n.Pos()), fmt.Sprintf(format, args...)))
}

func (c *condChecker) block(b *ast.BlockStmt) {
	if b == nil {
		return
	}
	for _, s := range b.List {
		c.stmt(s)
	}
}

// target reports a write to e unless e is a variable declared in the literal.
func (c *condChecker) target(n ast.Node, e ast.Expr) {
	if id, ok := e.(*ast.Ident); ok && (id.Name == "_" || c.locals[id.Name]) {
		return
	}
	c.report(n, "writes %s", exprString(e))
}

func (c *condChecker) stmt(s ast.Stmt) {
	switch s := s.(type) {
	case *ast.AssignStmt:
		for _, e := range s.Rhs {
			c.expr(e)
		}
		for _, e := range s.Lhs {
			if s.Tok == token.DEFINE {
				if id, ok := e.(*ast.Ident); ok {
					c.locals[id.Name] = true
					continue
				}
			}
			c.target(s, e)
			c.expr(e)
		}
	case *ast.IncDecStmt:
		c.target(s, s.X)
	case *ast.DeclStmt:
		if gd, ok := s.Decl.(*ast.GenDecl); ok {
			for _, sp := range gd.Specs {
				if vs, ok := sp.(*ast.ValueSpec); ok {
					for _, e := range vs.Values {
						c.expr(e)
					}
					for _, id := range vs.Names {
						c.locals[id.Name] = true
					}
				}
			}
		}
	case *ast.ExprStmt:
		c.expr(s.X)
	case *ast.ReturnStmt:
		for _, e := range s.Results {
			c.expr(e)
		}
	case *ast.IfStmt:
		if s.Init != nil {
			c.stmt(s.Init)
		}
		c.expr(s.Cond)
		c.block(s.Body)
		if s.Else != nil {
			c.stmt(s.Else)
		}
	case *ast.BlockStmt:
		c.block(s)
	case *ast.ForStmt:
		if s.Init != nil {
			c.stmt(s.Init)
		}
		if s.Cond != nil {
			c.expr(s.Cond)
		}
		if s.Post != nil {
			c.stmt(s.Post)
		}
		c.block(s.Body)
	case *ast.RangeStmt:
		c.expr(s.X)
		for _, e := range []ast.Expr{s.Key, s.Value} {
			if e == nil {
				continue
			}
			if id, ok := e.(*ast.Ident); ok && s.Tok == token.DEFINE {
				c.locals[id.Name] = true
				continue
			}
			c.target(s, e)
		}
		c.block(s.Body)
	case *ast.SwitchStmt:
		if s.Init != nil {
			c.stmt(s.Init)
		}
		if s.Tag != nil {
			c.expr(s.Tag)
		}
		for _, cc := range s.Body.List {
			for _, e := range cc.(*ast.CaseClause).List {
				c.expr(e)
			}
			for _, st := range cc.(*ast.CaseClause).Body {
				c.stmt(st)
			}
		}
	case *ast.BranchStmt, *ast.EmptyStmt:
	case *ast.SendStmt:
		c.report(s, "sends on a channel")
	case *ast.GoStmt:
		c.report(s, "starts a goroutine")
	case *ast.DeferStmt:
		c.report(s, "defers a call")
	default:
		c.report(s, "has a statement the check doesn't know (%T)", s)
	}
}

func (c *condChecker) expr(e ast.Expr) {
	ast.Inspect(e, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			// A closure inside a condition is part of it: its writes to the
			// condition's own variables are fine, any other is not.
			inner := &condChecker{fset: c.fset, locals: map[string]bool{}}
			for k := range c.locals {
				inner.locals[k] = true
			}
			if n.Type.Params != nil {
				for _, p := range n.Type.Params.List {
					for _, id := range p.Names {
						inner.locals[id.Name] = true
					}
				}
			}
			inner.block(n.Body)
			c.problems = append(c.problems, inner.problems...)
			return false
		case *ast.CallExpr:
			name := ""
			switch f := n.Fun.(type) {
			case *ast.Ident:
				name = f.Name
			case *ast.SelectorExpr:
				name = f.Sel.Name
			case *ast.FuncLit:
				return true
			}
			// Not even a closure held in a local: what it holds is unknown.
			if !readOnlyCalls[name] {
				c.report(n, "calls %s, which isn't known to be read-only", exprString(n.Fun))
			}
		}
		return true
	})
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
	}
	return fmt.Sprintf("%T", e)
}
