package credentialuse

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// TestConditionsWriteNothing is C-park's static check over the model's code
// (docs/credential-use-model-reduction.md §5): no Wait condition in this
// package or in apimodel, tests included, could write anything.
func TestConditionsWriteNothing(t *testing.T) {
	fset := token.NewFileSet()
	var files []*ast.File
	for _, glob := range []string{"*.go", "internal/apimodel/*.go"} {
		names, err := filepath.Glob(glob)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			f, err := parser.ParseFile(fset, n, nil, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, f)
		}
	}
	if len(files) < 10 {
		t.Fatalf("parsed only %d files: the check would see nothing", len(files))
	}
	for _, p := range apimodel.CheckConditions(fset, files) {
		t.Error(p)
	}
}

// The harness's runtime half of C-park: a Wait condition that writes shared
// state through code panics, at the model, a cache, the protocol's memory,
// a counter or a monitor (checkNotInCond).
func TestConditionWritesPanic(t *testing.T) {
	for _, c := range []struct {
		name, want string
		write      func(w *world, p **process)
	}{
		{"a counter", "wrote a scenario counter", func(w *world, _ **process) { w.inc(w.started, "x") }},
		{"the model", "changed the API model", func(w *world, _ **process) { _ = w.m.Delete(nsKey(w.ns)) }},
		{"a cache", "changed process A's cache", func(_ *world, p **process) { (*p).cache.Next() }},
		{"the protocol's memory", "touched process A's memory", func(_ *world, p **process) { (*p).ver.hasWritten("uid", "ns") }},
		{"a monitor", "recorded a failure", func(w *world, _ **process) { w.fail("x") }},
	} {
		var got any
		func() {
			defer func() { got = recover() }()
			_, _ = apimodel.Replay(apimodel.Bounds{Jumps: testJumps}, nil, apimodel.Budget{Jumps: testJumps}, -1, 0, func(s *apimodel.Sched) error {
				w := newWorld(s)
				seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
				pa := newProcess("A", w.m, w.s.Now())
				w.informer("informerA", &pa)
				w.deleter("secret", keyOf(w.p))
				w.s.Go("writer", 0, func(a *apimodel.Actor) {
					// condcheck:writes, on purpose: the write under test.
					a.Wait("deleting", time.Time{}, func() bool {
						o, err := w.m.Get(keyOf(w.p))
						if err == nil && o.DeletionTimestamp != nil {
							c.write(w, &pa)
							return true
						}
						return err != nil
					})
				})
				s.Key = w.stateKey
				err := s.Run()
				if errors.Is(err, apimodel.ErrPruned) {
					return nil
				}
				return err
			})
		}()
		if got == nil || !strings.Contains(fmt.Sprint(got), c.want) || !strings.Contains(fmt.Sprint(got), "C-park") {
			t.Errorf("%s: want a C-park panic that %s, got %v", c.name, c.want, got)
		}
	}
}
