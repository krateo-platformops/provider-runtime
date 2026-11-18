package credentialuse

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/tools/go/packages"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// TestConditionsWriteNothing is C-park's static check over the model's code
// (docs/credential-use-model-reduction.md §5): every Wait condition in this
// package and in apimodel, tests included, is a literal at its call site
// that could write nothing. The packages are loaded typed, with their tests,
// so that calls resolve by type, not by name.
func TestConditionsWriteNothing(t *testing.T) {
	if raceEnabled {
		t.Skip("a static check: once, without the race detector")
	}
	const apimodelPath = "github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
	pkgs, err := packages.Load(&packages.Config{
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Tests: true,
	}, ".", "./internal/apimodel")
	if err != nil {
		t.Fatal(err)
	}
	checked := map[string]bool{}
	for _, p := range pkgs {
		// The test variants hold every file of the package, its tests too.
		if !strings.HasSuffix(p.ID, ".test]") {
			continue
		}
		for _, e := range p.Errors {
			t.Fatalf("%s: %v", p.ID, e)
		}
		cfg := apimodel.CondConfig{APIModel: apimodelPath}
		if p.PkgPath == "github.com/krateo-platformops/provider-runtime/pkg/credentialuse" {
			cfg.Pure = []string{"keyOf", "widgetKey", "nsKey", "ID.GroupKind", "waitFor.met"}
		}
		for _, problem := range apimodel.CheckConditions(p.Fset, p.Syntax, p.TypesInfo, cfg) {
			t.Error(problem)
		}
		checked[p.PkgPath] = true
	}
	if !checked["github.com/krateo-platformops/provider-runtime/pkg/credentialuse"] || !checked[apimodelPath] {
		t.Fatalf("checked %v: want this package and apimodel, with their tests", checked)
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
