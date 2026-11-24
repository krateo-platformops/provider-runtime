package credentialuse

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The index's superset (§10.6.3) in the model: the reviewer's interleaving,
// and the startup recheck's prune against an A2, on every schedule within
// the bounds, through the index's own code, the acquirer's protocol and the
// release batch's. Every API call is a step of the schedule, so every order
// of H2's index read after its A2 and the pruner's write, and of the
// pruner's read of P after it, is searched. At the end of every schedule, an
// object that carries a finalizer of the kind has its line.
//
// The pruner's view of the records is the most adversarial one, a cache
// that never shows H2's: it prunes whenever the live object is bare.

// modelIndexKey is the index ConfigMap in the model.
var modelIndexKey = apimodel.Key{GK: configMapGroupKind, Namespace: "idx", Name: IndexName(widgetGK)}

// modelIndexAnnotation holds the index's lines in the model, which keeps
// metadata only.
const modelIndexAnnotation = "entries"

// modelIndexStore is the index over the model: an object whose lines live
// in an annotation, written with a merge patch guarded by its
// resourceVersion, as the ConfigMap's Update is. Each call is a step of its
// actor's schedule, and is logged. Its marker is "true": no copy of the index
// is restored here.
type modelIndexStore struct {
	m   *apimodel.Model
	a   *apimodel.Actor
	who string
	log *[]string
}

func (s modelIndexStore) version(o *metav1.PartialObjectMetadata) indexVersion {
	return indexVersion{lines: decodeIndex(o.Annotations[modelIndexAnnotation]), rv: o.ResourceVersion, rebuilt: o.Annotations[AnnotationRebuilt] == "true"}
}

func (s modelIndexStore) get(context.Context) (indexVersion, error) {
	var v indexVersion
	err := s.a.Call(s.who+" index get", func() error {
		o, err := s.m.Get(modelIndexKey)
		if err != nil {
			return err
		}
		v = s.version(o)
		*s.log = append(*s.log, fmt.Sprintf("%s get %v", s.who, v.lines))
		return nil
	})
	return v, err
}

func (s modelIndexStore) update(_ context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	var v indexVersion
	ann := map[string]any{modelIndexAnnotation: encodeIndex(lines)}
	if rebuilt {
		ann[AnnotationRebuilt] = "true"
	}
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": cur.rv,
		"annotations":     ann,
	}})
	err := s.a.Call(s.who+" index update", func() error {
		o, err := s.m.Patch(modelIndexKey, types.MergePatchType, data)
		if err != nil {
			return err
		}
		v = s.version(o)
		*s.log = append(*s.log, fmt.Sprintf("%s update %v", s.who, v.lines))
		return nil
	})
	return v, err
}

func (s modelIndexStore) create(_ context.Context, lines []string) (indexVersion, error) {
	var v indexVersion
	ann := map[string]string{modelIndexAnnotation: encodeIndex(lines)}
	err := s.a.Call(s.who+" index create", func() error {
		if err := s.m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: ann}); err != nil {
			return err
		}
		o, err := s.m.Get(modelIndexKey)
		if err != nil {
			return err
		}
		v = s.version(o)
		*s.log = append(*s.log, fmt.Sprintf("%s create %v", s.who, v.lines))
		return nil
	})
	return v, err
}

// loggingServer is a modelServer that logs the A2s and R3s on P.
type loggingServer struct {
	modelServer
	who string
	log *[]string
}

func (s loggingServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	out, err := s.modelServer.Patch(ctx, gk, namespace, name, pt, data)
	if err == nil && gk == secretGroupKind {
		*s.log = append(*s.log, fmt.Sprintf("%s patch %s %s", s.who, pt, name))
	}
	return out, err
}

func (s loggingServer) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	out, err := s.modelServer.Get(ctx, gk, namespace, name)
	if err == nil && gk == secretGroupKind {
		*s.log = append(*s.log, fmt.Sprintf("%s get secret %s", s.who, name))
	}
	return out, err
}

// modelIndex is an index of the Widget kind over the model, for actor a.
func modelIndex(m *apimodel.Model, a *apimodel.Actor, who string, log *[]string) *index {
	prefix, _ := FinalizerPrefix(widgetGK)
	st := modelIndexStore{m: m, a: a, who: who, log: log}
	srv := loggingServer{modelServer{m, a}, who, log}
	ix := newIndex(widgetGK, prefix, "idx", DefaultIndexMaxLines, map[schema.GroupKind]bool{secretGroupKind: true}, st, srv)
	// The acquirer's view of the index is a live read, a step of its own:
	// it may come before a prune, and so show the line present.
	ix.cached = func(ctx context.Context, line string) bool {
		v, err := st.get(ctx)
		return err == nil && slices.Contains(v.lines, line)
	}
	return ix
}

// indexSuperset checks the end state: P carries a finalizer of the kind
// only with its line in the index.
func indexSuperset(m *apimodel.Model, p ID) error {
	o, err := m.Get(secretKey(p.Namespace, p.Name))
	if err != nil {
		return nil
	}
	prefix, _ := FinalizerPrefix(widgetGK)
	if countPrefix(o.Finalizers, prefix) == 0 {
		return nil
	}
	ix, err := m.Get(modelIndexKey)
	if err != nil {
		return fmt.Errorf("P carries %v, and the index is gone", o.Finalizers)
	}
	if !slices.Contains(decodeIndex(ix.Annotations[modelIndexAnnotation]), p.String()) {
		return fmt.Errorf("P carries %v without its line: the index has %q", o.Finalizers, ix.Annotations[modelIndexAnnotation])
	}
	return nil
}

// indexOrders classifies a schedule's log: whether the pruner removed P's
// line after H2's A2 landed, and if so whether H2's index read after its A2
// came after the prune's write (tP < tI) or before it (tP > tI).
func indexOrders(log []string, p ID) (raced, pruneFirst bool) {
	a2, pruned, read := -1, -1, -1
	for i, e := range log {
		switch {
		case a2 < 0 && strings.HasPrefix(e, "acq patch "+string(types.StrategicMergePatchType)):
			a2 = i
		case strings.HasPrefix(e, "rel update") && !strings.Contains(e, p.String()) && a2 >= 0 && pruned < 0:
			pruned = i
		case strings.HasPrefix(e, "acq get") && a2 >= 0 && read < 0:
			read = i
		}
	}
	if a2 < 0 || pruned < 0 || read < 0 {
		return false, false
	}
	return true, pruned < read
}

// TestModelIndexSuperset runs the reviewer's interleaving of §10.6.3: H1 is
// gone, so the batch's R2 sees no users of P; H2, a new holder, announces,
// and may see P's line present; R3 removes the finalizer; H2's A2 re-adds
// it; the batch prunes the line. Both orders of H2's index read and the
// prune are reached, and in each the line is back.
func TestModelIndexSuperset(t *testing.T) {
	p := SecretRef("ns", "s").ID()
	run := func(s *apimodel.Sched, log *[]string) *apimodel.Model {
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, true)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{modelIndexAnnotation: encodeIndex([]string{p.String()}), AnnotationRebuilt: "true"}}))
		must(m.Create(widgetKey("ns", "h2"), metav1.ObjectMeta{}))
		s.Go("releaser", 0, func(a *apimodel.Actor) {
			srv := loggingServer{modelServer{m, a}, "rel", log}
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			res, err := pr.release(context.Background(), "ns", []ID{p})
			if err != nil {
				return
			}
			ix := modelIndex(m, a, "rel", log)
			_ = ix.prune(context.Background(), ix.prunable(res.Bare))
		})
		s.Go("acquirer", 0, func(a *apimodel.Actor) {
			srv := loggingServer{modelServer{m, a}, "acq", log}
			h, err := srv.Get(context.Background(), widgetGK, "ns", "h2")
			if err != nil {
				return
			}
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.lines = modelIndex(m, a, "acq", log)
			_, _ = pr.acquire(context.Background(), h, []Ref{SecretRef("ns", "s")})
		})
		return m
	}
	exploreIndex(t, p, run)
}

// TestModelIndexStartupPrune runs the startup recheck's prune against an
// A2 (§10.6.3, "the same argument covers the startup recheck's prune"): P is
// bare, with a stale line; the recheck reads it bare and prunes the line
// while H2 holds it.
func TestModelIndexStartupPrune(t *testing.T) {
	p := SecretRef("ns", "s").ID()
	run := func(s *apimodel.Sched, log *[]string) *apimodel.Model {
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, false)
		must(m.Create(nsKey("idx"), metav1.ObjectMeta{}))
		must(m.Create(modelIndexKey, metav1.ObjectMeta{Annotations: map[string]string{modelIndexAnnotation: encodeIndex([]string{p.String()}), AnnotationRebuilt: "true"}}))
		must(m.Create(widgetKey("ns", "h2"), metav1.ObjectMeta{}))
		s.Go("releaser", 0, func(a *apimodel.Actor) {
			ix := modelIndex(m, a, "rel", log)
			_ = ix.recheck(context.Background(), func(ID, string) {})
		})
		s.Go("acquirer", 0, func(a *apimodel.Actor) {
			srv := loggingServer{modelServer{m, a}, "acq", log}
			h, err := srv.Get(context.Background(), widgetGK, "ns", "h2")
			if err != nil {
				return
			}
			pr := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace)
			pr.lines = modelIndex(m, a, "acq", log)
			_, _ = pr.acquire(context.Background(), h, []Ref{SecretRef("ns", "s")})
		})
		return m
	}
	exploreIndex(t, p, run)
}

// exploreIndex searches every schedule of run with up to three preemptions,
// checks the superset at the end of each, and requires both orders of the
// race to be reached.
func exploreIndex(t *testing.T, p ID, run func(s *apimodel.Sched, log *[]string) *apimodel.Model) {
	t.Helper()
	orders := map[bool]int{}
	res := apimodel.Explore(apimodel.Bounds{Preemptions: 3}, func(s *apimodel.Sched) error {
		var log []string
		m := run(s, &log)
		if err := s.Run(); err != nil {
			return err
		}
		if err := indexSuperset(m, p); err != nil {
			return fmt.Errorf("%w; log %q", err, log)
		}
		if raced, pruneFirst := indexOrders(log, p); raced {
			orders[pruneFirst]++
		}
		return nil
	})
	for _, f := range res.Failures {
		t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
	}
	if !res.Finished {
		t.Errorf("the search didn't finish: %d runs", res.Runs)
	}
	if orders[true] == 0 || orders[false] == 0 {
		t.Errorf("both orders of H2's index read and the prune must be reached: prune first %d, read first %d", orders[true], orders[false])
	}
	t.Logf("%d schedules; prune before H2's read: %d, after: %d", res.Runs, orders[true], orders[false])
}
