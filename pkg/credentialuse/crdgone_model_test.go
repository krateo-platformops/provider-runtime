package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The holder kind's CRD gone, in the model (§6.4, "The holder kind itself
// gone"): a batch whose list is a 404 reads the CRD, while the CRD is created
// again, and a new holder holds P, at every point of the batch. Both are the
// protocol's own code; the model adds the CRD, and a list that answers 404
// while it isn't there. Its key is modelCRDKey, the Disown models' too.

// goneServer is a modelServer whose list of Widgets is a 404 while their CRD
// isn't there, or, with lags, whether or not it is: an API server whose CRD
// informer lags.
type goneServer struct {
	modelServer
	who  string
	lags bool
	log  *[]string
}

func (s goneServer) List(_ context.Context, gk schema.GroupKind, namespace string) ([]metav1.PartialObjectMetadata, error) {
	var out []metav1.PartialObjectMetadata
	err := s.call("list "+gk.Kind, func() error {
		if _, err := s.m.Get(modelCRDKey); err != nil || s.lags {
			return errTest404
		}
		out = s.m.List(gk, namespace)
		return nil
	})
	return out, err
}

func (s goneServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	out, err := s.modelServer.Patch(ctx, gk, namespace, name, pt, data)
	if gk == secretGroupKind {
		*s.log = append(*s.log, fmt.Sprintf("%s patch %s %v", s.who, pt, ClassifyJSONPatch(err)))
	}
	return out, err
}

// getCRD is the GET of the CRD, a step of its own, as Tracker.noHolders makes
// it.
func (s goneServer) getCRD() error {
	var err error
	_ = s.call("get CRD", func() error { _, err = s.m.Get(modelCRDKey); return nil })
	*s.log = append(*s.log, fmt.Sprintf("%s crd gone=%v", s.who, crdNotFound(err, modelCRDKey.Name)))
	return err
}

// goneOrders says what a schedule's log reached: a batch that read the CRD
// gone, and the new holder's A2 between that and its R3, which must fail; and
// one whose R3 landed before the A2, which adds the hold back.
func goneOrders(log []string) (broken, heldAgain bool) {
	gone, a2, r3 := -1, -1, -1
	for i, e := range log {
		switch {
		case e == "rel crd gone=true":
			gone, r3 = i, -1
		case strings.HasPrefix(e, "acq patch "+string(types.StrategicMergePatchType)) && a2 < 0:
			a2 = i
		case strings.HasPrefix(e, "rel patch "+string(types.JSONPatchType)) && gone >= 0 && r3 < 0:
			r3 = i
			if a2 > gone && strings.HasSuffix(e, ClassStateChanged.String()) {
				broken = true
			}
		}
	}
	return broken, r3 >= 0 && a2 > r3
}

// goneRule is how a searched releaser tells that the kind is gone.
type goneRule int

const (
	readAfterTheList goneRule = iota // the protocol's
	readBeforeR1                     // unsafe: a control
	listAlone                        // unsafe: a control
)

// exploreCRDGone searches every schedule, with up to preemptions of them, of
// two actors: a releaser of two batches with no caches, on a hold whose
// holder went with the CRD; and the kind's return: the CRD is created, then
// h2, which holds P, is established, and is deleted after P is. The first
// batch's lists are a 404 whether or not the CRD is there. No grace ends in a
// schedule, so h2's deletion pass must read P (§14).
func exploreCRDGone(preemptions int, rule goneRule) (res apimodel.Result, broken, heldAgain, goneReleased int) {
	p := SecretRef("ns", "s").ID()
	ctx := context.Background()
	res = apimodel.Explore(apimodel.Bounds{Preemptions: preemptions, MaxRuns: 200_000}, func(s *apimodel.Sched) error {
		var log []string
		var errs []error
		m := apimodel.New(s.Clock.Now)
		seed(m, "ns", "s", nil, true)
		protocolOf := func(a *apimodel.Actor, who string, lags bool) (*protocol, goneServer) {
			srv := goneServer{modelServer{m, a}, who, lags, &log}
			return newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), a.Clock(), testGrace), srv
		}
		released := false
		s.Go("releaser", 0, func(a *apimodel.Actor) {
			for batch := range 2 {
				pr, srv := protocolOf(a, "rel", batch == 0)
				var before error
				if rule == readBeforeR1 {
					before = srv.getCRD()
				}
				pr.noHolders = func(_ context.Context, listErr error) error {
					if !kindUnserved(listErr) {
						return listErr
					}
					crdErr := before
					if rule == readAfterTheList {
						crdErr = srv.getCRD()
					}
					if rule != listAlone && !crdNotFound(crdErr, modelCRDKey.Name) {
						return listErr
					}
					return nil
				}
				res, _ := pr.release(ctx, "ns", []ID{p})
				if len(res.Released) > 0 && slices.Contains(log, "rel crd gone=true") {
					released = true
				}
			}
		})
		s.Go("return", 0, func(a *apimodel.Actor) {
			pr, srv := protocolOf(a, "acq", false)
			do := func(what string, op func() error) {
				if err := a.Call(what, op); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", what, err))
				}
			}
			// pass is one pass of h2, and reports whether Connect read P.
			pass := func(finalizer bool) (held, read bool) {
				h, err := srv.Get(ctx, widgetGK, "ns", "h2")
				if err != nil {
					errs = append(errs, err)
					return false, false
				}
				acq, err := pr.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
				if err != nil {
					errs = append(errs, err)
					return false, false
				}
				held = len(acq.Held) == 1
				f, _ := FinalizerName(widgetGK, "ns")
				if o, err := m.Get(keyOf(p)); held && (err != nil || !hasFinalizer(o, f)) {
					errs = append(errs, errors.New("SAFETY: a pass reported P held, and it isn't"))
				}
				_, err = srv.Get(ctx, secretGroupKind, "ns", "s")
				read = err == nil
				if !finalizer {
					pr.forget(h)
				}
				if _, err := srv.Patch(ctx, widgetGK, "ns", "h2", types.MergePatchType, finalizerPatch(h, finalizer)); err != nil {
					errs = append(errs, err)
				}
				return held, read
			}
			do("create CRD", func() error { return m.Create(modelCRDKey, metav1.ObjectMeta{}) })
			do("create h2", func() error { return m.Create(widgetKey("ns", "h2"), metav1.ObjectMeta{}) })
			if held, read := pass(true); !held || !read {
				errs = append(errs, fmt.Errorf("h2's live pass: held %v, read %v", held, read))
			}
			do("delete P", func() error { return m.Delete(keyOf(p)) })
			do("delete h2", func() error { return m.Delete(widgetKey("ns", "h2")) })
			if _, read := pass(false); !read {
				errs = append(errs, errors.New("SAFETY: h2, deleting, read P as NotFound after holding it"))
			}
		})
		if err := s.Run(); err != nil {
			return err
		}
		if released {
			goneReleased++
		}
		b, h := goneOrders(log)
		if b {
			broken++
		}
		if h {
			heldAgain++
		}
		// With h2 gone, what is left of the hold goes.
		pr := newProtocol(goneServer{modelServer{m: m}, "end", false, &log}, noViews{}, newVerified(widgetGK), newFirstSight(), s.Clock, testGrace)
		if _, err := pr.release(ctx, "ns", []ID{p}); err != nil {
			errs = append(errs, err)
		}
		if o, err := m.Get(keyOf(p)); err == nil {
			errs = append(errs, fmt.Errorf("CONVERGENCE: nothing uses P, and it stays, with %v", o.Finalizers))
		}
		if err := errors.Join(errs...); err != nil {
			return fmt.Errorf("%w; log %q", err, log)
		}
		return nil
	})
	return res, broken, heldAgain, goneReleased
}

// TestModelCRDGoneAndBack: no schedule takes P from under the returned kind's
// holder. Its smallest schedule that matters is one preemption: R1 and the
// 404 list, the GET that finds the CRD gone; then all of the kind's return up
// to h2's A2; then R3, whose epoch test fails, and the next round, whose GET
// finds the CRD, or, if the list tells, whose list finds h2.
func TestModelCRDGoneAndBack(t *testing.T) {
	res, broken, heldAgain, released := exploreCRDGone(3, readAfterTheList)
	for _, f := range res.Failures {
		t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
	}
	if !res.Finished || broken == 0 || heldAgain == 0 || released == 0 {
		t.Errorf("finished %v; R3 broken by an A2 after the GET: %d, held again after R3: %d, released over the gone CRD: %d; want each reached",
			res.Finished, broken, heldAgain, released)
	}
	t.Logf("%d schedules; R3 broken by an A2 after the GET: %d, held again after R3: %d, released over the gone CRD: %d", res.Runs, broken, heldAgain, released)
}

// The controls: with the CRD read before R1, or not at all, the search finds
// the release of P from under h2, whose A2 came before R1, and whose record a
// lagging API server's 404 didn't show.
func TestModelCRDGoneControls(t *testing.T) {
	for name, rule := range map[string]goneRule{"the CRD is read before R1": readBeforeR1, "the list alone": listAlone} {
		t.Run(name, func(t *testing.T) {
			res, _, _, _ := exploreCRDGone(2, rule)
			if !slices.ContainsFunc(res.Failures, func(f apimodel.Failure) bool { return strings.Contains(f.Err.Error(), "SAFETY") }) {
				t.Errorf("%d schedules, finished %v, and none released P from under its holder: the search can't see what it is for", res.Runs, res.Finished)
			}
		})
	}
}
