package credentialuse_test

// A holder kind with several versions (§6.2), against the API server: A1 goes
// through the version the holder's own controller declares, so it loses nothing
// that the controller's own writes keep.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// trackedAt tracks p's kind as a holder kind declared at version, whose holders
// name Secret "creds". The mapper's version of the kind is v1.
func trackedAt(t *testing.T, p pruneRig, version string) *cu.Tracker {
	t.Helper()
	gvk := schema.GroupVersionKind{Group: pruneGroup, Version: version, Kind: p.kind}
	o := widgetOptions(func(types.NamespacedName) {})
	o.Holder = gvk
	o.Refs = func(obj metav1.Object) ([]cu.Ref, error) {
		return []cu.Ref{cu.SecretRef(obj.GetNamespace(), "creds")}, nil
	}
	tr, err := cu.NewShared(mappedClients(t, gvk.GroupKind()), cu.SharedOptions{}).Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// read is p's object name at version.
func (p pruneRig) read(name, version string) *unstructured.Unstructured {
	p.t.Helper()
	u, err := env.Dynamic.Resource(p.gvr(version)).Namespace(p.ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		p.t.Fatal(err)
	}
	return u
}

// acquired runs one pass of the holder name, read at version, and returns the
// holder as it then is there.
func acquired(t *testing.T, p pruneRig, tr *cu.Tracker, name, version string) *unstructured.Unstructured {
	t.Helper()
	a, err := tr.Acquire(context.Background(), p.read(name, version))
	if err != nil || len(a.Held) != 1 {
		t.Fatalf("Acquire = %+v, %v; want the Secret held", a, err)
	}
	return p.read(name, version)
}

// The conversion is None, the mapper's version is v1, and the holder kind is
// declared at v1beta1, its storage version, with a field v1 lacks. Acquire's A1
// keeps it.
func TestA1KeepsWhatTheHoldersOwnVersionHas(t *testing.T) {
	cutest.Require(t, env)
	p := newPruneRig(t, "HolderPrune", "v1beta1", nil)
	createSecret(t, p.ns, "creds")
	tr := trackedAt(t, p, "v1beta1")
	p.create("h")
	for _, pass := range []string{"the first pass", "the second"} {
		h := acquired(t, p, tr, "h", "v1beta1")
		if !cu.ReadRecord(h).Names(cu.SecretRef(p.ns, "creds").ID()) {
			t.Fatalf("precondition: after %s, no record: %v", pass, h.GetAnnotations())
		}
		if !p.kept("h") {
			t.Fatalf("%s dropped spec.%s from the holder", pass, onlyIn("v1beta1"))
		}
	}
}

// With a webhook that drops annotations, the record survives A1 where the
// declared version is the storage one. Where it isn't, nothing can keep it, and
// today's pass doesn't say so: it reports its ref held, with no record, so R2
// finds no user and the hold is released (§6.2).
func TestARecordAConversionDrops(t *testing.T) {
	cutest.Require(t, env)
	for i, c := range []struct {
		storage string
		kept    bool
	}{{"v1beta1", true}, {"v1", false}} {
		t.Run("declared at v1beta1, stored at "+c.storage, func(t *testing.T) {
			p := newPruneRig(t, fmt.Sprintf("HolderRecord%d", i), c.storage, roundTrips(t, dropsAnnotations))
			createSecret(t, p.ns, "creds")
			tr := trackedAt(t, p, "v1beta1")
			p.create("h")
			h := acquired(t, p, tr, "h", "v1beta1")
			if got := cu.ReadRecord(h).Names(cu.SecretRef(p.ns, "creds").ID()); got != c.kept {
				t.Errorf("the record is there: %v, want %v", got, c.kept)
			}
			users, err := tr.Users(context.Background(), cu.SecretRef(p.ns, "creds"), p.ns)
			if err != nil || (len(users) == 1) != c.kept {
				t.Errorf("Users = %v, %v; want the holder: %v", users, err, c.kept)
			}
		})
	}
}

// Two processes of one holder kind, each declaring the version its controller
// writes (§6.8): the pass's own process writes A1, at its own version, and each
// sees the other's holders as users, whatever their version.
func TestProcessesOnTwoVersionsOfOneHolderKind(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	p := newPruneRig(t, "HolderPair", "v1beta1", nil)
	createSecret(t, p.ns, "creds")
	trackers := map[string]*cu.Tracker{"v1beta1": trackedAt(t, p, "v1beta1"), "v1": trackedAt(t, p, "v1")}
	p.create("of-beta")
	acquired(t, p, trackers["v1beta1"], "of-beta", "v1beta1")
	for v, tr := range trackers {
		users, err := tr.Users(ctx, cu.SecretRef(p.ns, "creds"), p.ns)
		if err != nil || len(users) != 1 {
			t.Errorf("the users that the process at %s sees: %v, %v; want the holder of v1beta1", v, users, err)
		}
	}
	if !p.kept("of-beta") {
		t.Error("the holder lost a field")
	}
}

// A list of the holders at a version that one of them can't be converted to
// fails, or has it: it never leaves it out, which R2 would read as one user
// fewer.
func TestPremiseListWithAnUnconvertibleObject(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	var fails atomic.Bool
	p := newPruneRig(t, "HolderConvert", "v1", roundTrips(t, func(u *unstructured.Unstructured) error {
		if fails.Load() && u.GetName() == "bad" {
			return errors.New("bad can't be converted")
		}
		return nil
	}))
	p.create("good")
	p.create("bad")
	list := func(rv string) ([]string, error) {
		lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		l, err := env.Metadata.Resource(p.gvr("v1beta1")).Namespace(p.ns).List(lctx, metav1.ListOptions{ResourceVersion: rv, Limit: 500})
		if err != nil {
			return nil, err
		}
		var names []string
		for _, o := range l.Items {
			names = append(names, o.Name)
		}
		return names, nil
	}
	cutest.Eventually(t, time.Minute, func() error {
		names, err := list("")
		if err == nil && len(names) != 2 {
			err = fmt.Errorf("%v: %w", names, cutest.ErrNotYet)
		}
		return err
	})
	fails.Store(true)
	failed := 0
	for i := range 6 {
		rv := "" // R2's: consistent; "0" is the watch cache's, as it is
		if i%2 == 1 {
			rv = "0"
		}
		if i == 2 { // the watch cache must now read a version it can't convert
			p.patches()["a merge patch of one label"]("bad", "v1")
		}
		names, err := list(rv)
		answer := strings.Join(names, " ")
		if err != nil {
			answer = "error: " + err.Error()
			failed++
		}
		t.Logf("CONVERT list %d, resourceVersion %q: %s", i, rv, answer)
		if err == nil && !hasString(names, "bad") {
			t.Errorf("list %d left out the object that can't be converted: %v", i, names)
		}
	}
	if failed == 0 {
		t.Log("CONVERT no list failed: the API server answered from what it had converted before")
	}
}
