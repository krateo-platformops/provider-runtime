package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DisownPrefixOptions are DisownPrefix's (§7.4, A8).
type DisownPrefixOptions struct {
	// Holder is the deleted holder kind whose holds to strip. Required.
	Holder schema.GroupKind
	// Protected are the kinds to list, cluster-wide and live, by Holder's
	// marker label. Ignored when Names is set.
	Protected []schema.GroupVersionKind
	// Names, in place of the lists, are the objects to read and strip one by
	// one: for resourceNames RBAC, where cluster-wide lists aren't allowed.
	// A Ref's Next and Guaranteed don't apply.
	Names []Ref
	// DryRun reports what would be stripped, and changes nothing.
	DryRun bool
}

// DisownReport is what DisownPrefix found, and did.
type DisownReport struct {
	// Objects are the objects that carried a finalizer of Holder, or its
	// marker label, in the order found.
	Objects []ID
	// Finalizers is the number of Holder's finalizers on them.
	Finalizers int
	// Stripped are the objects it stripped: none with DryRun.
	Stripped []ID
}

// ErrHolderCRDExists is DisownPrefix's refusal while the holder kind's CRD
// exists, even while it is being deleted: its holders may still be deleting,
// and need their refs (§7.4).
var ErrHolderCRDExists = errors.New("credential-use: the holder kind's CRD still exists; use the Disown switch on it instead")

// DisownPrefix strips a deleted holder kind's holds (§7.4): every finalizer
// with the prefix credential-use.<kind>.<group>/, and its epoch, from every
// object Protected lists by the kind's marker label, or from each of Names;
// then the marker label, and the held label where no credential-use
// finalizer is left. It needs no holder kind and no switch, and refuses while
// a CRD of Holder exists. An object that changed since it was read is read
// and stripped again. Failures don't stop it: it strips what it can, and
// returns them joined.
func DisownPrefix(ctx context.Context, c Clients, o DisownPrefixOptions) (DisownReport, error) {
	var rep DisownReport
	prefix, err := FinalizerPrefix(o.Holder)
	if err != nil {
		return rep, err
	}
	if c.Metadata == nil || c.Dynamic == nil || c.Mapper == nil {
		return rep, errors.New("credential-use: Clients.Metadata, Clients.Dynamic and Clients.Mapper are required")
	}
	if len(o.Protected) == 0 && len(o.Names) == 0 {
		return rep, errors.New("credential-use: DisownPrefix needs Protected or Names")
	}
	if err := refuseWhileCRDExists(ctx, c, o.Holder); err != nil {
		return rep, err
	}
	srv := clientServer{md: c.Metadata, mapper: c.Mapper}
	var objs []metav1.PartialObjectMetadata
	var kinds []schema.GroupKind
	var errs []error
	if len(o.Names) > 0 {
		for _, r := range o.Names {
			m, err := srv.Get(ctx, r.GroupKind, r.Namespace, r.Name)
			if Classify(err) == ClassNotFound {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("reading %s: %w", r.ID(), err))
				continue
			}
			objs, kinds = append(objs, *m), append(kinds, r.GroupKind)
		}
	} else {
		for _, gvk := range o.Protected {
			gk := gvk.GroupKind()
			items, err := listLive(ctx, c, gk, "", MarkerLabel(o.Holder))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			for range items {
				kinds = append(kinds, gk)
			}
			objs = append(objs, items...)
		}
	}
	for i := range objs {
		m, gk := &objs[i], kinds[i]
		n := countPrefix(m.Finalizers, prefix)
		if _, marked := m.Labels[MarkerLabel(o.Holder)]; n == 0 && !marked {
			continue
		}
		id := ID{Group: gk.Group, Kind: gk.Kind, Namespace: m.Namespace, Name: m.Name}
		rep.Objects = append(rep.Objects, id)
		rep.Finalizers += n
		if o.DryRun {
			continue
		}
		stripped, _, err := stripDisowned(ctx, srv, gk, o.Holder, m)
		if err != nil {
			errs = append(errs, fmt.Errorf("stripping %s: %w", id, err))
			continue
		}
		if stripped {
			rep.Stripped = append(rep.Stripped, id)
		}
	}
	return rep, errors.Join(errs...)
}

// refuseWhileCRDExists returns ErrHolderCRDExists while a CRD of holder
// exists, found by its group and kind: the CRD's name needs the plural,
// which no mapper knows once the CRD is gone. A list that fails refuses
// too.
func refuseWhileCRDExists(ctx context.Context, c Clients, holder schema.GroupKind) error {
	l, err := c.Dynamic.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("credential-use: checking that the CRD of %s is gone: %w", holder, err)
	}
	for _, crd := range l.Items {
		group, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		kind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")
		if group == holder.Group && kind == holder.Kind {
			return fmt.Errorf("%w (%s)", ErrHolderCRDExists, crd.GetName())
		}
	}
	return nil
}

// countPrefix is the number of finalizers with prefix.
func countPrefix(finalizers []string, prefix string) int {
	n := 0
	for _, f := range finalizers {
		if strings.HasPrefix(f, prefix) {
			n++
		}
	}
	return n
}

// stripDisowned applies DisownPatch to m, of kind gk, for holder: on an
// object that changed since it was read, it reads it again and rebuilds the
// patch, up to maxGuardedAttempts times; any incarnation's holds of holder
// are stripped, since the prefix is the whole kind's. It reports whether it
// wrote, and how many of holder's finalizers the version it stripped
// carried; an object with nothing of holder's left, or gone, needs no write.
func stripDisowned(ctx context.Context, srv server, gk, holder schema.GroupKind, m *metav1.PartialObjectMetadata) (bool, int, error) {
	prefix, err := FinalizerPrefix(holder)
	if err != nil {
		return false, 0, err
	}
	cur := m
	var last error
	for range maxGuardedAttempts {
		p, err := DisownPatch(cur, holder)
		if errors.Is(err, ErrNotHeld) {
			return false, 0, nil
		}
		if err != nil {
			return false, 0, err
		}
		_, err = srv.Patch(ctx, gk, cur.Namespace, cur.Name, p.Type, p.Data)
		switch ClassifyJSONPatch(err) {
		case ClassNone:
			return true, countPrefix(cur.Finalizers, prefix), nil
		case ClassNotFound:
			return false, 0, nil
		case ClassStateChanged, ClassConflict:
			last = err
		default:
			return false, 0, err
		}
		cur, err = srv.Get(ctx, gk, m.Namespace, m.Name)
		if Classify(err) == ClassNotFound {
			return false, 0, nil
		}
		if err != nil {
			return false, 0, err
		}
	}
	return false, 0, fmt.Errorf("credential-use: %d changed reads in a row: %w", maxGuardedAttempts, last)
}
