// Package driver is a minimal reconcile loop for pkg/credentialuse's tests
// that follows §7.2 of docs/design-credential-use-finalizer.md to the
// letter: Acquire; a Connect that reads the protected objects; a recorded
// external Delete; BeforeEstablish; adding the owner finalizer; and Forget
// with the finalizer's removal, in one write. It is test-only: nothing
// outside the package's tests imports it.
package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// A Driver reconciles one holder kind.
type Driver struct {
	// Tracker is the kind's tracker. Nil runs the loop without
	// credential-use, as today's providers do: the control that must wedge
	// in a namespace teardown (§14). Refs then names what Connect reads.
	Tracker        *cu.Tracker
	Refs           cu.RefsFunc
	Dynamic        dynamic.Interface
	Holder         schema.GroupVersionResource
	OwnerFinalizer string
	// Connect reads a held ref's object, as a provider's Connect reads its
	// credentials. The test checks its invariant there. Nil reads nothing.
	Connect func(ctx context.Context, holder types.NamespacedName, ref cu.Ref) error
	// OnRead, if set, sees the holder each pass has just read, before its
	// Acquire. It must not change it.
	OnRead func(ctx context.Context, holder *unstructured.Unstructured)

	mu     sync.Mutex
	events []string
}

// A Result is what one pass did.
type Result struct {
	// Requeue: the pass asks to run again (a conflict, MustNotConnect, or a
	// failed BeforeEstablish).
	Requeue bool
	// Gone: the holder no longer exists.
	Gone bool
	// Acquired is the pass's Acquire result.
	Acquired cu.Acquired
	// Connected: the pass read its held refs.
	Connected bool
}

// Events are what the driver did, in order: "connect ns/name", "delete
// ns/name" (the external Delete), "establish ns/name", "release ns/name".
func (d *Driver) Events() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.events)
}

func (d *Driver) record(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, fmt.Sprintf(format, args...))
}

// Reconcile runs one pass of holder nn.
func (d *Driver) Reconcile(ctx context.Context, nn types.NamespacedName) (Result, error) {
	var res Result
	r := d.Dynamic.Resource(d.Holder).Namespace(nn.Namespace)
	obj, err := r.Get(ctx, nn.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		res.Gone = true
		return res, nil
	}
	if err != nil {
		return res, err
	}
	if d.OnRead != nil {
		d.OnRead(ctx, obj)
	}

	// §7.2 item 2: Acquire, before anything reads a protected object.
	var a cu.Acquired
	if d.Tracker != nil {
		a, err = d.Tracker.Acquire(ctx, obj)
		res.Acquired = a
		if errors.Is(err, cu.ErrHolderConflict) {
			res.Requeue = true
			return res, nil
		}
		if err != nil {
			return res, err
		}
		if a.MustNotConnect {
			res.Requeue = true
			return res, nil
		}
	} else {
		refs, err := d.Refs(obj)
		if err != nil {
			return res, err
		}
		a.Held = refs
	}

	// Connect: read every held ref.
	if d.Connect != nil {
		for _, ref := range a.Held {
			if err := d.Connect(ctx, nn, ref); err != nil {
				return res, fmt.Errorf("connect: %w", err)
			}
		}
	}
	res.Connected = true
	d.record("connect %s", nn)

	owned := slices.Contains(obj.GetFinalizers(), d.OwnerFinalizer)
	if obj.GetDeletionTimestamp() != nil {
		// The external Delete, then §7.2 item 4: Forget, in the same write
		// as the owner finalizer's removal; or on its own when the
		// finalizer is already gone but the record isn't.
		if owned {
			d.record("delete %s", nn)
		}
		changed := false
		if d.Tracker != nil {
			changed = d.Tracker.Forget(obj)
		}
		if !owned && !changed {
			return res, nil
		}
		obj.SetFinalizers(slices.DeleteFunc(obj.GetFinalizers(), func(f string) bool { return f == d.OwnerFinalizer }))
		if err := d.update(ctx, r, obj); err != nil {
			return res, err
		}
		d.record("release %s", nn)
		return res, nil
	}

	if !owned {
		// §7.2 item 3: BeforeEstablish, just before the owner finalizer.
		if d.Tracker != nil {
			if err := d.Tracker.BeforeEstablish(ctx, obj, a); err != nil {
				res.Requeue = true
				return res, nil
			}
		}
		obj.SetFinalizers(append(obj.GetFinalizers(), d.OwnerFinalizer))
		if err := d.update(ctx, r, obj); err != nil {
			return res, err
		}
		d.record("establish %s", nn)
	}
	return res, nil
}

// update writes obj; a conflict is a requeue the caller sees as an error.
func (d *Driver) update(ctx context.Context, r dynamic.ResourceInterface, obj *unstructured.Unstructured) error {
	_, err := r.Update(ctx, obj, metav1.UpdateOptions{})
	return err
}
