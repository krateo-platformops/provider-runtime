// Package scaletozero scales a controller Deployment to zero replicas while the
// Kind it reconciles has no instances, and back up when an instance appears.
//
// It is for owners that run one controller Deployment per Kind or per version,
// such as core-provider for its composition controllers and oasgen-provider for
// its REST controllers. The owner calls Reconcile from its own reconcile. The
// helper never runs inside the controller it scales: a controller at zero
// replicas can't wake itself.
//
// # The helper is the mechanism, not the trigger
//
// Nothing inside the helper watches anything. A controller at 0 with no
// instances gets no reconcile, and neither does its owner unless the owner
// arranges one. The owner must reconcile its owning object when the first
// instance of the Kind appears, for example with a per-Kind watch, and call
// Reconcile from there.
//
// # Only between idle and running
//
// The helper wakes a controller from 0 to the owner's active count and puts an
// idle one to 0. It never changes a non-zero count while instances exist: once
// a controller is up, the owner (or an HPA) owns how many replicas it runs.
//
// # Per-call cost
//
// Reconcile is safe to call on every poll. Each call makes two reads, a GET of
// the scale subresource and a metadata-only LIST that stops at the first
// instance, and patches only on change. During the hold-down,
// Result.RequeueAfter keeps the scale-down on time between polls.
//
// Design: docs/design-scale-to-zero.md. The API was settled with both owners in
// provider-runtime#39.
//
// # Precondition for every caller
//
// An owner that calls Reconcile must not write the controller Deployment's
// spec.replicas anywhere else, including by re-applying it from a desired spec.
// A full-object Update that omits replicas doesn't satisfy this either: the API
// server defaults a nil to 1, so the owner would scale a zeroed controller back
// up and fight the helper. An owner that renders the whole Deployment must
// render replicas from the last Result.Replicas.
//
// # The selector
//
// The selector must be exactly the one the controller uses to choose what it
// reconciles. A narrower selector misses instances, and a miss scales a
// controller to zero while instances it serves still exist. There is no
// default: pass labels.Everything() when the controller reconciles every
// instance of the Kind. A nil selector is an error.
//
// # A live reader is required
//
// Exists needs a reader that reads from the API server: pass
// mgr.GetAPIReader(), not mgr.GetClient(). Reconcile lists with the client it
// is given, so give it one built with client.New, not the manager's cached
// client. A cache-backed reader can't paginate, so Exists fails on it whenever
// the Kind is empty rather than report a wrong answer. It would also start a
// metadata informer per Kind, which is the memory this package exists to save.
//
// # RBAC
//
// The owner needs list on the watched resource across all namespaces, and get
// and patch on deployments/scale in the Deployment's namespace. It needs no
// access to the Deployment object itself.
package scaletozero

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
)

// pageSize is how many instances one LIST page asks for: one is enough to
// answer whether any exists.
const pageSize int64 = 1

// Reason says why Reconcile chose the replicas it did.
type Reason string

const (
	// ReasonActive: at least one instance exists, so the controller runs.
	ReasonActive Reason = "Active"
	// ReasonIdleHoldDown: no instance exists, but the Kind hasn't been empty
	// for the whole hold-down yet, so the controller keeps running.
	ReasonIdleHoldDown Reason = "IdleHoldDown"
	// ReasonIdle: no instance exists and the hold-down has passed, so the
	// controller runs zero replicas.
	ReasonIdle Reason = "Idle"
	// ReasonUnknown: Reconcile failed and decided nothing. Result.Replicas is
	// then a value that keeps the controller up (see Result).
	ReasonUnknown Reason = "Unknown"
)

// Result is what Reconcile decided.
//
// On an error, Reason is ReasonUnknown, LastSeen is the lastSeen that was
// passed in, and Replicas is never 0: it is the Deployment's current replicas
// when they were read and are non-zero, otherwise active (or 1 when active
// itself was the invalid argument). An owner that renders Replicas without
// checking the error therefore never scales a controller down. Check the error
// anyway: rendering active on an error wakes a controller at 0.
type Result struct {
	// Replicas is the replica count the Deployment has after Reconcile: 0 when
	// idle; active when Reconcile woke it from 0; otherwise the non-zero count
	// it already had, which Reconcile leaves alone.
	Replicas int32
	// Reason is why.
	Reason Reason
	// LastSeen is the value to persist and pass to the next call: now when an
	// instance exists or when lastSeen was zero, otherwise the lastSeen that
	// was passed in. On an error it is always the lastSeen passed in.
	LastSeen time.Time
	// RequeueAfter is when the decision changes with time alone: the rest of
	// the hold-down under ReasonIdleHoldDown, otherwise 0. Whether instances
	// exist is not time-based: the owner must call again when they are created
	// or deleted (a watch, or its own resync).
	RequeueAfter time.Duration
	// Exists reports whether an instance was found.
	Exists bool
	// Patched reports whether Reconcile changed the Deployment's scale.
	Patched bool
}

// Exists reports whether at least one instance of gvk matching sel exists, in
// any namespace, terminating ones included. It stops at the first match. A
// failed LIST, an unserved version or a missing CRD is an error, never false.
//
// It lists metadata-only, one item per page. With a label selector the API
// server may return a page with fewer items than the limit, even none, and
// still carry a continue token: Exists follows the token until it sees an item
// or the token runs out, so an empty page is never taken for an empty Kind. A
// failure on any page is an error.
//
// r must be a live reader such as mgr.GetAPIReader(): a cache-backed reader
// makes Exists fail whenever the Kind is empty.
//
// sel is required: pass labels.Everything() to match every instance. A nil sel
// is an error.
func Exists(ctx context.Context, r client.Reader, gvk schema.GroupVersionKind, sel labels.Selector) (bool, error) {
	if r == nil {
		return false, errors.New("scaletozero: Exists needs a reader")
	}
	if sel == nil {
		return false, errors.Errorf("scaletozero: listing %s needs a label selector; pass labels.Everything() to match every instance", gvk)
	}
	if gvk.Version == "" || gvk.Kind == "" {
		return false, errors.Errorf("scaletozero: listing needs a version and a kind, got %q", gvk)
	}

	cont := ""
	for {
		list := &metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		opts := []client.ListOption{client.MatchingLabelsSelector{Selector: sel}, client.Limit(pageSize)}
		if cont != "" {
			opts = append(opts, client.Continue(cont))
		}
		if err := r.List(ctx, list, opts...); err != nil {
			return false, fmt.Errorf("scaletozero: listing %s: %w", gvk, err)
		}
		if len(list.Items) > 0 {
			return true, nil
		}
		if list.Continue == "" {
			return false, nil
		}
		if list.Continue == cont {
			return false, errors.Errorf("scaletozero: listing %s: the API server returned the same continue token twice", gvk)
		}
		cont = list.Continue
	}
}

// Decide is pure: 1 while an instance exists; 0 only once the Kind has been
// empty for the whole holdDown since lastSeen. 1 means running and 0 means
// idle; Reconcile maps 1 to the owner's active count. Decide also returns when
// to look again: the rest of the hold-down while it runs, otherwise 0.
//
//   - exists: 1, and nothing time-based to wait for.
//   - empty and lastSeen zero: the hold-down starts now, so 1 for holdDown.
//     A Kind seen empty for the first time is never scaled down at once.
//   - empty within the hold-down: 1, and the remaining time.
//   - empty once holdDown has passed since lastSeen (inclusive): 0.
//   - now before lastSeen (the clock went backwards, or lastSeen came from a
//     clock ahead of this one): 1, looking again within one holdDown.
//   - holdDown <= 0 is invalid input: 1, so a bad argument never scales a
//     controller down. Reconcile rejects it as an error.
func Decide(exists bool, lastSeen, now time.Time, holdDown time.Duration) (replicas int32, requeueAfter time.Duration) {
	if exists || holdDown <= 0 {
		return 1, 0
	}
	if lastSeen.IsZero() {
		return 1, holdDown
	}
	elapsed := now.Sub(lastSeen)
	if elapsed < 0 {
		return 1, holdDown
	}
	if elapsed >= holdDown {
		return 0, 0
	}
	return 1, holdDown - elapsed
}

// nextLastSeen is the lastSeen Reconcile hands back after a successful list:
// now when an instance exists or no hold-down has started yet, otherwise
// unchanged. A lastSeen in the future is kept, not reset to now: resetting it
// would restart the hold-down from a clock that is behind, and the controller
// could then be scaled down sooner than holdDown after the last real sighting.
// The cost: a lastSeen far in the future keeps the controller up until the
// clock passes it plus holdDown.
func nextLastSeen(exists bool, lastSeen, now time.Time) time.Time {
	if exists || lastSeen.IsZero() {
		return now
	}
	return lastSeen
}

// Reconcile lists, decides and patches the Deployment's scale subresource only
// on change. It stores no state: lastSeen comes in, and the new value comes
// back in Result with the replicas and a reason (Active, IdleHoldDown, Idle).
// The owner persists LastSeen somewhere nothing else overwrites, such as its
// own status, and passes it to the next call.
//
// Precondition: the caller must not write the Deployment's spec.replicas
// anywhere else, including by re-applying it from a desired spec. A full-object
// Update that omits replicas doesn't satisfy this either: the API server
// defaults a nil to 1, so the owner would scale a zeroed controller back up and
// fight the helper.
//
// Reconcile is the mechanism, not the trigger: the owner must call it when the
// first instance of the Kind appears (see the package documentation).
//
// active (>= 1, an error otherwise) is the replica count to run while instances
// exist. Reconcile only moves between idle and running:
//   - running is decided and the Deployment is at 0: it is patched to active;
//   - running is decided and the Deployment is at any non-zero count: it is
//     left alone, even when that count isn't active, because the owner or an
//     HPA owns the count once the controller is up;
//   - idle is decided: it is patched to 0.
//
// Any error (a failed LIST, a missing CRD, an unserved version, a nil
// selector, a holdDown <= 0, an active < 1, a failed read or patch of the
// scale) changes nothing: there is no patch, Result.LastSeen is the lastSeen
// passed in, and Result.Replicas is never 0 (see Result). A failed LIST never
// scales a controller up from 0, nor down.
//
// Per call it reads twice, a GET of the scale subresource and a metadata-only
// LIST that stops at the first instance, and patches only on change, so it is
// safe to call on every poll. During the hold-down, Result.RequeueAfter keeps
// the scale-down on time between polls.
//
// The current replicas are read from the scale subresource, not from the
// Deployment's spec, and only spec.replicas is patched, with a merge patch on
// autoscaling/v1 Scale. c must read live: give it a client built with
// client.New, not mgr.GetClient().
func Reconcile(ctx context.Context, c client.Client, deploy types.NamespacedName, gvk schema.GroupVersionKind, sel labels.Selector, lastSeen time.Time, holdDown time.Duration, active int32) (Result, error) {
	// fail keeps everything as it was, and reports replicas that never read
	// as 0: the current ones when they were read and are non-zero, else active.
	read := false
	var current int32
	fail := func(err error) (Result, error) {
		r := active
		if r < 1 {
			r = 1
		}
		if read && current > 0 {
			r = current
		}
		return Result{Replicas: r, Reason: ReasonUnknown, LastSeen: lastSeen}, err
	}

	if c == nil {
		return fail(errors.New("scaletozero: Reconcile needs a client"))
	}
	if deploy.Namespace == "" || deploy.Name == "" {
		return fail(errors.Errorf("scaletozero: the Deployment needs a namespace and a name, got %q", deploy.String()))
	}
	obj := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: deploy.Namespace, Name: deploy.Name}}
	scale := &autoscalingv1.Scale{}
	if err := c.SubResource("scale").Get(ctx, obj, scale); err != nil {
		return fail(fmt.Errorf("scaletozero: reading the scale of deployment %s: %w", deploy, err))
	}
	read, current = true, scale.Spec.Replicas

	if holdDown <= 0 {
		return fail(errors.Errorf("scaletozero: holdDown must be greater than 0, got %s", holdDown))
	}
	if active < 1 {
		return fail(errors.Errorf("scaletozero: active must be at least 1, got %d", active))
	}
	exists, err := Exists(ctx, c, gvk, sel)
	if err != nil {
		return fail(err)
	}

	now := time.Now()
	running, requeueAfter := Decide(exists, lastSeen, now, holdDown)
	target := current
	switch {
	case running == 0:
		target = 0
	case current == 0:
		target = active
	}
	res := Result{
		Replicas:     target,
		Reason:       reasonFor(exists, running),
		LastSeen:     nextLastSeen(exists, lastSeen, now),
		RequeueAfter: requeueAfter,
		Exists:       exists,
	}
	if target == current {
		return res, nil
	}
	// A raw merge patch that says the number. One computed from the Scale
	// would send "replicas": null for 0, since ScaleSpec.Replicas is
	// omitempty. The API server reads that null as 0 (the revert check found
	// no difference), but the patch shouldn't depend on it.
	patch := client.RawPatch(types.MergePatchType, fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, target))
	if err := c.SubResource("scale").Patch(ctx, obj, patch, client.WithSubResourceBody(scale)); err != nil {
		return fail(fmt.Errorf("scaletozero: scaling deployment %s to %d: %w", deploy, target, err))
	}
	res.Patched = true
	return res, nil
}

func reasonFor(exists bool, running int32) Reason {
	switch {
	case exists:
		return ReasonActive
	case running > 0:
		return ReasonIdleHoldDown
	default:
		return ReasonIdle
	}
}
