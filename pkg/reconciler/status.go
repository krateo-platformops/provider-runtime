package reconciler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// errStatusConflictCause is what a status conflict's error says after the
// errUpdateManagedStatus prefix, the conflict's own text and the version,
// before the live hint and the pass's own error
// (docs/design-status-write-conflict.md §7.1).
const errStatusConflictCause = "if an ExternalClient method writes the object itself, it must copy the written " +
	"resourceVersion and the fields it wrote back onto mg (see ExternalClient)"

// statusConflictError is the error a status write that conflicted returns. It
// names the likely cause, and keeps the pass's own error, which a plain
// wrapped conflict would have hidden. It unwraps to both the conflict and the
// pass's error, so kerrors.IsConflict and errors.Is still see them.
//
// It is also a kerrors.APIStatus, the conflict's, and its Cause is the
// conflict. pkg/errors.Cause follows only a single Unwrap, so without that
// resource.IsAPIErrorWrapped, which a plain errors.Wrap of the conflict
// satisfied, would turn false.
type statusConflictError struct {
	// version is the resourceVersion the status write carried.
	version string
	// live describes the live object, or is empty when it wasn't read.
	live     string
	conflict error
	pass     error
}

func (e *statusConflictError) Error() string {
	var b strings.Builder
	// The prefix and the conflict's own text come first, as errors.Wrap wrote them
	// before, so "the object has been modified" and the object's name stay greppable.
	fmt.Fprintf(&b, "%s: %v: the object changed since the pass's read (read at resourceVersion %s): %s",
		errUpdateManagedStatus, e.conflict, e.version, errStatusConflictCause)
	if e.live != "" {
		b.WriteString("; ")
		b.WriteString(e.live)
	}
	if e.pass != nil {
		b.WriteString("; pass error: ")
		b.WriteString(e.pass.Error())
	}
	return b.String()
}

func (e *statusConflictError) Unwrap() []error {
	if e.pass == nil {
		return []error{e.conflict}
	}
	return []error{e.conflict, e.pass}
}

// Cause is the conflict, for callers that follow a Cause method, as
// github.com/pkg/errors does.
func (e *statusConflictError) Cause() error { return e.conflict }

// Status is the conflict's API status, so the error is a kerrors.APIStatus
// itself.
func (e *statusConflictError) Status() metav1.Status {
	var st kerrors.APIStatus
	if errors.As(e.conflict, &st) {
		return st.Status()
	}
	return metav1.Status{Status: metav1.StatusFailure, Code: http.StatusConflict, Reason: metav1.StatusReasonConflict, Message: e.conflict.Error()}
}

// statusConflict is the error for a status write of managed that returned
// conflict, on a pass that goes on to return passErr (or nil). With a live
// reader, it reads the object once and describes it against read, the object
// as the pass read it. Nothing is decided from that read: a failed one, or
// none, gives the plain message.
func (r *Reconciler) statusConflict(ctx context.Context, managed, read resource.Managed, conflict, passErr error) error {
	return &statusConflictError{
		version:  managed.GetResourceVersion(),
		live:     r.describeLive(ctx, managed, read),
		conflict: conflict,
		pass:     passErr,
	}
}

// describeLive reads managed live and says how it differs from read: its
// resourceVersion, its generation next to the pass's (a spec edit moves it, a
// status write doesn't), the top-level status keys that differ, and the newest
// managedFields entry, labelled as a hint. It returns "" when there's no live
// reader or the read fails.
func (r *Reconciler) describeLive(ctx context.Context, managed, read resource.Managed) string {
	if r.apiReader == nil {
		return ""
	}
	live := r.newManaged()
	if err := r.apiReader.Get(ctx, client.ObjectKeyFromObject(managed), live); err != nil {
		return ""
	}
	parts := []string{fmt.Sprintf("live resourceVersion %s and generation %d (the pass's generation %d)",
		live.GetResourceVersion(), live.GetGeneration(), managed.GetGeneration())}
	if read != nil {
		if keys, ok := statusKeysThatDiffer(read, live); ok {
			if len(keys) == 0 {
				parts = append(parts, "no status key differs from the pass's read")
			} else {
				parts = append(parts, "status keys that differ from the pass's read: ["+strings.Join(keys, ", ")+"]")
			}
		}
	}
	if e, ok := newestManagedFields(live.GetManagedFields()); ok {
		parts = append(parts, "newest managedFields entry (a hint, not proof of who wrote): "+describeEntry(e))
	}
	return strings.Join(parts, ", ")
}

// statusKeysThatDiffer are the top-level keys of status that differ between a
// and b, sorted. ok is false when either object can't be converted.
func statusKeysThatDiffer(a, b runtime.Object) (keys []string, ok bool) {
	sa, err := statusOf(a)
	if err != nil {
		return nil, false
	}
	sb, err := statusOf(b)
	if err != nil {
		return nil, false
	}
	for k, va := range sa {
		if vb, found := sb[k]; !found || !reflect.DeepEqual(va, vb) {
			keys = append(keys, k)
		}
	}
	for k := range sb {
		if _, found := sa[k]; !found {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys, true
}

// statusOf is obj's status as an unstructured map, empty when it has none.
func statusOf(obj runtime.Object) (map[string]any, error) {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	s, _ := u["status"].(map[string]any)
	return s, nil
}

// newestManagedFields is the entry with the latest time. ok is false when no
// entry has a time.
func newestManagedFields(entries []metav1.ManagedFieldsEntry) (newest metav1.ManagedFieldsEntry, ok bool) {
	for _, e := range entries {
		if e.Time == nil {
			continue
		}
		if !ok || !e.Time.Before(newest.Time) {
			newest, ok = e, true
		}
	}
	return newest, ok
}

func describeEntry(e metav1.ManagedFieldsEntry) string {
	on := "the object"
	if e.Subresource != "" {
		on = e.Subresource
	}
	return fmt.Sprintf("manager %q, %s of %s, at %s", e.Manager, e.Operation, on, e.Time.UTC().Format(time.RFC3339))
}
