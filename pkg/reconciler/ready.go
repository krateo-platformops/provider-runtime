package reconciler

import (
	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// readyAssessedError marks an error returned by an ExternalClient that set the
// Ready condition itself before returning it. See ReadyAssessed.
type readyAssessedError struct {
	err error
}

func (e *readyAssessedError) Error() string { return e.err.Error() }

func (e *readyAssessedError) Unwrap() error { return e.err }

// ReadyAssessed marks err, returned by Connect or Observe, as one that does not
// concern readiness: the provider set the Ready condition itself during this
// call, and the error is about something else (for example a write that
// failed after the external resource was read). The runtime then leaves Ready
// as the provider set it, instead of setting it to NotObserved. Synced still
// reports err as a ReconcileError. See the Ready contract on
// ExternalClient.Observe.
//
// Wrap only when the provider really set Ready on this call. An error that
// means "I could not tell" must not be wrapped: there Ready=NotObserved is the
// truthful answer.
//
// The message of the returned error is err's, and errors.Is, errors.As and
// errors.Unwrap see through it to err. ReadyAssessed(nil) returns nil, so
//
//	return obs, reconciler.ReadyAssessed(err)
//
// is safe whether or not err is nil.
func ReadyAssessed(err error) error {
	if err == nil {
		return nil
	}
	return &readyAssessedError{err: err}
}

// IsReadyAssessed reports whether err, or any error it wraps, was marked with
// ReadyAssessed. It is false for a nil error.
func IsReadyAssessed(err error) bool {
	var target *readyAssessedError
	return errors.As(err, &target)
}

// markNotObserved sets Ready after Connect or Observe failed with err:
//   - the provider assessed Ready itself (ReadyAssessed): Ready is left as the
//     provider set it;
//   - the resource is being deleted (it has a deletion timestamp, or Ready
//     already reads Deleting): Ready is Deleting, which is true whatever the
//     external system answers, so the runtime writes it rather than relying on
//     every provider to set it before an error on its deletion path (v1.8.2);
//   - otherwise: Ready is NotObserved.
func markNotObserved(mg resource.Managed, err error) {
	if IsReadyAssessed(err) {
		return
	}
	if meta.WasDeleted(mg) || mg.GetCondition(prv1.TypeReady).Reason == prv1.ReasonDeleting {
		mg.SetConditions(prv1.Deleting())
		return
	}
	mg.SetConditions(prv1.NotObserved())
}

// readyNotSetAfterObserve reports whether a successful Observe left Ready at
// the NotObserved marker an earlier failure set, which breaks the Ready
// contract.
func readyNotSetAfterObserve(mg resource.Managed) bool {
	return mg.GetCondition(prv1.TypeReady).Reason == prv1.ReasonNotObserved
}
