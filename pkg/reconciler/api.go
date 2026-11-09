package reconciler

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// Error strings.
const (
	errCreateOrUpdateSecret      = "cannot create or update connection secret"
	errUpdateManaged             = "cannot update managed resource"
	errUpdateManagedStatus       = "cannot update managed resource status"
	errResolveReferences         = "cannot resolve references"
	errUpdateCriticalAnnotations = "cannot update critical annotations"
)

// criticalAnnotationKeys are the annotations the runtime itself owns on a
// managed resource, and the only ones RetryingCriticalAnnotationUpdater writes
// back. Writing back any other key would revert a value another writer set
// after the object was read, such as a fresh krateo.io/traceparent
// (docs/design-tracing.md §6.2, "The critical write").
var criticalAnnotationKeys = []string{
	meta.AnnotationKeyExternalName,
	meta.AnnotationKeyExternalCreatePending,
	meta.AnnotationKeyExternalCreateSucceeded,
	meta.AnnotationKeyExternalCreateFailed,
	meta.AnnotationKeyTraceparentContinued,
}

// A RetryingCriticalAnnotationUpdater is a CriticalAnnotationUpdater that
// retries annotation updates in the face of API server errors.
type RetryingCriticalAnnotationUpdater struct {
	client client.Client
}

// NewRetryingCriticalAnnotationUpdater returns a CriticalAnnotationUpdater that
// retries annotation updates in the face of API server errors.
func NewRetryingCriticalAnnotationUpdater(c client.Client) *RetryingCriticalAnnotationUpdater {
	return &RetryingCriticalAnnotationUpdater{client: c}
}

// UpdateCriticalAnnotations persists the runtime's own annotations of the
// supplied Object: krateo.io/external-name, krateo.io/external-create-pending,
// krateo.io/external-create-succeeded, krateo.io/external-create-failed and
// krateo.io/traceparent-continued, those of them it carries. It re-reads the
// object and writes those keys back onto what it read, retrying in the face of
// any API server error several times, in order to ensure annotations that
// contain critical state are persisted.
//
// Every other annotation is left as the API server has it, so a value another
// writer set after the object was read is never reverted. An annotation a
// provider set on the object in memory, other than the keys above, is not
// persisted here. Any pending changes to the supplied Object's spec, status,
// or other metadata are reset to their current state according to the API
// server.
func (u *RetryingCriticalAnnotationUpdater) UpdateCriticalAnnotations(ctx context.Context, o client.Object) error {
	a := criticalAnnotations(o)
	err := retry.OnError(retry.DefaultRetry, resource.IsAPIError, func() error {
		nn := types.NamespacedName{Name: o.GetName(), Namespace: o.GetNamespace()}
		if err := u.client.Get(ctx, nn, o); err != nil {
			return err
		}
		meta.AddAnnotations(o, a)
		return u.client.Update(ctx, o)
	})
	return errors.Wrap(err, errUpdateCriticalAnnotations)
}

// criticalAnnotations returns the runtime's own annotations o carries.
func criticalAnnotations(o client.Object) map[string]string {
	all := o.GetAnnotations()
	out := make(map[string]string, len(criticalAnnotationKeys))
	for _, k := range criticalAnnotationKeys {
		if v, ok := all[k]; ok {
			out[k] = v
		}
	}
	return out
}
