package reconciler

import (
	"fmt"
	"strings"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// The management policy's reports on the managed object's conditions
// (unstructured-runtime#110: D2, D4; the user's decisions of 2026-11-21).
// unstructured-runtime reports the same reasons, with the same precedence:
// its Synced funnel is unstructured-runtime#117's policySynced.

// messageUpdateWithheld is appended to an unhealthy Ready whose drift the
// policy withholds: the health verdict keeps its own reason.
const messageUpdateWithheld = "updates are withheld by policy"

// policySynced replaces the pass's Synced outcome with the policy report when
// an annotation doesn't parse. writeStatus calls it before every status write.
//
//   - krateo.io/management-policy invalid: Synced=False/ManagementPolicyInvalid,
//     whatever the pass concluded (ReconcileSuccess or ReconcileError). The
//     pass's error is still returned, so the backoff is unchanged, and its
//     events still fire; only the condition changes. When the deletion policy
//     is invalid too, the message says so.
//   - else krateo.io/deletion-policy unrecognised:
//     Synced=False/DeletionPolicyInvalid, on every pass while it is set, so the
//     typo is visible before the deletion it decides.
//
// ReconcilePaused and ReconcileDeferred are left alone: no pass ran, and they
// say why. Ready is never touched.
func policySynced(managed resource.Managed) {
	c := managed.GetCondition(prv1.TypeSynced)
	if c.Reason != prv1.ReasonReconcileSuccess && c.Reason != prv1.ReasonReconcileError {
		return
	}
	d := meta.ManagementDecision(managed)
	switch {
	case d.Policy.Invalid != "":
		msg := fmt.Sprintf("%s %s; acting on nothing (observe only)", managementpolicy.AnnotationKey, d.Policy.Invalid)
		if d.DeletionInvalid != "" {
			msg += "; also, " + d.DeletionInvalid
		}
		managed.SetConditions(prv1.ManagementPolicyInvalid(msg))
	case d.DeletionInvalid != "":
		managed.SetConditions(prv1.DeletionPolicyInvalid(d.DeletionInvalid))
	}
}

// createWithheldMessage is the message of CreateWithheldByPolicy.
func createWithheldMessage(p managementpolicy.Policy) string {
	return fmt.Sprintf("the external resource does not exist, and %s %q withholds create", managementpolicy.AnnotationKey, p.String())
}

// updateWithheldReady is Ready for a resource that differs from the desired
// state when the policy withholds update. A healthy Ready (True) becomes
// UpdateWithheldByPolicy: the resource works, but its drift isn't corrected.
// Any other Ready keeps its status and reason, the provider's health verdict,
// with "updates are withheld by policy" added to its message.
func updateWithheldReady(ready prv1.Condition, p managementpolicy.Policy) prv1.Condition {
	if ready.Status == metav1.ConditionTrue {
		return prv1.UpdateWithheldByPolicy(fmt.Sprintf("the external resource differs from the desired state, and %s %q withholds update", managementpolicy.AnnotationKey, p.String()))
	}
	// A Ready that Observe didn't rewrite this pass may carry the note
	// already: add it once.
	switch {
	case ready.Message == "":
		ready.Message = messageUpdateWithheld
	case !strings.HasSuffix(ready.Message, messageUpdateWithheld):
		ready.Message += "; " + messageUpdateWithheld
	}
	return ready
}
