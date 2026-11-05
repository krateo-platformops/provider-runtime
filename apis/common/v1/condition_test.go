package v1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The reason string is a consumer contract. Its consumer is
// unstructured-runtime, whose pkg/tools/unstructured/condition declares
// ReasonReconcileDeferred = "ReconcileDeferred" with its own copy of this test
// (TestReconcileDeferredReasonIsDistinct). The two runtimes duplicate the
// literal on purpose rather than share a module (design §10, open question 1),
// so this test, not the compiler, is what keeps them saying the same word for
// one state. Anything that reads Synced's reason by equality, such as a
// dependency gate or a dashboard, is a consumer too.
//
// Changing the value here alone splits the vocabulary: a controller on one
// runtime and a controller on the other, both deferring against the same
// GitHub Enterprise, would report the state under two names.
func TestReasonReconcileDeferredIsAConsumerContract(t *testing.T) {
	const want = "ReconcileDeferred"
	if ReasonReconcileDeferred != want {
		t.Fatalf("ReasonReconcileDeferred = %q, want %q. unstructured-runtime's "+
			"condition.ReasonReconcileDeferred and every reader matching Synced's reason by "+
			"equality depend on this exact string", ReasonReconcileDeferred, want)
	}

	// It must also be distinct from every other reason, or a reader can't tell
	// a deferral from the state it collides with. The keys are constants, so
	// the compiler already rejects two existing reasons sharing a value; a new
	// reason belongs in this list.
	others := map[ConditionReason]string{
		ReasonAvailable:        "ReasonAvailable",
		ReasonUnavailable:      "ReasonUnavailable",
		ReasonCreating:         "ReasonCreating",
		ReasonDeleting:         "ReasonDeleting",
		ReasonNotObserved:      "ReasonNotObserved",
		ReasonReconcileSuccess: "ReasonReconcileSuccess",
		ReasonReconcileError:   "ReasonReconcileError",
		ReasonReconcilePaused:  "ReasonReconcilePaused",
	}
	if name, clash := others[ReasonReconcileDeferred]; clash {
		t.Errorf("ReconcileDeferred's value %q is also %s's", ReasonReconcileDeferred, name)
	}
}

func TestReconcileDeferredIsSyncedFalseAndLeavesReadyAlone(t *testing.T) {
	c := ReconcileDeferred("reconciliation deferred: api.github.com is rate-limited by the server; next attempt 2026-11-05T10:00:31Z")
	if c.Type != TypeSynced || c.Status != metav1.ConditionFalse || c.Reason != ReasonReconcileDeferred {
		t.Errorf("ReconcileDeferred() = %+v, want Synced=False/ReconcileDeferred", c)
	}

	s := NewConditionedStatus(Available())
	before := s.GetCondition(TypeReady)
	s.SetConditions(c)
	if after := s.GetCondition(TypeReady); after != before {
		t.Errorf("setting ReconcileDeferred changed Ready from %+v to %+v", before, after)
	}
}
