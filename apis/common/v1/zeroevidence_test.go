package v1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZeroEvidence must be Unknown, not False. That is the entire point of it existing: "I could not
// assess this" is a third answer, and collapsing it into either of the other two is what the
// condition is for avoiding.
//
// False would be the damaging collapse. A consumer gating on readiness waits on False, and
// absence of evidence is usually a steady state rather than a transient one, so it would wait
// forever on a resource that is, as far as anyone knows, fine.
func TestZeroEvidence_IsUnknownAndNotFalse(t *testing.T) {
	c := ZeroEvidence()

	if c.Type != TypeReady {
		t.Errorf("Type = %q, want %q", c.Type, TypeReady)
	}
	if c.Status != metav1.ConditionUnknown {
		t.Errorf("Status = %q, want %q — Unknown is the claim; False asserts unhealth we did not observe",
			c.Status, metav1.ConditionUnknown)
	}
	if c.Status == metav1.ConditionFalse {
		t.Error("ZeroEvidence reports False: every consumer gating on readiness would block indefinitely")
	}
	if c.Reason != ReasonZeroEvidence {
		t.Errorf("Reason = %q, want %q — consumers key on the reason, so it is the contract",
			c.Reason, ReasonZeroEvidence)
	}
}

// It must be distinguishable from every other Ready condition, because a consumer's whole ability
// to special-case it rests on telling them apart. Equal() ignores LastTransitionTime, so this
// compares what a consumer actually sees.
func TestZeroEvidence_IsDistinctFromEveryOtherReadyCondition(t *testing.T) {
	z := ZeroEvidence()

	for _, other := range []struct {
		name string
		cond Condition
	}{
		{"Available", Available()},
		{"Unavailable", Unavailable()},
		{"Creating", Creating()},
		{"Deleting", Deleting()},
	} {
		if z.Equal(other.cond) {
			t.Errorf("ZeroEvidence is indistinguishable from %s; a consumer cannot special-case it", other.name)
		}
	}

	// The one it must differ from most sharply. Unavailable means observed-and-sick; this means
	// not observed. They call for opposite handling, so sharing a status would be the subtler bug.
	if z.Status == Unavailable().Status {
		t.Error("ZeroEvidence shares Unavailable's status: 'not observed' and 'observed sick' would " +
			"be the same signal to anything that reads status without reading reason")
	}

	// Control arm: Equal() must be capable of returning true here, or the loop above proves
	// nothing. A constructor compared against itself is the same shape with a known answer.
	if !z.Equal(ZeroEvidence()) {
		t.Fatal("control arm: ZeroEvidence() does not equal itself, so the inequalities above are vacuous")
	}
}
