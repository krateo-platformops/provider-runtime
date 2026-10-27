package v1

import (
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A ConditionType represents a condition a resource could be in.
type ConditionType string

// Condition types.
const (
	// TypeReady resources are believed to be ready to handle work.
	TypeReady ConditionType = "Ready"

	// TypeSynced resources are believed to be in sync with the
	// Kubernetes resources that manage their lifecycle.
	TypeSynced ConditionType = "Synced"
)

// A ConditionReason represents the reason a resource is in a condition.
type ConditionReason string

// Reasons a resource is or is not ready.
const (
	ReasonAvailable   ConditionReason = "Available"
	ReasonUnavailable ConditionReason = "Unavailable"
	ReasonCreating    ConditionReason = "Creating"
	ReasonDeleting    ConditionReason = "Deleting"
	// ReasonZeroEvidence: readiness could not be assessed AT ALL — every signal the controller
	// knows how to read was absent, not negative. See ZeroEvidence.
	ReasonZeroEvidence ConditionReason = "ZeroEvidence"
)

// Reasons a resource is or is not synced.
const (
	ReasonReconcileSuccess ConditionReason = "ReconcileSuccess"
	ReasonReconcileError   ConditionReason = "ReconcileError"
	ReasonReconcilePaused  ConditionReason = "ReconcilePaused"
)

// A Condition that may apply to a resource.
type Condition struct {
	// Type of this condition. At most one of each condition type may apply to
	// a resource at any point in time.
	Type ConditionType `json:"type"`

	// Status of this condition; is it currently True, False, or Unknown?
	Status metav1.ConditionStatus `json:"status"`

	// LastTransitionTime is the last time this condition transitioned from one
	// status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// A Reason for this condition's last transition from one status to another.
	Reason ConditionReason `json:"reason"`

	// A Message containing details about this condition's last transition from
	// one status to another, if any.
	// +optional
	Message string `json:"message,omitempty"`
}

// Equal returns true if the condition is identical to the supplied condition,
// ignoring the LastTransitionTime.
func (c Condition) Equal(other Condition) bool {
	return c.Type == other.Type &&
		c.Status == other.Status &&
		c.Reason == other.Reason &&
		c.Message == other.Message
}

// WithMessage returns a condition by adding the provided message to existing
// condition.
func (c Condition) WithMessage(msg string) Condition {
	c.Message = msg
	return c
}

// Conditions are implemented as a slice rather than a map to comply
// with Kubernetes API conventions. Ideally we'd comply by using a map that
// marshalled to a JSON array, but doing so confuses the CRD schema generator.
// https://github.com/kubernetes/community/blob/9bf8cd/contributors/devel/sig-architecture/api-conventions.md#lists-of-named-subobjects-preferred-over-maps

// Do not manipulate Conditions directly. Use the Set method.

// A ConditionedStatus reflects the observed status of a resource. Only
// one condition of each type may exist.
type ConditionedStatus struct {
	// Conditions of the resource.
	// +optional
	Conditions []Condition `json:"conditions,omitempty"`
}

// NewConditionedStatus returns a stat with the supplied conditions set.
func NewConditionedStatus(c ...Condition) *ConditionedStatus {
	s := &ConditionedStatus{}
	s.SetConditions(c...)
	return s
}

// GetCondition returns the condition for the given ConditionType if exists,
// otherwise returns nil
func (s *ConditionedStatus) GetCondition(ct ConditionType) Condition {
	for _, c := range s.Conditions {
		if c.Type == ct {
			return c
		}
	}

	return Condition{Type: ct, Status: metav1.ConditionUnknown}
}

// SetConditions sets the supplied conditions, replacing any existing conditions
// of the same type. This is a no-op if all supplied conditions are identical,
// ignoring the last transition time, to those already set.
func (s *ConditionedStatus) SetConditions(c ...Condition) {
	for _, new := range c {
		exists := false
		for i, existing := range s.Conditions {
			if existing.Type != new.Type {
				continue
			}

			if existing.Equal(new) {
				exists = true
				continue
			}

			s.Conditions[i] = new
			exists = true
		}
		if !exists {
			s.Conditions = append(s.Conditions, new)
		}
	}
}

// Equal returns true if the status is identical to the supplied status,
// ignoring the LastTransitionTimes and order of statuses.
func (s *ConditionedStatus) Equal(other *ConditionedStatus) bool {
	if s == nil || other == nil {
		return s == nil && other == nil
	}

	if len(other.Conditions) != len(s.Conditions) {
		return false
	}

	sc := make([]Condition, len(s.Conditions))
	copy(sc, s.Conditions)

	oc := make([]Condition, len(other.Conditions))
	copy(oc, other.Conditions)

	// We should not have more than one condition of each type.
	sort.Slice(sc, func(i, j int) bool { return sc[i].Type < sc[j].Type })
	sort.Slice(oc, func(i, j int) bool { return oc[i].Type < oc[j].Type })

	for i := range sc {
		if !sc[i].Equal(oc[i]) {
			return false
		}
	}

	return true
}

// Creating returns a condition that indicates the resource is currently
// being created.
func Creating() Condition {
	return Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonCreating,
	}
}

// Deleting returns a condition that indicates the resource is currently
// being deleted.
func Deleting() Condition {
	return Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonDeleting,
	}
}

// Available returns a condition that indicates the resource is
// currently observed to be available for use.
func Available() Condition {
	return Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonAvailable,
	}
}

// ZeroEvidence returns a condition that indicates readiness could not be assessed at all: every
// signal the controller knows how to read was ABSENT, so neither True nor False is supportable
// and the status is Unknown.
//
// It is NOT "unhealthy" and it is NOT a milder Unavailable. Unavailable means the resource was
// observed and found sick; this means it was not observed. The distinction exists because the two
// call for opposite handling: a consumer gating on readiness must wait on Unavailable, while
// waiting on ZeroEvidence would wait forever — absence of evidence is usually a steady state, not
// a transient one.
//
// That is the trap it is built to avoid. Krateo's composition child-health rollup counts children
// whose kind it cannot interpret and, when EVERY child is uninterpretable, has been reporting
// Available over nothing at all (krateo-platformops/core-provider#165). Reporting False instead
// would be just as untrue and far more damaging: consumers that gate on readiness would block
// indefinitely on resources that are, as far as anyone knows, fine.
//
// So the reason is the contract, not the status. A consumer that can distinguish this case may
// treat it as passable; one that cannot must keep testing for True and is no worse off than
// before. Consumers MUST key on the reason, never on the message text, which is prose and will
// change.
func ZeroEvidence() Condition {
	return Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionUnknown,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonZeroEvidence,
	}
}

// Unavailable returns a condition that indicates the resource is not
// currently available for use. Unavailable should be set only when Crossplane
// expects the resource to be available but knows it is not, for example
// because its API reports it is unhealthy.
func Unavailable() Condition {
	return Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonUnavailable,
	}
}

// ReconcileSuccess returns a condition indicating that the provider successfully
// completed the most recent reconciliation of the resource.
func ReconcileSuccess() Condition {
	return Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileSuccess,
	}
}

// ReconcileError returns a condition indicating that the provider encountered an
// error while reconciling the resource. This could mean that the provider was
// unable to update the resource to reflect its desired state, or that
// was unable to determine the current actual state of the resource.
func ReconcileError(err error) Condition {
	return Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileError,
		Message:            err.Error(),
	}
}

// ReconcilePaused returns a condition that indicates reconciliation on
// the managed resource is paused via the pause annotation.
func ReconcilePaused() Condition {
	return Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcilePaused,
	}
}
