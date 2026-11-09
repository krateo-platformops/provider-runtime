package credentialuse

import (
	"errors"
	"net/http"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A Class is what an API error means to the protocol (§6.2). It is read from
// the status's code, reason, message and causes, never from IsForbidden or
// IsInvalid alone, because two different 422s and three different 403s
// exist.
type Class int

const (
	// ClassNone is a nil error.
	ClassNone Class = iota
	// ClassOther is any error none of the classes below describes,
	// including errors that carry no API status.
	ClassOther
	// ClassDeleting, class (a): 422 Invalid with a FieldValueForbidden cause
	// on metadata.finalizers, whose message is ValidateNoNewFinalizers'. P is being deleted and lacks the finalizer, so
	// no new finalizer can be added. The only class that is the Deleting
	// outcome.
	ClassDeleting
	// ClassPolicyDenied, class (b): a ValidatingAdmissionPolicy refused the
	// write. A 403 when the validation sets reason Forbidden, as ours does
	// (§10.6.1), and a 422 when it sets none.
	ClassPolicyDenied
	// ClassPolicyConfig, class (c): a policy or its binding couldn't be
	// configured, for example a missing parameter object under
	// parameterNotFoundAction Deny. Always a 422, and never Deleting.
	ClassPolicyConfig
	// ClassRBAC, class (d): the authorizer refused the request. Recognised
	// by the authorizer's own message (IP1). The only class PerNamePoll
	// retries as RBAC propagation (§10.7.2).
	ClassRBAC
	// ClassAdmissionDenied, class (e): any other 403, such as a webhook
	// denial, NamespaceLifecycle refusing a create in a terminating
	// namespace, or a 403 whose reason isn't Forbidden. An Acquire error at
	// once (IP1), and counted, so an RBAC message the pattern stops matching
	// shows up (L3 of the review of revision 16).
	ClassAdmissionDenied
	// ClassNotFound: the object doesn't exist (the Missing outcome of A2).
	ClassNotFound
	// ClassConflict: a resourceVersion precondition failed.
	ClassConflict
	// ClassAlreadyExists: a create found the object there already.
	ClassAlreadyExists
	// ClassStateChanged: a JSON patch's test op failed, or one of its ops
	// didn't apply, because the object changed since it was read. Returned
	// only by ClassifyJSONPatch (§6.2, "JSON patches are classified apart").
	ClassStateChanged
)

var classNames = map[Class]string{
	ClassNone:            "None",
	ClassOther:           "Other",
	ClassDeleting:        "Deleting",
	ClassPolicyDenied:    "PolicyDenied",
	ClassPolicyConfig:    "PolicyConfig",
	ClassRBAC:            "RBAC",
	ClassAdmissionDenied: "AdmissionDenied",
	ClassNotFound:        "NotFound",
	ClassConflict:        "Conflict",
	ClassAlreadyExists:   "AlreadyExists",
	ClassStateChanged:    "StateChanged",
}

func (c Class) String() string {
	if n, ok := classNames[c]; ok {
		return n
	}
	return "Class(?)"
}

// policyText starts a ValidatingAdmissionPolicy's denial. The dispatcher
// appends it as a cause with no type and no field, and wraps it in the status
// message as `<resource> "<name>" is forbidden: ValidatingAdmissionPolicy '…`
// (k8s.io/apiserver@v0.37.2 pkg/admission/plugin/policy/validating/dispatcher.go:300-317).
const policyText = "ValidatingAdmissionPolicy '"

// policyConfigTexts mark a policy that couldn't be configured
// (dispatcher.go:75-100).
var policyConfigTexts = []string{"failed to configure binding:", "failed to configure policy:"}

// rbacMessage matches the authorizer's own denial, as the API server writes
// it into a Forbidden status: `<resource> "<name>" is forbidden: User "<u>"
// cannot <verb> resource "<r>" in API group "<g>" in the namespace "<ns>"`, or
// `at the cluster scope`, or `cannot <verb> path "<p>"`, optionally followed
// by `: <reason>` (k8s.io/apiserver@v0.37.2
// pkg/endpoints/handlers/responsewriters/errors.go:44-80). Every quoted part
// is written with %q, so it may hold escaped quotes and backslashes.
var rbacMessage = regexp.MustCompile(`(?:^|forbidden: )User ` + quoted + ` cannot [^ ]+ (?:resource ` + quoted + ` in API group ` + quoted + ` (?:in the namespace ` + quoted + `|at the cluster scope)|path ` + quoted + `)(?::|$)`)

// quoted matches a Go %q string.
const quoted = `"(?:[^"\\]|\\.)*"`

// noNewFinalizers starts the cause ValidateNoNewFinalizers gives (k8s.io/
// apimachinery@v0.37.2 pkg/api/validation/objectmeta.go:131): a field.Error's
// body is its type, then its detail.
const noNewFinalizers = "Forbidden: no new finalizers can be added if the object is being deleted"

// genericInvalid is the message NewGenericServerResponse gives a 422, which a
// failed JSON-patch op gets (k8s.io/apimachinery@v0.37.2
// pkg/api/errors/errors.go:476-478).
const genericInvalid = "the server rejected our request due to an error in our request"

// Classify returns the class of an error from an acquiring write (A2), an
// index write (§10.6) or a live read in the consent gate (§6.2). Errors from
// JSON patches (R3, Disown's strips, the label janitor) go through
// ClassifyJSONPatch instead.
func Classify(err error) Class {
	if err == nil {
		return ClassNone
	}
	st, ok := statusOf(err)
	if !ok {
		return ClassOther
	}
	switch st.Code {
	case http.StatusNotFound:
		return ClassNotFound
	case http.StatusConflict:
		if st.Reason == metav1.StatusReasonAlreadyExists {
			return ClassAlreadyExists
		}
		return ClassConflict
	case http.StatusUnprocessableEntity:
		if st.Reason != metav1.StatusReasonInvalid {
			return ClassOther
		}
		policy, config := policyOf(st)
		switch {
		case config:
			return ClassPolicyConfig
		case policy:
			return ClassPolicyDenied
		case hasFinalizersForbiddenCause(st):
			return ClassDeleting
		}
		return ClassOther
	case http.StatusForbidden:
		if st.Reason != metav1.StatusReasonForbidden {
			// A 403 the authorizer didn't write: an admission denial.
			return ClassAdmissionDenied
		}
		policy, config := policyOf(st)
		switch {
		case config:
			return ClassPolicyConfig
		case policy:
			return ClassPolicyDenied
		case rbacMessage.MatchString(st.Message):
			return ClassRBAC
		}
		return ClassAdmissionDenied
	}
	return ClassOther
}

// ClassifyJSONPatch returns the class of an error from a JSON patch with test
// ops: R3, Disown's strips and the label janitor's patch.
//
// A failed test op is a 422 Invalid with the generic message and no causes:
// the patch handler builds it with NewGenericServerResponse, which drops
// json-patch's own text (k8s.io/apiserver@v0.37.2
// pkg/endpoints/handlers/patch.go:423-426). A remove at an index that has
// moved looks the same. Both mean the object changed since it was read, so
// such a 422, with exactly that message, is ClassStateChanged; any other
// causeless error is ClassOther. A 422 that carries a policy's text is
// ClassPolicyDenied or ClassPolicyConfig, and is never read as a state change.
// The Deleting outcome doesn't apply to a patch that only removes finalizers,
// so ClassDeleting is never returned.
func ClassifyJSONPatch(err error) Class {
	c := Classify(err)
	if c != ClassOther && c != ClassDeleting {
		return c
	}
	st, ok := statusOf(err)
	if !ok {
		return ClassOther
	}
	if st.Code == http.StatusUnprocessableEntity && st.Reason == metav1.StatusReasonInvalid && !hasCauses(st) && st.Message == genericInvalid {
		return ClassStateChanged
	}
	return ClassOther
}

// statusOf returns the API status err carries, if any, through wrapping.
func statusOf(err error) (metav1.Status, bool) {
	var s apierrors.APIStatus
	if !errors.As(err, &s) {
		return metav1.Status{}, false
	}
	return s.Status(), true
}

// policyOf reports whether a status carries a ValidatingAdmissionPolicy's
// text, in a cause or wrapped in the message, and whether that text is a
// configuration error.
func policyOf(st metav1.Status) (policy, config bool) {
	var texts []string
	if st.Details != nil {
		for _, c := range st.Details.Causes {
			if strings.HasPrefix(c.Message, policyText) {
				texts = append(texts, c.Message)
			}
		}
	}
	if i := strings.Index(st.Message, "forbidden: "+policyText); i >= 0 {
		texts = append(texts, st.Message[i+len("forbidden: "):])
	}
	for _, t := range texts {
		policy = true
		for _, ct := range policyConfigTexts {
			if strings.Contains(t, ct) {
				config = true
			}
		}
	}
	return policy, config
}

func hasFinalizersForbiddenCause(st metav1.Status) bool {
	if st.Details == nil {
		return false
	}
	for _, c := range st.Details.Causes {
		if c.Type == metav1.CauseTypeForbidden && c.Field == "metadata.finalizers" && strings.HasPrefix(c.Message, noNewFinalizers) {
			return true
		}
	}
	return false
}

func hasCauses(st metav1.Status) bool {
	return st.Details != nil && len(st.Details.Causes) > 0
}
