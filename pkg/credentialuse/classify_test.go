package credentialuse

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// These errors are built the way the API server builds them, with the same
// apimachinery constructors. premises_envtest_test.go produces each one for
// real and checks the classes again.

var secrets = schema.GroupResource{Resource: "secrets"}

// deletingError is ValidateNoNewFinalizers' error, as rest.BeforeUpdate
// returns it (k8s.io/apiserver@v0.37.2 pkg/registry/rest/update.go:157).
func deletingError() error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Secret"}, "s", field.ErrorList{
		field.Forbidden(field.NewPath("metadata", "finalizers"), `no new finalizers can be added if the object is being deleted, found new finalizers []string{"credential-use.repo.git.krateo.io/ns"}`),
	})
}

// policyError is a ValidatingAdmissionPolicy's denial as the dispatcher
// builds it (dispatcher.go:300-317): admission.NewForbidden, then the reason
// (Invalid when the validation sets none), the matching code, and a cause
// with the bare message.
func policyError(reason metav1.StatusReason, inner string) error {
	msg := "ValidatingAdmissionPolicy 'krateo-credential-use-namespace' with binding 'krateo-credential-use-namespace' denied request: " + inner
	err := apierrors.NewForbidden(secrets, "s", errors.New(msg))
	if reason == "" {
		reason = metav1.StatusReasonInvalid
	}
	err.ErrStatus.Reason = reason
	if reason == metav1.StatusReasonInvalid {
		err.ErrStatus.Code = http.StatusUnprocessableEntity
	}
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{Message: msg})
	return err
}

// rbacError is the authorizer's denial (responsewriters/errors.go:44-80).
func rbacError(scope, reason string) error {
	msg := `User "system:serviceaccount:krateo-system:git-provider" cannot patch resource "secrets" in API group ""` + scope
	if reason != "" {
		msg += ": " + reason
	}
	return apierrors.NewForbidden(secrets, "s", errors.New(msg))
}

// testFailedError is a failed JSON-patch test op (patch.go:423-426).
func testFailedError() error {
	return apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", "testing value /metadata/annotations/x failed: test failed", 0, false)
}

func TestClassify(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Class
	}{
		"nil":                                 {nil, ClassNone},
		"not an API error":                    {errors.New("connection refused"), ClassOther},
		"(a) deleting":                        {deletingError(), ClassDeleting},
		"(a) deleting, wrapped":               {fmt.Errorf("A2: %w", deletingError()), ClassDeleting},
		"(b) policy denial, reason Forbidden": {policyError(metav1.StatusReasonForbidden, "only cu-index-* ConfigMaps …"), ClassPolicyDenied},
		"(b) policy denial, no reason, 422":   {policyError("", "denied"), ClassPolicyDenied},
		"(c) missing params, 422":             {policyError("", "failed to configure binding: no params found for policy binding with `Deny` parameterNotFoundAction"), ClassPolicyConfig},
		"(c) policy config error":             {policyError("", "failed to configure policy: x"), ClassPolicyConfig},
		"(d) RBAC, namespaced":                {rbacError(` in the namespace "team-a"`, ""), ClassRBAC},
		"(d) RBAC, cluster scope":             {rbacError(` at the cluster scope`, ""), ClassRBAC},
		"(d) RBAC, with a reason":             {rbacError(` in the namespace "team-a"`, `RBAC: clusterrole.rbac.authorization.k8s.io "x" not found`), ClassRBAC},
		"(d) RBAC, a path":                    {apierrors.NewForbidden(schema.GroupResource{}, "", errors.New(`User "u" cannot get path "/apis"`)), ClassRBAC},
		// The pattern ends at the authorizer's text, or at ": <reason>": a
		// message that only starts like it isn't RBAC.
		"RBAC-like text with more after it": {apierrors.NewForbidden(secrets, "s", errors.New(`User "u" cannot patch resource "secrets" in API group "" in the namespace "a" because a webhook said so`)), ClassAdmissionDenied},
		"(e) NamespaceLifecycle":            {apierrors.NewForbidden(secrets, "s", errors.New("unable to create new content in namespace rlgov-probe because it is being terminated")), ClassAdmissionDenied},
		"(e) a webhook":                     {apierrors.NewForbidden(secrets, "s", errors.New(`admission webhook "v.example.com" denied the request: no`)), ClassAdmissionDenied},
		// A 403 is never Deleting, whatever its causes (§6.2).
		"a 403 with a finalizers cause": {func() error {
			e := apierrors.NewForbidden(secrets, "s", errors.New("x"))
			e.ErrStatus.Details.Causes = []metav1.StatusCause{{Type: metav1.CauseTypeForbidden, Field: "metadata.finalizers"}}
			return e
		}(), ClassAdmissionDenied},
		// A 422 without the finalizers cause is never Deleting.
		"a 422 on another field":          {apierrors.NewInvalid(schema.GroupKind{Kind: "Secret"}, "s", field.ErrorList{field.Forbidden(field.NewPath("metadata", "labels"), "x")}), ClassOther},
		"a 422 with an Invalid finalizer": {apierrors.NewInvalid(schema.GroupKind{Kind: "Secret"}, "s", field.ErrorList{field.Invalid(field.NewPath("metadata", "finalizers"), "x", "bad")}), ClassOther},
		"a failed JSON-patch test":        {testFailedError(), ClassOther},
		"not found":                       {apierrors.NewNotFound(secrets, "s"), ClassNotFound},
		"conflict":                        {apierrors.NewConflict(secrets, "s", errors.New("the object has been modified")), ClassConflict},
		"already exists":                  {apierrors.NewAlreadyExists(secrets, "s"), ClassAlreadyExists},
		"too many requests":               {apierrors.NewTooManyRequests("slow down", 1), ClassOther},
		"internal error":                  {apierrors.NewInternalError(errors.New("etcd")), ClassOther},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyJSONPatch(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Class
	}{
		"nil":                   {nil, ClassNone},
		"a failed test op":      {testFailedError(), ClassStateChanged},
		"wrapped":               {fmt.Errorf("R3: %w", testFailedError()), ClassStateChanged},
		"a policy config error": {policyError("", "failed to configure binding: no params found"), ClassPolicyConfig},
		"a policy denial, 422":  {policyError("", "denied"), ClassPolicyDenied},
		"RBAC":                  {rbacError(` in the namespace "a"`, ""), ClassRBAC},
		"not found":             {apierrors.NewNotFound(secrets, "s"), ClassNotFound},
		"conflict":              {apierrors.NewConflict(secrets, "s", errors.New("x")), ClassConflict},
		// A 422 with causes isn't a failed test op.
		"a 422 with causes": {apierrors.NewInvalid(schema.GroupKind{Kind: "Secret"}, "s", field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), "x", "bad")}), ClassOther},
		// Removing finalizers can't be the Deleting outcome.
		"a deleting-shaped 422": {deletingError(), ClassOther},
		"not an API error":      {errors.New("x"), ClassOther},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ClassifyJSONPatch(tc.err); got != tc.want {
				t.Errorf("ClassifyJSONPatch(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassString(t *testing.T) {
	for c := ClassNone; c <= ClassStateChanged; c++ {
		if s := c.String(); s == "" || s == "Class(?)" {
			t.Errorf("Class %d has no name", c)
		}
	}
}
