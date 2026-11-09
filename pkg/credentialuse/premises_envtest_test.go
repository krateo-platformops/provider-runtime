package credentialuse_test

// Premise tests: they pin, against a real API server, the API facts the
// protocol rests on (§2, and §14 "Error classification"). The in-memory model
// of the next PR encodes the same facts, and is checked against these.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

var repoGK = schema.GroupKind{Group: "git.krateo.io", Kind: "Repo"}

const other = "example.com/other"

func ctxFor(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func createSecret(t *testing.T, ns, name string, finalizers ...string) {
	t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Finalizers: finalizers, Labels: map[string]string{"app": "x"}}}
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(context.Background(), s, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating secret %s/%s: %v", ns, name, err)
	}
}

func createTarget(t *testing.T, ns, name string, finalizers ...string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(cutest.TargetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetFinalizers(finalizers)
	out, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating target %s/%s: %v", ns, name, err)
	}
	return out
}

func secrets(m metadata.Interface, ns string) metadata.ResourceInterface {
	return m.Resource(cutest.SecretGVR).Namespace(ns)
}

func send(ctx context.Context, r metadata.ResourceInterface, name string, p cu.Patch) (*metav1.PartialObjectMetadata, error) {
	return r.Patch(ctx, name, p.Type, p.Data, metav1.PatchOptions{})
}

func acquire(t *testing.T, ns, holderNS, name string) (*metav1.PartialObjectMetadata, string) {
	t.Helper()
	epoch, err := cu.NewEpoch()
	if err != nil {
		t.Fatal(err)
	}
	p, err := cu.AcquireStrategicPatch(repoGK, holderNS, epoch)
	if err != nil {
		t.Fatal(err)
	}
	got, err := send(ctxFor(t), secrets(env.Metadata, ns), name, p)
	if err != nil {
		t.Fatalf("A2 on %s/%s: %v", ns, name, err)
	}
	return got, epoch
}

func mustFinalizer(t *testing.T, ns string) string {
	t.Helper()
	f, err := cu.FinalizerName(repoGK, ns)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func statusOf(t *testing.T, err error) metav1.Status {
	t.Helper()
	var s apierrors.APIStatus
	if !errors.As(err, &s) {
		t.Fatalf("%v carries no API status", err)
	}
	return s.Status()
}

func sorted(s []string) []string {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}

// P1: a strategic merge patch on a Secret, through the metadata client,
// merges finalizers as a set and keeps everyone else's, and the response
// decodes as metadata.
func TestPremiseStrategicMergeOnASecret(t *testing.T) {
	cutest.Require(t, env)
	ns := env.Namespace(t, "smp")
	createSecret(t, ns, "s", other)

	got, epoch := acquire(t, ns, ns, "s")
	f := mustFinalizer(t, ns)
	if want := sorted([]string{other, f}); !slices.Equal(sorted(got.Finalizers), want) {
		t.Errorf("finalizers = %v, want %v", got.Finalizers, want)
	}
	if got.Annotations[f] != epoch {
		t.Errorf("epoch = %q, want %q", got.Annotations[f], epoch)
	}
	if got.Labels["app"] != "x" || got.Labels[cu.LabelHeld] != "true" || got.Labels[cu.MarkerLabel(repoGK)] != "true" {
		t.Errorf("labels = %v", got.Labels)
	}
	if got.Name != "s" || got.UID == "" {
		t.Errorf("the response didn't decode as the object's metadata: %+v", got.ObjectMeta)
	}

	// A second holder namespace of the same kind adds its own entry.
	got2, _ := acquire(t, ns, "team-b", "s")
	if want := sorted([]string{other, f, mustFinalizer(t, "team-b")}); !slices.Equal(sorted(got2.Finalizers), want) {
		t.Errorf("finalizers = %v, want %v", got2.Finalizers, want)
	}
	// And the same one again adds nothing.
	got3, _ := acquire(t, ns, ns, "s")
	if len(got3.Finalizers) != 3 {
		t.Errorf("a repeated A2 duplicated a finalizer: %v", got3.Finalizers)
	}
}

// P2: a merge patch replaces a custom resource's finalizer list, custom
// resources refuse strategic merge patches, and a stale resourceVersion in a
// merge patch is a conflict. That is why A2 on a custom resource is a
// guarded merge carrying the whole list (§6.2).
func TestPremiseMergePatchOnACustomResource(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "merge")
	f := mustFinalizer(t, ns)
	targets := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns)

	createTarget(t, ns, "replaced", other)
	out, err := targets.Patch(ctx, "replaced", types.MergePatchType, fmt.Appendf(nil, `{"metadata":{"finalizers":[%q]}}`, f), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(out.GetFinalizers(), []string{f}) {
		t.Errorf("a merge patch merged the list: %v", out.GetFinalizers())
	}

	smp, _ := cu.AcquireStrategicPatch(repoGK, ns, "e")
	_, err = targets.Patch(ctx, "replaced", smp.Type, smp.Data, metav1.PatchOptions{})
	if st := statusOf(t, err); st.Code != http.StatusUnsupportedMediaType {
		t.Errorf("a strategic merge patch on a custom resource: %d %v, want 415", st.Code, err)
	}

	stale := createTarget(t, ns, "guarded", other)
	if _, err := targets.Patch(ctx, "guarded", types.MergePatchType, []byte(`{"metadata":{"labels":{"bump":"1"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	p, _ := cu.AcquireGuardedPatch(stale, repoGK, ns, "e1")
	_, err = targets.Patch(ctx, "guarded", p.Type, p.Data, metav1.PatchOptions{})
	if c := cu.Classify(err); c != cu.ClassConflict {
		t.Errorf("a guarded A2 from a stale read: %v (%v), want Conflict", c, err)
	}

	current, err := targets.Get(ctx, "guarded", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p, _ = cu.AcquireGuardedPatch(current, repoGK, ns, "e2")
	out, err = targets.Patch(ctx, "guarded", p.Type, p.Data, metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := sorted([]string{other, f}); !slices.Equal(sorted(out.GetFinalizers()), want) {
		t.Errorf("finalizers = %v, want %v", out.GetFinalizers(), want)
	}
	if out.GetAnnotations()[f] != "e2" || out.GetLabels()[cu.LabelHeld] != "true" {
		t.Errorf("annotations %v, labels %v", out.GetAnnotations(), out.GetLabels())
	}
}

// Class (a), V5, P6 and P9: an acquiring write on an object that is being
// deleted and lacks the finalizer is a 422 Invalid with a FieldValueForbidden
// cause on metadata.finalizers, through the metadata and dynamic clients,
// for a Secret and for a custom resource. It is the only Deleting.
func TestPremiseClassDeleting(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "deleting")

	check := func(t *testing.T, err error) {
		t.Helper()
		st := statusOf(t, err)
		if st.Code != http.StatusUnprocessableEntity || st.Reason != metav1.StatusReasonInvalid {
			t.Errorf("status %d %s, want 422 Invalid: %v", st.Code, st.Reason, err)
		}
		found := st.Details != nil && slices.ContainsFunc(st.Details.Causes, func(c metav1.StatusCause) bool {
			// L2 of the review of #53: the message is ValidateNoNewFinalizers'.
			return c.Type == metav1.CauseTypeForbidden && c.Field == "metadata.finalizers" &&
				strings.HasPrefix(c.Message, "Forbidden: no new finalizers can be added if the object is being deleted")
		})
		if !found {
			t.Errorf("no FieldValueForbidden cause on metadata.finalizers with ValidateNoNewFinalizers' message: %+v", st.Details)
		}
		if c := cu.Classify(err); c != cu.ClassDeleting {
			t.Errorf("Classify = %v, want Deleting", c)
		}
	}

	t.Run("a Secret", func(t *testing.T) {
		createSecret(t, ns, "s", other)
		if err := env.Kube.CoreV1().Secrets(ns).Delete(ctx, "s", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		p, _ := cu.AcquireStrategicPatch(repoGK, ns, "e")
		_, err := send(ctx, secrets(env.Metadata, ns), "s", p)
		check(t, err)
	})
	t.Run("a custom resource", func(t *testing.T) {
		createTarget(t, ns, "t", other)
		targets := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns)
		if err := targets.Delete(ctx, "t", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		current, err := targets.Get(ctx, "t", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p, _ := cu.AcquireGuardedPatch(current, repoGK, ns, "e")
		_, err = targets.Patch(ctx, "t", p.Type, p.Data, metav1.PatchOptions{})
		check(t, err)
	})
}

// P4: an object being deleted *with* the finalizer still takes an acquiring
// write, which moves the epoch; its labels and annotations stay writable; and
// a release on it removes the finalizer.
func TestPremiseHeldWhileDeleting(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "held")
	f := mustFinalizer(t, ns)
	createSecret(t, ns, "s", other)
	_, e1 := acquire(t, ns, ns, "s")
	if err := env.Kube.CoreV1().Secrets(ns).Delete(ctx, "s", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	got, e2 := acquire(t, ns, ns, "s")
	if got.DeletionTimestamp == nil {
		t.Fatal("the Secret isn't being deleted")
	}
	if got.Annotations[f] != e2 || e2 == e1 {
		t.Errorf("the epoch didn't move: %q, then %q", e1, got.Annotations[f])
	}
	if _, err := secrets(env.Metadata, ns).Patch(ctx, "s", types.MergePatchType, []byte(`{"metadata":{"labels":{"late":"1"},"annotations":{"late":"1"}}}`), metav1.PatchOptions{}); err != nil {
		t.Errorf("labels and annotations aren't writable while it is deleted: %v", err)
	}

	read, err := secrets(env.Metadata, ns).Get(ctx, "s", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r3, err := cu.ReleasePatch(read, repoGK, ns)
	if err != nil {
		t.Fatal(err)
	}
	after, err := send(ctx, secrets(env.Metadata, ns), "s", r3)
	if err != nil {
		t.Fatalf("R3 on an object being deleted: %v", err)
	}
	if !slices.Equal(after.Finalizers, []string{other}) {
		t.Errorf("finalizers after R3 = %v", after.Finalizers)
	}
	if _, ok := after.Labels[cu.LabelHeld]; ok {
		t.Error("the held label stayed after the last credential-use finalizer went")
	}
}

// P5 and class (e): a namespace with no namespace controller stays
// Terminating; NamespaceLifecycle forbids creating in it, with a 403 that is
// not RBAC, and allows patching what is there; finalize removes it.
func TestPremiseNamespaceLifecycle(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "lifecycle")
	createSecret(t, ns, "s")
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp == nil || got.Status.Phase != corev1.NamespaceTerminating {
		t.Fatalf("namespace: deletionTimestamp %v, phase %s; want Terminating", got.DeletionTimestamp, got.Status.Phase)
	}

	_, err = env.Kube.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "late"}}, metav1.CreateOptions{})
	st := statusOf(t, err)
	if st.Code != http.StatusForbidden || !strings.Contains(st.Message, "because it is being terminated") {
		t.Fatalf("a create in a terminating namespace: %v, want NamespaceLifecycle's 403", err)
	}
	if c := cu.Classify(err); c != cu.ClassAdmissionDenied {
		t.Errorf("Classify = %v, want AdmissionDenied (class e), never RBAC", c)
	}

	if _, _ = acquire(t, ns, ns, "s"); t.Failed() {
		t.Fatal("a patch in a terminating namespace failed")
	}

	if err := env.FinalizeNamespace(ctx, ns); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		_, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("namespace still there: %v", err)
	})
}

// Class (d), IP1: the authorizer's own message, pinned exactly, for a
// namespaced and a cluster-scoped object.
func TestPremiseRBACMessage(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "rbac")
	createSecret(t, ns, "s")
	user := "system:serviceaccount:" + ns + ":nobody"
	md, err := metadata.NewForConfig(env.AsServiceAccount(ns, "nobody"))
	if err != nil {
		t.Fatal(err)
	}

	p, _ := cu.AcquireStrategicPatch(repoGK, ns, "e")
	_, err = send(ctx, secrets(md, ns), "s", p)
	st := statusOf(t, err)
	want := fmt.Sprintf(`secrets "s" is forbidden: User %q cannot patch resource "secrets" in API group "" in the namespace %q`, user, ns)
	if st.Code != http.StatusForbidden || st.Message != want {
		t.Errorf("an A2 without RBAC:\n got %d %q\nwant 403 %q", st.Code, st.Message, want)
	}
	if c := cu.Classify(err); c != cu.ClassRBAC {
		t.Errorf("Classify = %v, want RBAC", c)
	}

	// A cluster-scoped write: the holder CRD, which the config, the index
	// pointer and Disown's count write (§7.3, §7.4).
	crds := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crd := cutest.WidgetGVR.Resource + "." + cutest.Group
	_, err = md.Resource(crds).Patch(ctx, crd, types.MergePatchType, []byte(`{"metadata":{"annotations":{"x":"1"}}}`), metav1.PatchOptions{})
	st = statusOf(t, err)
	want = fmt.Sprintf(`customresourcedefinitions.apiextensions.k8s.io %q is forbidden: User %q cannot patch resource "customresourcedefinitions" in API group "apiextensions.k8s.io" at the cluster scope`, crd, user)
	if st.Code != http.StatusForbidden || st.Message != want {
		t.Errorf("a cluster-scoped write without RBAC:\n got %d %q\nwant 403 %q", st.Code, st.Message, want)
	}
	if c := cu.Classify(err); c != cu.ClassRBAC {
		t.Errorf("Classify = %v, want RBAC", c)
	}

	// A Namespace object's own write is authorized in that namespace: the
	// API server takes the request's namespace from the Namespace's name. So
	// it reads "in the namespace", not "at the cluster scope", and is RBAC
	// too.
	_, err = md.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Patch(ctx, ns, types.MergePatchType, []byte(`{"metadata":{"labels":{"x":"1"}}}`), metav1.PatchOptions{})
	st = statusOf(t, err)
	want = fmt.Sprintf(`namespaces %q is forbidden: User %q cannot patch resource "namespaces" in API group "" in the namespace %q`, ns, user, ns)
	if st.Code != http.StatusForbidden || st.Message != want {
		t.Errorf("a Namespace write without RBAC:\n got %d %q\nwant 403 %q", st.Code, st.Message, want)
	}
	if c := cu.Classify(err); c != cu.ClassRBAC {
		t.Errorf("Classify = %v, want RBAC", c)
	}
}

// Classes (b) and (c): ValidatingAdmissionPolicy denials, with and without a
// reason, and a binding whose parameter is missing. None of them is Deleting.
func TestPremisePolicyClasses(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	cases := map[string]struct {
		reason     *metav1.StatusReason
		params     bool
		wantCode   int32
		wantClass  cu.Class
		wantInText string
	}{
		"(b) reason Forbidden": {reason: ptr.To(metav1.StatusReasonForbidden), wantCode: http.StatusForbidden, wantClass: cu.ClassPolicyDenied, wantInText: "denied request: no"},
		"(b) no reason":        {wantCode: http.StatusUnprocessableEntity, wantClass: cu.ClassPolicyDenied, wantInText: "denied request: no"},
		"(c) missing params":   {reason: ptr.To(metav1.StatusReasonForbidden), params: true, wantCode: http.StatusUnprocessableEntity, wantClass: cu.ClassPolicyConfig, wantInText: "failed to configure binding: no params found"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ns := env.Namespace(t, "vap")
			createSecret(t, ns, "s")
			policy := "cu-test-" + ns
			vap := &admissionregistrationv1.ValidatingAdmissionPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: policy},
				Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
					FailurePolicy: ptr.To(admissionregistrationv1.Fail),
					MatchConstraints: &admissionregistrationv1.MatchResources{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns}},
						ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{RuleWithOperations: admissionregistrationv1.RuleWithOperations{
							Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
							Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"secrets"}},
						}}},
					},
					Validations: []admissionregistrationv1.Validation{{Expression: "false", Message: "no", Reason: tc.reason}},
				},
			}
			binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
				ObjectMeta: metav1.ObjectMeta{Name: policy},
				Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
					PolicyName:        policy,
					ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
				},
			}
			if tc.params {
				vap.Spec.ParamKind = &admissionregistrationv1.ParamKind{APIVersion: "v1", Kind: "ConfigMap"}
				binding.Spec.ParamRef = &admissionregistrationv1.ParamRef{
					Name: "cu-policy-params", Namespace: ns,
					ParameterNotFoundAction: ptr.To(admissionregistrationv1.DenyAction),
				}
			}
			ar := env.Kube.AdmissionregistrationV1()
			if _, err := ar.ValidatingAdmissionPolicies().Create(ctx, vap, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = ar.ValidatingAdmissionPolicies().Delete(context.Background(), policy, metav1.DeleteOptions{})
			})
			if _, err := ar.ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = ar.ValidatingAdmissionPolicyBindings().Delete(context.Background(), policy, metav1.DeleteOptions{})
			})

			// A policy takes effect when the API server's informers see it.
			var err error
			cutest.Eventually(t, 30*time.Second, func() error {
				p, _ := cu.AcquireStrategicPatch(repoGK, ns, "e")
				_, err = send(ctx, secrets(env.Metadata, ns), "s", p)
				if err == nil {
					return cutest.ErrNotYet
				}
				return nil
			})
			st := statusOf(t, err)
			if st.Code != tc.wantCode {
				t.Errorf("code %d, want %d: %v", st.Code, tc.wantCode, err)
			}
			// The policy's text is wrapped in the message, not a prefix of it
			// (revision 16, §6.2), and is the message of a cause.
			if prefix := fmt.Sprintf(`secrets "s" is forbidden: ValidatingAdmissionPolicy '%s' with binding '%s' denied request: `, policy, policy); !strings.HasPrefix(st.Message, prefix) {
				t.Errorf("message %q doesn't start with %q", st.Message, prefix)
			}
			if !strings.Contains(st.Message, tc.wantInText) {
				t.Errorf("message %q doesn't contain %q", st.Message, tc.wantInText)
			}
			hasCause := st.Details != nil && slices.ContainsFunc(st.Details.Causes, func(c metav1.StatusCause) bool {
				return strings.HasPrefix(c.Message, "ValidatingAdmissionPolicy '"+policy+"'") && c.Type == "" && c.Field == ""
			})
			if !hasCause {
				t.Errorf("no cause carries the policy's text: %+v", st.Details)
			}
			if c := cu.Classify(err); c != tc.wantClass {
				t.Errorf("Classify = %v, want %v", c, tc.wantClass)
			}
			if c := cu.ClassifyJSONPatch(err); c != tc.wantClass {
				t.Errorf("ClassifyJSONPatch = %v, want %v: a policy's 422 is never a state change", c, tc.wantClass)
			}
		})
	}
}

// V4: a JSON patch whose test op fails, or whose remove doesn't apply, is a
// 422 Invalid with the generic message and no causes: ClassStateChanged, and
// never Deleting. And the state-guarded patches (RestoreEpochPatch,
// JanitorPatch) fail the same way after any write.
func TestPremiseJSONPatchFailures(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "jsonpatch")
	r := secrets(env.Metadata, ns)

	checkStateChanged := func(t *testing.T, err error) {
		t.Helper()
		st := statusOf(t, err)
		if st.Code != http.StatusUnprocessableEntity || st.Reason != metav1.StatusReasonInvalid {
			t.Errorf("status %d %s, want 422 Invalid: %v", st.Code, st.Reason, err)
		}
		if want := "the server rejected our request due to an error in our request"; st.Message != want {
			t.Errorf("message %q, want %q", st.Message, want)
		}
		if st.Details != nil && len(st.Details.Causes) > 0 {
			t.Errorf("causes %+v, want none", st.Details.Causes)
		}
		if c := cu.ClassifyJSONPatch(err); c != cu.ClassStateChanged {
			t.Errorf("ClassifyJSONPatch = %v, want StateChanged", c)
		}
		if c := cu.Classify(err); c == cu.ClassDeleting {
			t.Error("Classify read a failed test op as Deleting")
		}
	}
	read := func(t *testing.T) *metav1.PartialObjectMetadata {
		t.Helper()
		m, err := r.Get(ctx, "s", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}

	createSecret(t, ns, "s", other)
	acquire(t, ns, ns, "s")

	t.Run("a moved epoch", func(t *testing.T) {
		r3, err := cu.ReleasePatch(read(t), repoGK, ns)
		if err != nil {
			t.Fatal(err)
		}
		acquire(t, ns, ns, "s") // a concurrent A2: a fresh epoch
		_, err = send(ctx, r, "s", r3)
		checkStateChanged(t, err)
	})
	t.Run("a moved index", func(t *testing.T) {
		// M2 of the review of #53: the index must really move, and the 422
		// must come from the index test. Two namespaces of K hold the
		// object, so no label is removed, and a finalizer sits before F.
		createSecret(t, ns, "shift")
		acquireNamed(t, ns, "shift")
		acquire(t, ns, "team-x", "shift")
		if _, err := r.Patch(ctx, "shift", types.JSONPatchType, fmt.Appendf(nil, `[{"op":"add","path":"/metadata/finalizers/0","value":%q}]`, other), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		m, err := r.Get(ctx, "shift", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		i := slices.Index(m.Finalizers, mustFinalizer(t, ns))
		if m.Finalizers[0] != other || i < 1 {
			t.Fatalf("finalizers %v: want %s first and F after it", m.Finalizers, other)
		}
		r3, err := cu.ReleasePatch(m, repoGK, ns)
		if err != nil {
			t.Fatal(err)
		}
		// The finalizer before F goes: F moves down, and index i names
		// another namespace's hold, or nothing.
		if _, err := r.Patch(ctx, "shift", types.JSONPatchType, []byte(`[{"op":"remove","path":"/metadata/finalizers/0"}]`), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		ops := opsOf(t, r3)
		if got := ops[2]["path"]; got != fmt.Sprintf("/metadata/finalizers/%d", i) {
			t.Fatalf("op 3 is %v, want the index test", ops[2])
		}
		// The uid and epoch tests still hold, so the index test is the one
		// that fails.
		if _, err := send(ctx, r, "shift", opsPatch(t, ops[:2])); err != nil {
			t.Fatalf("the uid and epoch tests alone failed: %v", err)
		}
		_, err = send(ctx, r, "shift", opsPatch(t, ops[:3]))
		checkStateChanged(t, err)
		// And the whole release fails with it.
		_, err = send(ctx, r, "shift", r3)
		checkStateChanged(t, err)
	})

	t.Run("a restored epoch from a stale read", func(t *testing.T) {
		stale := read(t)
		acquire(t, ns, ns, "s")
		p, err := cu.RestoreEpochPatch(stale, repoGK, ns, "e")
		if err != nil {
			t.Fatal(err)
		}
		_, err = send(ctx, r, "s", p)
		checkStateChanged(t, err)
	})
	t.Run("a release from a fresh read applies", func(t *testing.T) {
		r3, err := cu.ReleasePatch(read(t), repoGK, ns)
		if err != nil {
			t.Fatal(err)
		}
		after, err := send(ctx, r, "s", r3)
		if err != nil {
			t.Fatalf("R3 from a fresh read: %v", err)
		}
		if slices.Contains(after.Finalizers, mustFinalizer(t, ns)) {
			t.Error("the finalizer is still there")
		}
		if _, ok := after.Labels[cu.MarkerLabel(repoGK)]; ok {
			t.Error("K's marker label is still there")
		}
	})
	t.Run("the janitor", func(t *testing.T) {
		createSecret(t, ns, "stripped")
		_, _ = acquireNamed(t, ns, "stripped")
		// A kubectl replace that strips the finalizers but keeps the labels.
		if _, err := r.Patch(ctx, "stripped", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		m, err := r.Get(ctx, "stripped", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p, err := cu.JanitorPatch(m, repoGK)
		if err != nil {
			t.Fatal(err)
		}
		// A write in between: the janitor must not land.
		if _, err := r.Patch(ctx, "stripped", types.MergePatchType, []byte(`{"metadata":{"annotations":{"x":"1"}}}`), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err = send(ctx, r, "stripped", p)
		checkStateChanged(t, err)

		m, _ = r.Get(ctx, "stripped", metav1.GetOptions{})
		p, _ = cu.JanitorPatch(m, repoGK)
		after, err := send(ctx, r, "stripped", p)
		if err != nil {
			t.Fatalf("the janitor from a fresh read: %v", err)
		}
		if _, ok := after.Labels[cu.LabelHeld]; ok {
			t.Error("the held label is still there")
		}
	})
}

func acquireNamed(t *testing.T, ns, name string) (*metav1.PartialObjectMetadata, string) {
	t.Helper()
	return acquire(t, ns, ns, name)
}

// P3 and §2: a label-selected metadata watch delivers an object that stops
// matching as a deletion, and one that starts matching as an addition, with
// client-go's defaults (WatchListClient on).
func TestPremiseLabelSelectedWatch(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "watch")
	createSecret(t, ns, "s")

	var mu sync.Mutex
	var events []string
	record := func(kind string, obj any) {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = d.Obj
		}
		m, ok := obj.(*metav1.PartialObjectMetadata)
		if !ok || m.Name != "s" {
			return
		}
		mu.Lock()
		events = append(events, kind)
		mu.Unlock()
	}
	inf := metadatainformer.NewFilteredMetadataInformer(env.Metadata, cutest.SecretGVR, ns, 0, cache.Indexers{},
		func(o *metav1.ListOptions) { o.LabelSelector = cu.LabelHeld + "=true" }).Informer()
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { record("add", obj) },
		UpdateFunc: func(_, obj any) { record("update", obj) },
		DeleteFunc: func(obj any) { record("delete", obj) },
	}); err != nil {
		t.Fatal(err)
	}
	go inf.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		t.Fatal("the informer didn't sync")
	}
	waitFor := func(want ...string) {
		t.Helper()
		cutest.Eventually(t, 30*time.Second, func() error {
			mu.Lock()
			defer mu.Unlock()
			if slices.Equal(events, want) {
				return nil
			}
			return fmt.Errorf("events %v, want %v", events, want)
		})
	}

	acquire(t, ns, ns, "s")
	waitFor("add")
	if _, err := secrets(env.Metadata, ns).Patch(ctx, "s", types.JSONPatchType, fmt.Appendf(nil, `[{"op":"remove","path":"/metadata/labels/%s"}]`, strings.ReplaceAll(cu.LabelHeld, "/", "~1")), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("add", "delete")
	acquire(t, ns, ns, "s")
	waitFor("add", "delete", "add")
}

// opsOf decodes a JSON patch's ops.
func opsOf(t *testing.T, p cu.Patch) []map[string]any {
	t.Helper()
	var ops []map[string]any
	if err := json.Unmarshal(p.Data, &ops); err != nil {
		t.Fatal(err)
	}
	return ops
}

// opsPatch encodes ops as a JSON patch.
func opsPatch(t *testing.T, ops []map[string]any) cu.Patch {
	t.Helper()
	data, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	return cu.Patch{Type: types.JSONPatchType, Data: data}
}
