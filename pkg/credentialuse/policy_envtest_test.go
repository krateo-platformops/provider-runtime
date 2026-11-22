package credentialuse_test

// The dedicated index namespace and its admission policy (§10.6.1, §14), and
// PerName under resourceNames RBAC (§13 shape 1, P14), against envtest. The
// policy, its binding and its parameter ConfigMap are §10.6.1's, with the
// test's namespaces in place of krateo-credential-use and krateo-system.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// policyName is §10.6.1's policy, by its exact name: the missing-parameters
// symptom is matched by it (§10.6.1).
const policyName = "krateo-credential-use-namespace"

// policyExpression is §10.6.1's validation, verbatim.
const policyExpression = `(request.resource.resource == "events") ||
(variables.core && request.resource.resource == "configmaps" &&
 object.metadata.name == "kube-root-ca.crt" && variables.kubeControllers) ||
(variables.core && request.resource.resource == "serviceaccounts" &&
 object.metadata.name == "default" && variables.kubeControllers) ||
(variables.core && request.resource.resource == "configmaps" &&
 object.metadata.name.startsWith("cu-index-") &&
 ("system:serviceaccounts:" + params.data.managementNamespace) in request.userInfo.groups)`

// installPolicy applies §10.6.1's policy for index namespace idx, its
// binding, and its parameter ConfigMap in management namespace mgmt, and
// removes the policy and binding when the test ends. It returns once the
// policy denies a write.
func installPolicy(t *testing.T, idx, mgmt string) {
	t.Helper()
	ctx := context.Background()
	all := []string{"*"}
	vap := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: ptr.To(admissionregistrationv1.Fail),
			ParamKind:     &admissionregistrationv1.ParamKind{APIVersion: "v1", Kind: "ConfigMap"},
			MatchConstraints: &admissionregistrationv1.MatchResources{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": idx}},
				ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: all, APIVersions: all, Resources: all},
				}}},
				ExcludeResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.OperationAll},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: all, Resources: []string{"namespaces"}},
				}}},
			},
			Variables: []admissionregistrationv1.Variable{
				{Name: "kubeControllers", Expression: `request.userInfo.username in [
  "system:serviceaccount:kube-system:root-ca-cert-publisher",
  "system:serviceaccount:kube-system:service-account-controller",
  "system:kube-controller-manager"]`},
				{Name: "core", Expression: `request.resource.group == ""`},
			},
			Validations: []admissionregistrationv1.Validation{{
				Expression: policyExpression,
				Message: "only cu-index-* ConfigMaps from the management namespace's service accounts, events, and Kubernetes' own " +
					"kube-root-ca.crt and default ServiceAccount may be written in " + idx,
				Reason: ptr.To(metav1.StatusReasonForbidden),
			}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policyName,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
			ParamRef: &admissionregistrationv1.ParamRef{
				Name: "cu-policy-params", Namespace: mgmt,
				ParameterNotFoundAction: ptr.To(admissionregistrationv1.DenyAction),
			},
		},
	}
	params := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cu-policy-params", Namespace: mgmt}, Data: map[string]string{"managementNamespace": mgmt}}
	if _, err := env.Kube.CoreV1().ConfigMaps(mgmt).Create(ctx, params, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ar := env.Kube.AdmissionregistrationV1()
	if _, err := ar.ValidatingAdmissionPolicies().Create(ctx, vap, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ar.ValidatingAdmissionPolicies().Delete(context.Background(), policyName, metav1.DeleteOptions{})
	})
	if _, err := ar.ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ar.ValidatingAdmissionPolicyBindings().Delete(context.Background(), policyName, metav1.DeleteOptions{})
	})
	cutest.Eventually(t, 30*time.Second, func() error {
		_, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "probe"}}, metav1.CreateOptions{})
		if err == nil {
			_ = env.Kube.CoreV1().ConfigMaps(idx).Delete(ctx, "probe", metav1.DeleteOptions{})
			return fmt.Errorf("the policy doesn't deny yet: %w", cutest.ErrNotYet)
		}
		return nil
	})
}

// grantWrites lets users and groups get, create and update ConfigMaps and
// ServiceAccounts, and create Events, in namespace ns, so that a denial is
// the policy's, not RBAC's.
func grantWrites(t *testing.T, ns string, users, groups []string) {
	t.Helper()
	ctx := context.Background()
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "cu-writes", Namespace: ns}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"configmaps", "serviceaccounts"}, Verbs: []string{"get", "create", "update"}},
		{APIGroups: []string{"", "events.k8s.io"}, Resources: []string{"events"}, Verbs: []string{"create"}},
	}}
	if _, err := env.Kube.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var subjects []rbacv1.Subject
	for _, u := range users {
		subjects = append(subjects, rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: u})
	}
	for _, g := range groups {
		subjects = append(subjects, rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: g})
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "cu-writes", Namespace: ns},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cu-writes"},
		Subjects:   subjects,
	}
	if _, err := env.Kube.RbacV1().RoleBindings(ns).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func kubeAs(t *testing.T, cfg *rest.Config) kubernetes.Interface {
	t.Helper()
	k, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// allowed and denied check a write's outcome: denied wants the policy's
// 403, class (b) (§6.2).
func allowed(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v, want it allowed", what, err)
	}
}

func denied(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: allowed, want the policy's denial", what)
		return
	}
	if st := statusOf(t, err); st.Code != http.StatusForbidden || cu.Classify(err) != cu.ClassPolicyDenied {
		t.Errorf("%s: %d %v, class %v; want 403 and class (b) PolicyDenied", what, st.Code, err, cu.Classify(err))
	}
}

// The policy allows exactly §10.6.1's writes and denies the rest (§14):
// Kubernetes' writers by identity, the management namespace's dynamic
// service accounts' cu-index-* ConfigMaps, events, and installer's update of
// the Namespace itself; never a composition object, another ConfigMap, or a
// cu-index-* ConfigMap from outside the management namespace. Classes (b)
// and (c) come from it for real, (c) recognised by its text; and deleting
// the namespace completes, since the policy matches no DELETE.
func TestIndexNamespacePolicy(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, mgmt, other := env.Namespace(t, "cu-policy"), env.Namespace(t, "cu-mgmt"), env.Namespace(t, "cu-other")
	const (
		rootCA    = "system:serviceaccount:kube-system:root-ca-cert-publisher"
		saCtrl    = "system:serviceaccount:kube-system:service-account-controller"
		kcm       = "system:kube-controller-manager"
		impostor  = "system:serviceaccount:kube-system:impostor"
		alice     = "alice"
		dynamicSA = "widgets-v1-2-0"
	)
	grantWrites(t, idx, []string{rootCA, saCtrl, kcm, impostor, alice}, []string{"system:serviceaccounts:" + mgmt, "system:serviceaccounts:" + other})
	installPolicy(t, idx, mgmt)

	cms := func(k kubernetes.Interface) func(name string) error {
		return func(name string) error {
			_, err := k.CoreV1().ConfigMaps(idx).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
			return err
		}
	}
	updateCM := func(k kubernetes.Interface, name string) error {
		cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cm.Data = map[string]string{"ca.crt": time.Now().String()}
		_, err = k.CoreV1().ConfigMaps(idx).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	}
	updateSA := func(k kubernetes.Interface, name string) error {
		sa, err := env.Kube.CoreV1().ServiceAccounts(idx).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		sa.Labels = map[string]string{"touched": fmt.Sprint(time.Now().UnixNano())}
		_, err = k.CoreV1().ServiceAccounts(idx).Update(ctx, sa, metav1.UpdateOptions{})
		return err
	}
	asRootCA := kubeAs(t, env.AsServiceAccount("kube-system", "root-ca-cert-publisher"))
	asSACtrl := kubeAs(t, env.AsServiceAccount("kube-system", "service-account-controller"))
	asKCM := kubeAs(t, env.As(kcm))
	asImpostor := kubeAs(t, env.AsServiceAccount("kube-system", "impostor"))
	asAlice := kubeAs(t, env.As(alice))

	// Kubernetes' own writers, by identity, for their two names (P16).
	denied(t, "kube-root-ca.crt by another kube-system service account", cms(asImpostor)("kube-root-ca.crt"))
	denied(t, "kube-root-ca.crt by alice", cms(asAlice)("kube-root-ca.crt"))
	allowed(t, "kube-root-ca.crt by the root-CA publisher", cms(asRootCA)("kube-root-ca.crt"))
	allowed(t, "kube-root-ca.crt by kube-controller-manager", updateCM(asKCM, "kube-root-ca.crt"))
	denied(t, "kube-root-ca.crt updated by alice", updateCM(asAlice, "kube-root-ca.crt"))
	sa := func(k kubernetes.Interface) error {
		_, err := k.CoreV1().ServiceAccounts(idx).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default"}}, metav1.CreateOptions{})
		return err
	}
	denied(t, "the default ServiceAccount by another kube-system service account", sa(asImpostor))
	allowed(t, "the default ServiceAccount by the service-account controller", sa(asSACtrl))
	allowed(t, "the default ServiceAccount by kube-controller-manager", updateSA(asKCM, "default"))
	denied(t, "the default ServiceAccount updated by alice", updateSA(asAlice, "default"))
	denied(t, "kube-root-ca.crt by the service-account controller", updateCM(asSACtrl, "kube-root-ca.crt"))

	// A dynamic composition-controller service account, created now, writes
	// its cu-index-* ConfigMap; nothing else is allowed from it, or from
	// outside the management namespace.
	if _, err := env.Kube.CoreV1().ServiceAccounts(mgmt).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: dynamicSA}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	asDynamic := kubeAs(t, env.AsServiceAccount(mgmt, dynamicSA))
	allowed(t, "cu-index-* by the dynamic service account", cms(asDynamic)("cu-index-widgets.composition.krateo.io"))
	denied(t, "another ConfigMap by the dynamic service account", cms(asDynamic)("not-an-index"))
	denied(t, "cu-index-* from outside the management namespace", cms(kubeAs(t, env.AsServiceAccount(other, "x")))("cu-index-x.y.io"))
	denied(t, "cu-index-* by alice", cms(asAlice)("cu-index-a.b.io"))

	// A composition object is refused, even from the administrator.
	w := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": "s"}}}
	w.SetGroupVersionKind(cutest.WidgetGVK)
	w.SetName("composition")
	_, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(idx).Create(ctx, w, metav1.CreateOptions{})
	denied(t, "a composition object", err)

	// Events are allowed.
	_, err = asAlice.CoreV1().Events(idx).Create(ctx, &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "e"},
		InvolvedObject: corev1.ObjectReference{Kind: "ConfigMap", Namespace: idx, Name: "kube-root-ca.crt"},
		Reason:         "Test", Message: "an event",
	}, metav1.CreateOptions{})
	allowed(t, "an event", err)

	// installer's update of the Namespace object is allowed (P15).
	ns, err := env.Kube.CoreV1().Namespaces().Get(ctx, idx, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ns.Annotations = map[string]string{"helm.sh/resource-policy": "keep"}
	_, err = env.Kube.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
	allowed(t, "installer's update of the Namespace", err)

	// Class (c): without its parameters, every write fails with a 422 that
	// must not be read as Deleting, recognised by its text (§10.6.1).
	if err := env.Kube.CoreV1().ConfigMaps(mgmt).Delete(ctx, "cu-policy-params", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	var cerr error
	cutest.Eventually(t, 30*time.Second, func() error {
		cerr = updateCM(asDynamic, "cu-index-widgets.composition.krateo.io")
		if cerr == nil || cu.Classify(cerr) != cu.ClassPolicyConfig {
			return fmt.Errorf("%v: %w", cerr, cutest.ErrNotYet)
		}
		return nil
	})
	if st := statusOf(t, cerr); st.Code != http.StatusUnprocessableEntity {
		t.Errorf("class (c) code %d, want 422", st.Code)
	}
	if !cu.PolicyParamsMissing(cerr) {
		t.Errorf("the missing-parameters symptom isn't recognised by its text: %v", cerr)
	}
	for _, s := range []string{"ValidatingAdmissionPolicy '" + policyName + "'", "failed to configure binding: no params found"} {
		if !strings.Contains(cerr.Error(), s) {
			t.Errorf("class (c)'s message %q lacks %q", cerr, s)
		}
	}
	if kcmErr := updateCM(asKCM, "kube-root-ca.crt"); cu.Classify(kcmErr) != cu.ClassPolicyConfig {
		t.Errorf("kube-controller-manager's write without the parameters: %v, want class (c) too", kcmErr)
	}

	// Deleting the namespace completes: the policy matches no DELETE, and
	// the test plays the namespace controller.
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, idx, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kube-root-ca.crt", "cu-index-widgets.composition.krateo.io"} {
		if err := env.Kube.CoreV1().ConfigMaps(idx).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("emptying the namespace: %v", err)
		}
	}
	for _, del := range []func() error{
		func() error {
			return env.Kube.CoreV1().ServiceAccounts(idx).Delete(ctx, "default", metav1.DeleteOptions{})
		},
		func() error { return env.Kube.CoreV1().Events(idx).Delete(ctx, "e", metav1.DeleteOptions{}) },
		func() error {
			return env.Kube.RbacV1().RoleBindings(idx).Delete(ctx, "cu-writes", metav1.DeleteOptions{})
		},
		func() error { return env.Kube.RbacV1().Roles(idx).Delete(ctx, "cu-writes", metav1.DeleteOptions{}) },
	} {
		if err := del(); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("emptying the namespace: %v", err)
		}
	}
	if err := env.FinalizeNamespace(ctx, idx); err != nil {
		t.Fatal(err)
	}
	gone(t, time.Minute, "namespace "+idx, func() error {
		_, err := env.Kube.CoreV1().Namespaces().Get(ctx, idx, metav1.GetOptions{})
		return err
	})
}

// PerName under resourceNames RBAC (§13 shape 1, P14): a process whose
// service account may read and patch only the Secrets it names, by name,
// holds and releases them through field-selected watches and GETs, and its
// index's grants (§10.6.7); its startup recheck prunes a stale line.
func TestPerNameUnderResourceNamesRBAC(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx, ns := indexNamespace(t), env.Namespace(t, "pername-rbac")
	const sa, provider = "cu-provider", "default"
	user := "system:serviceaccount:" + provider + ":" + sa
	cluster := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cu-pername-" + ns}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"s", "stale"}, Verbs: []string{"get", "patch", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{cutest.Group}, Resources: []string{"widgets"}, Verbs: []string{"get", "list", "watch", "patch", "update"}},
		{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"}, ResourceNames: []string{widgetCRD}, Verbs: []string{"get", "list", "watch", "patch"}},
	}}
	if _, err := env.Kube.RbacV1().ClusterRoles().Create(ctx, cluster, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = env.Kube.RbacV1().ClusterRoles().Delete(context.Background(), cluster.Name, metav1.DeleteOptions{})
	})
	subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: provider, Name: sa}}
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name}, Subjects: subjects,
		RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: cluster.Name}}
	if _, err := env.Kube.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = env.Kube.RbacV1().ClusterRoleBindings().Delete(context.Background(), crb.Name, metav1.DeleteOptions{})
	})
	// §10.6.7: the index by name, and create in its namespace.
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "cu-index", Namespace: idx}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{widgetIndex}, Verbs: []string{"get", "update", "patch", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"create"}},
	}}
	if _, err := env.Kube.RbacV1().Roles(idx).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "cu-index", Namespace: idx}, Subjects: subjects,
		RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cu-index"}}
	if _, err := env.Kube.RbacV1().RoleBindings(idx).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// The grants hold: no list of Secrets without a name.
	asSA := kubeAs(t, env.AsServiceAccount(provider, sa))
	cutest.Eventually(t, 30*time.Second, func() error {
		if _, err := asSA.CoreV1().Secrets(ns).Get(ctx, "s", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the grant isn't in force: %v: %w", err, cutest.ErrNotYet)
		}
		return nil
	})
	if _, err := asSA.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("listing Secrets without a name: %v, want forbidden", err)
	}
	createSecret(t, ns, "s")
	createSecret(t, ns, "stale")
	createWidget(t, ns, "w", "s")
	// The index exists, as another process of the kind left it: a process
	// that creates it rebuilds it from every record, and the other tests'
	// holders name Secrets this one may not read.
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	resetPublished(t, cutest.WidgetGVR)
	t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
	c := newController(t)
	clients := clientsFor(t, env.AsServiceAccount(provider, sa))
	rec := events.NewFakeRecorder(100000)
	go func() {
		for range rec.Events {
		}
	}()
	clients.Events = rec
	// Namespaces restricts the label-scoped caches; a PerName kind's
	// per-name watches cover every namespace, so it needs no list of
	// Secrets in any (§10.5, §13 shape 1).
	s := cu.NewShared(clients, cu.SharedOptions{IndexNamespace: idx, Namespaces: []string{"unrelated"}})
	o := widgetOptions(c.enqueue)
	o.DeletionGrace, o.ProbeInterval = testGrace, time.Second
	perName(&o)
	tr, err := s.Track(ctx, o)
	if err != nil {
		t.Fatalf("Track as %s: %v", user, err)
	}
	setIndexLines(t, idx, func(l []string) []string { return append(l, secretLine(ns, "stale")) })
	running(t, s)
	c.start(newDriver(tr))

	// The startup recheck reads the stale line's Secret by name, and prunes
	// it.
	lined(t, idx, secretLine(ns, "stale"), false)
	c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	lined(t, idx, secretLine(ns, "s"), true)
	cutest.Eventually(t, time.Minute, func() error {
		if !tr.PerNameSynced(cu.SecretRef(ns, "s").ID()) {
			return fmt.Errorf("the field-selected watch hasn't synced: %w", cutest.ErrNotYet)
		}
		return nil
	})
	deleteWidget(t, ns, "w")
	c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, notHeldFor(ns, "s", ns))
	lined(t, idx, secretLine(ns, "s"), false)
	if v := c.violations(); len(v) > 0 {
		t.Errorf("violations: %v", v)
	}
	cutest.Eventually(t, time.Minute, func() error {
		if slices.Contains(tr.PerNameWatches(), cu.SecretRef(ns, "s").ID()) {
			return fmt.Errorf("the per-name watch stays after the prune: %w", cutest.ErrNotYet)
		}
		return nil
	})
}
