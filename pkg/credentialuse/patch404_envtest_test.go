package credentialuse_test

// A 404 of a patch that isn't about a missing object (§6.2): what answers one,
// and that nothing held is taken for gone.

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

const label404 = "credentialuse.test/patch-404"

var secretsGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

// webhook404 creates a validating webhook of updates of Secrets in the
// namespaces labelled with name, removed when t ends, or by the function
// returned.
func webhook404(t *testing.T, name string, cc admissionregistrationv1.WebhookClientConfig) (remove func()) {
	t.Helper()
	hooks := env.Kube.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	c := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: name + ".credentialuse.test", ClientConfig: cc,
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
				Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"secrets"}},
			}},
			FailurePolicy:           ptr.To(admissionregistrationv1.Fail),
			NamespaceSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{label404: name}},
			SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
			AdmissionReviewVersions: []string{"v1"},
		}},
	}
	if _, err := hooks.Create(context.Background(), c, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	remove = func() { _ = hooks.Delete(context.Background(), name, metav1.DeleteOptions{}) }
	t.Cleanup(remove)
	return remove
}

// noServer is a webhook nothing answers: a selector alone decides about it.
func noServer() admissionregistrationv1.WebhookClientConfig {
	return admissionregistrationv1.WebhookClientConfig{URL: ptr.To("https://127.0.0.1:1/never")}
}

// namespace404 creates a namespace that the webhook called name selects.
func namespace404(t *testing.T, name string) string {
	t.Helper()
	ns := env.Namespace(t, "patch404")
	label404In(t, ns, name)
	return ns
}

func label404In(t *testing.T, ns, name string) {
	t.Helper()
	if _, err := env.Kube.CoreV1().Namespaces().Patch(context.Background(), ns, types.MergePatchType,
		[]byte(`{"metadata":{"labels":{"`+label404+`":"`+name+`"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

// removeNamespace removes ns by hand, with what is in it, as someone who
// forces its finalizer does, until a patch of the Secret named is answered
// with a 404 that names ns; a cleanup creates ns again and empties it.
func removeNamespace(t *testing.T, ns, secret string) {
	t.Helper()
	ctx := context.Background()
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := env.FinalizeNamespace(ctx, ns); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		recreateNamespace(t, ns)
		l, err := env.Kube.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for i := range l.Items {
			_, _ = env.Kube.CoreV1().Secrets(ns).Patch(ctx, l.Items[i].Name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		}
	})
	cutest.Eventually(t, 30*time.Second, func() error {
		_, err := env.Metadata.Resource(secretsGVR).Namespace(ns).Patch(ctx, secret, types.MergePatchType, []byte(`{"metadata":{"annotations":{"probe":"1"}}}`), metav1.PatchOptions{})
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("the patch was answered %v: %w", err, cutest.ErrNotYet)
		}
		return nil
	})
}

// recreateNamespace creates ns again, without the label.
func recreateNamespace(t *testing.T, ns string) {
	t.Helper()
	_, err := env.Kube.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Error(err)
	}
}

// holdSecret creates Secret ns/name carrying gk's hold for ns.
func holdSecret(t *testing.T, gk schema.GroupKind, ns, name string) {
	t.Helper()
	createSecret(t, ns, name)
	pt, err := cu.AcquireStrategicPatch(gk, ns, "epoch")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Kube.CoreV1().Secrets(ns).Patch(context.Background(), name, pt.Type, pt.Data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

func finalizersOf(t *testing.T, ns, name string) []string {
	t.Helper()
	s, err := env.Kube.CoreV1().Secrets(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return s.Finalizers
}

// Premise P20 (a): with a webhook that has a namespaceSelector, a patch in a
// namespace that is gone is answered with a 404 naming the namespace, a Status
// without the cause; the object is there, and read.
func TestPremiseAPatchInANamespaceThatIsGone(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	webhook404(t, "gone-premise", noServer())
	ns := namespace404(t, "other") // not the one the webhook selects
	createSecret(t, ns, "s", "example.com/kept")
	removeNamespace(t, ns, "s")
	_, err := env.Metadata.Resource(secretsGVR).Namespace(ns).Patch(ctx, "s", types.MergePatchType, []byte(`{"metadata":{"annotations":{"a":"b"}}}`), metav1.PatchOptions{})
	st := statusOf(t, err)
	if st.Code != http.StatusNotFound || st.Details == nil || st.Details.Name != ns || st.Details.Kind != "namespaces" || len(st.Details.Causes) != 0 {
		t.Errorf("the patch's status is %+v, want a 404 naming the namespace", st)
	}
	if _, err := env.Metadata.Resource(secretsGVR).Namespace(ns).Get(ctx, "s", metav1.GetOptions{}); err != nil {
		t.Errorf("the object's read = %v, want it found", err)
	}
	_, err = env.Metadata.Resource(secretsGVR).Namespace(ns).Get(ctx, "never", metav1.GetOptions{})
	if st := statusOf(t, err); st.Details == nil || st.Details.Name != "never" || cu.Classify(err) != cu.ClassNotFound {
		t.Errorf("a missing object's read = %+v, want a 404 that names it", st)
	}
}

// R3 there: the hold stays in the index, and its batch is retried. Once the
// namespace is back, it is released.
func TestAHoldInANamespaceThatIsGone(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	idx := indexNamespace(t)
	webhook404(t, "gone-release", noServer())
	ns := namespace404(t, "other")
	holdSecret(t, cutest.WidgetGVK.GroupKind(), ns, "s")
	goneGK := schema.GroupKind{Group: "gone.example.com", Kind: "Gone"}
	holdSecret(t, goneGK, ns, "p")
	line := cu.SecretRef(ns, "s").ID().String()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: widgetIndex, Namespace: idx, Annotations: map[string]string{cu.AnnotationRebuilt: "true"}},
		Data: map[string]string{"entries": line}}
	if _, err := env.Kube.CoreV1().ConfigMaps(idx).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	removeNamespace(t, ns, "s")

	rep, err := cu.DisownPrefix(ctx, clientsFor(t, env.Config), cu.DisownPrefixOptions{Holder: goneGK, Names: []cu.Ref{cu.SecretRef(ns, "p")}})
	if err == nil || len(rep.Stripped) != 0 {
		t.Errorf("DisownPrefix = %+v, %v; want an error: the finalizer is still there", rep, err)
	}

	logs := &warnLog{}
	perNameRig(t, idx, func(o *cu.Options) { o.Logger = logs }, nil)
	failed := func() bool {
		logs.mu.Lock()
		defer logs.mu.Unlock()
		return slices.ContainsFunc(logs.warns, func(w string) bool { return strings.Contains(w, "release batch failed") })
	}
	lines := func() []string {
		cm, err := env.Kube.CoreV1().ConfigMaps(idx).Get(ctx, widgetIndex, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(cm.Data["entries"], "\n")
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if !slices.Contains(lines(), line) {
			return nil // which the checks below refuse
		}
		if !failed() {
			return fmt.Errorf("the batch hasn't failed: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if !slices.Contains(lines(), line) || len(finalizersOf(t, ns, "s")) != 1 {
		t.Fatalf("the index is %q, the Secret's finalizers %v; want the hold, and its line", lines(), finalizersOf(t, ns, "s"))
	}
	recreateNamespace(t, ns)
	cutest.Eventually(t, 60*time.Second, func() error {
		if f := finalizersOf(t, ns, "s"); len(f) != 0 {
			return fmt.Errorf("finalizers %v: %w", f, cutest.ErrNotYet)
		}
		if slices.Contains(lines(), line) {
			return fmt.Errorf("the line is still there: %w", cutest.ErrNotYet)
		}
		return nil
	})
}

// validator404 is a validator that refuses every update with a NotFound of its
// own for the very object, as a lookup of it in a cache that lags gives.
func validator404(t *testing.T) admissionregistrationv1.WebhookClientConfig {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rev admissionv1.AdmissionReview
		if err := json.NewDecoder(r.Body).Decode(&rev); err != nil || rev.Request == nil {
			http.Error(w, "not a review", http.StatusBadRequest)
			return
		}
		st := apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, rev.Request.Name).ErrStatus
		rev.Response = &admissionv1.AdmissionResponse{UID: rev.Request.UID, Allowed: false, Result: &st}
		rev.Request = nil
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rev)
	}))
	t.Cleanup(srv.Close)
	return admissionregistrationv1.WebhookClientConfig{URL: ptr.To(srv.URL + "/validate"),
		CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})}
}

// Premise P20 (b): a validator's refusal may be a NotFound that names the very
// object, which is there. A2 doesn't take it for Missing: a new holder doesn't
// connect or establish until it lands.
func TestAValidatorsOwnNotFound(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	remove := webhook404(t, "validator", validator404(t))
	ns := namespace404(t, "validator")
	createSecret(t, ns, "s")
	var refusal error
	cutest.Eventually(t, 30*time.Second, func() error {
		_, refusal = env.Metadata.Resource(secretsGVR).Namespace(ns).Patch(ctx, "s", types.MergePatchType, []byte(`{"metadata":{"annotations":{"a":"b"}}}`), metav1.PatchOptions{})
		if !apierrors.IsNotFound(refusal) {
			return fmt.Errorf("the patch was answered %v: %w", refusal, cutest.ErrNotYet)
		}
		return nil
	})
	if st := statusOf(t, refusal); st.Details == nil || st.Details.Name != "s" || len(st.Details.Causes) != 0 {
		t.Errorf("the refusal is %+v, want a 404 that names the object", st)
	}

	r := newWriteRig(t, env.Config, cu.SharedOptions{}, nil, nil)
	createWidget(t, ns, "w", "s")
	nn := widgetNN(ns, "w")
	r.c.enqueue(nn)
	widget := func() *unstructured.Unstructured {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if slices.Contains(widget().GetFinalizers(), ownerFinalizer) {
			return nil // which the checks below refuse
		}
		if r.c.lastResult(nn).Acquired.Err == nil {
			return fmt.Errorf("no failed pass: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if a := r.c.lastResult(nn).Acquired; !a.MustNotConnect || len(a.Unheld) != 0 || slices.Contains(widget().GetFinalizers(), ownerFinalizer) {
		t.Fatalf("w's finalizers %v, its pass %+v; want it not established, and not to connect", widget().GetFinalizers(), a)
	}
	remove()
	cutest.Eventually(t, 60*time.Second, func() error {
		r.c.enqueue(nn)
		if !slices.Contains(widget().GetFinalizers(), ownerFinalizer) {
			return fmt.Errorf("w isn't established, its last pass %+v: %w", r.c.lastResult(nn).Acquired, cutest.ErrNotYet)
		}
		return nil
	})
	if f := finalizersOf(t, ns, "s"); len(f) != 1 {
		t.Errorf("the Secret's finalizers are %v, want the hold", f)
	}
}
