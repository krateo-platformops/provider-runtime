package reconcilertest

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
)

// refsConnecter names refs, and its Connect reads the Secrets in reads, and
// the Secret its config ConfigMap's "secret" key names, through rd.
type refsConnecter struct {
	rd    client.Reader
	refs  []credentialuse.Ref
	reads []string
	chain bool
}

func (c *refsConnecter) CredentialRefs(resource.Managed) ([]credentialuse.Ref, error) {
	return c.refs, nil
}

func (c *refsConnecter) Connect(ctx context.Context, _ resource.Managed) (reconciler.ExternalClient, error) {
	for _, name := range c.reads {
		if err := c.rd.Get(ctx, client.ObjectKey{Namespace: "ns", Name: name}, &corev1.Secret{}); err != nil {
			return nil, err
		}
	}
	if c.chain {
		// The ConfigMap as the pass held it, as a provider's Connect reads a
		// chain's hop (§9.2).
		cm, ok := credentialuse.Resolved(ctx, configRef(nil))
		if !ok {
			// Without a Next the ConfigMap isn't followed: read it.
			cm = &unstructured.Unstructured{}
			cm.SetAPIVersion("v1")
			cm.SetKind("ConfigMap")
			if err := c.rd.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "config"}, cm); err != nil {
				return nil, err
			}
		}
		name, _, _ := unstructured.NestedString(cm.Object, "data", "secret")
		if err := c.rd.Get(ctx, client.ObjectKey{Namespace: "ns", Name: name}, &corev1.Secret{}); err != nil {
			return nil, err
		}
	}
	return &reconciler.NopClient{}, nil
}

// configRef names ConfigMap ns/config, whose "secret" key names a Secret.
func configRef(next func(*unstructured.Unstructured) ([]credentialuse.Ref, error)) credentialuse.Ref {
	return credentialuse.Ref{GroupKind: schema.GroupKind{Kind: "ConfigMap"}, Namespace: "ns", Name: "config", Next: next}
}

func secretFromConfig(u *unstructured.Unstructured) ([]credentialuse.Ref, error) {
	name, _, _ := unstructured.NestedString(u.Object, "data", "secret")
	return []credentialuse.Ref{credentialuse.SecretRef("ns", name)}, nil
}

func fixture(t *testing.T) (*RecordingReader, *runtime.Scheme) {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(s).WithObjects(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "creds"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "other"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "behind"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "config"}, Data: map[string]string{"secret": "behind"}},
	).Build()
	return NewRecordingReader(c, s), s
}

// §11.9: AssertCredentialRefsCover passes a connecter whose refs name what
// Connect reads, a chain's end included, and fails one that reads an
// unnamed Secret, and one whose ref resolves to nothing.
func TestAssertCredentialRefsCover(t *testing.T) {
	cases := map[string]struct {
		c    func(rd client.Reader) *refsConnecter
		want string // a failure's text, or "" for none
	}{
		"covers": {c: func(rd client.Reader) *refsConnecter {
			return &refsConnecter{rd: rd, refs: []credentialuse.Ref{credentialuse.SecretRef("ns", "creds")}, reads: []string{"creds"}}
		}},
		"covers a chain": {c: func(rd client.Reader) *refsConnecter {
			return &refsConnecter{rd: rd, refs: []credentialuse.Ref{configRef(secretFromConfig)}, chain: true}
		}},
		"an unnamed read": {c: func(rd client.Reader) *refsConnecter {
			return &refsConnecter{rd: rd, refs: []credentialuse.Ref{credentialuse.SecretRef("ns", "creds")}, reads: []string{"creds", "other"}}
		}, want: "Connect read Secret ns/other, which CredentialRefs doesn't name"},
		"a chain whose end isn't named": {c: func(rd client.Reader) *refsConnecter {
			return &refsConnecter{rd: rd, refs: []credentialuse.Ref{configRef(nil)}, chain: true}
		}, want: "Connect read Secret ns/behind, which CredentialRefs doesn't name"},
		"a ref that resolves to nothing": {c: func(rd client.Reader) *refsConnecter {
			return &refsConnecter{rd: rd, refs: []credentialuse.Ref{credentialuse.SecretRef("ns", "mistyped")}}
		}, want: "ref /Secret/ns/mistyped resolves to nothing"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec, _ := fixture(t)
			tb := &recordingTB{TB: t}
			_, _ = AssertCredentialRefsCover(tb, context.Background(), tc.c(rec), &fake.Managed{}, rec, schema.GroupKind{Kind: "ConfigMap"})
			got := strings.Join(tb.failures, "\n")
			switch {
			case tc.want == "" && got != "":
				t.Errorf("failed a covering connecter: %s", got)
			case tc.want != "" && !strings.Contains(got, tc.want):
				t.Errorf("failures = %q, want one containing %q", got, tc.want)
			}
		})
	}
}
