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

// withContent are a kind's options that protect Secrets and gvk, with Content.
func withContent(gvk schema.GroupVersionKind) credentialuse.KindOptions {
	return credentialuse.KindOptions{Protected: []credentialuse.ProtectedKind{
		{GVK: corev1.SchemeGroupVersion.WithKind("Secret"), Content: true}, {GVK: gvk, Content: true},
	}}
}

// managedFieldsReader serves every unstructured object with managedFields, as
// an API server does.
type managedFieldsReader struct{ client.Reader }

func (r managedFieldsReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if u, ok := obj.(*unstructured.Unstructured); ok && err == nil {
		u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl"}})
	}
	return err
}

// resolvingConnecter keeps what its Connect found in Resolved for ref.
type resolvingConnecter struct {
	refsConnecter
	ref credentialuse.Ref
	got *unstructured.Unstructured
}

func (c *resolvingConnecter) Connect(ctx context.Context, mg resource.Managed) (reconciler.ExternalClient, error) {
	c.got, _ = credentialuse.Resolved(ctx, c.ref)
	return c.refsConnecter.Connect(ctx, mg)
}

// §11.9: each Next, and Resolved, is given the object in the shape the
// runtime gives it, without status and managedFields (§10.4). So a connecter
// that names its Secret from an object's status, which nothing would hold in
// production, fails here.
func TestAssertCredentialRefsCoverGivesNextTheRuntimesObject(t *testing.T) {
	withChains(t)
	for name, guaranteed := range map[string]bool{"held": false, "Guaranteed": true} {
		t.Run(name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := corev1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			c := clientfake.NewClientBuilder().WithScheme(s).WithObjects(
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "creds"}},
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p"}, Status: corev1.PodStatus{NominatedNodeName: "creds"}},
			).Build()
			rec := NewRecordingReader(managedFieldsReader{c}, s)
			raw := &unstructured.Unstructured{}
			raw.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
			if err := rec.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, raw); err != nil || raw.Object["status"] == nil || raw.GetManagedFields() == nil {
				t.Fatalf("the fixture's object = %v, %v; want it with status and managedFields", raw.Object, err)
			}
			check := func(what string, u *unstructured.Unstructured) {
				t.Helper()
				if u == nil {
					t.Fatalf("%s: no object", what)
				}
				if _, has := u.Object["status"]; has || u.GetManagedFields() != nil || u.GetName() != "p" {
					t.Errorf("%s: %v; want p without status and managedFields", what, u.Object)
				}
			}
			var seen *unstructured.Unstructured
			ref := credentialuse.Ref{GroupKind: schema.GroupKind{Kind: "Pod"}, Namespace: "ns", Name: "p", Guaranteed: guaranteed,
				Next: func(u *unstructured.Unstructured) ([]credentialuse.Ref, error) {
					seen = u
					name, _, _ := unstructured.NestedString(u.Object, "status", "nominatedNodeName")
					if name == "" {
						return nil, nil
					}
					return []credentialuse.Ref{credentialuse.SecretRef("ns", name)}, nil
				}}
			conn := &resolvingConnecter{ref: ref, refsConnecter: refsConnecter{rd: rec, refs: []credentialuse.Ref{ref}, reads: []string{"creds"}}}
			tb := &recordingTB{TB: t}
			if _, err := AssertCredentialRefsCover(tb, context.Background(), conn, &fake.Managed{}, rec, withContent(corev1.SchemeGroupVersion.WithKind("Pod"))); err != nil {
				t.Fatal(err)
			}
			check("Next", seen)
			check("Resolved", conn.got)
			if got, want := strings.Join(tb.failures, "\n"), "Connect read Secret ns/creds, which CredentialRefs doesn't name"; !strings.Contains(got, want) {
				t.Errorf("failures = %q, want one containing %q", got, want)
			}
		})
	}
}

// What stops a holder at run time fails a test: a chain deeper than MaxDepth,
// the kind's own when it sets one, and a held ref with a Next of a kind
// without Content. The order the refs are met in, and so which of one object
// is first, is the runtime's.
func TestAssertCredentialRefsCoverEnforcesTheRuntimesRules(t *testing.T) {
	withChains(t)
	secretTo := func(name string, next credentialuse.Ref) credentialuse.Ref {
		s := credentialuse.SecretRef("ns", name)
		s.Next = func(*unstructured.Unstructured) ([]credentialuse.Ref, error) { return []credentialuse.Ref{next}, nil }
		return s
	}
	configMap := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	deep := []credentialuse.Ref{secretTo("creds", secretTo("other", configRef(secretFromConfig)))}
	deeper := withContent(configMap)
	deeper.MaxDepth = 4
	guaranteed := configRef(secretFromConfig)
	guaranteed.Guaranteed = true
	for name, tc := range map[string]struct {
		refs []credentialuse.Ref
		o    credentialuse.KindOptions
		want string
	}{
		"deeper than the default MaxDepth": {refs: deep, o: withContent(configMap), want: "deeper than MaxDepth (3)"},
		"within the kind's MaxDepth":       {refs: deep, o: deeper},
		"a level of refs already met isn't counted": {
			refs: []credentialuse.Ref{secretTo("creds", secretTo("other", secretTo("behind", credentialuse.SecretRef("ns", "creds"))))},
			o:    withContent(configMap),
		},
		"a held Next without Content": {
			refs: []credentialuse.Ref{configRef(secretFromConfig)},
			o:    credentialuse.KindOptions{Protected: []credentialuse.ProtectedKind{{GVK: configMap}}},
			want: "must be of a protected kind with Content: /ConfigMap/ns/config",
		},
		"a Guaranteed Next needs none": {
			refs: []credentialuse.Ref{guaranteed},
			o:    credentialuse.KindOptions{Protected: []credentialuse.ProtectedKind{{GVK: configMap}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec, _ := fixture(t)
			tb := &recordingTB{TB: t}
			_, _ = AssertCredentialRefsCover(tb, context.Background(), &refsConnecter{rd: rec, refs: tc.refs}, &fake.Managed{}, rec, tc.o)
			got := strings.Join(tb.failures, "\n")
			if tc.want == "" && got != "" || !strings.Contains(got, tc.want) {
				t.Errorf("failures = %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("the first Next met is the runtime's", func(t *testing.T) {
		rec, _ := fixture(t)
		var got []string
		config := func(name string) credentialuse.Ref {
			return configRef(func(*unstructured.Unstructured) ([]credentialuse.Ref, error) {
				got = append(got, name)
				return nil, nil
			})
		}
		byGuaranteed := secretTo("behind", config("behind a Guaranteed Secret"))
		byGuaranteed.Guaranteed = true
		refs := []credentialuse.Ref{byGuaranteed, secretTo("other", config("behind other")), secretTo("creds", config("behind creds"))}
		tb := &recordingTB{TB: t}
		_, _ = AssertCredentialRefsCover(tb, context.Background(), &refsConnecter{rd: rec, refs: refs}, &fake.Managed{}, rec, withContent(configMap))
		if len(tb.failures) != 0 || len(got) != 1 || got[0] != "behind creds" {
			t.Errorf("failures %q, followed %q; want only the one behind creds", tb.failures, got)
		}
	})
}

// AssertCredentialRefsCover refuses refs of one object of which a later one
// asks for more than the first, a hold or a Next, wherever it is named: the
// runtime takes the first and reports the rest (§9.1).
func TestAssertCredentialRefsCoverRefsThatConflict(t *testing.T) {
	withChains(t)
	held := configRef(secretFromConfig)
	plain := configRef(nil)
	guaranteed := held
	guaranteed.Guaranteed = true
	plainGuaranteed := plain
	plainGuaranteed.Guaranteed = true
	via := func(r credentialuse.Ref) credentialuse.Ref {
		s := credentialuse.SecretRef("ns", "creds")
		s.Next = func(*unstructured.Unstructured) ([]credentialuse.Ref, error) { return []credentialuse.Ref{r}, nil }
		return s
	}
	const hold, next = "a hold, which the first, Guaranteed, doesn't ask for", "a Next, which the first doesn't have"
	for name, tc := range map[string]struct {
		refs []credentialuse.Ref
		want []string
	}{
		"Guaranteed, then held":               {refs: []credentialuse.Ref{guaranteed, held}, want: []string{hold}},
		"no Next, then one":                   {refs: []credentialuse.Ref{plain, held}, want: []string{next}},
		"both":                                {refs: []credentialuse.Ref{plainGuaranteed, held}, want: []string{hold, next}},
		"Guaranteed, then held behind a Next": {refs: []credentialuse.Ref{guaranteed, via(held)}, want: []string{hold}},
		"no Next, then one behind a Next":     {refs: []credentialuse.Ref{plain, via(held)}, want: []string{next}},
		"held, then Guaranteed":               {refs: []credentialuse.Ref{held, guaranteed}},
		"a Next, then none":                   {refs: []credentialuse.Ref{held, plain}},
		"held twice":                          {refs: []credentialuse.Ref{held, held}},
		"held, then Guaranteed behind a Next": {refs: []credentialuse.Ref{held, via(guaranteed)}},
	} {
		t.Run(name, func(t *testing.T) {
			rec, _ := fixture(t)
			tb := &recordingTB{TB: t}
			_, _ = AssertCredentialRefsCover(tb, context.Background(), &refsConnecter{rd: rec, refs: tc.refs}, &fake.Managed{}, rec, withContent(corev1.SchemeGroupVersion.WithKind("ConfigMap")))
			got := strings.Join(tb.failures, "\n")
			if len(tc.want) == 0 && got != "" {
				t.Errorf("failures = %q, want none", got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, "/ConfigMap/ns/config") || !strings.Contains(got, w) {
					t.Errorf("failures = %q, want one naming /ConfigMap/ns/config, and %q", got, w)
				}
			}
		})
	}
}

// §11.9: AssertCredentialRefsCover passes a connecter whose refs name what
// Connect reads, a chain's end included, and fails one that reads an
// unnamed Secret, and one whose ref resolves to nothing.
func TestAssertCredentialRefsCover(t *testing.T) {
	withChains(t)
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
			_, _ = AssertCredentialRefsCover(tb, context.Background(), tc.c(rec), &fake.Managed{}, rec, withContent(corev1.SchemeGroupVersion.WithKind("ConfigMap")))
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

// withChains has AssertCredentialRefsCover follow chains until t ends, as it
// will once the runtime does.
func withChains(t *testing.T) {
	t.Helper()
	followChains = true
	t.Cleanup(func() { followChains = false })
}

// §9, "Not in this release": AssertCredentialRefsCover fails a connecter one
// of whose refs has a Next, as the runtime refuses it, before it reads
// anything or connects. One without is checked as before, Guaranteed refs
// included.
func TestAssertCredentialRefsCoverRefusesAChain(t *testing.T) {
	guaranteed := func(r credentialuse.Ref) credentialuse.Ref { r.Guaranteed = true; return r }
	direct := credentialuse.SecretRef("ns", "creds")
	for name, tc := range map[string]struct {
		refs    []credentialuse.Ref
		refused bool
	}{
		"a chain":                         {refs: []credentialuse.Ref{direct, configRef(secretFromConfig)}, refused: true},
		"a Guaranteed ref with a Next":    {refs: []credentialuse.Ref{direct, guaranteed(configRef(secretFromConfig))}, refused: true},
		"a Guaranteed ref without a Next": {refs: []credentialuse.Ref{direct, guaranteed(configRef(nil))}},
	} {
		t.Run(name, func(t *testing.T) {
			rec, _ := fixture(t)
			tb := &fatalTB{recordingTB: recordingTB{TB: t}}
			ec, err := AssertCredentialRefsCover(tb, context.Background(),
				&refsConnecter{rd: rec, refs: tc.refs, reads: []string{"creds"}}, &fake.Managed{}, rec, withContent(corev1.SchemeGroupVersion.WithKind("ConfigMap")))
			if err != nil {
				t.Fatal(err)
			}
			if !tc.refused {
				if ec == nil || len(tb.failures) != 0 {
					t.Errorf("client %v, failures %q; want it connected and passed", ec, tb.failures)
				}
				return
			}
			if len(tb.failures) != 1 || !strings.Contains(tb.failures[0], credentialuse.ErrChainsNotSupported.Error()) || !strings.Contains(tb.failures[0], "/ConfigMap/ns/config") {
				t.Errorf("failures = %q, want one saying chains aren't supported and naming the ref", tb.failures)
			}
			if ec != nil || len(rec.Reads()) != 0 {
				t.Errorf("client %v, reads %v; want it not connected", ec, rec.Reads())
			}
		})
	}
}
