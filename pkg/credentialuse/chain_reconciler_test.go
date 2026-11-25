package credentialuse_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

// connecterFn is a connecter that names refs, and whose Connect is a func.
type connecterFn struct {
	refs    []cu.Ref
	connect func(ctx context.Context)
}

func (c connecterFn) CredentialRefs(resource.Managed) ([]cu.Ref, error) { return c.refs, nil }

func (c connecterFn) Connect(ctx context.Context, _ resource.Managed) (reconciler.ExternalClient, error) {
	c.connect(ctx)
	return &reconciler.ExternalClientFns{
		ObserveFn: func(context.Context, resource.Managed) (reconciler.ExternalObservation, error) {
			return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	}, nil
}

// §9.2: through provider-runtime's reconciler, Connect's context carries what
// Acquire read for a ref with a Next, for Resolved; here a Guaranteed
// ConfigMap, read live. It is here and not in pkg/reconciler because only
// this package's tests can have a tracker follow a chain, which production
// refuses in this release.
func TestCredentialUseResolvedOnConnectsContext(t *testing.T) {
	ctx := context.Background()
	gvk := fake.GVK(&fake.Managed{})
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, gvk.GroupVersion()})
	mapper.Add(gvk, meta.RESTScopeNamespace)
	for _, kind := range []string{"Secret", "ConfigMap"} {
		mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: kind}, meta.RESTScopeNamespace)
	}
	m, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		t.Fatal(err)
	}
	crd := &unstructured.Unstructured{}
	crd.SetAPIVersion("apiextensions.k8s.io/v1")
	crd.SetKind("CustomResourceDefinition")
	crd.SetName(m.Resource.Resource + "." + gvk.Group)
	crd.SetResourceVersion("1")
	configMap := &unstructured.Unstructured{Object: map[string]any{"data": map[string]any{"host": "db"}}}
	configMap.SetAPIVersion("v1")
	configMap.SetKind("ConfigMap")
	configMap.SetNamespace("demo")
	configMap.SetName("config")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
			{Version: "v1", Resource: "configmaps"}:                                               "ConfigMapList",
		}, crd, configMap)
	mdScheme := metadatafake.NewTestScheme()
	if err := metav1.AddMetaToScheme(mdScheme); err != nil {
		t.Fatal(err)
	}

	config := cu.Ref{GroupKind: schema.GroupKind{Kind: "ConfigMap"}, Namespace: "demo", Name: "config", Guaranteed: true,
		// It leads to no ref, so the pass writes no record.
		Next: func(*unstructured.Unstructured) ([]cu.Ref, error) { return nil, nil }}
	for name, tc := range map[string]struct {
		follow bool
		want   string
	}{
		"followed": {follow: true, want: "db"},
		// The established holder proceeds to Connect, with nothing resolved.
		"refused, as in production": {},
	} {
		t.Run(name, func(t *testing.T) {
			s := cu.NewShared(cu.Clients{Metadata: metadatafake.NewSimpleMetadataClient(mdScheme), Dynamic: dyn, Mapper: mapper}, cu.SharedOptions{})
			if tc.follow {
				s.FollowChains()
			}
			kube := test.NewMockClient()
			kube.MockGet = test.NewMockGetFn(nil, func(o client.Object) error {
				o.SetNamespace("demo")
				o.SetName("w")
				o.SetUID("uid-w")
				o.SetFinalizers([]string{reconciler.FinalizerName})
				return nil
			})
			connected, got := false, ""
			c := connecterFn{refs: []cu.Ref{config}, connect: func(ctx context.Context) {
				connected = true
				if u, ok := cu.Resolved(ctx, config); ok {
					got, _, _ = unstructured.NestedString(u.Object, "data", "host")
				}
			}}
			r := reconciler.NewReconciler(&fake.Manager{Client: kube, Scheme: fake.SchemeWith(&fake.Managed{})}, resource.ManagedKind(gvk),
				reconciler.WithExternalConnecter(c), reconciler.WithCredentialUseShared(s))
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()
			if err := r.CredentialUseSource().Start(ctx, q); err != nil {
				t.Fatal(err)
			}
			req := reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "demo", Name: "w"}}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if !connected || got != tc.want {
				t.Errorf("connected %v, Resolved on Connect's context gave %q; want %q", connected, got, tc.want)
			}
		})
	}
}
